"""
Wish pool API.
"""

import json
import time
from typing import Any, Optional

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config import DEFAULT_MODEL
from database import get_db
from ws import manager
from wish_pool import (
    create_wish,
    delete_wish,
    draw_wish,
    get_actor_names,
    get_wish,
    list_wishes,
    update_wish,
)


router = APIRouter(prefix="/api/wishes", tags=["wishes"])


class WishCreate(BaseModel):
    content: str
    author: str = "user"
    visibility: str = "shared"


class WishUpdate(BaseModel):
    content: Optional[str] = None
    status: Optional[str] = None
    visibility: Optional[str] = None


class WishAnnounce(BaseModel):
    target: str
    conv_id: Optional[str] = None


def _clean_short_text(value: Any, limit: int = 600) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split())[:limit]


async def _get_or_create_main_conv(conv_id: str = "") -> str:
    conv_id = (conv_id or "").strip()
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        if conv_id:
            cur = await db.execute("SELECT id FROM conversations WHERE id=?", (conv_id,))
            row = await cur.fetchone()
            if row:
                return row["id"]

        cur = await db.execute("SELECT id FROM conversations ORDER BY updated_at DESC LIMIT 1")
        row = await cur.fetchone()
        if row:
            return row["id"]

        now = time.time()
        new_conv_id = f"conv_{time.time_ns()}"
        await db.execute(
            "INSERT INTO conversations (id, title, model, created_at, updated_at) VALUES (?,?,?,?,?)",
            (new_conv_id, "悄悄话", DEFAULT_MODEL, now, now),
        )
        await db.commit()

    conv = {"id": new_conv_id, "title": "悄悄话", "model": DEFAULT_MODEL, "created_at": now, "updated_at": now}
    await manager.broadcast({"type": "conv_created", "data": conv})
    return new_conv_id


async def _save_main_user_message(conv_id: str, content: str, attachments: list[dict[str, Any]]) -> dict[str, Any]:
    now = time.time()
    msg_id = f"msg_{time.time_ns()}_wish"
    async with get_db() as db:
        await db.execute(
            "INSERT INTO messages (id, conv_id, role, content, created_at, attachments) VALUES (?,?,?,?,?,?)",
            (msg_id, conv_id, "user", content, now, json.dumps(attachments, ensure_ascii=False)),
        )
        await db.execute("UPDATE conversations SET updated_at=? WHERE id=?", (now, conv_id))
        await db.commit()
    msg = {"id": msg_id, "conv_id": conv_id, "role": "user", "content": content, "created_at": now, "attachments": attachments}
    await manager.broadcast({"type": "msg_created", "data": msg})
    try:
        from routes.files import export_conversation

        await export_conversation(conv_id)
    except Exception:
        pass
    return msg


async def _get_or_create_group_room() -> str:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id FROM chatroom_rooms WHERE type='group' ORDER BY updated_at DESC LIMIT 1")
        row = await cur.fetchone()
        if row:
            return row["id"]

        now = time.time()
        room_id = f"cr_{time.time_ns()}"
        title = "群聊"
        await db.execute(
            "INSERT INTO chatroom_rooms (id, title, type, aion_persona, connor_persona, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (room_id, title, "group", "", "", now, now),
        )
        await db.commit()

    room = {
        "id": room_id,
        "title": title,
        "type": "group",
        "aion_persona": "",
        "connor_persona": "",
        "context_limit": 30,
        "context_minutes": 30,
        "ai_chat_rounds": 1,
        "created_at": now,
        "updated_at": now,
        "message_count": 0,
    }
    await manager.broadcast({"type": "chatroom_room_created", "data": room})
    return room_id


async def _mark_wish_mentioned(wish_id: str) -> None:
    now = time.time()
    async with get_db() as db:
        await db.execute("UPDATE wishes SET last_mentioned_at=?, updated_at=? WHERE id=?", (now, now, wish_id))
        await db.commit()


@router.get("")
async def api_list_wishes(
    status: str = Query("all", pattern="^(all|active|fulfilled|released)$"),
    author: str = Query("all", pattern="^(all|user|aion|connor)$"),
):
    return await list_wishes(status=status, author=author)


@router.get("/actors")
async def api_wish_actors():
    return {"actors": await get_actor_names()}


@router.post("")
async def api_create_wish(body: WishCreate):
    try:
        return await create_wish(
            author=body.author,
            content=body.content,
            visibility=body.visibility,
            origin="manual",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/draw")
async def api_draw_wish(
    author: str = Query("all", pattern="^(all|user|aion|connor)$"),
    drawer: str = Query("", pattern="^(|user|aion|connor)$"),
):
    wish = await draw_wish(author=author, drawer=drawer)
    return {"ok": True, "wish": wish}


@router.post("/{wish_id}/announce")
async def api_announce_wish(wish_id: str, body: WishAnnounce):
    target = (body.target or "").strip().lower()
    if target not in {"aion", "connor", "group"}:
        raise HTTPException(status_code=400, detail="invalid target")
    wish = await get_wish(wish_id)
    if not wish:
        raise HTTPException(status_code=404, detail="Wish not found")

    actors = await get_actor_names()
    author = wish.get("author") or "user"
    author_name = _clean_short_text(wish.get("author_name") or actors.get(author) or author, 80)
    content = _clean_short_text(wish.get("content") or "", 600)
    public_message = f"我捞起了【{author_name}】的愿望，愿望内容：{content}。现在将为他实现。"
    message_text = f"\u2063wish_fulfillment:{wish['id']}\u2063{public_message}"
    attachment = {
        "type": "wish_fulfillment",
        "wish_id": wish["id"],
        "author": author,
        "author_name": author_name,
        "content": content,
        "status": wish.get("status") or "active",
        "target": target,
        "message": public_message,
    }

    if target == "aion":
        conv_id = await _get_or_create_main_conv(body.conv_id or "")
        message = await _save_main_user_message(conv_id, message_text, [attachment])
        await _mark_wish_mentioned(wish["id"])
        return {"ok": True, "target": target, "message": message, "conv_id": conv_id, "url": "/chat"}

    from routes.chatroom import _get_or_create_connor_private_room, _save_msg

    room_id = await _get_or_create_connor_private_room() if target == "connor" else await _get_or_create_group_room()
    message = await _save_msg(room_id, "user", message_text, msg_id=f"cm_{time.time_ns()}_wish", attachments=[attachment], auto_tts=False)
    await _mark_wish_mentioned(wish["id"])
    return {"ok": True, "target": target, "message": message, "room_id": room_id, "url": f"/chatroom?room={room_id}"}


@router.get("/{wish_id}")
async def api_get_wish(wish_id: str):
    wish = await get_wish(wish_id)
    if not wish:
        raise HTTPException(status_code=404, detail="Wish not found")
    return wish


@router.patch("/{wish_id}")
async def api_update_wish(wish_id: str, body: WishUpdate):
    updates = body.model_dump(exclude_unset=True) if hasattr(body, "model_dump") else body.dict(exclude_unset=True)
    try:
        wish = await update_wish(wish_id, updates)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not wish:
        raise HTTPException(status_code=404, detail="Wish not found")
    return wish


@router.delete("/{wish_id}")
async def api_delete_wish(wish_id: str):
    ok = await delete_wish(wish_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Wish not found")
    return {"ok": True}
