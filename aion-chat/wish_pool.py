"""
Wish pool storage and optional AI wish generation.
"""

import json
import random
import re
import time
from typing import Any, Awaitable, Callable, Optional

import aiosqlite

from database import get_db
from ws import manager


VALID_AUTHORS = {"user", "aion", "connor"}
VALID_STATUSES = {"active", "fulfilled", "released"}
VALID_VISIBILITIES = {"shared", "private"}


def _now() -> float:
    return time.time()


async def ensure_wish_schema() -> None:
    async with get_db() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wishes (
                id TEXT PRIMARY KEY,
                author TEXT NOT NULL,
                author_name TEXT DEFAULT '',
                content TEXT NOT NULL,
                category TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                visibility TEXT DEFAULT 'shared',
                origin TEXT DEFAULT 'manual',
                source_type TEXT DEFAULT '',
                source_ref TEXT DEFAULT '',
                source_start_ts REAL,
                source_end_ts REAL,
                pulled_count INTEGER NOT NULL DEFAULT 0,
                last_pulled_at REAL,
                fulfilled_at REAL,
                released_at REAL,
                last_mentioned_at REAL,
                metadata TEXT DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        for col, defn in [
            ("author_name", "TEXT DEFAULT ''"),
            ("category", "TEXT DEFAULT ''"),
            ("visibility", "TEXT DEFAULT 'shared'"),
            ("origin", "TEXT DEFAULT 'manual'"),
            ("source_type", "TEXT DEFAULT ''"),
            ("source_ref", "TEXT DEFAULT ''"),
            ("source_start_ts", "REAL"),
            ("source_end_ts", "REAL"),
            ("pulled_count", "INTEGER NOT NULL DEFAULT 0"),
            ("last_pulled_at", "REAL"),
            ("fulfilled_at", "REAL"),
            ("released_at", "REAL"),
            ("last_mentioned_at", "REAL"),
            ("metadata", "TEXT DEFAULT '{}'"),
            ("updated_at", "REAL"),
        ]:
            try:
                await db.execute(f"ALTER TABLE wishes ADD COLUMN {col} {defn}")
            except Exception:
                pass
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wishes_status ON wishes(status, created_at DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wishes_author_status ON wishes(author, status, created_at DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wishes_pulled ON wishes(last_pulled_at)")
        await db.commit()


def normalize_author(author: str) -> str:
    value = (author or "user").strip().lower()
    return value if value in VALID_AUTHORS else "user"


def normalize_status(status: str) -> str:
    value = (status or "active").strip().lower()
    if value not in VALID_STATUSES:
        raise ValueError("invalid status")
    return value


async def _sync_wish_card_status(wish_id: str, status: str) -> None:
    if status not in VALID_STATUSES:
        return
    async with get_db() as db:
        for table in ("messages", "chatroom_messages"):
            cur = await db.execute(
                f"SELECT id, attachments FROM {table} WHERE attachments LIKE ?",
                (f'%"{wish_id}"%',),
            )
            rows = await cur.fetchall()
            for row in rows:
                msg_id, raw_attachments = row[0], row[1]
                try:
                    attachments = json.loads(raw_attachments or "[]")
                except Exception:
                    continue
                if not isinstance(attachments, list):
                    continue
                changed = False
                for item in attachments:
                    if (
                        isinstance(item, dict)
                        and item.get("type") == "wish_fulfillment"
                        and item.get("wish_id") == wish_id
                    ):
                        item["status"] = status
                        changed = True
                if changed:
                    await db.execute(
                        f"UPDATE {table} SET attachments=? WHERE id=?",
                        (json.dumps(attachments, ensure_ascii=False), msg_id),
                    )
        await db.commit()


def normalize_visibility(visibility: str) -> str:
    value = (visibility or "shared").strip().lower()
    return value if value in VALID_VISIBILITIES else "shared"


def _clean_text(value: str, limit: int = 500) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _safe_json_object(value: Any) -> str:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            return "{}"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return "{}"


def parse_json_object(raw: str) -> Optional[dict[str, Any]]:
    if not raw:
        return None
    text = raw.strip()
    if "```" in text:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            text = text[start:end]
    try:
        data = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start < 0 or end <= start:
            return None
        try:
            data = json.loads(text[start:end])
        except Exception:
            return None
    return data if isinstance(data, dict) else None


def serialize_wish(row: aiosqlite.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    try:
        metadata = json.loads(data.get("metadata") or "{}")
    except Exception:
        metadata = {}
    data["metadata"] = metadata if isinstance(metadata, dict) else {}
    return data


async def get_actor_names() -> dict[str, str]:
    from config import load_worldbook

    wb = load_worldbook()
    user_name = (wb.get("user_name") or "用户").strip() or "用户"
    ai_name = (wb.get("ai_name") or "AI").strip() or "AI"
    try:
        from chatroom import get_chatroom_names

        user_name2, ai_name2, connor_name = get_chatroom_names()
        user_name = (user_name2 or user_name).strip() or user_name
        ai_name = (ai_name2 or ai_name).strip() or ai_name
        connor_name = (connor_name or "第二位AI").strip() or "第二位AI"
    except Exception:
        connor_name = "第二位AI"
    return {"user": user_name, "aion": ai_name, "connor": connor_name}


async def list_wishes(status: str = "all", author: str = "all") -> dict[str, Any]:
    await ensure_wish_schema()
    where = []
    params: list[Any] = []
    if status != "all":
        where.append("status=?")
        params.append(normalize_status(status))
    if author != "all":
        where.append("author=?")
        params.append(normalize_author(author))
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM wishes {where_sql} ORDER BY created_at DESC",
            params,
        )
        rows = [serialize_wish(row) for row in await cur.fetchall()]
        cur = await db.execute(
            "SELECT status, COUNT(*) AS count FROM wishes GROUP BY status"
        )
        counts = {row["status"]: row["count"] for row in await cur.fetchall()}
    return {
        "items": rows,
        "counts": {
            "all": sum(int(v) for v in counts.values()),
            "active": int(counts.get("active", 0)),
            "fulfilled": int(counts.get("fulfilled", 0)),
            "released": int(counts.get("released", 0)),
        },
        "actors": await get_actor_names(),
    }


async def create_wish(
    *,
    author: str,
    content: str,
    visibility: str = "shared",
    origin: str = "manual",
    source_type: str = "",
    source_ref: str = "",
    source_start_ts: float | None = None,
    source_end_ts: float | None = None,
    metadata: dict[str, Any] | None = None,
    author_name: str = "",
) -> dict[str, Any]:
    await ensure_wish_schema()
    author_key = normalize_author(author)
    content_text = _clean_text(content, 600)
    if not content_text:
        raise ValueError("empty wish")
    actor_names = await get_actor_names()
    display_name = _clean_text(author_name or actor_names.get(author_key) or author_key, 80)
    now = _now()
    wish_id = f"wish_{time.time_ns()}_{author_key[:1]}"
    async with get_db() as db:
        await db.execute(
            "INSERT INTO wishes ("
            "id, author, author_name, content, category, status, visibility, origin, "
            "source_type, source_ref, source_start_ts, source_end_ts, metadata, created_at, updated_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                wish_id,
                author_key,
                display_name,
                content_text,
                "",
                "active",
                normalize_visibility(visibility),
                _clean_text(origin, 40) or "manual",
                _clean_text(source_type, 60),
                _clean_text(source_ref, 200),
                source_start_ts,
                source_end_ts,
                _safe_json_object(metadata or {}),
                now,
                now,
            ),
        )
        await db.commit()
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM wishes WHERE id=?", (wish_id,))
        row = await cur.fetchone()
    wish = serialize_wish(row)
    await manager.broadcast({"type": "wish_created", "data": wish})
    return wish


def _draw_weight(wish: dict[str, Any], now: float) -> float:
    pulled_count = max(0, int(wish.get("pulled_count") or 0))
    last_pulled = float(wish.get("last_pulled_at") or 0)
    age_days = max(0.0, (now - float(wish.get("created_at") or now)) / 86400)
    idle_days = 30.0 if last_pulled <= 0 else max(0.0, (now - last_pulled) / 86400)
    return max(0.1, 1.0 + min(3.0, idle_days / 7.0) + min(1.5, age_days / 30.0) - pulled_count * 0.18)


async def draw_wish(author: str = "all", drawer: str = "") -> dict[str, Any] | None:
    await ensure_wish_schema()
    where = ["status='active'"]
    params: list[Any] = []
    if author != "all":
        where.append("author=?")
        params.append(normalize_author(author))
    drawer_key = normalize_author(drawer) if drawer else ""
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM wishes WHERE {' AND '.join(where)}",
            params,
        )
        rows = [serialize_wish(row) for row in await cur.fetchall()]
        if not rows:
            return None
        if drawer_key:
            other_rows = [row for row in rows if row.get("author") != drawer_key]
            if drawer_key == "user":
                rows = other_rows
                if not rows:
                    return None
            elif other_rows:
                rows = other_rows
        now = _now()
        weights = [_draw_weight(row, now) for row in rows]
        selected = random.choices(rows, weights=weights, k=1)[0]
        await db.execute(
            "UPDATE wishes SET pulled_count=COALESCE(pulled_count,0)+1, last_pulled_at=?, updated_at=? WHERE id=?",
            (now, now, selected["id"]),
        )
        await db.commit()
        cur = await db.execute("SELECT * FROM wishes WHERE id=?", (selected["id"],))
        row = await cur.fetchone()
    wish = serialize_wish(row)
    await manager.broadcast({"type": "wish_pulled", "data": wish})
    return wish


async def update_wish(wish_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    await ensure_wish_schema()
    allowed_fields = []
    params: list[Any] = []
    now = _now()
    if "content" in updates and updates["content"] is not None:
        content = _clean_text(updates["content"], 600)
        if not content:
            raise ValueError("empty wish")
        allowed_fields.append("content=?")
        params.append(content)
    if "visibility" in updates and updates["visibility"] is not None:
        allowed_fields.append("visibility=?")
        params.append(normalize_visibility(updates["visibility"]))
    if "status" in updates and updates["status"] is not None:
        status = normalize_status(updates["status"])
        allowed_fields.append("status=?")
        params.append(status)
        if status == "fulfilled":
            allowed_fields.append("fulfilled_at=?")
            params.append(now)
            allowed_fields.append("released_at=NULL")
        elif status == "released":
            allowed_fields.append("released_at=?")
            params.append(now)
        elif status == "active":
            allowed_fields.append("fulfilled_at=NULL")
            allowed_fields.append("released_at=NULL")
    if not allowed_fields:
        return await get_wish(wish_id)
    allowed_fields.append("updated_at=?")
    params.append(now)
    params.append(wish_id)
    async with get_db() as db:
        cur = await db.execute(
            f"UPDATE wishes SET {', '.join(allowed_fields)} WHERE id=?",
            params,
        )
        await db.commit()
        if cur.rowcount == 0:
            return None
    wish = await get_wish(wish_id)
    if wish:
        if "status" in updates and updates["status"] is not None:
            await _sync_wish_card_status(wish_id, wish.get("status") or "active")
        await manager.broadcast({"type": "wish_updated", "data": wish})
    return wish


async def get_wish(wish_id: str) -> dict[str, Any] | None:
    await ensure_wish_schema()
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM wishes WHERE id=?", (wish_id,))
        row = await cur.fetchone()
    return serialize_wish(row) if row else None


async def delete_wish(wish_id: str) -> bool:
    await ensure_wish_schema()
    async with get_db() as db:
        cur = await db.execute("DELETE FROM wishes WHERE id=?", (wish_id,))
        await db.commit()
    ok = (cur.rowcount or 0) > 0
    if ok:
        await manager.broadcast({"type": "wish_deleted", "data": {"id": wish_id}})
    return ok


async def _recent_auto_wish_exists(actor: str) -> bool:
    now = _now()
    local = time.localtime(now)
    day_start = time.mktime((local.tm_year, local.tm_mon, local.tm_mday, 0, 0, 0, local.tm_wday, local.tm_yday, local.tm_isdst))
    async with get_db() as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM wishes WHERE author=? AND origin='auto_digest' AND created_at>=?",
            (actor, day_start),
        )
        today_count = (await cur.fetchone())[0]
        if today_count:
            return True
        cur = await db.execute(
            "SELECT COUNT(*) FROM wishes WHERE author=? AND status='active'",
            (actor,),
        )
        active_count = (await cur.fetchone())[0]
    return int(active_count or 0) >= 12


def _wish_fingerprint(content: str) -> str:
    return re.sub(r"[\W_]+", "", content.lower())[:80]


async def _looks_duplicate(actor: str, content: str) -> bool:
    fp = _wish_fingerprint(content)
    if len(fp) < 4:
        return False
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT content FROM wishes WHERE author=? AND status='active' ORDER BY created_at DESC LIMIT 40",
            (actor,),
        )
        rows = await cur.fetchall()
    for row in rows:
        other = _wish_fingerprint(row["content"])
        if not other:
            continue
        if fp == other or fp in other or other in fp:
            return True
    return False


async def maybe_create_ai_digest_wish(
    *,
    actor: str,
    actor_name: str,
    user_name: str,
    summaries: list[str],
    context_text: str = "",
    persona_block: str = "",
    source_ref: str = "",
    source_start_ts: float | None = None,
    source_end_ts: float | None = None,
    generate_text: Callable[[str], Awaitable[str | None]],
) -> dict[str, Any]:
    await ensure_wish_schema()
    actor_key = normalize_author(actor)
    if actor_key == "user" or not summaries:
        return {"ok": True, "created": False, "reason": "not_ai_or_empty"}
    if await _recent_auto_wish_exists(actor_key):
        return {"ok": True, "created": False, "reason": "daily_or_pool_limit"}

    summaries_text = "\n".join(f"- {str(s).strip()[:500]}" for s in summaries if str(s).strip())
    if not summaries_text:
        return {"ok": True, "created": False, "reason": "empty_summaries"}

    actor_names = await get_actor_names()
    participant_names = [name for name in actor_names.values() if name]
    other_names = [name for key, name in actor_names.items() if key != actor_key and name]
    participants_text = "、".join(participant_names) or user_name
    other_people_text = "、".join(other_names) or user_name

    prompt = (
        f"{persona_block}"
        f"你是{actor_name}。你刚刚整理完一段与你们共同生活/互动有关的记忆摘要。\n"
        f"参与者可能包括：{participants_text}。\n"
        f"你可以选择要不要往许愿池投下一枚小愿望；这不是任务，也不是要求{user_name}必须完成。\n"
        f"愿望可以关于你自己、{user_name}、{other_people_text}，也可以关于你们之间的相处；不必总是用户相关。\n"
        f"只有当今天的记忆让你自然地产生一个轻量、温柔、可被偶尔实现的小愿望时，才许愿。没有合适愿望就不要许。\n"
        f"愿望要像你自己的心愿，不要像系统提醒；不要索要金钱、医疗安全承诺、紧急行动、控制性要求或会让对方有压力的内容。\n"
        f"愿望 8-80 个中文字符，具体但不沉重。\n\n"
        f"【今日摘要】\n{summaries_text}\n\n"
        f"【最近上下文】\n{context_text[:1800]}\n\n"
        f"严格只输出 JSON，不要 Markdown，不要解释：\n"
        f"{{\n"
        f"  \"should_make_wish\": false,\n"
        f"  \"wish\": {{\"content\": \"\", \"visibility\": \"shared\", \"reason\": \"\"}}\n"
        f"}}"
    )
    try:
        raw = await generate_text(prompt)
    except Exception as exc:
        return {"ok": False, "created": False, "reason": f"model_error: {exc}"}
    data = parse_json_object(raw or "")
    if not data or not bool(data.get("should_make_wish")):
        return {"ok": True, "created": False, "reason": "declined"}
    wish_raw = data.get("wish") if isinstance(data.get("wish"), dict) else data
    content = _clean_text(wish_raw.get("content") if isinstance(wish_raw, dict) else "", 120)
    if len(content) < 4:
        return {"ok": True, "created": False, "reason": "too_short"}
    if await _looks_duplicate(actor_key, content):
        return {"ok": True, "created": False, "reason": "duplicate"}
    visibility = normalize_visibility(wish_raw.get("visibility") if isinstance(wish_raw, dict) else "shared")
    reason = _clean_text(wish_raw.get("reason") if isinstance(wish_raw, dict) else "", 240)
    wish = await create_wish(
        author=actor_key,
        author_name=actor_name,
        content=content,
        visibility=visibility,
        origin="auto_digest",
        source_type="memory_digest",
        source_ref=source_ref,
        source_start_ts=source_start_ts,
        source_end_ts=source_end_ts,
        metadata={"reason": reason, "summary_count": len(summaries)},
    )
    return {"ok": True, "created": True, "wish": wish}
