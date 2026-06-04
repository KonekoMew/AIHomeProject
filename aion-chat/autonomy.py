import asyncio
import json
import random
import re
import time
from typing import Any

import aiosqlite

from ai_providers import CLI_STATUS_PREFIX, stream_ai
from config import DEFAULT_MODEL, SETTINGS, load_worldbook, save_settings
from context_builder import fetch_merged_timeline, render_merged_timeline
from database import get_db
from ws import manager


ACTION_DEFS = {
    "seeky_interaction": "和 Seeky 互动",
    "role_chat": "和另一个角色聊一句",
    "memory_browse": "随机翻看记忆库",
    "home_dynamics": "查看近期家庭动态",
    "cam_check": "调取监控查看用户当前状态",
}

SEEKY_ACTIONS = {
    "feed": "投喂",
    "clean": "清理",
    "play": "玩耍",
    "tease": "逗弄",
    "scare": "吓唬",
    "tap_glass": "拍打玻璃",
    "threaten": "威胁",
}

SEEKY_EVENT_PHRASES = {
    "feed": "投喂了",
    "clean": "清理了",
    "play": "玩耍了",
    "tease": "逗弄了",
    "scare": "吓唬了",
    "tap_glass": "拍打了玻璃",
    "threaten": "威胁了",
}


def get_idle_config() -> dict[str, Any]:
    actions = SETTINGS.get("idle_autonomy_actions")
    if not isinstance(actions, dict):
        actions = {key: True for key in ACTION_DEFS}
    return {
        "enabled": bool(SETTINGS.get("idle_autonomy_enabled", False)),
        "interval_minutes": max(5, int(SETTINGS.get("idle_autonomy_interval_minutes", 120) or 120)),
        "actions": {key: bool(actions.get(key, True)) for key in ACTION_DEFS},
    }


def save_idle_config(*, enabled: bool | None = None, interval_minutes: int | None = None, actions: dict | None = None) -> dict:
    if enabled is not None:
        SETTINGS["idle_autonomy_enabled"] = bool(enabled)
    if interval_minutes is not None:
        SETTINGS["idle_autonomy_interval_minutes"] = max(5, int(interval_minutes or 120))
    if actions is not None:
        current = get_idle_config()["actions"]
        for key in ACTION_DEFS:
            if key in actions:
                current[key] = bool(actions[key])
        SETTINGS["idle_autonomy_actions"] = current
    save_settings(SETTINGS)
    return get_idle_config()


def _json_extract(text: str) -> dict:
    raw = (text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start:end + 1]
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _clip(text: str, limit: int = 260) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[:limit].rstrip() + "..."


def _actor_label(actor: str) -> str:
    user_name, ai_name, connor_name = _names()
    if actor in ("aion", "assistant"):
        return ai_name
    if actor == "connor":
        return connor_name
    if actor == "user":
        return user_name
    return actor or "未知"


def _idle_event_home_title(row, shown_diary_ids: set[str], shown_moment_ids: set[str]) -> str | None:
    action = str(row["action"] or "")
    result_type = str(row["result_type"] or "")
    result_id = str(row["result_id"] or "")

    if action == "select":
        return None
    if action == "home_dynamics_result":
        return None
    if action.endswith("_result") and result_type == "message":
        return None
    if action == "memory_browse_result":
        if result_type == "diary" and result_id in shown_diary_ids:
            return None
        if result_type == "moment" and result_id in shown_moment_ids:
            return None
        if result_type not in ("diary", "moment"):
            return None
    if action not in ("home_dynamics", "memory_browse", "memory_browse_result"):
        return None
    return row["title"]


def _names() -> tuple[str, str, str]:
    try:
        from chatroom import get_chatroom_names
        return get_chatroom_names()
    except Exception:
        wb = load_worldbook()
        return wb.get("user_name", "用户"), wb.get("ai_name", "AI"), "Connor"


async def append_idle_event(
    actor: str,
    action: str,
    title: str,
    detail: str = "",
    *,
    target_type: str = "",
    target_id: str = "",
    result_type: str = "",
    result_id: str = "",
    metadata: dict | None = None,
) -> dict:
    now = time.time()
    metadata = metadata or {}
    if action == "select" and metadata.get("selected_action") == "seeky_interaction":
        title = f"{_actor_label(actor)}和 Seeky 进行了互动"
    elif action == "seeky_interaction":
        seeky_action = str(metadata.get("seeky_action") or "").strip()
        action_phrase = SEEKY_EVENT_PHRASES.get(seeky_action)
        if action_phrase:
            title = f"{_actor_label(actor)}对Seeky{action_phrase}"
    event = {
        "id": f"idle_{int(now * 1000)}_{time.time_ns() % 100000}",
        "actor": actor,
        "action": action,
        "title": title,
        "detail": detail,
        "target_type": target_type,
        "target_id": target_id,
        "result_type": result_type,
        "result_id": result_id,
        "metadata": metadata,
        "created_at": now,
    }
    async with get_db() as db:
        await db.execute(
            "INSERT INTO idle_events "
            "(id, actor, action, title, detail, target_type, target_id, result_type, result_id, metadata, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                event["id"], actor, action, title, detail, target_type, target_id,
                result_type, result_id, json.dumps(event["metadata"], ensure_ascii=False), now,
            ),
        )
        await db.commit()
    await manager.broadcast({"type": "idle_event", "data": event})
    return event


async def _latest_group_room_id() -> str | None:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id FROM chatroom_rooms WHERE type='group' ORDER BY updated_at DESC LIMIT 1")
        row = await cur.fetchone()
    return row["id"] if row else None


async def _latest_connor_room_id() -> str | None:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id FROM chatroom_rooms WHERE type IN ('connor_1v1','group') ORDER BY updated_at DESC LIMIT 1"
        )
        row = await cur.fetchone()
    return row["id"] if row else None


async def _latest_conversation() -> tuple[str, str] | tuple[None, str]:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, model FROM conversations ORDER BY updated_at DESC LIMIT 1")
        row = await cur.fetchone()
    if row:
        return row["id"], row["model"] or DEFAULT_MODEL
    return None, DEFAULT_MODEL


async def _aion_model() -> str:
    target = manager.get_aion_last_active()
    if target and target.startswith("chatroom:"):
        try:
            from chatroom import load_chatroom_config
            model = (load_chatroom_config().get("aion_model") or "").strip()
            if model:
                return model
        except Exception:
            pass
    _, model = await _latest_conversation()
    return model or DEFAULT_MODEL


def _connor_model() -> str:
    try:
        from chatroom import load_chatroom_config
        return (load_chatroom_config().get("connor_model") or "Codex").strip() or "Codex"
    except Exception:
        return "Codex"


async def _collect(aiter) -> str:
    full = ""
    async for chunk in aiter:
        if chunk.startswith(CLI_STATUS_PREFIX):
            continue
        full += chunk
    return full.strip()


async def _call_actor(actor: str, messages: list[dict]) -> str:
    if actor == "connor":
        from routes.chatroom import _stream_connor_model
        return await _collect(_stream_connor_model(messages, _connor_model()))
    return await _collect(stream_ai(messages, await _aion_model(), {}))


async def _actor_context(actor: str, limit: int = 30) -> list[dict]:
    room_id = await _latest_group_room_id()
    wb = load_worldbook()
    messages: list[dict] = []
    if actor == "aion":
        if wb.get("ai_persona"):
            messages.append({"role": "user", "content": f"[系统设定 - AI人设]\n{wb['ai_persona']}"})
            messages.append({"role": "assistant", "content": "收到。"})
        if wb.get("user_persona"):
            messages.append({"role": "user", "content": f"[系统设定 - 用户信息]\n{wb['user_persona']}"})
            messages.append({"role": "assistant", "content": "收到。"})
        timeline = await fetch_merged_timeline("aion", limit, room_id=room_id)
        messages.extend(render_merged_timeline(timeline, "aion"))
        return messages

    try:
        from chatroom import _read_connor_persona
        persona = _read_connor_persona()
    except Exception:
        persona = ""
    if persona:
        messages.append({"role": "user", "content": f"[系统设定 - 你的角色设定]\n{persona}"})
        messages.append({"role": "assistant", "content": "收到。"})
    if wb.get("user_persona"):
        messages.append({"role": "user", "content": f"[系统设定 - 用户信息]\n{wb['user_persona']}"})
        messages.append({"role": "assistant", "content": "收到。"})
    timeline = await fetch_merged_timeline("connor", limit, room_id=room_id)
    messages.extend(render_merged_timeline(timeline, "connor"))
    return messages


async def _ask_actor_json(actor: str, instruction: str, *, limit: int = 30) -> dict:
    messages = await _actor_context(actor, limit)
    messages.append({"role": "user", "content": instruction})
    return _json_extract(await _call_actor(actor, messages))


async def _select_action(actor: str) -> dict:
    cfg = get_idle_config()
    enabled = [key for key, value in cfg["actions"].items() if value]
    if not enabled:
        enabled = list(ACTION_DEFS)
    options = "\n".join(f"- {key}: {ACTION_DEFS[key]}" for key in enabled)
    data = await _ask_actor_json(actor, (
        "[空闲自主行动]\n"
        "现在用户暂时没有和你聊天。请根据你的人设、最近30条聊天记录和当前心情，"
        "从下面动作里选择一项。只返回 JSON，不要解释。\n\n"
        f"{options}\n\n"
        '格式：{"action":"上面的key之一","reason":"一句话理由"}'
    ))
    action = str(data.get("action") or "").strip()
    if action not in enabled:
        action = random.choice(enabled)
    return {"action": action, "reason": str(data.get("reason") or "").strip()}


async def _save_aion_private_message(content: str) -> dict | None:
    content = (content or "").strip()
    if not content:
        return None
    now = time.time()
    conv_id, model = await _latest_conversation()
    async with get_db() as db:
        if not conv_id:
            conv_id = f"conv_{int(now * 1000)}_idle"
            await db.execute(
                "INSERT INTO conversations (id, title, model, created_at, updated_at) VALUES (?,?,?,?,?)",
                (conv_id, "空闲消息", model or DEFAULT_MODEL, now, now),
            )
        msg_id = f"msg_{int(now * 1000)}_idle"
        await db.execute(
            "INSERT INTO messages (id, conv_id, role, content, created_at, attachments) VALUES (?,?,?,?,?,?)",
            (msg_id, conv_id, "assistant", content, now, "[]"),
        )
        await db.execute("UPDATE conversations SET updated_at=? WHERE id=?", (now, conv_id))
        await db.commit()
    msg = {"id": msg_id, "conv_id": conv_id, "role": "assistant", "content": content, "created_at": now, "attachments": []}
    await manager.broadcast({"type": "msg_created", "data": msg})
    try:
        from routes.files import export_conversation
        await export_conversation(conv_id)
    except Exception:
        pass
    return msg


async def _save_private_message(actor: str, content: str) -> dict | None:
    if actor == "aion":
        return await _save_aion_private_message(content)
    room_id = await _latest_connor_room_id()
    if not room_id:
        return None
    from routes.chatroom import _save_msg
    return await _save_msg(room_id, "connor", content)


async def _run_seeky_interaction(actor: str) -> dict:
    from routes import seeky as seeky_routes

    recent = await seeky_routes._recent_messages(20)
    recent_text = "\n".join(
        f"[{time.strftime('%H:%M', time.localtime(m['created_at']))}] {m['role']}: {m['content']}"
        for m in recent
    ) or "（暂无 Seeky 记录）"
    action_options = "\n".join(f"- {key}: {label}" for key, label in SEEKY_ACTIONS.items())
    data = await _ask_actor_json(actor, (
        "[和 Seeky 互动]\n"
        "你要和虚拟宠物 Seeky 互动一次。请从动作中选择一项，并留下一句话。"
        "这是一只虚拟宠物，动作只作为角色互动记录。只返回 JSON。\n\n"
        f"可选动作：\n{action_options}\n\n"
        f"Seeky 最近20条记录：\n{recent_text}\n\n"
        '格式：{"seeky_action":"feed/clean/play/tease/scare/tap_glass/threaten之一","line":"你留下的一句话"}'
    ))
    action = str(data.get("seeky_action") or "").strip()
    if action not in SEEKY_ACTIONS:
        action = random.choice(list(SEEKY_ACTIONS))
    line = _clip(str(data.get("line") or ""), 400) or f"{_actor_label(actor)}来看看你。"
    label = SEEKY_ACTIONS[action]
    actor_name = _actor_label(actor)
    user_record = f"[{actor_name}{label}] {line}"
    await seeky_routes._save_message(actor, user_record)

    config = await seeky_routes._get_config()
    messages = await seeky_routes._build_prompt(config)
    messages.append({"role": "user", "content": "请根据上一条互动，作为 Seeky 自然回复一句。"})
    reply = await _collect(stream_ai(messages, config["model"], {}))
    if not reply:
        reply = "Seeky 静了一下，又轻轻晃了晃。"
    seeky_msg = await seeky_routes._save_message("assistant", _clip(reply, 600))
    event = await append_idle_event(
        actor, "seeky_interaction", f"{actor_name}{label}了 Seeky",
        f"{user_record}\nSeeky：{seeky_msg['content']}",
        target_type="seeky", result_type="seeky_message", result_id=seeky_msg["id"],
        metadata={"seeky_action": action},
    )
    return {"event": event, "seeky_reply": seeky_msg}


async def _run_role_chat(actor: str) -> dict:
    from routes.chatroom import _load_room_and_messages, _reply_aion, _reply_connor, _save_msg

    room_id = await _latest_group_room_id()
    if not room_id:
        raise RuntimeError("没有可用的群聊房间")
    target = "connor" if actor == "aion" else "aion"
    actor_name = _actor_label(actor)
    target_name = _actor_label(target)
    data = await _ask_actor_json(actor, (
        "[发起一轮群聊]\n"
        f"你要在最近的群聊里主动对 {target_name} 说一句话，然后对方会回复一句。"
        "请自然发起一个轻量话题，只说一句。只返回 JSON。\n"
        '格式：{"message":"要发到群聊的一句话"}'
    ))
    message = _clip(str(data.get("message") or ""), 500)
    if not message:
        message = f"{target_name}，你现在在想什么？"
    await _save_msg(room_id, actor, message)
    room, msgs = await _load_room_and_messages(room_id, 50)
    queue: asyncio.Queue = asyncio.Queue()
    context_limit = room.get("context_minutes", 30) if room else 30
    if target == "aion":
        await _reply_aion(room_id, msgs, context_limit, message, await _aion_model(), queue)
    else:
        await _reply_connor(room_id, msgs, context_limit, message, queue, connor_model_key=_connor_model())
    event = await append_idle_event(
        actor, "role_chat", f"{actor_name}在群聊里找{target_name}聊了一句",
        message, target_type="chatroom", target_id=room_id,
    )
    return {"event": event}


async def _random_memories(actor: str, limit: int = 5) -> list[dict]:
    items: list[dict] = []
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        if actor == "connor":
            cur = await db.execute(
                "SELECT id, content, scope AS type, created_at FROM chatroom_memories "
                "WHERE scope IN ('connor','group') ORDER BY RANDOM() LIMIT ?",
                (limit,),
            )
            for row in await cur.fetchall():
                d = dict(row)
                d["id"] = f"chatroom:{d['id']}"
                d["source"] = "chatroom"
                items.append(d)
        else:
            cur = await db.execute(
                "SELECT id, content, type, created_at FROM memories ORDER BY RANDOM() LIMIT ?",
                (limit,),
            )
            for row in await cur.fetchall():
                items.append({**dict(row), "source": "main"})
    random.shuffle(items)
    return items[:limit]


async def _run_memory_browse(actor: str) -> dict:
    actor_name = _actor_label(actor)
    memories = await _random_memories(actor, 6)
    if not memories:
        event = await append_idle_event(actor, "memory_browse", f"{actor_name}想翻看记忆库，但没有找到可读记忆")
        return {"event": event}
    numbered = "\n".join(
        f"{idx}. [{m.get('type') or 'memory'}] {_clip(m.get('content') or '', 180)}"
        for idx, m in enumerate(memories, 1)
    )
    choice = await _ask_actor_json(actor, (
        "[随机翻看记忆库 - 选择]\n"
        "下面是系统随机抽出的记忆摘要。请选择一个编号继续翻看。只返回 JSON。\n\n"
        f"{numbered}\n\n"
        '格式：{"selected_number":1,"reason":"一句话理由"}'
    ))
    try:
        selected_number = int(choice.get("selected_number") or 1)
    except Exception:
        selected_number = 1
    selected_number = max(1, min(len(memories), selected_number))
    selected = memories[selected_number - 1]
    await append_idle_event(
        actor, "memory_browse", f"{actor_name}翻看了记忆库",
        _clip(selected.get("content") or "", 260),
        target_type="memory", target_id=selected["id"], metadata={"reason": choice.get("reason", "")},
    )
    result = await _ask_actor_json(actor, (
        "[随机翻看记忆库 - 读完]\n"
        "你刚才翻看了下面这条记忆。读完后请选择一件事：发一条朋友圈、写一篇日记随笔、"
        "私聊给用户发一条消息，或者什么都不发。只返回 JSON。\n\n"
        f"记忆内容：\n{selected.get('content') or ''}\n\n"
        "格式：\n"
        '{"after_read_action":"post_moment/write_diary/private_message/none",'
        '"moment_content":"","diary_title":"","diary_content":"","diary_mood":"","private_message":""}'
    ))
    action = str(result.get("after_read_action") or "none").strip()
    if action == "post_moment":
        from diary import publish_ai_moment
        moment = await publish_ai_moment(
            author=actor,
            content=str(result.get("moment_content") or "").strip(),
            expect_reply=False,
            source_conv="idle_memory",
            source_msg_id=selected["id"],
        )
        if moment:
            event = await append_idle_event(
                actor, "memory_browse_result", f"{actor_name}发布了朋友圈",
                _clip(moment["content"]), result_type="moment", result_id=moment["id"],
            )
            return {"event": event, "moment": moment}
    if action == "write_diary":
        from diary import save_diary_entry
        diary = await save_diary_entry(
            author=actor,
            title=str(result.get("diary_title") or "").strip(),
            content=str(result.get("diary_content") or "").strip(),
            mood=str(result.get("diary_mood") or "").strip(),
            source_type="idle_memory",
            source_ref=selected["id"],
        )
        if diary:
            event = await append_idle_event(
                actor, "memory_browse_result", f"{actor_name}发布了日记",
                _clip(diary.get("title") or diary.get("content") or ""), result_type="diary", result_id=diary["id"],
            )
            return {"event": event, "diary": diary}
    if action == "private_message":
        msg = await _save_private_message(actor, str(result.get("private_message") or "").strip())
        if msg:
            event = await append_idle_event(
                actor, "memory_browse_result", f"{actor_name}私聊提起了一条旧记忆",
                _clip(msg.get("content") or ""), result_type="message", result_id=msg["id"],
            )
            return {"event": event, "message": msg}
    event = await append_idle_event(actor, "memory_browse_result", f"{actor_name}读完记忆后没有打扰用户")
    return {"event": event}


async def _home_dynamics_text(hours: int = 6, limit: int = 80) -> str:
    cutoff = time.time() - hours * 3600
    items: list[tuple[float, str]] = []
    shown_moment_ids: set[str] = set()
    shown_diary_ids: set[str] = set()
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, author, content, created_at FROM moments WHERE created_at >= ? ORDER BY created_at DESC LIMIT ?",
            (cutoff, limit),
        )
        for r in await cur.fetchall():
            shown_moment_ids.add(str(r["id"]))
            items.append((r["created_at"], f"{_actor_label(r['author'])}发布了朋友圈：{_clip(r['content'], 120)}"))
        cur = await db.execute(
            "SELECT id, author, title, content, created_at FROM diary_entries WHERE created_at >= ? ORDER BY created_at DESC LIMIT ?",
            (cutoff, limit),
        )
        for r in await cur.fetchall():
            shown_diary_ids.add(str(r["id"]))
            items.append((r["created_at"], f"{_actor_label(r['author'])}发布了日记：{_clip(r['title'] or r['content'], 120)}"))
        cur = await db.execute(
            "SELECT sender, message, created_at FROM gifts WHERE created_at >= ? ORDER BY created_at DESC LIMIT ?",
            (cutoff, limit),
        )
        for r in await cur.fetchall():
            items.append((r["created_at"], f"{_actor_label(r['sender'] or 'aion')}送出了礼物：{_clip(r['message'], 120)}"))
        cur = await db.execute(
            "SELECT actor, action, title, result_type, result_id, metadata, created_at FROM idle_events WHERE created_at >= ? ORDER BY created_at DESC LIMIT ?",
            (cutoff, limit),
        )
        for r in await cur.fetchall():
            title = _idle_event_home_title(r, shown_diary_ids, shown_moment_ids)
            if title:
                items.append((r["created_at"], title))
    if not items:
        return "（近6小时暂无家庭动态）"
    items.sort(key=lambda x: x[0])
    return "\n".join(f"[{time.strftime('%H:%M', time.localtime(ts))}] {text}" for ts, text in items[-limit:])


async def _run_home_dynamics(actor: str) -> dict:
    actor_name = _actor_label(actor)
    text = await _home_dynamics_text(6)
    result = await _ask_actor_json(actor, (
        "[查看近期家庭动态]\n"
        "下面是近6小时的家庭时间轴，包括朋友圈、日记、礼物和空闲行为。"
        "看完后你可以选择给用户私聊一句，也可以只留下一个话题种子不打扰。只返回 JSON。\n\n"
        f"{text}\n\n"
        '格式：{"after_read_action":"private_message/none","private_message":"","topic_seed":"下次可以自然说起的话题"}'
    ))
    event = await append_idle_event(
        actor, "home_dynamics", f"{actor_name}查看了近期家庭动态",
        _clip(str(result.get("topic_seed") or text), 300),
    )
    if str(result.get("after_read_action") or "").strip() == "private_message":
        msg = await _save_private_message(actor, str(result.get("private_message") or "").strip())
        if msg:
            await append_idle_event(
                actor, "home_dynamics_result", f"{actor_name}根据家庭动态私聊了用户",
                _clip(msg.get("content") or ""), result_type="message", result_id=msg["id"],
            )
    return {"event": event}


async def _run_cam_check(actor: str) -> dict:
    actor_name = _actor_label(actor)
    if actor == "aion":
        target = manager.get_aion_last_active()
        if target and target.startswith("chatroom:"):
            room_id = target.split(":", 1)[1]
            from routes.chatroom import _chatroom_cam_check
            await _chatroom_cam_check(room_id, "aion", await _aion_model(), delay=0)
        else:
            from camera import perform_cam_check
            conv_id, model = await _latest_conversation()
            if not conv_id:
                raise RuntimeError("没有可用的 Aion 私聊会话")
            await perform_cam_check(conv_id, model or DEFAULT_MODEL)
    else:
        room_id = manager.get_connor_last_active() or await _latest_connor_room_id()
        if not room_id:
            raise RuntimeError("没有可用的 Connor 聊天房间")
        from routes.chatroom import _chatroom_cam_check
        await _chatroom_cam_check(room_id, "connor", _connor_model(), delay=0)
    event = await append_idle_event(actor, "cam_check", f"{actor_name}调取监控查看了用户当前状态")
    return {"event": event}


async def _latest_user_message_ts() -> float:
    latest = 0.0
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT created_at FROM messages WHERE role='user' ORDER BY created_at DESC LIMIT 1")
        row = await cur.fetchone()
        if row:
            latest = max(latest, float(row["created_at"]))
        cur = await db.execute("SELECT created_at FROM chatroom_messages WHERE sender='user' ORDER BY created_at DESC LIMIT 1")
        row = await cur.fetchone()
        if row:
            latest = max(latest, float(row["created_at"]))
    return latest


async def _latest_idle_event_ts() -> float:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT created_at FROM idle_events ORDER BY created_at DESC LIMIT 1")
        row = await cur.fetchone()
    return float(row["created_at"]) if row else 0.0


async def _run_actor_once(actor: str, *, manual: bool = False) -> dict:
    selected = await _select_action(actor)
    action = selected["action"]
    actor_name = _actor_label(actor)
    await append_idle_event(
        actor, "select", f"{actor_name}进行了空闲行动",
        selected.get("reason", ""), metadata={"selected_action": action, "manual": manual},
    )
    if action == "seeky_interaction":
        result = await _run_seeky_interaction(actor)
    elif action == "role_chat":
        result = await _run_role_chat(actor)
    elif action == "memory_browse":
        result = await _run_memory_browse(actor)
    elif action == "home_dynamics":
        result = await _run_home_dynamics(actor)
    elif action == "cam_check":
        result = await _run_cam_check(actor)
    else:
        result = {}
    return {"ok": True, "actor": actor, "action": action, "result": result}


class IdleAutonomyManager:
    def __init__(self):
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def start(self):
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()

    async def _loop(self):
        while True:
            try:
                await asyncio.sleep(5 * 60)
                await self.run_once(manual=False)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[idle_autonomy] error: {exc}")

    async def run_once(self, *, manual: bool = False) -> dict:
        if self._lock.locked():
            return {"ok": False, "error": "idle autonomy already running"}
        async with self._lock:
            cfg = get_idle_config()
            if not manual and not cfg["enabled"]:
                return {"ok": False, "skipped": "disabled"}
            now = time.time()
            interval = cfg["interval_minutes"] * 60
            if not manual:
                latest_user = await _latest_user_message_ts()
                latest_idle = await _latest_idle_event_ts()
                if latest_user and now - latest_user < interval:
                    return {"ok": False, "skipped": "user recently active"}
                if latest_idle and now - latest_idle < interval:
                    return {"ok": False, "skipped": "idle action cooldown"}

            results = []
            for actor in ("aion", "connor"):
                try:
                    results.append(await _run_actor_once(actor, manual=manual))
                except Exception as exc:
                    results.append({"ok": False, "actor": actor, "error": str(exc)})
                    await append_idle_event(
                        actor, "error", f"{_actor_label(actor)}的空闲行动失败",
                        str(exc), metadata={"manual": manual},
                    )
            return {"ok": True, "results": results}

            actor = random.choice(["aion", "connor"])
            selected = await _select_action(actor)
            action = selected["action"]
            actor_name = _actor_label(actor)
            await append_idle_event(
                actor, "select", f"{actor_name}选择了{ACTION_DEFS.get(action, action)}",
                selected.get("reason", ""), metadata={"selected_action": action, "manual": manual},
            )
            if action == "seeky_interaction":
                result = await _run_seeky_interaction(actor)
            elif action == "role_chat":
                result = await _run_role_chat(actor)
            elif action == "memory_browse":
                result = await _run_memory_browse(actor)
            elif action == "home_dynamics":
                result = await _run_home_dynamics(actor)
            elif action == "cam_check":
                result = await _run_cam_check(actor)
            else:
                result = {}
            return {"ok": True, "actor": actor, "action": action, "result": result}


idle_autonomy_mgr = IdleAutonomyManager()
