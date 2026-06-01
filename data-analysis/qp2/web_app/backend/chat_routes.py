import asyncio
import json
import os
import time
import uuid
import socket

import redis
from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from qp2.web_app.backend.security import verify_token
from qp2.web_app.backend.auth import is_staff_member
from qp2.web_app.backend.email_utils import send_mail
from qp2.db import ExperimentForm
from qp2.config.servers import ServerConfig
from qp2.xio.db_manager import get_beamline_from_hostname, DBManager
from qp2.xio.user_group_manager import UserGroupManager
from qp2.image_viewer.ai.assistant import AIClient
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# --- Redis setup ---
_redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "127.0.0.1")
_redis_client = redis.Redis(host=_redis_host, port=6379, decode_responses=True)
_default_room = get_beamline_from_hostname()
_DEFAULT_HOST_EMAIL = os.environ.get("QP2_DEFAULT_HOST_EMAIL", "gmcahosts@anl.gov")

router = APIRouter(prefix="/chat", tags=["chat"])


def _validate_room(room: str, user: str):
    """Staff can access any room. Regular users restricted to their beamline."""
    from qp2.web_app.backend.auth import is_staff_member
    if is_staff_member(user):
        return
    # User's beamline is embedded in the default room detection
    # Allow access to the default room (auto-detected from hostname)
    if room != _default_room:
        try:
            from qp2.xio.user_group_manager import UserGroupManager
            ugm = UserGroupManager()
            info = ugm.latest_group_info_from_username(user)
            user_beamline = info.get("beamline") if info else None
            if user_beamline and room == user_beamline:
                return
        except Exception:
            pass
        raise HTTPException(status_code=403, detail="Access denied to this chat room")


def _keys(room: str):
    return {
        "history": f"ai_assistant:chat:{room}",
        "channel": f"ai_assistant:chat_channel:{room}",
        "presence": f"ai_assistant:presence:{room}",
    }


# --- Models ---
class SendMessage(BaseModel):
    content: str


class AskAIRequest(BaseModel):
    content: str


# --- Endpoints ---

@router.get("/history")
def get_history(room: str = Query(default=_default_room), user: str = Depends(verify_token)):
    _validate_room(room, user)
    keys = _keys(room)
    # Track presence — per-user key with 2-minute TTL
    _redis_client.setex(f"{keys['presence']}:{user}", 120, "1")
    raw = _redis_client.lrange(keys["history"], -50, -1)
    messages = []
    for item in raw:
        try:
            messages.append(json.loads(item))
        except json.JSONDecodeError:
            continue
    return {"messages": messages, "room": room}


def _evaluate_and_send_notification(room: str, msg_content: str, sender: str):
    """Sends an email to the host if no staff is present or @host is mentioned."""
    if is_staff_member(sender):
        return  # Don't notify hosts about other staff messages

    is_mention = "@host" in msg_content.lower() or "@staff" in msg_content.lower()

    # Check if staff is in the room
    prefix = f"{_keys(room)['presence']}:"
    user_keys = list(_redis_client.scan_iter(f"{prefix}*"))
    active_users = [k.replace(prefix, "") for k in user_keys]
    staff_present = any(is_staff_member(u) for u in active_users)

    if staff_present and not is_mention:
        return  # Staff is here to see it, and no explicit mention

    # Spam prevention: 5-minute cooldown per room unless explicit mention
    cooldown_key = f"chat_email_cooldown:{room}"
    if _redis_client.get(cooldown_key) and not is_mention:
        return

    # Find the host email(s)
    emails = []
    try:
        ugm = UserGroupManager()
        group_info = ugm.latest_group_info_from_username(sender, room)
        if group_info and group_info.get("esaf_id"):
            esaf_id = group_info["esaf_id"]
            db = DBManager()
            with db.get_session() as session:
                form = session.query(ExperimentForm).filter_by(esaf_id=str(esaf_id)).first()
                if form and form.hosts:
                    for h in form.hosts:
                        if h.staff and h.staff.email:
                            emails.append(h.staff.email)
    except Exception as e:
        logger.error(f"Failed to fetch host for notification: {e}")

    if not emails:
        emails = [_DEFAULT_HOST_EMAIL]

    subject = f"[{room.upper()}] Chat message from {sender}"
    body = f"User '{sender}' sent a message in the {room} AI Chat Portal:\n\n{msg_content}\n\n" \
           f"Portal Link: https://{socket.gethostname()}/data_portal"

    try:
        send_mail(subject, body, emails)
        _redis_client.setex(cooldown_key, 300, "1")
    except Exception as e:
        logger.error(f"Failed to send host chat notification: {e}")


@router.post("/send")
def send_message(msg: SendMessage, room: str = Query(default=_default_room), user: str = Depends(verify_token)):
    _validate_room(room, user)
    keys = _keys(room)
    _redis_client.setex(f"{keys['presence']}:{user}", 120, "1")
    msg_data = {
        "role": "user",
        "content": msg.content,
        "user": user,
        "timestamp": time.time(),
        "msg_id": str(uuid.uuid4()),
    }
    payload = json.dumps(msg_data)
    pipe = _redis_client.pipeline()
    pipe.rpush(keys["history"], payload)
    pipe.ltrim(keys["history"], -100, -1)
    pipe.expire(keys["history"], 7 * 24 * 3600)
    pipe.execute()
    _redis_client.publish(keys["channel"], payload)
    
    # Evaluate notification
    _evaluate_and_send_notification(room, msg.content, user)
    
    return {"status": "ok", "msg_id": msg_data["msg_id"]}


@router.post("/ask_ai")
def ask_ai(req: AskAIRequest, room: str = Query(default=_default_room), user: str = Depends(verify_token)):
    _validate_room(room, user)
    keys = _keys(room)

    # 1. Save user message
    user_msg = {
        "role": "user",
        "content": req.content,
        "user": user,
        "timestamp": time.time(),
        "msg_id": str(uuid.uuid4()),
    }
    user_payload = json.dumps(user_msg)
    pipe = _redis_client.pipeline()
    pipe.rpush(keys["history"], user_payload)
    pipe.ltrim(keys["history"], -100, -1)
    pipe.execute()
    _redis_client.publish(keys["channel"], user_payload)

    # Evaluate notification for AI query
    _evaluate_and_send_notification(room, req.content, user)

    # 2. Build context from recent history
    raw = _redis_client.lrange(keys["history"], -20, -1)
    context = []
    for item in raw:
        try:
            m = json.loads(item)
            if m.get("role") in ("user", "assistant"):
                context.append({"role": m["role"], "content": m["content"]})
        except (json.JSONDecodeError, KeyError):
            continue

    # 3. Call AI — use logged-in username as API key (not OS user which may be dmadmin)
    try:
        ai_user = user or os.environ.get("AI_API_KEY", "qxu")
        client = AIClient(api_key=ai_user)
        response_text = client.generate_code(context)
    except Exception as e:
        logger.error(f"AI generation failed: {e}")
        raise HTTPException(status_code=500, detail="AI generation failed")

    # 4. Save and publish AI response
    ai_msg = {
        "role": "assistant",
        "content": response_text,
        "user": "AI",
        "timestamp": time.time(),
        "msg_id": str(uuid.uuid4()),
    }
    ai_payload = json.dumps(ai_msg)
    pipe = _redis_client.pipeline()
    pipe.rpush(keys["history"], ai_payload)
    pipe.ltrim(keys["history"], -100, -1)
    pipe.execute()
    _redis_client.publish(keys["channel"], ai_payload)

    return {"status": "ok", "response": response_text, "msg_id": ai_msg["msg_id"]}


@router.get("/stream")
async def stream(request: Request, room: str = Query(default=_default_room)):
    """SSE endpoint. Auth via HttpOnly cookie (or query param for backward compat)."""
    import jwt as _jwt
    from qp2.web_app.backend.security import COOKIE_NAME, SECRET_KEY, ALGORITHM

    token = request.cookies.get(COOKIE_NAME)
    if not token:
        # Fallback: accept token query param for backward compat during migration
        token = request.query_params.get("token")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = _jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user = payload.get("sub")
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
    except _jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    _validate_room(room, user)

    keys = _keys(room)

    loop = asyncio.get_running_loop()

    async def event_generator():
        pubsub = _redis_client.pubsub()
        pubsub.subscribe(keys["channel"])
        try:
            while True:
                msg = await loop.run_in_executor(
                    None, lambda: pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                )
                if msg and msg["type"] == "message":
                    yield f"data: {msg['data']}\n\n"
                else:
                    await asyncio.sleep(0.1)
        finally:
            pubsub.unsubscribe()
            pubsub.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/users")
def get_active_users(room: str = Query(default=_default_room), user: str = Depends(verify_token)):
    keys = _keys(room)
    # Refresh own presence so idling users don't drop off the active list
    _redis_client.setex(f"{keys['presence']}:{user}", 120, "1")
    
    prefix = f"{keys['presence']}:"
    user_keys = list(_redis_client.scan_iter(f"{prefix}*"))
    users = [k.replace(prefix, "") for k in user_keys]
    return {"users": users, "room": room}


@router.get("/info")
def get_chat_info():
    """Return metadata about the chat environment, like the active LLM."""
    try:
        from qp2.image_viewer.ai.assistant import AIClient
        client = AIClient()
        return {"model": client.model_name}
    except Exception:
        return {"model": "unknown"}

@router.get("/rooms")
def get_available_rooms():
    """Return available chat rooms (standard ones + any active in Redis)."""
    try:
        rooms = {"23i", "23o"}
        prefix = "ai_assistant:chat:"
        for k in _redis_client.scan_iter(f"{prefix}*"):
            rooms.add(k.replace(prefix, ""))
        return {"rooms": sorted(list(rooms))}
    except Exception:
        return {"rooms": ["23i", "23o"]}

@router.post("/archive")
def trigger_manual_archive(room: str = Query(default=None), user: str = Depends(verify_token)):
    """Manually trigger the chat archival process (staff only)."""
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Only staff can trigger archive")
    try:
        from qp2.xio.db_manager import DBManager
        db = DBManager()
        with db.get_session() as session:
            archive_daily_chat_messages(session, room_target=room)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Manual archive failed: {e}")
        raise HTTPException(status_code=500, detail="Archive failed")

def archive_daily_chat_messages(db_session, room_target=None):
    """APScheduler background job to move Redis chat messages to database."""
    from datetime import datetime
    from qp2.db.models.misc import ChatArchiveMessage
    import json

    prefix = "ai_assistant:chat:"
    if room_target:
        # If targeting a specific room, only get that room's key
        key = f"{prefix}{room_target}"
        room_keys = [key] if _redis_client.exists(key) else []
    else:
        room_keys = [k for k in _redis_client.scan_iter(f"{prefix}*")
                     if not k.endswith(":presence") and "chat_email_cooldown" not in k]

    count = 0
    keys_to_delete = []
    for key in room_keys:
        room = key.replace(prefix, "")
        raw_msgs = _redis_client.lrange(key, 0, -1)
        if not raw_msgs:
            continue

        for item in raw_msgs:
            try:
                m = json.loads(item)
                ts = datetime.fromtimestamp(m.get("timestamp", time.time()))
                msg_id = m.get("msg_id", str(uuid.uuid4()))
                role = m.get("role", "unknown")
                content = m.get("content", "")
                username = m.get("user", "unknown")

                archive_msg = ChatArchiveMessage(
                    room=room,
                    msg_id=msg_id,
                    role=role,
                    content=content,
                    username=username,
                    timestamp=ts
                )
                db_session.add(archive_msg)
                count += 1
            except Exception as e:
                logger.error(f"Failed to archive message in {room}: {e}")

        keys_to_delete.append(key)

    if count > 0:
        db_session.commit()
        for key in keys_to_delete:
            _redis_client.delete(key)
        logger.info(f"Archived {count} chat messages to database.")
