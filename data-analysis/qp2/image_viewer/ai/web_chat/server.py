"""
Web chat gateway — bridges the desktop AI assistant chat (Redis pub/sub)
to phone browsers via SSE + REST.

Usage:
    python -m qp2.image_viewer.ai.web_chat.server
    # Then open http://<host>:8501 on your phone
"""

import asyncio
import json
import os
import time
import threading
import uuid
from typing import Optional

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

from qp2.config.servers import ServerConfig
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.image_viewer.ai.assistant import AIClient
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# --- Redis setup ---
_redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "127.0.0.1")
_redis_client = redis.Redis(host=_redis_host, port=6379, decode_responses=True)
_default_room = get_beamline_from_hostname()


def _keys(room: str):
    return {
        "history": f"ai_assistant:chat:{room}",
        "channel": f"ai_assistant:chat_channel:{room}",
        "presence": f"ai_assistant:presence:{room}",
    }


# --- FastAPI app ---
app = FastAPI(title="QP2 Chat Gateway")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Models ---
class SendMessage(BaseModel):
    content: str
    user: str
    role: str = "user"


class AskAI(BaseModel):
    content: str
    user: str


# --- Endpoints ---

@app.get("/", response_class=HTMLResponse)
def serve_chat_page():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path) as f:
        return f.read()


@app.get("/api/history")
def get_history(room: str = Query(default=_default_room)):
    keys = _keys(room)
    raw = _redis_client.lrange(keys["history"], -50, -1)
    messages = []
    for item in raw:
        try:
            messages.append(json.loads(item))
        except json.JSONDecodeError:
            continue
    return {"messages": messages, "room": room}


@app.post("/api/send")
def send_message(msg: SendMessage, room: str = Query(default=_default_room)):
    keys = _keys(room)
    msg_data = {
        "role": msg.role,
        "content": msg.content,
        "user": msg.user,
        "timestamp": time.time(),
        "msg_id": str(uuid.uuid4()),
    }
    payload = json.dumps(msg_data)
    _redis_client.rpush(keys["history"], payload)
    _redis_client.ltrim(keys["history"], -100, -1)
    _redis_client.expire(keys["history"], 7 * 24 * 3600)
    _redis_client.publish(keys["channel"], payload)
    return {"status": "ok", "msg_id": msg_data["msg_id"]}


@app.post("/api/ask_ai")
def ask_ai(req: AskAI, room: str = Query(default=_default_room)):
    keys = _keys(room)

    # 1. Save user message
    user_msg = {
        "role": "user",
        "content": req.content,
        "user": req.user,
        "timestamp": time.time(),
        "msg_id": str(uuid.uuid4()),
    }
    user_payload = json.dumps(user_msg)
    _redis_client.rpush(keys["history"], user_payload)
    _redis_client.ltrim(keys["history"], -100, -1)
    _redis_client.publish(keys["channel"], user_payload)

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

    # 3. Call AI
    try:
        client = AIClient()
        response_text = client.generate_code(context)
    except Exception as e:
        logger.error(f"AI generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"AI error: {e}")

    # 4. Save and publish AI response
    ai_msg = {
        "role": "assistant",
        "content": response_text,
        "user": "AI",
        "timestamp": time.time(),
        "msg_id": str(uuid.uuid4()),
    }
    ai_payload = json.dumps(ai_msg)
    _redis_client.rpush(keys["history"], ai_payload)
    _redis_client.ltrim(keys["history"], -100, -1)
    _redis_client.publish(keys["channel"], ai_payload)

    return {"status": "ok", "response": response_text, "msg_id": ai_msg["msg_id"]}


@app.get("/api/stream")
async def stream(room: str = Query(default=_default_room)):
    keys = _keys(room)

    async def event_generator():
        pubsub = _redis_client.pubsub()
        pubsub.subscribe(keys["channel"])
        try:
            while True:
                msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg["type"] == "message":
                    yield f"data: {msg['data']}\n\n"
                await asyncio.sleep(0.1)
        finally:
            pubsub.unsubscribe()
            pubsub.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/users")
def get_active_users(room: str = Query(default=_default_room)):
    keys = _keys(room)
    users = list(_redis_client.smembers(keys["presence"]))
    return {"users": users, "room": room}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="QP2 Web Chat Gateway")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=8501, help="Port")
    parser.add_argument("--room", default=None, help="Override room ID")
    args = parser.parse_args()

    if args.room:
        global _default_room
        _default_room = args.room

    logger.info(f"Starting web chat gateway on {args.host}:{args.port} (room: {_default_room})")
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
