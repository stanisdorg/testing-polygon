"""WS Gateway — WebSocket сервис для realtime рассылки событий через Redis Pub/Sub.

При подключении клиента:
- Подписывается на канал Redis "events_stream".
- При получении сообщения от Redis -> отправляет клиенту через ws.send().
- При отключении клиента -> отписывается и обновляет счётчик.
"""
import json
import os
import asyncio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
WS_CLIENTS_KEY = "ws_clients_count"

app = FastAPI(title="FulfilBox — WS Gateway")


def _get_redis_pubsub():
    """Создаёт Redis pubsub клиент."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        pubsub = r.pubsub()
        return r, pubsub
    except Exception:
        return None, None


def _increment_clients():
    """Увеличивает счётчик подключённых клиентов."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        return r.incr(WS_CLIENTS_KEY)
    except Exception:
        return -1


def _decrement_clients():
    """Уменьшает счётчик подключённых клиентов."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        val = r.decr(WS_CLIENTS_KEY)
        if val < 0:
            r.set(WS_CLIENTS_KEY, 0)
        return val
    except Exception:
        return -1


@app.websocket("/ws/events")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket endpoint: подписка на Redis Pub/Sub и рассылка клиентам."""
    await ws.accept()
    count = _increment_clients()
    print(f"  🔌 WebSocket client connected (total: {count})")

    r, pubsub = _get_redis_pubsub()
    if pubsub:
        pubsub.subscribe("events_stream")
    else:
        await ws.send_json({"error": "Redis connection failed"})

    try:
        while True:
            if pubsub:
                # Запускаем listen в отдельном потоке чтобы не блокировать
                message = await asyncio.to_thread(pubsub.get_message, ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        await ws.send_json(data)
                    except Exception:
                        pass
            else:
                # Redis недоступен — держим соединение
                await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        if pubsub:
            pubsub.unsubscribe("events_stream")
            pubsub.close()
        count = _decrement_clients()
        print(f"  🔌 WebSocket client disconnected (total: {count})")


@app.get("/health")
async def health():
    """Health check."""
    count = -1
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        val = r.get(WS_CLIENTS_KEY)
        count = int(val) if val else 0
    except Exception:
        pass
    return {"status": "ok", "connected_clients": count}
