"""WS Gateway — WebSocket сервис для realtime рассылки событий.

Принимает события через POST /publish и рассылает всем подключённым
WebSocket клиентам.
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

app = FastAPI(title="FulfilBox — WS Gateway")

# Хранилище подключённых клиентов
connected_clients: list[WebSocket] = []


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket endpoint для клиентов (UI)."""
    await ws.accept()
    connected_clients.append(ws)
    try:
        while True:
            # Держим соединение открытым, читаем ping'и если нужно
            await ws.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(ws)


class PublishEvent(BaseModel):
    type: str
    order_id: str
    status: str
    trace_id: str = ""


@app.post("/publish", status_code=200)
async def publish_event(event: PublishEvent):
    """HTTP endpoint для публикации событий (вызывает Event Consumer)."""
    message = event.model_dump()
    disconnected = []

    for client in connected_clients:
        try:
            await client.send_json(message)
        except Exception:
            disconnected.append(client)

    # Чистим отключившихся клиентов
    for client in disconnected:
        if client in connected_clients:
            connected_clients.remove(client)

    return {"status": "published", "clients": len(connected_clients)}
