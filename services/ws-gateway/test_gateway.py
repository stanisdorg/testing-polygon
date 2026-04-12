import asyncio
import json
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_websocket_receives_published_event():
    """Integration: клиент получает сообщение через POST /publish"""
    with client.websocket_connect("/ws") as ws:
        # Отправляем событие через HTTP POST
        response = client.post(
            "/publish",
            json={
                "type": "order_update",
                "order_id": "SIM-99999",
                "status": "reserved",
            },
        )
        assert response.status_code == 200

        # Клиент должен получить сообщение
        data = ws.receive_json()
        assert data["type"] == "order_update"
        assert data["order_id"] == "SIM-99999"
        assert data["status"] == "reserved"


def test_websocket_multiple_clients():
    """Integration: несколько клиентов получают одно и то же сообщение"""
    with client.websocket_connect("/ws") as ws1:
        with client.websocket_connect("/ws") as ws2:
            response = client.post(
                "/publish",
                json={
                    "type": "order_update",
                    "order_id": "SIM-88888",
                    "status": "failed",
                },
            )
            assert response.status_code == 200

            # Оба клиента получают сообщение
            data1 = ws1.receive_json()
            data2 = ws2.receive_json()

            assert data1["order_id"] == "SIM-88888"
            assert data1["status"] == "failed"
            assert data2["order_id"] == "SIM-88888"
            assert data2["status"] == "failed"
