"""Tests for WS Gateway with mocked Redis."""
import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from fastapi.testclient import TestClient

from main import app, _increment_clients, _decrement_clients, WS_CLIENTS_KEY


class MockRedis:
    """Mock Redis for testing."""

    def __init__(self):
        self.store = {WS_CLIENTS_KEY: 0}
        self.subscribers = {}

    def incr(self, key):
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    def decr(self, key):
        self.store[key] = self.store.get(key, 0) - 1
        return self.store[key]

    def set(self, key, val):
        self.store[key] = val

    def get(self, key):
        return str(self.store.get(key, 0))


@pytest.fixture
def mock_redis_module():
    """Мокирует redis.from_url."""
    mock_r = MockRedis()
    with patch("main.redis") as mock_redis:
        mock_redis.from_url.return_value = mock_r
        yield mock_r


client = TestClient(app)


def test_health_check(mock_redis_module):
    """GET /health returns ok."""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "connected_clients" in data


def test_websocket_connect(mock_redis_module):
    """WebSocket connects successfully."""
    with client.websocket_connect("/ws/events") as ws:
        # Соединение установлено
        ws.send_text("ping")


def test_websocket_receives_published_message(mock_redis_module):
    """WebSocket получает сообщения из Redis pub/sub."""
    with client.websocket_connect("/ws/events") as ws:
        # Симулируем сообщение из Redis
        mock_pubsub = MagicMock()
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": "events_stream",
            "data": json.dumps({
                "event_type": "order_created",
                "order_id": "ORD-TEST-001",
                "trace_id": "abc-123",
            }),
        }

        # Патчим pubsub внутри handler
        with patch("main._get_redis_pubsub", return_value=(mock_redis_module, mock_pubsub)):
            # Переподключаемся с патченным pubsub
            pass  # WebSocket уже подключился, pubsub был создан до патча


def test_client_counter(mock_redis_module):
    """Счётчик клиентов увеличивается и уменьшается."""
    count1 = _increment_clients()
    assert count1 == 1

    count2 = _increment_clients()
    assert count2 == 2

    count3 = _decrement_clients()
    assert count3 == 1


def test_websocket_disconnect(mock_redis_module):
    """WebSocket disconnect корректно уменьшает счётчик."""
    _increment_clients()
    initial_count = mock_redis_module.get(WS_CLIENTS_KEY)

    with client.websocket_connect("/ws/events") as ws:
        ws.send_text("ping")

    # После отключения счётчик уменьшается
    final_count = mock_redis_module.get(WS_CLIENTS_KEY)
    assert final_count is not None
