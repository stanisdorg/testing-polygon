"""Тесты trace_id в order-service."""
import json
import uuid
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_create_order_returns_trace_id():
    """POST /orders возвращает trace_id в ответе"""
    response = client.post(
        "/api/v1/orders",
        json={
            "order_id": f"ORD-TRACE-{uuid.uuid4().hex[:8]}",
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Москва, ул. Трассировки 1",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert "trace_id" in data
    assert len(data["trace_id"]) > 0  # Не пустой
    # Валидный UUID формат
    try:
        uuid.UUID(data["trace_id"])
    except ValueError:
        assert False, f"trace_id '{data['trace_id']}' не является валидным UUID"


def test_create_order_trace_id_is_unique():
    """Каждый заказ получает уникальный trace_id"""
    resp1 = client.post(
        "/api/v1/orders",
        json={
            "order_id": f"ORD-UNIQ-{uuid.uuid4().hex[:8]}",
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Тест 1",
        },
    )
    resp2 = client.post(
        "/api/v1/orders",
        json={
            "order_id": f"ORD-UNIQ-{uuid.uuid4().hex[:8]}",
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Тест 2",
        },
    )
    assert resp1.json()["trace_id"] != resp2.json()["trace_id"]
