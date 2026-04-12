"""Тесты Kafka producer в order-service."""
import json
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


@patch("main.kafka_producer")
def test_create_order_sends_kafka_event(mock_producer):
    """Integration: при создании заказа событие отправляется в Kafka"""
    mock_producer.send = MagicMock()

    response = client.post(
        "/api/v1/orders",
        json={
            "order_id": "ORD-KAFKA-001",
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Москва, ул. Кафки 1",
        },
    )
    assert response.status_code == 201

    # Проверяем что producer.send вызван
    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args

    # Правильный topic
    assert call_args[0][0] == "order_events"

    # Правильный payload
    payload = call_args[1]["value"]
    assert payload["event_type"] == "order_created"
    assert payload["order_id"] == "ORD-KAFKA-001"
    assert "items" in payload["payload"]
    assert "timestamp" in payload


@patch("main.kafka_producer")
def test_create_order_kafka_fails_still_returns_201(mock_producer):
    """Edge case: Kafka недоступна — заказ всё равно создаётся"""
    mock_producer.send = MagicMock(side_effect=Exception("Kafka down"))

    response = client.post(
        "/api/v1/orders",
        json={
            "order_id": "ORD-KAFKA-002",
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Москва, ул. Кафки 2",
        },
    )
    # Заказ создан несмотря на ошибку Kafka
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "created"
