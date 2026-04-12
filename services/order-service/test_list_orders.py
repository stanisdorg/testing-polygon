import os
import uuid
import psycopg2
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


def get_db_conn():
    return psycopg2.connect(DB_URL)


def test_list_orders_returns_created_order():
    """Integration: GET /orders возвращает созданный заказ"""
    order_id = f"ORD-LIST-{uuid.uuid4().hex[:8]}"

    # Создаём заказ
    resp_create = client.post(
        "/api/v1/orders",
        json={
            "order_id": order_id,
            "items": [{"sku": "SKU-200", "qty": 1}],
            "delivery_address": "Москва, ул. Списоковая 5",
        },
    )
    assert resp_create.status_code == 201

    # Получаем список
    resp_list = client.get("/api/v1/orders")
    assert resp_list.status_code == 200
    data = resp_list.json()
    assert "orders" in data

    # Находим наш заказ в списке
    order_ids = [o["order_id"] for o in data["orders"]]
    assert order_id in order_ids


def test_list_orders_empty():
    """Edge case: список заказов пуст (если БД пуста)"""
    # Просто проверяем что структура ответа корректна
    resp = client.get("/api/v1/orders")
    assert resp.status_code == 200
    data = resp.json()
    assert "orders" in data
    assert isinstance(data["orders"], list)
