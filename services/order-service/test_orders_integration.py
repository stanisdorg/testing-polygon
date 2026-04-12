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


def test_create_order_persisted_to_db():
    """Integration: заказ сохраняется в PostgreSQL"""
    order_id = f"ORD-INT-{uuid.uuid4().hex[:8]}"

    response = client.post(
        "/api/v1/orders",
        json={
            "order_id": order_id,
            "items": [{"sku": "SKU-100", "qty": 3}],
            "delivery_address": "Москва, ул. Тестовая 1",
        },
    )
    assert response.status_code == 201

    # Проверяем что заказ реально есть в PostgreSQL
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT order_id, items, delivery_address FROM orders WHERE order_id = %s", (order_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    assert row is not None, "Заказ не найден в базе данных"
    assert row[0] == order_id
