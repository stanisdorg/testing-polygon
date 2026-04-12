import json
import os
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


def test_create_order_emits_event():
    """Integration: при создании заказа — событие попадает в event log"""
    resp = client.post(
        "/api/v1/orders",
        json={
            "order_id": "ORD-EVT-001",
            "items": [{"sku": "SKU-300", "qty": 2}],
            "delivery_address": "Москва, ул. Событийная 10",
        },
    )
    assert resp.status_code == 201

    # Проверяем что событие записано
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT event_type, order_id, payload FROM events WHERE order_id = %s AND event_type = 'order_created'",
        ("ORD-EVT-001",),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    assert row is not None, "Событие не найдено в таблице events"
    assert row[0] == "order_created"
    assert row[1] == "ORD-EVT-001"
    # Payload — весь event объект: {"event_type": ..., "payload": {"items": ...}, ...}
    payload = json.loads(row[2]) if isinstance(row[2], str) else row[2]
    assert payload["payload"]["items"] == [{"sku": "SKU-300", "qty": 2}]
    assert "trace_id" in payload  # trace_id теперь есть
