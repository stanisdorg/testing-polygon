import json
import os
import sys
import pytest
import psycopg2

# Добавляем path к warehouse модулю из order-service
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../order-service"))

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


def get_db():
    return psycopg2.connect(DB_URL)


def _insert_event(event_type, order_id, payload, processed=False):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (event_type, order_id, payload, processed) VALUES (%s, %s, %s, %s)",
        (event_type, order_id, json.dumps(payload), processed),
    )
    conn.commit()
    cur.close()
    conn.close()


def _get_event(order_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT event_type, order_id, payload, processed FROM events WHERE order_id = %s",
        (order_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def test_order_reserved_when_all_items_available(capsys):
    """Happy path: все товары доступны → заказ зарезервирован"""
    from consumer import process_order

    _insert_event(
        "order_created",
        "ORD-RESERVE-001",
        {"items": [{"sku": "SKU-001", "qty": 1}], "delivery_address": "Тест"},
    )

    result = process_order("ORD-RESERVE-001", [{"sku": "SKU-001", "qty": 1}])

    captured = capsys.readouterr()
    assert "Order ORD-RESERVE-001 reserved" in captured.out
    assert result is True


def test_order_failed_when_item_out_of_stock(capsys):
    """Error case: товара нет в наличии → заказ отклонён"""
    from consumer import process_order

    result = process_order("ORD-FAIL-001", [{"sku": "SKU-999", "qty": 1}])

    captured = capsys.readouterr()
    assert "Order ORD-FAIL-001 failed: out of stock" in captured.out
    assert result is False


def test_order_failed_when_insufficient_qty(capsys):
    """Edge case: товара мало → заказ отклонён"""
    from consumer import process_order

    result = process_order("ORD-FAIL-002", [{"sku": "SKU-001", "qty": 999}])

    captured = capsys.readouterr()
    assert "Order ORD-FAIL-002 failed: out of stock" in captured.out
    assert result is False
