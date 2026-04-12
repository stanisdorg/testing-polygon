import json
import os
import psycopg2
from consumer import run_once

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


def get_db():
    return psycopg2.connect(DB_URL)


def _insert_event(event_type, order_id, payload):
    """Вставляет тестовое событие в БД."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (event_type, order_id, payload, processed) VALUES (%s, %s, %s, false)",
        (event_type, order_id, json.dumps(payload)),
    )
    conn.commit()
    cur.close()
    conn.close()


def _get_event(order_id):
    """Возвращает событие из БД."""
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


def test_process_order_created_event():
    """Integration: consumer обрабатывает order_created событие"""
    _insert_event(
        "order_created",
        "ORD-CONSUMER-001",
        {"items": [{"sku": "SKU-500", "qty": 1}], "delivery_address": "Тест"},
    )

    # Один цикл обработки
    run_once()

    # Проверяем что событие обработано
    row = _get_event("ORD-CONSUMER-001")
    assert row is not None, "Событие не найдено"
    assert row[3] is True, "Поле processed не стало True"


def test_ignores_other_event_types():
    """Edge case: consumer игнорирует другие типы событий"""
    _insert_event(
        "some_unknown_event",
        "ORD-CONSUMER-002",
        {"data": "test"},
    )

    run_once()

    row = _get_event("ORD-CONSUMER-002")
    assert row is not None
    assert row[3] is False, "Неизвестное событие не должно быть обработано"


def test_does_not_reprocess_already_processed():
    """Edge case: уже обработанное событие не обрабатывается повторно"""
    # Вставляем уже обработанное событие
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (event_type, order_id, payload, processed) VALUES (%s, %s, %s, true)",
        ("order_created", "ORD-CONSUMER-003", json.dumps({"items": []})),
    )
    conn.commit()
    cur.close()
    conn.close()

    run_once()

    # processed осталось True, не двойная обработка
    row = _get_event("ORD-CONSUMER-003")
    assert row is not None
    assert row[3] is True
