"""Тесты WebSocket стриминга событий."""
import json
import os
import time
import threading

import psycopg2
import pytest

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


def get_db():
    return psycopg2.connect(DB_URL)


def _insert_event(event_type, order_id, trace_id="test-trace"):
    """Вставляет событие в БД и возвращает его id."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (event_type, order_id, payload, trace_id) VALUES (%s, %s, %s, %s) RETURNING id",
        (event_type, order_id, json.dumps({"test": True}), trace_id),
    )
    event_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return event_id


def _get_max_event_id():
    """Возвращает максимальный id события в БД."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM events")
    max_id = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_id


def test_new_event_appears_in_stream():
    """Тест 1: создаём событие → проверяем что появляется в стриме."""
    from portal import EventStreamer

    # Запоминаем текущий max id
    last_id = _get_max_event_id()

    # Создаём событие в отдельном потоке через 0.5 сек
    def insert_later():
        time.sleep(0.5)
        _insert_event("order_created", "ORD-WS-TEST-1", "trace-ws-1")

    t = threading.Thread(target=insert_later, daemon=True)
    t.start()

    # Создаём стример и читаем
    streamer = EventStreamer(DB_URL, last_id)
    events = []
    for _ in range(10):  # ждём максимум 5 сек (10 * 0.5s)
        new_events = streamer.fetch_new()
        events.extend(new_events)
        if events:
            break
        time.sleep(0.5)

    t.join(timeout=2)

    assert len(events) > 0, "Событие не появилось в стриме"
    assert events[0]["order_id"] == "ORD-WS-TEST-1"
    assert events[0]["event_type"] == "order_created"


def test_only_new_events_streamed():
    """Тест 2: стримит только НОВЫЕ события, не старые."""
    from portal import EventStreamer

    # Создаём 2 события ДО старта стримера
    _insert_event("order_created", "ORD-OLD-1", "trace-old-1")
    _insert_event("order_picked", "ORD-OLD-1", "trace-old-1")
    time.sleep(0.3)

    # Стриминг начинается с ТЕКУЩЕГО max id
    last_id = _get_max_event_id()
    streamer = EventStreamer(DB_URL, last_id)

    # Создаём ещё 1 событие ПОСЛЕ старта
    _insert_event("order_packed", "ORD-NEW-1", "trace-new-1")
    time.sleep(0.5)

    events = streamer.fetch_new()

    # Должно прийти только новое событие
    assert len(events) == 1, f"Ожидалось 1 новое событие, получено {len(events)}"
    assert events[0]["order_id"] == "ORD-NEW-1"
    assert events[0]["event_type"] == "order_packed"


def test_trace_id_present_in_stream():
    """Тест 3: trace_id присутствует в стриме."""
    from portal import EventStreamer

    trace_id = f"trace-test-{time.time()}"
    last_id = _get_max_event_id()

    # Создаём событие
    _insert_event("order_created", "ORD-TRACE-TEST", trace_id)
    time.sleep(0.5)

    streamer = EventStreamer(DB_URL, last_id)
    events = streamer.fetch_new()

    assert len(events) > 0, "Событие не появилось"
    assert "trace_id" in events[0], "trace_id отсутствует в стриме"
    assert events[0]["trace_id"] == trace_id
