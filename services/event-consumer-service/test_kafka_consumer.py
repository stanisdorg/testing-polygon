"""Тесты Kafka consumer + deduplication в event-consumer."""
import json
import os
from unittest.mock import patch, MagicMock
import pytest
import redis

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")


def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)


def _dedup_key(order_id):
    return f"event:{order_id}"


def _is_deduped(order_id):
    r = get_redis()
    val = r.get(_dedup_key(order_id))
    r.close()
    return val is not None


@pytest.fixture(autouse=True)
def clean_redis():
    r = get_redis()
    r.flushdb()
    r.close()
    yield
    r = get_redis()
    r.flushdb()
    r.close()


@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_kafka_consumer_processes_order(mock_kafka, mock_process):
    """Happy path: Kafka consumer получает событие и вызывает process_order"""
    from consumer import run_kafka_once

    mock_kafka.poll.return_value = {
        None: [
            MagicMock(
                value={
                    "event_type": "order_created",
                    "order_id": "ORD-KAFKA-C-001",
                    "payload": {"items": [{"sku": "SKU-001", "qty": 1}], "delivery_address": "Тест"},
                }
            )
        ]
    }

    processed = run_kafka_once()

    assert processed == 1
    mock_process.assert_called_once()


@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_dedup_kafka_same_event_ignored(mock_kafka, mock_process):
    """Dedup: одно и то же событие из Kafka обрабатывается только раз"""
    from consumer import run_kafka_once

    mock_kafka.poll.return_value = {
        None: [
            MagicMock(
                value={
                    "event_type": "order_created",
                    "order_id": "ORD-DEDUP-001",
                    "payload": {"items": [{"sku": "SKU-001", "qty": 1}], "delivery_address": "Тест"},
                }
            )
        ]
    }

    # Первый вызов — обрабатывает
    result1 = run_kafka_once()
    assert result1 == 1
    mock_process.assert_called_once()

    # Второй вызов — то же событие — dedup, НЕ обрабатывает
    result2 = run_kafka_once()
    assert result2 == 0
    # process_order вызван только 1 раз
    assert mock_process.call_count == 1


@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_dedup_cross_source_db_then_kafka(mock_kafka, mock_process, capsys):
    """Dedup: событие из БД, потом то же из Kafka — не обрабатывается дважды"""
    from consumer import handle_order_created, run_kafka_once

    # Имитируем событие из БД (через handle_order_created)
    mock_process.return_value = True
    handle_order_created(None, "ORD-DB-KAFKA-001", {"items": [{"sku": "SKU-001", "qty": 1}], "delivery_address": "Тест"})

    # Теперь то же событие приходит из Kafka
    mock_kafka.poll.return_value = {
        None: [
            MagicMock(
                value={
                    "event_type": "order_created",
                    "order_id": "ORD-DB-KAFKA-001",
                    "payload": {"items": [{"sku": "SKU-001", "qty": 1}], "delivery_address": "Тест"},
                }
            )
        ]
    }

    result = run_kafka_once()
    # Dedup — событие уже обработано из БД
    assert result == 0
    # process_order вызван только 1 раз (из БД), Kafka пропущена
    assert mock_process.call_count == 1
