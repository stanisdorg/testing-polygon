"""Тесты retry + DLQ в Kafka consumer."""
import json
import os
from unittest.mock import MagicMock, patch
import pytest
import redis

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")


def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)


def _retry_count(order_id):
    r = get_redis()
    val = r.get(f"retry:{order_id}")
    r.close()
    return int(val) if val else 0


@pytest.fixture(autouse=True)
def clean_redis():
    r = get_redis()
    r.flushdb()
    r.close()
    yield
    r = get_redis()
    r.flushdb()
    r.close()


def _kafka_msg(order_id):
    return MagicMock(
        value={
            "event_type": "order_created",
            "order_id": order_id,
            "payload": {"items": [{"sku": "SKU-001", "qty": 1}]},
        }
    )


def _consumer_mock(messages=None):
    mock = MagicMock()
    mock.poll.return_value = {None: messages or []}
    return mock


# --- Test 1: retry < 3, commit NOT called ---
@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_retry_increments_no_commit(mock_kafka, mock_process):
    """Retry 1: ошибка, retry=1, commit НЕ вызывается"""
    from consumer import run_kafka_once

    consumer = _consumer_mock([_kafka_msg("ORD-RETRY-001")])
    mock_kafka.return_value = consumer
    import consumer as c
    c.kafka_consumer = consumer

    mock_process.side_effect = Exception("timeout")

    result = run_kafka_once()

    assert result == 0
    assert _retry_count("ORD-RETRY-001") == 1
    consumer.commit.assert_not_called()


# --- Test 2: retry = 3 → DLQ + commit ---
@patch("consumer.dlq_producer")
@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_retry_max_sent_to_dlq(mock_kafka, mock_process, mock_dlq):
    """Retry 3: ошибка 3 раза → DLQ + commit"""
    from consumer import run_kafka_once

    # Предзаполняем retry=2
    r = get_redis()
    r.set("retry:ORD-DLQ-001", "2")
    r.close()

    consumer = _consumer_mock([_kafka_msg("ORD-DLQ-001")])
    mock_kafka.return_value = consumer
    import consumer as c
    c.kafka_consumer = consumer

    mock_process.side_effect = Exception("permanent error")

    result = run_kafka_once()

    # Not counted (went to DLQ)
    assert result == 0
    # DLQ producer called
    mock_dlq.send.assert_called_once()
    call_args = mock_dlq.send.call_args
    assert call_args[0][0] == "order_events_dlq"
    # Offset committed so we don't re-read
    consumer.commit.assert_called_once()
    # Retry counter cleared
    assert _retry_count("ORD-DLQ-001") == 0


# --- Test 3: success → retry cleared ---
@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_success_clears_retry(mock_kafka, mock_process):
    """Успешная обработка → retry counter очищается"""
    from consumer import run_kafka_once

    # Предзаполняем retry=1 (была ошибка ранее)
    r = get_redis()
    r.set("retry:ORD-OK-001", "1")
    r.close()

    consumer = _consumer_mock([_kafka_msg("ORD-OK-001")])
    mock_kafka.return_value = consumer
    import consumer as c
    c.kafka_consumer = consumer

    mock_process.return_value = True

    result = run_kafka_once()

    assert result == 1
    consumer.commit.assert_called_once()
    # Retry counter cleared
    assert _retry_count("ORD-OK-001") == 0
