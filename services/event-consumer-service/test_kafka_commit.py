"""Тесты manual commit offset и error handling в Kafka consumer."""
import json
import os
from unittest.mock import MagicMock, patch, call
import pytest
import redis

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture(autouse=True)
def clean_redis():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.flushdb()
    r.close()
    yield
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.flushdb()
    r.close()


def _kafka_msg(order_id, payload=None):
    return MagicMock(
        value={
            "event_type": "order_created",
            "order_id": order_id,
            "payload": payload or {"items": [{"sku": "SKU-001", "qty": 1}]},
        }
    )


def _kafka_consumer_mock(messages=None):
    mock = MagicMock()
    mock.poll.return_value = {None: messages or []}
    return mock


@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_kafka_commit_on_success(mock_kafka, mock_process):
    """Happy path: message processed → commit called"""
    from consumer import run_kafka_once

    consumer = _kafka_consumer_mock([_kafka_msg("ORD-COMMIT-001")])
    mock_kafka.return_value = consumer
    # Invalidate cached consumer
    import consumer as c
    c.kafka_consumer = consumer

    mock_process.return_value = True

    result = run_kafka_once()

    assert result == 1
    mock_process.assert_called_once()
    consumer.commit.assert_called_once()


@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_kafka_no_commit_on_error(mock_kafka, mock_process):
    """Error: process_order raises → commit NOT called"""
    from consumer import run_kafka_once

    consumer = _kafka_consumer_mock([_kafka_msg("ORD-ERROR-001")])
    mock_kafka.return_value = consumer
    import consumer as c
    c.kafka_consumer = consumer

    mock_process.side_effect = Exception("timeout")

    result = run_kafka_once()

    # Not counted as processed
    assert result == 0
    # Commit NOT called
    consumer.commit.assert_not_called()


@patch("consumer.process_order")
@patch("consumer.kafka_consumer")
def test_kafka_partial_commit(mock_kafka, mock_process):
    """Mixed: 1 success + 1 error → only 1 committed"""
    from consumer import run_kafka_once

    consumer = _kafka_consumer_mock([
        _kafka_msg("ORD-OK-001"),
        _kafka_msg("ORD-FAIL-001"),
    ])
    mock_kafka.return_value = consumer
    import consumer as c
    c.kafka_consumer = consumer

    # First succeeds, second fails
    mock_process.side_effect = [True, Exception("timeout")]

    result = run_kafka_once()

    # Only first processed
    assert result == 1
    # Commit called only once (after first)
    assert consumer.commit.call_count == 1
