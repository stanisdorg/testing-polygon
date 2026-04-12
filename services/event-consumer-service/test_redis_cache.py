"""Тесты Redis кэширования в process_order."""
import json
import os
from unittest.mock import patch, MagicMock
import pytest
import redis

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")


def get_redis():
    return redis.from_url(REDIS_URL)


def _cache_value(sku):
    r = get_redis()
    val = r.get(f"stock:{sku}")
    r.close()
    return val


@patch("consumer.warehouse")
def test_cache_miss_warehouse_is_called(mock_warehouse):
    """Cache miss: warehouse вызывается, результат сохраняется в Redis"""
    from consumer import process_order

    mock_warehouse.check_availability.return_value = True

    result = process_order("ORD-CACHE-001", [{"sku": "SKU-999", "qty": 1}])

    assert result is True
    # Warehouse вызван
    mock_warehouse.check_availability.assert_called_once_with("SKU-999", 1)
    # Значение сохранено в Redis
    assert _cache_value("SKU-999") == b"true"


@patch("consumer.warehouse")
def test_cache_hit_warehouse_not_called(mock_warehouse):
    """Cache hit: warehouse НЕ вызывается, значение берётся из Redis"""
    from consumer import process_order

    # Предзаполняем кэш
    r = get_redis()
    r.setex("stock:SKU-888", 10, "true")
    r.close()

    result = process_order("ORD-CACHE-002", [{"sku": "SKU-888", "qty": 1}])

    assert result is True
    # Warehouse НЕ вызван — данные из кэша
    mock_warehouse.check_availability.assert_not_called()


@patch("consumer.warehouse")
def test_cache_hit_negative(mock_warehouse):
    """Cache hit: товар недоступен (кэшировано false)"""
    from consumer import process_order

    # Предзаполняем кэш — товара нет
    r = get_redis()
    r.setex("stock:SKU-777", 10, "false")
    r.close()

    result = process_order("ORD-CACHE-003", [{"sku": "SKU-777", "qty": 1}])

    assert result is False
    mock_warehouse.check_availability.assert_not_called()
