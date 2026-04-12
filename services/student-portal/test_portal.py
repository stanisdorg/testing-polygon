"""Тесты student-portal — config, SQL validator, Redis, Kafka."""
import json
import os
import pytest
from unittest.mock import MagicMock, patch

# ── Config loader ──

def test_config_loader_loads_defaults():
    """Config загружает defaults если поля отсутствуют"""
    from portal import load_config

    # Минимальный конфиг
    minimal = {"rate_limit": {"max_requests_per_minute": 10}}

    cfg = load_config(minimal)
    assert "kafka" in cfg
    assert "redis" in cfg
    assert "postgres" in cfg
    assert cfg["kafka"]["readonly"] is True
    assert cfg["redis"]["readonly"] is True


def test_config_loader_blocks_malicious_sql():
    """Config содержит blocked_keywords"""
    from portal import load_config

    cfg = load_config({})
    blocked = cfg["postgres"]["blocked_keywords"]
    assert "DROP" in blocked
    assert "DELETE" in blocked
    assert "UPDATE" in blocked


# ── SQL validator ──

def test_sql_validator_allows_select():
    """SELECT запрос проходит валидацию"""
    from portal import validate_sql

    result = validate_sql("SELECT * FROM orders LIMIT 10")
    assert result["ok"] is True
    assert "error" not in result


def test_sql_validator_blocks_drop():
    """DROP блокируется"""
    from portal import validate_sql

    result = validate_sql("DROP TABLE orders")
    assert result["ok"] is False
    # Сообщение на русском или английском — главное что запрос отклонён
    assert "select" in result["error"].lower() or "blocked" in result["error"].lower() or "blocked" in result.get("error", "").lower()


def test_sql_validator_blocks_delete():
    """DELETE блокируется"""
    from portal import validate_sql

    result = validate_sql("DELETE FROM events WHERE id = 1")
    assert result["ok"] is False


def test_sql_validator_blocks_update():
    """UPDATE блокируется"""
    from portal import validate_sql

    result = validate_sql("UPDATE orders SET status = 'done'")
    assert result["ok"] is False


def test_sql_validator_adds_limit():
    """Если нет LIMIT — добавляется автоматически"""
    from portal import validate_sql, enforce_limit

    query = "SELECT * FROM events"
    result = enforce_limit(query, max_rows=50)
    assert "LIMIT 50" in result


def test_sql_validator_respects_existing_limit():
    """Если LIMIT уже есть — не дублируется"""
    from portal import enforce_limit

    query = "SELECT * FROM events LIMIT 10"
    result = enforce_limit(query, max_rows=50)
    assert result == "SELECT * FROM events LIMIT 10"


def test_sql_validator_caps_max_limit():
    """Если LIMIT > max_rows — уменьшается"""
    from portal import enforce_limit

    query = "SELECT * FROM events LIMIT 9999"
    result = enforce_limit(query, max_rows=100)
    assert "LIMIT 100" in result


# ── Redis reader ──

def test_redis_reader_only_read_operations():
    """Redis reader разрешает только read команды"""
    from portal import redis_operation_allowed

    assert redis_operation_allowed("GET", "stock:SKU-001") is True
    assert redis_operation_allowed("KEYS", "stock:*") is True
    assert redis_operation_allowed("SCAN", "0") is True
    assert redis_operation_allowed("TYPE", "stock:SKU-001") is True
    assert redis_operation_allowed("TTL", "stock:SKU-001") is True


def test_redis_reader_blocks_write_operations():
    """Redis reader блокирует write команды"""
    from portal import redis_operation_allowed

    assert redis_operation_allowed("SET", "stock:SKU-001", "100") is False
    assert redis_operation_allowed("DEL", "stock:SKU-001") is False
    assert redis_operation_allowed("LPUSH", "mylist", "val") is False
    assert redis_operation_allowed("FLUSHALL") is False
    assert redis_operation_allowed("FLUSHDB") is False
