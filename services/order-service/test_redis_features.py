"""Tests for Redis caching and rate limiting with mocked Redis."""
import json
import uuid
from unittest.mock import MagicMock, patch

import psycopg2
from fastapi.testclient import TestClient

import main as app_module
from main import app

client = TestClient(app)


def _get_db_conn():
    import os
    db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
    )
    return psycopg2.connect(db_url)


def _seed_product(sku="SKU-CACHE-001", name="Cache Test Product", price=500):
    conn = _get_db_conn()
    cur = conn.cursor()
    # Удаляем если уже есть
    cur.execute("DELETE FROM products WHERE sku = %s", (sku,))
    cur.execute(
        "INSERT INTO products (sku, name, price) VALUES (%s, %s, %s) RETURNING id",
        (sku, name, price),
    )
    pid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return pid


# ── Mock Redis setup ─────────────────────────────────────────────────


class MockRedis:
    """Mock Redis that simulates caching behavior."""

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value

    def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)

    def keys(self, pattern):
        import fnmatch
        return [k for k in self.store if fnmatch.fnmatch(k, pattern)]

    def ping(self):
        return True

    def pipeline(self):
        return MockPipeline()

    def incr(self, key):
        val = self.store.get(key, 0) + 1
        self.store[key] = val
        return val

    def expire(self, key, seconds):
        pass  # TTL не нужен для тестов


class MockPipeline:
    def __init__(self):
        self._ops = []

    def incr(self, key):
        self._ops.append(("incr", key))
        return self

    def expire(self, key, seconds):
        self._ops.append(("expire", key, seconds))
        return self

    def execute(self):
        results = []
        mock_redis = app_module.redis_client
        for op in self._ops:
            if op[0] == "incr":
                results.append(mock_redis.incr(op[1]))
            elif op[0] == "expire":
                results.append(True)
        self._ops = []
        return results


def _setup_mock_redis():
    """Заменяет реальный Redis на мок."""
    mock = MockRedis()
    app_module.redis_client = mock
    return mock


# ── Cache Tests ──────────────────────────────────────────────────────


def test_cache_miss_first_request():
    """Cache MISS при первом запросе продукта."""
    mock = _setup_mock_redis()
    pid = _seed_product(sku="SKU-MISS-001", name="Miss Product", price=100)

    resp = client.get(f"/api/v1/products/{pid}")
    assert resp.status_code == 200
    assert resp.headers.get("X-Cache") == "MISS"
    assert resp.json()["name"] == "Miss Product"


def test_cache_hit_second_request():
    """Cache HIT при повторном запросе продукта."""
    mock = _setup_mock_redis()
    pid = _seed_product(sku="SKU-HIT-001", name="Hit Product", price=200)

    # Первый запрос — MISS
    resp1 = client.get(f"/api/v1/products/{pid}")
    assert resp1.status_code == 200
    assert resp1.headers.get("X-Cache") == "MISS"

    # Второй запрос — HIT
    resp2 = client.get(f"/api/v1/products/{pid}")
    assert resp2.status_code == 200
    assert resp2.headers.get("X-Cache") == "HIT"
    assert resp2.json()["name"] == "Hit Product"


def test_cache_invalidated_on_update():
    """Кэш инвалидируется после PUT запроса."""
    mock = _setup_mock_redis()
    pid = _seed_product(sku="SKU-INV-001", name="Before Update", price=300)

    # Первый запрос — MISS
    resp1 = client.get(f"/api/v1/products/{pid}")
    assert resp1.headers.get("X-Cache") == "MISS"

    # Второй — HIT
    resp2 = client.get(f"/api/v1/products/{pid}")
    assert resp2.headers.get("X-Cache") == "HIT"

    # Обновляем продукт
    resp_update = client.put(
        f"/api/v1/products/{pid}",
        json={"name": "After Update"},
    )
    assert resp_update.status_code == 200

    # После обновления — снова MISS (кэш сброшен)
    resp3 = client.get(f"/api/v1/products/{pid}")
    assert resp3.headers.get("X-Cache") == "MISS"
    assert resp3.json()["name"] == "After Update"


def test_cache_warehouse():
    """Кэш работает для warehouses."""
    mock = _setup_mock_redis()

    # WH-MSK-S существует в seed данных
    resp1 = client.get("/api/v1/warehouses/WH-MSK-S")
    assert resp1.status_code == 200
    assert resp1.headers.get("X-Cache") == "MISS"

    resp2 = client.get("/api/v1/warehouses/WH-MSK-S")
    assert resp2.status_code == 200
    assert resp2.headers.get("X-Cache") == "HIT"


# ── Rate Limiting Tests ──────────────────────────────────────────────


def test_rate_limit_allows_within_limit():
    """Rate limiter позволяет делать до 5 запросов в минуту."""
    mock = _setup_mock_redis()

    import uuid
    order_id = f"ORD-RL-OK-{uuid.uuid4().hex[:8]}"
    for i in range(5):
        resp = client.post(
            "/api/v1/orders",
            json={
                "order_id": f"ORD-RL-{i}-{uuid.uuid4().hex[:8]}",
                "items": [{"sku": "SKU-001", "qty": 1}],
                "delivery_address": "Moscow",
            },
        )
        assert resp.status_code == 201, f"Request {i+1} failed: {resp.text}"


def test_rate_limit_blocks_after_exceeded():
    """Rate limiter блокирует 6-й запрос."""
    mock = _setup_mock_redis()

    # 5 разрешённых запросов
    for i in range(5):
        resp = client.post(
            "/api/v1/orders",
            json={
                "order_id": f"ORD-RL2-{i}-{uuid.uuid4().hex[:8]}",
                "items": [{"sku": "SKU-001", "qty": 1}],
                "delivery_address": "Moscow",
            },
        )
        assert resp.status_code == 201, f"Request {i+1} failed"

    # 6-й запрос — должен быть заблокирован
    resp = client.post(
        "/api/v1/orders",
        json={
            "order_id": f"ORD-RL2-BLOCKED-{uuid.uuid4().hex[:8]}",
            "items": [{"sku": "SKU-001", "qty": 1}],
            "delivery_address": "Moscow",
        },
    )
    assert resp.status_code == 429
    assert "Rate limit exceeded" in resp.json()["detail"]
