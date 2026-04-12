"""Общие фикстуры для тестов event-consumer."""
import os
import redis
import pytest

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture(autouse=True)
def clean_redis_cache():
    """Очищает Redis кэш перед каждым тестом."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.flushdb()
        r.close()
    except Exception:
        pass
