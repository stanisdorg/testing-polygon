"""Общие фикстуры для тестов."""
import os
import pytest
import psycopg2

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


@pytest.fixture(autouse=True)
def clean_db():
    """Очищает таблицы перед каждым тестом."""
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        cur.execute("DELETE FROM events")
        cur.execute("DELETE FROM orders")
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass
