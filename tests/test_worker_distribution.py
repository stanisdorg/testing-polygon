"""Test worker distribution logic."""
import os
import sys
import time
from collections import Counter

import psycopg2
import pytest
import redis

# Override DB and Redis URLs before importing consumer
os.environ["DATABASE_URL"] = "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"

DB_URL = "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox"
REDIS_URL = "redis://localhost:6379/0"
CONSUMER_DIR = os.path.join(os.path.dirname(__file__), "..", "services", "event-consumer-service")

# Mock kafka before importing consumer
class _FakeKafka:
    class KafkaProducer:
        def __init__(self, *a, **kw): pass

sys.modules.setdefault('kafka', _FakeKafka)
sys.path.insert(0, CONSUMER_DIR)

import consumer
# Override module-level redis_client
consumer.redis_client = redis.from_url(REDIS_URL, decode_responses=True)

from consumer import assign_worker_with_load, decrement_worker_load, MAX_CONCURRENT


@pytest.fixture
def db_conn():
    conn = psycopg2.connect(DB_URL)
    yield conn
    conn.close()


@pytest.fixture
def r():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    for key in r.scan_iter("load:*"):
        r.delete(key)
    yield r
    for key in r.scan_iter("load:*"):
        r.delete(key)


class TestWorkerDistribution:
    """Test that work is distributed evenly across workers."""

    def test_round_robin_distribution(self, db_conn, r):
        """Multiple assignments should distribute among all workers of a role."""
        cur = db_conn.cursor()
        cur.execute("SELECT name FROM employees WHERE role = 'picker'")
        pickers = [row[0] for row in cur.fetchall()]
        cur.close()
        assert len(pickers) >= 3, f"Need at least 3 pickers, got {len(pickers)}"

        # Clean load keys
        for p in pickers:
            r.delete(f"load:picker:{p}")

        # Assign 20 times
        assignments = []
        for _ in range(20):
            name = assign_worker_with_load("picker")
            assignments.append(name)
            time.sleep(0.05)

        counts = Counter(assignments)
        # All pickers should have received at least 1 assignment (after 20, with 10 pickers)
        assigned_count = len([p for p in pickers if counts.get(p, 0) > 0])
        assert assigned_count >= len(pickers) * 0.7, f"Only {assigned_count}/{len(pickers)} pickers got work: {counts}"

    def test_respects_max_concurrent(self, r):
        """Worker at max load should be skipped."""
        # Set a worker to max
        r.set("load:picker:Иван П.", MAX_CONCURRENT["picker"])

        # Assign many times — Иван П. should be skipped
        assignments = []
        for _ in range(15):
            name = assign_worker_with_load("picker")
            assignments.append(name)

        ivan_count = assignments.count("Иван П.")
        assert ivan_count == 0, f"Иван П. at max load got {ivan_count} assignments"
        r.delete("load:picker:Иван П.")

    def test_uses_all_couriers_from_db(self, db_conn, r):
        """All couriers from DB should be eligible for assignments."""
        cur = db_conn.cursor()
        cur.execute("SELECT name FROM employees WHERE role = 'courier'")
        couriers = [row[0] for row in cur.fetchall()]
        cur.close()
        assert len(couriers) >= 5, f"Need at least 5 couriers, got {len(couriers)}"

        for c in couriers:
            r.delete(f"load:courier:{c}")

        # Assign 30 times
        assignments = set()
        for _ in range(30):
            name = assign_worker_with_load("courier")
            assignments.add(name)
            time.sleep(0.05)

        coverage = len(assignments) / len(couriers)
        assert coverage >= 0.6, f"Only {len(assignments)}/{len(couriers)} couriers used: {assignments}"

    def test_fallback_when_all_busy(self, db_conn, r):
        """If all workers at max, least loaded should still be picked."""
        cur = db_conn.cursor()
        cur.execute("SELECT name FROM employees WHERE role = 'packer'")
        packers = [row[0] for row in cur.fetchall()]
        cur.close()

        for p in packers:
            r.set(f"load:packer:{p}", MAX_CONCURRENT["packer"])

        name = assign_worker_with_load("packer")
        assert name is not None, "Should fallback to least loaded"
        assert name in packers

        for p in packers:
            r.delete(f"load:packer:{p}")

    def test_load_decrement_on_completion(self, r):
        """After completing an order, load should decrement."""
        name = assign_worker_with_load("picker")
        load_after = int(r.get(f"load:picker:{name}") or 0)

        decrement_worker_load("picker", name)
        load_after_decr = int(r.get(f"load:picker:{name}") or 0)

        assert load_after_decr == load_after - 1

    def test_distribution_evenness(self, db_conn, r):
        """Statistical test: 50 assignments, stddev should be reasonable."""
        import statistics

        cur = db_conn.cursor()
        cur.execute("SELECT name FROM employees WHERE role = 'picker'")
        pickers = [row[0] for row in cur.fetchall()]
        cur.close()

        for p in pickers:
            r.delete(f"load:picker:{p}")

        n = 50
        assignments = []
        for _ in range(n):
            name = assign_worker_with_load("picker")
            assignments.append(name)
            time.sleep(0.05)

        counts = [Counter(assignments).get(p, 0) for p in pickers]
        avg = sum(counts) / len(counts)
        stddev = statistics.stdev(counts) if len(counts) > 1 else 0

        assert stddev < max(5, avg * 0.8), f"Too uneven: counts={counts}, stddev={stddev:.1f}, avg={avg:.1f}"
