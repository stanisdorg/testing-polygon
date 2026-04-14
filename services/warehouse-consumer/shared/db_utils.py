"""Shared database and Kafka utilities for all consumers."""
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import redis
from kafka import KafkaProducer

# ── Config ──
DB_URL = os.environ.get("DATABASE_URL", "postgresql://fulfilbox:fulfilbox@postgres:5432/fulfilbox")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:29092")

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ── Kafka Producer (lazy init) ──
_kafka_producer: Optional[KafkaProducer] = None
_kafka_lock = threading.Lock()


def get_kafka_producer() -> Optional[KafkaProducer]:
    """Get or create singleton Kafka producer."""
    global _kafka_producer
    if _kafka_producer is None:
        with _kafka_lock:
            if _kafka_producer is None:
                try:
                    _kafka_producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKER_URL,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        request_timeout_ms=5000,
                        retries=3,
                    )
                except Exception as e:
                    print(f"⚠ Failed to create Kafka producer: {e}")
                    return None
    return _kafka_producer


def publish_kafka_event(
    topic: str,
    event_type: str,
    order_id: str,
    payload: dict,
    trace_id: str,
    entity_type: str,
    entity_id: str,
    saga_id: str,
    step_name: str,
    is_compensation: bool = False,
):
    """Publish event to Kafka topic."""
    try:
        producer = get_kafka_producer()
        if producer is None:
            print(f"⚠ Kafka producer not available, skipping event: {event_type}")
            return False

        message = {
            "event_type": event_type,
            "order_id": order_id,
            "trace_id": trace_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "saga_id": saga_id,
            "step_name": step_name,
            "is_compensation": is_compensation,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        future = producer.send(topic, value=message)
        producer.flush()
        future.get(timeout=5)
        print(f"  📤 Published to Kafka [{topic}]: {event_type} for {order_id}")
        return True
    except Exception as e:
        print(f"  ❌ Failed to publish to Kafka [{topic}]: {e}")
        return False


# ── Database helpers ──
def get_db():
    """Get database connection."""
    return psycopg2.connect(DB_URL)


def insert_event(
    event_type: str,
    order_id: str,
    payload: dict,
    trace_id: str,
    entity_type: str,
    entity_id: str,
    saga_id: str,
    step_name: str,
    is_compensation: bool = False,
):
    """Insert event into events table (idempotent) and publish to Redis pub/sub."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO events (event_type, order_id, payload, trace_id, "
            "entity_type, entity_id, saga_id, step_name, is_compensation) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                event_type,
                order_id,
                json.dumps(payload or {}),
                trace_id,
                entity_type,
                entity_id,
                saga_id,
                step_name,
                is_compensation,
            ),
        )
        # Update current_stage in orders for dashboard sync
        cur.execute(
            "UPDATE orders SET current_stage = %s WHERE id = %s",
            (event_type, order_id),
        )
        conn.commit()
        print(f"  💾 Event saved to DB: {event_type} for {order_id}")
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print(f"  ⚠ Event already exists (idempotent): {event_type} for {order_id}")
        return False
    finally:
        cur.close()
        conn.close()

    # Publish to Redis Pub/Sub for WebSocket real-time updates
    publish_event_to_redis({
        "event_type": event_type,
        "order_id": order_id,
        "trace_id": trace_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "saga_id": saga_id,
        "step_name": step_name,
        "is_compensation": is_compensation,
        "payload": payload or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    return True


def event_exists(order_id: str, event_type: str, saga_id: str = None) -> bool:
    """Check if event already exists (for idempotency)."""
    conn = get_db()
    cur = conn.cursor()
    try:
        if saga_id:
            cur.execute(
                "SELECT 1 FROM events WHERE order_id=%s AND event_type=%s AND saga_id=%s",
                (order_id, event_type, saga_id),
            )
        else:
            cur.execute(
                "SELECT 1 FROM events WHERE order_id=%s AND event_type=%s",
                (order_id, event_type),
            )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def update_order_status(order_id: str, status: str):
    """Update order status in orders table."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", (status, order_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_order(order_id: str) -> Optional[dict]:
    """Get order details from database."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, status, warehouse_id, total_price FROM orders WHERE id = %s",
            (order_id,),
        )
        row = cur.fetchone()
        if row:
            return {
                "id": row[0],
                "status": row[1],
                "warehouse_id": row[2],
                "total_price": row[3],
            }
        return None
    finally:
        cur.close()
        conn.close()


def get_last_event_payload(order_id: str, event_type: str) -> Optional[dict]:
    """Get payload of the last event of specified type for an order."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT payload FROM events WHERE order_id = %s AND event_type = %s ORDER BY id DESC LIMIT 1",
            (order_id, event_type),
        )
        row = cur.fetchone()
        if row and row[0]:
            try:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]
            except Exception:
                return None
        return None
    finally:
        cur.close()
        conn.close()


# ── Deduplication ──
def acquire_dedup_lock(key: str, ttl: int = 300) -> bool:
    """Acquire deduplication lock in Redis. Returns False if already locked."""
    return redis_client.set(key, "1", nx=True, ex=ttl)


def release_dedup_lock(key: str):
    """Release deduplication lock."""
    redis_client.delete(key)


# ── Redis Pub/Sub for WS Gateway ──────────────────────────────────
REDIS_PUBSUB_CHANNEL = "events_stream"


def publish_event_to_redis(event_data: dict) -> bool:
    """Публикует событие в Redis Pub/Sub для рассылки через WebSocket."""
    try:
        redis_client.publish(REDIS_PUBSUB_CHANNEL, json.dumps(event_data))
        return True
    except Exception as e:
        print(f"  ⚠ Redis pub/sub failed: {e}")
        return False
