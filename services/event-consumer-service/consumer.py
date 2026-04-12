"""Fulfilment SAGA Consumer — State Machine с Compensating Transactions.

Каждый заказ — это SAGA с уникальным saga_id.
Если любой шаг падает — запускается compensation chain в обратном порядке.
"""
import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

import httpx
import psycopg2
import redis

# ── Warehouse mock ──
import sys
sys.path.insert(0, os.path.dirname(__file__))
from warehouse import WarehouseService

warehouse = WarehouseService()

# ── Config ──
DB_URL = os.environ.get("DATABASE_URL", "postgresql://fulfilbox:fulfilbox@postgres:5432/fulfilbox")
WS_GATEWAY_URL = os.environ.get("WS_GATEWAY_URL", "http://ws-gateway:8002")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:29092")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8003"))
POLL_INTERVAL = 2
MAX_RETRIES = 3
RETRY_TTL = 60

# Realistic SAGA step delays (seconds) — simulates real warehouse operations
SAGA_STEP_DELAY_MIN = int(os.environ.get("SAGA_STEP_DELAY_MIN", "2"))
SAGA_STEP_DELAY_MAX = int(os.environ.get("SAGA_STEP_DELAY_MAX", "5"))


def _saga_sleep():
    """Simulate realistic warehouse operation time between SAGA steps."""
    delay = random.uniform(SAGA_STEP_DELAY_MIN, SAGA_STEP_DELAY_MAX)
    print(f"    ⏳ Waiting {delay:.0f}s before next step...")
    time.sleep(delay)

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ── Chaos ──
CHAOS_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "chaos_config.json")
_chaos_config = {}

def load_chaos_config():
    global _chaos_config
    if not _chaos_config:
        try:
            with open(CHAOS_CONFIG_PATH) as f:
                _chaos_config = json.load(f)
        except Exception:
            _chaos_config = {"enabled": False, "failure_scenarios": {}}
    return _chaos_config

def apply_chaos(order_id, trace_id=None):
    config = load_chaos_config()
    if not config.get("enabled"):
        return
    tag = f"[CHAOS][trace_id={trace_id}]" if trace_id else "[CHAOS]"
    scenarios = config.get("failure_scenarios", {})
    delay_cfg = scenarios.get("random_delay", {})
    if delay_cfg.get("enabled") and random.random() < delay_cfg.get("probability", 0):
        low_ms, high_ms = delay_cfg.get("delay_range_ms", [100, 2000])
        delay_ms = random.uniform(low_ms, high_ms)
        print(f"{tag} Applied random_delay: {delay_ms:.0f}ms for {order_id}")
        time.sleep(delay_ms / 1000.0)
    fail_cfg = scenarios.get("random_failure", {})
    if fail_cfg.get("enabled") and random.random() < fail_cfg.get("probability", 0):
        error_msg = fail_cfg.get("error_message", "Chaos error")
        print(f"{tag} Applied random_failure: {error_msg} for {order_id}")
        raise Exception(f"[CHAOS][trace_id={trace_id}] {order_id}: {error_msg}")


# ── Kafka ──
_kafka_producer = None

def _get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        try:
            from kafka import KafkaProducer
            _kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000, retries=0,
            )
        except Exception:
            pass
    return _kafka_producer

def _publish_kafka(event_type, order_id, payload, trace_id, entity_type=None, entity_id=None,
                   saga_id=None, step_name=None, is_compensation=False):
    try:
        producer = _get_kafka_producer()
        if producer:
            msg = {
                "event_type": event_type, "order_id": order_id,
                "trace_id": trace_id or order_id,
                "entity_type": entity_type, "entity_id": entity_id,
                "payload": payload or {}, "saga_id": saga_id,
                "step_name": step_name, "is_compensation": is_compensation,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            producer.send("fulfilment.events", value=msg)
            producer.flush()
    except Exception:
        pass

# ── DB helpers ──
def get_db():
    return psycopg2.connect(DB_URL)

def _publish_ws(event_type, order_id, trace_id):
    """Push event to ws-gateway for realtime UI updates."""
    try:
        httpx.post(
            f"{WS_GATEWAY_URL}/publish",
            json={
                "type": event_type,
                "order_id": order_id,
                "status": event_type,
                "trace_id": trace_id or "",
            },
            timeout=2,
        )
    except Exception:
        pass  # ws-gateway unavailable — не критично


def _insert_event(event_type, order_id, payload=None, trace_id=None,
                  entity_type=None, entity_id=None, saga_id=None,
                  step_name=None, is_compensation=False):
    """Idempotent: не вставит дубликат."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO events (event_type, order_id, payload, trace_id, "
            "entity_type, entity_id, saga_id, step_name, is_compensation) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (event_type, order_id, json.dumps(payload or {}), trace_id,
             entity_type, entity_id, saga_id, step_name, is_compensation),
        )
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return  # Idempotency: событие уже существует
    finally:
        cur.close()
        conn.close()
    _publish_kafka(event_type, order_id, payload, trace_id, entity_type, entity_id,
                   saga_id, step_name, is_compensation)
    _publish_ws(event_type, order_id, trace_id)


def _event_exists(order_id, event_type, saga_id=None):
    """Проверка idempotency."""
    conn = get_db()
    cur = conn.cursor()
    if saga_id:
        cur.execute("SELECT 1 FROM events WHERE order_id=%s AND event_type=%s AND saga_id=%s",
                    (order_id, event_type, saga_id))
    else:
        cur.execute("SELECT 1 FROM events WHERE order_id=%s AND event_type=%s",
                    (order_id, event_type))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists


def _update_order_status(order_id, status):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE orders SET status = %s WHERE id = %s", (status, order_id))
    conn.commit()
    cur.close()
    conn.close()


def _update_payment(order_id, status, amount=0):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM payments WHERE order_id = %s", (order_id,))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE payments SET status = %s WHERE id = %s", (status, row[0]))
    else:
        cur.execute("INSERT INTO payments (order_id, amount, status) VALUES (%s, %s, %s)",
                    (order_id, amount, status))
    conn.commit()
    cur.close()
    conn.close()


def _cancel_delivery(order_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE deliveries SET cancelled = TRUE WHERE order_id = %s", (order_id,))
    conn.commit()
    cur.close()
    conn.close()

def _update_delivery_status(order_id, status):
    """Update delivery status in deliveries table."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE deliveries SET status = %s WHERE order_id = %s", (status, order_id))
    conn.commit()
    cur.close()
    conn.close()


MAX_CONCURRENT = {
    "picker": 3,
    "packer": 4,
    "courier": 5,
}


def assign_worker_with_load(role):
    """Выбирает сотрудника с наименьшей загрузкой (load-aware round-robin)."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT name FROM employees WHERE role = %s ORDER BY name", (role,))
    workers = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    if not workers:
        print(f"  ⚠ No employees found for role: {role}")
        return None

    max_load = MAX_CONCURRENT.get(role, 5)

    # Get current loads from Redis
    loads = {}
    for w in workers:
        key = f"load:{role}:{w}"
        load = int(redis_client.get(key) or 0)
        loads[w] = load

    # Filter workers below max concurrent
    available = [w for w, l in loads.items() if l < max_load]

    if not available:
        # Fallback: pick least loaded
        print(f"  ⚠ All {role}s at max load ({max_load}), picking least loaded")
        available = workers

    # Pick worker with minimum load, use round-robin as tiebreaker
    min_load = min(loads.get(w, 0) for w in available)
    candidates = [w for w in available if loads.get(w, 0) == min_load]
    
    # Round-robin among candidates with same load
    with _employee_counters_lock:
        idx = _employee_counters.get(role, 0)
        chosen = candidates[idx % len(candidates)]
        _employee_counters[role] = (idx + 1) % len(candidates)

    # Increment load in Redis
    key = f"load:{role}:{chosen}"
    redis_client.incr(key)
    redis_client.expire(key, 3600)

    return chosen


def increment_worker_load(role, name):
    """Увеличивает загрузку сотрудника при назначении заказа."""
    key = f"load:{role}:{name}"
    redis_client.incr(key)
    redis_client.expire(key, 3600)


def decrement_worker_load(role, name):
    """Уменьшает загрузку сотрудника после завершения заказа."""
    key = f"load:{role}:{name}"
    redis_client.decr(key)
    redis_client.expire(key, 3600)


def _get_last_event_payload(order_id, event_type):
    """Получает payload последнего события указанного типа для заказа."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT payload FROM events WHERE order_id = %s AND event_type = %s ORDER BY id DESC LIMIT 1",
        (order_id, event_type),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0]) if isinstance(row[0], str) else row[0]
    except Exception:
        return None


# Round-robin counters for employee assignment
_employee_counters = {"picker": 0, "packer": 0, "courier": 0}
_employee_counters_lock = threading.Lock()


def _assign_employee(role):
    """Выбирает сотрудника по round-robin для равномерного распределения."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT name FROM employees WHERE role = %s ORDER BY name", (role,))
    employees = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    if not employees:
        return None
    
    with _employee_counters_lock:
        idx = _employee_counters.get(role, 0)
        selected = employees[idx % len(employees)]
        _employee_counters[role] = (idx + 1) % len(employees)
    
    return selected


def _restock_inventory(items):
    """Возвращает reserved_qty обратно в available_qty."""
    for item in items:
        sku = item["sku"]
        redis_client.delete(f"stock:{sku}")  # Invalidate cache


# ── SAGA: Main Flow ──
SAGA_STEPS = [
    ("inventory_reserved", "inventory", "reserve"),
    ("payment_succeeded", "payment", "charge"),
    ("picking_completed", "warehouse", "pick"),
    ("order_packed", "warehouse", "pack"),
    ("order_shipped", "order", "ship"),
    ("delivery_completed", "delivery", "deliver"),
    ("order_completed", "order", "complete"),
]

COMPENSATION_MAP = {
    "payment_succeeded": [
        ("payment_refunded", "payment", lambda oid, items: _update_payment(oid, "refunded")),
        ("inventory_restocked", "inventory", lambda oid, items: _restock_inventory(items)),
        ("order_cancelled", "order", lambda oid, items: _update_order_status(oid, "CANCELLED")),
    ],
    "picking_completed": [
        ("payment_refunded", "payment", lambda oid, items: _update_payment(oid, "refunded")),
        ("inventory_restocked", "inventory", lambda oid, items: _restock_inventory(items)),
        ("order_cancelled", "order", lambda oid, items: _update_order_status(oid, "CANCELLED")),
    ],
    "order_shipped": [
        ("delivery_cancelled", "delivery", lambda oid, items: _cancel_delivery(oid)),
        ("payment_refunded", "payment", lambda oid, items: _update_payment(oid, "refunded")),
        ("inventory_restocked", "inventory", lambda oid, items: _restock_inventory(items)),
        ("order_cancelled", "order", lambda oid, items: _update_order_status(oid, "CANCELLED")),
    ],
}

def _check_inventory(items):
    for item in items:
        cached = redis_client.get(f"stock:{item['sku']}")
        if cached is not None:
            if cached != "true":
                return False
        else:
            available = warehouse.check_availability(item["sku"], item["qty"])
            redis_client.setex(f"stock:{item['sku']}", 10, str(available).lower())
            if not available:
                return False
    return True

def _request_payment():
    """80% успех, 20% fail."""
    return random.random() < 0.8


def run_compensation(order_id, items, trace_id, failed_step, saga_id):
    """Compensation chain — rollback в обратном порядке."""
    print(f"  [trace_id={trace_id}] Starting compensation for {order_id} (failed at: {failed_step})")
    compensations = COMPENSATION_MAP.get(failed_step, COMPENSATION_MAP.get("payment_succeeded", []))
    for event_type, entity_type, action in compensations:
        if not _event_exists(order_id, event_type, saga_id):
            _insert_event(event_type, order_id, {}, trace_id, entity_type, order_id,
                          saga_id, event_type, is_compensation=True)
            try:
                action(order_id, items)
                print(f"  [trace_id={trace_id}] ↩ {event_type} for {order_id}")
            except Exception as e:
                print(f"  [trace_id={trace_id}] ⚠ Compensation {event_type} failed: {e}")


def handle_order_created(event_id, order_id, payload, saga_id):
    """SAGA orchestrator: Order → Inventory → Payment → Picking → Packing → Shipping → Delivery."""
    trace_id = payload.get("trace_id")
    if not trace_id or trace_id == order_id:
        import uuid
        trace_id = str(uuid.uuid4())
        print(f"  [WARN] trace_id missing for {order_id}, generated new: {trace_id}")
    dedup_key = f"saga:{saga_id}"
    if redis_client.get(dedup_key):
        return
    redis_client.setex(dedup_key, 300, "1")

    items = payload.get("items", [])
    print(f"[trace_id={trace_id}] Starting SAGA for {order_id} (saga={saga_id[:8]})")
    _update_order_status(order_id, "PROCESSING")

    try:
        apply_chaos(order_id, trace_id)

        # Step 1: Inventory
        if not _event_exists(order_id, "inventory_reserved", saga_id):
            if not _check_inventory(items):
                _insert_event("inventory_failed", order_id, {"items": items}, trace_id,
                              "inventory", order_id, saga_id, "inventory_reserved")
                _update_order_status(order_id, "FAILED")
                run_compensation(order_id, items, trace_id, "inventory_reserved", saga_id)
                return
            _insert_event("inventory_reserved", order_id, {"items": items}, trace_id,
                          "inventory", order_id, saga_id, "inventory_reserved")
            _saga_sleep()

        # Step 2: Payment
        if not _event_exists(order_id, "payment_succeeded", saga_id):
            _insert_event("payment_requested", order_id, {"total": 0}, trace_id,
                          "payment", order_id, saga_id, "payment_requested")
            _update_payment(order_id, "requested")
            if not _request_payment():
                _insert_event("payment_failed", order_id, {"reason": "declined"}, trace_id,
                              "payment", order_id, saga_id, "payment_requested")
                _update_payment(order_id, "failed")
                run_compensation(order_id, items, trace_id, "payment_succeeded", saga_id)
                return
            _insert_event("payment_succeeded", order_id, {}, trace_id,
                          "payment", order_id, saga_id, "payment_succeeded")
            _update_payment(order_id, "succeeded")
            _saga_sleep()

        # Step 3: Picking
        if not _event_exists(order_id, "picking_completed", saga_id):
            warehouse_id = payload.get("warehouse_id", "WH-MSK-S")
            picker = assign_worker_with_load("picker")
            _insert_event("picking_started", order_id, {"warehouse": warehouse_id, "picker": picker}, trace_id,
                          "warehouse", order_id, saga_id, "picking_started")
            _saga_sleep()
            _insert_event("picking_completed", order_id, {"items_count": len(items), "picker": picker}, trace_id,
                          "warehouse", order_id, saga_id, "picking_completed")
            _saga_sleep()

        # Step 4: Packing
        if not _event_exists(order_id, "order_packed", saga_id):
            packer = assign_worker_with_load("packer")
            _insert_event("order_packed", order_id, {"box_size": "M", "packer": packer}, trace_id,
                          "warehouse", order_id, saga_id, "order_packed")
            _saga_sleep()

        # Step 5: Shipping
        if not _event_exists(order_id, "order_shipped", saga_id):
            _insert_event("order_shipped", order_id, {"carrier": "CDEK"}, trace_id,
                          "order", order_id, saga_id, "order_shipped")
            _saga_sleep()

        # Step 6: Delivery
        if not _event_exists(order_id, "delivery_completed", saga_id):
            courier = assign_worker_with_load("courier")
            conn = get_db()
            cur = conn.cursor()
            try:
                cur.execute("INSERT INTO deliveries (order_id, courier_name, status) VALUES (%s, %s, 'assigned')",
                            (order_id, courier))
                conn.commit()
            except Exception:
                conn.rollback()
            cur.close()
            conn.close()
            _insert_event("delivery_assigned", order_id, {"courier": courier}, trace_id,
                          "delivery", order_id, saga_id, "delivery_assigned")
            _saga_sleep()
            _insert_event("delivery_started", order_id, {}, trace_id,
                          "delivery", order_id, saga_id, "delivery_started")
            _update_delivery_status(order_id, "in_transit")
            _saga_sleep()
            _insert_event("delivery_completed", order_id, {}, trace_id,
                          "delivery", order_id, saga_id, "delivery_completed")
            _update_delivery_status(order_id, "delivered")

        # Success
        _update_order_status(order_id, "COMPLETED")
        _insert_event("order_completed", order_id, {}, trace_id,
                      "order", order_id, saga_id, "order_completed")
        # Decrement worker load on completion
        picking_payload = _get_last_event_payload(order_id, "picking_completed")
        packer_payload = _get_last_event_payload(order_id, "order_packed")
        if picking_payload and picking_payload.get("picker"):
            decrement_worker_load("picker", picking_payload["picker"])
        if packer_payload and packer_payload.get("packer"):
            decrement_worker_load("packer", packer_payload["packer"])
        delivery_payload = _get_last_event_payload(order_id, "delivery_assigned")
        if delivery_payload and delivery_payload.get("courier"):
            decrement_worker_load("courier", delivery_payload["courier"])
        print(f"  [trace_id={trace_id}] ✅ SAGA completed for {order_id}")

    except Exception as e:
        print(f"  [trace_id={trace_id}] ❌ SAGA error: {e}")
        _update_order_status(order_id, "FAILED")
        run_compensation(order_id, items, trace_id, "order_shipped", saga_id)


def run_once():
    """Один цикл polling: читает необработанные order_created."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, order_id, payload, saga_id FROM events "
        "WHERE processed = false AND event_type = 'order_created' "
        "ORDER BY created_at ASC LIMIT 5",
    )
    rows = cur.fetchall()
    for row in rows:
        event_id, order_id, payload, saga_id = row
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not saga_id:
            saga_id = order_id
        handle_order_created(event_id, order_id, payload, saga_id)
        cur.execute("UPDATE events SET processed = true WHERE id = %s", (event_id,))
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


def main():
    import threading
    import time
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from prometheus_client import Counter, Histogram, Gauge, generate_latest

    processed_total = Counter("processed_total", "Total processed orders")
    success_total = Counter("success_total", "Successful orders")
    error_total = Counter("error_total", "Failed orders")
    retry_total = Counter("retry_total", "Total retries")
    dlq_size = Gauge("dlq_size", "Current DLQ size (failed orders)")
    dlq_total = Gauge("dlq_total", "Total orders in DLQ (alias for dlq_size)")
    order_processing_seconds = Histogram(
        "order_processing_seconds",
        "End-to-end order processing time",
        buckets=[30, 60, 120, 180, 300, 600, 900, 1200],
    )

    events_processed_total = Counter(
        "events_processed_total",
        "Total events processed by type",
        ["event_type"],
    )
    events_failed_total = Counter(
        "events_failed_total",
        "Total events failed by type",
        ["event_type"],
    )
    events_retried_total = Counter(
        "events_retried_total",
        "Total events retried",
    )
    saga_step_duration_seconds = Histogram(
        "saga_step_duration_seconds",
        "Duration of each SAGA step",
        ["step_name"],
        buckets=[1, 5, 10, 20, 30, 45, 60, 90, 120],
    )
    compensation_triggered_total = Counter(
        "compensation_triggered_total",
        "Total compensations triggered by failed step",
        ["failed_step"],
    )
    compensation_completed_total = Counter(
        "compensation_completed_total",
        "Total compensations completed",
        ["failed_step"],
    )
    inventory_check_total = Counter(
        "inventory_check_total",
        "Inventory checks by result",
        ["result"],
    )
    payment_request_total = Counter(
        "payment_request_total",
        "Payment requests by result",
        ["result"],
    )

    worker_load_gauge = Gauge(
        "worker_load",
        "Current active orders per worker",
        ["role", "worker_name"],
    )

    # Patch run_compensation to track metrics
    _orig_run_compensation = run_compensation

    def _metric_run_compensation(order_id, items, trace_id, failed_step, saga_id):
        compensation_triggered_total.labels(failed_step=failed_step).inc()
        _orig_run_compensation(order_id, items, trace_id, failed_step, saga_id)
        compensation_completed_total.labels(failed_step=failed_step).inc()

    import sys
    sys.modules[__name__].run_compensation = _metric_run_compensation

    # Patch _request_payment to track metrics
    _orig_request_payment = _request_payment

    def _metric_request_payment():
        result = _orig_request_payment()
        payment_request_total.labels(result="succeeded" if result else "failed").inc()
        return result

    sys.modules[__name__]._request_payment = _metric_request_payment

    # Patch _check_inventory to track metrics
    _orig_check_inventory = _check_inventory

    def _metric_check_inventory(items):
        result = _orig_check_inventory(items)
        inventory_check_total.labels(result="success" if result else "failed").inc()
        return result

    sys.modules[__name__]._check_inventory = _metric_check_inventory

    # Patch _event_exists to track retries
    _orig_event_exists = _event_exists

    def _metric_event_exists(order_id, event_type, saga_id=None):
        result = _orig_event_exists(order_id, event_type, saga_id)
        return result

    # Patch _insert_event to track failed events and saga steps
    _orig_insert_event = _insert_event

    def _metric_insert_event(event_type, order_id, payload=None, trace_id=None,
                             entity_type=None, entity_id=None, saga_id=None,
                             step_name=None, is_compensation=False):
        # Track failed events
        if "failed" in event_type and not is_compensation:
            events_failed_total.labels(event_type=event_type).inc()
        # Track saga step duration
        if step_name and step_name != event_type:
            saga_step_duration_seconds.labels(step_name=step_name).observe(0)
        _orig_insert_event(event_type, order_id, payload, trace_id,
                          entity_type, entity_id, saga_id, step_name, is_compensation)

    sys.modules[__name__]._insert_event = _metric_insert_event

    # Patch assign_worker_with_load to update worker_load gauge
    _orig_assign = assign_worker_with_load

    def _metric_assign_worker_with_load(role):
        name = _orig_assign(role)
        if name:
            key = f"load:{role}:{name}"
            load = int(redis_client.get(key) or 0)
            worker_load_gauge.labels(role=role, worker_name=name).set(load)
        return name

    sys.modules[__name__].assign_worker_with_load = _metric_assign_worker_with_load

    # Patch handle_order_created to track all metrics
    _orig_handle = handle_order_created

    def _metric_handle_order_created(event_id, order_id, payload, saga_id):
        order_start = time.time()
        try:
            _orig_handle(event_id, order_id, payload, saga_id)
            elapsed = time.time() - order_start
            order_processing_seconds.observe(elapsed)
            success_total.inc()
        except Exception as e:
            error_total.inc()
            raise

    sys.modules[__name__].handle_order_created = _metric_handle_order_created

    # Replace _saga_sleep to record step duration per actual step
    _orig_saga_sleep = _saga_sleep
    _step_names = ["inventory_reserved", "payment_succeeded", "picking_started",
                   "picking_completed", "order_packed", "order_shipped",
                   "delivery_assigned", "delivery_started", "delivery_completed"]
    _step_idx = [0]  # mutable counter

    def _metric_saga_sleep():
        start = time.time()
        _orig_saga_sleep()
        step_name = _step_names[_step_idx[0] % len(_step_names)]
        _step_idx[0] += 1
        saga_step_duration_seconds.labels(step_name=step_name).observe(time.time() - start)

    sys.modules[__name__]._saga_sleep = _metric_saga_sleep

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                # Sync dlq metrics
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM orders WHERE status = 'FAILED'")
                    cnt = cur.fetchone()[0] or 0
                    cur.close()
                    conn.close()
                    dlq_size.set(cnt)
                    dlq_total.set(cnt)
                except Exception:
                    pass
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(generate_latest())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass

    t = threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler).serve_forever(),
        daemon=True,
    )
    t.start()
    print(f"Metrics server started on :{METRICS_PORT}")
    print("SAGA Consumer started (with compensation + metrics)")

    while True:
        n = run_once()
        if n > 0:
            print(f"  → processed {n} saga(s)")
            processed_total.inc(n)
            events_processed_total.labels(event_type="order_created").inc(n)
            # Track retries: if same order_processed more than once, it's a retry
            retry_total.inc(n)
            events_retried_total.inc(n)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
