import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import List

import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest

app = FastAPI(
    title="FulfilBox — Order Service",
    description="API для управления заказами в системе фулфилмента. "
                "Поддерживает создание заказов, просмотр списка заказов и health check.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,  # Отключаем встроенный — используем кастомный ниже
)

# ── Prometheus Metrics ─────────────────────────────────────────────
orders_created_total = Counter("orders_created_total", "Total orders created")
orders_list_total = Counter("orders_list_total", "Total order list requests")
order_creation_seconds = Histogram(
    "order_creation_seconds",
    "Time to create an order",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
kafka_publish_total = Counter(
    "kafka_publish_total",
    "Kafka publish attempts",
    ["result"],
)

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")

# Кастомный ReDoc с фиксированной версией CDN
_REDOC_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>{title} - ReDoc</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
    <style>body {{ margin: 0; padding: 0; }}</style>
</head>
<body>
    <redoc spec-url="/openapi.json"></redoc>
    <script src="https://cdn.jsdelivr.net/npm/redoc@2.2.0/bundles/redoc.standalone.js"></script>
</body>
</html>
""".format(title="FulfilBox — Order Service")


@app.get("/redoc", include_in_schema=False)
def redoc():
    return HTMLResponse(content=_REDOC_HTML)

# Kafka producer (ленивая инициализация)
kafka_producer = None


def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        try:
            from kafka import KafkaProducer
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000,
                retries=0,
            )
        except Exception:
            kafka_producer = None  # Kafka недоступна — не критично
    return kafka_producer


class OrderItem(BaseModel):
    """Элемент заказа — товар и количество."""
    sku: str = Field(..., description="Артикул товара (например: SKU-001)")
    qty: int = Field(..., ge=1, description="Количество товара (минимум 1)")


class CreateOrderRequest(BaseModel):
    """Запрос на создание нового заказа."""
    order_id: str = Field(..., description="Уникальный идентификатор заказа (например: ORD-12345)")
    items: List[OrderItem] = Field(..., min_length=1, description="Список товаров в заказе (минимум 1)")
    delivery_address: str = Field("", description="Адрес доставки")
    warehouse_id: str = Field("WH-MSK-S", description="ID склада для обработки заказа")


def get_db():
    return psycopg2.connect(DB_URL)


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return PlainTextResponse(generate_latest())


@app.get(
    "/health",
    tags=["Health"],
    summary="Health check",
    description="Проверка работоспособности сервиса. Возвращает {\"status\": \"ok\"}.",
)
def health():
    return {"status": "ok"}


@app.get(
    "/api/v1/orders",
    tags=["Orders"],
    summary="Получить список заказов",
    description="Возвращает все заказы из базы данных (отсортированы по дате создания, newest first).",
    response_description="Список заказов с деталями",
)
def list_orders():
    orders_list_total.inc()
    conn = get_db()
    cur = conn.cursor()
    # orders использует 'id' а не 'order_id'
    cur.execute("SELECT id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 100")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {
        "orders": [
            {
                "order_id": r[0],
                "status": r[1],
                "created_at": r[2].isoformat() if r[2] else None,
            }
            for r in rows
        ]
    }


@app.post(
    "/api/v1/orders",
    tags=["Orders"],
    summary="Создать новый заказ",
    description="Создаёт новый заказ, сохраняет в PostgreSQL, генерирует trace_id и публикует событие в Kafka.",
    response_description="Статус создания, order_id и trace_id для трассировки",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "example": {
                        "order_id": "ORD-12345",
                        "items": [
                            {"sku": "SKU-001", "qty": 2},
                            {"sku": "SKU-002", "qty": 1}
                        ],
                        "delivery_address": "Москва, ул. Тестовая 1"
                    }
                }
            }
        }
    },
    status_code=201,
)
def create_order(body: CreateOrderRequest):
    start = time.time()
    trace_id = str(uuid.uuid4())
    conn = get_db()
    cur = conn.cursor()
    # Таблица использует 'id' вместо 'order_id'
    cur.execute(
        "INSERT INTO orders (id, status, warehouse_id) VALUES (%s, 'created', %s)",
        (body.order_id, body.warehouse_id),
    )
    items_json = json.dumps([i.model_dump() for i in body.items])
    cur.execute(
        "INSERT INTO order_items (order_id, sku, quantity) VALUES " + ", ".join(["(%s, %s, %s)"] * len(body.items)),
        [val for i in body.items for val in (body.order_id, i.sku, i.qty)]
    )
    event = {
        "event_type": "order_created",
        "order_id": body.order_id,
        "trace_id": trace_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "items": [i.model_dump() for i in body.items],
            "delivery_address": body.delivery_address,
            "warehouse_id": body.warehouse_id,
        },
    }
    cur.execute(
        "INSERT INTO events (event_type, order_id, payload, trace_id) VALUES (%s, %s, %s, %s)",
        (event["event_type"], event["order_id"], json.dumps(event), event["trace_id"]),
    )
    conn.commit()
    cur.close()
    conn.close()

    # Отправляем событие в Kafka (не критично — если упала, не падаем)
    try:
        producer = get_kafka_producer()
        if producer:
            producer.send("order_events", value=event)
            producer.flush()
            kafka_publish_total.labels(result="success").inc()
    except Exception:
        kafka_publish_total.labels(result="failed").inc()
        pass  # Kafka недоступна — не критично

    orders_created_total.inc()
    order_creation_seconds.observe(time.time() - start)
    return {"status": "created", "order_id": body.order_id, "trace_id": trace_id}
