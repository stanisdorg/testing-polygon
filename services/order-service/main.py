import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import List

import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest

# ── Redis ─────────────────────────────────────────────────────────────
try:
    import redis
    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=1, socket_connect_timeout=1)
    redis_client.ping()
except Exception:
    redis_client = None

# ── Cache config ─────────────────────────────────────────────────────
CACHE_TTL = 60  # seconds

def _cache_get(key: str) -> str | None:
    if redis_client is None:
        return None
    try:
        return redis_client.get(key)
    except Exception:
        return None


def _cache_set(key: str, value: str, ttl: int = CACHE_TTL):
    if redis_client is None:
        return
    try:
        redis_client.set(key, value, ex=ttl)
    except Exception:
        pass


def _cache_delete(pattern: str):
    """Удаляет все ключи по pattern (для инвалидации)."""
    if redis_client is None:
        return
    try:
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)
    except Exception:
        pass

# ── Rate Limiting ─────────────────────────────────────────────────────
RATE_LIMIT = 50  # requests per window
RATE_WINDOW = 60  # seconds

def _check_rate_limit(ip: str) -> tuple[bool, int]:
    """Возвращает (allowed, remaining)."""
    if redis_client is None:
        return True, RATE_LIMIT
    try:
        key = f"rate_limit:{ip}"
        pipe = redis_client.pipeline()
        pipe.incr(key)
        pipe.expire(key, RATE_WINDOW)
        result = pipe.execute()
        count = result[0]
        if count > RATE_LIMIT:
            return False, 0
        return True, RATE_LIMIT - count
    except Exception:
        return True, RATE_LIMIT


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


# ── Product Models ─────────────────────────────────────────────────

class CreateProductRequest(BaseModel):
    """Запрос на создание нового продукта."""
    sku: str = Field(..., min_length=1, description="Артикул товара (уникальный)")
    name: str = Field(..., min_length=1, description="Название товара")
    price: float = Field(..., gt=0, description="Цена товара (должна быть > 0)")


class UpdateProductRequest(BaseModel):
    """Запрос на обновление продукта (частичное обновление)."""
    sku: str | None = Field(None, min_length=1, description="Новый артикул")
    name: str | None = Field(None, min_length=1, description="Новое название")
    price: float | None = Field(None, gt=0, description="Новая цена")


# ── Warehouse Models ───────────────────────────────────────────────

class CreateWarehouseRequest(BaseModel):
    """Запрос на создание нового склада."""
    id: str = Field(..., min_length=1, description="Уникальный ID склада (например: WH-KZN)")
    name: str = Field(..., min_length=1, description="Название склада")
    location: str = Field(..., min_length=1, description="Адрес склада")
    capacity_m3: float = Field(..., gt=0, description="Вместимость в м³ (должна быть > 0)")


class UpdateWarehouseRequest(BaseModel):
    """Запрос на обновление склада (частичное обновление)."""
    name: str | None = Field(None, min_length=1, description="Новое название")
    location: str | None = Field(None, min_length=1, description="Новый адрес")
    capacity_m3: float | None = Field(None, gt=0, description="Новая вместимость")


# ── Employee Models ────────────────────────────────────────────────

VALID_ROLES = ("picker", "packer", "courier")


class CreateEmployeeRequest(BaseModel):
    """Запрос на создание нового сотрудника."""
    name: str = Field(..., min_length=1, description="Имя сотрудника")
    role: str = Field(..., description="Роль сотрудника", pattern="^(picker|packer|courier)$")


class UpdateEmployeeRequest(BaseModel):
    """Запрос на обновление сотрудника (частичное обновление)."""
    name: str | None = Field(None, min_length=1, description="Новое имя")
    role: str | None = Field(None, description="Новая роль", pattern="^(picker|packer|courier)$")


# ── Inventory Models ───────────────────────────────────────────────

class CreateInventoryRequest(BaseModel):
    """Запрос на создание записи inventory."""
    sku: str = Field(..., min_length=1, description="Артикул товара (должен существовать в products)")
    warehouse_id: str = Field(..., min_length=1, description="ID склада (должен существовать)")
    available_qty: int = Field(..., ge=0, description="Доступное количество (>= 0)")
    reserved_qty: int = Field(0, ge=0, description="Зарезервированное количество (>= 0)")


class UpdateInventoryRequest(BaseModel):
    """Запрос на обновление inventory (частичное обновление)."""
    available_qty: int | None = Field(None, ge=0, description="Новое доступное количество")
    reserved_qty: int | None = Field(None, ge=0, description="Новое зарезервированное количество")


class StockQuantityRequest(BaseModel):
    """Запрос на изменение количества stock/reserve/release."""
    quantity: int = Field(..., gt=0, description="Количество (должно быть > 0)")


# ── Vehicle Models ─────────────────────────────────────────────────

class CreateVehicleRequest(BaseModel):
    """Запрос на создание транспортного средства."""
    plate_number: str = Field(..., min_length=1, description="Гос. номер (уникальный)")
    capacity: float = Field(..., gt=0, description="Грузоподъёмность (должна быть > 0)")


class UpdateVehicleRequest(BaseModel):
    """Запрос на обновление транспортного средства (частичное обновление)."""
    plate_number: str | None = Field(None, min_length=1, description="Новый гос. номер")
    capacity: float | None = Field(None, gt=0, description="Новая грузоподъёмность")


# ── Payment Models ─────────────────────────────────────────────────

VALID_PAYMENT_STATUSES = ("requested", "succeeded", "failed", "refunded")
DELETABLE_PAYMENT_STATUSES = ("requested", "failed")


class CreatePaymentRequest(BaseModel):
    """Запрос на создание платежа."""
    order_id: str = Field(..., min_length=1, description="ID заказа (должен существовать)")
    amount: float = Field(..., gt=0, description="Сумма платежа (должна быть > 0)")


class UpdatePaymentStatusRequest(BaseModel):
    """Запрос на обновление статуса платежа."""
    status: str = Field(..., description="Новый статус", pattern="^(requested|succeeded|failed|refunded)$")


# ── Delivery Models ────────────────────────────────────────────────

VALID_DELIVERY_STATUSES = ("assigned", "in_transit", "delivered", "cancelled")
DELETABLE_DELIVERY_STATUSES = ("assigned", "cancelled")
CANCELABLE_DELIVERY_STATUSES = ("assigned", "in_transit")


class CreateDeliveryRequest(BaseModel):
    """Запрос на создание доставки."""
    order_id: str = Field(..., min_length=1, description="ID заказа (должен существовать)")
    courier_name: str = Field(..., min_length=1, description="Имя курьера")


class UpdateDeliveryStatusRequest(BaseModel):
    """Запрос на обновление статуса доставки."""
    status: str = Field(..., description="Новый статус", pattern="^(in_transit|delivered)$")


class CancelDeliveryRequest(BaseModel):
    """Запрос на отмену доставки."""
    reason: str | None = Field(None, description="Причина отмены (опционально)")


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


# ── System Health (gRPC-based) ──────────────────────────────────────
CONSUMER_GRPC_CONFIG = {
    "inventory": 50051,
    "payment": 50052,
    "warehouse": 50053,
    "delivery": 50054,
    "saga": 50055,
}


def _check_consumer_grpc(name: str, port: int) -> str:
    """Проверяет статус консьюмера через gRPC. Graceful degradation."""
    try:
        import grpc
        # Add local generated proto path
        local_proto = os.path.join(os.path.dirname(__file__), "shared_grpc")
        if local_proto not in sys.path:
            sys.path.insert(0, local_proto)
        import health_pb2
        import health_pb2_grpc

        channel = grpc.insecure_channel(f"localhost:{port}")
        stub = health_pb2_grpc.HealthStub(channel)
        request = health_pb2.HealthCheckRequest(service_name=name)
        response = stub.Check(request, timeout=2)
        channel.close()

        status_map = {
            0: "UNKNOWN",
            1: "SERVING",
            2: "NOT_SERVING",
        }
        return status_map.get(response.status, "UNKNOWN")
    except grpc.RpcError:
        return "UNAVAILABLE"
    except Exception:
        return "UNAVAILABLE"


@app.get(
    "/api/v1/system/health",
    tags=["System"],
    summary="gRPC Health Check всех консьюмеров",
    description="Делает gRPC запросы ко всем консьюмерам и возвращает их статусы.",
)
def system_health():
    """Проверяет здоровье всех консьюмеров через gRPC."""
    results = {}
    for name, port in CONSUMER_GRPC_CONFIG.items():
        results[name] = _check_consumer_grpc(name, port)
    return results


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
def create_order(body: CreateOrderRequest, request: Request):
    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    allowed, remaining = _check_rate_limit(client_ip)
    if not allowed:
        from fastapi import HTTPException
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Try again in {RATE_WINDOW} seconds.")

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
        "INSERT INTO order_items (order_id, sku, qty) VALUES " + ", ".join(["(%s, %s, %s)"] * len(body.items)),
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


# ═══════════════════════════════════════════════════════════════════
# Products CRUD
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/products",
    tags=["Products"],
    summary="Создать новый продукт",
    description="Создаёт новый продукт в каталоге с уникальным SKU, названием и ценой.",
    response_description="Созданный продукт",
    status_code=201,
)
def create_product(body: CreateProductRequest):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO products (sku, name, price) VALUES (%s, %s, %s) RETURNING id, sku, name, price, created_at",
            (body.sku, body.name, body.price),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "id": row[0],
            "sku": row[1],
            "name": row[2],
            "price": float(row[3]),
            "created_at": row[4].isoformat() if row[4] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail=f"Product with sku '{body.sku}' already exists")
    finally:
        cur.close()
        conn.close()


@app.get(
    "/api/v1/products",
    tags=["Products"],
    summary="Список продуктов",
    description="Возвращает список продуктов с пагинацией и поиском по name/sku.",
    response_description="Список продуктов с метаданными пагинации",
)
def list_products(page: int = 1, limit: int = 20, search: str = ""):
    conn = get_db()
    cur = conn.cursor()

    # Query for total count
    if search:
        search_filter = "WHERE name ILIKE %s OR sku ILIKE %s"
        search_param = f"%{search}%"
        cur.execute(f"SELECT COUNT(*) FROM products {search_filter}", (search_param, search_param))
    else:
        cur.execute("SELECT COUNT(*) FROM products")
    total = cur.fetchone()[0]

    # Query for products
    offset = (page - 1) * limit
    if search:
        cur.execute(
            f"SELECT id, sku, name, price, created_at FROM products {search_filter} ORDER BY id ASC LIMIT %s OFFSET %s",
            (search_param, search_param, limit, offset),
        )
    else:
        cur.execute(
            "SELECT id, sku, name, price, created_at FROM products ORDER BY id ASC LIMIT %s OFFSET %s",
            (limit, offset),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "products": [
            {
                "id": r[0],
                "sku": r[1],
                "name": r[2],
                "price": float(r[3]),
                "created_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/products/{product_id}",
    tags=["Products"],
    summary="Получить продукт по ID",
    description="Возвращает продукт по его внутреннему ID.",
    response_description="Продукт",
)
def get_product(product_id: int, request: Request):
    # Cache check
    cache_key = f"cache:product:{product_id}"
    cached = _cache_get(cache_key)
    if cached:
        response = JSONResponse(content=json.loads(cached))
        response.headers["X-Cache"] = "HIT"
        return response

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, sku, name, price, created_at FROM products WHERE id = %s",
        (product_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Product with id {product_id} not found")

    result = {
        "id": row[0],
        "sku": row[1],
        "name": row[2],
        "price": float(row[3]),
        "created_at": row[4].isoformat() if row[4] else None,
    }
    _cache_set(cache_key, json.dumps(result))

    response = JSONResponse(content=result)
    response.headers["X-Cache"] = "MISS"
    return response


@app.put(
    "/api/v1/products/{product_id}",
    tags=["Products"],
    summary="Обновить продукт",
    description="Частичное обновление продукта. Можно передать любое подмножество полей.",
    response_description="Обновлённый продукт",
)
def update_product(product_id: int, body: UpdateProductRequest):
    # Build dynamic UPDATE query
    updates = {}
    if body.sku is not None:
        updates["sku"] = body.sku
    if body.name is not None:
        updates["name"] = body.name
    if body.price is not None:
        updates["price"] = body.price

    if not updates:
        # Nothing to update — just return current state
        return get_product(product_id)

    set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
    values = list(updates.values())
    values.append(product_id)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE products SET {set_clause} WHERE id = %s RETURNING id, sku, name, price, created_at",
            values,
        )
        row = cur.fetchone()
        if row is None:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"Product with id {product_id} not found")
        conn.commit()
        result = {
            "id": row[0],
            "sku": row[1],
            "name": row[2],
            "price": float(row[3]),
            "created_at": row[4].isoformat() if row[4] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail="Product with this sku already exists")
    finally:
        cur.close()
        conn.close()

    # Invalidate cache after successful update
    _cache_delete("cache:product:*")
    return result


@app.delete(
    "/api/v1/products/{product_id}",
    tags=["Products"],
    summary="Удалить продукт",
    description="Удаляет продукт из каталога.",
    status_code=204,
)
def delete_product(product_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM products WHERE id = %s", (product_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Product with id {product_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    _cache_delete("cache:product:*")
    return None


# ═══════════════════════════════════════════════════════════════════
# Warehouses CRUD
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/warehouses",
    tags=["Warehouses"],
    summary="Создать новый склад",
    description="Создаёт новый склад с уникальным ID, названием, адресом и вместимостью.",
    response_description="Созданный склад",
    status_code=201,
)
def create_warehouse(body: CreateWarehouseRequest):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO warehouses (id, name, location, capacity_m3) VALUES (%s, %s, %s, %s) RETURNING id, name, location, capacity_m3, created_at",
            (body.id, body.name, body.location, body.capacity_m3),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "id": row[0],
            "name": row[1],
            "location": row[2],
            "capacity_m3": float(row[3]),
            "created_at": row[4].isoformat() if row[4] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail=f"Warehouse with id '{body.id}' already exists")
    finally:
        cur.close()
        conn.close()


@app.get(
    "/api/v1/warehouses",
    tags=["Warehouses"],
    summary="Список складов",
    description="Возвращает список складов с пагинацией и поиском по name/location.",
    response_description="Список складов с метаданными пагинации",
)
def list_warehouses(page: int = 1, limit: int = 20, search: str = ""):
    conn = get_db()
    cur = conn.cursor()

    if search:
        search_filter = "WHERE name ILIKE %s OR location ILIKE %s"
        search_param = f"%{search}%"
        cur.execute(f"SELECT COUNT(*) FROM warehouses {search_filter}", (search_param, search_param))
    else:
        cur.execute("SELECT COUNT(*) FROM warehouses")
    total = cur.fetchone()[0]

    offset = (page - 1) * limit
    if search:
        cur.execute(
            f"SELECT id, name, location, capacity_m3, created_at FROM warehouses {search_filter} ORDER BY id ASC LIMIT %s OFFSET %s",
            (search_param, search_param, limit, offset),
        )
    else:
        cur.execute(
            "SELECT id, name, location, capacity_m3, created_at FROM warehouses ORDER BY id ASC LIMIT %s OFFSET %s",
            (limit, offset),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "warehouses": [
            {
                "id": r[0],
                "name": r[1],
                "location": r[2],
                "capacity_m3": float(r[3]),
                "created_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/warehouses/{warehouse_id}",
    tags=["Warehouses"],
    summary="Получить склад по ID",
    description="Возвращает склад по его ID.",
    response_description="Склад",
)
def get_warehouse(warehouse_id: str):
    cache_key = f"cache:warehouse:{warehouse_id}"
    cached = _cache_get(cache_key)
    if cached:
        response = JSONResponse(content=json.loads(cached))
        response.headers["X-Cache"] = "HIT"
        return response

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, location, capacity_m3, created_at FROM warehouses WHERE id = %s",
        (warehouse_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Warehouse with id '{warehouse_id}' not found")

    result = {
        "id": row[0],
        "name": row[1],
        "location": row[2],
        "capacity_m3": float(row[3]),
        "created_at": row[4].isoformat() if row[4] else None,
    }
    _cache_set(cache_key, json.dumps(result))
    response = JSONResponse(content=result)
    response.headers["X-Cache"] = "MISS"
    return response


@app.put(
    "/api/v1/warehouses/{warehouse_id}",
    tags=["Warehouses"],
    summary="Обновить склад",
    description="Частичное обновление склада. Можно передать любое подмножество полей.",
    response_description="Обновлённый склад",
)
def update_warehouse(warehouse_id: str, body: UpdateWarehouseRequest):
    updates = {}
    if body.name is not None:
        updates["name"] = body.name
    if body.location is not None:
        updates["location"] = body.location
    if body.capacity_m3 is not None:
        updates["capacity_m3"] = body.capacity_m3

    if not updates:
        return get_warehouse(warehouse_id)

    set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
    values = list(updates.values())
    values.append(warehouse_id)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE warehouses SET {set_clause} WHERE id = %s RETURNING id, name, location, capacity_m3, created_at",
            values,
        )
        row = cur.fetchone()
        if row is None:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"Warehouse with id '{warehouse_id}' not found")
        conn.commit()
        return {
            "id": row[0],
            "name": row[1],
            "location": row[2],
            "capacity_m3": float(row[3]),
            "created_at": row[4].isoformat() if row[4] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail="Warehouse with this id already exists")
    finally:
        cur.close()
        conn.close()


@app.delete(
    "/api/v1/warehouses/{warehouse_id}",
    tags=["Warehouses"],
    summary="Удалить склад",
    description="Удаляет склад. Нельзя удалить если есть заказы на этом складе.",
    status_code=204,
)
def delete_warehouse(warehouse_id: str):
    conn = get_db()
    cur = conn.cursor()

    # Check if orders exist on this warehouse
    cur.execute("SELECT COUNT(*) FROM orders WHERE warehouse_id = %s", (warehouse_id,))
    order_count = cur.fetchone()[0]
    if order_count > 0:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete warehouse '{warehouse_id}': {order_count} order(s) exist on this warehouse",
        )

    cur.execute("DELETE FROM warehouses WHERE id = %s", (warehouse_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Warehouse with id '{warehouse_id}' not found")
    conn.commit()
    cur.close()
    conn.close()
    _cache_delete("cache:warehouse:*")
    return None


# ═══════════════════════════════════════════════════════════════════
# Employees CRUD
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/employees",
    tags=["Employees"],
    summary="Создать нового сотрудника",
    description="Создаёт сотрудника с именем и ролью (picker/packer/courier).",
    response_description="Созданный сотрудник",
    status_code=201,
)
def create_employee(body: CreateEmployeeRequest):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO employees (name, role) VALUES (%s, %s) RETURNING id, name, role, created_at",
        (body.name, body.role),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "name": row[1],
        "role": row[2],
        "created_at": row[3].isoformat() if row[3] else None,
    }


@app.get(
    "/api/v1/employees",
    tags=["Employees"],
    summary="Список сотрудников",
    description="Возвращает список сотрудников с пагинацией и фильтром по role.",
    response_description="Список сотрудников с метаданными пагинации",
)
def list_employees(page: int = 1, limit: int = 20, role: str = ""):
    conn = get_db()
    cur = conn.cursor()

    if role:
        role_filter = "WHERE role = %s"
        cur.execute(f"SELECT COUNT(*) FROM employees {role_filter}", (role,))
    else:
        cur.execute("SELECT COUNT(*) FROM employees")
    total = cur.fetchone()[0]

    offset = (page - 1) * limit
    if role:
        cur.execute(
            f"SELECT id, name, role, created_at FROM employees {role_filter} ORDER BY id ASC LIMIT %s OFFSET %s",
            (role, limit, offset),
        )
    else:
        cur.execute(
            "SELECT id, name, role, created_at FROM employees ORDER BY id ASC LIMIT %s OFFSET %s",
            (limit, offset),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "employees": [
            {
                "id": r[0],
                "name": r[1],
                "role": r[2],
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/employees/{employee_id}",
    tags=["Employees"],
    summary="Получить сотрудника по ID",
    description="Возвращает сотрудника по его ID.",
    response_description="Сотрудник",
)
def get_employee(employee_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, role, created_at FROM employees WHERE id = %s",
        (employee_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Employee with id {employee_id} not found")

    return {
        "id": row[0],
        "name": row[1],
        "role": row[2],
        "created_at": row[3].isoformat() if row[3] else None,
    }


@app.put(
    "/api/v1/employees/{employee_id}",
    tags=["Employees"],
    summary="Обновить сотрудника",
    description="Частичное обновление сотрудника. Можно передать любое подмножество полей.",
    response_description="Обновлённый сотрудник",
)
def update_employee(employee_id: int, body: UpdateEmployeeRequest):
    updates = {}
    if body.name is not None:
        updates["name"] = body.name
    if body.role is not None:
        updates["role"] = body.role

    if not updates:
        return get_employee(employee_id)

    set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
    values = list(updates.values())
    values.append(employee_id)

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE employees SET {set_clause} WHERE id = %s RETURNING id, name, role, created_at",
        values,
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Employee with id {employee_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "name": row[1],
        "role": row[2],
        "created_at": row[3].isoformat() if row[3] else None,
    }


@app.delete(
    "/api/v1/employees/{employee_id}",
    tags=["Employees"],
    summary="Удалить сотрудника",
    description="Удаляет сотрудника из системы.",
    status_code=204,
)
def delete_employee(employee_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM employees WHERE id = %s", (employee_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Employee with id {employee_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return None


# ═══════════════════════════════════════════════════════════════════
# Inventory CRUD + Stock Operations
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/inventory",
    tags=["Inventory"],
    summary="Создать запись inventory",
    description="Создаёт запись наличия товара на складе. SKU должен существовать в products, warehouse_id — в warehouses.",
    response_description="Созданная запись inventory",
    status_code=201,
)
def create_inventory(body: CreateInventoryRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    # Validate product exists
    cur.execute("SELECT 1 FROM products WHERE sku = %s", (body.sku,))
    if cur.fetchone() is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Product with sku '{body.sku}' not found")

    # Validate warehouse exists
    cur.execute("SELECT 1 FROM warehouses WHERE id = %s", (body.warehouse_id,))
    if cur.fetchone() is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Warehouse with id '{body.warehouse_id}' not found")

    try:
        cur.execute(
            "INSERT INTO inventory (sku, warehouse_id, available_qty, reserved_qty) VALUES (%s, %s, %s, %s) RETURNING id, sku, warehouse_id, available_qty, reserved_qty, created_at",
            (body.sku, body.warehouse_id, body.available_qty, body.reserved_qty),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "id": row[0],
            "sku": row[1],
            "warehouse_id": row[2],
            "available_qty": row[3],
            "reserved_qty": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(status_code=409, detail=f"Inventory entry for sku '{body.sku}' on warehouse '{body.warehouse_id}' already exists")
    finally:
        cur.close()
        conn.close()


@app.get(
    "/api/v1/inventory",
    tags=["Inventory"],
    summary="Список записей inventory",
    description="Возвращает список записей наличия товаров с пагинацией и фильтрами.",
    response_description="Список записей inventory с метаданными пагинации",
)
def list_inventory(page: int = 1, limit: int = 20, sku: str = "", warehouse_id: str = ""):
    conn = get_db()
    cur = conn.cursor()

    conditions = []
    params = []
    if sku:
        conditions.append("sku = %s")
        params.append(sku)
    if warehouse_id:
        conditions.append("warehouse_id = %s")
        params.append(warehouse_id)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    cur.execute(f"SELECT COUNT(*) FROM inventory{where_clause}", params)
    total = cur.fetchone()[0]

    offset = (page - 1) * limit
    cur.execute(
        f"SELECT id, sku, warehouse_id, available_qty, reserved_qty, created_at FROM inventory{where_clause} ORDER BY id ASC LIMIT %s OFFSET %s",
        params + [limit, offset],
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "inventory": [
            {
                "id": r[0],
                "sku": r[1],
                "warehouse_id": r[2],
                "available_qty": r[3],
                "reserved_qty": r[4],
                "created_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/inventory/{inventory_id}",
    tags=["Inventory"],
    summary="Получить запись inventory по ID",
    description="Возвращает запись наличия товара по ID.",
    response_description="Запись inventory",
)
def get_inventory(inventory_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, sku, warehouse_id, available_qty, reserved_qty, created_at FROM inventory WHERE id = %s",
        (inventory_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Inventory entry with id {inventory_id} not found")

    return {
        "id": row[0],
        "sku": row[1],
        "warehouse_id": row[2],
        "available_qty": row[3],
        "reserved_qty": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.put(
    "/api/v1/inventory/{inventory_id}",
    tags=["Inventory"],
    summary="Обновить запись inventory",
    description="Частичное обновление количества товаров.",
    response_description="Обновлённая запись inventory",
)
def update_inventory(inventory_id: int, body: UpdateInventoryRequest):
    updates = {}
    if body.available_qty is not None:
        updates["available_qty"] = body.available_qty
    if body.reserved_qty is not None:
        updates["reserved_qty"] = body.reserved_qty

    if not updates:
        return get_inventory(inventory_id)

    set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
    values = list(updates.values())
    values.append(inventory_id)

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE inventory SET {set_clause} WHERE id = %s RETURNING id, sku, warehouse_id, available_qty, reserved_qty, created_at",
        values,
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Inventory entry with id {inventory_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "sku": row[1],
        "warehouse_id": row[2],
        "available_qty": row[3],
        "reserved_qty": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.delete(
    "/api/v1/inventory/{inventory_id}",
    tags=["Inventory"],
    summary="Удалить запись inventory",
    description="Удаляет запись. Нельзя удалить если есть зарезервированные товары.",
    status_code=204,
)
def delete_inventory(inventory_id: int):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT reserved_qty FROM inventory WHERE id = %s", (inventory_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Inventory entry with id {inventory_id} not found")
    if row[0] > 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail=f"Cannot delete inventory: {row[0]} items are reserved")

    cur.execute("DELETE FROM inventory WHERE id = %s", (inventory_id,))
    conn.commit()
    cur.close()
    conn.close()
    return None


@app.post(
    "/api/v1/inventory/{inventory_id}/add-stock",
    tags=["Inventory"],
    summary="Добавить товар на склад",
    description="Увеличивает available_qty на указанное количество.",
    response_description="Обновлённая запись inventory",
)
def add_stock(inventory_id: int, body: StockQuantityRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE inventory SET available_qty = available_qty + %s WHERE id = %s RETURNING id, sku, warehouse_id, available_qty, reserved_qty, created_at",
        (body.quantity, inventory_id),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Inventory entry with id {inventory_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "sku": row[1],
        "warehouse_id": row[2],
        "available_qty": row[3],
        "reserved_qty": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.post(
    "/api/v1/inventory/{inventory_id}/reserve",
    tags=["Inventory"],
    summary="Зарезервировать товар",
    description="Перемещает количество из available_qty в reserved_qty.",
    response_description="Обновлённая запись inventory",
)
def reserve_stock(inventory_id: int, body: StockQuantityRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT available_qty, reserved_qty FROM inventory WHERE id = %s",
        (inventory_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Inventory entry with id {inventory_id} not found")

    if row[0] < body.quantity:
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail=f"Not enough stock: available={row[0]}, requested={body.quantity}")

    cur.execute(
        "UPDATE inventory SET available_qty = available_qty - %s, reserved_qty = reserved_qty + %s WHERE id = %s RETURNING id, sku, warehouse_id, available_qty, reserved_qty, created_at",
        (body.quantity, body.quantity, inventory_id),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "sku": row[1],
        "warehouse_id": row[2],
        "available_qty": row[3],
        "reserved_qty": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.post(
    "/api/v1/inventory/{inventory_id}/release",
    tags=["Inventory"],
    summary="Освободить резерв",
    description="Перемещает количество из reserved_qty обратно в available_qty.",
    response_description="Обновлённая запись inventory",
)
def release_stock(inventory_id: int, body: StockQuantityRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT available_qty, reserved_qty FROM inventory WHERE id = %s",
        (inventory_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Inventory entry with id {inventory_id} not found")

    if row[1] < body.quantity:
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail=f"Not enough reserved: reserved={row[1]}, requested={body.quantity}")

    cur.execute(
        "UPDATE inventory SET available_qty = available_qty + %s, reserved_qty = reserved_qty - %s WHERE id = %s RETURNING id, sku, warehouse_id, available_qty, reserved_qty, created_at",
        (body.quantity, body.quantity, inventory_id),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "sku": row[1],
        "warehouse_id": row[2],
        "available_qty": row[3],
        "reserved_qty": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.get(
    "/api/v1/inventory/sku/{sku}/available",
    tags=["Inventory"],
    summary="Доступное количество SKU на всех складах",
    description="Возвращает наличие товара по артикулу на каждом складе.",
    response_description="Список складов с количеством товара",
)
def get_sku_available(sku: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT warehouse_id, available_qty, reserved_qty FROM inventory WHERE sku = %s ORDER BY warehouse_id",
        (sku,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "warehouse_id": r[0],
            "available_qty": r[1],
            "reserved_qty": r[2],
        }
        for r in rows
    ]


# ═══════════════════════════════════════════════════════════════════
# Vehicles CRUD
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/vehicles",
    tags=["Vehicles"],
    summary="Создать транспортное средство",
    description="Создаёт ТС с уникальным гос. номером и грузоподъёмностью.",
    response_description="Созданное транспортное средство",
    status_code=201,
)
def create_vehicle(body: CreateVehicleRequest):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO vehicles (plate_number, capacity) VALUES (%s, %s) RETURNING id, plate_number, capacity, created_at",
            (body.plate_number, body.capacity),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "id": row[0],
            "plate_number": row[1],
            "capacity": float(row[2]),
            "created_at": row[3].isoformat() if row[3] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail=f"Vehicle with plate_number '{body.plate_number}' already exists")
    finally:
        cur.close()
        conn.close()


@app.get(
    "/api/v1/vehicles",
    tags=["Vehicles"],
    summary="Список транспортных средств",
    description="Возвращает список ТС с пагинацией и фильтром по min_capacity.",
    response_description="Список ТС с метаданными пагинации",
)
def list_vehicles(page: int = 1, limit: int = 20, min_capacity: float = 0):
    conn = get_db()
    cur = conn.cursor()

    if min_capacity > 0:
        where_clause = "WHERE capacity >= %s"
        cur.execute(f"SELECT COUNT(*) FROM vehicles {where_clause}", (min_capacity,))
    else:
        cur.execute("SELECT COUNT(*) FROM vehicles")
    total = cur.fetchone()[0]

    offset = (page - 1) * limit
    if min_capacity > 0:
        cur.execute(
            f"SELECT id, plate_number, capacity, created_at FROM vehicles {where_clause} ORDER BY id ASC LIMIT %s OFFSET %s",
            (min_capacity, limit, offset),
        )
    else:
        cur.execute(
            "SELECT id, plate_number, capacity, created_at FROM vehicles ORDER BY id ASC LIMIT %s OFFSET %s",
            (limit, offset),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "vehicles": [
            {
                "id": r[0],
                "plate_number": r[1],
                "capacity": float(r[2]),
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/vehicles/{vehicle_id}",
    tags=["Vehicles"],
    summary="Получить ТС по ID",
    description="Возвращает транспортное средство по его ID.",
    response_description="Транспортное средство",
)
def get_vehicle(vehicle_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, plate_number, capacity, created_at FROM vehicles WHERE id = %s",
        (vehicle_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Vehicle with id {vehicle_id} not found")

    return {
        "id": row[0],
        "plate_number": row[1],
        "capacity": float(row[2]),
        "created_at": row[3].isoformat() if row[3] else None,
    }


@app.put(
    "/api/v1/vehicles/{vehicle_id}",
    tags=["Vehicles"],
    summary="Обновить транспортное средство",
    description="Частичное обновление ТС. Можно передать любое подмножество полей.",
    response_description="Обновлённое транспортное средство",
)
def update_vehicle(vehicle_id: int, body: UpdateVehicleRequest):
    updates = {}
    if body.plate_number is not None:
        updates["plate_number"] = body.plate_number
    if body.capacity is not None:
        updates["capacity"] = body.capacity

    if not updates:
        return get_vehicle(vehicle_id)

    set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
    values = list(updates.values())
    values.append(vehicle_id)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE vehicles SET {set_clause} WHERE id = %s RETURNING id, plate_number, capacity, created_at",
            values,
        )
        row = cur.fetchone()
        if row is None:
            cur.close()
            conn.close()
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"Vehicle with id {vehicle_id} not found")
        conn.commit()
        return {
            "id": row[0],
            "plate_number": row[1],
            "capacity": float(row[2]),
            "created_at": row[3].isoformat() if row[3] else None,
        }
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail="Vehicle with this plate_number already exists")
    finally:
        cur.close()
        conn.close()


@app.delete(
    "/api/v1/vehicles/{vehicle_id}",
    tags=["Vehicles"],
    summary="Удалить транспортное средство",
    description="Удаляет транспортное средство из системы.",
    status_code=204,
)
def delete_vehicle(vehicle_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (vehicle_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Vehicle with id {vehicle_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return None


# ═══════════════════════════════════════════════════════════════════
# Payments CRUD
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/payments",
    tags=["Payments"],
    summary="Создать платёж",
    description="Создаёт платёж для заказа. Статус автоматически устанавливается в 'requested'.",
    response_description="Созданный платёж",
    status_code=201,
)
def create_payment(body: CreatePaymentRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    # Validate order exists
    cur.execute("SELECT 1 FROM orders WHERE id = %s", (body.order_id,))
    if cur.fetchone() is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Order with id '{body.order_id}' not found")

    cur.execute(
        "INSERT INTO payments (order_id, amount, status) VALUES (%s, %s, 'requested') RETURNING id, order_id, amount, status, created_at",
        (body.order_id, body.amount),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "order_id": row[1],
        "amount": float(row[2]),
        "status": row[3],
        "created_at": row[4].isoformat() if row[4] else None,
    }


@app.get(
    "/api/v1/payments",
    tags=["Payments"],
    summary="Список платежей",
    description="Возвращает список платежей с пагинацией и фильтрами.",
    response_description="Список платежей с метаданными пагинации",
)
def list_payments(page: int = 1, limit: int = 20, order_id: str = "", status: str = ""):
    conn = get_db()
    cur = conn.cursor()

    conditions = []
    params = []
    if order_id:
        conditions.append("order_id = %s")
        params.append(order_id)
    if status:
        conditions.append("status = %s")
        params.append(status)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    cur.execute(f"SELECT COUNT(*) FROM payments{where_clause}", params)
    total = cur.fetchone()[0]

    offset = (page - 1) * limit
    cur.execute(
        f"SELECT id, order_id, amount, status, created_at FROM payments{where_clause} ORDER BY id ASC LIMIT %s OFFSET %s",
        params + [limit, offset],
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "payments": [
            {
                "id": r[0],
                "order_id": r[1],
                "amount": float(r[2]),
                "status": r[3],
                "created_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/payments/{payment_id}",
    tags=["Payments"],
    summary="Получить платёж по ID",
    description="Возвращает платёж по его ID.",
    response_description="Платёж",
)
def get_payment(payment_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, order_id, amount, status, created_at FROM payments WHERE id = %s",
        (payment_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Payment with id {payment_id} not found")

    return {
        "id": row[0],
        "order_id": row[1],
        "amount": float(row[2]),
        "status": row[3],
        "created_at": row[4].isoformat() if row[4] else None,
    }


@app.put(
    "/api/v1/payments/{payment_id}/status",
    tags=["Payments"],
    summary="Обновить статус платежа",
    description="Обновляет статус платежа (requested/succeeded/failed/refunded).",
    response_description="Обновлённый платёж",
)
def update_payment_status(payment_id: int, body: UpdatePaymentStatusRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE payments SET status = %s WHERE id = %s RETURNING id, order_id, amount, status, created_at",
        (body.status, payment_id),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Payment with id {payment_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "order_id": row[1],
        "amount": float(row[2]),
        "status": row[3],
        "created_at": row[4].isoformat() if row[4] else None,
    }


@app.delete(
    "/api/v1/payments/{payment_id}",
    tags=["Payments"],
    summary="Удалить платёж",
    description="Удаляет платёж. Нельзя удалить если статус 'succeeded' или 'refunded'.",
    status_code=204,
)
def delete_payment(payment_id: int):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT status FROM payments WHERE id = %s", (payment_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Payment with id {payment_id} not found")
    if row[0] not in DELETABLE_PAYMENT_STATUSES:
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail=f"Cannot delete payment with status '{row[0]}': only 'requested' or 'failed' payments can be deleted")

    cur.execute("DELETE FROM payments WHERE id = %s", (payment_id,))
    conn.commit()
    cur.close()
    conn.close()
    return None


# ═══════════════════════════════════════════════════════════════════
# Deliveries CRUD
# ═══════════════════════════════════════════════════════════════════

@app.post(
    "/api/v1/deliveries",
    tags=["Deliveries"],
    summary="Создать доставку",
    description="Создаёт доставку для заказа. Статус автоматически устанавливается в 'assigned'.",
    response_description="Созданная доставка",
    status_code=201,
)
def create_delivery(body: CreateDeliveryRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    # Validate order exists
    cur.execute("SELECT 1 FROM orders WHERE id = %s", (body.order_id,))
    if cur.fetchone() is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Order with id '{body.order_id}' not found")

    cur.execute(
        "INSERT INTO deliveries (order_id, courier_name, status, cancelled) VALUES (%s, %s, 'assigned', false) RETURNING id, order_id, courier_name, status, cancelled, created_at",
        (body.order_id, body.courier_name),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "order_id": row[1],
        "courier_name": row[2],
        "status": row[3],
        "cancelled": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.get(
    "/api/v1/deliveries",
    tags=["Deliveries"],
    summary="Список доставок",
    description="Возвращает список доставок с пагинацией и фильтрами.",
    response_description="Список доставок с метаданными пагинации",
)
def list_deliveries(page: int = 1, limit: int = 20, order_id: str = "", courier_name: str = "", status: str = ""):
    conn = get_db()
    cur = conn.cursor()

    conditions = []
    params = []
    if order_id:
        conditions.append("order_id = %s")
        params.append(order_id)
    if courier_name:
        conditions.append("courier_name = %s")
        params.append(courier_name)
    if status:
        conditions.append("status = %s")
        params.append(status)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    cur.execute(f"SELECT COUNT(*) FROM deliveries{where_clause}", params)
    total = cur.fetchone()[0]

    offset = (page - 1) * limit
    cur.execute(
        f"SELECT id, order_id, courier_name, status, cancelled, created_at FROM deliveries{where_clause} ORDER BY id ASC LIMIT %s OFFSET %s",
        params + [limit, offset],
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "deliveries": [
            {
                "id": r[0],
                "order_id": r[1],
                "courier_name": r[2],
                "status": r[3],
                "cancelled": r[4],
                "created_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get(
    "/api/v1/deliveries/{delivery_id}",
    tags=["Deliveries"],
    summary="Получить доставку по ID",
    description="Возвращает доставку по её ID.",
    response_description="Доставка",
)
def get_delivery(delivery_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, order_id, courier_name, status, cancelled, created_at FROM deliveries WHERE id = %s",
        (delivery_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Delivery with id {delivery_id} not found")

    return {
        "id": row[0],
        "order_id": row[1],
        "courier_name": row[2],
        "status": row[3],
        "cancelled": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.put(
    "/api/v1/deliveries/{delivery_id}/status",
    tags=["Deliveries"],
    summary="Обновить статус доставки",
    description="Обновляет статус доставки (in_transit/delivered).",
    response_description="Обновлённая доставка",
)
def update_delivery_status(delivery_id: int, body: UpdateDeliveryStatusRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE deliveries SET status = %s WHERE id = %s RETURNING id, order_id, courier_name, status, cancelled, created_at",
        (body.status, delivery_id),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Delivery with id {delivery_id} not found")
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "order_id": row[1],
        "courier_name": row[2],
        "status": row[3],
        "cancelled": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.post(
    "/api/v1/deliveries/{delivery_id}/cancel",
    tags=["Deliveries"],
    summary="Отменить доставку",
    description="Отменяет доставку. Нельзя отменить если статус 'delivered' или уже 'cancelled'.",
    response_description="Отменённая доставка",
)
def cancel_delivery(delivery_id: int, body: CancelDeliveryRequest):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT status, cancelled FROM deliveries WHERE id = %s", (delivery_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Delivery with id {delivery_id} not found")
    if row[0] not in CANCELABLE_DELIVERY_STATUSES or row[1]:
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail=f"Cannot cancel delivery with status '{row[0]}' and cancelled={row[1]}")

    cur.execute(
        "UPDATE deliveries SET cancelled = true, status = 'cancelled' WHERE id = %s RETURNING id, order_id, courier_name, status, cancelled, created_at",
        (delivery_id,),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return {
        "id": row[0],
        "order_id": row[1],
        "courier_name": row[2],
        "status": row[3],
        "cancelled": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
    }


@app.delete(
    "/api/v1/deliveries/{delivery_id}",
    tags=["Deliveries"],
    summary="Удалить доставку",
    description="Удаляет доставку. Можно удалить только если статус 'assigned' или 'cancelled'.",
    status_code=204,
)
def delete_delivery(delivery_id: int):
    from fastapi import HTTPException

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT status FROM deliveries WHERE id = %s", (delivery_id,))
    row = cur.fetchone()
    if row is None:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Delivery with id {delivery_id} not found")
    if row[0] not in DELETABLE_DELIVERY_STATUSES:
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail=f"Cannot delete delivery with status '{row[0]}': only 'assigned' or 'cancelled' deliveries can be deleted")

    cur.execute("DELETE FROM deliveries WHERE id = %s", (delivery_id,))
    conn.commit()
    cur.close()
    conn.close()
    return None
