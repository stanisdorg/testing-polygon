"""GraphQL Resolvers: Queries и Mutations для FulfilBox."""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import strawberry

from dataloaders import create_dataloaders
from schema import (
    CreateOrderInput,
    Employee,
    Order,
    OrderItemInput,
    OrderResult,
    Product,
    StatusResult,
    Warehouse,
)

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")


def _get_conn():
    return psycopg2.connect(DB_URL)


def _format_product(row) -> Product:
    return Product(
        id=row["id"],
        sku=row["sku"],
        name=row["name"],
        price=float(row["price"]),
        created_at=row["created_at"].isoformat() if row["created_at"] else None,
    )


def _format_order(row) -> Order:
    return Order(
        id=row["id"],
        status=row["status"],
        total_price=float(row["total_price"]) if row["total_price"] else None,
        created_at=row["created_at"].isoformat() if row["created_at"] else None,
    )


def _format_warehouse(row) -> Warehouse:
    return Warehouse(
        id=row["id"],
        name=row["name"],
        location=row["location"],
        capacity_m3=float(row["capacity_m3"]) if row.get("capacity_m3") else None,
        created_at=row["created_at"].isoformat() if row.get("created_at") else None,
    )


def _format_employee(row) -> Employee:
    return Employee(
        id=row["id"],
        name=row["name"],
        role=row["role"],
        created_at=row["created_at"].isoformat() if row["created_at"] else None,
    )


# ── Queries ──────────────────────────────────────────────────────────


@strawberry.type
class Query:
    @strawberry.field
    async def products(
        self, page: int = 1, limit: int = 20, search: Optional[str] = None
    ) -> list[Product]:
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if search:
                cur.execute(
                    "SELECT id, sku, name, price, created_at FROM products WHERE name ILIKE %s OR sku ILIKE %s ORDER BY id ASC LIMIT %s OFFSET %s",
                    (f"%{search}%", f"%{search}%", limit, (page - 1) * limit),
                )
            else:
                cur.execute(
                    "SELECT id, sku, name, price, created_at FROM products ORDER BY id ASC LIMIT %s OFFSET %s",
                    (limit, (page - 1) * limit),
                )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return [_format_product(r) for r in rows]

    @strawberry.field
    async def product(self, id: int) -> Optional[Product]:
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT id, sku, name, price, created_at FROM products WHERE id = %s",
                (id,),
            )
            row = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        return _format_product(row) if row else None

    @strawberry.field
    async def orders(
        self, page: int = 1, limit: int = 20, status: Optional[str] = None
    ) -> list[Order]:
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if status:
                cur.execute(
                    "SELECT id, status, total_price, created_at FROM orders WHERE status = %s ORDER BY created_at DESC LIMIT %s OFFSET %s",
                    (status, limit, (page - 1) * limit),
                )
            else:
                cur.execute(
                    "SELECT id, status, total_price, created_at FROM orders ORDER BY created_at DESC LIMIT %s OFFSET %s",
                    (limit, (page - 1) * limit),
                )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return [_format_order(r) for r in rows]

    @strawberry.field
    async def order(self, id: strawberry.ID) -> Optional[Order]:
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT id, status, total_price, created_at FROM orders WHERE id = %s",
                (str(id),),
            )
            row = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        return _format_order(row) if row else None

    @strawberry.field
    async def warehouses(self) -> list[Warehouse]:
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT id, name, location, capacity_m3, created_at FROM warehouses ORDER BY id ASC"
            )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return [_format_warehouse(r) for r in rows]

    @strawberry.field
    async def employees(self, role: Optional[str] = None) -> list[Employee]:
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if role:
                cur.execute(
                    "SELECT id, name, role, created_at FROM employees WHERE role = %s ORDER BY id ASC",
                    (role,),
                )
            else:
                cur.execute(
                    "SELECT id, name, role, created_at FROM employees ORDER BY id ASC"
                )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return [_format_employee(r) for r in rows]


# ── Mutations ────────────────────────────────────────────────────────


def _get_kafka_producer():
    try:
        from kafka import KafkaProducer

        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=3000,
            retries=0,
        )
    except Exception:
        return None


@strawberry.type
class Mutation:
    @strawberry.mutation
    def create_order(self, input: CreateOrderInput) -> OrderResult:
        """Создаёт заказ через REST-совместимую логику."""
        trace_id = str(uuid.uuid4())
        conn = _get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO orders (id, status, warehouse_id) VALUES (%s, 'created', %s)",
                (input.order_id, input.warehouse_id),
            )
            # Order items
            items = input.items
            if items:
                placeholders = ", ".join(["(%s, %s, %s)"] * len(items))
                values = [val for item in items for val in (input.order_id, item.sku, item.qty)]
                cur.execute(
                    f"INSERT INTO order_items (order_id, sku, quantity) VALUES {placeholders}",
                    values,
                )
            # Event
            event = {
                "event_type": "order_created",
                "order_id": input.order_id,
                "trace_id": trace_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "items": [
                        {"sku": item.sku, "qty": item.qty} for item in items
                    ],
                    "delivery_address": input.delivery_address,
                    "warehouse_id": input.warehouse_id,
                },
            }
            cur.execute(
                "INSERT INTO events (event_type, order_id, payload, trace_id) VALUES (%s, %s, %s, %s)",
                (event["event_type"], event["order_id"], json.dumps(event), event["trace_id"]),
            )
            conn.commit()

            # Kafka (не критично)
            try:
                producer = _get_kafka_producer()
                if producer:
                    producer.send("order_events", value=event)
                    producer.flush()
            except Exception:
                pass

            return OrderResult(
                success=True,
                order=Order(
                    id=input.order_id,
                    status="created",
                    created_at=datetime.now(timezone.utc).isoformat(),
                ),
            )
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            return OrderResult(success=False, error=f"Order '{input.order_id}' already exists")
        finally:
            cur.close()
            conn.close()

    @strawberry.mutation
    def update_order_status(self, id: strawberry.ID, status: str) -> StatusResult:
        """Обновляет статус заказа."""
        valid_statuses = {"created", "PROCESSING", "COMPLETED", "CANCELLED", "FAILED"}
        if status not in valid_statuses:
            return StatusResult(success=False, error=f"Invalid status: {status}")

        conn = _get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE orders SET status = %s WHERE id = %s RETURNING id, status, total_price, created_at",
                (status, str(id)),
            )
            row = cur.fetchone()
            if row is None:
                return StatusResult(success=False, error=f"Order '{id}' not found")
            conn.commit()
            return StatusResult(
                success=True,
                order=Order(
                    id=row[0],
                    status=row[1],
                    total_price=float(row[2]) if row[2] else None,
                    created_at=row[3].isoformat() if row[3] else None,
                ),
            )
        finally:
            cur.close()
            conn.close()
