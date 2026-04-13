"""Strawberry GraphQL типы для FulfilBox."""
from __future__ import annotations

import datetime
from typing import Optional

import strawberry


@strawberry.type
class Product:
    id: int
    sku: str
    name: str
    price: float
    created_at: Optional[str] = None


@strawberry.type
class Order:
    id: str
    status: str
    total_price: Optional[float] = None
    created_at: Optional[str] = None


@strawberry.type
class Warehouse:
    id: str
    name: str
    location: str
    capacity_m3: Optional[float] = None
    created_at: Optional[str] = None


@strawberry.type
class Employee:
    id: int
    name: str
    role: str
    created_at: Optional[str] = None


# ── Input types для мутаций ─────────────────────────────────────────

@strawberry.input
class OrderItemInput:
    sku: str
    qty: int


@strawberry.input
class CreateOrderInput:
    order_id: str
    items: list[OrderItemInput]
    delivery_address: str = ""
    warehouse_id: str = "WH-MSK-S"


# ── Result types для мутаций ────────────────────────────────────────

@strawberry.type
class OrderResult:
    success: bool
    order: Optional[Order] = None
    error: Optional[str] = None


@strawberry.type
class StatusResult:
    success: bool
    order: Optional[Order] = None
    error: Optional[str] = None
