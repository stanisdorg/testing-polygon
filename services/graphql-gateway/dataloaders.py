"""DataLoaders для оптимизации N+1 запросов к БД."""
from __future__ import annotations

import os
from typing import Any

import psycopg2
import psycopg2.extras
from strawberry.dataloader import DataLoader

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://fulfilbox:fulfilbox@localhost:5432/fulfilbox",
)


def _get_conn():
    conn = psycopg2.connect(DB_URL)
    return conn


async def load_products_by_ids(keys: list[int]) -> list[Any | None]:
    """Загружает продукты по списку ID одним запросом."""
    conn = _get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, sku, name, price, created_at FROM products WHERE id = ANY(%s)",
            (list(keys),),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    product_map = {r["id"]: r for r in rows}
    return [product_map.get(k) for k in keys]


async def load_orders_by_ids(keys: list[str]) -> list[Any | None]:
    """Загружает заказы по списку ID одним запросом."""
    conn = _get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, status, total_price, created_at FROM orders WHERE id = ANY(%s)",
            (list(keys),),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    order_map = {r["id"]: r for r in rows}
    return [order_map.get(k) for k in keys]


def create_dataloaders() -> dict[str, DataLoader]:
    """Создаёт все DataLoaders для одного GraphQL запроса."""
    return {
        "product_loader": DataLoader(load_fn=load_products_by_ids),
        "order_loader": DataLoader(load_fn=load_orders_by_ids),
    }
