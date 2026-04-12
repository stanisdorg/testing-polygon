"""Warehouse Service — читает остатки из БД (не in-memory mock)."""
import os
import threading
from typing import Optional

import psycopg2

DB_URL = os.environ.get("DATABASE_URL", "postgresql://fulfilbox:fulfilbox@postgres:5432/fulfilbox")
_db_lock = threading.Lock()


def get_db():
    """Get database connection."""
    return psycopg2.connect(DB_URL)


class WarehouseService:
    """Warehouse Service — работает с реальной БД."""

    def check_availability(self, sku: str, qty: int = 1) -> bool:
        """Check if item is available in inventory (from DB)."""
        conn = get_db()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT available_qty FROM inventory WHERE sku = %s LIMIT 1",
                (sku,),
            )
            row = cur.fetchone()
            if row is None:
                return False
            return row[0] >= qty
        finally:
            cur.close()
            conn.close()

    def reserve_stock(self, sku: str, qty: int) -> bool:
        """Reserve stock in DB. Moves available_qty to reserved_qty."""
        with _db_lock:
            conn = get_db()
            cur = conn.cursor()
            try:
                # Check availability with FOR UPDATE (row lock)
                cur.execute(
                    "SELECT available_qty FROM inventory WHERE sku = %s FOR UPDATE",
                    (sku,),
                )
                row = cur.fetchone()
                if row is None or row[0] < qty:
                    conn.rollback()
                    return False

                # Reserve: decrease available, increase reserved
                cur.execute(
                    "UPDATE inventory SET available_qty = available_qty - %s, reserved_qty = reserved_qty + %s WHERE sku = %s",
                    (qty, qty, sku),
                )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                return False
            finally:
                cur.close()
                conn.close()

    def release_stock(self, sku: str, qty: int) -> bool:
        """Release reserved stock back to available (for compensation)."""
        with _db_lock:
            conn = get_db()
            cur = conn.cursor()
            try:
                cur.execute(
                    "UPDATE inventory SET available_qty = available_qty + %s, reserved_qty = GREATEST(reserved_qty - %s, 0) WHERE sku = %s",
                    (qty, qty, sku),
                )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                return False
            finally:
                cur.close()
                conn.close()
