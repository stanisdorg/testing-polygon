"""Simulation Service — генерирует заказы и отправляет в Order Service.

Имитирует поток реальных заказов для тестирования всей системы.
"""
import os
import random
import time

import httpx

ORDER_SERVICE_URL = os.environ.get("ORDER_SERVICE_URL", "http://localhost:8001")

# Weighted warehouse distribution (simulates real load patterns)
WAREHOUSE_WEIGHTS = {
    "WH-MSK-S": 0.5,   # 50% заказов — главный склад
    "WH-MSK-N": 0.3,   # 30% заказов
    "WH-KZN": 0.2,     # 20% заказов
}
_warehouse_list = list(WAREHOUSE_WEIGHTS.keys())
_warehouse_weights = list(WAREHOUSE_WEIGHTS.values())

SKUS = ["SKU-001", "SKU-002", "SKU-003"]
ADDRESSES = [
    "Москва, ул. Тестовая 1",
    "Москва, ул. Примерная 5",
    "Санкт-Петербург, пр. Учебный 10",
]


def _pick_warehouse() -> str:
    """Выбирает склад с учётом весов."""
    return random.choices(_warehouse_list, weights=_warehouse_weights, k=1)[0]


def generate_order() -> dict:
    """Генерирует случайный заказ."""
    order_id = f"SIM-{random.randint(10000, 99999)}"
    items = [
        {
            "sku": random.choice(SKUS),
            "qty": random.randint(1, 3),
        }
        for _ in range(random.randint(1, 2))
    ]
    return {
        "order_id": order_id,
        "items": items,
        "delivery_address": random.choice(ADDRESSES),
        "warehouse_id": _pick_warehouse(),
    }


def send_order(order: dict) -> bool:
    """Отправляет заказ в Order Service."""
    try:
        with httpx.Client() as client:
            resp = client.post(
                f"{ORDER_SERVICE_URL}/api/v1/orders",
                json=order,
                timeout=5,
            )
            if resp.status_code == 201:
                print(f"✅ Order {order['order_id']} created")
                return True
            else:
                print(f"❌ Order {order['order_id']} failed: {resp.status_code}")
                return False
    except Exception as e:
        print(f"❌ Order {order['order_id']} error: {e}")
        return False


def main():
    """Бесконечный цикл генерации заказов."""
    print("Simulation Service started")
    while True:
        order = generate_order()
        send_order(order)
        sleep_time = random.uniform(15, 30)  # 15-30 sec between orders
        print(f"  → Next order in {sleep_time:.0f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
