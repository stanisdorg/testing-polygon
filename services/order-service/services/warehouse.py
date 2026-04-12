# Фейковые остатки склада
_INVENTORY = {
    "SKU-001": 100,
    "SKU-002": 50,
    "SKU-003": 0,
}


class WarehouseService:
    """Mock Warehouse Service — заглушка для gRPC сервиса."""

    def check_availability(self, sku: str, qty: int = 1) -> bool:
        return _INVENTORY.get(sku, 0) >= qty
