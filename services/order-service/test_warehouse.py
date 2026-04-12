from services.warehouse import WarehouseService


def test_check_availability_returns_true():
    """Happy path: товар доступен"""
    service = WarehouseService()
    result = service.check_availability("SKU-001", qty=1)
    assert result is True


def test_check_availability_returns_false():
    """Edge case: товар отсутствует"""
    service = WarehouseService()
    result = service.check_availability("SKU-999", qty=1)
    assert result is False


def test_check_availability_insufficient_qty():
    """Edge case: товара мало"""
    service = WarehouseService()
    # SKU-001 имеет 100 шт, запрашиваем 200
    result = service.check_availability("SKU-001", qty=200)
    assert result is False
