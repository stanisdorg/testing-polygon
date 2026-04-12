import json
from unittest.mock import MagicMock, patch

from simulator import generate_order, send_order, ADDRESSES


def test_generate_order_returns_valid_structure():
    """Happy path: генерация заказа с корректной структурой"""
    order = generate_order()

    assert order["order_id"].startswith("SIM-")
    assert "items" in order
    assert isinstance(order["items"], list)
    assert len(order["items"]) > 0
    assert "sku" in order["items"][0]
    assert "qty" in order["items"][0]
    assert order["delivery_address"] in ADDRESSES


def test_generate_order_uses_known_skus():
    """Edge case: SKU из известного списка"""
    for _ in range(20):
        order = generate_order()
        skus = [item["sku"] for item in order["items"]]
        for sku in skus:
            assert sku in ["SKU-1", "SKU-2", "SKU-3"]


def test_generate_order_qty_in_range():
    """Edge case: qty в диапазоне 1–3"""
    for _ in range(20):
        order = generate_order()
        for item in order["items"]:
            assert 1 <= item["qty"] <= 3


@patch("simulator.httpx")
def test_send_order_sends_correct_post(mock_httpx):
    """Integration: send_order отправляет правильный POST запрос"""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_client.post.return_value = mock_response
    mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
    mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)

    order = {
        "order_id": "SIM-TEST-001",
        "items": [{"sku": "SKU-1", "qty": 2}],
        "delivery_address": "Test Address",
    }

    send_order(order)

    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    assert call_args[0][0] == "http://localhost:8001/api/v1/orders"
    assert call_args[1]["json"] == order
