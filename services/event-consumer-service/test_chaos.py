"""Тесты chaos engineering — управляемые сбои в event-consumer-service."""
import json
import os
import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

# Путь к конфигу chaos
CHAOS_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "chaos_config.json"
)

# Дефолтный конфиг (все сбои выключены)
DEFAULT_CHAOS_CONFIG = {
    "enabled": False,
    "failure_scenarios": {
        "random_delay": {
            "enabled": False,
            "probability": 0.1,
            "delay_range_ms": [100, 2000]
        },
        "random_failure": {
            "enabled": False,
            "probability": 0.05,
            "error_message": "Warehouse timeout"
        },
        "inventory_mismatch": {
            "enabled": False,
            "affected_skus": ["SKU-003"],
            "behavior": "report_available_but_missing"
        }
    }
}


def _write_chaos_config(config):
    """Записывает chaos конфиг в файл."""
    with open(CHAOS_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    # Сбрасываем кэш конфига в consumer
    from consumer import _chaos_config
    if _chaos_config is not None:
        _chaos_config.clear()
        _chaos_config.update(config)


@pytest.fixture(autouse=True)
def reset_chaos_config():
    """Сбрасывает chaos конфиг перед и после каждого теста."""
    # Сохраняем оригинальный конфиг
    original_config = {}
    if os.path.exists(CHAOS_CONFIG_PATH):
        with open(CHAOS_CONFIG_PATH) as f:
            original_config = json.load(f)
    
    # Устанавливаем дефолтный конфиг
    _write_chaos_config(DEFAULT_CHAOS_CONFIG)
    
    yield
    
    # Восстанавливаем оригинальный конфиг
    if original_config:
        _write_chaos_config(original_config)
    else:
        _write_chaos_config(DEFAULT_CHAOS_CONFIG)


# ── Тест 1: Chaos disabled ──

def test_chaos_disabled_no_side_effects():
    """Chaos выключен — обработка идёт без задержек и ошибок."""
    from consumer import apply_chaos, load_chaos_config
    
    config = DEFAULT_CHAOS_CONFIG.copy()
    config["enabled"] = False
    _write_chaos_config(config)
    
    trace_id = "trace-test-disabled"
    start = time.time()
    
    # Многократный вызов — никаких задержек/ошибок
    for i in range(10):
        apply_chaos(f"ORD-DISABLED-{i}", trace_id)
    
    elapsed = time.time() - start
    
    # 10 вызовов должны занять << 1 секунды (без задержек)
    assert elapsed < 0.5, f"Chaos disabled, но выполнение заняло {elapsed:.2f}s"


# ── Тест 2: Random delay ──

def test_chaos_random_delay_adds_sleep():
    """Chaos включён + random_delay → добавляется задержка."""
    from consumer import apply_chaos
    
    config = DEFAULT_CHAOS_CONFIG.copy()
    config["enabled"] = True
    config["failure_scenarios"]["random_delay"]["enabled"] = True
    config["failure_scenarios"]["random_delay"]["probability"] = 1.0  # Всегда
    config["failure_scenarios"]["random_delay"]["delay_range_ms"] = [100, 200]
    _write_chaos_config(config)
    
    trace_id = "trace-test-delay"
    start = time.time()
    apply_chaos("ORD-DELAY-001", trace_id)
    elapsed = time.time() - start
    
    # Должна быть задержка >= 100ms (с запасом)
    assert elapsed >= 0.08, f"Ожидалась задержка >=100ms, но прошло {elapsed*1000:.0f}ms"


# ── Тест 3: Random failure ──

def test_chaos_random_failure_raises_exception():
    """Chaos включён + random_failure → выбрасывается исключение."""
    from consumer import apply_chaos
    
    config = DEFAULT_CHAOS_CONFIG.copy()
    config["enabled"] = True
    config["failure_scenarios"]["random_failure"]["enabled"] = True
    config["failure_scenarios"]["random_failure"]["probability"] = 1.0  # Всегда
    config["failure_scenarios"]["random_failure"]["error_message"] = "Warehouse timeout"
    _write_chaos_config(config)
    
    trace_id = "trace-test-failure"
    
    with pytest.raises(Exception) as exc_info:
        apply_chaos("ORD-FAIL-001", trace_id)
    
    assert "Warehouse timeout" in str(exc_info.value)
    assert trace_id in str(exc_info.value)


# ── Тест 4: Inventory mismatch ──

def test_chaos_inventory_mismatch():
    """Chaos включён + inventory_mismatch → для SKU-003 warehouse врёт."""
    from consumer import apply_inventory_chaos
    
    config = DEFAULT_CHAOS_CONFIG.copy()
    config["enabled"] = True
    config["failure_scenarios"]["inventory_mismatch"]["enabled"] = True
    config["failure_scenarios"]["inventory_mismatch"]["affected_skus"] = ["SKU-003"]
    config["failure_scenarios"]["inventory_mismatch"]["behavior"] = "report_available_but_missing"
    _write_chaos_config(config)
    
    # SKU-003: warehouse говорит "есть", но chaos говорит "нет"
    actual_available = True
    result = apply_inventory_chaos("SKU-003", actual_available)
    assert result is False, "Chaos должен вернуть False для SKU-003"
    
    # SKU-001: chaos не влияет
    result = apply_inventory_chaos("SKU-001", actual_available)
    assert result is True, "Chaos не должен влиять на SKU-001"


# ── Тест 5: Chaos не влияет на отключённые сценарии ──

def test_chaos_does_not_affect_disabled_scenarios():
    """Включён только random_failure, random_delay — нет."""
    from consumer import apply_chaos
    
    config = DEFAULT_CHAOS_CONFIG.copy()
    config["enabled"] = True
    config["failure_scenarios"]["random_failure"]["enabled"] = True
    config["failure_scenarios"]["random_failure"]["probability"] = 1.0
    config["failure_scenarios"]["random_delay"]["enabled"] = False
    _write_chaos_config(config)
    
    # random_failure срабатывает
    with pytest.raises(Exception):
        apply_chaos("ORD-001", "trace-001")
    
    # Но delay не добавляется (probability = 0 для delay)
    start = time.time()
    try:
        apply_chaos("ORD-002", "trace-002")
    except Exception:
        pass  # ожидаемо — failure срабатывает
    
    elapsed = time.time() - start
    # Если бы delay был включён, elapsed был бы >= 100ms
    assert elapsed < 0.05, f"Delay не должен добавляться, но прошло {elapsed*1000:.0f}ms"
