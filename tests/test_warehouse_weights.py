"""Tests for weighted warehouse distribution in order generator."""
import os
import subprocess
import sys
from collections import Counter

import pytest

SIMULATOR_PATH = os.path.join(
    os.path.dirname(__file__), "..", "services", "simulation-service", "simulator.py"
)
SIMULATOR_DIR = os.path.dirname(SIMULATOR_PATH)

EXPECTED_WEIGHTS = {
    "WH-MSK-S": 0.5,
    "WH-MSK-N": 0.3,
    "WH-KZN": 0.2,
}
TOLERANCE = 0.10  # ±10%


def _generate_orders(n: int) -> list:
    """Generate orders via subprocess to avoid import issues."""
    code = f"""
import random, sys
sys.path.insert(0, '{SIMULATOR_DIR}')
# Patch httpx out
sys.modules['httpx'] = type(sys)('httpx')
from simulator import generate_order
orders = [generate_order() for _ in range({n})]
for o in orders:
    print(o['warehouse_id'])
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"Subprocess failed: {result.stderr}"
    return result.stdout.strip().split("\n")


class TestWarehouseWeights:
    """Verify warehouse weights are configured correctly."""

    def test_weights_config_exists(self):
        """WAREHOUSE_WEIGHTS must be defined in simulator.py."""
        with open(SIMULATOR_PATH) as f:
            content = f.read()
        assert "WAREHOUSE_WEIGHTS" in content, \
            "simulator.py must define WAREHOUSE_WEIGHTS"

    def test_weights_sum_to_one(self):
        """Weights should sum to ~1.0."""
        with open(SIMULATOR_PATH) as f:
            content = f.read()
        # Extract weights dict
        assert '"WH-MSK-S": 0.5' in content or "'WH-MSK-S': 0.5" in content
        assert '"WH-MSK-N": 0.3' in content or "'WH-MSK-N': 0.3" in content
        assert '"WH-KZN": 0.2' in content or "'WH-KZN': 0.2" in content

    def test_uses_random_choices(self):
        """Should use random.choices() for weighted selection."""
        with open(SIMULATOR_PATH) as f:
            content = f.read()
        assert "random.choices" in content or "random.choice" not in content.split("warehouse")[0], \
            "Should use random.choices with weights for warehouse selection"


class TestWeightedDistribution:
    """Verify actual distribution matches weights."""

    def test_distribution_matches_weights(self):
        """Generate 1000 orders and verify distribution matches weights ±10%."""
        warehouses = _generate_orders(1000)
        n = len(warehouses)
        counts = Counter(warehouses)

        for wh, expected_weight in EXPECTED_WEIGHTS.items():
            actual_count = counts.get(wh, 0)
            actual_ratio = actual_count / n
            expected_min = expected_weight * (1 - TOLERANCE)
            expected_max = expected_weight * (1 + TOLERANCE)
            assert expected_min <= actual_ratio <= expected_max, (
                f"{wh}: got {actual_ratio:.3f} ({actual_count}/{n}), "
                f"expected {expected_weight:.1f} ±{TOLERANCE*100:.0f}% "
                f"({expected_min:.3f}–{expected_max:.3f})"
            )

    def test_all_warehouses_generated(self):
        """All warehouses should appear in 1000 orders."""
        warehouses = set(_generate_orders(1000))
        assert warehouses == {"WH-MSK-S", "WH-MSK-N", "WH-KZN"}

    def test_msk_s_is_most_common(self):
        """WH-MSK-S (50%) should be the most common warehouse."""
        warehouses = _generate_orders(1000)
        counts = Counter(warehouses)
        assert counts["WH-MSK-S"] > counts["WH-MSK-N"]
        assert counts["WH-MSK-S"] > counts["WH-KZN"]
        assert counts["WH-MSK-N"] > counts["WH-KZN"]
