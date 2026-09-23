"""Corruption at each period boundary must fail the final task, not warn."""

from __future__ import annotations

import pytest

from nyc_taxi_lakehouse.orchestration.reliability import (
    ReconciliationError,
    reconcile_period_counts,
)
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.iceberg import GOLD_DATASETS, TABLES


def _counts() -> dict[str, object]:
    return {
        "bronze_rows": 12, "silver_rows": 10, "quarantine_rows": 2,
        "gold": {name: (2, 10) for name in GOLD_DATASETS},
        "iceberg_rows": {name: 2 if name in GOLD_DATASETS else
                         12 if name == "bronze" else 10 if name == "silver" else 2
                         for name in TABLES},
        "serving_trips": {name: 10 for name in GOLD_DATASETS},
    }


def test_period_reconciliation_passes_all_four_boundaries() -> None:
    result = reconcile_period_counts(ProcessingPeriod(2024, 1), **_counts())
    assert result == {
        "period": "2024-01", "bronze_silver_reconciled": True,
        "gold_reconciled": True, "iceberg_reconciled": True,
        "serving_reconciled": True, "overall_status": "PASS",
    }


@pytest.mark.parametrize(("corruption", "message"), [
    ({"bronze_rows": 11}, "Bronze/Silver/quarantine"),
    ({"gold": {name: (2, 9) for name in GOLD_DATASETS}}, "Silver/Gold"),
    ({"iceberg_rows": {name: 1 for name in TABLES}}, "Local/Iceberg"),
    ({"serving_trips": {name: 9 for name in GOLD_DATASETS}}, "Gold/serving"),
    ({"gold": {}}, "Gold mart set"),
])
def test_period_reconciliation_detects_corruption(
    corruption: dict[str, object], message: str,
) -> None:
    values = {**_counts(), **corruption}
    with pytest.raises(ReconciliationError, match=message):
        reconcile_period_counts(ProcessingPeriod(2024, 1), **values)


def test_filesystem_mode_does_not_claim_iceberg_was_checked() -> None:
    values = {**_counts(), "iceberg_rows": None}
    result = reconcile_period_counts(ProcessingPeriod(2024, 1), **values)
    assert result["iceberg_reconciled"] is None
