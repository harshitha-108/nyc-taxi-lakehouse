"""Small, fail-closed count reconciliation across a single source period."""

from __future__ import annotations

from collections.abc import Mapping

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.iceberg import GOLD_DATASETS, TABLES


class ReconciliationError(ValueError):
    """A published stage does not represent the expected period data."""


def reconcile_period_counts(
    period: ProcessingPeriod,
    *,
    bronze_rows: int,
    silver_rows: int,
    quarantine_rows: int,
    gold: Mapping[str, tuple[int, int]],
    serving_trips: Mapping[str, int],
    iceberg_rows: Mapping[str, int] | None = None,
) -> dict[str, object]:
    """Check row/represented-trip totals, returning only compact status metadata.

    Gold values are ``(row_count, represented_trip_count)``. A missing mart is a
    failure, not an implicit zero. Value-level Gold/serving comparison remains in
    the existing serving validator; this function checks cross-layer counts.
    """
    if bronze_rows < 1 or silver_rows < 1 or quarantine_rows < 0:
        raise ReconciliationError(f"Invalid period counts for {period.identifier}")
    if bronze_rows != silver_rows + quarantine_rows:
        raise ReconciliationError(f"Bronze/Silver/quarantine mismatch: {period.identifier}")
    if set(gold) != set(GOLD_DATASETS):
        raise ReconciliationError(f"Gold mart set mismatch: {period.identifier}")
    if any(rows < 1 or trips != silver_rows for rows, trips in gold.values()):
        raise ReconciliationError(f"Silver/Gold trip mismatch: {period.identifier}")
    if iceberg_rows is not None:
        if set(iceberg_rows) != set(TABLES):
            raise ReconciliationError(f"Iceberg table set mismatch: {period.identifier}")
        expected = {"bronze": bronze_rows, "silver": silver_rows,
                    "quarantine": quarantine_rows,
                    **{name: counts[0] for name, counts in gold.items()}}
        if any(iceberg_rows[name] != rows for name, rows in expected.items()):
            raise ReconciliationError(f"Local/Iceberg row mismatch: {period.identifier}")
    if set(serving_trips) != set(GOLD_DATASETS):
        raise ReconciliationError(f"Serving mart set mismatch: {period.identifier}")
    if any(trips != silver_rows for trips in serving_trips.values()):
        raise ReconciliationError(f"Gold/serving trip mismatch: {period.identifier}")
    return {
        "period": period.identifier,
        "bronze_silver_reconciled": True,
        "gold_reconciled": True,
        "iceberg_reconciled": True if iceberg_rows is not None else None,
        "serving_reconciled": True,
        "overall_status": "PASS",
    }
