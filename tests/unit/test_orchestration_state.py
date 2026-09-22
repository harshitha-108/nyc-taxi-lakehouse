"""Tests for period ranges and atomic local orchestration state."""

from pathlib import Path

import pytest

from nyc_taxi_lakehouse.orchestration.state import (
    PeriodState,
    ProcessingPeriod,
    StateStore,
    period_range,
)


def test_period_range_handles_same_month_and_year_boundary() -> None:
    assert period_range(ProcessingPeriod.parse("2024-01"), ProcessingPeriod.parse("2024-01")) == [
        ProcessingPeriod(2024, 1)
    ]
    assert [
        period.identifier
        for period in period_range(ProcessingPeriod(2024, 11), ProcessingPeriod(2025, 2))
    ] == [
        "2024-11",
        "2024-12",
        "2025-01",
        "2025-02",
    ]


@pytest.mark.parametrize("value", ["2024-13", "2024-1", "invalid"])
def test_invalid_periods_are_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        ProcessingPeriod.parse(value)


def test_reverse_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="precedes"):
        period_range(ProcessingPeriod(2024, 6), ProcessingPeriod(2024, 1))


def test_state_store_round_trip_is_atomic(tmp_path: Path) -> None:
    store = StateStore(tmp_path)
    state = PeriodState(
        "yellow", "2024-01", "SUCCESS", "BOOTSTRAPPED", "run-1", "incremental", "now"
    )
    store.save_period(state)
    loaded = store.load_period("yellow", ProcessingPeriod(2024, 1))
    assert loaded is not None
    assert loaded.action == "BOOTSTRAPPED"
    assert not list(tmp_path.rglob("*.part"))
