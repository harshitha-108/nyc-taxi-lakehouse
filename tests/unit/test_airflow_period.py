"""Pure period semantics; Airflow itself is exercised by the integration suite."""

from datetime import UTC, datetime

import pytest

from nyc_taxi_lakehouse.orchestration.airflow_stages import processing_period


@pytest.mark.parametrize(
    ("start", "expected"),
    [
        (datetime(2024, 1, 1, tzinfo=UTC), "2024-01"),
        (datetime(2024, 12, 1, tzinfo=UTC), "2024-12"),
        (datetime(2025, 1, 1, tzinfo=UTC), "2025-01"),
    ],
)
def test_monthly_interval_start_selects_source_month(start: datetime, expected: str) -> None:
    assert processing_period(start).identifier == expected


def test_manual_period_override_is_explicit_and_validated() -> None:
    assert processing_period(datetime(2026, 9, 1, tzinfo=UTC), "2024-03").identifier == "2024-03"
    with pytest.raises(ValueError):
        processing_period(datetime(2026, 9, 1, tzinfo=UTC), "2024-13")


def test_interval_must_be_month_aligned_and_timezone_aware() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        processing_period(datetime(2024, 1, 1))
    with pytest.raises(ValueError, match="month start"):
        processing_period(datetime(2024, 1, 2, tzinfo=UTC))
