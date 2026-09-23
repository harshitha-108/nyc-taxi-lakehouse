"""Fast serving contract tests without a database or historical files."""

from datetime import date
from pathlib import Path

import pytest

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.serving.database import MARTS
from nyc_taxi_lakehouse.serving.publisher import PublicationError, gold_path, read_gold_rows


def test_config_requires_all_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ANALYTICS_DB_HOST", "ANALYTICS_DB_PORT", "ANALYTICS_DB_NAME",
                 "ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="Missing analytics database configuration"):
        ServingConfig.from_env()


def test_connection_settings_and_schema_are_explicit() -> None:
    config = ServingConfig("db", 5432, "analytics_db", "publisher", "test_only", "test_schema")
    assert config.connect_kwargs()["dbname"] == "analytics_db"
    assert config.connect_kwargs()["host"] == "db"
    assert config.schema == "test_schema"
    with pytest.raises(ValueError, match="schema"):
        ServingConfig("db", 5432, "analytics_db", "publisher", "test_only", "bad;drop")


def test_marts_have_distinct_names_and_period_aware_grains() -> None:
    assert len(MARTS) == 5
    assert len({mart.name for mart in MARTS}) == 5
    for mart in MARTS:
        assert {"_source_taxi_type", "_source_year", "_source_month", "trip_count"} <= set(
            mart.names
        )
        assert mart.grain


def test_gold_source_path_uses_month_partition() -> None:
    path = gold_path(Path("gold"), MARTS[0], "yellow", ProcessingPeriod(2024, 3))
    assert path == Path("gold/daily_trip_metrics/yellow/year=2024/month=03")


def test_missing_gold_partition_fails_before_database_work(tmp_path: Path) -> None:
    with pytest.raises(PublicationError, match="missing or empty"):
        read_gold_rows(tmp_path, MARTS[0], "yellow", ProcessingPeriod(2024, 1))


def test_daily_grain_is_not_just_publication_month() -> None:
    assert "pickup_date" in MARTS[0].grain
    assert date(2024, 2, 1) != date(2024, 1, 1)
