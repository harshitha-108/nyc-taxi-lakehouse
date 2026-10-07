"""Small Spark contracts for the filter-responsive takeaway breakdown."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.serving.takeaways import (
    UNKNOWN_BOROUGH,
    UNKNOWN_PAYMENT_TYPE,
    TakeawayPublicationError,
    build_breakdown,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session("takeaway-unit-tests")
    yield session
    session.stop()


def test_breakdown_preserves_trips_and_filter_dimensions(spark: SparkSession) -> None:
    silver = spark.createDataFrame([
        (datetime(2024, 1, 1, 18, tzinfo=UTC), 161, 1, "yellow", 2024, 1),
        (datetime(2024, 1, 2, 18, tzinfo=UTC), 161, 1, "yellow", 2024, 1),
        (datetime(2024, 1, 2, 9, tzinfo=UTC), 132, 2, "yellow", 2024, 1),
        (datetime(2024, 1, 2, 9, tzinfo=UTC), 999, None, "yellow", 2024, 1),
    ], ["tpep_pickup_datetime", "pickup_location_id", "payment_type",
        "_source_taxi_type", "_source_year", "_source_month"])
    zones = spark.createDataFrame([
        (161, "Manhattan"), (132, "Queens"),
    ], ["location_id", "borough"])
    rows = {(row["borough"], row["pickup_hour"], row["payment_type"]):
            row["trip_count"] for row in build_breakdown(silver, zones).collect()}
    assert rows == {
        ("Manhattan", 18, 1): 2,
        ("Queens", 9, 2): 1,
        (UNKNOWN_BOROUGH, 9, UNKNOWN_PAYMENT_TYPE): 1,
    }
    assert sum(rows.values()) == silver.count()


def test_breakdown_rejects_missing_dimensions(spark: SparkSession) -> None:
    silver = spark.createDataFrame([(161,)], ["pickup_location_id"])
    zones = spark.createDataFrame([(161, "Manhattan")], ["location_id", "borough"])
    with pytest.raises(TakeawayPublicationError, match="missing insight columns"):
        build_breakdown(silver, zones)
