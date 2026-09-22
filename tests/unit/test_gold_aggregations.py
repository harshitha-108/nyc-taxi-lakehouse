"""Unit tests for Gold aggregation contracts using small synthetic Silver data."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.gold.processor import (
    DAILY_TRIP_METRICS,
    HOURLY_DEMAND,
    PAYMENT_TYPE_SUMMARY,
    PICKUP_LOCATION_PERFORMANCE,
    GoldError,
    GoldRequest,
    build_gold_datasets,
    gold_partition_path,
)
from nyc_taxi_lakehouse.ingestion.nyc_taxi import InvalidRequestError


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session("gold-unit-tests")
    yield session
    session.stop()


def silver_dataframe(spark: SparkSession):
    """Return deterministic valid Silver rows with two dates, hours, locations, and payments."""
    rows = [
        (
            datetime(2024, 1, 1, 8, 30, tzinfo=UTC),
            date(2024, 1, 1),
            2.0,
            10.0,
            10.0,
            2.0,
            12.0,
            1,
            1,
        ),
        (
            datetime(2024, 1, 1, 8, 45, tzinfo=UTC),
            date(2024, 1, 1),
            4.0,
            20.0,
            20.0,
            4.0,
            24.0,
            1,
            1,
        ),
        (
            datetime(2024, 1, 1, 9, 0, tzinfo=UTC),
            date(2024, 1, 1),
            1.0,
            5.0,
            5.0,
            0.0,
            6.0,
            2,
            2,
        ),
        (
            datetime(2024, 1, 2, 8, 0, tzinfo=UTC),
            date(2024, 1, 2),
            3.0,
            15.0,
            15.0,
            3.0,
            18.0,
            2,
            2,
        ),
    ]
    columns = [
        "tpep_pickup_datetime",
        "pickup_date",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "pickup_location_id",
        "payment_type",
    ]
    dataframe = spark.createDataFrame(rows, columns)
    return (
        dataframe.withColumn("_source_taxi_type", dataframe.payment_type.cast("string"))
        .withColumn("_source_year", dataframe.payment_type.cast("int"))
        .withColumn("_source_month", dataframe.payment_type.cast("int"))
    )


def test_gold_aggregations_calculate_expected_metrics(spark: SparkSession) -> None:
    request = GoldRequest("yellow", 2024, 1)
    processed_at = datetime(2024, 2, 1, tzinfo=UTC)
    datasets = build_gold_datasets(
        silver_dataframe(spark),
        request,
        processed_at,
        input_rows=4,
        input_total_revenue=60.0,
    )

    daily_rows = {row.pickup_date: row for row in datasets[DAILY_TRIP_METRICS].collect()}
    assert daily_rows[date(2024, 1, 1)].trip_count == 3
    assert daily_rows[date(2024, 1, 1)].total_revenue == pytest.approx(42.0)
    assert daily_rows[date(2024, 1, 1)].total_trip_distance == pytest.approx(7.0)
    assert daily_rows[date(2024, 1, 2)].trip_count == 1

    hourly_rows = {
        (row.pickup_date, row.pickup_hour): row
        for row in datasets[HOURLY_DEMAND].select(
            "pickup_date", "pickup_hour", "trip_count"
        ).collect()
    }
    assert hourly_rows[(date(2024, 1, 1), 8)].trip_count == 2
    assert hourly_rows[(date(2024, 1, 1), 9)].trip_count == 1

    location_rows = {
        row.pickup_location_id: row
        for row in datasets[PICKUP_LOCATION_PERFORMANCE].collect()
    }
    assert location_rows[1].trip_count == 2
    assert location_rows[1].total_revenue == pytest.approx(36.0)
    assert location_rows[1].average_revenue_per_trip == pytest.approx(18.0)

    payment_rows = {row.payment_type: row for row in datasets[PAYMENT_TYPE_SUMMARY].collect()}
    assert payment_rows[1].trip_count == 2
    assert payment_rows[1].trip_percentage == pytest.approx(50.0)
    assert payment_rows[1].revenue_percentage == pytest.approx(60.0)
    assert sum(row.trip_percentage for row in payment_rows.values()) == pytest.approx(100.0)
    assert sum(row.revenue_percentage for row in payment_rows.values()) == pytest.approx(100.0)

    daily_columns = datasets[DAILY_TRIP_METRICS].columns
    assert "_gold_processed_at" in daily_columns
    assert "_source_taxi_type" in daily_columns


def test_gold_input_and_request_validation(spark: SparkSession, tmp_path) -> None:
    with pytest.raises(GoldError, match="missing required columns"):
        build_gold_datasets(
            spark.createDataFrame([(1,)], ["payment_type"]),
            GoldRequest("yellow", 2024, 1),
            datetime.now(UTC),
            input_rows=1,
            input_total_revenue=1.0,
        )
    with pytest.raises(InvalidRequestError, match="Month"):
        GoldRequest("yellow", 2024, 13)
    with pytest.raises(GoldError, match="Unsupported"):
        gold_partition_path(GoldRequest("yellow", 2024, 1), tmp_path, "unknown")
