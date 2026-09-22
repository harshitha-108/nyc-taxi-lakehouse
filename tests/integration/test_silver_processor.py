"""Local Spark integration tests for Silver quality processing."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.ingestion.nyc_taxi import InvalidRequestError
from nyc_taxi_lakehouse.silver.processor import (
    QUALITY_REASONS_COLUMN,
    SilverRequest,
    classify_records,
    process_silver_partition,
    quarantine_partition_path,
    silver_partition_path,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session("silver-tests")
    yield session
    session.stop()


def _bronze_rows() -> list[tuple[object, ...]]:
    pickup = datetime(2024, 1, 1, 10, tzinfo=UTC)
    return [
        (1, pickup, datetime(2024, 1, 1, 10, 30, tzinfo=UTC), 1.0, 10.0, 12.0, 1, 2),
        (2, None, datetime(2024, 1, 1, 10, 30, tzinfo=UTC), 1.0, 10.0, 12.0, 1, 2),
        (3, pickup, None, 1.0, 10.0, 12.0, 1, 2),
        (4, pickup, datetime(2024, 1, 1, 9, 30, tzinfo=UTC), -1.0, -2.0, -3.0, 0, 0),
    ]


def _bronze_dataframe(spark: SparkSession):
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
    ]
    source_dataframe = spark.createDataFrame(_bronze_rows(), columns)
    return (
        source_dataframe.withColumn(
            "_bronze_ingested_at", source_dataframe.tpep_pickup_datetime
        )
        .withColumn("_source_file", source_dataframe.VendorID.cast("string"))
        .withColumn("_source_taxi_type", source_dataframe.VendorID.cast("string"))
        .withColumn("_source_year", source_dataframe.VendorID.cast("int"))
        .withColumn("_source_month", source_dataframe.VendorID.cast("int"))
    )


def test_classification_retains_all_failure_reasons(spark: SparkSession) -> None:
    classified = classify_records(_bronze_dataframe(spark), datetime.now(UTC))
    quality_rows = classified.select("vendor_id", QUALITY_REASONS_COLUMN).collect()
    rows = {row.vendor_id: row for row in quality_rows}

    assert rows[1][QUALITY_REASONS_COLUMN] == []
    assert rows[2][QUALITY_REASONS_COLUMN] == ["MISSING_PICKUP_TIMESTAMP"]
    assert rows[3][QUALITY_REASONS_COLUMN] == ["MISSING_DROPOFF_TIMESTAMP"]
    assert set(rows[4][QUALITY_REASONS_COLUMN]) == {
        "INVALID_TIMESTAMP_ORDER",
        "NEGATIVE_TRIP_DISTANCE",
        "NEGATIVE_FARE_AMOUNT",
        "NEGATIVE_TOTAL_AMOUNT",
        "INVALID_PICKUP_LOCATION",
        "INVALID_DROPOFF_LOCATION",
    }
    assert "vendor_id" in classified.columns
    assert "trip_duration_minutes" in classified.columns
    assert "_silver_processed_at" in classified.columns


def test_silver_write_reconciles_and_replaces_only_target_partition(
    spark: SparkSession, tmp_path: Path
) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    january = SilverRequest("yellow", 2024, 1)
    february = SilverRequest("yellow", 2024, 2)
    for request in (january, february):
        path = bronze_dir / "yellow" / f"year={request.year}" / f"month={request.month:02d}"
        _bronze_dataframe(spark).write.mode("overwrite").parquet(str(path))

    january_first = process_silver_partition(
        spark, january, bronze_dir=bronze_dir, silver_dir=silver_dir, quarantine_dir=quarantine_dir
    )
    process_silver_partition(
        spark, february, bronze_dir=bronze_dir, silver_dir=silver_dir, quarantine_dir=quarantine_dir
    )
    january_second = process_silver_partition(
        spark, january, bronze_dir=bronze_dir, silver_dir=silver_dir, quarantine_dir=quarantine_dir
    )

    assert january_first.input_rows == january_first.valid_rows + january_first.rejected_rows == 4
    assert january_second.valid_rows == 1
    assert january_second.rejected_rows == 3
    assert silver_partition_path(february, silver_dir).exists()
    assert quarantine_partition_path(february, quarantine_dir).exists()
    assert spark.read.parquet(str(silver_partition_path(january, silver_dir))).count() == 1
    assert spark.read.parquet(str(quarantine_partition_path(january, quarantine_dir))).count() == 3


def test_invalid_silver_request_rejects_month() -> None:
    with pytest.raises(InvalidRequestError, match="Month"):
        SilverRequest("yellow", 2024, 13)
