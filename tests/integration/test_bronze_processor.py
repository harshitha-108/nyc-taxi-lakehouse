"""Local Spark integration tests for source-aligned Bronze processing."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.bronze import processor
from nyc_taxi_lakehouse.bronze.processor import (
    LINEAGE_COLUMNS,
    BronzeRequest,
    add_lineage_columns,
    bronze_partition_path,
    create_spark_session,
    write_bronze_partition,
)
from nyc_taxi_lakehouse.ingestion.nyc_taxi import InvalidRequestError, raw_data_path


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session("bronze-tests")
    yield session
    session.stop()


def _write_raw_fixture(spark: SparkSession, raw_dir: Path, request: BronzeRequest) -> None:
    del spark
    raw_path = raw_data_path(request.source_request, raw_dir)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "VendorID": pa.array([1, 2], type=pa.int32()),
                "tpep_pickup_datetime": pa.array(
                    [datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 1, tzinfo=UTC)]
                ),
                "trip_distance": pa.array([2.5, -1.0], type=pa.float64()),
            }
        ),
        raw_path,
    )


def test_lineage_columns_preserve_source_columns(spark: SparkSession, tmp_path: Path) -> None:
    request = BronzeRequest(taxi_type="yellow", year=2024, month=1)
    source = spark.createDataFrame([(1, "yellow")], ["VendorID", "service"])
    output = add_lineage_columns(
        source,
        request,
        tmp_path / "yellow_tripdata_2024-01.parquet",
        datetime.now(UTC),
    )

    assert source.columns == output.columns[: len(source.columns)]
    assert output.count() == source.count()
    assert set(LINEAGE_COLUMNS).issubset(output.columns)
    assert output.filter("_source_year = 2024 AND _source_month = 1").count() == 1


def test_partition_path_and_invalid_month(tmp_path: Path) -> None:
    request = BronzeRequest(taxi_type="yellow", year=2024, month=1)
    assert bronze_partition_path(request, tmp_path) == (
        tmp_path / "yellow" / "year=2024" / "month=01"
    )
    with pytest.raises(InvalidRequestError, match="Month"):
        BronzeRequest(taxi_type="yellow", year=2024, month=13)


def test_bronze_write_readback_and_partition_level_idempotency(
    spark: SparkSession, tmp_path: Path
) -> None:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    january = BronzeRequest(taxi_type="yellow", year=2024, month=1)
    february = BronzeRequest(taxi_type="yellow", year=2024, month=2)
    _write_raw_fixture(spark, raw_dir, january)
    _write_raw_fixture(spark, raw_dir, february)

    january_first = write_bronze_partition(spark, january, raw_dir=raw_dir, bronze_dir=bronze_dir)
    february_result = write_bronze_partition(
        spark, february, raw_dir=raw_dir, bronze_dir=bronze_dir
    )
    january_second = write_bronze_partition(spark, january, raw_dir=raw_dir, bronze_dir=bronze_dir)

    assert january_first.source_rows == january_first.bronze_rows == 2
    assert january_second.bronze_rows == 2
    assert january_second.output_file_count == 1
    assert february_result.target_path.exists()
    assert january_second.target_path.exists()
    january_readback = spark.read.parquet(str(january_second.target_path))
    assert january_readback.count() == 2
    assert set(LINEAGE_COLUMNS).issubset(january_readback.columns)


def test_failed_bronze_promotion_preserves_previous_partition(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir, bronze_dir = tmp_path / "raw", tmp_path / "bronze"
    request = BronzeRequest("yellow", 2024, 1)
    _write_raw_fixture(spark, raw_dir, request)
    first = write_bronze_partition(spark, request, raw_dir=raw_dir, bronze_dir=bronze_dir)
    before = next(first.target_path.glob("part-*.parquet")).read_bytes()

    def fail_promotion(*args: object) -> None:
        raise RuntimeError("injected Bronze promotion fault")

    with monkeypatch.context() as fault:
        fault.setattr(processor, "_replace_partition", fail_promotion)
        with pytest.raises(RuntimeError, match="Bronze promotion fault"):
            write_bronze_partition(spark, request, raw_dir=raw_dir, bronze_dir=bronze_dir)
    assert next(first.target_path.glob("part-*.parquet")).read_bytes() == before
    assert not list(first.target_path.parent.glob("*.__temporary__*"))
    recovered = write_bronze_partition(spark, request, raw_dir=raw_dir, bronze_dir=bronze_dir)
    assert recovered.bronze_rows == first.bronze_rows == 2
