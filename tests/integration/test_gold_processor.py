"""End-to-end local Spark tests for the complete Gold output set."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.gold import processor
from nyc_taxi_lakehouse.gold.processor import (
    GOLD_DATASETS,
    GRAINS,
    GoldRequest,
    gold_partition_path,
    process_gold_partition,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session("gold-integration-tests")
    yield session
    session.stop()


def _silver_dataframe(spark: SparkSession):
    rows = [
        (datetime(2024, 1, 1, 8, tzinfo=UTC), date(2024, 1, 1), 2.0, 10.0, 10.0, 2.0, 12.0, 1, 1),
        (datetime(2024, 1, 1, 9, tzinfo=UTC), date(2024, 1, 1), 4.0, 20.0, 20.0, 4.0, 24.0, 2, 2),
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
        dataframe.withColumn("_source_taxi_type", F.lit("yellow"))
        .withColumn("_source_year", F.lit(2024))
        .withColumn("_source_month", F.lit(1))
    )


def _silver_partition_path(silver_dir: Path, request: GoldRequest) -> Path:
    return silver_dir / request.taxi_type / f"year={request.year}" / f"month={request.month:02d}"


def test_gold_write_validates_all_outputs_and_replaces_target_partition(
    spark: SparkSession, tmp_path: Path
) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    january = GoldRequest("yellow", 2024, 1)
    february = GoldRequest("yellow", 2024, 2)
    for request in (january, february):
        _silver_dataframe(spark).write.mode("overwrite").parquet(
            str(_silver_partition_path(silver_dir, request))
        )

    january_first = process_gold_partition(
        spark, january, silver_dir=silver_dir, gold_dir=gold_dir
    )
    process_gold_partition(spark, february, silver_dir=silver_dir, gold_dir=gold_dir)
    january_second = process_gold_partition(
        spark, january, silver_dir=silver_dir, gold_dir=gold_dir
    )

    assert january_first.input_rows == january_second.input_rows == 2
    assert set(january_second.datasets) == set(GOLD_DATASETS)
    for dataset_name in GOLD_DATASETS:
        january_path = gold_partition_path(january, gold_dir, dataset_name)
        february_path = gold_partition_path(february, gold_dir, dataset_name)
        dataframe = spark.read.parquet(str(january_path))
        assert january_path.exists()
        assert february_path.exists()
        assert dataframe.agg(F.sum("trip_count")).first()[0] == 2
        duplicate_grains = (
            dataframe.groupBy(*GRAINS[dataset_name])
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert duplicate_grains == 0

    payment_dataframe = spark.read.parquet(
        str(gold_partition_path(january, gold_dir, "payment_type_summary"))
    )
    assert payment_dataframe.agg(F.sum("trip_percentage")).first()[0] == pytest.approx(100.0)
    assert payment_dataframe.agg(F.sum("revenue_percentage")).first()[0] == pytest.approx(100.0)


def test_failed_gold_promotion_preserves_all_four_prior_marts(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    silver_dir, gold_dir = tmp_path / "silver", tmp_path / "gold"
    request = GoldRequest("yellow", 2024, 1)
    _silver_dataframe(spark).write.mode("overwrite").parquet(
        str(_silver_partition_path(silver_dir, request)))
    process_gold_partition(spark, request, silver_dir=silver_dir, gold_dir=gold_dir)
    before = {name: next(gold_partition_path(request, gold_dir, name).glob(
        "part-*.parquet")).read_bytes() for name in GOLD_DATASETS}

    def fail_promotion(*args: object) -> None:
        raise RuntimeError("injected Gold promotion fault")

    with monkeypatch.context() as fault:
        fault.setattr(processor, "_promote_partitions", fail_promotion)
        with pytest.raises(RuntimeError, match="Gold promotion fault"):
            process_gold_partition(spark, request, silver_dir=silver_dir, gold_dir=gold_dir)
    for name in GOLD_DATASETS:
        path = gold_partition_path(request, gold_dir, name)
        assert next(path.glob("part-*.parquet")).read_bytes() == before[name]
        assert not list(path.parent.glob("*.__temporary__*"))
    recovered = process_gold_partition(spark, request, silver_dir=silver_dir, gold_dir=gold_dir)
    assert all(item["rows"] > 0 for item in recovered.datasets.values())
