"""Spark integration coverage for left-join geographic enrichment."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.gold import geographic
from nyc_taxi_lakehouse.gold.geographic import (
    enrich_with_taxi_zone,
    pickup_zone_partition_path,
    process_geographic_enrichment,
)
from nyc_taxi_lakehouse.gold.processor import GoldRequest, gold_partition_path


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session("geographic-tests")
    yield session
    session.stop()


def test_left_broadcast_enrichment_preserves_unmatched_rows(spark: SparkSession) -> None:
    facts = spark.createDataFrame(
        [(1, 10), (2, 20), (None, 5)], ["pickup_location_id", "trip_count"]
    )
    zones = spark.createDataFrame(
        [(1, "Manhattan", "Alpha", "Boro Zone")],
        ["location_id", "borough", "zone", "service_zone"],
    )
    result = enrich_with_taxi_zone(facts, zones).orderBy("trip_count").collect()
    assert len(result) == 3
    rows = {row.pickup_location_id: row for row in result}
    assert rows[1].borough == "Manhattan"
    assert rows[2].borough is None
    assert rows[None].zone is None


def _write_reference_csv(reference_dir: Path) -> None:
    path = reference_dir / "taxi_zones" / "taxi_zone_lookup.csv"
    path.parent.mkdir(parents=True)
    path.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,Manhattan,Alpha,Boro Zone\n"
        "2,Queens,Beta,Yellow Zone\n",
        encoding="utf-8",
    )


def _write_location_gold(spark: SparkSession, gold_dir: Path, request: GoldRequest) -> None:
    dataframe = spark.createDataFrame(
        [
            (1, 10, 100.0, 10.0, 2.0, 5.0, 20.0),
            (2, 20, 300.0, 15.0, 3.0, 7.0, 40.0),
            (3, 5, 80.0, 16.0, 4.0, 8.0, 10.0),
        ],
        [
            "pickup_location_id",
            "trip_count",
            "total_revenue",
            "average_revenue_per_trip",
            "average_trip_distance",
            "average_trip_duration_minutes",
            "total_tip_amount",
        ],
    )
    output = (
        dataframe.withColumn("_gold_processed_at", F.current_timestamp())
        .withColumn("_source_taxi_type", F.lit("yellow"))
        .withColumn("_source_year", F.lit(2024))
        .withColumn("_source_month", F.lit(1))
    )
    output.write.mode("overwrite").parquet(
        str(gold_partition_path(request, gold_dir, "pickup_location_performance"))
    )


def test_geographic_pipeline_reconciles_and_is_idempotent(
    spark: SparkSession, tmp_path: Path
) -> None:
    gold_dir = tmp_path / "gold"
    reference_dir = tmp_path / "reference"
    january = GoldRequest("yellow", 2024, 1)
    february = GoldRequest("yellow", 2024, 2)
    _write_reference_csv(reference_dir)
    _write_location_gold(spark, gold_dir, january)
    _write_location_gold(spark, gold_dir, february)
    first = process_geographic_enrichment(
        spark, january, gold_dir=gold_dir, reference_dir=reference_dir
    )
    process_geographic_enrichment(spark, february, gold_dir=gold_dir, reference_dir=reference_dir)
    second = process_geographic_enrichment(
        spark, january, gold_dir=gold_dir, reference_dir=reference_dir
    )
    enriched = spark.read.parquet(str(pickup_zone_partition_path(january, gold_dir)))
    assert first.output_rows == second.output_rows == 3
    assert second.matched_location_ids == 2
    assert second.unmatched_location_ids == 1
    assert second.unmatched_trip_count == 5
    assert enriched.agg(F.sum("trip_count")).first()[0] == 35
    assert enriched.filter(F.col("pickup_location_id") == 3).first().borough is None
    assert pickup_zone_partition_path(february, gold_dir).exists()


def test_failed_geographic_promotion_preserves_prior_zone_and_location_marts(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    gold_dir, reference_dir = tmp_path / "gold", tmp_path / "reference"
    request = GoldRequest("yellow", 2024, 1)
    _write_reference_csv(reference_dir)
    _write_location_gold(spark, gold_dir, request)
    first = process_geographic_enrichment(
        spark, request, gold_dir=gold_dir, reference_dir=reference_dir)
    prior_zone = next(first.target_path.glob("part-*.parquet")).read_bytes()
    location_path = gold_partition_path(request, gold_dir, "pickup_location_performance")
    prior_location = next(location_path.glob("part-*.parquet")).read_bytes()

    def fail_promotion(*args: object) -> None:
        raise RuntimeError("injected geographic promotion fault")

    with monkeypatch.context() as fault:
        fault.setattr(geographic, "_replace_partition", fail_promotion)
        with pytest.raises(RuntimeError, match="geographic promotion fault"):
            process_geographic_enrichment(
                spark, request, gold_dir=gold_dir, reference_dir=reference_dir)
    assert next(first.target_path.glob("part-*.parquet")).read_bytes() == prior_zone
    assert next(location_path.glob("part-*.parquet")).read_bytes() == prior_location
    assert not list(first.target_path.parent.glob("*.__temporary__*"))
    recovered = process_geographic_enrichment(
        spark, request, gold_dir=gold_dir, reference_dir=reference_dir)
    assert recovered.output_rows == first.output_rows == 3
