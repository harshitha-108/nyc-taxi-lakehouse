"""Publish a compact Silver-derived breakdown for filter-responsive dashboard insights."""

from __future__ import annotations

import argparse
import logging
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod, period_range
from nyc_taxi_lakehouse.reference.taxi_zones import load_taxi_zones, taxi_zone_csv_path
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.silver.processor import SilverRequest, silver_partition_path
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.spark import create_spark_session

LOGGER = logging.getLogger(__name__)
TABLE_NAME = "takeaway_breakdown"
UNKNOWN_BOROUGH = "Unknown / unmapped"
UNKNOWN_PAYMENT_TYPE = -1
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime", "pickup_location_id", "payment_type",
    "_source_taxi_type", "_source_year", "_source_month",
}


class TakeawayPublicationError(ValueError):
    """A source, reference, or serving reconciliation check failed."""


@dataclass(frozen=True)
class TakeawayResult:
    """Published breakdown size and reconciled trip count for one period."""

    period: str
    rows: int
    trips: int


def build_breakdown(silver: DataFrame, zones: DataFrame) -> DataFrame:
    """Count valid trips by borough, pickup hour, and payment code without losing rows."""
    missing = REQUIRED_COLUMNS.difference(silver.columns)
    if missing:
        raise TakeawayPublicationError(f"Silver is missing insight columns: {sorted(missing)}")
    if not {"location_id", "borough"}.issubset(zones.columns):
        raise TakeawayPublicationError("Taxi Zone reference lacks location_id or borough.")
    reference = F.broadcast(zones.select(
        F.col("location_id").alias("_zone_location_id"), "borough"
    ))
    enriched = silver.join(
        reference, F.col("pickup_location_id") == F.col("_zone_location_id"), "left"
    )
    return (
        enriched.select(
            F.coalesce(F.col("borough"), F.lit(UNKNOWN_BOROUGH)).alias("borough"),
            F.hour("tpep_pickup_datetime").alias("pickup_hour"),
            F.coalesce(F.col("payment_type"), F.lit(UNKNOWN_PAYMENT_TYPE))
            .cast("long").alias("payment_type"),
        )
        .groupBy("borough", "pickup_hour", "payment_type")
        .agg(F.count("*").alias("trip_count"))
    )


def ensure_breakdown_table(cursor: object, schema: str) -> None:
    """Create a small, month-partitioned serving table without altering existing marts."""
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    cursor.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            _source_taxi_type TEXT NOT NULL,
            _source_year INTEGER NOT NULL,
            _source_month INTEGER NOT NULL,
            borough TEXT NOT NULL,
            pickup_hour SMALLINT NOT NULL,
            payment_type INTEGER NOT NULL,
            trip_count BIGINT NOT NULL CHECK (trip_count > 0),
            PRIMARY KEY (_source_taxi_type, _source_year, _source_month,
                         borough, pickup_hour, payment_type)
        )
    """).format(sql.Identifier(schema), sql.Identifier(TABLE_NAME)))
    cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} "
                           "(_source_year, _source_month, borough)").format(
        sql.Identifier(f"{TABLE_NAME}_period_borough_idx"),
        sql.Identifier(schema), sql.Identifier(TABLE_NAME)))


def publish_takeaway_period(
    spark: SparkSession,
    config: ServingConfig,
    period: ProcessingPeriod,
    *,
    taxi_type: str = "yellow",
    silver_dir: Path = Path("data/silver"),
    reference_dir: Path = Path("data/reference"),
) -> TakeawayResult:
    """Transactionally replace one period after matching its published Gold trip total."""
    if taxi_type != "yellow":
        raise TakeawayPublicationError(f"Unsupported taxi type: {taxi_type}")
    source_path = silver_partition_path(
        SilverRequest(taxi_type, period.year, period.month), silver_dir
    )
    if not source_path.is_dir() or not any(source_path.glob("part-*.parquet")):
        raise TakeawayPublicationError(f"Valid Silver partition is missing: {source_path}")
    silver = spark.read.parquet(str(source_path))
    if silver.where(
        (F.col("_source_taxi_type") != taxi_type)
        | (F.col("_source_year") != period.year)
        | (F.col("_source_month") != period.month)
        | F.col("_source_taxi_type").isNull()
        | F.col("_source_year").isNull()
        | F.col("_source_month").isNull()
    ).limit(1).count():
        raise TakeawayPublicationError("Silver partition contains mismatched source lineage.")
    zones = load_taxi_zones(spark, taxi_zone_csv_path(reference_dir))
    rows = build_breakdown(silver, zones).collect()
    if not rows or any(row["pickup_hour"] is None for row in rows):
        raise TakeawayPublicationError("Silver breakdown is empty or has a null pickup hour.")
    source_trips = sum(int(row["trip_count"]) for row in rows)
    values = [(
        taxi_type, period.year, period.month, row["borough"],
        int(row["pickup_hour"]), int(row["payment_type"]), int(row["trip_count"]),
    ) for row in rows]
    with closing(psycopg2.connect(**config.connect_kwargs())) as connection:
        with connection:
            with connection.cursor() as cursor:
                ensure_breakdown_table(cursor, config.schema)
                cursor.execute(sql.SQL(
                    "SELECT SUM(trip_count) FROM {}.daily_trip_metrics "
                    "WHERE _source_taxi_type=%s AND _source_year=%s AND _source_month=%s"
                ).format(sql.Identifier(config.schema)), (taxi_type, period.year, period.month))
                gold_trips = cursor.fetchone()[0]
                if gold_trips is None or int(gold_trips) != source_trips:
                    raise TakeawayPublicationError(
                        f"Insight/Silver-Gold trip totals differ for {period.identifier}: "
                        f"insights={source_trips} gold={gold_trips}"
                    )
                cursor.execute(sql.SQL(
                    "DELETE FROM {}.{} WHERE _source_taxi_type=%s "
                    "AND _source_year=%s AND _source_month=%s"
                ).format(sql.Identifier(config.schema), sql.Identifier(TABLE_NAME)),
                               (taxi_type, period.year, period.month))
                statement = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
                    sql.Identifier(config.schema), sql.Identifier(TABLE_NAME))
                execute_values(cursor, statement.as_string(connection), values, page_size=500)
                cursor.execute(sql.SQL(
                    "SELECT COUNT(*), SUM(trip_count) FROM {}.{} "
                    "WHERE _source_taxi_type=%s AND _source_year=%s AND _source_month=%s"
                ).format(sql.Identifier(config.schema), sql.Identifier(TABLE_NAME)),
                               (taxi_type, period.year, period.month))
                published_rows, published_trips = cursor.fetchone()
                if published_rows != len(values) or published_trips != source_trips:
                    raise TakeawayPublicationError("Insight read-back reconciliation failed.")
    LOGGER.info("Takeaway breakdown published: period=%s rows=%s trips=%s",
                period.identifier, len(values), source_trips)
    return TakeawayResult(period.identifier, len(values), source_trips)


def main() -> int:
    """Backfill or replay a period range from existing valid Silver and TLC reference data."""
    parser = argparse.ArgumentParser(description="Publish dashboard takeaway breakdowns.")
    parser.add_argument("--start", required=True, help="First period, YYYY-MM")
    parser.add_argument("--end", required=True, help="Last period, YYYY-MM")
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = create_spark_session("nyc-taxi-takeaway-breakdown",
                                 StorageConfig.from_env("filesystem"))
    try:
        config = ServingConfig.from_env()
        for period in period_range(ProcessingPeriod.parse(args.start),
                                   ProcessingPeriod.parse(args.end)):
            publish_takeaway_period(spark, config, period, taxi_type=args.taxi_type)
    except (ValueError, psycopg2.Error, OSError) as exc:
        LOGGER.error("Takeaway publication failed: %s", exc)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
