"""Explicit PostgreSQL schemas reflecting the existing five Gold Parquet schemas."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg2 import sql


@dataclass(frozen=True)
class Mart:
    """One Gold mart's columns, business grain, and additive validation metrics."""

    name: str
    columns: tuple[tuple[str, str], ...]
    grain: tuple[str, ...]
    additive: tuple[str, ...]

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)


COMMON = (
    ("_gold_processed_at", "TIMESTAMP NOT NULL"),
    ("_source_taxi_type", "TEXT NOT NULL"),
    ("_source_year", "INTEGER NOT NULL"),
    ("_source_month", "INTEGER NOT NULL"),
)
LOCATION = (
    ("pickup_location_id", "INTEGER NOT NULL"),
    ("trip_count", "BIGINT NOT NULL"),
    ("total_revenue", "NUMERIC(24,2)"),
    ("average_revenue_per_trip", "NUMERIC(18,6)"),
    ("average_trip_distance", "DOUBLE PRECISION"),
    ("average_trip_duration_minutes", "NUMERIC(16,6)"),
    ("total_tip_amount", "NUMERIC(24,2)"),
)
MARTS = (
    Mart("daily_trip_metrics", (
        ("pickup_date", "DATE NOT NULL"), ("trip_count", "BIGINT NOT NULL"),
        ("total_revenue", "NUMERIC(24,2)"),
        ("average_fare_amount", "NUMERIC(18,6)"),
        ("average_total_amount", "NUMERIC(18,6)"),
        ("average_trip_distance", "DOUBLE PRECISION"),
        ("average_trip_duration_minutes", "NUMERIC(16,6)"),
        ("total_trip_distance", "DOUBLE PRECISION"),
        ("average_tip_amount", "NUMERIC(18,6)"),
        ("total_tip_amount", "NUMERIC(24,2)"),
        *COMMON,
    ), ("pickup_date",), ("trip_count", "total_revenue", "total_tip_amount",
                            "total_trip_distance")),
    Mart("hourly_demand", (
        ("pickup_date", "DATE NOT NULL"), ("pickup_hour", "INTEGER NOT NULL"),
        ("trip_count", "BIGINT NOT NULL"), ("total_revenue", "NUMERIC(24,2)"),
        ("average_trip_distance", "DOUBLE PRECISION"),
        ("average_trip_duration_minutes", "NUMERIC(16,6)"), *COMMON,
    ), ("pickup_date", "pickup_hour"), ("trip_count", "total_revenue")),
    Mart("pickup_location_performance", (*LOCATION, *COMMON),
         ("pickup_location_id",), ("trip_count", "total_revenue", "total_tip_amount")),
    Mart("payment_type_summary", (
        ("payment_type", "BIGINT NOT NULL"), ("trip_count", "BIGINT NOT NULL"),
        ("total_revenue", "NUMERIC(24,2)"),
        ("average_total_amount", "NUMERIC(18,6)"),
        ("average_tip_amount", "NUMERIC(18,6)"),
        ("total_tip_amount", "NUMERIC(24,2)"),
        ("trip_percentage", "DOUBLE PRECISION"),
        ("revenue_percentage", "DOUBLE PRECISION"), *COMMON,
    ), ("payment_type",), ("trip_count", "total_revenue", "total_tip_amount")),
    Mart("pickup_zone_performance", (*LOCATION, ("_source_taxi_type", "TEXT NOT NULL"),
         ("_source_year", "INTEGER NOT NULL"), ("_source_month", "INTEGER NOT NULL"),
         ("borough", "TEXT"), ("zone", "TEXT"), ("service_zone", "TEXT"),
         ("_gold_processed_at", "TIMESTAMP NOT NULL")),
         ("pickup_location_id",), ("trip_count", "total_revenue", "total_tip_amount")),
)
MART_BY_NAME = {mart.name: mart for mart in MARTS}


def ensure_tables(cursor: object, schema: str) -> None:
    """Create only the serving schema and five mart tables, with useful BI indexes."""
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    for mart in MARTS:
        columns = [sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(type_name))
                   for name, type_name in mart.columns]
        columns.append(sql.SQL('"_serving_published_at" TIMESTAMPTZ NOT NULL'))
        key = ("_source_taxi_type", "_source_year", "_source_month", *mart.grain)
        columns.append(sql.SQL("PRIMARY KEY ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, key))))
        cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(mart.name), sql.SQL(", ").join(columns)))
    indexes = (
        ("daily_trip_metrics_date_idx", "daily_trip_metrics", ("pickup_date",)),
        ("hourly_demand_date_hour_idx", "hourly_demand", ("pickup_date", "pickup_hour")),
        ("pickup_zone_borough_zone_idx", "pickup_zone_performance", ("borough", "zone")),
        ("pickup_location_id_idx", "pickup_location_performance", ("pickup_location_id",)),
        ("payment_type_idx", "payment_type_summary", ("payment_type",)),
    )
    for index_name, table_name, columns in indexes:
        cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
            sql.Identifier(index_name), sql.Identifier(schema), sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns))))
