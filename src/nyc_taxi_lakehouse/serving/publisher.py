"""Publish compact, validated Gold Parquet marts to PostgreSQL without rebuilding Gold."""

from __future__ import annotations

import argparse
import logging
import math
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import pyarrow.dataset as ds
from psycopg2 import sql
from psycopg2.extras import execute_values

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod, period_range
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.serving.database import MARTS, Mart, ensure_tables

LOGGER = logging.getLogger(__name__)


class PublicationError(ValueError):
    """Gold source or serving reconciliation violated the publication contract."""


@dataclass(frozen=True)
class MartResult:
    """Verified results for a single mart and month."""

    rows: int
    trip_count: int
    total_revenue: Decimal


@dataclass(frozen=True)
class PublicationResult:
    """Small, JSON-safe operational result for CLI and Airflow."""

    period: str
    taxi_type: str
    marts: dict[str, MartResult]
    elapsed_seconds: float


def gold_path(gold_dir: Path, mart: Mart, taxi_type: str, period: ProcessingPeriod) -> Path:
    """Resolve a single existing Gold source partition."""
    return (gold_dir / mart.name / taxi_type / f"year={period.year}"
            / f"month={period.month:02d}")


def read_gold_rows(
    gold_dir: Path, mart: Mart, taxi_type: str, period: ProcessingPeriod
) -> list[dict[str, object]]:
    """Read a compact Gold mart, checking schema and publication-period lineage."""
    path = gold_path(gold_dir, mart, taxi_type, period)
    if not path.is_dir() or not any(path.glob("part-*.parquet")):
        raise PublicationError(f"Gold partition missing or empty: {path}")
    table = ds.dataset(str(path), format="parquet").to_table()
    if set(table.column_names) != set(mart.names):
        missing = set(mart.names) - set(table.column_names)
        unexpected = set(table.column_names) - set(mart.names)
        raise PublicationError(
            f"Gold columns differ for {mart.name}: missing={missing}, unexpected={unexpected}"
        )
    rows = table.to_pylist()
    if not rows:
        raise PublicationError(f"Gold mart contains no rows: {path}")
    for row in rows:
        if (row["_source_taxi_type"], row["_source_year"], row["_source_month"]) != (
            taxi_type, period.year, period.month
        ):
            raise PublicationError(f"Gold period lineage does not match {path}")
    return rows


def _same_value(left: object, right: object) -> bool:
    if isinstance(left, float) or isinstance(right, float):
        if left is None or right is None:
            return left is right
        return math.isclose(float(left), float(right), rel_tol=1e-10, abs_tol=1e-8)
    return left == right


def reconcile_mart(
    cursor: object, schema: str, mart: Mart, taxi_type: str,
    period: ProcessingPeriod, source_rows: list[dict[str, object]],
) -> MartResult:
    """Compare every Gold business value with its visible transactional PostgreSQL row."""
    cursor.execute(sql.SQL("SELECT {} FROM {}.{} WHERE _source_taxi_type=%s "
                           "AND _source_year=%s AND _source_month=%s").format(
        sql.SQL(", ").join(map(sql.Identifier, mart.names)),
        sql.Identifier(schema), sql.Identifier(mart.name)),
        (taxi_type, period.year, period.month))
    actual = [dict(zip(mart.names, values, strict=True)) for values in cursor.fetchall()]
    source_by_key = {tuple(row[name] for name in mart.grain): row for row in source_rows}
    actual_by_key = {tuple(row[name] for name in mart.grain): row for row in actual}
    if len(source_by_key) != len(source_rows) or len(actual_by_key) != len(actual):
        raise PublicationError(f"Duplicate grain in {mart.name} for {period.identifier}")
    if len(actual) != len(source_rows) or actual_by_key.keys() != source_by_key.keys():
        raise PublicationError(f"Row/grain mismatch in {mart.name} for {period.identifier}")
    for key, expected in source_by_key.items():
        observed = actual_by_key[key]
        for name in mart.names:
            if name == "_gold_processed_at":
                continue  # technical timestamps may have different sub-microsecond precision
            if not _same_value(expected[name], observed[name]):
                raise PublicationError(
                    f"Gold/serving metric mismatch: {mart.name} period={period.identifier} "
                    f"grain={key} column={name}"
                )
    return MartResult(
        rows=len(actual),
        trip_count=sum(int(row["trip_count"]) for row in actual),
        total_revenue=sum((row["total_revenue"] for row in actual), Decimal(0)),
    )


def publish_period(
    config: ServingConfig, period: ProcessingPeriod, *, taxi_type: str = "yellow",
    gold_dir: Path = Path("data/gold"),
) -> PublicationResult:
    """Replace all five target-period marts in one PostgreSQL transaction.

    Gold is read before BEGIN. DELETE, INSERT, per-row reconciliation and cross-mart
    reconciliation all happen before one COMMIT. Any exception rolls all five back.
    """
    if taxi_type != "yellow":
        raise PublicationError(f"Unsupported taxi type: {taxi_type}")
    start = time.monotonic()
    source = {mart.name: read_gold_rows(gold_dir, mart, taxi_type, period) for mart in MARTS}
    LOGGER.info("Serving publication started: period=%s taxi_type=%s marts=%s",
                period.identifier, taxi_type, len(MARTS))
    published_at = datetime.now(UTC)
    try:
        with closing(psycopg2.connect(**config.connect_kwargs())) as connection:
            with connection:
                with connection.cursor() as cursor:
                    ensure_tables(cursor, config.schema)
                    results: dict[str, MartResult] = {}
                    for mart in MARTS:
                        rows = source[mart.name]
                        cursor.execute(sql.SQL("DELETE FROM {}.{} WHERE _source_taxi_type=%s "
                                               "AND _source_year=%s AND _source_month=%s").format(
                            sql.Identifier(config.schema), sql.Identifier(mart.name)),
                            (taxi_type, period.year, period.month))
                        statement = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                            sql.Identifier(config.schema), sql.Identifier(mart.name),
                            sql.SQL(", ").join(map(sql.Identifier,
                                                   (*mart.names, "_serving_published_at"))))
                        values = [tuple(row[name] for name in mart.names) + (published_at,)
                                  for row in rows]
                        execute_values(cursor, statement.as_string(connection), values,
                                       page_size=500)
                        results[mart.name] = reconcile_mart(
                            cursor, config.schema, mart, taxi_type, period, rows)
                        LOGGER.info("Serving mart verified: mart=%s period=%s rows=%s trips=%s",
                                    mart.name, period.identifier, results[mart.name].rows,
                                    results[mart.name].trip_count)
                    trip_totals = {name: item.trip_count for name, item in results.items()}
                    if len(set(trip_totals.values())) != 1:
                        raise PublicationError(
                            f"Cross-mart trip-count reconciliation failed: {trip_totals}")
                    revenue_totals = {name: item.total_revenue for name, item in results.items()}
                    if len(set(revenue_totals.values())) != 1:
                        raise PublicationError(
                            f"Cross-mart total-amount reconciliation failed: {revenue_totals}")
    except Exception:
        LOGGER.exception("Serving publication rolled back: period=%s", period.identifier)
        raise
    elapsed = time.monotonic() - start
    LOGGER.info("Serving publication committed: period=%s elapsed_seconds=%.2f",
                period.identifier, elapsed)
    return PublicationResult(period.identifier, taxi_type, results, elapsed)


def validate_period(
    config: ServingConfig, period: ProcessingPeriod, *, taxi_type: str = "yellow",
    gold_dir: Path = Path("data/gold"),
) -> dict[str, MartResult]:
    """Read-only Gold/serving reconciliation for a previously committed period."""
    source = {mart.name: read_gold_rows(gold_dir, mart, taxi_type, period) for mart in MARTS}
    with closing(psycopg2.connect(**config.connect_kwargs())) as connection:
        with connection.cursor() as cursor:
            results = {mart.name: reconcile_mart(
                cursor, config.schema, mart, taxi_type, period, source[mart.name])
                for mart in MARTS}
    if len({item.trip_count for item in results.values()}) != 1:
        raise PublicationError(f"Serving trip counts differ across marts: {period.identifier}")
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish Gold marts into PostgreSQL analytics.")
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--start", required=True, help="First period, YYYY-MM")
    parser.add_argument("--end", required=True, help="Last period, YYYY-MM")
    parser.add_argument("--gold-dir", type=Path, default=Path("data/gold"))
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING"))
    return parser.parse_args()


def main() -> int:
    """Publish a chronological inclusive range, safely restartable month by month."""
    args = _parse_args()
    logging.basicConfig(level=args.log_level,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        config = ServingConfig.from_env()
        for period in period_range(ProcessingPeriod.parse(args.start),
                                   ProcessingPeriod.parse(args.end)):
            publish_period(config, period, taxi_type=args.taxi_type, gold_dir=args.gold_dir)
    except (ValueError, psycopg2.Error, OSError) as exc:
        LOGGER.error("Serving publication failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
