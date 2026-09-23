"""Read-only historical Gold, serving, and Silver reconciliation."""

from __future__ import annotations

import argparse
import logging
import time
from contextlib import closing
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq
from psycopg2 import sql

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod, period_range
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.serving.database import MARTS
from nyc_taxi_lakehouse.serving.publisher import PublicationError, validate_period

LOGGER = logging.getLogger(__name__)


def _silver_rows(root: Path, taxi_type: str, period: ProcessingPeriod) -> int:
    """Read only Parquet footers, never the trip-level dataset."""
    path = root / taxi_type / f"year={period.year}" / f"month={period.month:02d}"
    files = list(path.glob("part-*.parquet"))
    if not files:
        raise PublicationError(f"Silver partition is missing: {path}")
    return sum(pq.ParquetFile(file).metadata.num_rows for file in files)


def reconcile_history(
    config: ServingConfig, periods: list[ProcessingPeriod], *, taxi_type: str = "yellow",
    gold_dir: Path = Path("data/gold"), silver_dir: Path = Path("data/silver"),
) -> dict[str, object]:
    """Verify each month, each Gold/serving value, and six-month trip totals."""
    historical: dict[str, dict[str, object]] = {}
    totals = {mart.name: {"rows": 0, "trips": 0} for mart in MARTS}
    silver_total = 0
    for period in periods:
        results = validate_period(config, period, taxi_type=taxi_type, gold_dir=gold_dir)
        valid_silver = _silver_rows(silver_dir, taxi_type, period)
        for name, item in results.items():
            if item.trip_count != valid_silver:
                raise PublicationError(
                    f"Silver/serving trip mismatch: {period.identifier} {name} "
                    f"silver={valid_silver} serving={item.trip_count}"
                )
            totals[name]["rows"] += item.rows
            totals[name]["trips"] += item.trip_count
        historical[period.identifier] = {"valid_silver": valid_silver,
                                         "serving_trips": results[MARTS[0].name].trip_count,
                                         "mart_rows": {name: item.rows for name, item in
                                                       results.items()}}
        silver_total += valid_silver
        LOGGER.info("Reconciled %s: valid_silver=%s all_five_marts=PASS",
                    period.identifier, valid_silver)
    if any(item["trips"] != silver_total for item in totals.values()):
        raise PublicationError("Cross-month serving trip totals do not reconcile to Silver.")
    return {"periods": historical, "mart_totals": totals, "valid_silver_total": silver_total}


def timed_queries(config: ServingConfig) -> list[dict[str, object]]:
    """Execute a few dashboard-style SQL queries and record client-observed elapsed time."""
    queries = (
        ("highest_volume_day", "SELECT pickup_date, SUM(trip_count) FROM {}.daily_trip_metrics "
         "GROUP BY pickup_date ORDER BY SUM(trip_count) DESC LIMIT 1"),
        ("busiest_pickup_hour", "SELECT pickup_hour, SUM(trip_count) FROM {}.hourly_demand "
         "GROUP BY pickup_hour ORDER BY SUM(trip_count) DESC LIMIT 1"),
        ("top_pickup_borough", "SELECT borough, SUM(trip_count) FROM {}.pickup_zone_performance "
         "GROUP BY borough ORDER BY SUM(trip_count) DESC LIMIT 1"),
        ("top_pickup_zone", "SELECT borough, zone, SUM(trip_count) "
         "FROM {}.pickup_zone_performance GROUP BY borough, zone "
         "ORDER BY SUM(trip_count) DESC LIMIT 1"),
        ("top_payment_type", "SELECT payment_type, SUM(trip_count) "
         "FROM {}.payment_type_summary GROUP BY payment_type "
         "ORDER BY SUM(trip_count) DESC LIMIT 1"),
    )
    results = []
    with closing(psycopg2.connect(**config.connect_kwargs())) as connection:
        with connection.cursor() as cursor:
            for name, template in queries:
                query = sql.SQL(template).format(sql.Identifier(config.schema))
                started = time.perf_counter()
                cursor.execute(query)
                row = cursor.fetchone()
                elapsed = time.perf_counter() - started
                results.append({"query": name, "result": tuple(row), "seconds": elapsed})
                LOGGER.info("Serving query=%s result=%s elapsed_seconds=%.6f",
                            name, row, elapsed)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile Gold, serving, and valid Silver.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    args = parser.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = ServingConfig.from_env()
    periods = period_range(ProcessingPeriod.parse(args.start), ProcessingPeriod.parse(args.end))
    result = reconcile_history(config, periods, taxi_type=args.taxi_type)
    LOGGER.info("History PASS: valid_silver=%s marts=%s", result["valid_silver_total"],
                result["mart_totals"])
    timed_queries(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
