"""Inspect Iceberg table rows, active data files, and snapshot history."""

from __future__ import annotations

import argparse
import json

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import TABLES, latest_snapshot, period_filter, table_name
from nyc_taxi_lakehouse.storage.spark import create_spark_session


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=tuple(TABLES))
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()
    if (args.year is None) != (args.month is None):
        parser.error("--year and --month must be supplied together")
    period = ProcessingPeriod(args.year, args.month) if args.year is not None else None
    spark = create_spark_session("nyc-taxi-iceberg-inspect", StorageConfig.from_env("iceberg"))
    try:
        for dataset in ([args.dataset] if args.dataset else TABLES):
            table = table_name(dataset)
            if not spark.catalog.tableExists(table):
                print(json.dumps({"table": table, "exists": False}))
                continue
            snapshot = latest_snapshot(spark, table)
            report = {
                "table": table,
                "row_count": spark.table(table).count(),
                "active_data_files": spark.sql(f"SELECT * FROM {table}.files").count(),
                "snapshot_count": spark.sql(f"SELECT * FROM {table}.snapshots").count(),
                "current_snapshot_id": snapshot.snapshot_id if snapshot else None,
                "current_operation": snapshot.operation if snapshot else None,
                "committed_at": snapshot.committed_at if snapshot else None,
            }
            if period:
                report["period"] = period.identifier
                report["period_rows"] = spark.table(table).filter(
                    period_filter(args.taxi_type, period)
                ).count()
            print(json.dumps(report))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
