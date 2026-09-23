"""Read-only January–June reconciliation across local, Iceberg, and serving data."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.orchestration.reliability import reconcile_period_counts
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.serving.publisher import validate_period
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import GOLD_DATASETS, TABLES, table_name
from nyc_taxi_lakehouse.storage.spark import create_spark_session

ROOT = Path("data/state/benchmarks")
PERIODS = [ProcessingPeriod(2024, month) for month in range(1, 7)]


def _partition(root: Path, period: ProcessingPeriod) -> Path:
    return root / "yellow" / f"year={period.year}" / f"month={period.month:02d}"


def _local_rows(path: Path) -> int:
    files = sorted(path.glob("part-*.parquet"))
    if not files:
        raise FileNotFoundError(f"Local partition lacks Parquet parts: {path}")
    return sum(pq.ParquetFile(file).metadata.num_rows for file in files)


def _gold_counts(period: ProcessingPeriod) -> dict[str, tuple[int, int]]:
    result = {}
    for name in GOLD_DATASETS:
        path = _partition(Path("data/gold") / name, period)
        files = sorted(path.glob("part-*.parquet"))
        rows = _local_rows(path)
        trips = pc.sum(ds.dataset([str(file) for file in files], format="parquet")
                       .to_table(columns=["trip_count"])["trip_count"]).as_py()
        result[name] = (rows, int(trips))
    return result


def _iceberg_monthly_counts() -> tuple[dict[int, dict[str, int]], dict[int, dict[str, int]]]:
    spark = create_spark_session("phase14-history-readonly", StorageConfig.from_env("iceberg"))
    spark.sparkContext.setLogLevel("ERROR")
    try:
        counts: dict[int, dict[str, int]] = {month: {} for month in range(1, 7)}
        represented: dict[int, dict[str, int]] = {month: {} for month in range(1, 7)}
        for name in TABLES:
            source = (spark.table(table_name(name))
                      .filter((F.col("_source_taxi_type") == "yellow")
                              & (F.col("_source_year") == 2024)
                              & F.col("_source_month").between(1, 6)))
            aggregation = [F.count("*").alias("rows")]
            if name in GOLD_DATASETS:
                aggregation.append(F.sum("trip_count").alias("trips"))
            rows = source.groupBy("_source_month").agg(*aggregation).collect()
            for row in rows:
                month = int(row["_source_month"])
                counts[month][name] = int(row["rows"])
                if name in GOLD_DATASETS:
                    represented[month][name] = int(row["trips"])
        if any(set(values) != set(TABLES) for values in counts.values()):
            raise AssertionError("Historical Iceberg period/table set is incomplete")
        return counts, represented
    finally:
        spark.stop()


def main() -> None:
    iceberg, iceberg_trips = _iceberg_monthly_counts()
    serving = ServingConfig.from_env()
    results = {}
    for period in PERIODS:
        bronze_rows = _local_rows(_partition(Path("data/bronze"), period))
        silver_rows = _local_rows(_partition(Path("data/silver"), period))
        quarantine_rows = _local_rows(_partition(Path("data/quarantine/silver"), period))
        gold = _gold_counts(period)
        if any(trips != silver_rows for trips in iceberg_trips[period.month].values()):
            raise AssertionError(f"Iceberg represented-trip mismatch: {period.identifier}")
        serving_trips = {
            name: result.trip_count for name, result in
            validate_period(serving, period).items()
        }
        reconciliation = reconcile_period_counts(
            period,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            quarantine_rows=quarantine_rows,
            gold=gold,
            iceberg_rows=iceberg[period.month],
            serving_trips=serving_trips,
        )
        results[period.identifier] = {
            "bronze_rows": bronze_rows,
            "silver_rows": silver_rows,
            "quarantine_rows": quarantine_rows,
            "gold": gold,
            "iceberg_rows": iceberg[period.month],
            "iceberg_represented_trips": iceberg_trips[period.month],
            "serving_trips": serving_trips,
            "reconciliation": reconciliation,
        }
    totals = {
        "bronze_rows": sum(item["bronze_rows"] for item in results.values()),
        "silver_rows": sum(item["silver_rows"] for item in results.values()),
        "quarantine_rows": sum(item["quarantine_rows"] for item in results.values()),
    }
    if totals["bronze_rows"] != totals["silver_rows"] + totals["quarantine_rows"]:
        raise AssertionError("Cross-month Bronze/Silver/quarantine totals do not reconcile")
    output = {"periods": results, "totals": totals, "status": "PASS"}
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / "history.json").write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    main()
