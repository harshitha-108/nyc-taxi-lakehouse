"""Isolated Phase 14 experiments; never writes to historical data partitions.

Method: January 2024 real data. Paired experiments use one untimed warm-up
per variant and three A/B, B/A, A/B measurements. The production Gold run
uses one untimed warm-up followed by three measured runs. Wall time uses
perf_counter.
The same Docker session and source files are used throughout; OS/JVM caches are
not cleared. Results are local, warm/mixed observations, not universal speedups.
Generated files live only under the ignored data/state/benchmarks directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

import psycopg2
import pyarrow.dataset as ds
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.gold.geographic import enrich_with_taxi_zone
from nyc_taxi_lakehouse.gold.processor import (
    GOLD_DATASETS,
    GRAINS,
    GoldRequest,
    _standard_metrics,
    build_gold_datasets,
    process_gold_partition,
)
from nyc_taxi_lakehouse.reference.taxi_zones import load_taxi_zones, taxi_zone_csv_path
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.silver.processor import classify_records
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.spark import create_spark_session

ROOT = Path("data/state/benchmarks")
SILVER = Path("data/silver/yellow/year=2024/month=01")
BRONZE = Path("data/bronze/yellow/year=2024/month=01")
LOCATION = Path("data/gold/pickup_location_performance/yellow/year=2024/month=01")
REQUEST = GoldRequest("yellow", 2024, 1)


def _plan_lines(dataframe: DataFrame) -> list[str]:
    plan = dataframe._jdf.queryExecution().executedPlan().toString()
    tokens = ("Exchange", "Aggregate", "Scan", "Filter", "Join", "ReadSchema")
    return [line.strip() for line in plan.splitlines() if any(x in line for x in tokens)][:16]


def _paired(cases: dict[str, object], repetitions: int = 3) -> dict[str, object]:
    """Run warm-ups, then alternating pairs; record every observation, not only medians."""
    names = list(cases)
    if len(names) != 2:
        raise ValueError("Exactly two experiment variants are required.")
    if repetitions < 1:
        raise ValueError("At least one measured repetition is required.")
    for name in names:
        cases[name]("warmup")
    runs = {name: [] for name in names}
    results = {name: [] for name in names}
    for iteration in range(repetitions):
        order = names if iteration % 2 == 0 else list(reversed(names))
        for name in order:
            started = time.perf_counter()
            result = cases[name](str(iteration + 1))
            duration = time.perf_counter() - started
            runs[name].append(round(duration, 6))
            results[name].append(result)
    return {
        "runs_seconds": runs,
        "median_seconds": {name: statistics.median(values) for name, values in runs.items()},
        "min_seconds": {name: min(values) for name, values in runs.items()},
        "max_seconds": {name: max(values) for name, values in runs.items()},
        "results": results,
    }


def percentage_reduction(baseline_seconds: float, candidate_seconds: float) -> float:
    """Return wall-time reduction, rejecting impossible or non-finite measurements."""
    if not all(math.isfinite(value) and value > 0 for value in
               (baseline_seconds, candidate_seconds)):
        raise ValueError("Benchmark durations must be finite and positive.")
    return (baseline_seconds - candidate_seconds) / baseline_seconds * 100


def _gold_digest(root: Path) -> dict[str, str]:
    """Hash every compact Gold business row, excluding run-specific processing time."""
    digests = {}
    for name in GOLD_DATASETS:
        path = root / name / "yellow/year=2024/month=01"
        table = ds.dataset(str(path), format="parquet").to_table()
        columns = [column for column in table.column_names if column != "_gold_processed_at"]
        rows = table.select(columns).to_pylist()
        rows.sort(key=lambda row: tuple(str(row[key]) for key in GRAINS[name]))
        payload = json.dumps(rows, sort_keys=True, default=str).encode()
        digests[name] = hashlib.sha256(payload).hexdigest()
    return digests


def production_gold_experiment(spark) -> dict[str, object]:
    """Measure the actual production Gold job, without monkeypatching its cache behavior."""
    def run(label: str) -> dict[str, object]:
        target = ROOT / "gold_production" / label
        result = process_gold_partition(spark, REQUEST, gold_dir=target)
        return {
            "input_rows": result.input_rows,
            "revenue": str(result.input_total_revenue),
            "mart_rows": {
                name: item["rows"] if isinstance(item, dict) else item.rows
                for name, item in result.datasets.items()
            },
            "business_digests": _gold_digest(target),
        }

    warmup = run("warmup")
    durations = []
    results = []
    for iteration in range(3):
        started = time.perf_counter()
        results.append(run(f"run_{iteration + 1}"))
        durations.append(round(time.perf_counter() - started, 6))
    return {
        "runs_seconds": durations,
        "median_seconds": statistics.median(durations),
        "min_seconds": min(durations),
        "max_seconds": max(durations),
        "warmup_result": warmup,
        "results": results,
        "correctness": all(item == warmup for item in results),
    }


def shuffle_experiment(spark, candidate: int = 2) -> dict[str, object]:
    """Compare current four shuffle partitions to one candidate setting."""
    source = spark.read.parquet(str(SILVER))

    def run(count: int, _label: str) -> dict[str, object]:
        spark.conf.set("spark.sql.shuffle.partitions", str(count))
        daily = source.groupBy("pickup_date").agg(*_standard_metrics())
        row = daily.agg(
            F.sum("trip_count").alias("trips"),
            F.sum("total_revenue").alias("revenue"),
        ).first()
        return {"trips": int(row.trips), "revenue": str(row.revenue)}

    measured = _paired({
        "current_4": lambda label: run(4, label),
        f"candidate_{candidate}": lambda label: run(candidate, label),
    })
    measured["correctness"] = len({
        json.dumps(item, sort_keys=True)
        for cases in measured["results"].values() for item in cases
    }) == 1
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    measured["aqe_enabled"] = spark.conf.get("spark.sql.adaptive.enabled")
    measured["physical_plan"] = _plan_lines(source.groupBy("pickup_date").agg(*_standard_metrics()))
    return measured


def parquet_experiment(spark) -> dict[str, object]:
    """Measure one-file coalesce versus two-file shuffle on real January Silver."""
    source = spark.read.parquet(str(SILVER))

    def run(label: str, repartition: bool) -> dict[str, object]:
        target = ROOT / "parquet" / ("repartition_2" if repartition else "coalesce_1") / label
        selected = source.repartition(2) if repartition else source.coalesce(1)
        write_start = time.perf_counter()
        selected.write.mode("overwrite").option("compression", "snappy").parquet(str(target))
        write_seconds = time.perf_counter() - write_start
        read_start = time.perf_counter()
        row = spark.read.parquet(str(target)).agg(
            F.count("*").alias("rows"), F.sum("total_amount").alias("revenue")
        ).first()
        read_seconds = time.perf_counter() - read_start
        sizes = sorted(path.stat().st_size for path in target.glob("part-*.parquet"))
        return {
            "write_seconds": round(write_seconds, 6),
            "read_seconds": round(read_seconds, 6),
            "rows": int(row.rows),
            "revenue": str(row.revenue),
            "file_count": len(sizes),
            "file_sizes": sizes,
        }

    measured = _paired({
        "coalesce_1_current": lambda label: run(label, False),
        "repartition_2_candidate": lambda label: run(label, True),
    })
    values = [
        (item["rows"], item["revenue"])
        for cases in measured["results"].values() for item in cases
    ]
    measured["correctness"] = len(set(values)) == 1
    return measured


def iceberg_experiment(spark) -> dict[str, object]:
    """Inspect existing period layout and compare broad versus January scan scope."""
    table = "lakehouse.nyc_taxi.bronze_trips"
    source = spark.table(table)
    file_rows = spark.sql(
        f"SELECT partition, record_count, file_size_in_bytes FROM {table}.files"
    ).collect()
    month = source.filter(
        (F.col("_source_taxi_type") == "yellow")
        & (F.col("_source_year") == 2024)
        & (F.col("_source_month") == 1)
    )

    def run(frame: DataFrame) -> dict[str, object]:
        row = frame.agg(
            F.count("*").alias("rows"), F.sum("trip_distance").alias("distance")
        ).first()
        return {"rows": int(row.rows), "distance": float(row.distance)}

    for frame in (source, month):
        run(frame)  # warm-up
    runs = {"all_months": [], "january_only": []}
    results = {"all_months": [], "january_only": []}
    for iteration in range(3):
        order = [("all_months", source), ("january_only", month)]
        if iteration % 2:
            order.reverse()
        for name, frame in order:
            start = time.perf_counter()
            result = run(frame)
            runs[name].append(round(time.perf_counter() - start, 6))
            results[name].append(result)
    return {
        "runs_seconds": runs,
        "median_seconds": {name: statistics.median(times) for name, times in runs.items()},
        "results": results,
        "files": len(file_rows),
        "total_file_bytes": sum(int(row.file_size_in_bytes) for row in file_rows),
        "partitions": sorted({str(row.partition) for row in file_rows}),
        "full_scan_tasks": source.select("trip_distance").rdd.getNumPartitions(),
        "january_scan_tasks": month.select("trip_distance").rdd.getNumPartitions(),
        "full_plan": _plan_lines(source.agg(F.sum("trip_distance"))),
        "january_plan": _plan_lines(month.agg(F.sum("trip_distance"))),
    }


def plans_experiment(spark) -> dict[str, object]:
    """Capture compact physical-plan evidence without executing the full transformations."""
    bronze = spark.read.parquet(str(BRONZE))
    silver = classify_records(bronze, datetime.now(UTC))
    source = spark.read.parquet(str(SILVER))
    input_row = source.agg(
        F.count("*").alias("rows"), F.sum("total_amount").alias("revenue")
    ).first()
    marts = build_gold_datasets(
        source, REQUEST, datetime.now(UTC), input_rows=int(input_row.rows),
        input_total_revenue=input_row.revenue,
    )
    zones = load_taxi_zones(spark, taxi_zone_csv_path(Path("data/reference")))
    enriched = enrich_with_taxi_zone(spark.read.parquet(str(LOCATION)), zones)
    return {
        "silver": _plan_lines(silver),
        "gold": {name: _plan_lines(frame) for name, frame in marts.items()},
        "geographic": _plan_lines(enriched),
        "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "default_parallelism": spark.sparkContext.defaultParallelism,
    }


def _node_types(node: dict[str, object]) -> list[str]:
    names = [str(node["Node Type"])]
    for child in node.get("Plans", []):
        names.extend(_node_types(child))
    return names


def postgres_experiment() -> dict[str, object]:
    """Use a session-local table to test a candidate index without changing serving."""
    config = ServingConfig.from_env()
    query = (
        "SELECT pickup_location_id, trip_count FROM zone_bench "
        "WHERE _source_year = 2024 AND borough = 'Manhattan' "
        "ORDER BY trip_count DESC LIMIT 10"
    )
    with closing(psycopg2.connect(**config.connect_kwargs())) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW server_version")
            version = cursor.fetchone()[0]
            cursor.execute(
                "CREATE TEMP TABLE zone_bench ON COMMIT PRESERVE ROWS AS "
                "SELECT * FROM analytics.pickup_zone_performance"
            )
            cursor.execute("ANALYZE zone_bench")
            cursor.execute("SELECT COUNT(*) FROM zone_bench")
            rows = cursor.fetchone()[0]

            def measure() -> dict[str, object]:
                cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query)
                plan = cursor.fetchone()[0][0]
                cursor.execute(query)
                values = cursor.fetchall()
                return {
                    "planning_ms": float(plan["Planning Time"]),
                    "execution_ms": float(plan["Execution Time"]),
                    "node_types": _node_types(plan["Plan"]),
                    "shared_hit_blocks": int(plan["Plan"].get("Shared Hit Blocks", 0)),
                    "business_rows": values,
                }

            measure()  # warm-up before baseline
            baseline = [measure() for _ in range(3)]
            cursor.execute("CREATE INDEX zone_bench_borough_trip_idx ON zone_bench "
                           "(borough, trip_count DESC)")
            cursor.execute("ANALYZE zone_bench")
            cursor.execute("SELECT pg_relation_size('zone_bench_borough_trip_idx')")
            index_bytes = int(cursor.fetchone()[0])
            measure()  # warm-up after index creation
            candidate = [measure() for _ in range(3)]
    return {
        "postgres_version": version,
        "table_rows": rows,
        "index_bytes": index_bytes,
        "baseline_execution_ms": [item["execution_ms"] for item in baseline],
        "candidate_execution_ms": [item["execution_ms"] for item in candidate],
        "baseline_median_ms": statistics.median(item["execution_ms"] for item in baseline),
        "candidate_median_ms": statistics.median(item["execution_ms"] for item in candidate),
        "baseline_plans": [item["node_types"] for item in baseline],
        "candidate_plans": [item["node_types"] for item in candidate],
        "correctness": all(
            item["business_rows"] == baseline[0]["business_rows"]
            for item in baseline + candidate
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "experiment",
        choices=("plans", "shuffle", "production_gold", "parquet", "iceberg", "postgres"),
    )
    parser.add_argument("--shuffle-candidate", type=int, choices=(2, 8), default=2)
    args = parser.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True)
    if args.experiment == "postgres":
        result = postgres_experiment()
    else:
        backend = "iceberg" if args.experiment == "iceberg" else "filesystem"
        spark = create_spark_session("phase14-benchmark", StorageConfig.from_env(backend))
        spark.sparkContext.setLogLevel("ERROR")
        try:
            functions = {
                "plans": plans_experiment,
                "shuffle": lambda session: shuffle_experiment(session, args.shuffle_candidate),
                "production_gold": production_gold_experiment,
                "parquet": parquet_experiment,
                "iceberg": iceberg_experiment,
            }
            result = functions[args.experiment](spark)
        finally:
            spark.stop()
    output = {"experiment": args.experiment, "period": "2024-01", "result": result}
    filename = (
        f"shuffle_{args.shuffle_candidate}.json" if args.experiment == "shuffle"
        else f"{args.experiment}.json"
    )
    destination = ROOT / filename
    destination.write_text(
        json.dumps(output, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    print(json.dumps(output, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
