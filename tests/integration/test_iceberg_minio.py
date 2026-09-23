"""Opt-in Docker integration against the real MinIO/Iceberg stack."""

import json
import os
import subprocess
import sys
import uuid
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.readwriter import DataFrameWriterV2

from nyc_taxi_lakehouse.ingestion.nyc_taxi import TaxiDataRequest, raw_data_path
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import (
    IcebergStorageError,
    latest_snapshot,
    period_filter,
    replace_period,
    table_name,
)
from nyc_taxi_lakehouse.storage.migrate import MigrationPaths, migrate_period
from nyc_taxi_lakehouse.storage.smoke import main as smoke_main
from nyc_taxi_lakehouse.storage.spark import create_spark_session


@pytest.mark.skipif(
    os.getenv("RUN_ICEBERG_INTEGRATION") != "1",
    reason="Start MinIO, then set RUN_ICEBERG_INTEGRATION=1 to exercise real object storage.",
)
def test_real_minio_iceberg_snapshot_and_replay() -> None:
    """No mock: write/read/replay/time-travel in an isolated namespace."""
    assert smoke_main() == 0


@pytest.mark.skipif(
    os.getenv("RUN_ICEBERG_INTEGRATION") != "1",
    reason="Requires local MinIO and Iceberg Docker services.",
)
def test_breaking_source_contract_creates_no_iceberg_bronze_commit(tmp_path: Path) -> None:
    """Use a real catalog while a real metadata-only breaking gate blocks migration."""
    period = ProcessingPeriod(2024, 1)
    source = raw_data_path(TaxiDataRequest("yellow", 2024, 1), tmp_path / "raw")
    source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"unexpected_field": [1]}), source)
    config = replace(
        StorageConfig.from_env("iceberg"), catalog_path=tmp_path / "isolated_catalog.db"
    )
    spark = create_spark_session("iceberg-breaking-gate-test", config)
    try:
        with pytest.raises(IcebergStorageError, match="Breaking raw schema"):
            migrate_period(spark, "yellow", period, ("bronze",), MigrationPaths(tmp_path))
        assert not spark.catalog.tableExists(table_name("bronze"))
    finally:
        spark.stop()


@pytest.mark.skipif(os.getenv("RUN_ICEBERG_INTEGRATION") != "1",
                    reason="Requires local MinIO and Iceberg Docker services.")
def test_iceberg_commit_failure_preserves_snapshot_and_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail immediately before an Iceberg overwrite commit in a unique namespace."""
    config = replace(StorageConfig.from_env("iceberg"),
                     catalog_path=tmp_path / "reliability_catalog.db")
    namespace = f"lakehouse.reliability_{uuid.uuid4().hex[:12]}"
    table = table_name("bronze", namespace)
    spark = create_spark_session("iceberg-commit-recovery", config)
    january, february = ProcessingPeriod(2024, 1), ProcessingPeriod(2024, 2)
    schema = "trip_id long, _source_taxi_type string, _source_year int, _source_month int"
    try:
        first = spark.createDataFrame([(1, "yellow", 2024, 1)], schema)
        replacement = spark.createDataFrame([(2, "yellow", 2024, 1)], schema)
        other = spark.createDataFrame([(3, "yellow", 2024, 2)], schema)
        _, initial = replace_period(spark, "bronze", first, "yellow", january,
                                    namespace=namespace)
        replace_period(spark, "bronze", other, "yellow", february, namespace=namespace)
        before = latest_snapshot(spark, table)
        assert before is not None

        def fail_commit(*args: object, **kwargs: object) -> None:
            raise RuntimeError("injected pre-commit failure")

        with monkeypatch.context() as fault:
            fault.setattr(DataFrameWriterV2, "overwrite", fail_commit)
            with pytest.raises(RuntimeError, match="pre-commit"):
                replace_period(spark, "bronze", replacement, "yellow", january,
                               namespace=namespace)
        assert latest_snapshot(spark, table).snapshot_id == before.snapshot_id
        assert spark.table(table).filter(period_filter("yellow", january)).count() == 1
        assert spark.table(table).filter(period_filter("yellow", february)).count() == 1
        _, recovered = replace_period(spark, "bronze", replacement, "yellow", january,
                                      namespace=namespace)
        assert recovered.snapshot_id != before.snapshot_id
        assert spark.table(table).filter(period_filter("yellow", january)).count() == 1
        assert spark.table(table).filter(period_filter("yellow", february)).count() == 1
        replace_period(spark, "bronze", replacement, "yellow", january, namespace=namespace)
        assert spark.table(table).filter(period_filter("yellow", january)).count() == 1
        assert spark.read.option("snapshot-id", initial.snapshot_id).table(table).count() == 1
    finally:
        if spark.catalog.tableExists(table):
            spark.sql(f"DROP TABLE {table} PURGE")
            spark.sql(f"DROP NAMESPACE {namespace}")
        spark.stop()


@pytest.mark.skipif(os.getenv("RUN_ICEBERG_INTEGRATION") != "1",
                    reason="Requires local MinIO and Iceberg Docker services.")
def test_unreachable_minio_endpoint_fails_without_fallback_and_recovers(
    tmp_path: Path,
) -> None:
    """Fresh Spark JVM per attempt avoids stale S3A/catalog state after an outage."""
    catalog = tmp_path / "outage_catalog.db"
    namespace = f"lakehouse.reliability_{uuid.uuid4().hex[:12]}"
    worker = Path(__file__).parent / "iceberg_recovery_worker.py"

    def phase(name: str, *extra: str) -> dict[str, object]:
        run = subprocess.run([sys.executable, str(worker), name, str(catalog), namespace,
                              *extra], capture_output=True, text=True, timeout=180,
                             check=False)
        assert run.returncode == 0, run.stderr[-1500:]
        return json.loads(run.stdout.splitlines()[-1])

    try:
        before = phase("baseline")
        assert before["january_rows"] == before["february_rows"] == 1
        failure = phase("outage", "http://minio:1")
        assert failure["failed"] is True
        assert failure["storage_error"] is True
        recovered = phase("recover", str(before["snapshot_id"]))
        assert recovered["prior_snapshot_preserved"] is True
        assert recovered["january_rows"] == recovered["february_rows"] == 1
        assert recovered["new_snapshot_id"] != before["snapshot_id"]
        assert recovered["retry_rows"] == 1
        assert recovered["historical_rows"] == 2
    finally:
        if catalog.exists():
            phase("cleanup")
