"""Opt-in Docker integration against the real MinIO/Iceberg stack."""

import os
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi_lakehouse.ingestion.nyc_taxi import TaxiDataRequest, raw_data_path
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import IcebergStorageError, table_name
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
