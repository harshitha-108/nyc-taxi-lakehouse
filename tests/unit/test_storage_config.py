"""Storage selection and naming contract tests."""

from pathlib import Path

import pytest

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.config import StorageConfig, StorageConfigError
from nyc_taxi_lakehouse.storage.iceberg import IcebergStorageError, table_name
from nyc_taxi_lakehouse.storage.migrate import MigrationPaths


def test_filesystem_is_backward_compatible_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
    assert StorageConfig.from_env().backend == "filesystem"


def test_iceberg_rejects_missing_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
    with pytest.raises(StorageConfigError, match="requires MinIO"):
        StorageConfig.from_env("iceberg")


def test_iceberg_config_uses_explicit_object_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "test_user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test_password")
    monkeypatch.setenv("MINIO_BUCKET", "test-bucket")
    config = StorageConfig.from_env("iceberg")
    assert config.warehouse == "s3a://test-bucket/warehouse"
    assert config.jdbc_uri.startswith("jdbc:sqlite:")


def test_invalid_backend_is_rejected() -> None:
    with pytest.raises(StorageConfigError, match="Unsupported"):
        StorageConfig.from_env("surprise")


def test_table_names_are_controlled() -> None:
    assert table_name("bronze") == "lakehouse.nyc_taxi.bronze_trips"
    assert table_name("pickup_zone_performance").endswith("gold_pickup_zone_performance")
    with pytest.raises(IcebergStorageError, match="Unknown"):
        table_name("arbitrary_sql")


def test_period_source_paths_cover_all_layers() -> None:
    paths = MigrationPaths(Path("data"))
    period = ProcessingPeriod(2024, 3)
    assert paths.source("bronze", "yellow", period) == Path(
        "data/bronze/yellow/year=2024/month=03"
    )
    assert paths.source("quarantine", "yellow", period) == Path(
        "data/quarantine/silver/yellow/year=2024/month=03"
    )
    assert paths.source("hourly_demand", "yellow", period) == Path(
        "data/gold/hourly_demand/yellow/year=2024/month=03"
    )
