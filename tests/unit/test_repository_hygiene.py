"""Guard the Git hygiene policy against both false positives and missed artifacts."""

import pytest
from scripts.check_repository_hygiene import forbidden_path


@pytest.mark.parametrize("path", [
    "data/raw/yellow/2024/01/trips.parquet",
    "data/state/iceberg_catalog.db",
    "data/gold/run_manifest.json",
    "airflow/logs/scheduler.log",
    "warehouse/metadata.json",
    ".env",
    ".env.local",
    "keys/id_rsa",
])
def test_generated_or_sensitive_paths_are_forbidden(path: str) -> None:
    assert forbidden_path(path)


@pytest.mark.parametrize("path", [
    "data/raw/.gitkeep",
    "data/state/.gitkeep",
    ".env.example",
    "sql/serving_examples.sql",
    "configs/contracts/yellow_taxi.json",
])
def test_versioned_placeholders_and_source_are_allowed(path: str) -> None:
    assert not forbidden_path(path)
