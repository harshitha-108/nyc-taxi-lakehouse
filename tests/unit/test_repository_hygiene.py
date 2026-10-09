"""Guard the Git hygiene policy against both false positives and missed artifacts."""

import json
import subprocess

import pytest
from scripts.check_repository_hygiene import (
    CSV_ROOT_PLACEHOLDER,
    POWERBI_MODEL,
    forbidden_path,
    main,
    personal_csv_root_in_model,
)


def _model_with_csv_root(path: str) -> bytes:
    return json.dumps({"model": {"expressions": [
        {"name": "CsvRootPath", "expression": f'"{path}" meta [IsParameterQuery=true]'},
    ]}}).encode()


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


@pytest.mark.parametrize("path", [
    r"C:\Users\alice\repo\data\gold\powerbi",
    "D:/Projects/alice/repo/data/gold/powerbi",
    r"\\server\users\alice\repo\data\gold\powerbi",
    "/home/alice/repo/data/gold/powerbi",
    "/Users/alice/repo/data/gold/powerbi",
])
def test_personal_absolute_csv_root_is_rejected_on_any_host(path: str) -> None:
    assert personal_csv_root_in_model(_model_with_csv_root(path))


@pytest.mark.parametrize("path", [CSV_ROOT_PLACEHOLDER, "data/gold/powerbi"])
def test_placeholder_or_relative_csv_root_is_allowed(path: str) -> None:
    assert not personal_csv_root_in_model(_model_with_csv_root(path))


def test_hygiene_checks_git_index_instead_of_local_powerbi_copy(
    tmp_path, monkeypatch, capsys
) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    model_path = tmp_path / POWERBI_MODEL
    model_path.parent.mkdir(parents=True)
    model_path.write_bytes(_model_with_csv_root(CSV_ROOT_PLACEHOLDER))
    subprocess.run(["git", "-C", str(tmp_path), "add", "--", POWERBI_MODEL], check=True)
    monkeypatch.chdir(tmp_path)

    model_path.write_bytes(_model_with_csv_root(r"C:\Users\alice\repo\data\gold\powerbi"))
    assert main() == 0  # Only the working copy contains the personal path.

    subprocess.run(["git", "add", "--", POWERBI_MODEL], check=True)
    model_path.write_bytes(_model_with_csv_root(CSV_ROOT_PLACEHOLDER))
    assert main() == 1  # The staged blob remains private despite the safe working copy.
    assert "CsvRootPath contains a local absolute path" in capsys.readouterr().out
