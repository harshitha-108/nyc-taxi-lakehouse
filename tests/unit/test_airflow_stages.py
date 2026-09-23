"""Focused control-plane adapters without network or large Spark inputs."""

import json
from pathlib import Path

import pytest

from nyc_taxi_lakehouse.orchestration import airflow_stages
from nyc_taxi_lakehouse.orchestration.pipeline import PipelinePaths
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.schema.validator import Compatibility
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import TABLES


def test_breaking_schema_stage_raises_without_spark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        airflow_stages, "validate_and_report",
        lambda *args: {"compatibility": Compatibility.BREAKING,
                       "changes": [{"type": "REMOVED_COLUMN"}]},
    )
    with pytest.raises(airflow_stages.SchemaContractFailure, match="Breaking schema"):
        airflow_stages.execute_stage(
            "validate_schema", ProcessingPeriod(2024, 1), paths=PipelinePaths(state_dir=tmp_path)
        )


@pytest.mark.parametrize("compatibility", [Compatibility.COMPATIBLE, Compatibility.WARNING])
def test_nonbreaking_schema_stage_returns_small_json_metadata(
    compatibility: Compatibility, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        airflow_stages, "validate_and_report",
        lambda *args: {"compatibility": compatibility, "actual_fingerprint": "abc",
                       "changes": [{"type": "ADDED_COLUMN"}]},
    )
    result = airflow_stages.execute_stage("validate_schema", ProcessingPeriod(2024, 1))
    assert result == {"compatibility": compatibility.value,
                      "actual_fingerprint": "abc", "change_count": 1}
    json.dumps(result)


def test_filesystem_publication_does_not_connect_to_minio(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        airflow_stages, "create_spark_session",
        lambda *args: pytest.fail("Filesystem mode must not start Iceberg Spark"),
    )
    assert airflow_stages.execute_stage(
        "publish_iceberg", ProcessingPeriod(2024, 1), storage_backend="filesystem"
    ) == {"skipped": True}


def test_iceberg_publication_delegates_to_existing_publisher(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeSpark:
        stopped = False

        def stop(self) -> None:
            self.stopped = True

    spark = FakeSpark()
    calls = []
    monkeypatch.setattr(airflow_stages, "create_spark_session", lambda *args: spark)
    monkeypatch.setattr(
        airflow_stages.StorageConfig, "from_env",
        lambda backend: StorageConfig(backend=backend),
    )

    def fake_publish(spark_arg, dataset, path, taxi_type, period):
        calls.append((dataset, path, taxi_type, period.identifier))
        return {"rows": 1, "snapshot_id": len(calls)}

    monkeypatch.setattr(airflow_stages, "publish_dataset", fake_publish)
    result = airflow_stages.execute_stage(
        "publish_iceberg", ProcessingPeriod(2024, 1), storage_backend="iceberg"
    )
    assert [name for name, *_ in calls] == list(TABLES)
    assert result["snapshots"]["bronze"] == 1
    assert result["snapshots"]["pickup_zone_performance"] == len(TABLES)
    assert calls[-1][1] == Path(
        "data/gold/pickup_zone_performance/yellow/year=2024/month=01"
    )
    assert all(taxi == "yellow" and period == "2024-01" for _, _, taxi, period in calls)
    assert spark.stopped
    json.dumps(result)
