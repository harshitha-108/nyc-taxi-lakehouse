"""Real Airflow parsing, dependency, and isolated DAG-run behavior."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pendulum
import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402
from airflow.utils.state import DagRunState, TaskInstanceState  # noqa: E402

from nyc_taxi_lakehouse.orchestration import airflow_stages  # noqa: E402


@pytest.fixture(scope="module")
def dag():
    folder = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    bag = DagBag(str(folder), include_examples=False)
    assert not bag.import_errors
    assert "nyc_taxi_monthly_lakehouse" in bag.dags
    return bag.dags["nyc_taxi_monthly_lakehouse"]


def test_dag_graph_schedule_and_retries(dag) -> None:
    ordered = (
        "ingest_raw", "validate_schema", "bronze", "silver", "gold", "geographic",
        "publish_iceberg", "publish_serving", "validate_reconciliation",
    )
    assert set(dag.task_ids) == set(ordered)
    for before, after in zip(ordered[:-1], ordered[1:], strict=True):
        assert after in dag.get_task(before).downstream_task_ids
    assert dag.catchup is False
    assert dag.start_date.isoformat().startswith("2024-01-01T00:00:00")
    assert dag.max_active_runs == 1
    assert dag.get_task("ingest_raw").retries == 2
    assert dag.get_task("validate_schema").retries == 0
    assert dag.get_task("bronze").retries == 1
    assert dag.get_task("publish_serving").retries == 1


def test_monthly_timetable_supports_controlled_historical_interval(dag) -> None:
    interval = dag.timetable.infer_manual_data_interval(
        run_after=pendulum.datetime(2024, 4, 1, tz="UTC")
    )
    assert interval.start.isoformat().startswith("2024-03-01T00:00:00")
    assert interval.end.isoformat().startswith("2024-04-01T00:00:00")


def test_filesystem_dag_run_uses_real_airflow_states_without_processing(
    dag, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def fake_stage(stage, period, taxi_type, storage_backend, paths=None):
        calls.append((stage, period.identifier, storage_backend))
        return {"skipped": True} if stage == "publish_iceberg" else {"rows": 2}

    monkeypatch.setattr(airflow_stages, "execute_stage", fake_stage)
    run = dag.test(
        execution_date=datetime.now(UTC) - timedelta(days=3),
        run_conf={"period": "2024-03", "storage_backend": "filesystem"},
    )
    assert run.state == DagRunState.SUCCESS
    states = {task.task_id: task.state for task in run.get_task_instances()}
    assert states["publish_iceberg"] == TaskInstanceState.SKIPPED
    assert states["publish_serving"] == TaskInstanceState.SUCCESS
    assert states["validate_reconciliation"] == TaskInstanceState.SUCCESS
    assert len(calls) == 9
    assert all(period == "2024-03" and mode == "filesystem" for _, period, mode in calls)


def test_breaking_schema_fails_airflow_gate_and_blocks_downstream(
    dag, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def fake_stage(stage, period, taxi_type, storage_backend, paths=None):
        calls.append(stage)
        if stage == "validate_schema":
            raise airflow_stages.SchemaContractFailure("Breaking schema contract")
        return {"rows": 2}

    monkeypatch.setattr(airflow_stages, "execute_stage", fake_stage)
    run = dag.test(
        execution_date=datetime.now(UTC) - timedelta(days=2),
        run_conf={"period": "2024-04", "storage_backend": "filesystem"},
    )
    assert run.state == DagRunState.FAILED
    states = {task.task_id: task.state for task in run.get_task_instances()}
    assert states["validate_schema"] == TaskInstanceState.FAILED
    assert states["bronze"] == TaskInstanceState.UPSTREAM_FAILED
    assert calls == ["ingest_raw", "validate_schema"]


def test_compatible_schema_and_iceberg_publication_are_reachable(
    dag, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def fake_stage(stage, period, taxi_type, storage_backend, paths=None):
        calls.append(stage)
        if stage == "validate_schema":
            return {"compatibility": "COMPATIBLE", "change_count": 1}
        if stage == "publish_iceberg":
            assert storage_backend == "iceberg"
            return {"snapshots": {"bronze": 123}}
        return {"rows": 2}

    monkeypatch.setattr(airflow_stages, "execute_stage", fake_stage)
    run = dag.test(
        execution_date=datetime.now(UTC) - timedelta(days=1),
        run_conf={"period": "2024-05", "storage_backend": "iceberg"},
    )
    assert run.state == DagRunState.SUCCESS
    assert len(calls) == 9
    states = {task.task_id: task.state for task in run.get_task_instances()}
    assert states["bronze"] == TaskInstanceState.SUCCESS
    assert states["publish_iceberg"] == TaskInstanceState.SUCCESS


def test_serving_failure_does_not_change_upstream_success(
    dag, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def fake_stage(stage, period, taxi_type, storage_backend, paths=None):
        calls.append(stage)
        if stage == "publish_serving":
            raise RuntimeError("Serving unavailable")
        return {"rows": 2}

    monkeypatch.setattr(airflow_stages, "execute_stage", fake_stage)
    task = dag.get_task("publish_serving")
    original_retries = task.retries
    task.retries = 0  # Test terminal failure, not Airflow's wall-clock retry delay.
    try:
        run = dag.test(
            execution_date=datetime.now(UTC) - timedelta(days=4),
            run_conf={"period": "2024-06", "storage_backend": "iceberg"},
        )
    finally:
        task.retries = original_retries
    assert run.state == DagRunState.FAILED
    states = {item.task_id: item.state for item in run.get_task_instances()}
    assert states["gold"] == TaskInstanceState.SUCCESS
    assert states["publish_iceberg"] == TaskInstanceState.SUCCESS
    assert states["publish_serving"] == TaskInstanceState.FAILED
    assert states["validate_reconciliation"] == TaskInstanceState.UPSTREAM_FAILED


@pytest.mark.parametrize(("failing_stage", "position"), [
    ("ingest_raw", 0), ("validate_schema", 1), ("silver", 3),
    ("publish_iceberg", 6), ("publish_serving", 7),
    ("validate_reconciliation", 8),
])
def test_airflow_failure_state_matrix(
    dag, monkeypatch: pytest.MonkeyPatch, failing_stage: str, position: int,
) -> None:
    ordered = (
        "ingest_raw", "validate_schema", "bronze", "silver", "gold",
        "geographic", "publish_iceberg", "publish_serving", "validate_reconciliation",
    )
    calls = []

    def fake_stage(stage, period, taxi_type, storage_backend, paths=None):
        calls.append(stage)
        if stage == failing_stage:
            raise RuntimeError(f"injected {stage} fault")
        return {"rows": 2}

    monkeypatch.setattr(airflow_stages, "execute_stage", fake_stage)
    task = dag.get_task(failing_stage)
    original_retries = task.retries
    task.retries = 0  # Observe terminal state without waiting through retry delay.
    try:
        run = dag.test(
            execution_date=datetime.now(UTC) - timedelta(days=30 + position),
            run_conf={"period": "2024-03", "storage_backend": "iceberg"},
        )
    finally:
        task.retries = original_retries
    states = {item.task_id: item.state for item in run.get_task_instances()}
    assert run.state == DagRunState.FAILED
    assert states[failing_stage] == TaskInstanceState.FAILED
    assert all(states[stage] == TaskInstanceState.SUCCESS for stage in ordered[:position])
    assert all(states[stage] == TaskInstanceState.UPSTREAM_FAILED
               for stage in ordered[position + 1:])
    assert calls == list(ordered[:position + 1])


def test_airflow_serving_recovery_preserves_upstream_success(
    dag, monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    visible_serving_rows = {"2024-03": 2}

    def fake_stage(stage, period, taxi_type, storage_backend, paths=None):
        nonlocal attempts
        if stage == "publish_serving":
            attempts += 1
            if attempts == 1:
                raise ConnectionError("injected PostgreSQL outage")
            visible_serving_rows[period.identifier] = 2
        if stage == "validate_reconciliation":
            assert visible_serving_rows[period.identifier] == 2
        return {"rows": 2}

    monkeypatch.setattr(airflow_stages, "execute_stage", fake_stage)
    task = dag.get_task("publish_serving")
    original_retries = task.retries
    task.retries = 0
    try:
        failed = dag.test(
            execution_date=datetime.now(UTC) - timedelta(days=45),
            run_conf={"period": "2024-03", "storage_backend": "iceberg"},
        )
        failed_states = {item.task_id: item.state for item in failed.get_task_instances()}
        assert failed_states["gold"] == TaskInstanceState.SUCCESS
        assert failed_states["publish_serving"] == TaskInstanceState.FAILED
        assert visible_serving_rows == {"2024-03": 2}
        recovered = dag.test(
            execution_date=datetime.now(UTC) - timedelta(days=46),
            run_conf={"period": "2024-03", "storage_backend": "iceberg"},
        )
    finally:
        task.retries = original_retries
    assert recovered.state == DagRunState.SUCCESS
    states = {item.task_id: item.state for item in recovered.get_task_instances()}
    assert states["publish_serving"] == TaskInstanceState.SUCCESS
    assert states["validate_reconciliation"] == TaskInstanceState.SUCCESS
    assert attempts == 2
    assert visible_serving_rows == {"2024-03": 2}
