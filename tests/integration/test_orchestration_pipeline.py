"""Focused operational tests for incremental state decisions."""

import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi_lakehouse.ingestion.nyc_taxi import TaxiDataRequest, raw_data_path
from nyc_taxi_lakehouse.orchestration import pipeline
from nyc_taxi_lakehouse.orchestration.pipeline import PipelineError, PipelinePaths, run_period
from nyc_taxi_lakehouse.orchestration.state import PeriodState, ProcessingPeriod, StateStore
from nyc_taxi_lakehouse.schema.validator import Contract, Field, validate_and_report


@dataclass
class StageResult:
    file_size_bytes: int = 100
    bronze_rows: int = 1
    valid_rows: int = 1
    rejected_rows: int = 0
    input_rows: int = 1
    match_percentage: float = 100.0


class FakeSpark:
    def stop(self) -> None:
        pass


@pytest.fixture
def stage_harness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Synthetic Raw input and recorded stage calls; no real Spark work."""
    paths = PipelinePaths(
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gold_dir=tmp_path / "gold",
        reference_dir=tmp_path / "reference",
        state_dir=tmp_path / "state",
    )
    period = ProcessingPeriod(2024, 3)
    source = raw_data_path(TaxiDataRequest("yellow", period.year, period.month), paths.raw_dir)
    source.parent.mkdir(parents=True)
    calls: list[str] = []
    contract = Contract("synthetic_yellow", 1, "yellow", (Field("trip_distance", "double", True),))

    def write_source(*, breaking: bool = False) -> None:
        fields = [pa.field("extra", pa.string())]
        if not breaking:
            fields.insert(0, pa.field("trip_distance", pa.float64()))
        schema = pa.schema(fields)
        pq.write_table(
            pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in fields], schema=schema
            ),
            source,
        )

    write_source()

    def record(name: str):
        def call(*args: object, **kwargs: object) -> StageResult:
            calls.append(name)
            return StageResult()

        return call

    monkeypatch.setattr(pipeline, "ingest_taxi_data", record("ingestion"))

    def schema_gate(path: Path, checked_period: ProcessingPeriod, taxi_type: str, state_dir: Path):
        calls.append("schema_validation")
        return validate_and_report(path, checked_period, taxi_type, state_dir, contract)

    monkeypatch.setattr(pipeline, "validate_schema", schema_gate)
    monkeypatch.setattr(pipeline, "write_bronze_partition", record("bronze"))
    monkeypatch.setattr(pipeline, "process_silver_partition", record("silver"))
    monkeypatch.setattr(pipeline, "process_gold_partition", record("gold"))
    monkeypatch.setattr(pipeline, "process_geographic_enrichment", record("geographic"))
    monkeypatch.setattr(pipeline, "create_spark_session", lambda *args: FakeSpark())
    monkeypatch.setattr(pipeline, "validate_parquet", lambda *args: None)
    monkeypatch.setattr(pipeline, "_directory_has_parquet", lambda *args: True)
    monkeypatch.setattr(pipeline, "validate_taxi_zone_csv", lambda *args: None)
    monkeypatch.setattr(pipeline, "ingest_taxi_zones", lambda **kwargs: None)
    return paths, period, calls, write_source


def test_completed_period_is_skipped_without_stage_execution(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    paths = PipelinePaths(state_dir=tmp_path / "state")
    store = StateStore(paths.state_dir)
    period = ProcessingPeriod(2024, 1)
    store.save_period(
        PeriodState(
            "yellow", period.identifier, "SUCCESS", "PROCESSED", "old", "incremental", "now"
        )
    )
    monkeypatch.setattr(
        "nyc_taxi_lakehouse.orchestration.pipeline.validate_period_artifacts",
        lambda *_: {"raw_file_size_bytes": 1},
    )
    result = run_period(
        period,
        taxi_type="yellow",
        mode="incremental",
        from_stage="ingestion",
        run_id="test-run",
        paths=paths,
        store=store,
    )
    assert result.action == "SKIPPED"
    assert result.status == "SUCCESS"


def test_breaking_contract_blocks_all_downstream_stages(stage_harness) -> None:
    paths, period, calls, write_source = stage_harness
    write_source(breaking=True)
    store = StateStore(paths.state_dir)
    with pytest.raises(PipelineError, match="Breaking schema contract"):
        run_period(
            period,
            taxi_type="yellow",
            mode="replay",
            from_stage="ingestion",
            run_id="breaking",
            paths=paths,
            store=store,
        )
    assert calls == ["ingestion", "schema_validation"]
    state = store.load_period("yellow", period)
    assert state is not None and state.status == "FAILED"
    assert state.stages["ingestion"]["status"] == "SUCCESS"
    assert state.stages["schema_validation"]["status"] == "FAILED"
    assert all(
        state.stages[stage]["status"] == "PENDING"
        for stage in ("bronze", "silver", "gold", "geographic")
    )
    assert "REMOVED_COLUMN" in str(state.stages["schema_validation"]["metrics"])


def test_breaking_contract_marks_run_unsuccessful(stage_harness) -> None:
    paths, period, calls, write_source = stage_harness
    write_source(breaking=True)
    with pytest.raises(PipelineError, match="1 failed period"):
        pipeline.run_pipeline(
            period, period, mode="replay", from_stage="ingestion", paths=paths
        )
    assert calls == ["ingestion", "schema_validation"]
    run_files = list((paths.state_dir / "runs").glob("*.json"))
    assert len(run_files) == 1
    history = json.loads(run_files[0].read_text(encoding="utf-8"))
    assert history["status"] == "PARTIAL_FAILURE"
    assert history["results"][0]["status"] == "FAILED"
    assert "schema contract" in history["failures"][0]


@pytest.mark.parametrize(
    ("from_stage", "expected"),
    [
        ("ingestion", ["ingestion", "schema_validation", "bronze", "silver", "gold", "geographic"]),
        ("bronze", ["schema_validation", "bronze", "silver", "gold", "geographic"]),
        ("silver", ["silver", "gold", "geographic"]),
        ("gold", ["gold", "geographic"]),
        ("geographic", ["geographic"]),
    ],
)
def test_replay_stage_dependencies(stage_harness, from_stage: str, expected: list[str]) -> None:
    paths, period, calls, _ = stage_harness
    result = run_period(
        period,
        taxi_type="yellow",
        mode="replay",
        from_stage=from_stage,
        run_id="replay",
        paths=paths,
        store=StateStore(paths.state_dir),
    )
    assert result.status == "SUCCESS"
    assert calls == expected
    if from_stage in ("ingestion", "bronze"):
        report = StateStore(paths.state_dir).load_period("yellow", period)
        assert report is not None
        assert report.stages["schema_validation"]["metrics"]["compatibility"] == "COMPATIBLE"
        assert report.stages["schema_validation"]["metrics"]["changes"][0]["type"] == "ADDED_COLUMN"


def test_new_historical_period_runs_schema_gate_before_bronze(
    stage_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, period, calls, _ = stage_harness

    def incomplete(*args: object) -> None:
        raise PipelineError("No complete prior output")

    monkeypatch.setattr(pipeline, "validate_period_artifacts", incomplete)
    result = run_period(
        period,
        taxi_type="yellow",
        mode="incremental",
        from_stage="ingestion",
        run_id="backfill",
        paths=paths,
        store=StateStore(paths.state_dir),
    )
    assert result.status == "SUCCESS" and result.action == "PROCESSED"
    assert calls == ["ingestion", "schema_validation", "bronze", "silver", "gold", "geographic"]


@pytest.mark.parametrize("action", ["BOOTSTRAPPED", "PROCESSED"])
def test_legacy_success_state_skips_without_new_schema_stage(
    stage_harness, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    paths, period, calls, _ = stage_harness
    store = StateStore(paths.state_dir)
    legacy_stages = {
        stage: {"status": "ADOPTED" if action == "BOOTSTRAPPED" else "SUCCESS"}
        for stage in ("ingestion", "bronze", "silver", "gold", "geographic")
    }
    store.save_period(
        PeriodState(
            "yellow",
            period.identifier,
            "SUCCESS",
            action,
            "old",
            "incremental",
            "now",
            stages=legacy_stages,
        )
    )
    monkeypatch.setattr(
        pipeline, "validate_period_artifacts", lambda *_: {"raw_file_size_bytes": 100}
    )
    result = run_period(
        period,
        taxi_type="yellow",
        mode="incremental",
        from_stage="ingestion",
        run_id="current",
        paths=paths,
        store=store,
    )
    assert result.action == "SKIPPED" and result.status == "SUCCESS"
    assert calls == []
    loaded = store.load_period("yellow", period)
    assert loaded is not None and loaded.action == action and loaded.stages == legacy_stages
