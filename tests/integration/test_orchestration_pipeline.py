"""Focused operational tests for incremental state decisions."""

from pathlib import Path

import pytest

from nyc_taxi_lakehouse.orchestration.pipeline import PipelinePaths, run_period
from nyc_taxi_lakehouse.orchestration.state import PeriodState, ProcessingPeriod, StateStore


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
