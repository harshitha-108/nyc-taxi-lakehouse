"""Dependency-aware local orchestration for incremental periods, replay, and backfill."""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from nyc_taxi_lakehouse.bronze.processor import (
    BronzeRequest,
    create_spark_session,
    write_bronze_partition,
)
from nyc_taxi_lakehouse.gold.geographic import process_geographic_enrichment
from nyc_taxi_lakehouse.gold.processor import (
    GOLD_DATASETS,
    GoldRequest,
    gold_partition_path,
    process_gold_partition,
)
from nyc_taxi_lakehouse.ingestion.nyc_taxi import (
    TaxiDataRequest,
    ingest_taxi_data,
    raw_data_path,
    validate_parquet,
)
from nyc_taxi_lakehouse.orchestration.state import (
    PeriodState,
    ProcessingPeriod,
    StateStore,
    period_range,
)
from nyc_taxi_lakehouse.reference.taxi_zones import (
    ingest_taxi_zones,
    taxi_zone_csv_path,
    validate_taxi_zone_csv,
)
from nyc_taxi_lakehouse.schema.validator import Compatibility
from nyc_taxi_lakehouse.schema.validator import validate_and_report as validate_schema
from nyc_taxi_lakehouse.silver.processor import (
    SilverRequest,
    bronze_partition_path,
    process_silver_partition,
    quarantine_partition_path,
    silver_partition_path,
)

LOGGER = logging.getLogger(__name__)
STAGES = ("ingestion", "schema_validation", "bronze", "silver", "gold", "geographic")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


class PipelineError(Exception):
    """Raised for explicit orchestration validation or stage failures."""


@dataclass(frozen=True)
class PipelinePaths:
    raw_dir: Path = Path("data/raw")
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    quarantine_dir: Path = Path("data/quarantine")
    gold_dir: Path = Path("data/gold")
    reference_dir: Path = Path("data/reference")
    state_dir: Path = Path("data/state")


@dataclass(frozen=True)
class PeriodRunResult:
    period: ProcessingPeriod
    action: str
    status: str
    metrics: dict[str, object]
    elapsed_seconds: float


def _directory_has_parquet(path: Path) -> bool:
    return path.is_dir() and any(path.glob("part-*.parquet"))


def validate_period_artifacts(
    period: ProcessingPeriod, taxi_type: str, paths: PipelinePaths
) -> dict[str, object]:
    """Perform lightweight validation suitable for adopting/skipping an already-complete period."""
    request = TaxiDataRequest(taxi_type, period.year, period.month)
    raw_path = raw_data_path(request, paths.raw_dir)
    validate_parquet(raw_path)
    bronze_path = bronze_partition_path(
        SilverRequest(taxi_type, period.year, period.month), paths.bronze_dir
    )
    silver_path = silver_partition_path(
        SilverRequest(taxi_type, period.year, period.month), paths.silver_dir
    )
    quarantine_path = quarantine_partition_path(
        SilverRequest(taxi_type, period.year, period.month), paths.quarantine_dir
    )
    if not all(
        (
            _directory_has_parquet(bronze_path),
            _directory_has_parquet(silver_path),
            _directory_has_parquet(quarantine_path),
        )
    ):
        raise PipelineError(
            f"Required Bronze/Silver artifacts are incomplete for {period.identifier}."
        )
    for dataset_name in (*GOLD_DATASETS, "pickup_zone_performance"):
        if not _directory_has_parquet(
            gold_partition_path(
                GoldRequest(taxi_type, period.year, period.month), paths.gold_dir, dataset_name
            )
            if dataset_name in GOLD_DATASETS
            else paths.gold_dir
            / dataset_name
            / taxi_type
            / f"year={period.year}"
            / f"month={period.month:02d}"
        ):
            raise PipelineError(
                f"Required Gold artifact {dataset_name} is incomplete for {period.identifier}."
            )
    return {"raw_file_size_bytes": raw_path.stat().st_size}


def _stage_state(
    status: str, started: float, metrics: dict[str, object] | None = None
) -> dict[str, object]:
    return {
        "status": status,
        "duration_seconds": round(time.monotonic() - started, 3),
        "metrics": metrics or {},
    }


def run_period(
    period: ProcessingPeriod,
    *,
    taxi_type: str,
    mode: str,
    from_stage: str,
    run_id: str,
    paths: PipelinePaths,
    store: StateStore,
) -> PeriodRunResult:
    """Run one period, or safely adopt/skip a complete existing period in incremental mode."""
    started_at = time.monotonic()
    previous = store.load_period(taxi_type, period)
    if mode == "incremental" and previous and previous.status == "SUCCESS":
        try:
            metrics = validate_period_artifacts(period, taxi_type, paths)
        except Exception:
            LOGGER.warning(
                "State for %s is stale or incomplete; retrying period.", period.identifier
            )
        else:
            return PeriodRunResult(
                period, "SKIPPED", "SUCCESS", metrics, time.monotonic() - started_at
            )
    if mode == "incremental" and previous is None:
        try:
            metrics = validate_period_artifacts(period, taxi_type, paths)
        except Exception:
            pass
        else:
            state = PeriodState(
                taxi_type,
                period.identifier,
                "SUCCESS",
                "BOOTSTRAPPED",
                run_id,
                mode,
                StateStore.now(),
                StateStore.now(),
                {stage: {"status": "ADOPTED"} for stage in STAGES},
                metrics,
            )
            store.save_period(state)
            return PeriodRunResult(
                period, "BOOTSTRAPPED", "SUCCESS", metrics, time.monotonic() - started_at
            )

    state = PeriodState(
        taxi_type,
        period.identifier,
        "RUNNING",
        "REPLAYED" if mode == "replay" else "PROCESSED",
        run_id,
        mode,
        StateStore.now(),
        stages={stage: {"status": "PENDING"} for stage in STAGES},
    )
    store.save_period(state)
    metrics: dict[str, object] = {}
    spark = None
    try:
        stage_start = time.monotonic()
        request = TaxiDataRequest(taxi_type, period.year, period.month)
        if STAGE_INDEX[from_stage] <= STAGE_INDEX["ingestion"]:
            ingestion = ingest_taxi_data(request, paths.raw_dir)
            metrics["raw_file_size_bytes"] = ingestion.file_size_bytes
            state.stages["ingestion"] = _stage_state("SUCCESS", stage_start, asdict(ingestion))
        else:
            validate_parquet(raw_data_path(request, paths.raw_dir))
            state.stages["ingestion"] = {"status": "SKIPPED"}

        if STAGE_INDEX[from_stage] <= STAGE_INDEX["bronze"]:
            schema_result = validate_schema(
                raw_data_path(request, paths.raw_dir), period, taxi_type, paths.state_dir
            )
            if schema_result["compatibility"] == Compatibility.BREAKING:
                state.stages["schema_validation"] = {"status": "FAILED", "metrics": schema_result}
                raise PipelineError(
                    f"Breaking schema contract for {period.identifier}: {schema_result['changes']}"
                )
            state.stages["schema_validation"] = {"status": "SUCCESS", "metrics": schema_result}
        else:
            state.stages["schema_validation"] = {"status": "SKIPPED"}

        spark = create_spark_session("nyc-taxi-multi-month")
        stage_start = time.monotonic()
        if STAGE_INDEX[from_stage] <= STAGE_INDEX["bronze"]:
            bronze = write_bronze_partition(
                spark,
                BronzeRequest(taxi_type, period.year, period.month),
                raw_dir=paths.raw_dir,
                bronze_dir=paths.bronze_dir,
            )
            metrics["bronze_rows"] = bronze.bronze_rows
            state.stages["bronze"] = _stage_state("SUCCESS", stage_start, asdict(bronze))
        else:
            if not _directory_has_parquet(
                bronze_partition_path(
                    SilverRequest(taxi_type, period.year, period.month), paths.bronze_dir
                )
            ):
                raise PipelineError(f"Bronze prerequisite is missing for {period.identifier}.")
            state.stages["bronze"] = {"status": "SKIPPED"}

        stage_start = time.monotonic()
        if STAGE_INDEX[from_stage] <= STAGE_INDEX["silver"]:
            silver = process_silver_partition(
                spark,
                SilverRequest(taxi_type, period.year, period.month),
                bronze_dir=paths.bronze_dir,
                silver_dir=paths.silver_dir,
                quarantine_dir=paths.quarantine_dir,
            )
            metrics.update(
                {"silver_valid_rows": silver.valid_rows, "quarantine_rows": silver.rejected_rows}
            )
            state.stages["silver"] = _stage_state("SUCCESS", stage_start, asdict(silver))
        else:
            if not _directory_has_parquet(
                silver_partition_path(
                    SilverRequest(taxi_type, period.year, period.month), paths.silver_dir
                )
            ):
                raise PipelineError(f"Silver prerequisite is missing for {period.identifier}.")
            state.stages["silver"] = {"status": "SKIPPED"}

        stage_start = time.monotonic()
        if STAGE_INDEX[from_stage] <= STAGE_INDEX["gold"]:
            gold = process_gold_partition(
                spark,
                GoldRequest(taxi_type, period.year, period.month),
                silver_dir=paths.silver_dir,
                gold_dir=paths.gold_dir,
            )
            metrics["gold_reconciliation_rows"] = gold.input_rows
            state.stages["gold"] = _stage_state("SUCCESS", stage_start, asdict(gold))
        else:
            state.stages["gold"] = {"status": "SKIPPED"}

        stage_start = time.monotonic()
        validate_taxi_zone_csv(taxi_zone_csv_path(paths.reference_dir))
        geographic = process_geographic_enrichment(
            spark,
            GoldRequest(taxi_type, period.year, period.month),
            gold_dir=paths.gold_dir,
            reference_dir=paths.reference_dir,
        )
        metrics["geographic_match_percentage"] = geographic.match_percentage
        state.stages["geographic"] = _stage_state("SUCCESS", stage_start, asdict(geographic))
        state.status, state.completed_at, state.metrics = "SUCCESS", StateStore.now(), metrics
        store.save_period(state)
        return PeriodRunResult(
            period, state.action, "SUCCESS", metrics, time.monotonic() - started_at
        )
    except Exception as exc:
        state.status, state.completed_at, state.metrics = "FAILED", StateStore.now(), metrics
        pending = next(
            (stage for stage in STAGES if state.stages[stage]["status"] == "PENDING"), None
        )
        if pending and not any(stage.get("status") == "FAILED" for stage in state.stages.values()):
            state.stages[pending] = {"status": "FAILED", "error": str(exc)}
        store.save_period(state)
        raise PipelineError(f"Period {period.identifier} failed: {exc}") from exc
    finally:
        if spark is not None:
            spark.stop()


def run_pipeline(
    start: ProcessingPeriod,
    end: ProcessingPeriod,
    *,
    taxi_type: str = "yellow",
    mode: str = "incremental",
    from_stage: str = "ingestion",
    paths: PipelinePaths | None = None,
) -> list[PeriodRunResult]:
    """Run independent periods, continuing after failures and writing run history atomically."""
    if mode not in ("incremental", "replay") or from_stage not in STAGE_INDEX:
        raise PipelineError(
            "Mode must be incremental/replay and from-stage must be a pipeline stage."
        )
    active_paths = paths or PipelinePaths()
    run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
    store = StateStore(active_paths.state_dir)
    ingest_taxi_zones(reference_dir=active_paths.reference_dir)
    results: list[PeriodRunResult] = []
    failures: list[str] = []
    for period in period_range(start, end):
        try:
            results.append(
                run_period(
                    period,
                    taxi_type=taxi_type,
                    mode=mode,
                    from_stage=from_stage,
                    run_id=run_id,
                    paths=active_paths,
                    store=store,
                )
            )
        except PipelineError as exc:
            LOGGER.error("%s", exc)
            failures.append(str(exc))
            results.append(PeriodRunResult(period, "FAILED", "FAILED", {}, 0.0))
    store.save_run(
        run_id,
        {
            "run_id": run_id,
            "mode": mode,
            "from_stage": from_stage,
            "start": start.identifier,
            "end": end.identifier,
            "status": "PARTIAL_FAILURE" if failures else "SUCCESS",
            "results": [asdict(result) for result in results],
            "failures": failures,
        },
    )
    if failures:
        raise PipelineError(f"Run {run_id} completed with {len(failures)} failed period(s).")
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run incremental or replayed NYC Taxi monthly pipeline periods."
    )
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--mode", default="incremental", choices=("incremental", "replay"))
    parser.add_argument("--from-stage", default="ingestion", choices=STAGES)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        results = run_pipeline(
            ProcessingPeriod.parse(args.start),
            ProcessingPeriod.parse(args.end),
            taxi_type=args.taxi_type,
            mode=args.mode,
            from_stage=args.from_stage,
        )
    except (PipelineError, ValueError) as exc:
        LOGGER.error("Pipeline failed: %s", exc)
        return 1
    LOGGER.info(
        "Pipeline complete: processed=%s skipped=%s",
        sum(r.action not in ("SKIPPED", "BOOTSTRAPPED") for r in results),
        sum(r.action in ("SKIPPED", "BOOTSTRAPPED") for r in results),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
