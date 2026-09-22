"""Atomic, Git-ignored local run and per-period pipeline state."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, order=True)
class ProcessingPeriod:
    """One validated, chronologically sortable monthly processing period."""

    year: int
    month: int

    def __post_init__(self) -> None:
        if not 2009 <= self.year <= 2100:
            raise ValueError("Year must be between 2009 and 2100.")
        if not 1 <= self.month <= 12:
            raise ValueError("Month must be between 1 and 12.")

    @property
    def identifier(self) -> str:
        return f"{self.year}-{self.month:02d}"

    @classmethod
    def parse(cls, value: str) -> ProcessingPeriod:
        try:
            year_text, month_text = value.split("-", maxsplit=1)
            if len(month_text) != 2:
                raise ValueError
            return cls(int(year_text), int(month_text))
        except ValueError as exc:
            raise ValueError(f"Invalid period '{value}'; expected YYYY-MM.") from exc


def period_range(start: ProcessingPeriod, end: ProcessingPeriod) -> list[ProcessingPeriod]:
    """Return an inclusive monthly range, including ranges that cross calendar years."""
    if end < start:
        raise ValueError(f"End period {end.identifier} precedes start period {start.identifier}.")
    periods: list[ProcessingPeriod] = []
    current = start
    while current <= end:
        periods.append(current)
        current = (
            ProcessingPeriod(current.year + 1, 1)
            if current.month == 12
            else ProcessingPeriod(current.year, current.month + 1)
        )
    return periods


@dataclass
class PeriodState:
    """Latest known state for one taxi-type/month, stored separately from immutable run history."""

    taxi_type: str
    period: str
    status: str
    action: str
    latest_run_id: str
    mode: str
    started_at: str
    completed_at: str | None = None
    stages: dict[str, dict[str, object]] = field(default_factory=dict)
    metrics: dict[str, object] = field(default_factory=dict)


class StateStore:
    """Safely persist latest period state plus append-only local run history."""

    def __init__(self, root: Path = Path("data/state")) -> None:
        self.root = root

    def _period_path(self, taxi_type: str, period: ProcessingPeriod) -> Path:
        return self.root / "periods" / taxi_type / f"{period.identifier}.json"

    def _run_path(self, run_id: str) -> Path:
        return self.root / "runs" / f"{run_id}.json"

    @staticmethod
    def _write_json(path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_suffix(".json.part")
        temporary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, path)

    def load_period(self, taxi_type: str, period: ProcessingPeriod) -> PeriodState | None:
        path = self._period_path(taxi_type, period)
        if not path.is_file():
            return None
        return PeriodState(**json.loads(path.read_text(encoding="utf-8")))

    def save_period(self, state: PeriodState) -> None:
        self._write_json(
            self._period_path(state.taxi_type, ProcessingPeriod.parse(state.period)), asdict(state)
        )

    def save_run(self, run_id: str, payload: dict[str, object]) -> None:
        self._write_json(self._run_path(run_id), payload)

    @staticmethod
    def now() -> str:
        return datetime.now(UTC).isoformat()
