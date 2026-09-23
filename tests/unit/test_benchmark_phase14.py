"""Deterministic checks for the local-only performance harness."""

import math

import pytest
from scripts.benchmark_phase14 import _paired, percentage_reduction


def test_paired_benchmark_alternates_and_records_each_result() -> None:
    calls: list[tuple[str, str]] = []

    def run(name: str, label: str) -> str:
        calls.append((name, label))
        return f"{name}:{label}"

    measured = _paired({
        "baseline": lambda label: run("baseline", label),
        "candidate": lambda label: run("candidate", label),
    })
    assert calls == [
        ("baseline", "warmup"), ("candidate", "warmup"),
        ("baseline", "1"), ("candidate", "1"),
        ("candidate", "2"), ("baseline", "2"),
        ("baseline", "3"), ("candidate", "3"),
    ]
    assert len(measured["runs_seconds"]["baseline"]) == 3
    assert measured["results"]["candidate"] == [
        "candidate:1", "candidate:2", "candidate:3",
    ]


@pytest.mark.parametrize("repetitions", [0, -1])
def test_paired_benchmark_rejects_invalid_repetitions(repetitions: int) -> None:
    with pytest.raises(ValueError, match="At least one"):
        _paired({"a": lambda _: None, "b": lambda _: None}, repetitions=repetitions)


@pytest.mark.parametrize(("baseline", "candidate", "expected"), [
    (60.0, 45.0, 25.0),
    (60.0, 60.0, 0.0),
    (60.0, 75.0, -25.0),
])
def test_percentage_reduction_handles_improvement_no_change_and_regression(
    baseline: float, candidate: float, expected: float,
) -> None:
    assert percentage_reduction(baseline, candidate) == pytest.approx(expected)


@pytest.mark.parametrize(("baseline", "candidate"), [
    (0.0, 1.0), (-1.0, 1.0), (1.0, 0.0), (math.inf, 1.0), (1.0, math.nan),
])
def test_percentage_reduction_rejects_invalid_times(
    baseline: float, candidate: float,
) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        percentage_reduction(baseline, candidate)
