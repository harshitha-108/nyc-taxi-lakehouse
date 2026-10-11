"""Deterministic calculations over approved, already-retrieved mart rows.

These helpers never execute SQL or call an LLM. They preserve missing-value and
partial-period caveats so an answer renderer cannot quietly present incomplete
data as a verified full-period result.
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any

from chatbot.metrics import MetricSpec, metric_spec


class AnalyticsError(ValueError):
    """Input rows or a proposed analytical plan are invalid or unsupported."""


@dataclass(frozen=True)
class MetricValue:
    metric: MetricSpec
    value: Decimal | None
    included_rows: int
    skipped_rows: int
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class PeriodCoverage:
    period: tuple[int, int]
    observed_days: int
    expected_days: int
    complete: bool | None
    out_of_period_rows: int
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class Comparison:
    baseline: MetricValue
    current: MetricValue
    difference: Decimal | None
    percentage_change: Decimal | None
    percentage_point_difference: Decimal | None
    baseline_period: tuple[int, int] | None
    current_period: tuple[int, int] | None
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class TrendPoint:
    period: tuple[int, int]
    metric: MetricValue
    change_from_previous_pct: Decimal | None
    rolling_mean: Decimal | None
    coverage: PeriodCoverage
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class WeekdayWeekend:
    metric: MetricSpec
    weekday_daily_average: Decimal | None
    weekend_daily_average: Decimal | None
    weekday_days: int
    weekend_days: int
    weekend_minus_weekday: Decimal | None
    weekend_vs_weekday_pct: Decimal | None
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class ShareComparison:
    baseline_share_pct: Decimal | None
    current_share_pct: Decimal | None
    percentage_point_difference: Decimal | None
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class Anomaly:
    pickup_date: date
    metric: MetricSpec
    observed: Decimal
    baseline: Decimal
    difference: Decimal
    percentage_change: Decimal
    peer_days: int
    method: str


@dataclass(frozen=True)
class AnomalyResult:
    anomalies: tuple[Anomaly, ...]
    caveats: tuple[str, ...]


@dataclass(frozen=True)
class PlanStep:
    """One bounded operation; only ``query`` may consume a database query."""

    kind: str
    metric: str
    result_limit: int = 100


_STEP_KINDS = frozenset({"query", "aggregate", "compare", "trend", "anomaly"})


def bounded_steps(
    steps: Iterable[PlanStep], *, max_steps: int = 4,
    max_queries: int = 3, max_result_rows: int = 500,
) -> tuple[PlanStep, ...]:
    """Fail closed on agent-style plans exceeding fixed work and row budgets."""
    if not (1 <= max_steps <= 8 and 1 <= max_queries <= max_steps
            and 1 <= max_result_rows <= 1000):
        raise AnalyticsError("Invalid plan budget")
    plan = tuple(steps)
    if not plan or len(plan) > max_steps:
        raise AnalyticsError(f"A question may use at most {max_steps} analytical steps")
    if any(not isinstance(step, PlanStep) or step.kind not in _STEP_KINDS
           for step in plan):
        raise AnalyticsError("Unsupported analytical step")
    if sum(step.kind == "query" for step in plan) > max_queries:
        raise AnalyticsError(f"A question may use at most {max_queries} queries")
    for step in plan:
        metric_spec(step.metric)
        if not 1 <= step.result_limit <= max_result_rows:
            raise AnalyticsError(f"Each step is limited to {max_result_rows} result rows")
    return plan


def _decimal(value: Any, *, column: str) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise AnalyticsError(f"Invalid {column} value")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise AnalyticsError(f"Invalid {column} value") from exc
    if not result.is_finite():
        raise AnalyticsError(f"Non-finite {column} value")
    return result


def _column(row: Mapping[str, Any], name: str) -> Any:
    if name not in row:
        raise AnalyticsError(f"Published row is missing {name}")
    return row[name]


def _count(row: Mapping[str, Any]) -> Decimal | None:
    value = _decimal(_column(row, "trip_count"), column="trip_count")
    if value is not None and (value < 0 or value != value.to_integral_value()):
        raise AnalyticsError("trip_count must be a non-negative integer")
    return value


def _unique(notes: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(notes))


def aggregate_metric(rows: Sequence[Mapping[str, Any]], key: str) -> MetricValue:
    """Aggregate one catalogued metric without converting missing values to zero."""
    spec = metric_spec(key)
    total = Decimal(0)
    denominator = Decimal(0)
    included = skipped = 0
    missing_distance = 0
    distance_seen = False
    for row in rows:
        if spec.key in {"estimated_fare_amount", "average_fare_amount"}:
            count = _count(row)
            value = _decimal(_column(row, "average_fare_amount"),
                             column="average_fare_amount")
            if count is None or count <= 0 or value is None:
                skipped += 1
                continue
            total += value * count
            denominator += count
        elif spec.key == "average_trip_distance":
            count = _count(row)
            value = _decimal(_column(row, "total_trip_distance"),
                             column="total_trip_distance")
            if count is None or count <= 0:
                skipped += 1
                continue
            denominator += count
            if value is None:
                missing_distance += 1
            else:
                total += value
                distance_seen = True
        else:
            name = spec.source_columns[0]
            value = (_count(row) if name == "trip_count"
                     else _decimal(_column(row, name), column=name))
            if value is None:
                skipped += 1
                continue
            total += value
        included += 1
    if (not included or (not spec.additive and denominator == 0)
            or (spec.key == "average_trip_distance" and not distance_seen)):
        result = None
    elif spec.additive:
        result = total
    else:
        result = total / denominator
    notes = []
    if spec.caveat:
        notes.append(spec.caveat)
    if skipped:
        notes.append(f"{skipped} row(s) with null or zero-weight values were excluded.")
    if missing_distance:
        notes.append(f"{missing_distance} row(s) with null distance contributed trips "
                     "to the denominator but no distance to the numerator.")
    if result is None:
        notes.append("No usable observations; the result is unavailable, not zero.")
    return MetricValue(spec, result, included, skipped, tuple(notes))


def _period(row: Mapping[str, Any]) -> tuple[int, int]:
    try:
        year = int(_column(row, "_source_year"))
        month = int(_column(row, "_source_month"))
        if not (2000 <= year <= 2100 and 1 <= month <= 12):
            raise ValueError
        return year, month
    except (TypeError, ValueError) as exc:
        raise AnalyticsError("Invalid published source period") from exc


def _date(row: Mapping[str, Any]) -> date:
    value = _column(row, "pickup_date")
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise AnalyticsError("Invalid pickup_date") from exc


def period_coverage(rows: Sequence[Mapping[str, Any]], period: tuple[int, int]) -> PeriodCoverage:
    """Report observed calendar days; this cannot prove record completeness.

    Source-month partitions may include pickup dates just outside that month.
    Those spillover dates merit a scope caveat, but are not evidence that a
    calendar day *within* the source month is missing.
    """
    year, month = period
    if (isinstance(year, bool) or isinstance(month, bool)
            or not isinstance(year, int) or not isinstance(month, int)
            or not 2000 <= year <= 2100):
        raise AnalyticsError("Invalid comparison period")
    try:
        expected = calendar.monthrange(year, month)[1]
    except (ValueError, TypeError) as exc:
        raise AnalyticsError("Invalid comparison period") from exc
    if not rows or "pickup_date" not in rows[0]:
        return PeriodCoverage(period, 0, expected, None, 0,
                              ("Calendar-day completeness cannot be verified at this grain.",))
    dates: set[date] = set()
    outside = 0
    for row in rows:
        pickup_day = _date(row)
        if _period(row) != period or (pickup_day.year, pickup_day.month) != period:
            outside += 1
        else:
            dates.add(pickup_day)
    complete = len(dates) == expected
    notes = []
    if len(dates) < expected:
        notes.append(f"Only {len(dates)} of {expected} calendar days are present.")
    if outside:
        notes.append(
            f"{outside} row(s) have pickup dates outside their source month; "
            "totals still follow the published source-month partition."
        )
    return PeriodCoverage(period, len(dates), expected, complete, outside, tuple(notes))


def compare_values(
    baseline: MetricValue, current: MetricValue, *,
    baseline_period: tuple[int, int] | None = None,
    current_period: tuple[int, int] | None = None,
    baseline_coverage: PeriodCoverage | None = None,
    current_coverage: PeriodCoverage | None = None,
) -> Comparison:
    """Compute observed difference; avoid percent math on missing/zero/partial data."""
    if baseline.metric.key != current.metric.key:
        raise AnalyticsError("Comparison metrics must match")
    notes = [*baseline.caveats, *current.caveats]
    if baseline_coverage:
        notes.extend(baseline_coverage.caveats)
    if current_coverage:
        notes.extend(current_coverage.caveats)
    if baseline.value is None or current.value is None:
        return Comparison(baseline, current, None, None, None,
                          baseline_period, current_period, _unique(notes))
    difference = current.value - baseline.value
    partial = any(item is not None and item.complete is False
                  for item in (baseline_coverage, current_coverage))
    if partial:
        notes.append("Percentage change is withheld because a compared period is incomplete.")
    if baseline.metric.unit == "%":
        return Comparison(baseline, current, difference, None, difference,
                          baseline_period, current_period, _unique(notes))
    if baseline.value <= 0:
        notes.append("Percentage change is undefined for a zero or negative baseline.")
        pct = None
    elif partial:
        pct = None
    else:
        pct = difference * Decimal(100) / baseline.value
    return Comparison(baseline, current, difference, pct, None,
                      baseline_period, current_period, _unique(notes))


def compare_periods(
    baseline_rows: Sequence[Mapping[str, Any]], current_rows: Sequence[Mapping[str, Any]],
    key: str, *, baseline_period: tuple[int, int], current_period: tuple[int, int],
) -> Comparison:
    """Compare two explicit source periods, preserving partial-period warnings."""
    if baseline_period == current_period:
        raise AnalyticsError("Comparison periods must be different")
    for row in baseline_rows:
        if _period(row) != baseline_period:
            raise AnalyticsError("Baseline contains rows outside its requested source period")
    for row in current_rows:
        if _period(row) != current_period:
            raise AnalyticsError("Current rows contain another source period")
    baseline = aggregate_metric(baseline_rows, key)
    current = aggregate_metric(current_rows, key)
    return compare_values(
        baseline, current, baseline_period=baseline_period,
        current_period=current_period,
        baseline_coverage=period_coverage(baseline_rows, baseline_period),
        current_coverage=period_coverage(current_rows, current_period),
    )


def compare_shares(
    baseline_part: Decimal | int | str | None,
    baseline_total: Decimal | int | str | None,
    current_part: Decimal | int | str | None,
    current_total: Decimal | int | str | None,
) -> ShareComparison:
    """Compare two shares in percentage *points*, not relative percent change."""
    values = [_decimal(value, column="share component") for value in (
        baseline_part, baseline_total, current_part, current_total)]
    if any(value is None for value in values):
        return ShareComparison(None, None, None,
                               ("A share component is missing; no comparison was calculated.",))
    baseline_part_value, baseline_total_value, current_part_value, current_total_value = values
    assert baseline_part_value is not None and baseline_total_value is not None
    assert current_part_value is not None and current_total_value is not None
    if baseline_total_value <= 0 or current_total_value <= 0:
        return ShareComparison(None, None, None,
                               ("Share denominators must be positive.",))
    if (baseline_part_value < 0 or current_part_value < 0
            or baseline_part_value > baseline_total_value
            or current_part_value > current_total_value):
        raise AnalyticsError("Share numerators must be between zero and their totals")
    baseline_share = baseline_part_value * Decimal(100) / baseline_total_value
    current_share = current_part_value * Decimal(100) / current_total_value
    return ShareComparison(baseline_share, current_share,
                           current_share - baseline_share, ())


def _previous_month(period: tuple[int, int]) -> tuple[int, int]:
    year, month = period
    return (year - 1, 12) if month == 1 else (year, month - 1)


def monthly_trend(
    rows: Sequence[Mapping[str, Any]], key: str, *, rolling_window: int = 3,
) -> tuple[TrendPoint, ...]:
    """Return monthly metrics, consecutive changes, and complete rolling values.

    Additive metrics use the mean of monthly totals. Ratio metrics, such as
    average fare, are recomputed across the window's underlying rows so they
    retain trip weighting rather than averaging monthly averages.
    """
    spec = metric_spec(key)
    if not 2 <= rolling_window <= 6:
        raise AnalyticsError("Rolling window must be between two and six months")
    groups: dict[tuple[int, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_period(row)].append(row)
    output: list[TrendPoint] = []
    for period, group in sorted(groups.items()):
        value = aggregate_metric(group, key)
        coverage = period_coverage(group, period)
        notes = list((*value.caveats, *coverage.caveats))
        change = None
        if output and output[-1].period == _previous_month(period):
            previous = output[-1]
            comparison = compare_values(previous.metric, value,
                                        baseline_coverage=previous.coverage,
                                        current_coverage=coverage)
            change = comparison.percentage_change
            notes.extend(comparison.caveats)
        elif output:
            notes.append("A missing intervening month prevents month-over-month change.")
        recent = output[-rolling_window + 1:] + [
            TrendPoint(period, value, change, None, coverage, ())]
        contiguous = all(recent[index - 1].period == _previous_month(recent[index].period)
                         for index in range(1, len(recent)))
        complete_window = (len(recent) == rolling_window and contiguous
                           and all(item.metric.value is not None
                                   and item.coverage.complete is not False for item in recent))
        if not complete_window:
            rolling = None
        elif spec.additive:
            rolling = sum((item.metric.value for item in recent), Decimal(0)) / rolling_window
        else:
            window_rows = [row for item in recent for row in groups[item.period]]
            rolling = aggregate_metric(window_rows, key).value
        output.append(TrendPoint(period, value, change, rolling, coverage, _unique(notes)))
    return tuple(output)


def weekday_weekend(
    rows: Sequence[Mapping[str, Any]], key: str, *, min_days_per_group: int = 2,
) -> WeekdayWeekend:
    """Compare mean *daily* values, not incomparable five-day/two-day totals."""
    spec = metric_spec(key)
    if not spec.additive or "daily_trip_metrics" not in spec.allowed_marts:
        raise AnalyticsError("Weekday/weekend analysis needs an additive daily metric")
    if min_days_per_group < 2:
        raise AnalyticsError("At least two days per group are required")
    per_day: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    excluded = 0
    for row in rows:
        day = _date(row)
        if (day.year, day.month) != _period(row):
            excluded += 1
            continue
        per_day[day].append(row)
    values: dict[str, list[Decimal]] = {"weekday": [], "weekend": []}
    for day, group in per_day.items():
        metric = aggregate_metric(group, key)
        if metric.value is not None:
            values["weekend" if day.weekday() >= 5 else "weekday"].append(metric.value)
    weekday_days, weekend_days = len(values["weekday"]), len(values["weekend"])
    weekday_avg = (sum(values["weekday"], Decimal(0)) / weekday_days
                   if weekday_days else None)
    weekend_avg = (sum(values["weekend"], Decimal(0)) / weekend_days
                   if weekend_days else None)
    notes = []
    if spec.caveat:
        notes.append(spec.caveat)
    source_groups: dict[tuple[int, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        source_groups[_period(row)].append(row)
    for period, group in sorted(source_groups.items()):
        coverage = period_coverage(group, period)
        if coverage.complete is False:
            notes.append(
                f"{period[0]}-{period[1]:02d} has only {coverage.observed_days}/"
                f"{coverage.expected_days} observed calendar days; averages describe "
                "observed days only."
            )
    if excluded:
        notes.append(f"{excluded} out-of-source-month daily row(s) were excluded.")
    if min(weekday_days, weekend_days) < min_days_per_group:
        notes.append("Too few observed days for a reliable weekday/weekend comparison.")
        difference = pct = None
    else:
        assert weekday_avg is not None and weekend_avg is not None
        difference = weekend_avg - weekday_avg
        pct = difference * Decimal(100) / weekday_avg if weekday_avg > 0 else None
        if weekday_avg <= 0:
            notes.append("Percentage change is undefined for a zero or negative weekday baseline.")
    return WeekdayWeekend(spec, weekday_avg, weekend_avg, weekday_days, weekend_days,
                          difference, pct, tuple(notes))


def detect_anomalies(
    rows: Sequence[Mapping[str, Any]], key: str = "trip_count", *,
    min_peers: int = 4, min_relative_change: Decimal = Decimal("0.25"),
    max_results: int = 5,
) -> AnomalyResult:
    """Flag candidates only when same-weekday peers support a robust baseline.

    A point must differ by at least 25% *and* three scaled median absolute
    deviations from its same-weekday peers. This reports an observation, not a
    cause or forecast. Very small peer groups are not classified.
    """
    spec = metric_spec(key)
    if not spec.additive or "daily_trip_metrics" not in spec.allowed_marts:
        raise AnalyticsError("Anomaly detection needs an additive daily metric")
    if not (4 <= min_peers <= 30 and Decimal("0") < min_relative_change <= Decimal("1")
            and 1 <= max_results <= 10):
        raise AnalyticsError("Invalid anomaly-detection bounds")
    by_day: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    excluded = 0
    for row in rows:
        day = _date(row)
        if (day.year, day.month) != _period(row):
            excluded += 1
            continue
        by_day[day].append(row)
    daily: dict[date, Decimal] = {}
    for day, group in by_day.items():
        value = aggregate_metric(group, key).value
        if value is not None:
            daily[day] = value
    notes = []
    if spec.caveat:
        notes.append(spec.caveat)
    if excluded:
        notes.append(f"{excluded} out-of-source-month daily row(s) were excluded.")
    if len(daily) < min_peers + 1:
        notes.append("Too few daily observations to evaluate unusual patterns.")
        return AnomalyResult((), tuple(notes))
    findings: list[Anomaly] = []
    for day, observed in daily.items():
        peers = [value for peer_day, value in daily.items()
                 if peer_day != day and peer_day.weekday() == day.weekday()]
        if len(peers) < min_peers:
            continue
        baseline = Decimal(median(peers))
        if baseline <= 0:
            continue
        mad = Decimal(median([abs(value - baseline) for value in peers]))
        difference = observed - baseline
        relative = abs(difference) / baseline
        robust_cutoff = Decimal("3") * Decimal("1.4826") * mad
        if relative < min_relative_change or abs(difference) <= robust_cutoff:
            continue
        threshold_percent = min_relative_change * Decimal(100)
        findings.append(Anomaly(
            day, spec, observed, baseline, difference,
            difference * Decimal(100) / baseline, len(peers),
            f"same-weekday peer median; >={threshold_percent}% and >3 scaled MAD",
        ))
    findings.sort(key=lambda item: (-abs(item.percentage_change), item.pickup_date))
    if not findings:
        notes.append("No days crossed both robust and relative-change thresholds.")
    notes.append("Unusual means statistically distinct in this period, not causally explained.")
    return AnomalyResult(tuple(findings[:max_results]), _unique(notes))
