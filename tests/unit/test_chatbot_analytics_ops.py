"""Hand-calculated contracts for the deterministic Phase 2 analytics layer."""

from __future__ import annotations

import calendar
import unittest
from datetime import date, timedelta
from decimal import Decimal

from chatbot.analytics_ops import (
    AnalyticsError,
    PlanStep,
    aggregate_metric,
    bounded_steps,
    compare_periods,
    compare_shares,
    detect_anomalies,
    monthly_trend,
    period_coverage,
    weekday_weekend,
)
from chatbot.metrics import DIMENSIONS, METRICS, dimension_spec, metric_spec


def _row(day: date, trips: int, **values: object) -> dict[str, object]:
    return {
        "_source_year": day.year, "_source_month": day.month,
        "pickup_date": day.isoformat(), "trip_count": trips,
        "total_revenue": str(trips * 12), "total_tip_amount": str(trips * 2),
        "average_fare_amount": "8.000000", "total_trip_distance": str(trips * 3),
        **values,
    }


def _month(year: int, month: int, trips_each_day: int) -> list[dict[str, object]]:
    return [_row(date(year, month, day), trips_each_day)
            for day in range(1, calendar.monthrange(year, month)[1] + 1)]


class MetricCatalogTests(unittest.TestCase):
    def test_total_amount_is_explicitly_tlc_total_amount(self) -> None:
        spec = metric_spec("total_amount")
        self.assertEqual(spec.source_columns, ("total_revenue",))
        self.assertEqual(spec.sql_aggregate("payment_type_summary"), "SUM(total_revenue)")
        self.assertIn("TLC total_amount", spec.definition)
        self.assertIs(spec, metric_spec("total_revenue"))
        self.assertNotIn("total_fare", METRICS)

    def test_fare_estimate_is_not_offered_at_zone_grain(self) -> None:
        fare = metric_spec("estimated_fare_amount")
        self.assertIn("Estimated", fare.label)
        self.assertIn("rounded", fare.caveat or "")
        with self.assertRaises(ValueError):
            fare.sql_aggregate("pickup_zone_performance")
        with self.assertRaises(ValueError):
            metric_spec("model_invented_measure")

    def test_grouping_dimensions_are_explicitly_tied_to_serving_grain(self) -> None:
        zone = dimension_spec("pickup_zone")
        self.assertEqual(zone.source_columns, ("borough", "zone"))
        self.assertEqual(zone.allowed_marts, frozenset({"pickup_zone_performance"}))
        self.assertIn("payment_type", DIMENSIONS)
        with self.assertRaises(ValueError):
            dimension_spec("dropoff_route")


class DeterministicCalculationTests(unittest.TestCase):
    def test_additive_and_weighted_metrics_have_hand_calculated_values(self) -> None:
        rows = [
            _row(date(2024, 1, 1), 2, average_fare_amount="10.000001",
                 total_trip_distance="6"),
            _row(date(2024, 1, 2), 3, average_fare_amount="20.000000",
                 total_trip_distance="15"),
        ]
        self.assertEqual(aggregate_metric(rows, "trip_count").value, Decimal(5))
        self.assertEqual(aggregate_metric(rows, "total_amount").value, Decimal(60))
        self.assertEqual(aggregate_metric(rows, "tip_amount").value, Decimal(10))
        self.assertEqual(aggregate_metric(rows, "estimated_fare_amount").value,
                         Decimal("80.000002"))
        self.assertEqual(aggregate_metric(rows, "average_fare_amount").value,
                         Decimal("16.0000004"))
        self.assertEqual(aggregate_metric(rows, "average_trip_distance").value,
                         Decimal("4.2"))

    def test_nulls_do_not_become_zero_and_partial_estimate_is_labelled(self) -> None:
        rows = [_row(date(2024, 1, 1), 10, average_fare_amount=None),
                _row(date(2024, 1, 2), 5, average_fare_amount="12")]
        value = aggregate_metric(rows, "average_fare_amount")
        self.assertEqual(value.value, Decimal(12))
        self.assertEqual((value.included_rows, value.skipped_rows), (1, 1))
        self.assertTrue(any("excluded" in note for note in value.caveats))
        self.assertIsNone(aggregate_metric(rows[:1], "average_fare_amount").value)
        self.assertIsNone(aggregate_metric([], "trip_count").value)

    def test_distance_null_uses_powerbi_trip_denominator(self) -> None:
        rows = [_row(date(2024, 1, 1), 2, total_trip_distance="6"),
                _row(date(2024, 1, 2), 3, total_trip_distance=None)]
        value = aggregate_metric(rows, "average_trip_distance")
        self.assertEqual(value.value, Decimal("1.2"))
        self.assertTrue(any("denominator" in note for note in value.caveats))
        self.assertIsNone(aggregate_metric(rows[1:], "average_trip_distance").value)

    def test_bad_or_missing_numbers_fail_closed(self) -> None:
        for bad in ("NaN", "Infinity", "n/a"):
            with self.subTest(bad=bad), self.assertRaises(AnalyticsError):
                aggregate_metric([_row(date(2024, 1, 1), 1, total_revenue=bad)],
                                 "total_amount")
        with self.assertRaises(AnalyticsError):
            aggregate_metric([_row(date(2024, 1, 1), -1)], "trip_count")
        with self.assertRaises(AnalyticsError):
            aggregate_metric([{"trip_count": 3}], "tip_amount")

    def test_full_month_comparison_has_absolute_and_percentage_change(self) -> None:
        january = _month(2024, 1, 10)  # 31 * 10 = 310 trips.
        february = _month(2024, 2, 20)  # Leap-year February: 29 * 20 = 580.
        result = compare_periods(january, february, "trip_count",
                                 baseline_period=(2024, 1), current_period=(2024, 2))
        self.assertEqual(result.difference, Decimal(270))
        self.assertEqual(result.percentage_change, Decimal(27000) / Decimal(310))
        self.assertEqual(result.baseline.value, Decimal(310))
        self.assertEqual(result.current.value, Decimal(580))

    def test_missing_partial_and_zero_baseline_are_not_fabricated(self) -> None:
        january = _month(2024, 1, 10)
        missing = compare_periods(january, [], "trip_count",
                                  baseline_period=(2024, 1), current_period=(2024, 2))
        self.assertIsNone(missing.current.value)
        self.assertIsNone(missing.difference)
        partial = compare_periods(january[:2], _month(2024, 2, 20)[:2], "trip_count",
                                  baseline_period=(2024, 1), current_period=(2024, 2))
        self.assertEqual(partial.difference, Decimal(20))
        self.assertIsNone(partial.percentage_change)
        self.assertTrue(any("incomplete" in note for note in partial.caveats))
        zero = compare_periods(_month(2024, 1, 0), _month(2024, 2, 20), "trip_count",
                               baseline_period=(2024, 1), current_period=(2024, 2))
        self.assertIsNone(zero.percentage_change)
        self.assertTrue(any("zero" in note for note in zero.caveats))

    def test_period_coverage_does_not_hide_out_of_month_rows(self) -> None:
        rows = _month(2024, 1, 10) + [{**_row(date(2024, 2, 1), 5), "_source_month": 1}]
        coverage = period_coverage(rows, (2024, 1))
        self.assertEqual(coverage.observed_days, 31)
        self.assertEqual(coverage.out_of_period_rows, 1)
        self.assertTrue(coverage.complete)
        self.assertTrue(any("source-month partition" in note for note in coverage.caveats))
        with self.assertRaises(AnalyticsError):
            period_coverage(rows, (2024, 13))

    def test_payment_share_difference_uses_percentage_points(self) -> None:
        change = compare_shares(30, 100, 45, 100)
        self.assertEqual(change.baseline_share_pct, Decimal(30))
        self.assertEqual(change.current_share_pct, Decimal(45))
        self.assertEqual(change.percentage_point_difference, Decimal(15))
        self.assertIsNone(compare_shares(1, 0, 2, 10).percentage_point_difference)
        with self.assertRaises(AnalyticsError):
            compare_shares(11, 10, 2, 10)

    def test_trend_needs_adjacent_complete_periods_for_change_and_rolling(self) -> None:
        jan = _month(2024, 1, 10)
        feb = _month(2024, 2, 20)
        mar = _month(2024, 3, 30)
        trend = monthly_trend(jan + feb + mar, "trip_count", rolling_window=3)
        self.assertEqual([point.metric.value for point in trend],
                         [Decimal(310), Decimal(580), Decimal(930)])
        self.assertEqual(trend[2].rolling_mean, Decimal(1820) / Decimal(3))
        self.assertEqual(trend[1].change_from_previous_pct,
                         Decimal(27000) / Decimal(310))
        gap = monthly_trend(jan + mar, "trip_count", rolling_window=2)
        self.assertIsNone(gap[1].change_from_previous_pct)
        self.assertIsNone(gap[1].rolling_mean)
        self.assertTrue(any("missing intervening" in note for note in gap[1].caveats))

    def test_average_fare_rolling_trend_remains_trip_weighted(self) -> None:
        jan = [{**row, "average_fare_amount": "10"} for row in _month(2024, 1, 10)]
        feb = [{**row, "average_fare_amount": "20"} for row in _month(2024, 2, 20)]
        mar = [{**row, "average_fare_amount": "30"} for row in _month(2024, 3, 30)]
        trend = monthly_trend(jan + feb + mar, "average_fare_amount", rolling_window=3)
        self.assertEqual([item.metric.value for item in trend],
                         [Decimal(10), Decimal(20), Decimal(30)])
        self.assertEqual(trend[-1].rolling_mean, Decimal(42600) / Decimal(1820))
        self.assertNotEqual(trend[-1].rolling_mean, Decimal(20))

    def test_weekday_weekend_uses_daily_averages_and_minimum_sample(self) -> None:
        rows = [_row(date(2024, 1, 1), 10), _row(date(2024, 1, 2), 20),
                _row(date(2024, 1, 6), 30), _row(date(2024, 1, 7), 40)]
        pattern = weekday_weekend(rows, "trip_count")
        self.assertEqual(pattern.weekday_daily_average, Decimal(15))
        self.assertEqual(pattern.weekend_daily_average, Decimal(35))
        self.assertEqual(pattern.weekend_minus_weekday, Decimal(20))
        self.assertEqual(pattern.weekend_vs_weekday_pct, Decimal(2000) / Decimal(15))
        small = weekday_weekend(rows[:3], "trip_count")
        self.assertIsNone(small.weekend_vs_weekday_pct)
        self.assertTrue(any("Too few" in note for note in small.caveats))

    def test_anomaly_requires_peer_support_and_explains_baseline(self) -> None:
        mondays = [date(2024, 1, 1) + timedelta(days=7 * n) for n in range(6)]
        rows = [_row(day, 200 if n == 5 else 100) for n, day in enumerate(mondays)]
        result = detect_anomalies(rows)
        self.assertEqual(len(result.anomalies), 1)
        anomaly = result.anomalies[0]
        self.assertEqual((anomaly.pickup_date, anomaly.observed, anomaly.baseline),
                         (mondays[-1], Decimal(200), Decimal(100)))
        self.assertEqual(anomaly.percentage_change, Decimal(100))
        self.assertIn("same-weekday", anomaly.method)
        self.assertFalse(detect_anomalies(rows[:4]).anomalies)

    def test_multi_step_plan_is_bounded_before_execution(self) -> None:
        plan = bounded_steps([PlanStep("query", "trip_count", 100),
                              PlanStep("query", "average_fare_amount", 100),
                              PlanStep("compare", "trip_count", 100)])
        self.assertEqual(len(plan), 3)
        with self.assertRaises(AnalyticsError):
            bounded_steps([PlanStep("query", "trip_count", 501)])
        with self.assertRaises(AnalyticsError):
            bounded_steps([PlanStep("query", "trip_count") for _ in range(4)])
        with self.assertRaises(ValueError):
            bounded_steps([PlanStep("query", "invented_metric")])
        with self.assertRaises(AnalyticsError):
            bounded_steps(["query"])  # type: ignore[list-item]


if __name__ == "__main__":
    unittest.main()
