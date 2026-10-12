"""Focused integration checks for the bounded advanced chat router."""

from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from chatbot.advanced_engine import AdvancedChatEngine
from chatbot.analytics_ops import Anomaly, AnomalyResult
from chatbot.data import CsvSource
from chatbot.evaluation.fixture import write_fixture
from chatbot.metrics import metric_spec
from chatbot.sql_execution import QueryResult


class AdvancedChatRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        write_fixture(Path(self.temporary.name))
        self.source = CsvSource(Path(self.temporary.name))

    def _add_april(self) -> None:
        """Add one invented April observation without changing the shared fixture."""
        path = Path(self.temporary.name) / "daily_trip_metrics.csv"
        with path.open(newline="", encoding="utf-8") as stream:
            columns = list(csv.DictReader(stream).fieldnames or [])
        with path.open("a", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writerow({"_source_taxi_type": "yellow", "_source_year": 2024,
                             "_source_month": 4, "pickup_date": "2024-04-01",
                             "trip_count": 70, "total_revenue": 1000,
                             "total_tip_amount": 140, "average_fare_amount": 12,
                             "total_trip_distance": 280})
        path = Path(self.temporary.name) / "pickup_zone_performance.csv"
        with path.open(newline="", encoding="utf-8") as stream:
            columns = list(csv.DictReader(stream).fieldnames or [])
        with path.open("a", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writerow({"_source_taxi_type": "yellow", "_source_year": 2024,
                             "_source_month": 4, "borough": "Manhattan",
                             "zone": "Midtown", "pickup_location_id": 101,
                             "trip_count": 33, "total_revenue": 500,
                             "total_tip_amount": 55})

    @staticmethod
    def _ask(engine: AdvancedChatEngine, questions: list[str],
             *, session_id: str | None = None) -> list[dict]:
        answers = []
        for question in questions:
            answer = engine.answer({"question": question, "session_id": session_id})
            answers.append(answer)
            session_id = answer["session_id"]
        return answers

    def test_clear_month_metric_and_comparison_chain(self) -> None:
        self._add_april()
        engine = AdvancedChatEngine(self.source)
        first, second, third = self._ask(engine, [
            "Which month had the most taxi trips?",
            "What were the total tips for that month?",
            "How does that compare with April?",
        ])
        self.assertIn("May 2024", first["answer"])
        self.assertIn("$180.00", second["answer"])
        self.assertIn("April 2024", third["answer"])
        self.assertIn("$140.00", third["answer"])
        self.assertIn("$180.00", third["answer"])
        self.assertIn("$40.00 more", third["answer"])
        self.assertNotIn("TLC total amount", third["answer"])
        self.assertEqual(engine.sessions.resolve(third["session_id"])[1].metric,
                         "tip_amount")

    def test_period_change_keeps_metric_and_multistep_metric_change(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, tips, june, trips = self._ask(engine, [
            "Which month had the most trips?",
            "What were the total tips for that month?",
            "What about June 2024?",
            "And for trips?",
        ])
        self.assertIn("$180.00", tips["answer"])
        self.assertIn("$100.00", june["answer"])
        self.assertIn("June 2024", trips["answer"])
        self.assertIn("50", trips["answer"])
        self.assertEqual(engine.sessions.resolve(trips["session_id"])[1].metric,
                         "trip_count")

    def test_ambiguous_comparison_month_requires_clarification(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first, followup = self._ask(engine, [
            "How did trips change from May to June 2024?",
            "What were recorded tips for that month?",
        ])
        self.assertIn("ambiguous", followup["answer"].lower())
        self.assertFalse(followup["evidence"])
        context = engine.sessions.resolve(first["session_id"])[1]
        self.assertEqual(context.ambiguous_months, ("2024-05", "2024-06"))

    def test_anomaly_referents_replace_stale_comparison_and_can_be_disambiguated(self) -> None:
        engine = AdvancedChatEngine(self.source)
        anomaly = AnomalyResult((
            Anomaly(date(2024, 2, 13), metric_spec("trip_count"), Decimal(10),
                    Decimal(20), Decimal(-10), Decimal(-50), 4, "peer median"),
            Anomaly(date(2024, 1, 7), metric_spec("trip_count"), Decimal(12),
                    Decimal(20), Decimal(-8), Decimal(-40), 4, "peer median"),
        ), ())
        first = engine.answer({"question": "How did trips change from May to June 2024?"})
        with patch("chatbot.advanced_engine.detect_anomalies", return_value=anomaly):
            second = engine.answer({"question": "Which days had unusually low trips?",
                                    "session_id": first["session_id"]})
        context = engine.sessions.resolve(second["session_id"])[1]
        self.assertEqual(context.ambiguous_months, ("2024-02", "2024-01"))
        self.assertEqual(context.dates, ("2024-02-13", "2024-01-07"))
        third, fourth = self._ask(engine, [
            "And what were the total tips for that month?", "February 2024",
        ], session_id=second["session_id"])
        self.assertIn("February 2024, January 2024", third["answer"])
        self.assertIn("$84.00", fourth["answer"])

    def test_unrelated_answer_does_not_reuse_old_month(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first, _, third = self._ask(engine, [
            "Which month had the most trips?", "What was the busiest pickup hour?",
            "How much was recorded in tips that month?",
        ])
        self.assertIn("May", first["answer"])
        self.assertIn("clarify", third["answer"].lower())
        self.assertFalse(third["evidence"])

    def test_unsupported_metric_does_not_default_to_trips(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, unsupported = self._ask(engine, [
            "Which month had the most trips?",
            "What were the total CO2 emissions for that month?",
        ])
        self.assertIn("not published", unsupported["answer"])
        self.assertFalse(unsupported["evidence"])

    def test_new_chat_and_other_session_do_not_inherit_referents(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first = engine.answer({"question": "Which month had the most trips?"})
        other = engine.answer({"question": "What were tips for that month?"})
        reset = engine.answer({"question": "What were tips for that month?",
                               "session_id": first["session_id"], "reset": True})
        self.assertNotEqual(first["session_id"], other["session_id"])
        self.assertNotEqual(first["session_id"], reset["session_id"])
        self.assertIn("clarify", other["answer"].lower())
        self.assertIn("clarify", reset["answer"].lower())

    def test_date_range_and_borough_referents_are_scoped(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, range_tips = self._ask(engine, [
            "How many trips from 2024-01-01 to 2024-01-06?",
            "And for recorded tips?",
        ])
        self.assertIn("$34.00", range_tips["answer"])
        borough, borough_tips = self._ask(engine, [
            "Which borough had the most trips?",
            "What were the total tips for that borough?",
        ])
        self.assertIn("Manhattan", borough["answer"])
        self.assertIn("Manhattan", borough_tips["answer"])
        self.assertIn("$234.00", borough_tips["answer"])
        may_borough, may_tips = self._ask(engine, [
            "Which borough had the most trips in May 2024?",
            "What were the total tips for that borough?",
        ])
        self.assertIn("Manhattan", may_borough["answer"])
        self.assertIn("May 2024", may_tips["answer"])
        self.assertIn("$110.00", may_tips["answer"])

    def test_date_range_does_not_become_whole_month_by_pronoun(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, followup = self._ask(engine, [
            "How many trips from 2024-01-01 to 2024-01-06?",
            "How much was recorded in tips that month?",
        ])
        self.assertIn("clarify", followup["answer"].lower())
        self.assertFalse(followup["evidence"])

    def test_two_date_ranges_keep_exact_boundaries_for_new_metric(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first, tips = self._ask(engine, [
            "Compare trips from 2024-01-01 to 2024-01-06 and "
            "from 2024-02-01 to 2024-02-03",
            "And for recorded tips?",
        ])
        self.assertIn("2024-01-01 to 2024-01-06", first["answer"])
        self.assertIn("2024-01-01 to 2024-01-06: $34.00", tips["answer"])
        self.assertIn("2024-02-01 to 2024-02-03: $84.00", tips["answer"])
        self.assertEqual(len(engine.sessions.resolve(tips["session_id"])[1].date_ranges), 2)

    def test_borough_remains_scoped_across_period_and_metric_followups(self) -> None:
        self._add_april()
        engine = AdvancedChatEngine(self.source)
        first, comparison, april, trips = self._ask(engine, [
            "What were recorded tips in Manhattan in May 2024?",
            "How does that compare with April?",
            "What about April?",
            "And for trips?",
        ])
        self.assertIn("$110.00", first["answer"])
        self.assertIn("$55.00", comparison["answer"])
        self.assertIn("$110.00", comparison["answer"])
        self.assertIn("$55.00", april["answer"])
        self.assertIn("33", trips["answer"])
        self.assertNotIn("70 trips", trips["answer"])
        self.assertEqual(engine.sessions.resolve(trips["session_id"])[1].borough,
                         "Manhattan")

    def test_single_zone_metric_followup_uses_exact_zone_id(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first, tips, june = self._ask(engine, [
            "How many Midtown pickup trips in May 2024?",
            "And for recorded tips?",
            "What about June 2024?",
        ])
        self.assertIn("Midtown", first["answer"])
        self.assertIn("$110.00", tips["answer"])
        self.assertNotIn("$180.00", tips["answer"])
        self.assertIn("Midtown", june["answer"])
        self.assertIn("$40.00", june["answer"])
        self.assertEqual(engine.sessions.resolve(june["session_id"])[1].zones, ("101",))

    def test_zone_followup_does_not_silently_reuse_id_for_another_zone(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, reply = self._ask(engine, [
            "How many Midtown pickup trips in May 2024?",
            "What about Astoria in June 2024?",
        ])
        self.assertIn("different pickup zone", reply["answer"].lower())
        self.assertFalse(reply["evidence"])

    def test_singular_month_after_zone_comparison_is_ambiguous(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first, reply, clarified = self._ask(engine, [
            "Compare Midtown pickup trips from May to June 2024",
            "What were recorded tips for that month?",
            "May 2024",
        ])
        self.assertIn("Midtown", first["answer"])
        self.assertIn("which month", reply["answer"].lower())
        self.assertFalse(reply["evidence"])
        self.assertIn("$110.00", clarified["answer"])
        self.assertIn("Midtown", clarified["answer"])

    def test_singular_month_after_unscoped_zone_answer_is_ambiguous(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first, reply, clarified = self._ask(engine, [
            "How many Midtown pickup trips?",
            "What were recorded tips for that month?",
            "May 2024",
        ])
        self.assertIn("Midtown", first["answer"])
        self.assertIn("which month", reply["answer"].lower())
        self.assertFalse(reply["evidence"])
        self.assertIn("$110.00", clarified["answer"])

    def test_multiple_zones_clarify_metric_followup(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, followup = self._ask(engine, [
            "What are the top 3 pickup zones in May 2024?",
            "And for recorded tips?",
        ])
        self.assertIn("which pickup zone", followup["answer"].lower())
        self.assertFalse(followup["evidence"])

    def test_zone_scope_clears_after_unrelated_answer(self) -> None:
        engine = AdvancedChatEngine(self.source)
        zone, unrelated, followup = self._ask(engine, [
            "How many Midtown pickup trips in May 2024?",
            "What was the busiest pickup hour?",
            "And for recorded tips?",
        ])
        self.assertIn("Midtown", zone["answer"])
        self.assertNotIn("Midtown", unrelated["answer"])
        self.assertIn("clarify", followup["answer"].lower())
        self.assertFalse(followup["evidence"])

    def test_peer_median_anomaly_is_not_unsupported_metric(self) -> None:
        engine = AdvancedChatEngine(self.source)
        answer = engine.answer({
            "question": "Which days had unusually low trips versus the peer median?",
        })
        self.assertNotIn("not published", answer["answer"].lower())

    def test_specific_metric_outweighs_trip_word_in_followup(self) -> None:
        engine = AdvancedChatEngine(self.source)
        _, distance = self._ask(engine, [
            "Which month had the most trips?",
            "And for average trip distance?",
        ])
        self.assertIn("May 2024", distance["answer"])
        self.assertIn("miles per trip", distance["answer"])
        self.assertNotIn("90 trips", distance["answer"])

    def test_share_change_is_in_percentage_points(self) -> None:
        answer = AdvancedChatEngine(self.source).answer({
            "question": "How did credit card share change from May to June 2024?",
        })
        self.assertIn("-10.0 percentage points", answer["answer"])
        self.assertEqual(answer["evidence"][0]["source"],
                         self.source.label("payment_type_summary"))

    def test_monthly_comparison_keeps_context_for_tip_followup(self) -> None:
        engine = AdvancedChatEngine(self.source)
        first = engine.answer({"question": "How did trips change from May to June 2024?"})
        second = engine.answer({"question": "And for recorded tips?",
                                "session_id": first["session_id"]})
        self.assertIn("$180.00", second["answer"])
        self.assertIn("$100.00", second["answer"])

    def test_explicit_date_range_counts_only_observed_days(self) -> None:
        answer = AdvancedChatEngine(self.source).answer({
            "question": "How many trips from 2024-01-01 to 2024-01-06?",
        })
        self.assertIn("30 trips", answer["answer"])
        self.assertIn("2/6 calendar days observed", answer["answer"])

    def test_date_range_refuses_unavailable_borough_grain(self) -> None:
        answer = AdvancedChatEngine(self.source).answer({
            "question": "How many Manhattan trips from 2024-01-01 to 2024-01-06?",
        })
        self.assertIn("cannot be combined", answer["answer"])
        self.assertFalse(answer["evidence"])

    def test_model_sql_is_recompiled_before_executor_and_not_used_as_answer(self) -> None:
        class FakeExecutor:
            def __init__(self) -> None:
                self.received = None

            def execute(self, query):
                self.received = query
                return QueryResult("payment_type_summary", ("payment_type", "trips"),
                                   ({"payment_type": 1, "trips": 176},), 2.5)

        executor = FakeExecutor()
        engine = AdvancedChatEngine(self.source, sql_executor=executor)
        proposal = ("SELECT payment_type, SUM(trip_count) AS trips "
                    "FROM analytics.payment_type_summary GROUP BY payment_type "
                    "ORDER BY trips DESC LIMIT 5")
        with patch("chatbot.advanced_engine.propose_analytic_sql", return_value=proposal):
            answer = engine.answer({"question": "Group by payment method"})
        self.assertTrue(answer["ai_used"])
        self.assertIn("176", answer["answer"])
        self.assertIn("_source_taxi_type = %s", executor.received.sql)
        self.assertEqual(executor.received.params[0], "yellow")
        self.assertNotIn("model answer", answer["answer"])

    def test_unsafe_model_sql_never_reaches_executor(self) -> None:
        class ExplodingExecutor:
            def execute(self, _query):
                raise AssertionError("Unsafe SQL must not execute")

        engine = AdvancedChatEngine(self.source, sql_executor=ExplodingExecutor())
        with patch("chatbot.advanced_engine.propose_analytic_sql",
                   return_value="SELECT * FROM pg_catalog.pg_user"):
            answer = engine.answer({"question": "Group by some taxi metric"})
        self.assertIn("rejected", answer["answer"])
        self.assertFalse(answer["ai_used"])

    def test_model_cannot_invent_a_month_filter(self) -> None:
        class ExplodingExecutor:
            def execute(self, _query):
                raise AssertionError("Invented filter must not execute")

        engine = AdvancedChatEngine(self.source, sql_executor=ExplodingExecutor())
        proposal = ("SELECT payment_type, SUM(trip_count) AS trips "
                    "FROM analytics.payment_type_summary "
                    "WHERE _source_month = 5 GROUP BY payment_type")
        with patch("chatbot.advanced_engine.propose_analytic_sql",
                   return_value=proposal):
            answer = engine.answer({"question": "Group by payment method"})
        self.assertIn("filter", answer["answer"])
        self.assertFalse(answer["ai_used"])

    def test_model_cannot_answer_payment_grouping_with_hourly_grouping(self) -> None:
        class ExplodingExecutor:
            def execute(self, _query):
                raise AssertionError("Wrong grouping must not execute")

        engine = AdvancedChatEngine(self.source, sql_executor=ExplodingExecutor())
        proposal = ("SELECT pickup_hour, SUM(trip_count) AS trips "
                    "FROM analytics.hourly_demand GROUP BY pickup_hour")
        with patch("chatbot.advanced_engine.propose_analytic_sql",
                   return_value=proposal):
            answer = engine.answer({"question": "Group by payment method"})
        self.assertIn("grouping did not match", answer["answer"])


if __name__ == "__main__":
    unittest.main()
