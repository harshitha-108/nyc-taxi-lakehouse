"""Focused integration checks for the bounded advanced chat router."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from chatbot.advanced_engine import AdvancedChatEngine
from chatbot.data import CsvSource
from chatbot.evaluation.fixture import write_fixture
from chatbot.sql_execution import QueryResult


class AdvancedChatRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        write_fixture(Path(self.temporary.name))
        self.source = CsvSource(Path(self.temporary.name))

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
