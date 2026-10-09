"""Small contract tests for the report chat without live services or model downloads."""

from __future__ import annotations

import csv
import io
import json
import os
import sys
import tempfile
import threading
import types
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import patch

from chatbot.data import TABLES, CsvSource, PostgresSource, SourceError
from chatbot.engine import ChatEngine, InvalidRequest, parse_month
from chatbot.local_ai import QuestionPlan
from chatbot.server import (
    ACCESS_CODE_HEADER,
    PUBLIC_RATE_LIMIT,
    ChatHandler,
    allowed_origins_from_env,
    auth_mode_from_env,
    make_server,
)


def _write_csv(folder: Path, table: str, rows: list[dict[str, object]]) -> None:
    with (folder / f"{table}.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class ChatEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temp.cleanup)
        folder = Path(self.temp.name)
        base = {"_source_taxi_type": "yellow", "_source_year": 2024}
        _write_csv(folder, "daily_trip_metrics", [
            {**base, "_source_month": 1, "pickup_date": "2024-01-01", "trip_count": 10,
             "total_revenue": "100.00", "total_tip_amount": "10.00"},
            {**base, "_source_month": 1, "pickup_date": "2024-01-02", "trip_count": 20,
             "total_revenue": "220.00", "total_tip_amount": "20.00"},
            {**base, "_source_month": 2, "pickup_date": "2024-02-01", "trip_count": 40,
             "total_revenue": "480.00", "total_tip_amount": "40.00"},
        ])
        _write_csv(folder, "pickup_zone_performance", [
            {**base, "_source_month": 1, "borough": "Manhattan", "zone": "Midtown",
             "trip_count": 12, "total_revenue": "144.00", "total_tip_amount": "12.00"},
            {**base, "_source_month": 1, "borough": "Queens", "zone": "Astoria",
             "trip_count": 18, "total_revenue": "176.00", "total_tip_amount": "18.00"},
            {**base, "_source_month": 2, "borough": "Manhattan", "zone": "Midtown",
             "trip_count": 40, "total_revenue": "480.00", "total_tip_amount": "40.00"},
        ])
        _write_csv(folder, "hourly_demand", [
            {**base, "_source_month": 1, "pickup_hour": 8, "trip_count": 10},
            {**base, "_source_month": 1, "pickup_hour": 9, "trip_count": 20},
        ])
        _write_csv(folder, "payment_type_summary", [
            {**base, "_source_month": 1, "payment_type": 1, "trip_count": 25,
             "total_revenue": "120.00", "total_tip_amount": "30.00"},
            {**base, "_source_month": 1, "payment_type": 2, "trip_count": 5,
             "total_revenue": "200.00", "total_tip_amount": "0.00"},
        ])
        _write_csv(folder, "takeaway_breakdown", [
            {**base, "_source_month": 1, "borough": "Manhattan", "pickup_hour": 8,
             "payment_type": 1, "trip_count": 8},
            {**base, "_source_month": 1, "borough": "Manhattan", "pickup_hour": 9,
             "payment_type": 2, "trip_count": 4},
        ])
        self.folder = folder
        self.engine = ChatEngine(CsvSource(folder))

    def test_month_and_borough_filter_are_grounded_in_same_export(self) -> None:
        result = self.engine.answer({"question": "How many trips?", "month": "2024-01",
                                     "borough": "Manhattan", "session_id": "a"})
        self.assertIn("12 yellow taxi trips", result["answer"])
        self.assertEqual(result["evidence"][0]["value"], "12")
        self.assertEqual(result["evidence"][0]["source"],
                         "Power BI CSV snapshot: pickup_zone_performance.csv")
        self.assertEqual(result["evidence"][0]["filters"]["borough"], "Manhattan")
        self.assertEqual(result["session_id"], "a")
        self.assertFalse(result["ai_used"])

    def test_local_ai_interprets_unfamiliar_wording_but_cannot_supply_facts(self) -> None:
        plan = QuestionPlan("trips", "trip_count")
        with patch("chatbot.engine.plan_question", return_value=plan) as planner:
            result = self.engine.answer({"question": "How many journeys?"})
        planner.assert_called_once_with("How many journeys?")
        self.assertTrue(result["ai_used"])
        self.assertIn("70 yellow taxi trips", result["answer"])
        self.assertEqual(result["evidence"][0]["source"],
                         "Power BI CSV snapshot: daily_trip_metrics.csv")

    def test_local_ai_unavailable_or_unsupported_measure_fails_closed(self) -> None:
        with patch("chatbot.engine.plan_question", return_value=None):
            result = self.engine.answer({"question": "How many journeys?"})
        self.assertFalse(result["ai_used"])
        self.assertEqual(result["evidence"], [])
        with patch("chatbot.engine.plan_question") as planner:
            rejected = self.engine.answer({"question": "What is the trip duration?"})
        planner.assert_not_called()
        self.assertEqual(rejected["evidence"], [])
        self.assertIn("cannot answer", rejected["answer"])

    def test_causal_relative_date_and_route_questions_do_not_return_wrong_totals(self) -> None:
        for question in (
            "Why did trips fall?",
            "Forecast next month's rides",
            "How many journeys last month?",
            "How many trips from JFK to a Manhattan dropoff?",
        ):
            with self.subTest(question=question), \
                 patch("chatbot.engine.plan_question") as planner:
                result = self.engine.answer({"question": question})
                self.assertEqual(result["evidence"], [])
                self.assertFalse(result["ai_used"])
                planner.assert_not_called()

    def test_question_period_overrides_report_selection(self) -> None:
        result = self.engine.answer({"question": "Revenue in February 2024?",
                                     "month": "2024-01"})
        self.assertIn("$480.00", result["answer"])
        self.assertEqual(result["evidence"][0]["filters"]["periods"], ["February 2024"])

    def test_monthly_comparison_and_peak_use_published_month(self) -> None:
        comparison = self.engine.answer({"question": "Compare January and February"})
        self.assertIn("January 2024: 30 trips", comparison["answer"])
        self.assertIn("February 2024: 40 trips", comparison["answer"])
        self.assertIn("February 2024 had 10 trips more than January 2024", comparison["answer"])
        self.assertIn("+33.3%", comparison["answer"])
        self.assertEqual(len(comparison["evidence"]), 8)
        tips = self.engine.answer({"question": "Compare tips in January and February"})
        self.assertIn("January 2024: $30.00 in recorded tips", tips["answer"])
        self.assertEqual(len(tips["evidence"]), 4)
        peak = self.engine.answer({"question": "Which month had the most trips?"})
        self.assertIn("February 2024 had the most trips: 40", peak["answer"])
        scoped_peak = self.engine.answer({"question": "Which month had the most trips?",
                                          "month": "2024-01"})
        self.assertIn("Only January 2024", scoped_peak["answer"])
        scoped_borough = self.engine.answer({"question": "Which borough had most trips?",
                                             "month": "2024-01", "borough": "Manhattan"})
        self.assertIn("Only Manhattan", scoped_borough["answer"])

    def test_rankings_follow_the_requested_metric(self) -> None:
        base = {"_source_taxi_type": "yellow", "_source_year": 2024}
        _write_csv(self.folder, "daily_trip_metrics", [
            {**base, "_source_month": 1, "pickup_date": "2024-01-01", "trip_count": 30,
             "total_revenue": "1000.00", "total_tip_amount": "100.00"},
            {**base, "_source_month": 2, "pickup_date": "2024-02-01", "trip_count": 40,
             "total_revenue": "480.00", "total_tip_amount": "40.00"},
        ])
        _write_csv(self.folder, "pickup_zone_performance", [
            {**base, "_source_month": 1, "borough": "Manhattan", "zone": "Midtown",
             "trip_count": 12, "total_revenue": "300.00", "total_tip_amount": "50.00"},
            {**base, "_source_month": 1, "borough": "Queens", "zone": "Astoria",
             "trip_count": 18, "total_revenue": "176.00", "total_tip_amount": "18.00"},
        ])
        trip_peak = self.engine.answer({"question": "Which month had the most trips?"})
        revenue_peak = self.engine.answer({"question": "Which month had highest revenue?"})
        tips_peak = self.engine.answer({"question": "Which month had the most tips?"})
        self.assertIn("February 2024", trip_peak["answer"])
        self.assertIn("January 2024", revenue_peak["answer"])
        self.assertEqual(revenue_peak["evidence"][0]["metric"],
                         "January 2024 total_revenue")
        self.assertIn("January 2024", tips_peak["answer"])
        borough = self.engine.answer({"question": "Which borough had highest revenue?",
                                      "month": "2024-01"})
        self.assertIn("Manhattan", borough["answer"])
        self.assertEqual(borough["evidence"][1]["metric"], "total_revenue")
        zone = self.engine.answer({"question": "Which pickup zone had the most tips?",
                                   "month": "2024-01"})
        self.assertIn("Midtown", zone["answer"])
        self.assertEqual(zone["evidence"][1]["metric"], "total_tip_amount")

    def test_specific_payment_and_unsupported_combined_scopes(self) -> None:
        cash = self.engine.answer({"question": "How many cash trips?", "month": "2024-01"})
        self.assertIn("Cash accounted for 5 trips", cash["answer"])
        self.assertEqual(cash["evidence"][0]["value"], "2")
        self.assertEqual(cash["evidence"][1]["value"], "5")
        unknown = self.engine.answer({"question": "How many unknown payment trips?",
                                      "month": "2024-01"})
        self.assertEqual(unknown["evidence"][0]["filters"]["borough"], None)
        self.assertEqual(unknown["evidence"][1]["value"], "0")
        no_charge = self.engine.answer({"question": "How many no charge trips?",
                                        "month": "2024-01"})
        self.assertEqual(no_charge["evidence"][0]["value"], "3")
        for question in ("Compare Manhattan vs Brooklyn",
                         "How many trips in Manhattan and Brooklyn?",
                         "Compare cash vs card", "Which month had most cash trips?",
                         "How many trips in 2024?",
                         "Which borough had highest average fare?",
                         "What was the average trip distance?",
                         "What share of trips came from Manhattan?"):
            with self.subTest(question=question):
                result = self.engine.answer({"question": question})
                self.assertEqual(result["evidence"], [])
                self.assertIn("cannot", result["answer"].casefold())

    def test_two_month_metric_changes_and_zero_baseline(self) -> None:
        trips = self.engine.answer({"question": "What was the percent change in trips "
                                               "from January to February?"})
        self.assertIn("February 2024 had 10 trips more than January 2024", trips["answer"])
        self.assertEqual(trips["evidence"][-2]["value"], "10")
        self.assertEqual(trips["evidence"][-1]["value"], "33.3")
        revenue = self.engine.answer({"question": "Compare revenue in January and February"})
        self.assertIn("$160.00 in TLC total amount more", revenue["answer"])
        self.assertIn("+50.0%", revenue["answer"])
        reversed_months = self.engine.answer({
            "question": "How many fewer trips in January than February?"})
        self.assertIn("January 2024 had 10 trips less than February 2024",
                      reversed_months["answer"])
        self.assertIn("-25.0%", reversed_months["answer"])
        polite = self.engine.answer({"question": "Can you compare Jan and Feb trips?"})
        self.assertIn("+33.3%", polite["answer"])
        vague = self.engine.answer({"question": "Compare boroughs by trips"})
        self.assertEqual(vague["evidence"], [])
        self.assertIn("name two months or two boroughs", vague["answer"])
        base = {"_source_taxi_type": "yellow", "_source_year": 2024}
        _write_csv(self.folder, "daily_trip_metrics", [
            {**base, "_source_month": 1, "pickup_date": "2024-01-01",
             "trip_count": 10, "total_revenue": "100.00", "total_tip_amount": "0.00"},
            {**base, "_source_month": 2, "pickup_date": "2024-02-01",
             "trip_count": 20, "total_revenue": "200.00", "total_tip_amount": "20.00"},
        ])
        tips = self.engine.answer({"question": "Percent change in tips from Jan to Feb?"})
        self.assertIn("Percentage change is undefined", tips["answer"])
        self.assertEqual(tips["evidence"][-1]["value"], "20.00")

    def test_two_borough_comparisons_use_one_metric_and_exact_filters(self) -> None:
        trips = self.engine.answer({"question": "Compare trips in Manhattan vs Queens",
                                    "month": "2024-01"})
        self.assertIn("Manhattan: 12 trips; Queens: 18 trips", trips["answer"])
        self.assertIn("6 trips less than Queens", trips["answer"])
        self.assertIn("-33.3%", trips["answer"])
        self.assertEqual(trips["evidence"][0]["filters"]["borough"], "Manhattan")
        self.assertEqual(trips["evidence"][1]["filters"]["borough"], "Queens")
        self.assertEqual(trips["evidence"][2]["value"], "-6")
        revenue = self.engine.answer({"question": "How much more total amount did "
                                               "Queens have than Manhattan in Jan 2024?"})
        self.assertIn("Queens: $176.00", revenue["answer"])
        self.assertIn("$32.00 in TLC total amount more", revenue["answer"])
        tips = self.engine.answer({"question": "Compare tips in Manhattan and Queens",
                                   "month": "2024-01"})
        self.assertEqual(tips["evidence"][0]["metric"], "total_tip_amount")
        self.assertEqual(tips["evidence"][2]["value"], "-6.00")
        mixed = self.engine.answer({"question": "Compare trips and revenue in Manhattan "
                                                "vs Queens"})
        self.assertEqual(mixed["evidence"], [])

    def test_payment_amounts_and_lowest_rankings(self) -> None:
        cash = self.engine.answer({"question": "How much revenue came from cash?",
                                   "month": "2024-01"})
        self.assertIn("$200.00 in TLC total amount", cash["answer"])
        self.assertIn("62.5% of TLC total amount", cash["answer"])
        self.assertEqual(cash["evidence"][1]["metric"], "total_revenue")
        self.assertEqual(cash["evidence"][2]["metric"], "revenue_share")
        highest = self.engine.answer({"question": "Which payment method had highest revenue?",
                                      "month": "2024-01"})
        self.assertIn("Cash was the highest payment method", highest["answer"])
        self.assertEqual(highest["evidence"][1]["value"], "200.00")
        borough = self.engine.answer({"question": "How much revenue came from cash "
                                                  "in Manhattan?"})
        self.assertEqual(borough["evidence"], [])
        self.assertIn("trip counts only", borough["answer"])
        for question, expected in (
            ("Which zone had the fewest trips in Jan 2024?", "Manhattan / Midtown"),
            ("Which borough had lowest revenue in Jan 2024?", "Manhattan"),
            ("Which month had fewest trips?", "January 2024"),
            ("Which day had fewest trips in Jan 2024?", "2024-01-01"),
            ("Which hour had fewest trips in Jan 2024?", "8 AM"),
            ("Which payment method had fewest trips in Jan 2024?", "Cash"),
        ):
            with self.subTest(question=question):
                result = self.engine.answer({"question": question})
                self.assertIn(expected, result["answer"])
                self.assertTrue(result["evidence"])

    def test_published_na_borough_is_accepted(self) -> None:
        _write_csv(self.folder, "pickup_zone_performance", [
            {"_source_taxi_type": "yellow", "_source_year": 2024,
             "_source_month": 1, "borough": "N/A", "zone": "Unknown",
             "trip_count": 7, "total_revenue": "70.00", "total_tip_amount": "7.00"},
        ])
        result = self.engine.answer({"question": "How many trips?", "borough": "N/A"})
        self.assertIn("7 yellow taxi trips in N/A", result["answer"])
        explicit = self.engine.answer({"question": "How many trips in N/A?"})
        self.assertEqual(explicit["evidence"][0]["value"], "7")

    def test_unknown_borough_includes_unmapped_takeaway_rows(self) -> None:
        base = {"_source_taxi_type": "yellow", "_source_year": 2024,
                "_source_month": 1}
        _write_csv(self.folder, "takeaway_breakdown", [
            {**base, "borough": "Unknown", "pickup_hour": 7,
             "payment_type": 2, "trip_count": 4},
            {**base, "borough": "Unknown / unmapped", "pickup_hour": 7,
             "payment_type": 1, "trip_count": 6},
            {**base, "borough": "Queens", "pickup_hour": 9,
             "payment_type": 1, "trip_count": 50},
        ])
        hour = self.engine.answer({"question": "What was the busiest hour?",
                                   "month": "2024-01", "borough": "Unknown"})
        self.assertIn("7 AM", hour["answer"])
        self.assertEqual(hour["evidence"][1]["value"], "10")
        payment = self.engine.answer({"question": "How many card trips?",
                                      "month": "2024-01",
                                      "borough": "Unknown / unmapped"})
        self.assertEqual(payment["evidence"][1]["value"], "6")
        self.assertEqual(payment["evidence"][2]["value"], "60.0")
        self.assertEqual(payment["evidence"][0]["filters"]["borough"], "Unknown")

    def test_highest_revenue_hour_uses_revenue_and_declines_missing_grain(self) -> None:
        base = {"_source_taxi_type": "yellow", "_source_year": 2024,
                "_source_month": 1}
        _write_csv(self.folder, "hourly_demand", [
            {**base, "pickup_hour": 8, "trip_count": 10, "total_revenue": "100.00"},
            {**base, "pickup_hour": 9, "trip_count": 20, "total_revenue": "50.00"},
        ])
        revenue = self.engine.answer({"question": "What hour had highest revenue?",
                                      "month": "2024-01"})
        self.assertIn("8 AM", revenue["answer"])
        self.assertEqual(revenue["evidence"][1]["metric"], "total_revenue")
        borough = self.engine.answer({"question": "What hour had highest revenue?",
                                      "month": "2024-01", "borough": "Manhattan"})
        self.assertEqual(borough["evidence"], [])
        self.assertIn("does not contain", borough["answer"])

    def test_day_ranking_excludes_dates_outside_the_published_month(self) -> None:
        base = {"_source_taxi_type": "yellow", "_source_year": 2024}
        _write_csv(self.folder, "daily_trip_metrics", [
            {**base, "_source_month": 1, "pickup_date": "2002-12-31",
             "trip_count": 1, "total_revenue": "10.00", "total_tip_amount": "0.00"},
            {**base, "_source_month": 1, "pickup_date": "2024-01-01",
             "trip_count": 10, "total_revenue": "100.00", "total_tip_amount": "10.00"},
            {**base, "_source_month": 1, "pickup_date": "2024-01-02",
             "trip_count": 20, "total_revenue": "200.00", "total_tip_amount": "20.00"},
            {**base, "_source_month": 2, "pickup_date": "2024-01-31",
             "trip_count": 2, "total_revenue": "20.00", "total_tip_amount": "0.00"},
            {**base, "_source_month": 2, "pickup_date": "2024-02-01",
             "trip_count": 40, "total_revenue": "400.00", "total_tip_amount": "40.00"},
        ])
        scoped = self.engine.answer({
            "question": "Which pickup day had the fewest trips in January 2024?"})
        self.assertIn("2024-01-01", scoped["answer"])
        self.assertIn("Pickup dates outside their published source month were excluded",
                      scoped["answer"])
        self.assertEqual(scoped["evidence"][0]["value"], "2024-01-01")
        unscoped = self.engine.answer({"question": "Which pickup day had the fewest trips?"})
        self.assertEqual(unscoped["evidence"][0]["value"], "2024-01-01")
        total = self.engine.answer({"question": "How many trips in January 2024?"})
        self.assertIn("31 yellow taxi trips", total["answer"])

    def test_day_ranking_explains_when_only_out_of_month_dates_exist(self) -> None:
        _write_csv(self.folder, "daily_trip_metrics", [
            {"_source_taxi_type": "yellow", "_source_year": 2024,
             "_source_month": 1, "pickup_date": "2002-12-31",
             "trip_count": 1, "total_revenue": "10.00", "total_tip_amount": "0.00"},
        ])
        result = self.engine.answer({
            "question": "Which pickup day had the fewest trips in January 2024?"})
        self.assertIn("no pickup dates fall within their published source month",
                      result["answer"])
        self.assertEqual(result["evidence"], [])

    def test_borough_hour_uses_joint_grain_and_daily_borough_refuses(self) -> None:
        hour = self.engine.answer({"question": "What was the busiest hour?",
                                   "month": "2024-01", "borough": "Manhattan"})
        self.assertIn("8 AM", hour["answer"])
        self.assertIn("takeaway_breakdown.csv", hour["evidence"][0]["source"])
        day = self.engine.answer({"question": "What was the busiest day?",
                                  "borough": "Manhattan"})
        self.assertIn("cannot identify", day["answer"])
        self.assertEqual(day["evidence"], [])

    def test_named_borough_hour_and_payment_choose_requested_measure(self) -> None:
        hour = self.engine.answer({"question": "Which hour was busiest in Manhattan?",
                                   "month": "2024-01"})
        self.assertIn("8 AM", hour["answer"])
        self.assertEqual(hour["evidence"][0]["metric"], "pickup_hour")
        self.assertIn("takeaway_breakdown.csv", hour["evidence"][0]["source"])

        payment = self.engine.answer({
            "question": "Which payment method was most common in Manhattan?",
            "month": "2024-01",
        })
        self.assertIn("Credit card was the most common payment method", payment["answer"])
        self.assertEqual(payment["evidence"][0]["metric"], "payment_type")
        self.assertEqual(payment["evidence"][1]["value"], "8")
        self.assertIn("takeaway_breakdown.csv", payment["evidence"][0]["source"])

    def test_specific_pickup_zone_total_is_not_misreported_as_a_ranking(self) -> None:
        result = self.engine.answer({"question": "How many trips in Midtown pickup zone?",
                                     "month": "2024-01"})
        self.assertIn("cannot filter totals to a specific pickup zone", result["answer"])
        self.assertEqual(result["evidence"], [])
        ranking = self.engine.answer({"question": "Which pickup zone had the most trips?",
                                      "month": "2024-01"})
        self.assertIn("Astoria", ranking["answer"])

    def test_invalid_scope_is_rejected(self) -> None:
        self.assertEqual(parse_month("Mar 2024"), (2024, 3))
        with self.assertRaises(InvalidRequest):
            self.engine.answer({"question": "Trips?", "borough": "x' OR 1=1 --"})
        with self.assertRaises(InvalidRequest):
            self.engine.answer({"question": "Trips?", "month": "13"})

    def test_http_contract_and_cors(self) -> None:
        handler = object.__new__(ChatHandler)
        handler.engine = self.engine
        handler.access_code = "abcdefghijklmnop"
        handler.path = "/health"
        handler.headers = {"Origin": "https://ms-pbi.pbi.microsoft.com"}
        handler.wfile = io.BytesIO()
        status: list[int] = []
        headers: dict[str, str] = {}
        handler.send_response = status.append
        handler.send_header = lambda key, value: headers.__setitem__(key, value)
        handler.end_headers = lambda: None
        handler.log_message = lambda *_: None
        handler.do_GET()
        self.assertEqual(status[-1], 200)
        self.assertEqual(json.loads(handler.wfile.getvalue())["status"], "ready")
        self.assertEqual(headers["Access-Control-Allow-Origin"],
                         "https://ms-pbi.pbi.microsoft.com")
        body = json.dumps({"question": "How many trips?", "month": 1}).encode()
        handler.path = "/chat"
        handler.wfile = io.BytesIO()
        handler.rfile = io.BytesIO(body)
        handler.headers = {"Origin": "https://ms-pbi.pbi.microsoft.com",
                           "Content-Type": "application/json", "Content-Length": str(len(body)),
                           ACCESS_CODE_HEADER: handler.access_code}
        handler.do_POST()
        self.assertEqual(status[-1], 200)
        self.assertIn("30 yellow taxi trips", json.loads(handler.wfile.getvalue())["answer"])
        handler.wfile = io.BytesIO()
        handler.do_OPTIONS()
        self.assertEqual(status[-1], 200)
        self.assertEqual(headers["Access-Control-Allow-Private-Network"], "true")
        self.assertIn(ACCESS_CODE_HEADER, headers["Access-Control-Allow-Headers"])
        handler.path = "/health"
        handler.headers = {"Origin": "https://untrusted.example"}
        handler.wfile = io.BytesIO()
        headers.clear()
        handler.do_GET()
        self.assertEqual(status[-1], 403)
        self.assertNotIn("Access-Control-Allow-Origin", headers)

    def test_null_origin_requires_code_for_every_chat_post(self) -> None:
        handler = object.__new__(ChatHandler)
        handler.engine = self.engine
        handler.allowed_origins = frozenset({"null"})
        handler.access_code = "abcdefghijklmnop"
        handler.path = "/chat"
        handler.log_message = lambda *_: None
        status: list[int] = []
        headers: dict[str, str] = {}
        handler.send_response = status.append
        handler.send_header = lambda key, value: headers.__setitem__(key, value)
        handler.end_headers = lambda: None
        body = json.dumps({"question": "How many trips?", "month": 1}).encode()
        for code in (None, "wrong-code"):
            with self.subTest(code=code):
                handler.headers = {"Origin": "null", "Content-Type": "application/json",
                                   "Content-Length": str(len(body))}
                if code is not None:
                    handler.headers[ACCESS_CODE_HEADER] = code
                handler.rfile = io.BytesIO(body)
                handler.wfile = io.BytesIO()
                headers.clear()
                handler.do_POST()
                self.assertEqual(status[-1], 401)
                self.assertEqual(handler.rfile.tell(), 0)
                self.assertEqual(headers["Access-Control-Allow-Origin"], "null")
                reply = json.loads(handler.wfile.getvalue())
                self.assertEqual(reply["answer"], "")
                self.assertEqual(reply["evidence"], [])

        handler.headers = {"Origin": "null", "Content-Type": "application/json",
                           "Content-Length": str(len(body)),
                           ACCESS_CODE_HEADER: handler.access_code}
        handler.rfile = io.BytesIO(body)
        handler.wfile = io.BytesIO()
        handler.do_POST()
        self.assertEqual(status[-1], 200)
        self.assertIn("30 yellow taxi trips", json.loads(handler.wfile.getvalue())["answer"])

        handler.headers = {"Origin": "null", "Access-Control-Request-Headers":
                           "content-type,x-city-pulse-code"}
        handler.wfile = io.BytesIO()
        handler.do_OPTIONS()
        self.assertEqual(status[-1], 200)
        self.assertEqual(headers["Access-Control-Allow-Origin"], "null")
        self.assertIn(ACCESS_CODE_HEADER, headers["Access-Control-Allow-Headers"])
        self.assertEqual(json.loads(handler.wfile.getvalue()), {})

        handler.path = "/health"
        handler.wfile = io.BytesIO()
        handler.do_GET()
        self.assertEqual(status[-1], 200)
        self.assertEqual(json.loads(handler.wfile.getvalue())["status"], "ready")

    def test_code_free_local_mode_reads_only_csvs_without_a_code(self) -> None:
        handler = object.__new__(ChatHandler)
        handler.engine = self.engine
        handler.allowed_origins = frozenset({"null"})
        handler.auth_mode = "local_public"
        handler.public_request_times = deque()
        handler.public_rate_lock = threading.Lock()
        handler.log_message = lambda *_: None
        statuses: list[int] = []
        handler.send_response = statuses.append
        handler.send_header = lambda *_: None
        handler.end_headers = lambda: None
        handler.path = "/health"
        handler.headers = {"Origin": "null"}
        handler.wfile = io.BytesIO()
        handler.do_GET()
        self.assertFalse(json.loads(handler.wfile.getvalue())["auth_required"])

        body = json.dumps({"question": "How many trips?", "month": 1}).encode()
        handler.path = "/chat"
        handler.headers = {"Origin": "null", "Content-Type": "application/json",
                           "Content-Length": str(len(body))}
        handler.rfile = io.BytesIO(body)
        handler.wfile = io.BytesIO()
        handler.do_POST()
        self.assertEqual(statuses[-1], 200)
        self.assertIn("30 yellow taxi trips", json.loads(handler.wfile.getvalue())["answer"])

        handler.public_request_times = deque([1_000.0] * PUBLIC_RATE_LIMIT)
        handler.rfile = io.BytesIO(body)
        handler.wfile = io.BytesIO()
        with patch("chatbot.server.time.monotonic", return_value=1_000.0):
            handler.do_POST()
        self.assertEqual(statuses[-1], 429)
        self.assertEqual(handler.rfile.tell(), 0)

    def test_code_free_mode_rejects_postgres_and_unknown_auth_modes(self) -> None:
        with patch.dict(os.environ, {"CHAT_AUTH_MODE": "local_public",
                                  "CHAT_DATA_SOURCE": "postgres"}):
            with self.assertRaisesRegex(ValueError, "only exported CSV"):
                auth_mode_from_env()
        with patch.dict(os.environ, {"CHAT_AUTH_MODE": "unknown"}):
            with self.assertRaisesRegex(ValueError, "CHAT_AUTH_MODE"):
                auth_mode_from_env()
        with patch.dict(os.environ, {"CHAT_AUTH_MODE": "local_public",
                                  "CHAT_DATA_SOURCE": "csv"}):
            self.assertEqual(auth_mode_from_env(), "local_public")

    def test_each_server_gets_a_fresh_url_safe_access_code(self) -> None:
        with patch.dict(os.environ, {"CHAT_AUTH_MODE": "code"}), \
             patch("chatbot.server.source_from_env", return_value=self.engine.source), \
             patch("chatbot.server.ThreadingHTTPServer") as factory:
            make_server()
            first = factory.call_args.args[1].access_code
            make_server()
            second = factory.call_args.args[1].access_code
        self.assertEqual(len(first), 16)
        self.assertEqual(len(second), 16)
        self.assertTrue(all(character.isascii() and
                            (character.isalnum() or character in "-_") for character in first))
        self.assertNotEqual(first, second)

    def test_origin_configuration_rejects_wildcard(self) -> None:
        with patch.dict(os.environ, {"CHAT_ALLOWED_ORIGINS": "*"}):
            with self.assertRaises(ValueError):
                allowed_origins_from_env()
        with patch.dict(os.environ, {
            "CHAT_ALLOWED_ORIGINS": "https://ms-pbi.pbi.microsoft.com,null"
        }):
            self.assertEqual(allowed_origins_from_env(),
                             {"https://ms-pbi.pbi.microsoft.com", "null"})

    def test_csv_health_requires_borough_breakdown_export(self) -> None:
        (self.folder / "takeaway_breakdown.csv").unlink()
        health = self.engine.source.health()
        self.assertEqual(health["status"], "missing_exports")
        self.assertEqual(health["missing_exports"], ["takeaway_breakdown"])


class PostgresContractTests(unittest.TestCase):
    def test_health_checks_every_required_mart(self) -> None:
        source = PostgresSource()
        seen: list[str] = []

        def fetch(table: str, *_: object) -> list[dict[str, object]]:
            seen.append(table)
            if table == "takeaway_breakdown":
                raise SourceError("missing")
            return []

        source.fetch = fetch  # type: ignore[method-assign]
        self.assertEqual(source.health(), {"status": "unavailable", "source": "postgres"})
        self.assertIn("takeaway_breakdown", seen)
        self.assertEqual(set(seen), TABLES)

    def test_fixed_read_only_query_and_parameterized_values(self) -> None:
        calls: dict[str, object] = {}

        class FakeSQL(str):
            def format(self, *parts: object) -> FakeSQL:
                return FakeSQL(str(self).format(*parts))

            def __add__(self, other: object) -> FakeSQL:
                return FakeSQL(str(self) + str(other))

        class Cursor:
            description = [types.SimpleNamespace(name="trip_count")]

            def __enter__(self) -> Cursor:
                return self

            def __exit__(self, *_: object) -> None:
                pass

            def execute(self, statement: object, parameters: object) -> None:
                calls["statement"] = str(statement)
                calls["parameters"] = parameters

            def fetchall(self) -> list[tuple[int]]:
                return [(12,)]

        class Connection:
            def __enter__(self) -> Connection:
                return self

            def __exit__(self, *_: object) -> None:
                pass

            def set_session(self, **kwargs: object) -> None:
                calls["session"] = kwargs

            def cursor(self) -> Cursor:
                return Cursor()

        fake_sql = types.ModuleType("psycopg2.sql")
        fake_sql.SQL = FakeSQL  # type: ignore[attr-defined]
        fake_sql.Identifier = lambda value: FakeSQL('"' + value + '"')  # type: ignore[attr-defined]
        fake_psycopg = types.ModuleType("psycopg2")
        fake_psycopg.Error = Exception  # type: ignore[attr-defined]
        fake_psycopg.connect = lambda **_: Connection()  # type: ignore[attr-defined]
        fake_psycopg.sql = fake_sql  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"psycopg2": fake_psycopg, "psycopg2.sql": fake_sql}):
            with patch.dict(os.environ, {"ANALYTICS_DB_PASSWORD": "test-only"}):
                source = PostgresSource()
                rows = source.fetch("pickup_zone_performance", (2024, 1), "Manhattan")
                borough_statement = calls["statement"]
                borough_parameters = calls["parameters"]
                source.fetch("takeaway_breakdown", (2024, 1), "Unknown")
        self.assertEqual(rows, [{"trip_count": 12}])
        self.assertEqual(calls["session"], {"readonly": True})
        self.assertIn("SELECT * FROM analytics.", borough_statement)
        self.assertIn("lower(borough) = lower(%s)", borough_statement)
        self.assertNotIn("Manhattan", borough_statement)
        self.assertEqual(borough_parameters, ["yellow", 2024, 1, "Manhattan"])
        self.assertIn("lower(borough) IN (lower(%s), lower(%s))", calls["statement"])
        self.assertEqual(calls["parameters"],
                         ["yellow", 2024, 1, "Unknown", "Unknown / unmapped"])


class ExportContractTests(unittest.TestCase):
    def test_exporter_includes_every_chat_mart_without_database_dependency(self) -> None:
        fake_psycopg = types.ModuleType("psycopg2")
        fake_psycopg.sql = types.ModuleType("psycopg2.sql")  # type: ignore[attr-defined]
        fake_modules = {"psycopg2": fake_psycopg,
                        "psycopg2.sql": fake_psycopg.sql}  # type: ignore[attr-defined]
        with patch.dict(sys.modules, fake_modules):
            from scripts.export_powerbi_marts import MARTS
        self.assertTrue(TABLES <= set(MARTS))


if __name__ == "__main__":
    unittest.main()
