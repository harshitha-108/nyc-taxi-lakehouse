"""Honest, deterministic checks for the 58-question Ask City Pulse evaluation set."""

from __future__ import annotations

import tempfile
import unittest
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from chatbot.data import CsvSource
from chatbot.evaluation.fixture import DAILY, mart_rows, write_fixture
from chatbot.evaluation.harness import (
    CATEGORIES,
    evaluate_case,
    load_manifest,
    validate_manifest,
)


class EvaluationManifestTests(unittest.TestCase):
    def test_manifest_has_unique_representative_cases(self) -> None:
        manifest = load_manifest()
        self.assertGreaterEqual(len(manifest["cases"]), 40)
        self.assertEqual({case["category"] for case in manifest["cases"]}, CATEGORIES)
        self.assertEqual(len({case["id"] for case in manifest["cases"]}),
                         len(manifest["cases"]))
        self.assertGreaterEqual(sum(case["category"] == "followup"
                                    for case in manifest["cases"]), 5)
        self.assertGreaterEqual(sum(case["category"] == "unsupported_safety"
                                    for case in manifest["cases"]), 10)

    def test_invalid_or_misleading_manifest_is_rejected(self) -> None:
        manifest = load_manifest()
        manifest["cases"][1]["id"] = manifest["cases"][0]["id"]
        with self.assertRaisesRegex(ValueError, "unique"):
            validate_manifest(manifest)


class SyntheticFixtureTests(unittest.TestCase):
    def test_all_five_marts_reconcile_by_month(self) -> None:
        marts = mart_rows()
        expected = {1: 30, 2: 60, 5: 90, 6: 50}
        for table, rows in marts.items():
            with self.subTest(table=table):
                totals: dict[int, int] = defaultdict(int)
                for row in rows:
                    totals[int(row["_source_month"])] += int(row["trip_count"])
                self.assertEqual(dict(totals), expected)

    def test_independent_oracles_match_manifest_landmarks(self) -> None:
        daily = mart_rows()["daily_trip_metrics"]
        self.assertEqual(sum(row["trip_count"] for row in daily), 230)
        self.assertEqual(sum(row["total_revenue"] for row in daily), 3280)
        self.assertEqual(sum(row["total_tip_amount"] for row in daily), 398)
        may = [row for row in daily if row["_source_month"] == 5]
        june = [row for row in daily if row["_source_month"] == 6]
        may_fare = sum(Decimal(str(row["average_fare_amount"])) * row["trip_count"]
                       for row in may) / sum(row["trip_count"] for row in may)
        june_fare = sum(Decimal(str(row["average_fare_amount"])) * row["trip_count"]
                        for row in june) / sum(row["trip_count"] for row in june)
        self.assertAlmostEqual(float(may_fare), 15.5555556)
        self.assertEqual(june_fare, Decimal("11.8"))
        by_kind: dict[str, list[int]] = defaultdict(list)
        for date_text, _, trips, *_ in DAILY:
            kind = "weekend" if date.fromisoformat(date_text).weekday() >= 5 else "weekday"
            by_kind[kind].append(trips)
        self.assertEqual(sum(by_kind["weekend"]) / len(by_kind["weekend"]), 30)
        self.assertEqual(sum(by_kind["weekday"]) / len(by_kind["weekday"]), 27.5)

    def test_csv_source_can_read_synthetic_fixture(self) -> None:
        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as name:
            write_fixture(Path(name))
            source = CsvSource(Path(name))
            self.assertEqual(source.health()["status"], "ready")
            self.assertEqual(len(source.fetch("pickup_zone_performance")), 12)


class HarnessTests(unittest.TestCase):
    def test_followup_reuses_session_and_checks_visible_answer(self) -> None:
        class FakeEngine:
            def __init__(self) -> None:
                self.payloads: list[dict[str, object]] = []

            def answer(self, payload: dict[str, object]) -> dict[str, object]:
                self.payloads.append(payload)
                return {"answer": "May recorded 180 in tips.", "evidence": [],
                        "session_id": "synthetic-session", "ai_used": False}

        engine = FakeEngine()
        case = next(case for case in load_manifest()["cases"] if case["id"] == "FUP01")
        result = evaluate_case(engine, case)
        self.assertEqual(result["status"], "pass")
        self.assertIsNone(engine.payloads[0]["session_id"])
        self.assertEqual(engine.payloads[1]["session_id"], "synthetic-session")
        self.assertGreaterEqual(result["latency_ms"], 0)

    def test_injection_cannot_pass_with_data_evidence(self) -> None:
        class UnsafeFake:
            def answer(self, _payload: dict[str, object]) -> dict[str, object]:
                return {"answer": "Done; 230 trips.", "evidence": [{"value": "230"}],
                        "session_id": "synthetic-session", "ai_used": False}

        case = next(case for case in load_manifest()["cases"] if case["id"] == "U06")
        result = evaluate_case(UnsafeFake(), case)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["safety"], "fail")


class AdvancedEngineCoreTests(unittest.TestCase):
    """A small CI gate; the full observed 58-case report is run separately."""

    def test_core_and_injection_cases(self) -> None:
        from chatbot.advanced_engine import AdvancedChatEngine

        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as name:
            write_fixture(Path(name))
            engine = AdvancedChatEngine(CsvSource(Path(name)))
            cases = {case["id"]: case for case in load_manifest()["cases"]}
            must_pass = ("B01", "B02", "F01", "G01", "U06", "U07")
            # Deterministic CI must not depend on a local Ollama process/model.
            with patch("chatbot.advanced_engine.propose_analytic_sql", return_value=None), \
                    patch("chatbot.engine.plan_question", return_value=None):
                failures = {case_id: result["issues"]
                            for case_id in must_pass
                            if (result := evaluate_case(engine, cases[case_id]))["status"]
                            != "pass"}
            self.assertEqual(failures, {})


if __name__ == "__main__":
    unittest.main()
