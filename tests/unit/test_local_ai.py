"""A local model may classify wording, but never choose data, filters, or facts."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from chatbot.local_ai import QuestionPlan, plan_question


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        pass

    def read(self, _: int) -> bytes:
        return self.content


class _Opener:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.request = None
        self.timeout = None

    def open(self, request: object, *, timeout: int) -> _Response:
        self.request = request
        self.timeout = timeout
        return _Response(self.content)


def _ollama_response(value: object) -> bytes:
    return json.dumps({"response": json.dumps(value)}).encode()


class LocalPlannerTests(unittest.TestCase):
    def _ask(self, question: str, value: object) -> tuple[QuestionPlan | None, _Opener]:
        opener = _Opener(_ollama_response(value))
        with patch("chatbot.local_ai.urllib.request.build_opener", return_value=opener):
            result = plan_question(question, model="qwen2.5:1.5b")
        return result, opener

    def test_valid_plan_is_structured_and_sends_only_to_loopback(self) -> None:
        value = {"intent": "trips", "metric": "trip_count"}
        result, opener = self._ask("How many journeys?", value)
        self.assertEqual(result, QuestionPlan("trips", "trip_count"))
        self.assertEqual(opener.request.full_url,
                         "http://127.0.0.1:11434/api/generate")
        self.assertLessEqual(opener.timeout, 30)
        sent = json.loads(opener.request.data)
        self.assertFalse(sent["stream"])
        self.assertFalse(sent["format"]["additionalProperties"])
        self.assertEqual(set(sent["format"]["required"]), set(value))

    def test_disabled_or_remote_endpoint_never_calls_transport(self) -> None:
        with patch("chatbot.local_ai.urllib.request.build_opener") as build:
            self.assertIsNone(plan_question("How many journeys?", model=""))
            self.assertIsNone(plan_question("How many journeys?", model="gemma4:cloud"))
            self.assertIsNone(plan_question(
                "How many journeys?", model="qwen2.5:1.5b",
                endpoint="https://example.com/api/generate"
            ))
            self.assertIsNone(plan_question(
                "How many journeys?", model="qwen2.5:1.5b",
                endpoint="http://127.0.0.1:11434/api/generate#redirect"
            ))
        build.assert_not_called()

    def test_model_output_cannot_add_filters_sql_facts_or_unlisted_fields(self) -> None:
        bad_values = (
            {"intent": "trips", "metric": "trip_count", "sql": "SELECT * FROM trips"},
            {"intent": "trips", "metric": "trip_count", "borough": "Manhattan"},
            {"intent": "trips", "metric": "trip_count", "answer": "42 trips"},
            {"intent": "custom_sql", "metric": None},
            {"intent": "trips", "metric": "fare_amount"},
            {"intent": "revenue", "metric": "trip_count"},
            {"intent": "help", "metric": "trip_count"},
            {"intent": "top_borough", "metric": None},
            {"intent": "payment", "metric": None},
        )
        for value in bad_values:
            with self.subTest(value=value):
                self.assertIsNone(self._ask("How many journeys?", value)[0])

    def test_malformed_or_unavailable_model_falls_back(self) -> None:
        duplicate = '{"intent":"trips","intent":"revenue","metric":null}'
        malformed = (
            b"not json",
            b'{"response":"not json"}',
            json.dumps({"response": duplicate}).encode(),
            b"x" * 65537,
        )
        for content in malformed:
            opener = _Opener(content)
            with self.subTest(content=content[:30]), \
                 patch("chatbot.local_ai.urllib.request.build_opener", return_value=opener):
                self.assertIsNone(plan_question("How many journeys?", model="qwen2.5:1.5b"))
        with patch("chatbot.local_ai.urllib.request.build_opener") as build:
            build.return_value.open.side_effect = TimeoutError()
            self.assertIsNone(plan_question("How many journeys?", model="qwen2.5:1.5b"))


if __name__ == "__main__":
    unittest.main()
