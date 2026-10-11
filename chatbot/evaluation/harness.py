"""Run the synthetic Ask City Pulse question suite without live services.

This module records observed results; it never invents a score or calls a
network model. The caller supplies an engine with ``answer(payload)``.
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

MANIFEST = Path(__file__).with_name("cases.json")
CATEGORIES = frozenset({"basic", "filters", "grouping", "comparison",
                        "trend_anomaly", "multistep", "followup",
                        "unsupported_safety"})
OUTCOMES = frozenset({"answer", "refuse", "unavailable", "clarify"})
NUMBER = re.compile(r"(?<![\w.])[-+]?\$?\d[\d,]*(?:\.\d+)?%?(?!\w)")
ISO_DATE = re.compile(r"\b\d{4}-\d{2}(?:-\d{2})?\b")


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("version") != 1 or manifest.get("fixture") != "synthetic_marts_v1":
        raise ValueError("Unsupported evaluation manifest version or fixture")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or len(cases) < 40:
        raise ValueError("Evaluation needs at least 40 cases")
    ids: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("Every case must be an object")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id or case_id in ids:
            raise ValueError("Case IDs must be unique nonempty strings")
        ids.add(case_id)
        category = case.get("category")
        if category not in CATEGORIES:
            raise ValueError(f"{case_id}: unknown category")
        turns = case.get("turns")
        if (not isinstance(turns, list) or not 1 <= len(turns) <= 4
                or not all(isinstance(turn, str) and 1 <= len(turn.strip()) <= 500
                           for turn in turns)):
            raise ValueError(f"{case_id}: turns must be 1–4 questions of 1–500 characters")
        if (category == "followup") != (len(turns) > 1):
            raise ValueError(f"{case_id}: follow-up cases require multiple turns")
        context = case.get("context", {})
        if (not isinstance(context, dict) or set(context) - {"month", "borough"}
                or any(value is not None and not isinstance(value, str)
                       for value in context.values())):
            raise ValueError(f"{case_id}: invalid report-filter context")
        expected = case.get("expected")
        if not isinstance(expected, dict) or expected.get("outcome") not in OUTCOMES:
            raise ValueError(f"{case_id}: invalid expected outcome")
        numbers = expected.get("numbers", [])
        if not isinstance(numbers, list):
            raise ValueError(f"{case_id}: numeric expectations must be a list")
        for value in numbers:
            try:
                if not Decimal(str(value)).is_finite():
                    raise ValueError
            except (InvalidOperation, ValueError):
                raise ValueError(f"{case_id}: non-finite expected number") from None
        try:
            if Decimal(str(expected.get("tolerance", 0))) < 0:
                raise ValueError
        except (InvalidOperation, ValueError):
            raise ValueError(f"{case_id}: invalid numeric tolerance") from None
        for field in ("terms", "terms_any"):
            terms = expected.get(field, [])
            if not isinstance(terms, list) or not all(
                    isinstance(term, str) and term.strip() for term in terms):
                raise ValueError(f"{case_id}: invalid {field}")
        if expected["outcome"] == "answer" and not (numbers or expected.get("review_required")):
            raise ValueError(f"{case_id}: answer needs numbers or explicit manual review")
        if expected["outcome"] != "answer" and (numbers or not expected.get("terms_any")):
            raise ValueError(f"{case_id}: non-answers need refusal markers, not numeric claims")
    if {case["category"] for case in cases} != CATEGORIES:
        raise ValueError("Evaluation must cover every required category")


def _answer_numbers(answer: str) -> list[Decimal]:
    """Read only visible answer numbers, not hidden evidence or filter dates."""
    scrubbed = ISO_DATE.sub(" ", answer)
    parsed = []
    for match in NUMBER.finditer(scrubbed):
        token = match.group().replace("$", "").replace(",", "").replace("%", "")
        try:
            parsed.append(Decimal(token))
        except InvalidOperation:
            continue
    return parsed


def _checks(answer: str, expected: dict[str, Any]) -> tuple[str, str, list[str]]:
    lowered = answer.casefold()
    missing_terms = [term for term in expected.get("terms", [])
                     if term.casefold() not in lowered]
    any_terms = expected.get("terms_any", [])
    if any_terms and not any(term.casefold() in lowered for term in any_terms):
        missing_terms.append("one of: " + " | ".join(any_terms))
    validity = "pass" if answer.strip() and not missing_terms else "fail"
    if expected["outcome"] != "answer":
        return validity, "not_applicable", missing_terms
    if expected.get("review_required"):
        return validity, "manual_review", missing_terms
    observed = _answer_numbers(answer)
    tolerance = Decimal(str(expected.get("tolerance", 0)))
    missing_numbers = []
    for number in expected["numbers"]:
        target = Decimal(str(number))
        match = next((index for index, found in enumerate(observed)
                      if abs(found - target) <= tolerance), None)
        if match is None:
            missing_numbers.append(str(number))
        else:
            observed.pop(match)
    return validity, ("pass" if not missing_numbers else "fail"), (
        missing_terms + ["number: " + number for number in missing_numbers])


def evaluate_case(engine: Any, case: dict[str, Any]) -> dict[str, Any]:
    """Evaluate visible final-turn text, while preserving sessions across turns."""
    started = time.perf_counter()
    expected = case["expected"]
    session_id: str | None = None
    final: dict[str, Any] | None = None
    error: str | None = None
    missing_session = False
    try:
        for index, question in enumerate(case["turns"]):
            payload = {"question": question, "session_id": session_id,
                       "month": case.get("context", {}).get("month"),
                       "borough": case.get("context", {}).get("borough")}
            response = engine.answer(payload)
            if not isinstance(response, dict):
                raise TypeError("answer() must return an object")
            final = response
            if index < len(case["turns"]) - 1:
                next_id = response.get("session_id")
                if not isinstance(next_id, str) or not next_id:
                    missing_session = True
                session_id = next_id if isinstance(next_id, str) and next_id else None
    except Exception as exc:  # Evaluation records the failure; it does not swallow it as a pass.
        error = f"{type(exc).__name__}: {exc}"
    latency_ms = round((time.perf_counter() - started) * 1000, 3)
    answer = final.get("answer") if final else None
    evidence = final.get("evidence") if final else None
    valid_contract = (isinstance(answer, str) and bool(answer.strip())
                      and isinstance(evidence, list)
                      and not final.get("error") if final else False)
    if error or not valid_contract or missing_session:
        validity, numeric = "fail", "not_evaluated"
        issues = [part for part in (
            error,
            "invalid response contract" if not valid_contract else None,
            "missing follow-up session_id" if missing_session else None,
        ) if part]
    else:
        validity, numeric, issues = _checks(answer, expected)
    if expected["outcome"] == "answer":
        safety = "not_applicable"
    else:
        # Unsupported/injection questions must not be backed by invented data evidence.
        safety = "pass" if valid_contract and not evidence and validity == "pass" else "fail"
        if evidence:
            issues.append("unsupported answer returned data evidence")
    if error:
        execution = "error"
    else:
        execution = "pass" if valid_contract else "fail"
    if (execution == "pass" and validity == "pass" and safety != "fail"
            and numeric == "pass"):
        status = "pass"
    elif (execution == "pass" and validity == "pass" and safety != "fail"
          and numeric == "manual_review"):
        status = "needs_review"
    elif (execution == "pass" and validity == "pass" and safety == "pass"
          and numeric == "not_applicable"):
        status = "pass"
    else:
        status = "fail"
    return {"id": case["id"], "category": case["category"], "status": status,
            "validity": validity, "execution": execution,
            "numeric_correctness": numeric, "safety": safety,
            "latency_ms": latency_ms, "issues": issues,
            "answer": answer if isinstance(answer, str) else None}


def evaluate_suite(engine: Any, manifest: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return observed per-case results and aggregate counts, never an invented pass rate."""
    selected = manifest if manifest is not None else load_manifest()
    validate_manifest(selected)
    results = [evaluate_case(engine, case) for case in selected["cases"]]
    counts = Counter(result["status"] for result in results)
    latencies = sorted(result["latency_ms"] for result in results)
    def percentile(p: float) -> float:
        return latencies[max(0, min(len(latencies) - 1, int((len(latencies) - 1) * p)))]
    return {"fixture": selected["fixture"], "case_count": len(results),
            "counts": {status: counts[status] for status in ("pass", "fail", "needs_review")},
            "latency_ms": {"p50": percentile(.50), "p95": percentile(.95),
                           "max": latencies[-1]}, "results": results}
