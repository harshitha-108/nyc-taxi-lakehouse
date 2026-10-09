"""Constrained natural-language intents grounded in published mart values."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import uuid4

from chatbot.data import CsvSource, PostgresSource
from chatbot.local_ai import plan_question

MONTHS = (
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December",
)
BOROUGHS = ("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", "Unknown", "N/A")
PAYMENTS = {
    0: "Flex Fare trip", 1: "credit card", 2: "cash", 3: "no charge",
    4: "dispute", 5: "unknown", 6: "voided trip",
}
MONTH_PATTERN = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|"
    r"December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b(?:\s+(20\d{2}))?",
    re.IGNORECASE,
)
ISO_PATTERN = re.compile(r"(?<!\d)(20\d{2})-(0[1-9]|1[0-2])(?!\d)")


class InvalidRequest(ValueError):
    """A chat request cannot be interpreted safely."""


@dataclass(frozen=True)
class Scope:
    periods: tuple[tuple[int | None, int], ...] | None
    borough: str | None


def _month_from_name(name: str) -> int:
    short = name.casefold()[:3]
    return next(index for index, item in enumerate(MONTHS, 1) if item.casefold()[:3] == short)


def parse_month(value: str | int | None) -> tuple[int | None, int] | None:
    """Accept the Power BI month slicer's number or a human-readable period."""
    if value is None or str(value).strip().casefold() in {"", "all", "(all)"}:
        return None
    raw = str(value).strip()
    if match := ISO_PATTERN.fullmatch(raw):
        return int(match.group(1)), int(match.group(2))
    if raw.isdigit() and 1 <= int(raw) <= 12:
        return None, int(raw)
    if match := MONTH_PATTERN.fullmatch(raw):
        return (int(match.group(2)) if match.group(2) else None,
                _month_from_name(match.group(1)))
    raise InvalidRequest("month must be 1–12, YYYY-MM, or a month name")


def _question_periods(question: str) -> tuple[tuple[int | None, int], ...]:
    periods = [(int(match.group(1)), int(match.group(2)))
               for match in ISO_PATTERN.finditer(question)]
    periods.extend((int(match.group(2)) if match.group(2) else None,
                    _month_from_name(match.group(1)))
                   for match in MONTH_PATTERN.finditer(question))
    return tuple(dict.fromkeys(periods))


def _has_bare_year(question: str) -> bool:
    """A year without a month cannot be represented by the report month scope."""
    without_periods = ISO_PATTERN.sub(" ", MONTH_PATTERN.sub(" ", question))
    return re.search(r"\b20\d{2}\b", without_periods) is not None


def _borough(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidRequest("borough must be a borough name")
    if value.strip().casefold() in {"", "all", "(all)"}:
        return None
    for candidate in BOROUGHS:
        if value.strip().casefold() == candidate.casefold():
            return candidate
    if value.strip().casefold() == "unknown / unmapped":
        return "Unknown"
    raise InvalidRequest("borough must be a NYC borough, EWR, Unknown, or N/A")


def _question_boroughs(question: str) -> tuple[str, ...]:
    question = re.sub(r"\bunknown payment(?: method| type)?\b", "", question,
                      flags=re.IGNORECASE)
    matches = ((match.start(), candidate) for candidate in BOROUGHS
               if (match := re.search(rf"\b{re.escape(candidate)}\b", question,
                                      re.IGNORECASE)))
    return tuple(candidate for _, candidate in sorted(matches))


def _question_payments(question: str) -> tuple[int, ...]:
    patterns = (
        (0, r"\bflex fare\b"),
        (1, r"\b(?:credit card|card)\b"),
        (2, r"\bcash\b"),
        (3, r"\bno charge\b"),
        (4, r"\bdispute\b"),
        (5, r"\bunknown payment\b"),
        (6, r"\bvoided (?:trip|payment)\b"),
    )
    return tuple(code for code, pattern in patterns if re.search(pattern, question, re.IGNORECASE))


def _requested_metrics(question: str) -> tuple[str, ...]:
    metrics = []
    if re.search(r"\b(trips?|rides?|volume|demand)\b", question, re.IGNORECASE):
        metrics.append("trip_count")
    if re.search(r"\b(revenue|total amount|earnings|sales)\b", question, re.IGNORECASE):
        metrics.append("total_revenue")
    if re.search(r"\b(tips?|tipping)\b", question, re.IGNORECASE):
        metrics.append("total_tip_amount")
    return tuple(metrics)


def _is_comparison(question: str) -> bool:
    return re.search(r"\b(compare|versus|vs\.?|difference|more|less|fewer|higher|lower)\b",
                     question, re.IGNORECASE) is not None


def _lowest(question: str) -> bool:
    return re.search(r"\b(fewest|least|lowest|minimum|smallest|bottom)\b",
                     question, re.IGNORECASE) is not None


def _intent(question: str) -> str:
    q = question.casefold()
    if re.search(r"\b(help|what can|examples)\b", q):
        return "help"
    if re.search(r"\bmonth\b", q) and re.search(
        r"\b(most|highest|busiest|top|fewest|least|lowest|bottom)\b", q
    ):
        return "peak_month"
    if re.search(r"\b(trend|monthly|by month|each month)\b", q):
        return "monthly"
    if _question_periods(question) and re.search(
        r"\b(compare|versus|vs\.?|change|growth|difference)\b", q
    ):
        return "monthly"
    if len(_question_periods(question)) == 2 and re.search(
        r"\b(increase|decrease|more|less|fewer|higher|lower|between|from)\b", q
    ):
        return "monthly"
    if re.search(r"\b(zone|neighbou?rhood|pickup area)\b", q) and re.search(
        r"\b(top|most|highest|busiest|leading|peak|greatest|largest|"
        r"fewest|least|lowest|minimum|smallest|bottom)\b", q
    ):
        return "top_zone"
    if re.search(r"\b(hour|time of day|peak time)\b", q):
        return "busiest_hour"
    if re.search(r"\b(day|date)\b", q) and re.search(
        r"\b(busiest|highest|most|peak|fewest|least|lowest|bottom)\b", q
    ):
        return "busiest_day"
    if re.search(r"\b(payment|credit card|cash|card)\b", q):
        return "payment"
    if re.search(r"\b(borough|bronx|brooklyn|manhattan|queens|staten island)\b", q) and re.search(
        r"\b(top|most|highest|busiest|leading|which|fewest|least|lowest|bottom)\b", q
    ):
        return "top_borough"
    if re.search(r"\b(tip|tips|tipping)\b", q):
        return "tips"
    if re.search(r"\b(average|avg|per trip)\b.*\b(revenue|amount|value)\b", q):
        return "average_trip_value"
    if re.search(r"\b(revenue|total amount|earnings|sales)\b", q):
        return "revenue"
    if re.search(r"\b(trips?|rides?|pickups?|volume|demand)\b", q):
        return "trips"
    if re.search(r"\b(overview|summary|snapshot)\b", q):
        return "overview"
    return "help"


def _number(value: Any) -> Decimal:
    return Decimal(str(value)) if value not in {None, ""} else Decimal(0)


def _trips(rows: list[dict[str, Any]]) -> int:
    return sum(int(row["trip_count"]) for row in rows)


def _sum(rows: list[dict[str, Any]], column: str) -> Decimal:
    return sum((_number(row[column]) for row in rows), Decimal(0))


def _rank_value(row: dict[str, Any], metric: str) -> Decimal:
    return _number(row[metric])


def _rank_phrase(metric: str, value: Decimal) -> str:
    if metric == "trip_count":
        return f"{int(value):,} trips"
    if metric == "total_revenue":
        return f"${value:,.2f} in TLC total amount"
    return f"${value:,.2f} in recorded tips"


def _metric_unit(metric: str) -> str:
    return "trips" if metric == "trip_count" else "USD"


def _share_base(metric: str) -> str:
    return ("trips" if metric == "trip_count" else
            "TLC total amount" if metric == "total_revenue" else "recorded tips")


def _change_text(metric: str, subject: str, subject_value: Decimal,
                 baseline: str, baseline_value: Decimal) -> tuple[str, Decimal, Decimal | None]:
    difference = subject_value - baseline_value
    if difference == 0:
        sentence = f"{subject} and {baseline} were equal at {_rank_phrase(metric, subject_value)}."
    else:
        direction = "more" if difference > 0 else "less"
        sentence = (f"{subject} had {_rank_phrase(metric, abs(difference))} {direction} "
                    f"than {baseline}.")
    if baseline_value <= 0:
        return (sentence + " Percentage change is undefined because the baseline "
                "is zero or negative.", difference, None)
    change = difference * Decimal(100) / baseline_value
    return sentence[:-1] + f" ({change:+.1f}% versus {baseline}).", difference, change


def _period_text(period: tuple[int | None, int]) -> str:
    year, month = period
    return MONTHS[month - 1] + (f" {year}" if year else "")


def _scope_text(scope: Scope, rows: list[dict[str, Any]]) -> str:
    if scope.periods:
        dates = ", ".join(_period_text(period) for period in scope.periods)
    else:
        values = sorted({(int(row["_source_year"]), int(row["_source_month"])) for row in rows})
        if not values:
            dates = "the selected period"
        elif len(values) == 1:
            dates = _period_text(values[0])
        else:
            dates = f"{_period_text(values[0])}–{_period_text(values[-1])}"
    return f"in {scope.borough} for {dates}" if scope.borough else f"for {dates}"


class ChatEngine:
    def __init__(self, source: CsvSource | PostgresSource) -> None:
        self.source = source

    def _rows(self, table: str, scope: Scope, *, borough: bool = False) -> list[dict[str, Any]]:
        selected = scope.periods[0] if scope.periods and len(scope.periods) == 1 else None
        rows = self.source.fetch(table, selected, scope.borough if borough else None)
        if scope.periods and len(scope.periods) > 1:
            rows = [row for row in rows if any(
                (year is None or int(row["_source_year"]) == year)
                and int(row["_source_month"]) == month
                for year, month in scope.periods
            )]
        return rows

    def _evidence(
        self, table: str, scope: Scope, metric: str, value: int | Decimal | str,
        unit: str = "",
    ) -> dict[str, Any]:
        return {"metric": metric, "value": str(value), "unit": unit,
                "source": self.source.label(table),
                "filters": {"periods": ([_period_text(p) for p in scope.periods]
                                         if scope.periods else []),
                            "borough": scope.borough, "taxi_type": "yellow"}}

    def _borough_comparison(self, scope: Scope, boroughs: tuple[str, str],
                            metric: str) -> dict[str, Any]:
        table = "pickup_zone_performance"
        rows = self._rows(table, scope)
        totals: dict[str, Decimal] = defaultdict(Decimal)
        seen: set[str] = set()
        for row in rows:
            name = str(row.get("borough") or "Unknown")
            if name in boroughs:
                totals[name] += _rank_value(row, metric)
                seen.add(name)
        if len(seen) != 2:
            missing = ", ".join(name for name in boroughs if name not in seen)
            return {"answer": f"I cannot compare those boroughs: no published rows for {missing} "
                    "in the selected period.", "evidence": []}
        subject, baseline = boroughs
        subject_value, baseline_value = totals[subject], totals[baseline]
        change, difference, percent = _change_text(
            metric, subject, subject_value, baseline, baseline_value)
        subject_scope = Scope(scope.periods, subject)
        baseline_scope = Scope(scope.periods, baseline)
        evidence = [
            self._evidence(table, subject_scope, metric, subject_value, _metric_unit(metric)),
            self._evidence(table, baseline_scope, metric, baseline_value, _metric_unit(metric)),
            self._evidence(table, scope, f"{subject} - {baseline} {metric}",
                           difference, _metric_unit(metric)),
        ]
        if percent is not None:
            evidence.append(self._evidence(table, scope,
                                           f"{subject} vs {baseline} percent_change",
                                           percent.quantize(Decimal("0.1")), "%"))
        return {"answer": f"{_scope_text(scope, rows).capitalize()}, "
                f"{subject}: {_rank_phrase(metric, subject_value)}; "
                f"{baseline}: {_rank_phrase(metric, baseline_value)}. {change}",
                "evidence": evidence}

    def answer(self, payload: dict[str, Any]) -> dict[str, Any]:
        question = payload.get("question")
        if not isinstance(question, str) or not 1 <= len(question.strip()) <= 500:
            raise InvalidRequest("question must be 1–500 characters")
        session = payload.get("session_id")
        if session is not None and (not isinstance(session, str) or len(session) > 128):
            raise InvalidRequest("session_id must be a string of at most 128 characters")
        session = session or uuid4().hex
        context_month = parse_month(payload.get("month"))
        context_borough = _borough(payload.get("borough"))
        if re.search(r"\b(why|caused?|reason|forecast|predict(?:ion)?)\b",
                     question, re.IGNORECASE):
            return {"answer": "These published aggregates show what happened, not its "
                    "cause or a forecast. Ask for a measured comparison instead.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if re.search(r"\b(?:last|this|current|previous) month\b|\b(?:today|yesterday)\b",
                     question, re.IGNORECASE):
            return {"answer": "Please name an exact month such as 2024-03. Relative dates "
                    "do not identify a published report month reliably.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if re.search(r"\b(drop[- ]?off|destination|route)\b", question, re.IGNORECASE):
            return {"answer": "The published chat marts do not include pickup-to-dropoff "
                    "routes. I can analyse pickup zones and boroughs instead.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if re.search(r"\b(longest|shortest)\b",
                     question, re.IGNORECASE):
            return {"answer": "I cannot answer distance or duration rankings from this "
                    "chat. Please ask for trip, TLC total-amount, or tip totals.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if _has_bare_year(question):
            return {"answer": "I cannot filter by a year alone. Please specify a month as "
                    "YYYY-MM or a month name with its year.",
                    "evidence": [], "session_id": session, "ai_used": False}
        question_boroughs = _question_boroughs(question)
        borough_comparison = len(question_boroughs) == 2 and _is_comparison(question)
        if len(question_boroughs) > 1 and not borough_comparison:
            return {"answer": "I cannot combine multiple boroughs without a comparison. "
                    "Please ask to compare two boroughs using one metric.",
                    "evidence": [], "session_id": session, "ai_used": False}
        question_payments = _question_payments(question)
        if len(question_payments) > 1:
            return {"answer": "I cannot compare multiple payment methods in one question. "
                    "Please ask about one method at a time.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if (re.search(r"\b(zone|neighbou?rhood|pickup area)\b", question, re.IGNORECASE)
                and not re.search(r"\b(top|most|highest|busiest|leading|peak|greatest|largest|"
                                  r"fewest|least|lowest|minimum|smallest|bottom)\b",
                                  question, re.IGNORECASE)):
            return {"answer": "I cannot filter totals to a specific pickup zone from this "
                    "chat. Please ask which pickup zone ranked highest instead.",
                    "evidence": [], "session_id": session, "ai_used": False}
        mentioned = _question_periods(question)
        month_comparison = len(mentioned) == 2 and _intent(question) == "monthly"
        if (re.search(r"\b(distance|duration|median)\b", question, re.IGNORECASE)
                or (re.search(r"\bfare\b", question, re.IGNORECASE)
                    and not re.search(r"\bflex fare\b", question, re.IGNORECASE))
                or (re.search(r"\b(average|avg)\b", question, re.IGNORECASE)
                    and (not re.search(r"\b(revenue|total amount|value)\b", question,
                                       re.IGNORECASE)
                         or re.search(r"\b(tips?|tipping)\b", question, re.IGNORECASE)))
                or (re.search(r"\b(percentage|percent|share)\b", question, re.IGNORECASE)
                    and not (question_payments or borough_comparison or month_comparison))
                or re.search(r"\b(?:per|each)\s+(?:day|hour)\b", question, re.IGNORECASE)):
            return {"answer": "I cannot answer that measure from the published chat marts. "
                    "Please ask about trip totals, TLC total amount, recorded tips, "
                    "or a supported ranking.",
                    "evidence": [], "session_id": session, "ai_used": False}
        scope = Scope(mentioned or ((context_month,) if context_month else None),
                      (None if borough_comparison else
                       question_boroughs[0] if question_boroughs else context_borough))
        requested_metrics = _requested_metrics(question)
        if borough_comparison:
            if len(requested_metrics) > 1:
                result = {"answer": "Please compare two boroughs using one metric: trips, "
                          "TLC total amount, or tips.", "evidence": []}
            else:
                result = self._borough_comparison(
                    scope, (question_boroughs[0], question_boroughs[1]),
                    requested_metrics[0] if requested_metrics else "trip_count")
            return {**result, "session_id": session, "ai_used": False}
        heuristic = _intent(question)
        if question_payments and scope.borough and any(
            metric != "trip_count" for metric in requested_metrics
        ):
            return {"answer": "The borough payment breakdown contains trip counts only. "
                    "Please ask for payment trips or share in that borough.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if question_payments and heuristic in {"trips", "revenue", "tips", "help"}:
            heuristic = "payment"
        elif question_payments and heuristic != "payment":
            return {"answer": "The published data cannot answer that breakdown for a "
                    "specific payment method. Please ask for a supported payment total instead.",
                    "evidence": [], "session_id": session, "ai_used": False}
        if (not mentioned and not borough_comparison and re.search(
            r"\b(compare|versus|vs\.?|difference)\b", question, re.IGNORECASE
        ) and heuristic != "monthly"):
            return {"answer": "Please name two months or two boroughs to compare, "
                    "or ask for a monthly comparison.", "evidence": [],
                    "session_id": session, "ai_used": False}
        model_plan = (plan_question(question) if heuristic == "help" and not re.search(
            r"\b(help|what can you|examples)\b", question, re.IGNORECASE
        ) else None)
        intent = model_plan.intent if model_plan else heuristic
        if model_plan and model_plan.metric and not requested_metrics:
            requested_metrics = (model_plan.metric,)
        ranking_intents = {"top_borough", "top_zone", "busiest_hour", "busiest_day", "peak_month"}
        if intent in ranking_intents and re.search(
            r"\b(average|avg|median|distance|duration|fare|percentage|percent|share)\b",
            question, re.IGNORECASE
        ):
            return {"answer": "I cannot rank that measure from the published chat marts. "
                    "Please ask for trip, TLC total-amount, or tip totals.",
                    "evidence": [], "session_id": session, "ai_used": model_plan is not None}
        if intent in ranking_intents | {"payment"} and len(requested_metrics) > 1:
            return {"answer": "Please ask for a single ranking metric: trips, TLC total "
                    "amount, or tips.", "evidence": [], "session_id": session,
                    "ai_used": model_plan is not None}
        # A local model may classify the wording, but fixed code performs every calculation.
        result = self._answer_intent(
            intent, scope, question,
            requested_metrics[0] if requested_metrics else "trip_count",
            requested_metrics,
        )
        return {**result, "session_id": session, "ai_used": model_plan is not None}

    def _answer_intent(self, intent: str, scope: Scope, question: str,
                       rank_metric: str,
                       requested_metrics: tuple[str, ...]) -> dict[str, Any]:
        if intent == "help":
            return {"answer": (
                "Ask about trip totals, TLC total-amount revenue, tips, highest or lowest "
                "hour, day, borough, zone, or month, payment totals and shares, or "
                "two-month and two-borough differences with percent change. "
                "I use published yellow taxi aggregates and can follow the report's month and "
                "borough selections."
            ), "evidence": []}
        if intent in {"overview", "trips", "revenue", "tips", "average_trip_value"}:
            table = "pickup_zone_performance" if scope.borough else "daily_trip_metrics"
            rows = self._rows(table, scope, borough=bool(scope.borough))
            if not rows:
                return self._empty(scope)
            trips = _trips(rows)
            revenue = _sum(rows, "total_revenue")
            place = _scope_text(scope, rows)
            evidence = [self._evidence(table, scope, "trip_count", trips, "trips"),
                        self._evidence(table, scope, "total_revenue", revenue, "USD")]
            if intent == "trips":
                answer = f"There were {trips:,} yellow taxi trips {place}."
            elif intent == "revenue":
                answer = (f"TLC total amount was ${revenue:,.2f} {place}. "
                          "This is trip-charge revenue, not company accounting revenue.")
            elif intent == "tips":
                tips = _sum(rows, "total_tip_amount")
                evidence.append(self._evidence(table, scope, "total_tip_amount", tips, "USD"))
                answer = f"Recorded tips totaled ${tips:,.2f} across {trips:,} trips {place}."
            elif intent == "average_trip_value":
                average = revenue / trips if trips else Decimal(0)
                evidence.append(self._evidence(table, scope, "total_revenue / trip_count",
                                               average.quantize(Decimal("0.01")), "USD/trip"))
                answer = f"Average TLC total amount was ${average:,.2f} per trip {place}."
            else:
                answer = (f"{trips:,} yellow taxi trips and ${revenue:,.2f} in TLC total amount "
                          f"{place}.")
            return {"answer": answer, "evidence": evidence}
        if intent in {"top_borough", "top_zone"}:
            table = "pickup_zone_performance"
            rows = self._rows(table, scope, borough=bool(scope.borough))
            if not rows:
                return self._empty(scope)
            key_columns = ("borough", "zone") if intent == "top_zone" else ("borough",)
            grouped: dict[tuple[str, ...], Decimal] = defaultdict(Decimal)
            for row in rows:
                key = tuple(str(row.get(column) or "Unknown") for column in key_columns)
                grouped[key] += _rank_value(row, rank_metric)
            ascending = _lowest(question)
            winner, value = sorted(grouped.items(), key=lambda item: (
                item[1] if ascending else -item[1], item[0]))[0]
            name = " / ".join(winner)
            kind = "pickup zone" if len(key_columns) == 2 else "pickup borough"
            if intent == "top_borough" and len(grouped) == 1:
                guidance = ("Clear the borough filter to compare boroughs." if scope.borough
                            else "A cross-borough ranking needs more published boroughs.")
                answer = (f"Only {name} has published rows in this borough scope, with "
                          f"{_rank_phrase(rank_metric, value)} {_scope_text(scope, rows)}. "
                          + guidance)
                return {"answer": answer,
                        "evidence": [self._evidence(table, scope, "pickup_borough", name),
                                     self._evidence(table, scope, rank_metric, value,
                                                    "trips" if rank_metric == "trip_count"
                                                    else "USD")]}
            if rank_metric == "trip_count":
                adjective = "least busy" if ascending else "busiest"
            else:
                adjective = "lowest-ranked" if ascending else "highest-ranked"
            return {"answer": f"The {adjective} {kind} was {name}, with "
                    f"{_rank_phrase(rank_metric, value)} "
                    f"{_scope_text(scope, rows)}.",
                    "evidence": [self._evidence(table, scope, f"top_{kind.replace(' ', '_')}",
                                                name),
                                 self._evidence(table, scope, rank_metric, value,
                                                "trips" if rank_metric == "trip_count" else "USD")]}
        if intent == "busiest_hour":
            if rank_metric == "total_tip_amount" or (scope.borough and rank_metric != "trip_count"):
                return {"answer": "The published hourly data does not contain that metric "
                        "at the selected borough grain.", "evidence": []}
            table = "takeaway_breakdown" if scope.borough else "hourly_demand"
            rows = self._rows(table, scope, borough=bool(scope.borough))
            if not rows:
                return self._empty(scope)
            grouped: dict[int, Decimal] = defaultdict(Decimal)
            for row in rows:
                grouped[int(row["pickup_hour"])] += _rank_value(row, rank_metric)
            ascending = _lowest(question)
            hour, value = sorted(grouped.items(), key=lambda item: (
                item[1] if ascending else -item[1], item[0]))[0]
            clock = f"{hour % 12 or 12} {'AM' if hour < 12 else 'PM'}"
            if rank_metric == "trip_count":
                description = "least busy" if ascending else "busiest"
            else:
                description = "lowest-revenue" if ascending else "highest-revenue"
            return {"answer": f"The {description} pickup hour was {clock} ({hour:02d}:00), "
                    f"with {_rank_phrase(rank_metric, value)} {_scope_text(scope, rows)}.",
                    "evidence": [self._evidence(table, scope, "pickup_hour", hour, "0–23"),
                                 self._evidence(table, scope, rank_metric, value,
                                                "trips" if rank_metric == "trip_count" else "USD")]}
        if intent == "busiest_day":
            if scope.borough:
                return {"answer": "The published daily mart has no borough breakdown, so I "
                        "cannot identify a busiest day for that borough from this report data.",
                        "evidence": []}
            table = "daily_trip_metrics"
            rows = self._rows(table, scope)
            if not rows:
                return self._empty(scope)
            grouped: dict[str, Decimal] = defaultdict(Decimal)
            excluded = 0
            for row in rows:
                try:
                    pickup_day = date.fromisoformat(str(row["pickup_date"]))
                except (TypeError, ValueError):
                    excluded += 1
                    continue
                source_period = int(row["_source_year"]), int(row["_source_month"])
                if (pickup_day.year, pickup_day.month) != source_period:
                    excluded += 1
                    continue
                grouped[pickup_day.isoformat()] += _rank_value(row, rank_metric)
            if not grouped:
                return {"answer": "I cannot rank a pickup date in this scope because no "
                        "pickup dates fall within their published source month.",
                        "evidence": []}
            ascending = _lowest(question)
            day, value = sorted(grouped.items(), key=lambda item: (
                item[1] if ascending else -item[1], item[0]))[0]
            if rank_metric == "trip_count":
                description = "least busy" if ascending else "busiest"
            else:
                description = "lowest-ranked" if ascending else "highest-ranked"
            answer = (f"The {description} pickup date was {day}, with "
                      f"{_rank_phrase(rank_metric, value)} "
                      f"{_scope_text(scope, rows)}.")
            if excluded:
                answer += " Pickup dates outside their published source month were excluded."
            return {"answer": answer,
                    "evidence": [self._evidence(table, scope, "pickup_date", day),
                                 self._evidence(table, scope, rank_metric, value,
                                                "trips" if rank_metric == "trip_count" else "USD")]}
        if intent == "payment":
            if scope.borough and rank_metric != "trip_count":
                return {"answer": "The borough payment breakdown contains trip counts only. "
                        "I cannot rank payment amounts within a borough.", "evidence": []}
            table = "takeaway_breakdown" if scope.borough else "payment_type_summary"
            rows = self._rows(table, scope, borough=bool(scope.borough))
            if not rows:
                return self._empty(scope)
            grouped: dict[int, Decimal] = defaultdict(Decimal)
            for row in rows:
                grouped[int(row["payment_type"])] += _rank_value(row, rank_metric)
            ascending = _lowest(question)
            ranked = sorted(grouped.items(), key=lambda item: (
                item[1] if ascending else -item[1], item[0]))
            total = sum(grouped.values())
            requested = _question_payments(question)
            code = requested[0] if requested else ranked[0][0]
            value = grouped.get(code, Decimal(0))
            label = PAYMENTS.get(code, f"code {code}")
            share = Decimal(100) * value / total if total > 0 else None
            amount = _rank_phrase(rank_metric, value)
            share_text = f"{share:.1f}% of {_share_base(rank_metric)}" if share is not None else (
                "share unavailable because the total is zero or negative")
            if requested:
                answer = (f"{label.capitalize()} accounted for {amount} "
                          f"{_scope_text(scope, rows)} ({share_text}).")
            else:
                if rank_metric == "trip_count":
                    qualifier = "least common" if ascending else "most common"
                else:
                    qualifier = "lowest" if ascending else "highest"
                answer = (f"{label.capitalize()} was the {qualifier} payment method "
                          f"{_scope_text(scope, rows)}: {amount} ({share_text}).")
            evidence = [self._evidence(table, scope, "payment_type", code),
                        self._evidence(table, scope, rank_metric, value,
                                       _metric_unit(rank_metric))]
            if share is not None:
                share_metric = ("trip_share" if rank_metric == "trip_count" else
                                "revenue_share" if rank_metric == "total_revenue" else "tip_share")
                evidence.append(self._evidence(table, scope, share_metric,
                                               share.quantize(Decimal("0.1")), "%"))
            return {"answer": answer, "evidence": evidence}
        if intent in {"monthly", "peak_month"}:
            table = "pickup_zone_performance" if scope.borough else "daily_trip_metrics"
            rows = self._rows(table, scope, borough=bool(scope.borough))
            if not rows:
                return self._empty(scope)
            grouped: dict[tuple[int, int], dict[str, Decimal]] = defaultdict(
                lambda: {"trip_count": Decimal(0), "total_revenue": Decimal(0),
                         "total_tip_amount": Decimal(0)})
            for row in rows:
                period = int(row["_source_year"]), int(row["_source_month"])
                for metric in grouped[period]:
                    grouped[period][metric] += _rank_value(row, metric)
            requested = requested_metrics
            metrics = (requested or ("trip_count", "total_revenue")) if intent == "monthly" else (
                rank_metric,)
            evidence = []
            for period, totals in sorted(grouped.items()):
                for metric in metrics:
                    evidence.append(self._evidence(table, scope,
                                                   f"{_period_text(period)} {metric}",
                                                   totals[metric],
                                                   "trips" if metric == "trip_count" else "USD"))
            if intent == "peak_month":
                ascending = _lowest(question)
                period, totals = sorted(grouped.items(), key=lambda item: (
                    item[1][rank_metric] if ascending else -item[1][rank_metric],
                    item[0]))[0]
                if rank_metric == "trip_count":
                    description = "fewest trips" if ascending else "most trips"
                elif rank_metric == "total_revenue":
                    description = ("lowest TLC total amount" if ascending
                                   else "highest TLC total amount")
                else:
                    description = "fewest tips" if ascending else "most tips"
                value = (f"{int(totals[rank_metric]):,} trips" if rank_metric == "trip_count"
                         else f"${totals[rank_metric]:,.2f}")
                if len(grouped) == 1:
                    guidance = ("Ask without a month restriction to compare months."
                                if scope.periods else "A peak needs more published months.")
                    answer = (f"Only {_period_text(period)} has published rows in this "
                              "month scope, "
                              f"with {value}. " + guidance)
                else:
                    answer = f"{_period_text(period)} had the {description}: {value}"
                    answer += f" in {scope.borough}." if scope.borough else "."
            else:
                parts = [f"{_period_text(period)}: " + ", ".join(
                    _rank_phrase(metric, totals[metric]) for metric in metrics)
                    for period, totals in sorted(grouped.items())]
                if len(grouped) == 1:
                    answer = "Only one month has published rows in this scope: " + parts[0] + "."
                else:
                    answer = "Monthly comparison"
                    answer += f" for {scope.borough}" if scope.borough else ""
                    answer += ": " + "; ".join(parts) + "."
                if scope.periods and len(scope.periods) == 2:
                    ordered = []
                    for year, month in scope.periods:
                        matches = [period for period in grouped if period[1] == month
                                   and (year is None or period[0] == year)]
                        if len(matches) != 1:
                            return {"answer": "I cannot calculate a two-month change because "
                                    "one month is missing or its year is ambiguous in the "
                                    "published snapshot. Specify two YYYY-MM periods.",
                                    "evidence": []}
                        ordered.append(matches[0])
                    if re.search(r"\bfrom\b.*\bto\b", question, re.IGNORECASE):
                        baseline, subject = ordered
                    elif re.search(r"\b(more|less|fewer|higher|lower)\b.*\bthan\b",
                                   question, re.IGNORECASE):
                        subject, baseline = ordered
                    else:
                        baseline, subject = sorted(ordered)
                    for metric in metrics:
                        change_text, difference, percent = _change_text(
                            metric, _period_text(subject), grouped[subject][metric],
                            _period_text(baseline), grouped[baseline][metric])
                        answer += " " + change_text
                        difference_metric = (
                            f"{_period_text(subject)} - {_period_text(baseline)} {metric}")
                        evidence.append(self._evidence(
                            table, scope, difference_metric,
                            difference, _metric_unit(metric)))
                        if percent is not None:
                            percent_metric = (
                                f"{_period_text(subject)} vs {_period_text(baseline)} "
                                "percent_change")
                            evidence.append(self._evidence(
                                table, scope, percent_metric,
                                percent.quantize(Decimal("0.1")), "%"))
            return {"answer": answer, "evidence": evidence}
        raise AssertionError("unreachable intent")

    @staticmethod
    def _empty(scope: Scope) -> dict[str, Any]:
        filters = []
        if scope.borough:
            filters.append(scope.borough)
        if scope.periods:
            filters.extend(_period_text(period) for period in scope.periods)
        suffix = " for " + ", ".join(filters) if filters else ""
        return {"answer": "I found no published yellow taxi aggregate rows" + suffix + ".",
                "evidence": []}
