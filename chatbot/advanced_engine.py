"""Bounded analytical extensions around the existing City Pulse chat contract.

Published aggregate rows, not a language model, supply every numeric answer.
The legacy engine remains the fallback for its previously supported questions.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from chatbot.analytics_ops import (
    AnalyticsError,
    PlanStep,
    aggregate_metric,
    bounded_steps,
    compare_periods,
    compare_shares,
    compare_values,
    detect_anomalies,
    monthly_trend,
    weekday_weekend,
)
from chatbot.data import CsvSource, PostgresSource, SourceError
from chatbot.engine import (
    MONTHS,
    PAYMENTS,
    ChatEngine,
    InvalidRequest,
    _borough,
    _intent,
    _question_boroughs,
    _question_payments,
    _question_periods,
    parse_month,
)
from chatbot.metrics import metric_spec
from chatbot.session import ConversationContext, SessionStore
from chatbot.sql_ai import propose_analytic_sql

_MONTH = re.compile(r"\b(?:month|monthly|by month|each month)\b", re.I)
_NUMBER = re.compile(r"\b(?:top|first|leading)\s+(\d{1,2}|one|two|three|four|five|ten)\b", re.I)
_NUMBER_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "ten": 10}
_COMPARISON = re.compile(
    r"\b(compare|comparison|change|difference|growth|from|versus|vs\.?|"
    r"increase|decrease|decline|busier|higher|lower)\b", re.I,
)


def _period_name(period: tuple[int, int]) -> str:
    return f"{MONTHS[period[1] - 1]} {period[0]}"


def _amount(value: Decimal | None, unit: str) -> str:
    if value is None:
        return "unavailable"
    if unit == "trips":
        return f"{value:,.0f} trips"
    if unit in {"USD", "USD/trip"}:
        return f"${value:,.2f}" + (" per trip" if unit == "USD/trip" else "")
    if unit == "miles/trip":
        return f"{value:,.2f} miles per trip"
    return f"{value:,.2f} {unit}"


def _pct(value: Decimal | None) -> str:
    return "undefined" if value is None else f"{value:+,.1f}%"


def _daily_amount(value: Decimal | None, unit: str) -> str:
    if value is None:
        return "unavailable"
    if unit == "trips":
        return f"{value:,.1f} trips per observed day"
    return _amount(value, unit) + " per observed day"


def _metric(question: str) -> str:
    q = question.casefold()
    if "fare" in q and "flex fare" not in q:
        return ("average_fare_amount" if re.search(r"\b(avg|average|mean|per trip)\b", q)
                else "estimated_fare_amount")
    if re.search(r"\b(tip|tips|gratuity|gratuities)\b", q):
        return "tip_amount"
    if re.search(r"\b(distance|miles)\b|\bhow far\b", q):
        return "average_trip_distance"
    if re.search(r"\b(revenue|total amount|money|dollars|earnings)\b", q):
        return "total_amount"
    return "trip_count"


_METRIC_WORDING = {
    "trip_count": "trips",
    "tip_amount": "recorded tips",
    "total_amount": "TLC total amount",
    "estimated_fare_amount": "estimated fare amount",
    "average_fare_amount": "average fare",
    "average_trip_distance": "average trip distance",
}


def _explicit_metric(question: str) -> str | None:
    """Return a metric only when the current turn actually names one."""
    if re.search(r"\b(tips?|gratuity|gratuities)\b", question, re.I):
        return "tip_amount"
    if re.search(r"\b(fares?|distance|miles)\b", question, re.I):
        return _metric(question)
    if re.search(r"\b(revenue|total amount|trip amount|money|earnings)\b",
                 question, re.I):
        return "total_amount"
    if re.search(r"\b(trips?|rides?|volume|demand|pickups?)\b", question, re.I):
        return "trip_count"
    return None


def _unsupported_metric(question: str) -> bool:
    """Do not coerce a clearly unavailable measure into default trip counts."""
    unsupported = bool(re.search(
        r"\b(?:co2|co₂|carbon|emissions?|fuel|gasoline|speed|wait(?:ing)? time|"
        r"driver earnings|trip duration)\b", question, re.I,
    ))
    # Peer medians describe the supported anomaly calculation; an arbitrary
    # median taxi measure is not a published metric.
    return unsupported or bool(re.search(r"\bmedian\b", question, re.I) and not
                               re.search(r"\b(?:peer median|unusual(?:ly)?|anomal(?:y|ies)|"
                                         r"outlier)\b", question, re.I))


def _month_label(value: str) -> str:
    year, month = (int(part) for part in value.split("-"))
    return _period_name((year, month))


def _requested_month_label(question: str, context: ConversationContext) -> str | None:
    mentioned = _question_periods(question)
    if len(mentioned) != 1:
        return None
    year, month = mentioned[0]
    if year is None and context.month_referent:
        year = int(context.month_referent[:4])
    return f"{MONTHS[month - 1]} {year}" if year is not None else MONTHS[month - 1]


class AdvancedChatEngine:
    """Extends the existing API without an unbounded planning/agent loop."""

    def __init__(self, source: CsvSource | PostgresSource,
                 *, sessions: SessionStore | None = None,
                 sql_executor: Any = None) -> None:
        self.source = source
        self.legacy = ChatEngine(source)
        self.sessions = sessions or SessionStore()
        self.sql_executor = sql_executor

    def _reply(self, session_id: str, answer: str, *, calculation: str = "",
               evidence: list[dict[str, Any]] | None = None,
               context: ConversationContext | None = None,
               ai_used: bool = False) -> dict[str, Any]:
        if context is not None:
            self.sessions.update(session_id, context)
        elif evidence:
            # A new analytical answer without an established singular referent
            # must not leave the previous answer's month or group in focus.
            self.sessions.update(session_id, ConversationContext())
        return {"answer": answer, "evidence": evidence or [], "session_id": session_id,
                "ai_used": ai_used, "calculation": calculation}

    def _evidence(self, mart: str, metric: str, value: Any,
                  periods: tuple[tuple[int, int], ...] = ()) -> dict[str, Any]:
        return {"metric": metric, "value": str(value), "unit": metric_spec(metric).unit,
                "source": self.source.label(mart),
                "filters": {"periods": [_period_name(p) for p in periods],
                            "taxi_type": "yellow"}}

    def _available_periods(self) -> tuple[tuple[int, int], ...]:
        rows = self.source.fetch("daily_trip_metrics")
        return tuple(sorted({(int(row["_source_year"]), int(row["_source_month"]))
                             for row in rows}))

    @staticmethod
    def _resolve_periods(question: str, payload: dict[str, Any],
                         available: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
        mentioned = _question_periods(question)
        if not mentioned:
            selected = parse_month(payload.get("month"))
            mentioned = (selected,) if selected else ()
        resolved = []
        for year, month in mentioned:
            matches = [period for period in available
                       if period[1] == month and (year is None or period[0] == year)]
            if len(matches) != 1:
                raise AnalyticsError(
                    f"{MONTHS[month - 1]} {year or ''} is not available or is ambiguous. "
                    "Choose a published YYYY-MM month."
                )
            resolved.append(matches[0])
        return tuple(dict.fromkeys(resolved))

    @staticmethod
    def _month_rows(rows: list[dict[str, Any]], period: tuple[int, int]) -> list[dict[str, Any]]:
        return [row for row in rows if (int(row["_source_year"]),
                                        int(row["_source_month"])) == period]

    @staticmethod
    def _zone_counts(rows: list[dict[str, Any]]) -> dict[str, tuple[str, int]]:
        totals: dict[str, tuple[str, int]] = {}
        for row in rows:
            zone_id = str(row["pickup_location_id"])
            label = str(row.get("zone") or f"Zone {zone_id}")
            previous = totals.get(zone_id, (label, 0))[1]
            totals[zone_id] = label, previous + int(row["trip_count"])
        return totals

    def _zone_scope(self, question: str, payload: dict[str, Any]) -> str | None:
        named = _question_boroughs(question)
        if len(named) > 1:
            raise AnalyticsError("Choose one pickup borough when ranking zones.")
        return named[0] if named else _borough(payload.get("borough"))

    def _rank_zones(self, question: str, payload: dict[str, Any],
                    periods: tuple[tuple[int, int], ...], session_id: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("aggregate", "trip_count", 200)])
        if len(periods) > 1:
            raise AnalyticsError("Choose one month for a top-zone ranking.")
        borough = self._zone_scope(question, payload)
        rows = self.source.fetch("pickup_zone_performance", periods[0] if periods else None,
                                 borough)
        counts = self._zone_counts(rows)
        if not counts:
            return self._reply(
                session_id, "No published pickup-zone rows match that period and borough."
            )
        match = _NUMBER.search(question)
        n = min(10, int(match.group(1)) if match and match.group(1).isdigit() else
                _NUMBER_WORDS.get(match.group(1).lower(), 5) if match else
                1 if re.search(r"\bwhich\s+(?:pickup\s+)?zone\b", question, re.I) else 5)
        low = bool(re.search(r"\b(bottom|lowest|fewest|least)\b", question, re.I))
        ranked = sorted(counts.items(), key=lambda item: (item[1][1] if low else -item[1][1],
                                                          item[1][0], item[0]))[:n]
        period_label = _period_name(periods[0]) if periods else "the published months"
        heading = f"{'Bottom' if low else 'Top'} {len(ranked)} pickup zones in {period_label}"
        if borough:
            heading += f" ({borough})"
        parts = [f"{position}. {label}: {count:,} trips"
                 for position, (_, (label, count)) in enumerate(ranked, 1)]
        context = ConversationContext(metric="trip_count",
                                      periods=tuple(f"{y}-{m:02d}" for y, m in periods),
                                      zones=tuple(zone_id for zone_id, _ in ranked),
                                      borough=borough)
        return self._reply(session_id, heading + ": " + "; ".join(parts) + ".",
                           calculation="Sum trip_count by pickup_location_id from "
                           "analytics.pickup_zone_performance, sort, and return at most 10.",
                           evidence=[self._evidence("pickup_zone_performance", "trip_count",
                                                    count, periods)
                                     for _, (_, count) in ranked], context=context)

    def _zone_followup(self, question: str, payload: dict[str, Any],
                       periods: tuple[tuple[int, int], ...], session_id: str,
                       context: ConversationContext) -> dict[str, Any]:
        if not context.zones:
            return self._reply(session_id, "Please ask for a set of pickup zones first, "
                               "then ask how those zones performed in another month.")
        if len(periods) != 1:
            return self._reply(session_id, "Name one published month for the zone follow-up.")
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("query", "trip_count", 200),
                       PlanStep("compare", "trip_count", 200)])
        prior = tuple(tuple(int(part) for part in value.split("-"))
                      for value in context.periods)
        new_period = periods[0]
        current = self._zone_counts(self.source.fetch("pickup_zone_performance", new_period,
                                                      context.borough))
        baseline = (self._zone_counts(self.source.fetch("pickup_zone_performance", prior[0],
                                                         context.borough))
                    if len(prior) == 1 and prior[0] != new_period else {})
        details = []
        evidence = []
        for zone_id in context.zones[:10]:
            value = current.get(zone_id)
            name = (value or baseline.get(zone_id) or (f"Zone {zone_id}", 0))[0]
            if value is None:
                details.append(f"{name}: no published rows in {_period_name(new_period)}")
                continue
            item = f"{name}: {value[1]:,} trips in {_period_name(new_period)}"
            if zone_id in baseline:
                before = baseline[zone_id][1]
                delta = value[1] - before
                pct = _pct(Decimal(delta) * 100 / Decimal(before)) if before else "undefined"
                item += f" vs {before:,} in {_period_name(prior[0])} ({delta:+,}; {pct})"
            details.append(item)
            evidence.append(self._evidence("pickup_zone_performance", "trip_count",
                                           value[1], (new_period,)))
        return self._reply(session_id, "; ".join(details) + ".",
                           calculation="The same pickup_location_id values from the prior "
                           "answer are summed in the requested source month. Percentage change "
                           "uses the prior month as denominator when nonzero.",
                           evidence=evidence,
                           context=ConversationContext(
                               "trip_count", (f"{new_period[0]}-{new_period[1]:02d}",),
                               context.zones, context.borough,
                           ))

    def _zone_change(self, question: str, payload: dict[str, Any],
                     periods: tuple[tuple[int, int], ...],
                     available: tuple[tuple[int, int], ...], session_id: str) -> dict[str, Any]:
        if not periods and len(available) >= 2:
            periods = available[-2:]
        if len(periods) != 2:
            return self._reply(session_id, "Name two published months to compare pickup zones.")
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("query", "trip_count", 200),
                       PlanStep("compare", "trip_count", 200)])
        first, second = (periods if re.search(r"\bfrom\b.*\bto\b", question, re.I)
                         else sorted(periods))
        borough = self._zone_scope(question, payload)
        previous = self._zone_counts(self.source.fetch("pickup_zone_performance", first, borough))
        current = self._zone_counts(self.source.fetch("pickup_zone_performance", second, borough))
        if not previous or not current:
            return self._reply(
                session_id, "Both months need published pickup-zone rows for comparison."
            )
        percent_rank = bool(re.search(r"\b(percent|percentage|relative)\b", question, re.I))
        declining = bool(re.search(r"\b(decline|decrease|drop|fall|loss|shrink)\b", question, re.I))
        changes = []
        for zone_id in previous.keys() & current.keys():
            name, before = previous[zone_id]
            after = current[zone_id][1]
            # Low baselines distort percentage rankings, so report a threshold.
            if before < (100 if percent_rank else 1):
                continue
            difference = after - before
            if (declining and difference >= 0) or (not declining and difference <= 0):
                continue
            percentage = Decimal(difference) * 100 / Decimal(before)
            changes.append((zone_id, name, before, after, difference, percentage))
        changes.sort(key=lambda item: (item[5] if declining else -item[5])
                     if percent_rank else (item[4] if declining else -item[4]))
        selected = changes[:5]
        if not selected:
            return self._reply(session_id, "No qualifying pickup zones had that change across "
                               f"{_period_name(first)} and {_period_name(second)}.")
        parts = [f"{name}: {before:,} → {after:,} trips ({difference:+,}; {_pct(pct)})"
                 for _, name, before, after, difference, pct in selected]
        direction = "declines" if declining else "increases"
        answer = (f"Largest {'percentage ' if percent_rank else ''}{direction} in pickup-zone "
                  f"trips, {_period_name(first)} to {_period_name(second)}: "
                  + "; ".join(parts) + ".")
        if percent_rank:
            answer += " Zones with fewer than 100 baseline trips were excluded."
        if "fare" in question.casefold():
            answer += (" The published zone mart has no fare measure, so average fare change "
                       "for these zones cannot be verified.")
        return self._reply(session_id, answer,
                           calculation="Join the two approved monthly zone aggregates on "
                           "pickup_location_id in deterministic code; subtract trip counts and "
                           "divide by the first month where the baseline is positive. "
                           "At most two mart reads and one comparison step.",
                           evidence=[self._evidence("pickup_zone_performance", "trip_count",
                                                    item[3], (second,)) for item in selected],
                           context=ConversationContext("trip_count",
                                                       (f"{first[0]}-{first[1]:02d}",
                                                        f"{second[0]}-{second[1]:02d}"),
                                                       tuple(item[0] for item in selected),
                                                       borough))

    def _top_zone_change(self, question: str, payload: dict[str, Any],
                         periods: tuple[tuple[int, int], ...],
                         session_id: str) -> dict[str, Any]:
        """Answer a bounded two-query ranking followed by a comparison."""
        if len(periods) != 2:
            return self._reply(
                session_id, "Name the first and second month for this zone comparison."
            )
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("query", "trip_count", 200),
                       PlanStep("aggregate", "trip_count", 200),
                       PlanStep("compare", "trip_count", 200)])
        first, second = periods
        borough = self._zone_scope(question, payload)
        before = self._zone_counts(self.source.fetch("pickup_zone_performance", first, borough))
        after = self._zone_counts(self.source.fetch("pickup_zone_performance", second, borough))
        if not before or not after:
            return self._reply(session_id, "Both months need published pickup-zone rows.")
        match = _NUMBER.search(question)
        n = min(10, int(match.group(1)) if match and match.group(1).isdigit() else
                _NUMBER_WORDS.get(match.group(1).lower(), 5) if match else 5)
        selected = sorted(before.items(), key=lambda item: (-item[1][1], item[1][0]))[:n]
        details = []
        evidence = []
        for zone_id, (name, count) in selected:
            newer = after.get(zone_id)
            if newer is None:
                details.append(f"{name}: {count:,} trips in {_period_name(first)}; "
                               f"no published row in {_period_name(second)}")
                continue
            delta = newer[1] - count
            percent = Decimal(delta) * 100 / Decimal(count) if count else None
            details.append(f"{name}: {count:,} → {newer[1]:,} trips "
                           f"({delta:+,}; {_pct(percent)})")
            evidence.append(self._evidence("pickup_zone_performance", "trip_count",
                                           newer[1], (second,)))
        answer = (f"Top {len(selected)} pickup zones in {_period_name(first)} and their "
                  f"{_period_name(second)} trip changes: " + "; ".join(details) + ".")
        if "fare" in question.casefold():
            answer += " Zone-level fares are unavailable in the published mart."
        return self._reply(session_id, answer,
                           calculation="Rank at most 10 pickup_location_id values by first-month "
                           "trip_count, retrieve those same IDs for the second month, then "
                           "calculate absolute and baseline-relative changes.",
                           evidence=evidence,
                           context=ConversationContext("trip_count",
                                                       (f"{first[0]}-{first[1]:02d}",
                                                        f"{second[0]}-{second[1]:02d}"),
                                                       tuple(zone_id for zone_id, _ in selected),
                                                       borough))

    def _named_zone_comparison(self, question: str, payload: dict[str, Any],
                               periods: tuple[tuple[int, int], ...],
                               session_id: str) -> dict[str, Any] | None:
        if (len(periods) != 2 or not _COMPARISON.search(question)
                or not re.search(
                    r"\b(zone|neighbou?rhood|pickup|area|district)\b", question, re.I,
                )):
            return None
        borough = self._zone_scope(question, payload)
        rows = self.source.fetch("pickup_zone_performance", borough=borough)
        names = {str(row.get("zone") or "").strip() for row in rows}
        mentioned = [name for name in names if name and re.search(
            rf"(?<!\w){re.escape(name)}(?!\w)", question, re.I)]
        if not mentioned:
            # A partial zone reference may match multiple published zones.
            words = set(re.findall(r"\b[A-Za-z][A-Za-z/-]{3,}\b", question.casefold()))
            candidates = sorted(name for name in names if any(
                word in name.casefold().split() for word in words))
            if len(candidates) > 1:
                return self._reply(session_id, "That pickup-zone name is ambiguous. "
                                   "Please use the full published zone name, such as "
                                   + ", ".join(candidates[:3]) + ".")
            if len(candidates) == 1:
                mentioned = candidates
        if len(mentioned) != 1:
            return None
        first, second = sorted(periods)
        name = mentioned[0]
        before_rows = [row for row in rows if str(row.get("zone")) == name and
                       (int(row["_source_year"]), int(row["_source_month"])) == first]
        after_rows = [row for row in rows if str(row.get("zone")) == name and
                      (int(row["_source_year"]), int(row["_source_month"])) == second]
        if not before_rows or not after_rows:
            return self._reply(session_id, f"{name} has no published row in one of the "
                               "requested months; a verified comparison is unavailable.")
        before = aggregate_metric(before_rows, "trip_count").value
        after = aggregate_metric(after_rows, "trip_count").value
        assert before is not None and after is not None
        delta = after - before
        pct = delta * 100 / before if before > 0 else None
        return self._reply(session_id,
                           f"{name} pickup trips: {_amount(before, 'trips')} in "
                           f"{_period_name(first)} and {_amount(after, 'trips')} in "
                           f"{_period_name(second)} ({delta:+,.0f}; {_pct(pct)}).",
                           calculation="Sum trip_count for the exact published zone name "
                           "within each source month. Percent change uses the earlier month "
                           "as baseline when positive.",
                           evidence=[self._evidence("pickup_zone_performance", "trip_count",
                                                    before, (first,)),
                                     self._evidence("pickup_zone_performance", "trip_count",
                                                    after, (second,))],
                           context=ConversationContext(
                               metric="trip_count",
                               periods=(f"{first[0]}-{first[1]:02d}",
                                        f"{second[0]}-{second[1]:02d}"),
                               zones=(str(before_rows[0]["pickup_location_id"]),),
                               borough=borough,
                               comparison_group=(_period_name(first), _period_name(second)),
                           ))

    def _named_zone_total(self, question: str, payload: dict[str, Any],
                          periods: tuple[tuple[int, int], ...],
                          session_id: str) -> dict[str, Any] | None:
        if len(periods) > 1 or not re.search(r"\b(pickups?|zone|area|neighbou?rhood)\b",
                                            question, re.I):
            return None
        if (re.search(r"\bborough\b", question, re.I) and not
                re.search(r"\b(zone|area|neighbou?rhood)\b", question, re.I)):
            return None
        borough = self._zone_scope(question, payload)
        rows = self.source.fetch("pickup_zone_performance", periods[0] if periods else None,
                                 borough)
        names = {str(row.get("zone") or "").strip() for row in rows}
        words = set(re.findall(r"\b[A-Za-z][A-Za-z/-]{3,}\b", question.casefold()))
        candidates = sorted(name for name in names if name and (
            re.search(rf"(?<!\w){re.escape(name)}(?!\w)", question, re.I)
            or any(word in name.casefold().split() for word in words)))
        if len(candidates) > 1:
            return self._reply(session_id, "That pickup-zone name is ambiguous; "
                               "please use the full published zone name. Matches include "
                               + ", ".join(candidates[:3]) + ".")
        if len(candidates) != 1:
            return None
        name = candidates[0]
        selected = [row for row in rows if str(row.get("zone")) == name]
        total = aggregate_metric(selected, "trip_count").value
        if total is None:
            return self._reply(session_id, f"No published trip total is available for {name}.")
        period_label = _period_name(periods[0]) if periods else "the published months"
        zone_ids = tuple(sorted({str(row["pickup_location_id"]) for row in selected}))
        return self._reply(session_id,
                           f"{name} recorded {_amount(total, 'trips')} in {period_label}.",
                           calculation="Sum trip_count for the exact approved pickup-zone "
                           "label in the selected source-month rows.",
                           evidence=[self._evidence("pickup_zone_performance", "trip_count",
                                                    total, periods)],
                           context=ConversationContext("trip_count",
                                                       tuple(f"{year}-{month:02d}"
                                                             for year, month in periods),
                                                       zone_ids, borough))

    def _zone_metric_followup(self, question: str, payload: dict[str, Any],
                              session_id: str,
                              context: ConversationContext) -> dict[str, Any]:
        """Retain an answer-backed zone ID for an elliptical metric/period change."""
        if len(context.zones) != 1:
            return self._reply(session_id, "Which pickup zone do you mean? The previous "
                               "answer named more than one zone.")
        metric = _explicit_metric(question) or context.pending_metric or context.metric
        if (re.search(r"\b(?:that month|that period|same period)\b", question, re.I)
                and not _question_periods(question) and context.month_referent is None):
            return self._reply(session_id, "Which month do you mean for that pickup "
                               "zone? The previous answer did not establish one month.",
                               context=replace(context, pending_metric=metric))
        if metric not in {"trip_count", "total_amount", "tip_amount"}:
            return self._reply(session_id, "That measure is not published at pickup-zone "
                               "grain. Ask about zone trips, TLC total amount, or "
                               "recorded tips.")
        try:
            mentioned = (self._resolve_periods(question, payload, self._available_periods())
                         if _question_periods(question) else ())
        except AnalyticsError as exc:
            return self._reply(session_id, str(exc))
        if len(mentioned) > 1:
            return self._reply(session_id, "Please name one new month for this pickup-zone "
                               "follow-up, or ask to compare the previous two months.")
        if context.dates or context.date_ranges:
            return self._reply(session_id, "Pickup-zone figures cannot be scoped to the "
                               "previous daily date or date range in the published marts.")
        previous = tuple(tuple(int(part) for part in period.split("-"))
                         for period in context.periods)
        if mentioned and previous and _COMPARISON.search(question):
            if len(previous) != 1:
                return self._reply(session_id, "Which previous month should this zone be "
                                   "compared with?")
            selected = (mentioned[0], previous[0])
        else:
            selected = mentioned or previous
        if len(selected) > 2:
            return self._reply(session_id, "Please narrow this pickup-zone question to "
                               "one or two months.")
        rows = self.source.fetch("pickup_zone_performance")
        zone_rows = [row for row in rows
                     if str(row["pickup_location_id"]) == context.zones[0]]
        if not zone_rows:
            return self._reply(session_id, "That pickup zone has no published rows in "
                               "the current scope.")
        named_boroughs = _question_boroughs(question)
        zone_borough = str(zone_rows[0]["borough"])
        if named_boroughs and named_boroughs != (zone_borough,):
            return self._reply(session_id, f"The saved pickup zone belongs to "
                               f"{zone_borough}; name a new zone for another borough.")
        name = str(zone_rows[0].get("zone") or f"Zone {context.zones[0]}")
        # A new zone name must never be interpreted as an elliptical reference
        # to the previous location ID. Check exact names and distinctive tokens.
        names = {str(row.get("zone") or "").strip() for row in rows}
        named = {candidate for candidate in names if candidate and re.search(
            rf"(?<!\w){re.escape(candidate)}(?!\w)", question, re.I,
        )}
        zone_words = {word.casefold() for word in re.findall(
            r"\b[A-Za-z][A-Za-z/-]{2,}\b", question,
        ) if word.casefold() not in {
            "what", "about", "that", "zone", "pickup", "trips", "tips", "recorded",
            "total", "amount", "revenue", "month", "period", "compare", "with",
            "from", "citywide", "borough", "and", "for", "were", "january",
            "february", "march", "april", "may", "june", "july", "august",
            "september", "october", "november", "december",
        }}
        named.update(candidate for candidate in names if candidate and
                     zone_words.intersection(candidate.casefold().split()))
        if named and named != {name}:
            return self._reply(session_id, "That question may name a different pickup "
                               "zone. Please ask it as a complete question with the "
                               "zone and month so I do not reuse the previous zone.")
        periods_to_query = selected or (None,)
        values = []
        for period in periods_to_query:
            scoped = (self._month_rows(zone_rows, period) if period else zone_rows)
            if not scoped:
                label = _period_name(period) if period else "the published months"
                return self._reply(session_id, f"{name} has no published row in {label}.")
            result = aggregate_metric(scoped, metric)
            if result.value is None:
                return self._reply(session_id, f"No usable {result.metric.label.lower()} "
                                   f"is published for {name} in that period.")
            values.append((period, result))
        label = _METRIC_WORDING[metric]
        if len(values) == 2:
            baseline, current = values
            comparison = compare_values(baseline[1], current[1],
                                        baseline_period=baseline[0],
                                        current_period=current[0])
            answer = (f"{name} {label}: {_period_name(baseline[0])}: "
                      f"{_amount(baseline[1].value, baseline[1].metric.unit)}; "
                      f"{_period_name(current[0])}: "
                      f"{_amount(current[1].value, current[1].metric.unit)}. "
                      f"Change: {_amount(abs(comparison.difference), baseline[1].metric.unit)} "
                      f"{'more' if comparison.difference >= 0 else 'less'}.")
            if comparison.percentage_change is not None:
                answer += f" Percentage change: {_pct(comparison.percentage_change)}."
            else:
                answer += " Percentage change is unavailable for this baseline."
        else:
            period, result = values[0]
            period_label = _period_name(period) if period else "the published months"
            answer = (f"{name} recorded {_amount(result.value, result.metric.unit)} "
                      f"in {period_label} ({label}).")
        return self._reply(
            session_id, answer,
            calculation="Filter pickup_zone_performance to the exact saved "
            "pickup_location_id, then aggregate the requested published metric "
            "within the named source month(s).",
            evidence=[self._evidence("pickup_zone_performance", metric, item.value,
                                     (period,) if period else ())
                      for period, item in values],
            context=ConversationContext(
                metric=metric,
                periods=tuple(f"{period[0]}-{period[1]:02d}"
                              for period in selected),
                zones=context.zones,
                borough=zone_borough,
                comparison_group=tuple(_period_name(period) for period in selected)
                if len(selected) == 2 else (),
            ),
        )

    def _daily_analysis(self, question: str, periods: tuple[tuple[int, int], ...],
                        session_id: str, mode: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", _metric(question), 200),
                       PlanStep("anomaly" if mode == "anomaly" else "compare",
                                _metric(question), 200)])
        metric = _metric(question)
        if metric not in {"trip_count", "total_amount", "tip_amount"}:
            return self._reply(session_id, "Daily pattern analysis supports trips, TLC total "
                               "amount, or recorded tips, not that measure.")
        rows = self.source.fetch("daily_trip_metrics")
        if periods:
            rows = [row for row in rows if (int(row["_source_year"]),
                                             int(row["_source_month"])) in periods]
        if mode == "weekday":
            pattern = weekday_weekend(rows, metric)
            if pattern.weekend_minus_weekday is None:
                return self._reply(session_id, "There are too few observed weekdays or weekends "
                                   "to make a reliable comparison.",
                                   calculation="At least two observed days are required "
                                   "in each group.")
            answer = (f"Observed weekday daily average: "
                      f"{_daily_amount(pattern.weekday_daily_average, pattern.metric.unit)} "
                      f"over {pattern.weekday_days} days; weekend daily average: "
                      f"{_daily_amount(pattern.weekend_daily_average, pattern.metric.unit)} over "
                      f"{pattern.weekend_days} days. Weekends were "
                      f"{_daily_amount(abs(pattern.weekend_minus_weekday), pattern.metric.unit)} "
                      f"{'higher' if pattern.weekend_minus_weekday > 0 else 'lower'} "
                      f"({_pct(pattern.weekend_vs_weekday_pct)} vs weekdays).")
            return self._reply(session_id, answer,
                               calculation="Aggregate valid in-source-month pickup dates by day, "
                               "then compare the mean daily value for Mon–Fri with Sat–Sun. "
                               + " ".join(pattern.caveats),
                               evidence=[self._evidence("daily_trip_metrics", metric,
                                                        pattern.weekday_daily_average, periods),
                                         self._evidence("daily_trip_metrics", metric,
                                                        pattern.weekend_daily_average, periods)],
                               context=ConversationContext(
                                   metric=metric,
                                   periods=tuple(f"{year}-{month:02d}"
                                                 for year, month in periods),
                                   comparison_group=("weekday", "weekend"),
                               ))
        result = detect_anomalies(rows, metric)
        low = bool(re.search(r"\b(low|lower|drop|fewest)\b", question, re.I))
        findings = [item for item in result.anomalies if not low or item.difference < 0]
        if not findings:
            return self._reply(session_id,
                               "No qualifying unusually " + ("low" if low else "high or low")
                               + " days were detected in the selected published period.",
                               calculation="Same-weekday peer median; a candidate must differ "
                               "by at least 25% and more than three scaled median absolute "
                               "deviations. " + " ".join(result.caveats))
        parts = [f"{item.pickup_date.isoformat()}: {_amount(item.observed, item.metric.unit)} "
                 f"vs {_amount(item.baseline, item.metric.unit)} peer median "
                 f"({_pct(item.percentage_change)})"
                 for item in findings[:5]]
        shown = findings[:5]
        shown_months = tuple(dict.fromkeys(
            f"{item.pickup_date.year}-{item.pickup_date.month:02d}" for item in shown
        ))
        return self._reply(session_id, "Unusual observed days: " + "; ".join(parts)
                           + ". This identifies a pattern, not its cause.",
                           calculation="Compare each valid pickup date with other observed dates "
                           "on the same weekday; require at least four peers, >=25% relative "
                           "difference, and >3 scaled median absolute deviations. "
                           + " ".join(result.caveats),
                           evidence=[self._evidence("daily_trip_metrics", metric,
                                                    item.observed, periods)
                                     for item in shown],
                           context=ConversationContext(
                               metric=metric,
                               periods=shown_months,
                               dates=tuple(item.pickup_date.isoformat() for item in shown),
                               comparison_group=tuple(item.pickup_date.isoformat()
                                                      for item in shown),
                           ))

    def _monthly_analysis(self, question: str, payload: dict[str, Any],
                          periods: tuple[tuple[int, int], ...], session_id: str) -> dict[str, Any]:
        metric = _metric(question)
        bounded_steps([PlanStep("query", metric, 200), PlanStep("aggregate", metric, 200)])
        borough = (self._zone_scope(question, payload) if _question_boroughs(question)
                   else _borough(payload.get("borough")))
        if borough and metric in {"average_fare_amount", "estimated_fare_amount",
                                 "average_trip_distance"}:
            return self._reply(session_id, "That fare or distance measure is not available "
                               "at borough grain in the published marts; choose all "
                               "boroughs for that metric.")
        mart = "pickup_zone_performance" if borough else "daily_trip_metrics"
        rows = self.source.fetch(mart, borough=borough)
        available = sorted({(int(row["_source_year"]), int(row["_source_month"])) for row in rows})
        selected = periods or tuple(available)
        points = []
        for period in selected:
            subset = self._month_rows(rows, period)
            value = aggregate_metric(subset, metric)
            if value.value is None:
                return self._reply(session_id, f"No usable {value.metric.label.lower()} rows "
                                   f"are published for {_period_name(period)} in that scope.")
            points.append((period, value))
        if len(points) == 2 and _COMPARISON.search(question):
            first, second = (points if re.search(r"\bfrom\b.*\bto\b", question, re.I)
                             else sorted(points, key=lambda item: item[0]))
            if mart == "daily_trip_metrics":
                comparison = compare_periods(
                    self._month_rows(rows, first[0]), self._month_rows(rows, second[0]),
                    metric, baseline_period=first[0], current_period=second[0],
                )
            else:
                # Zone mart does not prove per-day completeness at its grain.
                from chatbot.analytics_ops import compare_values
                comparison = compare_values(first[1], second[1], baseline_period=first[0],
                                            current_period=second[0])
            answer = (f"{value_label(first[1])} in {_period_name(first[0])}: "
                      f"{_amount(first[1].value, first[1].metric.unit)}; "
                      f"{_period_name(second[0])}: "
                      f"{_amount(second[1].value, second[1].metric.unit)}. "
                      f"Change: {_amount(abs(comparison.difference), first[1].metric.unit)} "
                      f"{'more' if comparison.difference >= 0 else 'less'}.")
            if comparison.percentage_change is None:
                answer += " Percentage change is withheld because the compared period "
                answer += "is incomplete or its baseline is zero."
            else:
                answer += (f" Percentage change: {_pct(comparison.percentage_change)} "
                           f"vs {_period_name(first[0])}.")
            return self._reply(session_id, answer,
                               calculation="Sum published source-month mart rows (or trip-weight "
                               "the rounded daily fare averages), subtract baseline from current, "
                               "and divide by the baseline when valid. "
                               + " ".join(comparison.caveats),
                               evidence=[self._evidence(mart, metric, item.value, (period,))
                                         for period, item in points],
                               context=ConversationContext(
                                   metric=metric,
                                   periods=(f"{first[0][0]}-{first[0][1]:02d}",
                                            f"{second[0][0]}-{second[0][1]:02d}"),
                                   borough=borough,
                                   comparison_group=(_period_name(first[0]),
                                                     _period_name(second[0])),
                               ))
        if metric in {"trip_count", "total_amount", "tip_amount"} and not borough:
            trend = monthly_trend(rows, metric)
            trend_by_period = {point.period: point for point in trend}
            details = [f"{_period_name(period)}: {_amount(value.value, value.metric.unit)}"
                       + (f" ({_pct(trend_by_period[period].change_from_previous_pct)} MoM)"
                          if period in trend_by_period
                          and trend_by_period[period].change_from_previous_pct is not None else "")
                       for period, value in points]
            caveats = tuple(dict.fromkeys(note for point in trend for note in point.caveats))
            method = ("Sum source-month rows; month-over-month percent uses the "
                      "immediately preceding complete month. ")
        else:
            details = [f"{_period_name(period)}: {_amount(value.value, value.metric.unit)}"
                       for period, value in points]
            caveats = tuple(dict.fromkeys(note for _, value in points for note in value.caveats))
            method = ("Aggregate the approved source-month mart rows using the "
                      "catalogued metric definition. ")
        if len(details) > 12:
            return self._reply(session_id, "This report has more than 12 published months; "
                               "please narrow the month range.")
        return self._reply(session_id, f"Monthly {points[0][1].metric.label.lower()}: "
                           + "; ".join(details) + ".",
                           calculation=method + " ".join(caveats),
                           evidence=[self._evidence(mart, metric, value.value, (period,))
                                     for period, value in points],
                           context=ConversationContext(
                               metric=metric,
                               periods=tuple(f"{year}-{month:02d}"
                                             for (year, month), _ in points),
                               borough=borough,
                           ))

    def _dynamic_sql(self, question: str, session_id: str) -> dict[str, Any] | None:
        """Use a local model for *proposals* and a separate validated query executor.

        Explicit date scopes are left to deterministic paths until a planner can
        prove that its proposed predicates exactly match the requested scope.
        """
        if self.sql_executor is None or _question_periods(question):
            return None
        proposed = propose_analytic_sql(question)
        if proposed is None:
            return self._reply(session_id, "The local model could not form a supported "
                               "analytics query. Try naming a metric and published grouping.")
        from chatbot.sql_guard import UnsafeQuery, validate_analytic_sql
        try:
            approved = validate_analytic_sql(proposed, max_rows=100)
        except UnsafeQuery:
            return self._reply(session_id, "The proposed query was rejected by the "
                               "read-only analytics validator. Try a simpler grouping "
                               "or filter over the published marts.")
        # The optional planner currently receives only questions without an
        # explicit source period. Its SQL must not invent a filter (even one
        # that is syntactically safe) or silently change the requested metric.
        if " WHERE _source_taxi_type = %s AND " in approved.sql:
            return self._reply(session_id, "The local model proposed a filter that "
                               "was not established by your question. No query ran; "
                               "please name the grouping and metric more explicitly.")
        metric = _metric(question)
        measures = {"trip_count": "SUM(trip_count)",
                    "total_amount": "SUM(total_revenue)",
                    "tip_amount": "SUM(total_tip_amount)"}
        expected = measures.get(metric)
        actual_measures = re.findall(r"\b(?:SUM|AVG|MIN|MAX)\([a-z_]+\)", approved.sql)
        if expected is None or actual_measures != [expected]:
            return self._reply(session_id, "The proposed query did not match the "
                               "requested published metric. No query ran; try a "
                               "simpler trip, TLC total amount, or tip grouping.")
        dimensions = {"payment": "payment_type", "borough": "borough",
                      "zone": "zone", "hour": "pickup_hour",
                      "date": "pickup_date", "day": "pickup_date",
                      "month": "_source_month"}
        requested_dimensions = {column for cue, column in dimensions.items()
                                if re.search(rf"\b{cue}s?\b", question, re.I)}
        if not requested_dimensions:
            return self._reply(session_id, "Name a published grouping such as payment "
                               "type, pickup borough, zone, hour, date, or month. "
                               "No query ran.")
        grouping = (approved.sql.split(" GROUP BY ", 1)[1].split(" ORDER BY ", 1)[0]
                    .split(" LIMIT ", 1)[0].strip()
                    if " GROUP BY " in approved.sql else "")
        if set(grouping.split(", ")) != requested_dimensions:
            return self._reply(session_id, "The proposed grouping did not match "
                               "your question. No query ran; name one published "
                               "dimension such as payment type or pickup hour.")
        for cue, column in dimensions.items():
            if re.search(rf"\b{cue}s?\b", question, re.I) and column not in approved.columns:
                return self._reply(session_id, "The proposed grouping did not match "
                                   "your question. No query ran; name one published "
                                   "dimension such as payment type or pickup hour.")
        from chatbot.sql_execution import SqlExecutionError
        try:
            result = self.sql_executor.execute(approved)
        except SqlExecutionError as exc:
            raise SourceError(str(exc)) from exc
        rows = result.rows
        if not rows:
            return self._reply(session_id, "The validated query returned no published rows.",
                               calculation=f"Approved mart: analytics.{approved.mart}; "
                               f"validated SQL: {approved.sql}; bound values: "
                               f"{approved.params}.", ai_used=True)
        displayed = rows[:10]
        parts = [", ".join(f"{key}: {value}" for key, value in row.items())
                 for row in displayed]
        answer = (f"The approved analytics mart returned {len(rows)} row(s): "
                  + "; ".join(parts) + ".")
        if len(rows) > 10:
            answer += " Showing the first 10 rows; ask for a narrower grouping."
        return self._reply(session_id, answer,
                           calculation=f"Executed only the validator-recompiled SELECT on "
                           f"analytics.{approved.mart} with bound values {approved.params}, "
                           f"{approved.timeout_ms} ms statement timeout, and a "
                           f"{approved.limit}-row cap. Execution: {result.elapsed_ms} ms. "
                           f"SQL: {approved.sql}",
                           evidence=[{"metric": key, "value": str(value), "unit": "",
                                      "source": self.source.label(approved.mart),
                                      "filters": {"taxi_type": "yellow"}}
                                     for row in displayed for key, value in row.items()],
                           ai_used=True)

    def _payment_share_change(self, question: str,
                              periods: tuple[tuple[int, int], ...],
                              session_id: str) -> dict[str, Any]:
        if len(periods) != 2:
            return self._reply(session_id, "Name two published months to compare "
                               "a payment-method share.")
        requested = _question_payments(question)
        if len(requested) != 1:
            return self._reply(session_id, "Name one payment method, such as credit card "
                               "or cash, for this share comparison.")
        bounded_steps([PlanStep("query", "trip_count", 100),
                       PlanStep("query", "trip_count", 100),
                       PlanStep("compare", "trip_count", 100)])
        first, second = (periods if re.search(r"\bfrom\b.*\bto\b", question, re.I)
                         else sorted(periods))
        rows = self.source.fetch("payment_type_summary")
        values = []
        for period in (first, second):
            selected = self._month_rows(rows, period)
            total = sum(Decimal(str(row["trip_count"])) for row in selected)
            part = sum((Decimal(str(row["trip_count"])) for row in selected
                        if int(row["payment_type"]) == requested[0]), Decimal(0))
            values.append((part, total))
        comparison = compare_shares(*values[0], *values[1])
        if comparison.percentage_point_difference is None:
            return self._reply(session_id, "The payment share cannot be compared because "
                               "one month has no usable published trips.")
        label = PAYMENTS.get(requested[0], f"payment code {requested[0]}")
        answer = (f"{label.capitalize()} trip share: {_period_name(first)} "
                  f"{comparison.baseline_share_pct:.1f}% "
                  f"({values[0][0]:,.0f}/{values[0][1]:,.0f} trips); "
                  f"{_period_name(second)} {comparison.current_share_pct:.1f}% "
                  f"({values[1][0]:,.0f}/{values[1][1]:,.0f}). "
                  f"Change: {comparison.percentage_point_difference:+.1f} percentage points.")
        return self._reply(session_id, answer,
                           calculation="For each source month, divide selected payment "
                           "trip_count by all payment trip_count; subtract the two shares "
                           "in percentage points, not relative percent.",
                           evidence=[self._evidence("payment_type_summary", "trip_count",
                                                    part, (period,))
                                     for (part, _), period in zip(
                                         values, (first, second), strict=True
                                     )],
                           context=ConversationContext("trip_count",
                                                       (f"{first[0]}-{first[1]:02d}",
                                                        f"{second[0]}-{second[1]:02d}"),
                                                       payment_type=label))

    def _steepest_decline(self, session_id: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("aggregate", "trip_count", 200),
                       PlanStep("compare", "trip_count", 200)])
        rows = self.source.fetch("daily_trip_metrics")
        periods = sorted({(int(row["_source_year"]), int(row["_source_month"]))
                          for row in rows})
        totals = {period: aggregate_metric(self._month_rows(rows, period), "trip_count").value
                  for period in periods}
        changes = []
        for prior, current in zip(periods, periods[1:], strict=False):
            if (current[0] * 12 + current[1]) - (prior[0] * 12 + prior[1]) != 1:
                continue
            before, after = totals[prior], totals[current]
            if before is not None and after is not None and after < before:
                changes.append((after - before, prior, current, before, after))
        if not changes:
            return self._reply(session_id, "No decline between consecutive published "
                               "source months can be verified.")
        difference, prior, current, before, after = min(changes)
        return self._reply(session_id,
                           f"{_period_name(current)} had the steepest observed "
                           f"consecutive-month trip decline: {abs(difference):,.0f} fewer "
                           f"trips than {_period_name(prior)} "
                           f"({before:,.0f} → {after:,.0f}).",
                           calculation="Sum source-month trip_count, compare only adjacent "
                           "published months, and select the largest negative absolute "
                           "difference. This is an observed change, not an anomaly or cause.",
                           evidence=[self._evidence("daily_trip_metrics", "trip_count",
                                                    before, (prior,)),
                                     self._evidence("daily_trip_metrics", "trip_count",
                                                    after, (current,))],
                           context=ConversationContext(
                               metric="trip_count",
                               periods=(f"{prior[0]}-{prior[1]:02d}",
                                        f"{current[0]}-{current[1]:02d}"),
                               comparison_group=(_period_name(prior),
                                                 _period_name(current)),
                           ))

    def _date_rank(self, question: str, periods: tuple[tuple[int, int], ...],
                   session_id: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("aggregate", "trip_count", 200)])
        rows = self.source.fetch("daily_trip_metrics")
        daily: dict[date, Decimal] = defaultdict(Decimal)
        for row in rows:
            day = date.fromisoformat(str(row["pickup_date"]))
            source_period = int(row["_source_year"]), int(row["_source_month"])
            if (day.year, day.month) != source_period or (periods and source_period not in periods):
                continue
            daily[day] += Decimal(str(row["trip_count"]))
        if not daily:
            return self._reply(session_id, "No valid pickup dates are published "
                               "in that period.")
        lowest = bool(re.search(r"\b(fewest|lowest|least|minimum)\b", question, re.I))
        day, total = sorted(daily.items(), key=lambda item: (
            item[1] if lowest else -item[1], item[0]))[0]
        label = f"{MONTHS[day.month - 1]} {day.day}, {day.year}"
        return self._reply(session_id,
                           f"{label} had the {'fewest' if lowest else 'most'} observed "
                           f"pickup trips: {_amount(total, 'trips')}.",
                           calculation="Sum trip_count by valid pickup_date; exclude dates "
                           "outside their published source month before ranking.",
                           evidence=[self._evidence("daily_trip_metrics", "trip_count",
                                                    total, ((day.year, day.month),))],
                           context=ConversationContext(
                               metric="trip_count",
                               periods=(f"{day.year}-{day.month:02d}",),
                               dates=(day.isoformat(),),
                           ))

    def _date_range_analysis(self, question: str, session_id: str,
                             dates: tuple[date, ...]) -> dict[str, Any]:
        """Use only observed pickup dates inside one or two explicit ISO ranges."""
        if len(dates) not in {2, 4}:
            return self._reply(session_id, "Use two ISO dates (YYYY-MM-DD) for one "
                               "inclusive date range, or four dates to compare two ranges.")
        ranges = [(dates[index], dates[index + 1]) for index in range(0, len(dates), 2)]
        if any(end < start or end - start > timedelta(days=366) for start, end in ranges):
            return self._reply(session_id, "Each date range must run forward and span "
                               "at most 367 calendar days.")
        metric = _metric(question)
        bounded_steps([PlanStep("query", metric, 200),
                       PlanStep("aggregate", metric, 200)] +
                      ([PlanStep("compare", metric, 200)] if len(ranges) == 2 else []))
        rows = self.source.fetch("daily_trip_metrics")
        values = []
        for start, end in ranges:
            selected = [row for row in rows
                        if start <= date.fromisoformat(str(row["pickup_date"])) <= end
                        and (date.fromisoformat(str(row["pickup_date"])).year,
                             date.fromisoformat(str(row["pickup_date"])).month)
                        == (int(row["_source_year"]), int(row["_source_month"]))]
            if not selected:
                return self._reply(session_id, f"No published daily rows cover "
                                   f"{start.isoformat()} to {end.isoformat()}.")
            result = aggregate_metric(selected, metric)
            days = {str(row["pickup_date"]) for row in selected}
            expected_days = (end - start).days + 1
            values.append((start, end, result, len(days), expected_days))
        descriptions = [f"{start.isoformat()} to {end.isoformat()}: "
                        f"{_amount(result.value, result.metric.unit)} "
                        f"({observed}/{total} calendar days observed)"
                        for start, end, result, observed, total in values]
        if len(values) == 1:
            answer = f"Observed {values[0][2].metric.label.lower()} for " + descriptions[0]
        else:
            first, second = values
            if first[2].value is None or second[2].value is None:
                return self._reply(session_id, "The requested comparison has no usable "
                                   "published metric value in one range.")
            difference = second[2].value - first[2].value
            answer = "; ".join(descriptions)
            answer += f". Change: {_amount(abs(difference), first[2].metric.unit)} "
            answer += "more." if difference >= 0 else "less."
            if first[3] == first[4] and second[3] == second[4] and first[2].value:
                answer += f" Relative change: {_pct(difference * 100 / first[2].value)}."
            else:
                answer += " Percentage change is withheld because a range is incomplete "
                answer += "or the baseline is zero."
        return self._reply(
            session_id, answer,
            calculation="Filter valid pickup_date rows to the requested inclusive ISO "
            "range, aggregate the catalogued metric, and count distinct observed days. "
            "Missing dates are not silently treated as zero trips.",
            evidence=[self._evidence("daily_trip_metrics", metric, item[2].value)
                      for item in values],
            context=ConversationContext(
                metric=metric,
                periods=tuple(dict.fromkeys(
                    f"{day.year}-{day.month:02d}"
                    for start, end in ranges for day in (start, end)
                )),
                date_ranges=tuple((start.isoformat(), end.isoformat())
                                  for start, end in ranges),
                comparison_group=tuple(f"{start.isoformat()} to {end.isoformat()}"
                                       for start, end in ranges)
                if len(ranges) == 2 else (),
            ),
        )

    def _month_leader_tips(self, session_id: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", "trip_count", 200),
                       PlanStep("aggregate", "trip_count", 200),
                       PlanStep("aggregate", "tip_amount", 200)])
        rows = self.source.fetch("daily_trip_metrics")
        periods = sorted({(int(row["_source_year"]), int(row["_source_month"]))
                          for row in rows})
        totals = [(period,
                   aggregate_metric(self._month_rows(rows, period), "trip_count").value,
                   aggregate_metric(self._month_rows(rows, period), "tip_amount").value)
                  for period in periods]
        selected = [item for item in totals if item[1] is not None and item[2] is not None]
        if not selected:
            return self._reply(session_id, "No usable monthly trips and tips are published.")
        period, trips, tips = max(selected, key=lambda item: (item[1], -item[0][0], -item[0][1]))
        return self._reply(session_id,
                           f"{_period_name(period)} led with {_amount(trips, 'trips')} "
                           f"and recorded {_amount(tips, 'USD')} in tips.",
                           calculation="First rank published source months by summed trip_count; "
                           "then sum total_tip_amount for the winning month. Two deterministic "
                           "aggregations from the daily mart.",
                           evidence=[self._evidence("daily_trip_metrics", "trip_count",
                                                    trips, (period,)),
                                     self._evidence("daily_trip_metrics", "tip_amount",
                                                    tips, (period,))],
                           context=ConversationContext("trip_count",
                                                       (f"{period[0]}-{period[1]:02d}",)))

    def _hour_share(self, session_id: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", "trip_count", 100),
                       PlanStep("aggregate", "trip_count", 100),
                       PlanStep("compare", "trip_count", 100)])
        rows = self.source.fetch("hourly_demand")
        by_hour: dict[int, Decimal] = defaultdict(Decimal)
        for row in rows:
            by_hour[int(row["pickup_hour"])] += Decimal(str(row["trip_count"]))
        total = sum(by_hour.values(), Decimal(0))
        if not by_hour or total <= 0:
            return self._reply(session_id, "No usable published pickup-hour trips are available.")
        hour, count = sorted(by_hour.items(), key=lambda item: (-item[1], item[0]))[0]
        label = f"{hour % 12 or 12} {'AM' if hour < 12 else 'PM'}"
        share = count * 100 / total
        return self._reply(session_id,
                           f"{label} was the busiest pickup hour: {_amount(count, 'trips')}, "
                           f"{share:.1f}% of all {_amount(total, 'trips')}.",
                           calculation="Sum trip_count by pickup_hour, select the highest "
                           "hour, and divide its trips by all published hourly trips.",
                           evidence=[self._evidence("hourly_demand", "trip_count", count),
                                     self._evidence("hourly_demand", "trip_count", total)],
                           context=ConversationContext(metric="trip_count"))

    def _borough_tip_gap(self, period: tuple[int, int],
                         session_id: str) -> dict[str, Any]:
        bounded_steps([PlanStep("query", "tip_amount", 200),
                       PlanStep("aggregate", "tip_amount", 200),
                       PlanStep("compare", "tip_amount", 200)])
        rows = self.source.fetch("pickup_zone_performance", period)
        groups: dict[str, Decimal] = defaultdict(Decimal)
        for row in rows:
            groups[str(row.get("borough") or "Unknown")] += Decimal(
                str(row["total_tip_amount"]))
        if len(groups) < 2:
            return self._reply(session_id, "At least two boroughs with published tips "
                               "are needed for that comparison.")
        ranked = sorted(groups.items(), key=lambda item: (-item[1], item[0]))
        leader, runner = ranked[:2]
        difference = leader[1] - runner[1]
        return self._reply(session_id,
                           f"{leader[0]} led recorded tips in {_period_name(period)} "
                           f"with {_amount(leader[1], 'USD')}; {runner[0]} recorded "
                           f"{_amount(runner[1], 'USD')}, so the lead was "
                           f"{_amount(difference, 'USD')}.",
                           calculation="Sum total_tip_amount by published pickup borough, "
                           "rank the two highest, then subtract runner-up from leader.",
                           evidence=[self._evidence("pickup_zone_performance", "tip_amount",
                                                    amount, (period,))
                                     for _, amount in ranked[:2]],
                           context=ConversationContext(
                               metric="tip_amount",
                               periods=(f"{period[0]}-{period[1]:02d}",),
                               boroughs=(leader[0], runner[0]),
                               comparison_group=(leader[0], runner[0]),
                           ))

    @staticmethod
    def _rewrite_followup(question: str, context: ConversationContext) -> str | None:
        """Ground elliptical questions in structured, answer-backed referents.

        A multiple-month answer deliberately has no singular month focus.  A
        named comparison month is the baseline and the focused month is the
        subject, so the answer can state an unambiguous signed change.
        """
        q = question.casefold().strip()
        named_month = _requested_month_label(question, context)
        borough_suffix = (f" in {context.borough}"
                          if context.borough and not _question_boroughs(question)
                          and not re.search(r"\b(?:citywide|all boroughs|whole city)\b", q)
                          else "")
        if (context.pending_metric and named_month and
                re.fullmatch(r"\s*(?:20\d{2}-\d{2}|[a-z]+(?:\s+20\d{2})?)\??\s*",
                             question, re.I)):
            phrase = _METRIC_WORDING.get(context.pending_metric)
            return f"What were {phrase} in {named_month}{borough_suffix}?" if phrase else None
        if "that month" in q:
            if context.month_referent is None:
                return None
            suffix = f" in {_month_label(context.month_referent)}"
            suffix += borough_suffix
            return question + suffix
        if "that borough" in q:
            if context.borough is None:
                return None
            if len(context.periods) > 1 and not _question_periods(question):
                return None
            suffix = f" in {context.borough}"
            if context.month_referent and not _question_periods(question):
                suffix += f" in {_month_label(context.month_referent)}"
            return question + suffix
        if "that period" in q or "same period" in q:
            if len(context.date_ranges) == 1:
                start, end = context.date_ranges[0]
                return question + f" from {start} to {end}"
            if context.month_referent:
                return question + f" in {_month_label(context.month_referent)}" + borough_suffix
            return None
        if (re.search(r"\b(?:that|it)\b", q) and
                re.search(r"\b(?:compare|versus|vs\.?)\b", q) and named_month):
            metric = _explicit_metric(question) or context.metric
            phrase = _METRIC_WORDING.get(metric or "")
            if context.month_referent is None or phrase is None:
                return None
            return (f"How did {phrase} change from {named_month} "
                    f"to {_month_label(context.month_referent)}{borough_suffix}?")
        if re.search(r"\b(?:what|how) about\b", q) and named_month:
            if len(context.boroughs) == 2 and context.metric == "trip_count":
                return (f"Compare {context.boroughs[0]} and {context.boroughs[1]} "
                        f"trips in {question.split('about', 1)[1].strip().rstrip('?')}.")
            phrase = _METRIC_WORDING.get(context.metric or "")
            if named_month and phrase:
                return f"What were {phrase} in {named_month}{borough_suffix}?"
            return None
        if "what share was that" in q:
            if context.payment_type and context.month_referent:
                return (f"What share of {_month_label(context.month_referent)} trips used "
                        f"{context.payment_type}?")
            return None
        if re.search(r"\band for\b", q):
            metric = _explicit_metric(question)
            phrase = _METRIC_WORDING.get(metric or "")
            if phrase is None:
                return None
            if len(context.date_ranges) == 2:
                first, second = context.date_ranges
                return (f"Compare {phrase} from {first[0]} to {first[1]} "
                        f"and from {second[0]} to {second[1]}?")
            if len(context.date_ranges) == 1:
                start, end = context.date_ranges[0]
                return f"What were {phrase} from {start} to {end}?"
            if len(context.periods) == 2 and context.comparison_group:
                return (f"How did {phrase} change from "
                        f"{_month_label(context.periods[0])} to "
                        f"{_month_label(context.periods[1])}{borough_suffix}?")
            if context.month_referent:
                return (f"What were {phrase} in "
                        f"{_month_label(context.month_referent)}{borough_suffix}?")
            return None
        return question

    def _remember_legacy(self, question: str, payload: dict[str, Any],
                         available: tuple[tuple[int, int], ...],
                         response: dict[str, Any], session_id: str,
                         prior: ConversationContext) -> None:
        """Persist typed references from validated evidence, not answer wording."""
        if not response.get("evidence"):
            self.sessions.update(session_id, ConversationContext())
            return
        periods = self._resolve_periods(question, payload, available)
        if not periods and _intent(question) == "peak_month":
            evidence_metric = {"trip_count": "trip_count",
                               "tip_amount": "total_tip_amount",
                               "total_amount": "total_revenue"}.get(_metric(question))
            ranked = []
            if evidence_metric:
                for item in response["evidence"]:
                    label = str(item.get("metric", ""))
                    suffix = " " + evidence_metric
                    if not label.endswith(suffix):
                        continue
                    named = _question_periods(label[:-len(suffix)])
                    if len(named) == 1 and named[0][0] is not None:
                        ranked.append((Decimal(str(item["value"])), named[0]))
            if ranked:
                low = bool(re.search(r"\b(fewest|least|lowest|bottom)\b", question, re.I))
                winning = (min(ranked, key=lambda item: (item[0], item[1])) if low else
                           max(ranked, key=lambda item: (item[0], -item[1][0], -item[1][1])))
                periods = (winning[1],)
        boroughs = _question_boroughs(question)
        payment = None
        winners = [item for item in response["evidence"]
                   if item.get("metric") == "top_pickup_borough"]
        winner_borough = _borough(str(winners[0]["value"])) if winners else None
        payment_items = [item for item in response["evidence"]
                         if item.get("metric") == "payment_type"]
        if payment_items:
            payment = PAYMENTS.get(int(payment_items[0]["value"]))
        elif re.search(r"\bcredit card\b", question, re.I):
            payment = "credit card"
        elif re.search(r"\bcash\b", question, re.I):
            payment = "cash"
        scope_borough = (boroughs[0] if len(boroughs) == 1 else
                         winner_borough or _borough(payload.get("borough")))
        saved_periods = tuple(f"{year}-{month:02d}" for year, month in periods)
        context = ConversationContext(
            metric=_metric(question),
            periods=saved_periods,
            zones=(),  # Legacy answers do not establish an exact zone-ID scope.
            borough=scope_borough,
            boroughs=boroughs if len(boroughs) == 2 else (),
            payment_type=payment,
            comparison_group=(tuple(_month_label(value) for value in saved_periods)
                              if len(saved_periods) == 2 and _COMPARISON.search(question)
                              else boroughs if len(boroughs) == 2 else ()),
        )
        self.sessions.update(session_id, context)

    def answer(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise InvalidRequest("JSON request must be an object")
        question = payload.get("question")
        if not isinstance(question, str) or not 1 <= len(question.strip()) <= 500:
            raise InvalidRequest("question must be 1–500 characters")
        proposed_id = payload.get("session_id")
        if proposed_id is not None and (not isinstance(proposed_id, str) or len(proposed_id) > 128):
            raise InvalidRequest("session_id must be a string of at most 128 characters")
        reset = payload.get("reset", False)
        if not isinstance(reset, bool):
            raise InvalidRequest("reset must be true or false")
        session_id, context = self.sessions.resolve(None if reset else proposed_id)
        q = question.casefold()
        if (re.search(r"\b(?:drop|truncate|alter|delete|insert|update|create)\s+"
                      r"(?:table|view|schema|database|from|into)\b", q)
                or re.search(r"\b(?:pg_catalog|information_schema|pg_user|passwords?)\b", q)
                or re.search(r"\bignore\b.{0,50}\b(?:rules|instructions)\b", q)):
            return self._reply(session_id, "I cannot run database commands, inspect "
                               "system tables, or reveal credentials. Ask a read-only "
                               "question about published taxi aggregates instead.")
        if re.search(r"\b(passengers?|drivers?)\s+(?:named|name|identity)\b", q):
            return self._reply(session_id, "Passenger and driver identities are not "
                               "available in the published aggregate marts.")
        if re.search(r"\b(routes?|drop[- ]?off|destinations?)\b", q):
            return self._reply(session_id, "Pickup-to-dropoff routes are not available "
                               "in the published aggregate marts.")
        if re.search(r"\b(?:forecast|predict|prediction)\b", q):
            return self._reply(session_id, "I cannot forecast or predict future taxi "
                               "activity from these published aggregates.")
        if _unsupported_metric(question):
            return self._reply(session_id, "That measure is not published in the "
                               "chat marts. Ask about trips, TLC total amount, "
                               "recorded tips, or a supported fare/distance aggregate.",
                               context=ConversationContext())
        if re.search(r"\b(?:why|caused?|reason)\b", q):
            return self._reply(session_id, "I cannot determine causes from these "
                               "aggregates; I can describe observed changes only.")
        if re.search(r"^\s*how many\??\s*$", q):
            return self._reply(session_id, "Which published taxi metric do you mean—"
                               "trips, TLC total amount, or recorded tips?")
        years = {int(year) for year in re.findall(r"\b(20\d{2})\b", q)}
        if years and not _question_periods(question) and any(year != 2024 for year in years):
            return self._reply(session_id, "Data for that year is not available in "
                               "this January–June 2024 report.")
        zone_followup = bool(re.search(
            r"\b(?:and for|what about|how about|that zone|that month|"
            r"that period|same period)\b", q,
        ) or (re.search(r"\b(?:that|it)\b", q) and
              re.search(r"\b(?:compare|versus|vs\.?)\b", q)) or
            (context.pending_metric and _question_periods(question)))
        if zone_followup and context.zones and not re.search(
            r"\b(?:citywide|all boroughs|whole city)\b", q,
        ):
            return self._zone_metric_followup(question, payload, session_id, context)
        if "that zone" in q and not context.zones:
            return self._reply(session_id, "Which pickup zone do you mean? Name one "
                               "published zone first.")
        if (re.search(r"\b(that month|that borough|that period|same period|"
                      r"what about|how about|what share was that|and for)\b", q)
                or (re.search(r"\b(?:that|it)\b", q) and
                    re.search(r"\b(?:compare|versus|vs\.?)\b", q))
                or (context.pending_metric and _question_periods(question))):
            rewritten = self._rewrite_followup(question, context)
            if rewritten is None:
                if context.ambiguous_months and re.search(r"\b(?:that month|that period|"
                                                          r"same period)\b", q):
                    choices = ", ".join(_month_label(value)
                                        for value in context.ambiguous_months)
                    self.sessions.update(session_id, replace(
                        context, pending_metric=_explicit_metric(question) or context.metric,
                    ))
                    return self._reply(session_id, "That month is ambiguous because the "
                                       f"previous answer involved {choices}. Which one "
                                       "do you mean?")
                return self._reply(session_id, "Please clarify the month, metric, or "
                                   "group you want to compare; no single matching "
                                   "referent is available in this chat.")
            question = rewritten
            q = question.casefold()
        try:
            available = self._available_periods()
            if not available:
                return self._reply(session_id, "No published taxi periods are available.")
            date_tokens = re.findall(r"\b20\d{2}-\d{2}-\d{2}\b", question)
            if date_tokens:
                if (_question_boroughs(question) or _borough(payload.get("borough"))
                        or _question_payments(question)
                        or re.search(r"\b(zone|neighbou?rhood|pickup area)\b", q)):
                    return self._reply(session_id, "Daily date ranges cannot be "
                                       "combined with borough, zone, or payment filters "
                                       "at the published mart grain.")
                try:
                    dates = tuple(date.fromisoformat(token) for token in date_tokens)
                except ValueError:
                    return self._reply(session_id, "Use valid ISO calendar dates "
                                       "(YYYY-MM-DD) for a date-range question.")
                return self._date_range_analysis(question, session_id, dates)
            periods = self._resolve_periods(question, payload, available)
            if (len(periods) == 2 and "share" in q and
                    len(_question_payments(question)) == 1 and _COMPARISON.search(q)):
                return self._payment_share_change(question, periods, session_id)
            if (re.search(r"\bsteepest\b", q) and
                    re.search(r"\b(decline|decrease|drop)\b", q) and
                    re.search(r"\b(month|monthly)\b", q)):
                return self._steepest_decline(session_id)
            if (re.search(r"\b(date|day)\b", q) and
                    re.search(r"\b(highest|most|fewest|lowest|least|maximum|minimum)\b", q)
                    and re.search(r"\b(trip|pickup)\b", q)):
                return self._date_rank(question, periods, session_id)
            if (re.search(r"\b(which|what) month\b", q) and
                    re.search(r"\b(led|most|highest|busiest)\b", q) and
                    re.search(r"\btrips?\b", q) and
                    re.search(r"\b(tips?|gratuities)\b", q)):
                return self._month_leader_tips(session_id)
            if (re.search(r"\bbusiest\b.*\b(hour|pickup hour)\b", q) and
                    re.search(r"\bshare\b", q)):
                return self._hour_share(session_id)
            if (len(periods) == 1 and re.search(r"\bborough\b", q) and
                    re.search(r"\b(led|highest|most)\b", q) and
                    re.search(r"\b(tips?|gratuities)\b", q) and
                    re.search(r"\b(more|gap|difference)\b", q)):
                return self._borough_tip_gap(periods[0], session_id)
            if (re.search(r"\b(they|those|them)\b", q)
                    and re.search(r"\b(zone|perform|compare|how|do|did)\b", q)):
                return self._zone_followup(question, payload, periods, session_id, context)
            if (context.zones and len(context.zones) == 1 and
                    re.search(r"\b(it|that one)\b", q) and
                    re.search(r"\b(do|did|perform|compare|change)\b", q)):
                return self._zone_followup(question, payload, periods, session_id, context)
            zone_question = bool(re.search(r"\b(zones?|neighbou?rhoods?|pickup areas?)\b", q))
            if (zone_question and len(periods) == 2 and
                    re.search(r"\b(top|leading|highest|most)\b", q) and
                    re.search(r"\b(change|compare|perform|difference)\b", q)):
                return self._top_zone_change(question, payload, periods, session_id)
            if zone_question and re.search(
                r"\b(increase|decline|decrease|drop|change|growth|fell|rose)\b", q,
            ):
                return self._zone_change(question, payload, periods, available, session_id)
            if zone_question and re.search(
                r"\b(top|leading|highest|most|bottom|lowest|fewest)\b", q,
            ):
                return self._rank_zones(question, payload, periods, session_id)
            named_zone = self._named_zone_comparison(question, payload, periods, session_id)
            if named_zone is not None:
                return named_zone
            named_zone = self._named_zone_total(question, payload, periods, session_id)
            if named_zone is not None:
                return named_zone
            if re.search(r"\b(weekday|weekend|weekdays|weekends)\b", q):
                return self._daily_analysis(question, periods, session_id, "weekday")
            if re.search(r"\b(anomal(?:y|ies)|unusual(?:ly)?|abnormal|outlier)\b", q):
                return self._daily_analysis(question, periods, session_id, "anomaly")
            if (("fare" in q or "distance" in q or "how far" in q) and
                    (_MONTH.search(q) or periods)) or (_MONTH.search(q) and
                    re.search(r"\b(trend|compare|change|growth|by month|monthly)\b", q)) or (
                        len(periods) == 2 and _COMPARISON.search(q)):
                return self._monthly_analysis(question, payload, periods, session_id)
            if (_intent(question) == "help" or re.search(
                    r"\b(over|under|at least|at most|threshold|grouped|group by)\b", q)):
                dynamic = self._dynamic_sql(question, session_id)
                if dynamic is not None:
                    return dynamic
        except AnalyticsError as exc:
            return self._reply(session_id, str(exc))
        # The original intent path is deliberately preserved for established questions.
        response = self.legacy.answer({**payload, "question": question,
                                       "session_id": session_id})
        response["session_id"] = session_id
        response.setdefault("calculation", "")
        self._remember_legacy(question, payload, available, response, session_id, context)
        return response


def value_label(value: Any) -> str:
    return value.metric.label
