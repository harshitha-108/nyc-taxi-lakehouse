"""Canonical, auditable metrics for approved taxi serving marts.

``total_revenue`` in Gold is the sum of TLC ``total_amount``. It is not a
company-accounting revenue measure and must never be silently confused with
fare amount. Gold does not publish an additive fare total; the fare measures
below are estimates derived from rounded daily average fares.

SQL expressions in this module are fixed application code, not model output.
Callers must still validate the complete SQL AST and the selected mart.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class MetricSpec:
    key: str
    label: str
    unit: str
    definition: str
    source_columns: tuple[str, ...]
    sql_by_mart: Mapping[str, str]
    additive: bool
    caveat: str | None = None

    @property
    def allowed_marts(self) -> frozenset[str]:
        return frozenset(self.sql_by_mart)

    def sql_aggregate(self, mart: str) -> str:
        """Return a trusted SQL expression for an explicitly approved mart."""
        try:
            return self.sql_by_mart[mart]
        except KeyError as exc:
            raise ValueError(f"{self.key} is not available from {mart}") from exc


@dataclass(frozen=True)
class DimensionSpec:
    key: str
    label: str
    source_columns: tuple[str, ...]
    allowed_marts: frozenset[str]


_DAILY = "daily_trip_metrics"
_ZONE = "pickup_zone_performance"
_PAYMENT = "payment_type_summary"
_HOURLY = "hourly_demand"
_BREAKDOWN = "takeaway_breakdown"


def _same(expression: str, *marts: str) -> Mapping[str, str]:
    return MappingProxyType({mart: expression for mart in marts})


_FARE_WEIGHT = (
    "SUM(CASE WHEN average_fare_amount IS NOT NULL AND trip_count > 0 "
    "THEN average_fare_amount * trip_count END)"
)
_FARE_TRIPS = (
    "SUM(CASE WHEN average_fare_amount IS NOT NULL AND trip_count > 0 "
    "THEN trip_count ELSE 0 END)"
)
_DISTANCE = (
    "SUM(CASE WHEN total_trip_distance IS NOT NULL AND trip_count > 0 "
    "THEN total_trip_distance END)"
)
_DISTANCE_TRIPS = (
    "SUM(CASE WHEN trip_count > 0 "
    "THEN trip_count ELSE 0 END)"
)

_CATALOG = {
    "trip_count": MetricSpec(
        "trip_count", "Trips", "trips", "Sum of quality-validated trip_count.",
        ("trip_count",),
        _same("SUM(trip_count)", _DAILY, _ZONE, _PAYMENT, _HOURLY, _BREAKDOWN), True,
    ),
    "total_amount": MetricSpec(
        "total_amount", "TLC total amount", "USD",
        "Sum of Gold total_revenue, sourced from TLC total_amount; not company revenue.",
        ("total_revenue",),
        _same("SUM(total_revenue)", _DAILY, _ZONE, _PAYMENT, _HOURLY), True,
    ),
    "tip_amount": MetricSpec(
        "tip_amount", "Recorded tips", "USD", "Sum of Gold total_tip_amount.",
        ("total_tip_amount",),
        _same("SUM(total_tip_amount)", _DAILY, _ZONE, _PAYMENT), True,
    ),
    "estimated_fare_amount": MetricSpec(
        "estimated_fare_amount", "Estimated fare amount", "USD",
        "Sum of Gold daily average_fare_amount multiplied by daily trip_count; "
        "not the unavailable exact fare_amount total.",
        ("average_fare_amount", "trip_count"),
        _same(_FARE_WEIGHT, _DAILY), True,
        "Approximate: daily averages are rounded, and the published mart lacks "
        "non-null fare counts for exact weighting.",
    ),
    "average_fare_amount": MetricSpec(
        "average_fare_amount", "Average fare amount", "USD/trip",
        "Trip-weighted mean of Gold daily average_fare_amount, excluding null averages.",
        ("average_fare_amount", "trip_count"),
        _same(f"({_FARE_WEIGHT}) / NULLIF(({_FARE_TRIPS}), 0)", _DAILY), False,
        "Approximate: daily averages are rounded, and the published mart lacks "
        "non-null fare counts for exact weighting.",
    ),
    "average_trip_distance": MetricSpec(
        "average_trip_distance", "Average trip distance", "miles/trip",
        "Gold total_trip_distance divided by trip_count, matching the Power BI KPI; "
        "null source distances, if any, contribute no distance but remain in trip_count.",
        ("total_trip_distance", "trip_count"),
        _same(f"({_DISTANCE}) / NULLIF(({_DISTANCE_TRIPS}), 0)", _DAILY), False,
        "Computed as published total distance divided by all validated trips; "
        "null source distances may lower this ratio.",
    ),
}

METRICS: Mapping[str, MetricSpec] = MappingProxyType(_CATALOG)

DIMENSIONS: Mapping[str, DimensionSpec] = MappingProxyType({
    "source_month": DimensionSpec(
        "source_month", "Source month", ("_source_year", "_source_month"),
        frozenset({_DAILY, _ZONE, _PAYMENT, _HOURLY, _BREAKDOWN}),
    ),
    "pickup_date": DimensionSpec(
        "pickup_date", "Pickup date", ("pickup_date",), frozenset({_DAILY, _HOURLY}),
    ),
    "pickup_zone": DimensionSpec(
        "pickup_zone", "Pickup zone", ("borough", "zone"), frozenset({_ZONE}),
    ),
    "pickup_borough": DimensionSpec(
        "pickup_borough", "Pickup borough", ("borough",),
        frozenset({_ZONE, _BREAKDOWN}),
    ),
    "payment_type": DimensionSpec(
        "payment_type", "Payment type", ("payment_type",),
        frozenset({_PAYMENT, _BREAKDOWN}),
    ),
    "pickup_hour": DimensionSpec(
        "pickup_hour", "Pickup hour", ("pickup_hour",),
        frozenset({_HOURLY, _BREAKDOWN}),
    ),
})

ALIASES: Mapping[str, str] = MappingProxyType({
    "total_revenue": "total_amount",  # Legacy Gold/Power BI field name.
    "revenue": "total_amount",  # Answer must explicitly label TLC total_amount.
    "total_tip_amount": "tip_amount",
    "total_fare": "estimated_fare_amount",
    "average_fare": "average_fare_amount",
    "avg_fare": "average_fare_amount",
    "average_distance": "average_trip_distance",
    "avg_distance": "average_trip_distance",
})


def metric_spec(key: str) -> MetricSpec:
    """Resolve only catalogued metric IDs; unknown/model-invented metrics fail closed."""
    if not isinstance(key, str):
        raise ValueError("Metric ID must be text")
    canonical = ALIASES.get(key, key)
    try:
        return METRICS[canonical]
    except KeyError as exc:
        raise ValueError(f"Unsupported metric: {key}") from exc


def dimension_spec(key: str) -> DimensionSpec:
    """Resolve only approved group-by dimensions and their serving grains."""
    if not isinstance(key, str):
        raise ValueError("Dimension ID must be text")
    try:
        return DIMENSIONS[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported dimension: {key}") from exc
