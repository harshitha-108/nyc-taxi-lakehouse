"""Idempotently create the local Superset database, datasets, charts and dashboard via REST API."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from urllib.parse import quote_plus

import requests

LOGGER = logging.getLogger(__name__)
TITLE = "NYC Urban Mobility Overview"
DATABASE_NAME = "NYC Taxi Analytics"
THEME_NAME = "THEME_DEFAULT"
DAILY_DISPLAY_RANGE = "2024-01-01 : 2024-07-01"
# Official TLC Yellow Taxi data dictionary: payment_type codes 0–6.
# Keep the translation in this chart, not in the serving mart.
PAYMENT_TYPE_LABEL_SQL = (
    "CASE payment_type "
    "WHEN 0 THEN 'Flex Fare trip' "
    "WHEN 1 THEN 'Credit card' "
    "WHEN 2 THEN 'Cash' "
    "WHEN 3 THEN 'No charge' "
    "WHEN 4 THEN 'Dispute' "
    "WHEN 5 THEN 'Unknown' "
    "WHEN 6 THEN 'Voided trip' "
    "ELSE 'Code ' || CAST(payment_type AS TEXT) END"
)
SERIES_COLORS = {
    "Trips": "#1e4765",
    "Total Amount": "#0e7c86",
    "Hourly Trips": "#0e7c86",
    "Borough Pickups": "#2e8062",
    "Zone Pickups": "#1e4765",
    "Payment Trips": "#5b6c82",
}
MART_NAMES = (
    "daily_trip_metrics", "hourly_demand", "pickup_location_performance",
    "payment_type_summary", "pickup_zone_performance", "takeaway_breakdown",
)
SUPPORTED_VIZ_TYPES = frozenset({
    "big_number_total", "echarts_timeseries_line", "echarts_timeseries_bar", "pie", "table",
})
HEADER_MARKDOWN = (
    "# 🗽 NYC Urban Mobility\n"
    "Yellow Taxi Performance & Demand Analytics"
)
PUBLISHED_OVERVIEW_MARKDOWN = (
    "# NYC Urban Mobility\n"
    "Yellow Taxi Performance & Demand Analytics\n\n"
    "January–June 2024 | NYC Yellow Taxi Trips"
)
TAKEAWAYS_MARKDOWN = (
    "### Key Takeaways\n"
    "January–June 2024, all boroughs (unfiltered snapshot):\n\n"
    "- **20.0M** quality-validated Yellow Taxi trips.\n"
    "- **6 PM** was the busiest pickup hour across the six months.\n"
    "- **Manhattan** accounted for most pickup trips.\n"
    "- **Credit card** was the most common payment method."
)
LEGACY_OVERVIEW_MARKDOWN = (HEADER_MARKDOWN + "\n\n" + TAKEAWAYS_MARKDOWN).replace(
    "Yellow Taxi Performance & Demand Analytics",
    "Yellow Taxi Performance & Demand Analytics | January–June 2024",
)
OVERVIEW_MARKDOWN = (HEADER_MARKDOWN + "\n\n### Key Takeaways\n"
                     "These insights update with Source Month and Pickup Borough filters.")
TAXI_ICON_OVERVIEW_MARKDOWN = LEGACY_OVERVIEW_MARKDOWN.replace("# 🗽 ", "# 🚕 ", 1)
PREVIOUS_OVERVIEW_MARKDOWN = (HEADER_MARKDOWN + "\n\n### Key Takeaways\n"
                              "The four figures below respond to Source Month and Pickup "
                              "Borough filters.")
OVERVIEW_HEIGHT = 18
INSIGHT_NAMES = (
    "Insight Busiest Hour", "Insight Top Borough", "Insight Top Payment",
)
DASHBOARD_CSS = """
.dashboard-content { background: #f7f9fc !important; }
.dashboard-component-chart-holder:has(> h1) {
  padding: 10px 18px;
  border-top: 3px solid #1e4765;
  background: linear-gradient(180deg, #f5f9ff, #fff 75%);
}
.dashboard-component-chart-holder > h1 {
  margin: 0 0 2px; font-size: 26px; font-weight: 700; letter-spacing: -0.025em;
  color: #14243a;
}
.dashboard-component-chart-holder > p { margin: 0 0 3px; color: #52657d; font-size: 13px; }
.dashboard-component-chart-holder {
  background: #fff; border: 1px solid #e4eaf2; border-radius: 9px;
  box-shadow: 0 2px 8px rgba(20, 36, 58, 0.035);
}
.dashboard-component-chart-holder .header-title { color: #24364d; font-weight: 600; }
.dashboard-component-chart-holder:has(.chart-slice):not(:has(.big_number_total)) {
  border-top: 2px solid var(--chart-accent, #dce5f0);
  background: #fff;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Daily Trips"]) {
  --chart-accent: #1e4765; border-top-width: 3px;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Daily Total Amount"]) {
  --chart-accent: #0e7c86; --chart-tint: #edf8f8;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Pickup Trips by Borough"]) {
  --chart-accent: #2e8062; --chart-tint: #f0f8f3;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Hourly Pickup Demand"]) {
  --chart-accent: #0e7c86; --chart-tint: #edf8f8;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Top Pickup Zones"]) {
  --chart-accent: #2e8062; --chart-tint: #f0f8f3;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Payment Type Trips"]) {
  --chart-accent: #5b6c82; --chart-tint: #f3f5f8;
}
.dashboard-component-chart-holder:has(.big_number_total) {
  border-top: 3px solid var(--kpi-accent, #1e4765);
  background: linear-gradient(135deg, var(--kpi-tint, #f3f7fa), #fff 90%);
}
.dashboard-component-chart-holder:has([data-test-chart-name="Total Valid Trips"]) {
  --kpi-accent: #1e4765; --kpi-tint: #f2f6f9;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Total Amount"]) {
  --kpi-accent: #0e7c86; --kpi-tint: #edf8f8;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Average Trip Distance"]) {
  --kpi-accent: #ae8234; --kpi-tint: #fff8eb;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Average Trip Duration"]) {
  --kpi-accent: #bd625c; --kpi-tint: #fff1f0;
}
.dashboard-component-chart-holder .big_number_total .header-line {
  color: var(--kpi-accent, #1e4765) !important;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.dashboard-component-chart-holder:has(.big_number_total) .header-title {
  display: flex; align-items: center; gap: 9px;
}
.dashboard-component-chart-holder:has(.big_number_total) .header-title::before {
  display: inline-grid; place-items: center; flex: 0 0 28px;
  width: 28px; height: 28px; border-radius: 8px;
  background: var(--kpi-tint, #f3f7fa); color: var(--kpi-accent, #1e4765);
  font-size: 20px; line-height: 1;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Total Valid Trips"])
  .header-title::before { content: "👥"; }
.dashboard-component-chart-holder:has([data-test-chart-name="Total Amount"])
  .header-title::before { content: "＄"; font-weight: 700; }
.dashboard-component-chart-holder:has([data-test-chart-name="Average Trip Distance"])
  .header-title::before { content: "📍"; }
.dashboard-component-chart-holder:has([data-test-chart-name="Average Trip Duration"])
  .header-title::before { content: "◷"; font-weight: 700; }
.dashboard-component-chart-holder:has([data-test-chart-name^="Insight "]) {
  border-top: 4px solid var(--insight-accent, #1e4765) !important;
  background: linear-gradient(180deg, var(--insight-tint, #f3f7fa), #fff 75%);
}
.dashboard-component-chart-holder:has([data-test-chart-name="Insight Busiest Hour"]) {
  --insight-accent: #0e7c86; --insight-tint: #edf8f8;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Insight Top Borough"]) {
  --insight-accent: #2e8062; --insight-tint: #f0f8f3;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Insight Top Payment"]) {
  --insight-accent: #ae8234; --insight-tint: #fff8eb;
}
.grid-row:has([data-test-chart-name="Insight Busiest Hour"]) {
  margin-top: -1px !important;
  padding: 0 12px 14px;
  background: #fff;
  border: 1px solid #e4eaf2;
  border-top: 0;
  border-radius: 0 0 9px 9px;
}
.grid-row:has(#MARKDOWN-INTRO) {
  margin-bottom: 0 !important;
  padding-bottom: 0 !important;
}
.grid-row:has(#MARKDOWN-INTRO) .resizable-container,
.grid-row:has(#MARKDOWN-INTRO) .dashboard-component-chart-holder {
  height: auto !important;
  min-height: 0 !important;
  max-height: none !important;
  overflow: visible !important;
}
.grid-row:has(#MARKDOWN-INTRO) .dashboard-component-chart-holder {
  border-bottom: 0;
  border-radius: 9px 9px 0 0;
  box-shadow: none;
}
[data-test-chart-name^="Insight "] .dt-controls,
[data-test-chart-name^="Insight "] .dt-pagination,
[data-test-chart-name^="Insight "] thead,
[data-test-chart-name^="Insight "] tbody td:nth-child(2),
[data-test-chart-name^="Insight "] tbody tr:not(:first-child) {
  display: none;
}
[data-test-chart-name^="Insight "] .chart-container,
[data-test-chart-name^="Insight "] .superset-chart-table > div,
[data-test-chart-name^="Insight "] .superset-chart-table > div > div,
[data-test-chart-name^="Insight "] [role="table"],
[data-test-chart-name^="Insight "] [role="presentation"] {
  width: 100% !important; overflow: visible !important; visibility: visible !important;
}
[data-test-chart-name^="Insight "] table {
  width: 100% !important; table-layout: auto !important;
}
[data-test-chart-name^="Insight "] tbody tr,
[data-test-chart-name^="Insight "] tbody td {
  background: transparent !important;
}
[data-test-chart-name^="Insight "] colgroup {
  display: none;
}
[data-test-chart-name^="Insight "] tbody td:first-child {
  width: 100%; padding: 12px 6px; border: 0;
  color: var(--insight-accent, #1e4765); font-size: 22px; font-weight: 700;
  overflow: visible; white-space: normal; overflow-wrap: anywhere;
}
.dashboard-component-chart-holder:has(> h3) {
  padding: 12px 18px;
  background: linear-gradient(135deg, #eef5ff, #f8fbff);
  border-color: #d9e6f8;
}
.dashboard-component-chart-holder > h3 { color: #203b60; margin: 0 0 8px; }
.dashboard-component-chart-holder > ul { padding-left: 20px; line-height: 1.65; }
.dashboard-component-chart-holder:has(> h1):has(> h3) {
  display: flex; flex-direction: column;
  padding: 18px 24px; border-top: 3px solid #1e4765;
  background: linear-gradient(180deg, #f5f9ff, #fff 44%);
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > h1 {
  margin: 0 0 2px;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > p:first-of-type {
  margin: 0 0 12px; padding-bottom: 12px; border-bottom: 1px solid #dbe6f2;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > h3 {
  margin: 0 0 2px; font-size: 18px; font-weight: 700;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > p:nth-of-type(2) {
  margin: 0 0 10px;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul {
  display: grid; grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px; margin: 0; padding: 0; list-style: none; line-height: 1.4;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li {
  position: relative; padding: 12px 14px 12px 48px;
  border: 1px solid #dde8f3; border-left: 3px solid #1e4765;
  border-radius: 7px; background: #f7faff; font-size: 14px; font-weight: 400;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li strong {
  font-weight: 700;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li::before {
  position: absolute; left: 13px; top: 50%; transform: translateY(-50%);
  font-size: 20px; line-height: 1;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(1)::before {
  content: "🚕";
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(2) {
  border-left-color: #0e7c86; background: #f4fbfb;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(2)::before {
  content: "🕕";
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(3) {
  border-left-color: #2e8062; background: #f5fbf7;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(3)::before {
  content: "🏙️";
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(4) {
  border-left-color: #ae8234; background: #fffbf3;
}
.dashboard-component-chart-holder:has(> h1):has(> h3) > ul > li:nth-child(4)::before {
  content: "💳";
}
@media (max-width: 1100px) {
  .resizable-container:has(h1):has(h3) {
    height: auto !important;
  }
  .dashboard-component-chart-holder:has(> h1):has(> h3) {
    height: auto !important; overflow: visible;
  }
  .dashboard-component-chart-holder:has(> h1):has(> h3) > ul {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
@media (max-width: 800px) {
  .resizable-container:has(h1):has(h3) {
    height: auto !important;
  }
  .dashboard-component-chart-holder:has(> h1):has(> h3) {
    height: auto !important; overflow: visible;
  }
  .dashboard-component-chart-holder:has(> h1):has(> h3) > h3 {
    margin-top: 0;
  }
  .dashboard-component-chart-holder:has(> h1):has(> h3) > ul {
    grid-template-columns: minmax(0, 1fr);
  }
  .grid-row:has(.big_number_total) { flex-wrap: wrap !important; }
  .grid-row:has(.big_number_total) > .dragdroppable-column {
    flex: 0 0 calc(50% - 20px) !important;
  }
  .grid-row:has(.big_number_total) .resizable-container {
    width: 100% !important; max-width: 100% !important;
  }
}
""".strip()


def required(name: str) -> str:
    """Reject missing credentials without printing them."""
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Dashboard bootstrap requires {name}")
    return value


@dataclass(frozen=True)
class ChartSpec:
    """A visual based exclusively on a compact serving dataset."""

    name: str
    mart: str
    viz_type: str
    params: dict[str, object]


def metric(column: str, label: str) -> dict[str, object]:
    return {"expressionType": "SIMPLE", "column": {"column_name": column},
            "aggregate": "SUM", "label": label}


def weighted_average(column: str, label: str) -> dict[str, object]:
    return {"expressionType": "SQL", "sqlExpression":
            f"SUM({column} * trip_count) / NULLIF(SUM(trip_count), 0)", "label": label}


def chart_specs() -> tuple[ChartSpec, ...]:
    """Use Gold metrics directly; weight monthly average-of-averages by trip count."""
    no_time = {"time_range": "No filter", "row_limit": 1000}
    return (
        ChartSpec("Total Valid Trips", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": metric("trip_count", "Trips"),
                   "y_axis_format": ".3s"}),
        ChartSpec("Total Amount", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": metric("total_revenue", "Total Amount"),
                   "y_axis_format": "$,.3s"}),
        ChartSpec("Average Trip Distance", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": weighted_average(
                      "average_trip_distance", "Average Distance"),
                   "y_axis_format": ",.2f"}),
        ChartSpec("Average Trip Duration", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": weighted_average(
                      "average_trip_duration_minutes", "Average Minutes"),
                   "y_axis_format": ",.1f"}),
        ChartSpec("Daily Trips", "daily_trip_metrics", "echarts_timeseries_line",
                  {**no_time, "time_range": DAILY_DISPLAY_RANGE,
                   "x_axis": "pickup_date", "granularity_sqla": "pickup_date",
                   "metrics": [metric("trip_count", "Trips")], "groupby": [],
                   "x_axis_title": "Pickup Date", "y_axis_title": "Trips",
                   "x_axis_title_margin": 35, "y_axis_title_margin": 50,
                   "y_axis_format": ".3s", "tooltip_time_format": "%b %d, %Y",
                   "rich_tooltip": True, "show_legend": False}),
        ChartSpec("Daily Total Amount", "daily_trip_metrics", "echarts_timeseries_line",
                  {**no_time, "time_range": DAILY_DISPLAY_RANGE,
                   "x_axis": "pickup_date", "granularity_sqla": "pickup_date",
                   "metrics": [metric("total_revenue", "Total Amount")], "groupby": [],
                   "x_axis_title": "Pickup Date", "y_axis_title": "Total Amount ($)",
                   "x_axis_title_margin": 35, "y_axis_title_margin": 62,
                   "y_axis_format": "$,.3s", "tooltip_time_format": "%b %d, %Y",
                   "rich_tooltip": True, "show_legend": False}),
        ChartSpec("Hourly Pickup Demand", "hourly_demand", "echarts_timeseries_bar",
                  {**no_time, "x_axis": "pickup_hour", "x_axis_force_categorical": True,
                   "groupby": [], "metrics": [metric("trip_count", "Hourly Trips")],
                   "x_axis_title": "Pickup Hour (0–23)", "y_axis_title": "Trips",
                   "x_axis_title_margin": 35, "y_axis_title_margin": 50,
                   "y_axis_format": ".3s", "rich_tooltip": True,
                   "show_legend": False}),
        ChartSpec("Pickup Trips by Borough", "pickup_zone_performance",
                  "echarts_timeseries_bar",
                  {**no_time, "x_axis": "borough", "groupby": [],
                   "orientation": "horizontal", "show_legend": False,
                   "x_axis_sort": "Borough Pickups", "x_axis_sort_asc": True,
                   "y_axis_title": "Trips", "y_axis_title_margin": 35,
                   "y_axis_format": ".3s", "rich_tooltip": True,
                   "metrics": [metric("trip_count", "Borough Pickups")]}),
        ChartSpec("Top Pickup Zones", "pickup_zone_performance",
                  "echarts_timeseries_bar",
                  {**no_time, "x_axis": "zone", "groupby": [], "row_limit": 10,
                   "order_desc": True, "orientation": "horizontal",
                   "x_axis_sort": "Zone Pickups", "x_axis_sort_asc": True,
                   "y_axis_title": "Trips", "y_axis_title_margin": 35,
                   "y_axis_format": ".3s", "rich_tooltip": True,
                   "metrics": [metric("trip_count", "Zone Pickups")],
                   "show_legend": False}),
        ChartSpec("Payment Type Trips", "payment_type_summary",
                  "pie",
                  {**no_time, "groupby": [{"expressionType": "SQL",
                                            "columnType": "GROUP_BY",
                                            "sqlExpression": PAYMENT_TYPE_LABEL_SQL,
                                            "label": "Payment method"}],
                   "metric": metric("trip_count", "Payment Trips"),
                   "row_limit": 10, "sort_by_metric": True,
                   "donut": True, "innerRadius": 55, "outerRadius": 75,
                   "show_legend": True, "legendOrientation": "right",
                   "legendType": "plain", "show_labels": False,
                   "number_format": ",.3s"}),
        ChartSpec("Insight Busiest Hour", "takeaway_breakdown", "table",
                  {**no_time, "query_mode": "aggregate", "page_length": 1,
                   "groupby": [{"expressionType": "SQL", "columnType": "GROUP_BY",
                                "sqlExpression": (
                                    "to_char(make_time(pickup_hour, 0, 0), 'FMHH12 AM')"),
                                "label": "Pickup hour"}],
                   "metrics": [metric("trip_count", "Trips")],
                   "orderby": [[metric("trip_count", "Trips"), False]]}),
        ChartSpec("Insight Top Borough", "takeaway_breakdown", "table",
                  {**no_time, "query_mode": "aggregate", "page_length": 1,
                   "groupby": ["borough"],
                   "metrics": [metric("trip_count", "Trips")],
                   "orderby": [[metric("trip_count", "Trips"), False]]}),
        ChartSpec("Insight Top Payment", "takeaway_breakdown", "table",
                  {**no_time, "query_mode": "aggregate", "page_length": 1,
                   "groupby": [{"expressionType": "SQL", "columnType": "GROUP_BY",
                                "sqlExpression": PAYMENT_TYPE_LABEL_SQL,
                                "label": "Payment method"}],
                   "metrics": [metric("trip_count", "Trips")],
                   "orderby": [[metric("trip_count", "Trips"), False]]}),
    )


def validate_chart_specs(specs: tuple[ChartSpec, ...]) -> None:
    """Fail before API writes if a saved chart cannot render in pinned Superset 6."""
    if len(specs) != 13 or len({spec.name for spec in specs}) != len(specs):
        raise ValueError("Dashboard requires thirteen uniquely named charts.")
    for spec in specs:
        if spec.mart not in MART_NAMES or spec.viz_type not in SUPPORTED_VIZ_TYPES:
            raise ValueError(f"Unsupported dashboard definition: {spec.name}")
        if spec.viz_type == "echarts_timeseries_bar" and not spec.params.get("x_axis"):
            raise ValueError(f"Bar chart lacks a category axis: {spec.name}")
        if spec.viz_type == "pie" and not spec.params.get("groupby"):
            raise ValueError(f"Pie chart lacks a category dimension: {spec.name}")
        if spec.viz_type == "table" and not spec.params.get("groupby"):
            raise ValueError(f"Table chart lacks a category dimension: {spec.name}")


def query_context(dataset_id: int, params: dict[str, object]) -> str:
    """Persist a chart-data context so saved charts can execute without a UI edit."""
    metric_value = params.get("metric")
    metrics = [metric_value] if metric_value else params.get("metrics", [])
    columns = ([params["x_axis"]] if "x_axis" in params else params.get("groupby", []))
    return json.dumps({
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [{"columns": columns, "metrics": metrics, "filters": [],
                     "time_range": params.get("time_range", "No filter"),
                     "row_limit": params.get("row_limit", 1000),
                     "order_desc": params.get("order_desc", False),
                     "orderby": params.get("orderby", [])}],
        "form_data": params, "result_format": "json", "result_type": "full",
    })


class SupersetClient:
    """Small authenticated wrapper around Superset's documented REST resources."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        login = self.request("POST", "/api/v1/security/login", {
            "username": required("SUPERSET_ADMIN_USER"),
            "password": required("SUPERSET_ADMIN_PASSWORD"),
            "provider": "db", "refresh": True,
        })
        self.session.headers["Authorization"] = f"Bearer {login['access_token']}"
        csrf = self.request("GET", "/api/v1/security/csrf_token/")
        self.session.headers["X-CSRFToken"] = csrf["result"]
        self.session.headers["Referer"] = self.base_url + "/"

    def request(
        self, method: str, path: str, payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        response = self.session.request(method, self.base_url + path, json=payload,
                                        timeout=30)
        if not response.ok:
            # Some error payloads can echo connection settings; do not print them.
            raise RuntimeError(f"Superset API {method} {path} failed: "
                               f"HTTP {response.status_code}")
        return response.json()

    def listed(self, resource: str) -> list[dict[str, object]]:
        return self.request("GET", f"/api/v1/{resource}/?q=(page_size:1000)")["result"]


def _layout(chart_ids: dict[str, int]) -> str:
    """Create a fresh dashboard in the saved five-row presentation order."""
    positions: dict[str, object] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"],
                    "children": []},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}},
    }
    names = [name for name in chart_ids if name not in INSIGHT_NAMES]
    rows = (
        ("MARKDOWN-INTRO",),
        tuple(names[:4]),
        ("Daily Trips", "Hourly Pickup Demand"),
        ("Payment Type Trips", "Pickup Trips by Borough"),
        ("Top Pickup Zones", "Daily Total Amount"),
    )
    for index, row in enumerate(rows, start=1):
        row_id = f"ROW-{index}"
        children = [name if name.startswith("MARKDOWN-")
                    else f"CHART-{chart_ids[name]}" for name in row]
        positions["GRID_ID"]["children"].append(row_id)
        positions[row_id] = {"id": row_id, "type": "ROW", "children": children,
                             "parents": ["ROOT_ID", "GRID_ID"],
                             "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        if index == 1:
            positions["MARKDOWN-INTRO"] = {
                "id": "MARKDOWN-INTRO", "type": "MARKDOWN", "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"code": OVERVIEW_MARKDOWN, "width": 12,
                         "height": OVERVIEW_HEIGHT},
            }
            continue
        for name in row:
            chart_id = chart_ids[name]
            if index == 2:
                width = 3
            else:
                width = 6
            label = {
                "Total Valid Trips": "Total Trips",
                "Average Trip Distance": "Avg Trip Distance (mi)",
                "Average Trip Duration": "Avg Trip Duration (min)",
                "Daily Trips": "Daily Trip Trend",
                "Pickup Trips by Borough": "Trips by Borough",
                "Top Pickup Zones": "Top 10 Pickup Zones",
                "Payment Type Trips": "Trips by Payment Type",
            }.get(name, name)
            positions[f"CHART-{chart_id}"] = {
                "id": f"CHART-{chart_id}", "type": "CHART", "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": chart_id, "sliceName": name,
                         "sliceNameOverride": label,
                         "width": width, "height": 24 if index == 2 else 48},
            }
    _add_insight_row(positions, chart_ids)
    return json.dumps(positions)


def _add_insight_row(positions: dict[str, object], chart_ids: dict[str, int]) -> None:
    """Insert responsive insights without touching the user's existing chart rows."""
    row_id = "ROW-INSIGHTS"
    if row_id in positions["GRID_ID"]["children"]:
        positions["GRID_ID"]["children"].remove(row_id)
    positions["GRID_ID"]["children"].insert(1, row_id)
    positions[row_id] = {
        "id": row_id, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"],
        "children": [f"CHART-{chart_ids[name]}" for name in INSIGHT_NAMES],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    labels = ("🕕 Busiest Pickup Hour", "🏙️ Top Pickup Borough",
              "💳 Most Used Payment Method")
    for name, label in zip(INSIGHT_NAMES, labels, strict=True):
        chart_id = chart_ids[name]
        positions[f"CHART-{chart_id}"] = {
            "id": f"CHART-{chart_id}", "type": "CHART", "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"chartId": chart_id, "sliceName": name,
                     "sliceNameOverride": label, "width": 4, "height": 18},
        }


def _merge_overview_layout(saved_position_json: str,
                           chart_ids: dict[str, int]) -> str:
    """Replace static insights while preserving every existing chart row and its order."""
    positions = json.loads(saved_position_json)
    intro = positions.get("MARKDOWN-INTRO")
    if not isinstance(intro, dict) or intro.get("type") != "MARKDOWN":
        raise ValueError("Saved dashboard has no expected introduction tile.")
    if intro.get("meta", {}).get("code") not in (
        OVERVIEW_MARKDOWN, PREVIOUS_OVERVIEW_MARKDOWN, LEGACY_OVERVIEW_MARKDOWN,
        TAXI_ICON_OVERVIEW_MARKDOWN, HEADER_MARKDOWN, PUBLISHED_OVERVIEW_MARKDOWN,
    ):
        raise ValueError("Saved dashboard has no expected overview tile.")
    top = positions.get("ROW-1")
    if top is None or top.get("children") not in (
        ["MARKDOWN-INTRO"], ["MARKDOWN-INTRO", "MARKDOWN-TAKEAWAYS"],
    ):
        raise ValueError("Saved dashboard has an unexpected top row.")
    if "MARKDOWN-TAKEAWAYS" in positions:
        takeaway = positions["MARKDOWN-TAKEAWAYS"]
        if (takeaway.get("type") != "MARKDOWN"
                or takeaway.get("meta", {}).get("code") != TAKEAWAYS_MARKDOWN):
            raise ValueError("Saved takeaway tile was edited; refusing to remove it.")
        del positions["MARKDOWN-TAKEAWAYS"]
        top["children"] = ["MARKDOWN-INTRO"]
    intro["meta"]["code"] = OVERVIEW_MARKDOWN
    intro["meta"]["width"] = 12
    intro["meta"]["height"] = OVERVIEW_HEIGHT
    old_insight_id = next((key for key, node in positions.items()
                           if key.startswith("CHART-") and isinstance(node, dict)
                           and node.get("meta", {}).get("sliceName") == "Insight Total Trips"),
                          None)
    if old_insight_id is not None:
        del positions[old_insight_id]
    _add_insight_row(positions, chart_ids)
    return json.dumps(positions)


def _filters(dataset_ids: dict[str, int], chart_ids: dict[str, int]) -> list[dict[str, object]]:
    """Month applies to all; borough applies to geography and responsive insights."""
    common = {"type": "NATIVE_FILTER", "filterType": "filter_select",
              "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
              "cascadeParentIds": [], "controlValues": {"multiSelect": True,
              "enableEmptyFilter": False, "defaultToFirstItem": False,
              "searchAllOptions": False, "inverseSelection": False},
              "scope": {"rootPath": ["ROOT_ID"], "excluded": []}}
    month = {**common, "id": "NATIVE_FILTER-month", "name": "Source Month",
             "targets": [{"datasetId": dataset_ids["daily_trip_metrics"],
                          "column": {"name": "_source_month"}}]}
    geographic = {chart_ids[name] for name in (
        "Pickup Trips by Borough", "Top Pickup Zones", *INSIGHT_NAMES
    )}
    borough = {**common, "id": "NATIVE_FILTER-borough", "name": "Pickup Borough",
               "targets": [{"datasetId": dataset_ids["pickup_zone_performance"],
                            "column": {"name": "borough"}}],
               "scope": {"rootPath": ["ROOT_ID"], "excluded": [
                   chart_id for chart_id in chart_ids.values() if chart_id not in geographic]}}
    return [month, borough]


def _unlink_legacy_insight(client: SupersetClient, charts: dict[str, int],
                           dashboard_id: int) -> None:
    """Remove the retired duplicate trip insight from this dashboard only."""
    chart_id = charts.get("Insight Total Trips")
    if chart_id is None:
        return
    chart = client.request("GET", f"/api/v1/chart/{chart_id}")["result"]
    if chart.get("slice_name") != "Insight Total Trips":
        raise ValueError("Retired insight chart has an unexpected identity.")
    dashboards = [item["id"] for item in chart.get("dashboards", [])]
    if dashboard_id in dashboards:
        client.request("PUT", f"/api/v1/chart/{chart_id}", {
            "dashboards": [item_id for item_id in dashboards if item_id != dashboard_id],
        })


def bootstrap(client: SupersetClient) -> dict[str, object]:
    """Create or update exactly one dashboard and its serving-only assets."""
    specs = chart_specs()
    validate_chart_specs(specs)
    themes = {item["theme_name"]: item["id"] for item in client.listed("theme")}
    if THEME_NAME not in themes:
        raise RuntimeError(f"Superset theme not available: {THEME_NAME}")
    existing_databases = {item["database_name"]: item["id"]
                          for item in client.listed("database")}
    database_id = existing_databases.get(DATABASE_NAME)
    if database_id is None:
        uri = (f"postgresql+psycopg2://{quote_plus(required('ANALYTICS_DB_USER'))}:"
               f"{quote_plus(required('ANALYTICS_DB_PASSWORD'))}@"
               f"{required('ANALYTICS_DB_HOST')}:{required('ANALYTICS_DB_PORT')}/"
               f"{required('ANALYTICS_DB_NAME')}")
        database_id = client.request("POST", "/api/v1/database/", {
            "database_name": DATABASE_NAME, "sqlalchemy_uri": uri,
            "expose_in_sqllab": True, "allow_dml": False,
        })["id"]
    LOGGER.info("Superset analytics database ready: id=%s", database_id)

    existing_datasets = {(item["schema"], item["table_name"]): item["id"]
                         for item in client.listed("dataset")
                         if item.get("database", {}).get("id") == database_id}
    dataset_ids = {}
    for name in MART_NAMES:
        dataset_id = existing_datasets.get(("analytics", name))
        if dataset_id is None:
            dataset_id = client.request("POST", "/api/v1/dataset/", {
                "database": database_id, "schema": "analytics", "table_name": name,
            })["id"]
        dataset_ids[name] = dataset_id
    LOGGER.info("Superset serving datasets ready: %s", dataset_ids)

    existing_charts = {item["slice_name"]: item["id"] for item in client.listed("chart")}
    chart_ids = {}
    for spec in specs:
        params = {**spec.params, "datasource": f"{dataset_ids[spec.mart]}__table",
                  "viz_type": spec.viz_type}
        body = {"slice_name": spec.name, "datasource_id": dataset_ids[spec.mart],
                "datasource_type": "table", "viz_type": spec.viz_type,
                "params": json.dumps(params),
                "query_context": query_context(dataset_ids[spec.mart], params)}
        chart_id = existing_charts.get(spec.name)
        if chart_id is None:
            chart_id = client.request("POST", "/api/v1/chart/", body)["id"]
        else:
            client.request("PUT", f"/api/v1/chart/{chart_id}", body)
        chart_ids[spec.name] = chart_id
    LOGGER.info("Superset charts ready: count=%s", len(chart_ids))

    metadata = {"native_filter_configuration": _filters(dataset_ids, chart_ids),
                "label_colors": SERIES_COLORS,
                "filter_bar_orientation": "HORIZONTAL", "refresh_frequency": 0}
    dashboard_body = {"dashboard_title": TITLE, "published": True,
                      "slug": "nyc-urban-mobility-overview",
                      "position_json": _layout(chart_ids), "json_metadata": json.dumps(metadata),
                      "css": DASHBOARD_CSS, "theme_id": themes[THEME_NAME]}
    existing_dashboards = {item["dashboard_title"]: item["id"]
                           for item in client.listed("dashboard")}
    dashboard_id = existing_dashboards.get(TITLE)
    if dashboard_id is None:
        dashboard_id = client.request("POST", "/api/v1/dashboard/", dashboard_body)["id"]
    else:
        saved = client.request("GET", f"/api/v1/dashboard/{dashboard_id}")["result"]
        dashboard_body["position_json"] = _merge_overview_layout(
            saved["position_json"], chart_ids)
        client.request("PUT", f"/api/v1/dashboard/{dashboard_id}", dashboard_body)
    for chart_id in chart_ids.values():
        client.request("PUT", f"/api/v1/chart/{chart_id}", {"dashboards": [dashboard_id]})
    _unlink_legacy_insight(client, existing_charts, dashboard_id)
    LOGGER.info("Superset dashboard ready: id=%s title=%s", dashboard_id, TITLE)
    return {"database_id": database_id, "dataset_ids": dataset_ids,
            "chart_ids": chart_ids, "dashboard_id": dashboard_id}


def main() -> int:
    logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    result = bootstrap(client)
    LOGGER.info("Dashboard URL: http://localhost:8088/superset/dashboard/%s/",
                result["dashboard_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
