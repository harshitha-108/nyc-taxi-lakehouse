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
    "payment_type_summary", "pickup_zone_performance",
)
SUPPORTED_VIZ_TYPES = frozenset({
    "big_number_total", "echarts_timeseries_line", "echarts_timeseries_bar",
})
HEADER_MARKDOWN = (
    "# NYC Urban Mobility\n"
    "Yellow Taxi Performance & Demand Analytics\n\n"
    "January–June 2024 | NYC Yellow Taxi Trips"
)
DASHBOARD_CSS = """
.dashboard-content { background: #f5f7fb !important; }
.dashboard-component-chart-holder:has(> h1) {
  padding: 8px 20px;
  border-top: 4px solid #1e4765;
  background: linear-gradient(180deg, #f2f6f9, #fff 70%);
}
.dashboard-component-chart-holder > h1 {
  margin: 0 0 4px; font-size: 30px; font-weight: 700; letter-spacing: -0.025em;
  color: #14243a;
}
.dashboard-component-chart-holder > p { margin: 0 0 4px; color: #52657d; font-size: 14px; }
.dashboard-component-chart-holder {
  background: #fff; border: 1px solid #e2e8f0; border-radius: 10px;
  box-shadow: 0 2px 10px rgba(20, 36, 58, 0.04);
}
.dashboard-component-chart-holder .header-title { color: #24364d; font-weight: 600; }
.dashboard-component-chart-holder:has(.chart-slice):not(:has(.big_number_total)) {
  border-top: 4px solid var(--chart-accent, #1e4765);
  background: linear-gradient(180deg, var(--chart-tint, #f2f6f9), #fff 24%);
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
  border-top: 4px solid var(--kpi-accent, #1e4765);
  background: linear-gradient(180deg, var(--kpi-tint, #f3f7fa), #fff 45%);
}
.dashboard-component-chart-holder:has([data-test-chart-name="Total Valid Trips"]) {
  --kpi-accent: #1e4765; --kpi-tint: #f2f6f9;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Total Amount"]) {
  --kpi-accent: #0e7c86; --kpi-tint: #edf8f8;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Average Trip Distance"]) {
  --kpi-accent: #2e8062; --kpi-tint: #f0f8f3;
}
.dashboard-component-chart-holder:has([data-test-chart-name="Average Trip Duration"]) {
  --kpi-accent: #b0783d; --kpi-tint: #fcf6ed;
}
.dashboard-component-chart-holder .big_number_total .header-line {
  color: var(--kpi-accent, #1e4765) !important;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
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
                  "echarts_timeseries_bar",
                  {**no_time, "x_axis": {"expressionType": "SQL",
                                           "columnType": "BASE_AXIS",
                                           "sqlExpression": PAYMENT_TYPE_LABEL_SQL,
                                           "label": "Payment method"},
                   "x_axis_force_categorical": True, "x_axis_label_rotation": 35,
                   "groupby": [], "metrics": [metric("trip_count", "Payment Trips")],
                   "x_axis_title": "Payment method", "y_axis_title": "Trips",
                   "x_axis_title_margin": 65, "y_axis_title_margin": 50,
                   "y_axis_format": ".3s", "rich_tooltip": True,
                   "show_legend": False}),
    )


def validate_chart_specs(specs: tuple[ChartSpec, ...]) -> None:
    """Fail before API writes if a saved chart cannot render in pinned Superset 6."""
    if len(specs) != 10 or len({spec.name for spec in specs}) != len(specs):
        raise ValueError("Dashboard requires ten uniquely named charts.")
    for spec in specs:
        if spec.mart not in MART_NAMES or spec.viz_type not in SUPPORTED_VIZ_TYPES:
            raise ValueError(f"Unsupported dashboard definition: {spec.name}")
        if spec.viz_type == "echarts_timeseries_bar" and not spec.params.get("x_axis"):
            raise ValueError(f"Bar chart lacks a category axis: {spec.name}")


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
                     "order_desc": params.get("order_desc", False)}],
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
    """Put a compact introduction above KPIs and three balanced analytical rows."""
    positions: dict[str, object] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"],
                    "children": []},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}},
    }
    names = list(chart_ids)
    rows = (
        ("MARKDOWN-INTRO",),
        tuple(names[:4]),
        ("Daily Trips", "Pickup Trips by Borough"),
        ("Hourly Pickup Demand", "Top Pickup Zones"),
        ("Daily Total Amount", "Payment Type Trips"),
    )
    for index, row in enumerate(rows, start=1):
        row_id = f"ROW-{index}"
        children = ["MARKDOWN-INTRO" if name == "MARKDOWN-INTRO"
                    else f"CHART-{chart_ids[name]}" for name in row]
        positions["GRID_ID"]["children"].append(row_id)
        positions[row_id] = {"id": row_id, "type": "ROW", "children": children,
                             "parents": ["ROOT_ID", "GRID_ID"],
                             "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        if index == 1:
            positions["MARKDOWN-INTRO"] = {
                "id": "MARKDOWN-INTRO", "type": "MARKDOWN", "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"code": HEADER_MARKDOWN, "width": 12, "height": 18},
            }
            continue
        for name in row:
            chart_id = chart_ids[name]
            if index == 2:
                width = 3
            elif index == 4:
                width = 7 if name == row[0] else 5
            else:
                width = 8 if name == row[0] else 4
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
    return json.dumps(positions)


def _filters(dataset_ids: dict[str, int], chart_ids: dict[str, int]) -> list[dict[str, object]]:
    """Month applies across all marts; borough only to the two geographic charts."""
    common = {"type": "NATIVE_FILTER", "filterType": "filter_select",
              "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
              "cascadeParentIds": [], "controlValues": {"multiSelect": True,
              "enableEmptyFilter": False, "defaultToFirstItem": False,
              "searchAllOptions": False, "inverseSelection": False},
              "scope": {"rootPath": ["ROOT_ID"], "excluded": []}}
    month = {**common, "id": "NATIVE_FILTER-month", "name": "Source Month",
             "targets": [{"datasetId": dataset_ids["daily_trip_metrics"],
                          "column": {"name": "_source_month"}}]}
    geographic = {chart_ids["Pickup Trips by Borough"], chart_ids["Top Pickup Zones"]}
    borough = {**common, "id": "NATIVE_FILTER-borough", "name": "Pickup Borough",
               "targets": [{"datasetId": dataset_ids["pickup_zone_performance"],
                            "column": {"name": "borough"}}],
               "scope": {"rootPath": ["ROOT_ID"], "excluded": [
                   chart_id for chart_id in chart_ids.values() if chart_id not in geographic]}}
    return [month, borough]


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
        client.request("PUT", f"/api/v1/dashboard/{dashboard_id}", dashboard_body)
    for chart_id in chart_ids.values():
        client.request("PUT", f"/api/v1/chart/{chart_id}", {"dashboards": [dashboard_id]})
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
