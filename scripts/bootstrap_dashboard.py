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
MART_NAMES = (
    "daily_trip_metrics", "hourly_demand", "pickup_location_performance",
    "payment_type_summary", "pickup_zone_performance",
)


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
                  {**no_time, "metric": metric("trip_count", "Trips")}),
        ChartSpec("Total Amount", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": metric("total_revenue", "Total Amount")}),
        ChartSpec("Average Trip Distance", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": weighted_average(
                      "average_trip_distance", "Average Distance")}),
        ChartSpec("Average Trip Duration", "daily_trip_metrics", "big_number_total",
                  {**no_time, "metric": weighted_average(
                      "average_trip_duration_minutes", "Average Minutes")}),
        ChartSpec("Daily Trips", "daily_trip_metrics", "echarts_timeseries_line",
                  {**no_time, "x_axis": "pickup_date", "granularity_sqla": "pickup_date",
                   "metrics": [metric("trip_count", "Trips")], "groupby": []}),
        ChartSpec("Daily Total Amount", "daily_trip_metrics", "echarts_timeseries_line",
                  {**no_time, "x_axis": "pickup_date", "granularity_sqla": "pickup_date",
                   "metrics": [metric("total_revenue", "Total Amount")], "groupby": []}),
        ChartSpec("Hourly Pickup Demand", "hourly_demand", "echarts_timeseries_bar",
                  {**no_time, "x_axis": "pickup_hour", "x_axis_force_categorical": True,
                   "groupby": [], "metrics": [metric("trip_count", "Trips")]}),
        ChartSpec("Pickup Trips by Borough", "pickup_zone_performance",
                  "echarts_timeseries_bar",
                  {**no_time, "x_axis": "borough", "groupby": [],
                   "metrics": [metric("trip_count", "Trips")]}),
        ChartSpec("Top Pickup Zones", "pickup_zone_performance",
                  "echarts_timeseries_bar",
                  {**no_time, "x_axis": "zone", "groupby": [], "row_limit": 10,
                   "order_desc": True, "orientation": "horizontal",
                   "metrics": [metric("trip_count", "Trips")]}),
        ChartSpec("Payment Type Trips", "payment_type_summary",
                  "echarts_timeseries_bar",
                  {**no_time, "x_axis": "payment_type", "x_axis_force_categorical": True,
                   "groupby": [], "metrics": [metric("trip_count", "Trips")]}),
    )


def query_context(dataset_id: int, params: dict[str, object]) -> str:
    """Persist a chart-data context so saved charts can execute without a UI edit."""
    metric_value = params.get("metric")
    metrics = [metric_value] if metric_value else params.get("metrics", [])
    columns = ([params["x_axis"]] if "x_axis" in params else params.get("groupby", []))
    return json.dumps({
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [{"columns": columns, "metrics": metrics, "filters": [],
                     "time_range": "No filter", "row_limit": params.get("row_limit", 1000),
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
    """Use Superset's documented position_json tree, with four KPI cards first."""
    positions: dict[str, object] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"],
                    "children": []},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}},
    }
    names = list(chart_ids)
    rows = (names[:4], names[4:6], names[6:8], names[8:10])
    for index, row in enumerate(rows, start=1):
        row_id = f"ROW-{index}"
        children = [f"CHART-{chart_ids[name]}" for name in row]
        positions["GRID_ID"]["children"].append(row_id)
        positions[row_id] = {"id": row_id, "type": "ROW", "children": children,
                             "parents": ["ROOT_ID", "GRID_ID"],
                             "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for name in row:
            chart_id = chart_ids[name]
            positions[f"CHART-{chart_id}"] = {
                "id": f"CHART-{chart_id}", "type": "CHART", "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": chart_id, "sliceName": name,
                         "width": 3 if index == 1 else 6, "height": 40},
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
    for spec in chart_specs():
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
                "filter_bar_orientation": "HORIZONTAL", "refresh_frequency": 0}
    dashboard_body = {"dashboard_title": TITLE, "published": True,
                      "slug": "nyc-urban-mobility-overview",
                      "position_json": _layout(chart_ids), "json_metadata": json.dumps(metadata)}
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
