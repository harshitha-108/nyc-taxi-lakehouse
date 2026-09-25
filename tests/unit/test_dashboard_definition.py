"""Dashboard contract tests requiring neither Superset nor PostgreSQL."""

import json
from dataclasses import replace

import pytest
from scripts.bootstrap_dashboard import (
    DAILY_DISPLAY_RANGE,
    DASHBOARD_CSS,
    HEADER_MARKDOWN,
    MART_NAMES,
    PAYMENT_TYPE_LABEL_SQL,
    SERIES_COLORS,
    TITLE,
    _filters,
    _layout,
    bootstrap,
    chart_specs,
    query_context,
    validate_chart_specs,
)

from nyc_taxi_lakehouse.serving.database import MARTS


def test_dashboard_charts_use_only_serving_marts() -> None:
    specs = chart_specs()
    assert len(specs) == 10
    assert {spec.mart for spec in specs} == {
        "daily_trip_metrics", "hourly_demand", "pickup_zone_performance",
        "payment_type_summary",
    }
    assert all(spec.viz_type for spec in specs)


def test_bar_charts_use_superset_6_echarts_contract() -> None:
    """The Superset 6 frontend registers ECharts bars, not legacy dist_bar."""
    specs = {spec.name: spec for spec in chart_specs()}
    expected_axes = {
        "Hourly Pickup Demand": "pickup_hour",
        "Pickup Trips by Borough": "borough",
        "Top Pickup Zones": "zone",
    }
    assert {spec.viz_type for spec in specs.values()} == {
        "big_number_total", "echarts_timeseries_line", "echarts_timeseries_bar",
    }
    for name, axis in expected_axes.items():
        spec = specs[name]
        assert spec.viz_type == "echarts_timeseries_bar"
        assert spec.params["x_axis"] == axis
        assert spec.params["groupby"] == []
        assert spec.params["metrics"][0]["column"]["column_name"] == "trip_count"
        context = json.loads(query_context(7, spec.params))
        assert context["queries"][0]["columns"] == [axis]
    payment = specs["Payment Type Trips"]
    assert payment.viz_type == "echarts_timeseries_bar"
    assert payment.params["x_axis"] == {
        "expressionType": "SQL", "columnType": "BASE_AXIS",
        "sqlExpression": PAYMENT_TYPE_LABEL_SQL, "label": "Payment method",
    }
    assert json.loads(query_context(7, payment.params))["queries"][0][
        "columns"] == [payment.params["x_axis"]]
    top_zones = specs["Top Pickup Zones"].params
    assert top_zones["row_limit"] == 10
    assert top_zones["order_desc"] is True
    assert top_zones["orientation"] == "horizontal"
    for name in ("Pickup Trips by Borough", "Top Pickup Zones"):
        params = specs[name].params
        assert params["x_axis_sort"] == params["metrics"][0]["label"]
        # ECharts reverses the categorical Y axis for horizontal bars:
        # ascending data order displays the largest bar at the top.
        assert params["x_axis_sort_asc"] is True


def test_dashboard_layout_contains_all_charts_once() -> None:
    chart_ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    layout = json.loads(_layout(chart_ids))
    assert layout["ROOT_ID"]["children"] == ["GRID_ID"]
    chart_nodes = {name for name in layout if name.startswith("CHART-")}
    assert chart_nodes == {f"CHART-{number}" for number in chart_ids.values()}
    assert len(layout["GRID_ID"]["children"]) == 5
    assert layout["ROW-1"]["children"] == ["MARKDOWN-INTRO"]
    assert layout["MARKDOWN-INTRO"]["meta"]["code"] == HEADER_MARKDOWN
    assert "NYC Urban Mobility" in HEADER_MARKDOWN
    assert [layout[node]["meta"]["width"] for node in layout["ROW-2"]["children"]] == [3] * 4
    for row_id in ("ROW-3", "ROW-5"):
        assert [layout[node]["meta"]["width"] for node in layout[row_id]["children"]] == [8, 4]
    assert [layout[node]["meta"]["width"] for node in layout["ROW-4"]["children"]] == [7, 5]
    assert layout[f"CHART-{chart_ids['Daily Trips']}"]["meta"][
        "sliceNameOverride"] == "Daily Trip Trend"


def test_presentation_changes_preserve_metrics_and_safe_css() -> None:
    specs = {spec.name: spec for spec in chart_specs()}
    for name in ("Total Valid Trips", "Total Amount", "Average Trip Distance",
                 "Average Trip Duration"):
        assert specs[name].params["y_axis_format"]
    assert "dashboard-component-chart-holder" in DASHBOARD_CSS
    assert "#CHART-" not in DASHBOARD_CSS
    assert "display: none" not in DASHBOARD_CSS
    assert "--kpi-accent" in DASHBOARD_CSS
    assert "linear-gradient" in DASHBOARD_CSS
    assert "↑" not in DASHBOARD_CSS and "↓" not in DASHBOARD_CSS
    for name in ("Daily Trips", "Daily Total Amount"):
        spec = specs[name]
        assert spec.params["time_range"] == DAILY_DISPLAY_RANGE
        assert json.loads(query_context(7, spec.params))["queries"][0][
            "time_range"] == DAILY_DISPLAY_RANGE
    assert specs["Average Trip Distance"].params["metric"]["sqlExpression"] == (
        "SUM(average_trip_distance * trip_count) / NULLIF(SUM(trip_count), 0)"
    )
    assert specs["Average Trip Duration"].params["metric"]["sqlExpression"] == (
        "SUM(average_trip_duration_minutes * trip_count) / NULLIF(SUM(trip_count), 0)"
    )


def test_all_analytical_charts_have_units_tooltips_and_stable_colors() -> None:
    specs = {spec.name: spec for spec in chart_specs()}
    axes = {
        "Daily Trips": ("Pickup Date", "Trips"),
        "Daily Total Amount": ("Pickup Date", "Total Amount ($)"),
        "Hourly Pickup Demand": ("Pickup Hour (0–23)", "Trips"),
        "Pickup Trips by Borough": (None, "Trips"),
        "Top Pickup Zones": (None, "Trips"),
        "Payment Type Trips": ("Payment method", "Trips"),
    }
    for name, (category, measure) in axes.items():
        params = specs[name].params
        assert (params.get("x_axis_title"), params["y_axis_title"]) == (category, measure)
        assert params["y_axis_title_margin"] > 0
        if category is not None:
            assert params["x_axis_title_margin"] > 0
        assert params["y_axis_format"] == ("$,.3s" if name == "Daily Total Amount"
                                            else ".3s")
        assert params["rich_tooltip"] is True
        assert params["metrics"][0]["label"] in SERIES_COLORS
    assert len(set(SERIES_COLORS.values())) > 2
    assert "2024-01-01 : 2024-07-01" == DAILY_DISPLAY_RANGE


def test_payment_labels_follow_official_tlc_dictionary_and_keep_unknown_codes() -> None:
    labels = {
        0: "Flex Fare trip", 1: "Credit card", 2: "Cash", 3: "No charge",
        4: "Dispute", 5: "Unknown", 6: "Voided trip",
    }
    for code, label in labels.items():
        assert f"WHEN {code} THEN '{label}'" in PAYMENT_TYPE_LABEL_SQL
    assert "ELSE 'Code ' || CAST(payment_type AS TEXT)" in PAYMENT_TYPE_LABEL_SQL
    assert chart_specs()[-1].params["x_axis_label_rotation"] == 35


def test_bootstrap_reuses_dashboard_assets_and_persists_styling(monkeypatch) -> None:
    for name in ("ANALYTICS_DB_USER", "ANALYTICS_DB_PASSWORD", "ANALYTICS_DB_HOST",
                 "ANALYTICS_DB_PORT", "ANALYTICS_DB_NAME"):
        monkeypatch.setenv(name, "test-only")

    class MemoryClient:
        def __init__(self) -> None:
            self.items = {"database": [], "dataset": [], "chart": [], "dashboard": []}
            self.dashboard_body = {}

        def listed(self, resource):
            if resource == "theme":
                return [{"id": 1, "theme_name": "THEME_DEFAULT"}]
            return self.items[resource]

        def request(self, method, path, payload=None):
            parts = path.strip("/").split("/")
            resource = parts[2]
            if resource == "dashboard":
                self.dashboard_body = payload
            if method == "POST":
                new_id = len(self.items[resource]) + 1
                item = {"id": new_id, **payload}
                if resource == "dataset":
                    item["database"] = {"id": payload["database"]}
                self.items[resource].append(item)
                return {"id": new_id}
            if resource == "dashboard" and method == "PUT":
                self.items[resource][int(parts[3]) - 1].update(payload)
            return {}

    client = MemoryClient()
    first = bootstrap(client)
    second = bootstrap(client)
    assert first == second
    assert {name: len(items) for name, items in client.items.items()} == {
        "database": 1, "dataset": 5, "chart": 10, "dashboard": 1,
    }
    assert client.dashboard_body["css"] == DASHBOARD_CSS
    assert client.dashboard_body["theme_id"] == 1
    metadata = json.loads(client.dashboard_body["json_metadata"])
    assert metadata["label_colors"] == SERIES_COLORS
    filters = metadata["native_filter_configuration"]
    assert [item["name"] for item in filters] == ["Source Month", "Pickup Borough"]


def test_borough_filter_excludes_non_geographic_charts() -> None:
    ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    filters = _filters({"daily_trip_metrics": 1, "pickup_zone_performance": 5}, ids)
    assert len(filters) == 2
    assert filters[0]["targets"][0]["column"]["name"] == "_source_month"
    excluded = set(filters[1]["scope"]["excluded"])
    assert ids["Daily Trips"] in excluded
    assert ids["Top Pickup Zones"] not in excluded


def test_saved_query_context_identifies_serving_dataset() -> None:
    spec = chart_specs()[0]
    context = json.loads(query_context(7, spec.params))
    assert context["datasource"] == {"id": 7, "type": "table"}
    assert context["queries"][0]["metrics"][0]["label"] == "Trips"


def test_dashboard_definition_references_existing_serving_fields() -> None:
    specs = chart_specs()
    validate_chart_specs(specs)
    assert TITLE == "NYC Urban Mobility Overview"
    assert len(MART_NAMES) == len(MARTS) == 5
    names_by_mart = {mart.name: set(mart.names) for mart in MARTS}
    assert len({spec.name for spec in specs}) == 10
    for spec in specs:
        fields = names_by_mart[spec.mart]
        if "x_axis" in spec.params:
            if spec.name == "Payment Type Trips":
                assert "payment_type" in fields
                assert spec.params["x_axis"]["sqlExpression"] == PAYMENT_TYPE_LABEL_SQL
            else:
                assert spec.params["x_axis"] in fields
        for value in spec.params.get("metrics", []):
            assert value["column"]["column_name"] in fields
    chart_ids = {spec.name: number for number, spec in enumerate(specs, start=1)}
    layout = json.loads(_layout(chart_ids))
    assert {node["meta"]["chartId"] for node in layout.values()
            if isinstance(node, dict) and node.get("type") == "CHART"} == set(chart_ids.values())
    filters = _filters({name: index for index, name in enumerate(MART_NAMES, start=1)},
                       chart_ids)
    assert [item["name"] for item in filters] == ["Source Month", "Pickup Borough"]
    assert filters[0]["targets"][0]["column"]["name"] in names_by_mart[
        "daily_trip_metrics"]
    assert filters[1]["targets"][0]["column"]["name"] in names_by_mart[
        "pickup_zone_performance"]
    expected_geographic = {chart_ids[name] for name in (
        "Pickup Trips by Borough", "Top Pickup Zones")}
    assert set(chart_ids.values()) - set(filters[1]["scope"]["excluded"]) == expected_geographic


def test_unsupported_chart_fails_before_dashboard_api_mutation() -> None:
    specs = chart_specs()
    unsupported = (*specs[:-1], replace(specs[-1], viz_type="dist_bar"))
    with pytest.raises(ValueError, match="Unsupported dashboard definition"):
        validate_chart_specs(unsupported)
    bad_axis = (*specs[:-1], replace(specs[-1], params={"metrics": []}))
    with pytest.raises(ValueError, match="lacks a category axis"):
        validate_chart_specs(bad_axis)
