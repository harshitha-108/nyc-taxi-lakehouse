"""Dashboard contract tests requiring neither Superset nor PostgreSQL."""

import json
from dataclasses import replace

import pytest
from scripts.bootstrap_dashboard import (
    MART_NAMES,
    TITLE,
    _filters,
    _layout,
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
        "Payment Type Trips": "payment_type",
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
    top_zones = specs["Top Pickup Zones"].params
    assert top_zones["row_limit"] == 10
    assert top_zones["order_desc"] is True
    assert top_zones["orientation"] == "horizontal"


def test_dashboard_layout_contains_all_charts_once() -> None:
    chart_ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    layout = json.loads(_layout(chart_ids))
    assert layout["ROOT_ID"]["children"] == ["GRID_ID"]
    chart_nodes = {name for name in layout if name.startswith("CHART-")}
    assert chart_nodes == {f"CHART-{number}" for number in chart_ids.values()}


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
