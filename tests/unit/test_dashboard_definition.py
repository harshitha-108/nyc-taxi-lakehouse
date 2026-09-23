"""Dashboard contract tests requiring neither Superset nor PostgreSQL."""

import json

from scripts.bootstrap_dashboard import _filters, _layout, chart_specs, query_context


def test_dashboard_charts_use_only_serving_marts() -> None:
    specs = chart_specs()
    assert len(specs) == 10
    assert {spec.mart for spec in specs} == {
        "daily_trip_metrics", "hourly_demand", "pickup_zone_performance",
        "payment_type_summary",
    }
    assert all(spec.viz_type for spec in specs)


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
