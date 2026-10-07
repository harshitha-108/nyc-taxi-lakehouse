"""Dashboard contract tests requiring neither Superset nor PostgreSQL."""

import json
from dataclasses import replace

import pytest
from scripts.bootstrap_dashboard import (
    DAILY_DISPLAY_RANGE,
    DASHBOARD_CSS,
    HEADER_MARKDOWN,
    INSIGHT_NAMES,
    LEGACY_OVERVIEW_MARKDOWN,
    MART_NAMES,
    OVERVIEW_HEIGHT,
    OVERVIEW_MARKDOWN,
    PAYMENT_TYPE_LABEL_SQL,
    PREVIOUS_OVERVIEW_MARKDOWN,
    PUBLISHED_OVERVIEW_MARKDOWN,
    SERIES_COLORS,
    TAKEAWAYS_MARKDOWN,
    TAXI_ICON_OVERVIEW_MARKDOWN,
    TITLE,
    _filters,
    _layout,
    _merge_overview_layout,
    _unlink_legacy_insight,
    bootstrap,
    chart_specs,
    query_context,
    validate_chart_specs,
)

from nyc_taxi_lakehouse.serving.database import MARTS


def test_dashboard_charts_use_only_serving_marts() -> None:
    specs = chart_specs()
    assert len(specs) == 13
    assert "Insight Total Trips" not in {spec.name for spec in specs}
    assert {spec.mart for spec in specs} == {
        "daily_trip_metrics", "hourly_demand", "pickup_zone_performance",
        "payment_type_summary", "takeaway_breakdown",
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
        "big_number_total", "echarts_timeseries_line", "echarts_timeseries_bar", "pie",
        "table",
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
    assert payment.viz_type == "pie"
    assert payment.params["donut"] is True
    assert payment.params["show_legend"] is True
    assert payment.params["groupby"] == [{
        "expressionType": "SQL", "columnType": "GROUP_BY",
        "sqlExpression": PAYMENT_TYPE_LABEL_SQL, "label": "Payment method",
    }]
    assert json.loads(query_context(7, payment.params))["queries"][0][
        "columns"] == payment.params["groupby"]
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
    assert len(layout["GRID_ID"]["children"]) == 6
    assert layout["GRID_ID"]["children"][1] == "ROW-INSIGHTS"
    assert layout["ROW-INSIGHTS"]["children"] == [
        f"CHART-{chart_ids[name]}" for name in INSIGHT_NAMES]
    assert [layout[node]["meta"]["width"] for node in layout["ROW-INSIGHTS"][
        "children"]] == [4, 4, 4]
    assert layout["ROW-1"]["children"] == ["MARKDOWN-INTRO"]
    assert layout["MARKDOWN-INTRO"]["meta"]["code"] == OVERVIEW_MARKDOWN
    assert "MARKDOWN-TAKEAWAYS" not in layout
    assert HEADER_MARKDOWN in OVERVIEW_MARKDOWN
    assert "Key Takeaways" in OVERVIEW_MARKDOWN
    assert "20.0M" not in OVERVIEW_MARKDOWN
    assert "NYC Urban Mobility" in HEADER_MARKDOWN
    assert HEADER_MARKDOWN.startswith("# 🗽 ")
    assert "🚕" not in HEADER_MARKDOWN
    assert [layout[node]["meta"]["width"] for node in layout["ROW-2"]["children"]] == [3] * 4
    for row_id in ("ROW-3", "ROW-4", "ROW-5"):
        assert [layout[node]["meta"]["width"] for node in layout[row_id]["children"]] == [6, 6]
    assert layout["ROW-3"]["children"] == [
        f"CHART-{chart_ids['Daily Trips']}", f"CHART-{chart_ids['Hourly Pickup Demand']}"]
    assert layout["ROW-4"]["children"] == [
        f"CHART-{chart_ids['Payment Type Trips']}",
        f"CHART-{chart_ids['Pickup Trips by Borough']}"]
    assert layout["ROW-5"]["children"] == [
        f"CHART-{chart_ids['Top Pickup Zones']}",
        f"CHART-{chart_ids['Daily Total Amount']}"]
    assert layout[f"CHART-{chart_ids['Daily Trips']}"]["meta"][
        "sliceNameOverride"] == "Daily Trip Trend"


def test_presentation_changes_preserve_metrics_and_safe_css() -> None:
    specs = {spec.name: spec for spec in chart_specs()}
    for name in ("Total Valid Trips", "Total Amount", "Average Trip Distance",
                 "Average Trip Duration"):
        assert specs[name].params["y_axis_format"]
    assert "dashboard-component-chart-holder" in DASHBOARD_CSS
    assert "#CHART-" not in DASHBOARD_CSS
    assert "dashboard-component-chart-holder { display: none" not in DASHBOARD_CSS
    assert "--kpi-accent" in DASHBOARD_CSS
    assert "border-top: 3px solid var(--kpi-accent" in DASHBOARD_CSS
    assert 'data-test-chart-name="Daily Trips"' in DASHBOARD_CSS
    assert OVERVIEW_HEIGHT >= 18
    assert ".grid-row:has(#MARKDOWN-INTRO) .resizable-container" in DASHBOARD_CSS
    assert "overflow: visible !important" in DASHBOARD_CSS
    assert "margin-bottom: 0 !important" in DASHBOARD_CSS
    assert "margin-top: -1px !important" in DASHBOARD_CSS
    assert "linear-gradient" in DASHBOARD_CSS
    assert "grid-template-columns: repeat(4, minmax(0, 1fr))" in DASHBOARD_CSS
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in DASHBOARD_CSS
    assert "grid-template-columns: minmax(0, 1fr)" in DASHBOARD_CSS
    assert DASHBOARD_CSS.count('.header-title::before { content:') == 4
    assert DASHBOARD_CSS.count('> li:nth-child(') == 7
    assert "> ul > li strong {\n  font-weight: 700;" in DASHBOARD_CSS
    assert "background: #f7faff; font-size: 14px; font-weight: 400;" in DASHBOARD_CSS
    for icon in ('🚕', '🕕', '🏙️', '💳'):
        assert f'content: "{icon}"' in DASHBOARD_CSS
    assert "@media (max-width: 800px)" in DASHBOARD_CSS
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
    assert {spec.name: spec for spec in chart_specs()}[
        "Payment Type Trips"].params["groupby"][0]["sqlExpression"] == (
        PAYMENT_TYPE_LABEL_SQL
    )


def test_insights_query_filtered_breakdown_and_rank_by_trips() -> None:
    specs = {spec.name: spec for spec in chart_specs()}
    for name in INSIGHT_NAMES:
        assert specs[name].mart == "takeaway_breakdown"
        context = json.loads(query_context(6, specs[name].params))
        assert context["datasource"] == {"id": 6, "type": "table"}
        query = context["queries"][0]
        assert query["row_limit"] == 1000
        assert specs[name].params["page_length"] == 1
        assert query["orderby"][0][1] is False
        assert query["orderby"][0][0]["column"]["column_name"] == "trip_count"
    assert "January–June 2024" not in OVERVIEW_MARKDOWN
    assert "20.0M" in LEGACY_OVERVIEW_MARKDOWN


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
            if resource == "dashboard" and method in ("POST", "PUT"):
                self.dashboard_body = payload
            if resource == "dashboard" and method == "GET":
                return {"result": self.items[resource][int(parts[3]) - 1]}
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
        "database": 1, "dataset": 6, "chart": 13, "dashboard": 1,
    }
    assert client.dashboard_body["css"] == DASHBOARD_CSS
    assert client.dashboard_body["theme_id"] == 1
    metadata = json.loads(client.dashboard_body["json_metadata"])
    assert metadata["label_colors"] == SERIES_COLORS
    filters = metadata["native_filter_configuration"]
    assert [item["name"] for item in filters] == ["Source Month", "Pickup Borough"]


def test_merge_overview_preserves_manual_chart_arrangement() -> None:
    ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    saved = json.loads(_layout(ids))
    saved["MARKDOWN-INTRO"]["meta"].update(
        {"code": HEADER_MARKDOWN, "width": 6, "height": 28})
    saved["MARKDOWN-TAKEAWAYS"] = {
        "id": "MARKDOWN-TAKEAWAYS", "type": "MARKDOWN", "children": [],
        "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
        "meta": {"code": TAKEAWAYS_MARKDOWN, "width": 6, "height": 28},
    }
    saved["ROW-1"]["children"].append("MARKDOWN-TAKEAWAYS")
    saved["ROW-3"]["children"].reverse()
    saved["CHART-5"]["meta"]["height"] = 55

    saved["MARKDOWN-INTRO"]["meta"]["code"] = HEADER_MARKDOWN
    del saved["ROW-INSIGHTS"]
    saved["GRID_ID"]["children"].remove("ROW-INSIGHTS")
    for name in INSIGHT_NAMES:
        del saved[f"CHART-{ids[name]}"]
    merged_json = _merge_overview_layout(json.dumps(saved), ids)
    merged = json.loads(merged_json)
    assert merged["ROW-1"]["children"] == ["MARKDOWN-INTRO"]
    assert merged["MARKDOWN-INTRO"]["meta"] == {
        "code": OVERVIEW_MARKDOWN, "width": 12, "height": OVERVIEW_HEIGHT}
    assert "MARKDOWN-TAKEAWAYS" not in merged
    assert merged["GRID_ID"]["children"] == [
        "ROW-1", "ROW-INSIGHTS", *saved["GRID_ID"]["children"][1:]]
    for row_id in ("ROW-2", "ROW-3", "ROW-4", "ROW-5"):
        assert merged[row_id] == saved[row_id]
    for chart_id in (ids[name] for name in ids if name not in INSIGHT_NAMES):
        assert merged[f"CHART-{chart_id}"] == saved[f"CHART-{chart_id}"]
    assert _merge_overview_layout(merged_json, ids) == merged_json


def test_overview_icon_update_preserves_saved_chart_layout() -> None:
    ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    saved = json.loads(_layout(ids))
    saved["MARKDOWN-INTRO"]["meta"]["code"] = TAXI_ICON_OVERVIEW_MARKDOWN
    updated = json.loads(_merge_overview_layout(json.dumps(saved), ids))
    assert updated["MARKDOWN-INTRO"]["meta"]["code"] == OVERVIEW_MARKDOWN
    for key in saved.keys() - {"MARKDOWN-INTRO"}:
        assert updated[key] == saved[key]


def test_committed_dashboard_header_migrates_without_rearranging_charts() -> None:
    ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    saved = json.loads(_layout(ids))
    saved["MARKDOWN-INTRO"]["meta"]["code"] = PUBLISHED_OVERVIEW_MARKDOWN
    saved["GRID_ID"]["children"].remove("ROW-INSIGHTS")
    del saved["ROW-INSIGHTS"]
    for name in INSIGHT_NAMES:
        del saved[f"CHART-{ids[name]}"]
    saved["ROW-4"]["children"].reverse()

    updated = json.loads(_merge_overview_layout(json.dumps(saved), ids))
    assert updated["MARKDOWN-INTRO"]["meta"]["code"] == OVERVIEW_MARKDOWN
    assert updated["GRID_ID"]["children"][1] == "ROW-INSIGHTS"
    for row_id in ("ROW-2", "ROW-3", "ROW-4", "ROW-5"):
        assert updated[row_id] == saved[row_id]


def test_migration_refuses_to_delete_user_edited_takeaway_tile() -> None:
    ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    saved = json.loads(_layout(ids))
    saved["ROW-1"]["children"].append("MARKDOWN-TAKEAWAYS")
    saved["MARKDOWN-TAKEAWAYS"] = {
        "id": "MARKDOWN-TAKEAWAYS", "type": "MARKDOWN", "children": [],
        "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
        "meta": {"code": "User-edited takeaway text", "width": 6, "height": 28},
    }
    with pytest.raises(ValueError, match="refusing to remove"):
        _merge_overview_layout(json.dumps(saved), ids)


def test_retired_trip_insight_unlinks_only_this_dashboard() -> None:
    class Client:
        def __init__(self) -> None:
            self.updated = None

        def request(self, method, path, payload=None):
            assert path == "/api/v1/chart/99"
            if method == "GET":
                return {"result": {"slice_name": "Insight Total Trips",
                                   "dashboards": [{"id": 4}, {"id": 8}]}}
            self.updated = payload
            return {}

    client = Client()
    _unlink_legacy_insight(client, {"Insight Total Trips": 99}, 4)
    assert client.updated == {"dashboards": [8]}


def test_prior_four_takeaways_migrate_without_moving_original_chart_rows() -> None:
    ids = {spec.name: index for index, spec in enumerate(chart_specs(), start=1)}
    saved = json.loads(_layout(ids))
    saved["MARKDOWN-INTRO"]["meta"]["code"] = PREVIOUS_OVERVIEW_MARKDOWN
    saved["GRID_ID"]["children"][1] = "ROW-INSIGHTS"
    saved["ROW-INSIGHTS"]["children"].insert(0, "CHART-99")
    saved["CHART-99"] = {
        "id": "CHART-99", "type": "CHART", "children": [],
        "parents": ["ROOT_ID", "GRID_ID", "ROW-INSIGHTS"],
        "meta": {"chartId": 99, "sliceName": "Insight Total Trips",
                 "sliceNameOverride": "🚕 Valid Trips", "width": 3, "height": 22},
    }
    before = {key: saved[key] for key in ("ROW-2", "ROW-3", "ROW-4", "ROW-5")}
    merged = json.loads(_merge_overview_layout(json.dumps(saved), ids))
    assert "CHART-99" not in merged
    assert merged["ROW-INSIGHTS"]["children"] == [
        f"CHART-{ids[name]}" for name in INSIGHT_NAMES]
    assert {key: merged[key] for key in before} == before


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
    assert set(MART_NAMES) == {mart.name for mart in MARTS} | {"takeaway_breakdown"}
    names_by_mart = {mart.name: set(mart.names) for mart in MARTS}
    names_by_mart["takeaway_breakdown"] = {
        "_source_taxi_type", "_source_year", "_source_month", "borough",
        "pickup_hour", "payment_type", "trip_count",
    }
    assert len({spec.name for spec in specs}) == 13
    for spec in specs:
        fields = names_by_mart[spec.mart]
        if "x_axis" in spec.params:
            assert spec.params["x_axis"] in fields
        if spec.name == "Payment Type Trips":
            assert "payment_type" in fields
            assert spec.params["groupby"][0]["sqlExpression"] == PAYMENT_TYPE_LABEL_SQL
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
        "Pickup Trips by Borough", "Top Pickup Zones", *INSIGHT_NAMES)}
    assert set(chart_ids.values()) - set(filters[1]["scope"]["excluded"]) == expected_geographic


def test_unsupported_chart_fails_before_dashboard_api_mutation() -> None:
    specs = chart_specs()
    unsupported = (*specs[:-1], replace(specs[-1], viz_type="dist_bar"))
    with pytest.raises(ValueError, match="Unsupported dashboard definition"):
        validate_chart_specs(unsupported)
    payment = next(spec for spec in specs if spec.name == "Payment Type Trips")
    bad_groupby = tuple(replace(spec, params={"metric": {}}) if spec == payment
                        else spec for spec in specs)
    with pytest.raises(ValueError, match="lacks a category dimension"):
        validate_chart_specs(bad_groupby)
