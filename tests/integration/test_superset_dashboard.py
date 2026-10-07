"""Optional real Superset dashboard/data-source smoke without UI automation."""

import json
import os

import pytest
from scripts.bootstrap_dashboard import SupersetClient, chart_specs, metric, query_context
from scripts.smoke_dashboard import smoke

pytestmark = [pytest.mark.integration, pytest.mark.docker, pytest.mark.heavy]


@pytest.mark.skipif(os.environ.get("RUN_SUPERSET_INTEGRATION") != "1",
                    reason="Set RUN_SUPERSET_INTEGRATION=1 with Superset running.")
def test_saved_dashboard_executes_all_serving_charts() -> None:
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    result = smoke(client)
    assert result["datasets"] == 6
    assert result["charts"] == 13
    assert all(count > 0 for count in result["query_rows"].values())


@pytest.mark.skipif(os.environ.get("RUN_SUPERSET_INTEGRATION") != "1",
                    reason="Set RUN_SUPERSET_INTEGRATION=1 with Superset running.")
def test_payment_chart_uses_readable_labels_without_changing_trip_totals() -> None:
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    chart_ids = {chart["slice_name"]: chart["id"] for chart in client.listed("chart")}
    payment_rows = client.request(
        "GET", f"/api/v1/chart/{chart_ids['Payment Type Trips']}/data/?force=true",
    )["result"][0]["data"]
    total_rows = client.request(
        "GET", f"/api/v1/chart/{chart_ids['Total Valid Trips']}/data/?force=true",
    )["result"][0]["data"]
    labels = {row["Payment method"] for row in payment_rows}
    assert {"Flex Fare trip", "Credit card", "Cash"} <= labels
    assert all(isinstance(label, str) and not label.isdigit() for label in labels)
    assert sum(row["Payment Trips"] for row in payment_rows) == total_rows[0]["Trips"]


@pytest.mark.skipif(os.environ.get("RUN_SUPERSET_INTEGRATION") != "1",
                    reason="Set RUN_SUPERSET_INTEGRATION=1 with Superset running.")
def test_takeaways_respond_to_month_and_borough_filters() -> None:
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    dataset = next(item for item in client.listed("dataset")
                   if item["table_name"] == "takeaway_breakdown")
    specs = {spec.name: spec for spec in chart_specs()}

    def rows(name: str, month: int, borough: str | None = None) -> list[dict]:
        context = json.loads(query_context(dataset["id"], specs[name].params))
        filters = [{"col": "_source_month", "op": "IN", "val": [month]}]
        if borough:
            filters.append({"col": "borough", "op": "IN", "val": [borough]})
        context["queries"][0]["filters"] = filters
        return client.request("POST", "/api/v1/chart/data", context)["result"][0]["data"]

    def filtered_trips(month: int, borough: str | None = None) -> int:
        context = json.loads(query_context(dataset["id"], {
            "metric": metric("trip_count", "Trips"), "time_range": "No filter",
        }))
        context["queries"][0]["filters"] = [
            {"col": "_source_month", "op": "IN", "val": [month]},
        ]
        if borough:
            context["queries"][0]["filters"].append(
                {"col": "borough", "op": "IN", "val": [borough]})
        return client.request("POST", "/api/v1/chart/data", context)["result"][0][
            "data"][0]["Trips"]

    january = filtered_trips(1)
    february = filtered_trips(2)
    queens = filtered_trips(1, "Queens")
    assert january == 2_927_000
    assert february == 2_966_785
    assert 0 < queens < january
    assert rows("Insight Top Borough", 1, "Queens")[0]["borough"] == "Queens"
    assert rows("Insight Busiest Hour", 1, "Queens")[0]["Pickup hour"]
    assert rows("Insight Top Payment", 1, "Queens")[0]["Payment method"] == "Credit card"
