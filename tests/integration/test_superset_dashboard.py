"""Optional real Superset dashboard/data-source smoke without UI automation."""

import json
import os

import psycopg2
import pytest
from psycopg2 import sql
from scripts.bootstrap_dashboard import SupersetClient, chart_specs, metric, query_context
from scripts.smoke_dashboard import smoke

from nyc_taxi_lakehouse.serving.config import ServingConfig

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


@pytest.mark.skipif(os.environ.get("RUN_SUPERSET_INTEGRATION") != "1",
                    reason="Set RUN_SUPERSET_INTEGRATION=1 with Superset running.")
def test_top_pickup_zones_ranks_filtered_complete_aggregate() -> None:
    """The first ten table rows match the serving mart, without a SQL row-limit hit."""
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    dataset = next(item for item in client.listed("dataset")
                   if item["table_name"] == "pickup_zone_performance")
    spec = next(spec for spec in chart_specs() if spec.name == "Top Pickup Zones")
    context = json.loads(query_context(dataset["id"], spec.params))
    assert context["queries"][0]["row_limit"] == 1000

    config = ServingConfig.from_env()
    with psycopg2.connect(**config.connect_kwargs()) as connection:
        with connection.cursor() as cursor:
            for month, borough in ((1, "Queens"), (6, "Manhattan")):
                context["queries"][0]["filters"] = [
                    {"col": "_source_month", "op": "IN", "val": [month]},
                    {"col": "borough", "op": "IN", "val": [borough]},
                ]
                result = client.request("POST", "/api/v1/chart/data", context)[
                    "result"][0]
                rows = result["data"]
                assert 10 < result["rowcount"] < 1000
                assert len(rows) == result["rowcount"]
                assert all(row["zone"] is not None for row in rows[:10])
                values = [row["Zone Pickups"] for row in rows]
                assert values == sorted(values, reverse=True)

                cursor.execute(sql.SQL(
                    "SELECT zone, SUM(trip_count) AS trips "
                    "FROM {}.pickup_zone_performance "
                    "WHERE _source_month = %s AND borough = %s "
                    "GROUP BY zone ORDER BY trips DESC"
                ).format(sql.Identifier(config.schema)), (month, borough))
                expected = cursor.fetchall()
                assert values[:10] == [trips for _, trips in expected[:10]]
                expected_by_zone = {zone: trips for zone, trips in expected}
                assert all(row["Zone Pickups"] == expected_by_zone[row["zone"]]
                           for row in rows[:10])
