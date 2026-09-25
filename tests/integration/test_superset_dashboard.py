"""Optional real Superset dashboard/data-source smoke without UI automation."""

import os

import pytest
from scripts.bootstrap_dashboard import SupersetClient
from scripts.smoke_dashboard import smoke

pytestmark = [pytest.mark.integration, pytest.mark.docker, pytest.mark.heavy]


@pytest.mark.skipif(os.environ.get("RUN_SUPERSET_INTEGRATION") != "1",
                    reason="Set RUN_SUPERSET_INTEGRATION=1 with Superset running.")
def test_saved_dashboard_executes_all_serving_charts() -> None:
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    result = smoke(client)
    assert result["datasets"] == 5
    assert result["charts"] == 10
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
