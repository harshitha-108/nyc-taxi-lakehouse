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
