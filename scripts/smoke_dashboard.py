"""Prove the saved Superset dashboard executes all ten PostgreSQL-backed charts."""

from __future__ import annotations

import json
import logging
import os

from scripts.bootstrap_dashboard import DATABASE_NAME, MART_NAMES, TITLE, SupersetClient

LOGGER = logging.getLogger(__name__)


def smoke(client: SupersetClient) -> dict[str, object]:
    """Read metadata and chart-data endpoints; never change dashboard definitions."""
    databases = [item for item in client.listed("database")
                 if item["database_name"] == DATABASE_NAME]
    if len(databases) != 1:
        raise RuntimeError("Superset analytics database connection is missing or duplicated.")
    db_id = databases[0]["id"]
    datasets = [item for item in client.listed("dataset")
                if item.get("database", {}).get("id") == db_id
                and item.get("schema") == "analytics"]
    if {item["table_name"] for item in datasets} != set(MART_NAMES):
        raise RuntimeError("Superset datasets do not match the five serving marts.")
    dashboards = [item for item in client.listed("dashboard")
                  if item["dashboard_title"] == TITLE]
    if len(dashboards) != 1:
        raise RuntimeError("Expected one NYC Urban Mobility Overview dashboard.")
    dashboard_id = dashboards[0]["id"]
    detail = client.request("GET", f"/api/v1/dashboard/{dashboard_id}")["result"]
    if not detail.get("position_json"):
        raise RuntimeError("Superset dashboard has no saved chart layout.")
    filters = json.loads(detail.get("json_metadata") or "{}").get(
        "native_filter_configuration", [])
    if {item.get("name") for item in filters} != {"Source Month", "Pickup Borough"}:
        raise RuntimeError("Expected source-month and pickup-borough dashboard filters.")
    charts = client.request("GET", f"/api/v1/dashboard/{dashboard_id}/charts")["result"]
    if len(charts) != 10:
        raise RuntimeError(f"Dashboard has {len(charts)} charts rather than ten.")
    query_proof = {}
    for chart in charts:
        chart_id = chart["id"]
        payload = client.request("GET", f"/api/v1/chart/{chart_id}/data/?force=true")
        result = payload.get("result", [])
        if not result or result[0].get("rowcount", 0) < 1:
            raise RuntimeError(f"Saved Superset chart {chart_id} did not return rows.")
        query_proof[chart["slice_name"]] = result[0]["rowcount"]
        LOGGER.info("Superset query PASS: chart=%s rows=%s", chart["slice_name"],
                    result[0]["rowcount"])
    return {"dashboard_id": dashboard_id, "datasets": len(datasets),
            "charts": len(charts), "query_rows": query_proof}


def main() -> int:
    logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
    client = SupersetClient(os.environ.get("SUPERSET_URL", "http://superset:8088"))
    result = smoke(client)
    LOGGER.info("Superset dashboard smoke PASS: %s", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
