"""Monthly control-plane DAG for the existing NYC Taxi data-plane processors."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _run_stage(stage: str, **context: object) -> dict[str, object]:
    """Import processing code only during task execution, never during DAG parsing."""
    from nyc_taxi_lakehouse.orchestration.airflow_stages import execute_stage, processing_period

    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    params = context["params"]
    taxi_type = conf.get("taxi_type", params["taxi_type"])
    backend = conf.get("storage_backend", params["storage_backend"])
    period = processing_period(context["data_interval_start"], conf.get("period"))
    result = execute_stage(stage, period, taxi_type, backend)
    if stage == "publish_iceberg" and result.get("skipped"):
        raise AirflowSkipException("Filesystem mode has no Iceberg publication.")
    return {"period": period.identifier, "taxi_type": taxi_type,
            "storage_backend": backend, **result}


with DAG(
    dag_id="nyc_taxi_monthly_lakehouse",
    description="Monthly NYC Taxi ingestion, quality, analytics and optional Iceberg publication",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    params={"taxi_type": "yellow", "storage_backend": "filesystem"},
    tags=["nyc-taxi", "lakehouse"],
) as dag:
    tasks = {}
    for stage in (
        "ingest_raw", "validate_schema", "bronze", "silver", "gold",
        "geographic", "publish_iceberg", "validate_reconciliation",
    ):
        retries = 2 if stage == "ingest_raw" else 0 if stage == "validate_schema" else 1
        tasks[stage] = PythonOperator(
            task_id=stage,
            python_callable=_run_stage,
            op_kwargs={"stage": stage},
            retries=retries,
            retry_delay=timedelta(minutes=2),
            trigger_rule=(TriggerRule.NONE_FAILED if stage == "validate_reconciliation"
                          else TriggerRule.ALL_SUCCESS),
        )
    for upstream, downstream in zip(tuple(tasks)[:-1], tuple(tasks)[1:], strict=True):
        tasks[upstream] >> tasks[downstream]
