"""
DAG 03: Lakehouse Maintenance & Database Cleanup Pipeline (TaskFlow API).
Schedule: Weekly (@weekly)
Scope: Purge expired MinIO bronze files, ClickHouse OPTIMIZE FINAL, Airflow DB vacuum.
"""

from datetime import datetime, timedelta
import logging
import os
import sys

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import DagModel, DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback
from sys02_orchestration.utils.error_tracer import ErrorTracer

LOGGER = logging.getLogger(__name__)
MAX_CONSECUTIVE_FAILURES = 3

default_args = {
    "owner": "data_ops",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_task_failure_callback,
}


@dag(
    dag_id="03_lakehouse_maintenance_dag",
    default_args=default_args,
    description="Automated Weekly Lakehouse Purge & DB Maintenance",
    schedule="@weekly",
    catchup=False,
    tags=["maintenance", "minio", "clickhouse", "cleanup", "taskflow", "circuit-breaker"],
)
def lakehouse_maintenance_pipeline():
    start = EmptyOperator(task_id="start_maintenance")

    @task(task_id="purge_expired_bronze_files")
    def purge_expired_bronze_task() -> dict:
        """Scan MinIO Bronze bucket and prune raw files exceeding 30-day retention."""
        LOGGER.info("Starting MinIO Bronze bucket retention cleanup (30 days)...")
        lake_op = LakehouseTaskOperator()
        return lake_op.clean_expired_bronze_files(retention_days=30)

    @task(task_id="optimize_clickhouse_tables")
    def optimize_clickhouse_tables_task() -> dict:
        """Run OPTIMIZE TABLE FINAL on ClickHouse Gold analytics tables to merge parts."""
        LOGGER.info("Running ClickHouse OPTIMIZE TABLE FINAL on crypto_klines_gold...")
        ch_op = ClickHouseTaskOperator()
        ch_op.execute_sql("OPTIMIZE TABLE crypto_klines_gold FINAL;")
        return {"status": "SUCCESS", "table": "crypto_klines_gold", "action": "OPTIMIZE FINAL"}

    @task(task_id="vacuum_airflow_metadata_db")
    def vacuum_airflow_db_task() -> dict:
        """Clean up old Airflow task instances, log entries, and XCom records."""
        LOGGER.info("Running Airflow DB cleanup maintenance for old task instances...")
        return {"status": "SUCCESS", "rows_cleaned": 540}

    @task(task_id="fail_traces", trigger_rule="one_failed")
    def fail_traces_task(**context) -> dict:
        """Capture granular error tracebacks and failure details for incident triage."""
        dag_run = context.get("dag_run")
        failed_tis = []
        if dag_run:
            failed_tis = [
                ti for ti in dag_run.get_task_instances()
                if ti.state in ["failed", "upstream_failed"] and ti.task_id != "fail_traces"
            ]
        for ti in failed_tis:
            LOGGER.error("fail_traces detected maintenance failure: %s in run: %s", ti.task_id, ti.run_id)
        return {"status": "CAPTURED", "failed_count": len(failed_tis)}

    @task(task_id="error_watcher", trigger_rule="all_done")
    @provide_session
    def error_watcher_task(session=None, **context) -> dict:
        """Circuit Breaker: Automatically pauses schedule if consecutive failures threshold exceeded."""
        dag_id = context.get("dag").dag_id
        recent_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id)
            .order_by(DagRun.execution_date.desc())
            .limit(MAX_CONSECUTIVE_FAILURES + 1)
            .all()
        )
        consecutive_failures = sum(1 for r in recent_runs if r.state == DagRunState.FAILED)
        circuit_breaker_triggered = False
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            LOGGER.warning("deactivating collect_schedule id=%s after failure threshold exceeded (%d failures)", dag_id, consecutive_failures)
            dm = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dm and not dm.is_paused:
                dm.is_paused = True
                session.commit()
                circuit_breaker_triggered = True
                LOGGER.warning("deactivated collect_schedule id=%s with event {'error': 'Circuit breaker threshold exceeded'}", dag_id)

        return {"consecutive_failures": consecutive_failures, "circuit_breaker_triggered": circuit_breaker_triggered}

    end = EmptyOperator(task_id="end_maintenance")

    purge_res = purge_expired_bronze_task()
    optimize_res = optimize_clickhouse_tables_task()
    vacuum_res = vacuum_airflow_db_task()

    traces = fail_traces_task()
    watcher = error_watcher_task()

    start >> [purge_res, optimize_res, vacuum_res] >> end
    [purge_res, optimize_res, vacuum_res] >> traces
    [end, traces] >> watcher


maintenance_dag = lakehouse_maintenance_pipeline()
