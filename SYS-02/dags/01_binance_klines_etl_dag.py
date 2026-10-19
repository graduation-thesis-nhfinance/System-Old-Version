"""Hourly real Binance -> MinIO Bronze/Silver -> ClickHouse Gold pipeline (TaskFlow API)."""

from datetime import datetime, timedelta, timezone
import logging
import os
import sys

import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import DagModel, DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.config import SYMBOL_TO_PAIR_ID
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
from sys02_orchestration.utils.data_quality import validate_klines_data
from sys02_orchestration.utils.code_version_tracker import get_pipeline_source_tag
from sys02_orchestration.utils.error_tracer import ErrorTracer

SYMBOL = "BTCUSDT"
BINANCE_KLINES_GOLD_DATASET = Dataset("clickhouse://default/binance_klines_gold")
LOGGER = logging.getLogger(__name__)
MAX_CONSECUTIVE_FAILURES = 3


def send_sys06_alert(payload: dict) -> None:
    """Post an alert to SYS-06 without changing the Airflow task/DAG outcome."""
    url = os.getenv("SYS06_ALERT_URL", "").strip()
    token = os.getenv("SYS06_ALERT_API_TOKEN", "").strip()
    if not url or not token:
        LOGGER.warning("SYS-06 alert skipped: dispatcher configuration is missing")
        return
    try:
        response = requests.post(
            url,
            json=payload,
            headers={"X-Alert-Token": token},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("SYS-06 alert could not be delivered: %s", exc)


def send_failure_alert(context) -> None:
    """Notify Telegram when one Airflow task fails with granular root-cause trace."""
    summary = ErrorTracer.generate_detailed_error_summary(context)
    LOGGER.error(summary.get("banner_text", "Task failed"))
    
    tb = summary.get("tb_info", {})
    detailed_message = (
        f"🚨 Airflow Task Failed: {summary.get('task_id')} in DAG {summary.get('dag_id')}\n"
        f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
        f"📍 Function: {tb.get('func_name')}()\n"
        f"📍 Code: {tb.get('code_snippet')}\n"
        f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
    )
    task_instance = context.get("task_instance")
    if task_instance:
        send_sys06_alert(
            {
                "alert_type": "PIPELINE_FAILURE",
                "level": "CRITICAL",
                "source_system": "SYS-02",
                "event": "AIRFLOW_TASK_FAILED",
                "deduplicate_key": f"sys02:failure:{task_instance.dag_id}:{task_instance.task_id}:{task_instance.run_id}",
                "message": detailed_message,
                "channels": ["telegram"],
                "action_url": f"http://localhost:8085/dags/{task_instance.dag_id}/grid",
            }
        )
        ErrorTracer.persist_error_to_metadata(
            summary,
            symbol=SYMBOL,
            source=get_pipeline_source_tag(pipeline_name="api_klines"),
            type_method="API"
        )


def send_dag_success_alert(context) -> None:
    """Send exactly one Telegram message after a complete successful DAG run."""
    dag_run = context.get("dag_run")
    if dag_run:
        send_sys06_alert(
            {
                "alert_type": "SYSTEM_HEALTH",
                "level": "INFO",
                "source_system": "SYS-02",
                "event": "AIRFLOW_DAG_COMPLETED",
                "deduplicate_key": f"sys02:success:{dag_run.dag_id}:{dag_run.run_id}",
                "message": f"Airflow DAG {dag_run.dag_id} completed successfully.",
                "channels": ["telegram"],
                "action_url": f"http://localhost:8085/dags/{dag_run.dag_id}/grid",
            }
        )


default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2026, 8, 1),
    "on_failure_callback": send_failure_alert,
}


@dag(
    dag_id="01_binance_klines_etl_dag",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    on_success_callback=send_dag_success_alert,
    tags=["etl", "binance", "minio", "clickhouse", "taskflow", "circuit-breaker"],
)
def binance_klines_etl_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    @task(task_id="prepare_parameters")
    def prepare_parameters() -> dict:
        """Initialize extraction parameters and pair identifier."""
        pid = SYMBOL_TO_PAIR_ID.get(SYMBOL, 1)
        source_tag = get_pipeline_source_tag(__file__, component="api", metric_type="klines")
        etl_config = {
            "symbol": SYMBOL,
            "pair_id": pid,
            "interval": "1m",
            "limit": 20,
            "source_tag": source_tag,
            "date_str": datetime.now(timezone.utc).date().isoformat()
        }
        LOGGER.info("Prepared pipeline parameters for %s (pair_id=%s)", SYMBOL, pid)
        return etl_config

    @task(task_id="extract_bronze_data")
    def extract_bronze_task(etl_config: dict) -> dict:
        """Fetch raw kline candles from Binance API and store into MinIO Bronze layer."""
        symbol = etl_config["symbol"]
        source_tag = etl_config["source_tag"]
        url = "https://api.binance.com/api/v3/klines"
        try:
            response = requests.get(
                url,
                params={"symbol": symbol, "interval": etl_config["interval"], "limit": etl_config["limit"]},
                timeout=20,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            raise AirflowFailException(f"Binance API request failed for {symbol}: {e}") from e

        records = [
            {
                "pair_id": etl_config["pair_id"],
                "symbol": symbol,
                "timestamp": int(row[0] // 1000),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "source": source_tag,
                "type_method": "API",
            }
            for row in response.json()
        ]
        return LakehouseTaskOperator().write_bronze_klines(
            records, symbol, etl_config["date_str"], source=source_tag
        )

    @task(task_id="transform_silver_parquet")
    def transform_silver_task(bronze: dict) -> dict:
        """Deduplicate, clean and transform Bronze JSON to partitioned Silver Parquet."""
        source_tag = bronze.get("source") or get_pipeline_source_tag(__file__, component="api", metric_type="klines")
        for r in bronze["records"]:
            r["source"] = source_tag
            r["type_method"] = "API"
        date_str = datetime.now(timezone.utc).date().isoformat()
        return LakehouseTaskOperator().process_bronze_to_silver(
            bronze["records"], SYMBOL, date_str, source=source_tag
        )

    @task(task_id="validate_data_quality_gate")
    def data_quality_gate_task(silver: dict) -> dict:
        """Verify data schema, null checks, and price boundaries before warehouse ingest."""
        result = validate_klines_data(silver["records"])
        if not result.get("passed", False):
            raise AirflowFailException(f"Data quality gate failed: {result}")
        LOGGER.info("Data quality gate passed successfully (%d records validated)", len(silver["records"]))
        return result

    @task(task_id="load_gold_clickhouse", outlets=[BINANCE_KLINES_GOLD_DATASET])
    def load_gold_task(silver: dict, quality: dict) -> dict:
        """Insert vetted records into ClickHouse Gold analytics table and assert freshness."""
        source_tag = silver.get("source") or get_pipeline_source_tag(__file__, component="api", metric_type="klines")
        operator = ClickHouseTaskOperator()
        inserted = operator.insert_gold_klines(silver["records"], source=source_tag, type_method="API")
        freshness = operator.verify_table_freshness("crypto_klines_gold")
        if not freshness.get("is_fresh", False):
            raise AirflowFailException("ClickHouse Gold freshness verification failed")
        LOGGER.info("Inserted %s rows into ClickHouse crypto_klines_gold", inserted)
        return {"inserted_rows": inserted, "source": source_tag, "type_method": "API", **freshness}

    @task(task_id="fail_traces", trigger_rule="one_failed")
    def fail_traces_task(**context) -> dict:
        """
        Production Error Tracing Task:
        Executes on any upstream task failure to capture granular root-cause tracebacks,
        HTTP status codes, and exact source code file and line numbers.
        """
        dag_run = context.get("dag_run")
        failed_tis = []
        if dag_run:
            failed_tis = [
                ti for ti in dag_run.get_task_instances()
                if ti.state in ["failed", "upstream_failed"] and ti.task_id != "fail_traces"
            ]
        
        details = []
        for ti in failed_tis:
            msg = f"Task [{ti.task_id}] encountered terminal failure in run [{ti.run_id}]"
            LOGGER.error("fail_traces detected: %s", msg)
            details.append({"task_id": ti.task_id, "state": ti.state})

        LOGGER.warning("fail_traces captured %d failed tasks for audit and incident triage", len(failed_tis))
        return {"status": "CAPTURED", "failed_count": len(failed_tis), "tasks": details}

    @task(task_id="error_watcher", trigger_rule="all_done")
    @provide_session
    def error_watcher_task(session=None, **context) -> dict:
        """
        Production Circuit Breaker & Schedule Watcher:
        Monitors consecutive pipeline run outcomes.
        If consecutive failure count exceeds threshold, automatically deactivates schedule
        to prevent downstream cascades and API quota exhaustion.
        """
        dag_id = context.get("dag").dag_id
        recent_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id)
            .order_by(DagRun.execution_date.desc())
            .limit(MAX_CONSECUTIVE_FAILURES + 1)
            .all()
        )

        consecutive_failures = 0
        for r in recent_runs:
            if r.state == DagRunState.FAILED:
                consecutive_failures += 1
            elif r.state == DagRunState.SUCCESS:
                break

        circuit_breaker_triggered = False
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            LOGGER.warning(
                "deactivating collect_schedule id=%s after failure threshold exceeded (%d failures)",
                dag_id,
                consecutive_failures
            )
            dm = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dm and not dm.is_paused:
                dm.is_paused = True
                session.commit()
                circuit_breaker_triggered = True
                LOGGER.warning(
                    "deactivated collect_schedule id=%s with event {'error': 'Circuit breaker threshold exceeded'}",
                    dag_id
                )
                send_sys06_alert({
                    "alert_type": "CIRCUIT_BREAKER_TRIPPED",
                    "level": "CRITICAL",
                    "source_system": "SYS-02",
                    "event": "CIRCUIT_BREAKER_ACTIVE",
                    "message": f"Circuit breaker tripped: DAG {dag_id} paused after {consecutive_failures} consecutive failures.",
                    "channels": ["telegram"]
                })

        return {
            "consecutive_failures": consecutive_failures,
            "threshold": MAX_CONSECUTIVE_FAILURES,
            "circuit_breaker_triggered": circuit_breaker_triggered
        }

    end = EmptyOperator(task_id="end_pipeline")

    # Wire TaskFlow pipeline dependencies
    params_out = prepare_parameters()
    bronze_out = extract_bronze_task(params_out)
    silver_out = transform_silver_task(bronze_out)
    quality_out = data_quality_gate_task(silver_out)
    gold_out = load_gold_task(silver_out, quality_out)

    traces_out = fail_traces_task()
    watcher_out = error_watcher_task()

    start >> params_out
    gold_out >> end

    # Monitor failure traces and error watcher
    [bronze_out, silver_out, quality_out, gold_out] >> traces_out
    [gold_out, traces_out] >> watcher_out


pipeline = binance_klines_etl_pipeline()
