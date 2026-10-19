"""Collect workbook-defined Binance metrics every 15 minutes (TaskFlow API)."""

from datetime import datetime, timedelta, timezone
import logging
import os
import sys
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import DagModel, DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.operators.market_metrics_operator import (
    BinanceMarketMetricsOperator,
    MarketMetricsPersistenceOperator,
)
from sys02_orchestration.utils.error_tracer import ErrorTracer

MARKET_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT")
LOGGER = logging.getLogger(__name__)
NOTIFY_TIMEZONE = "Asia/Ho_Chi_Minh"
MAX_CONSECUTIVE_FAILURES = 3


def send_sys06_alert(payload: dict) -> None:
    url = os.getenv("SYS06_ALERT_URL", "").strip()
    token = os.getenv("SYS06_ALERT_API_TOKEN", "").strip()
    if not url or not token:
        LOGGER.warning("SYS-06 notification skipped: dispatcher configuration is missing")
        return
    try:
        response = requests.post(url, json=payload, headers={"X-Alert-Token": token}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("SYS-06 notification could not be delivered: %s", exc)


def notify_hourly_collection_success(context: dict) -> None:
    """Notify after the second 15-minute run, then once each following hour."""
    logical_date = context.get("logical_date")
    if not logical_date:
        return
    run_time = logical_date.in_timezone(NOTIFY_TIMEZONE)
    if run_time.minute != 15:
        return
    dag_run = context.get("dag_run")
    send_sys06_alert(
        {
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "MARKET_METRICS_HOURLY_COLLECTION_COMPLETED",
            "deduplicate_key": f"sys02:market-metrics-hourly:{run_time.strftime('%Y%m%dT%H%M')}",
            "message": f"Two or more 15-minute market-metrics collections completed; hourly status for {run_time.strftime('%Y-%m-%d %H:%M %Z')} is ready.",
            "channels": ["telegram"],
            "action_url": f"http://127.0.0.1:8085/dags/{dag_run.dag_id}/grid",
        }
    )


default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": pendulum.datetime(2026, 8, 30, 22, 0, tz="Asia/Ho_Chi_Minh"),
}


@dag(
    dag_id="04_binance_market_metrics_15m_dag",
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    on_success_callback=notify_hourly_collection_success,
    tags=["binance", "15m", "market-metrics", "minio", "clickhouse", "taskflow", "circuit-breaker"],
)
def binance_market_metrics_15m_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    @task(task_id="extract_market_metrics", execution_timeout=timedelta(minutes=15))
    def extract_metrics_task() -> dict:
        """Extract multi-symbol spot ticker, orderbook, and liquidations from Binance."""
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        collector = BinanceMarketMetricsOperator(MARKET_SYMBOLS)
        active, unavailable = collector.available_symbols()
        rows = collector.collect_spot(active, now)
        liq_sec = int(os.getenv("LIQUIDATION_COLLECTION_SECONDS", "15"))
        rows["futures_liquidations"] = collector.collect_liquidations(liq_sec, now)
        key = datetime.fromtimestamp(now / 1000, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        persistence = MarketMetricsPersistenceOperator()
        bronze_paths = persistence.write_layer("bronze", key, rows)
        return {
            "run_key": key,
            "available_symbols": active,
            "unavailable_symbols": unavailable,
            "bronze_paths": bronze_paths,
        }

    @task(task_id="transform_market_metrics_silver")
    def transform_metrics_silver_task(extracted: dict) -> dict:
        """Process Bronze market metric payloads into normalized Silver Parquet partitions."""
        persistence = MarketMetricsPersistenceOperator()
        raw_data = persistence.read_layer(extracted["bronze_paths"])
        silver_paths = persistence.write_layer("silver", extracted["run_key"], raw_data)
        return {"silver_paths": silver_paths, "run_key": extracted["run_key"]}

    @task(task_id="load_market_metrics_gold")
    def load_metrics_gold_task(silver_res: dict) -> dict:
        """Ingest vetted Silver metrics into ClickHouse Gold analytics tables."""
        persistence = MarketMetricsPersistenceOperator()
        silver_data = persistence.read_layer(silver_res["silver_paths"])
        return persistence.load_gold(silver_data)

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
            LOGGER.error("fail_traces detected market metrics failure: %s in run: %s", ti.task_id, ti.run_id)
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

    end = EmptyOperator(task_id="end_pipeline")

    extracted_out = extract_metrics_task()
    silver_out = transform_metrics_silver_task(extracted_out)
    gold_out = load_metrics_gold_task(silver_out)

    traces = fail_traces_task()
    watcher = error_watcher_task()

    start >> extracted_out
    gold_out >> end

    [extracted_out, silver_out, gold_out] >> traces
    [gold_out, traces] >> watcher


market_metrics_dag = binance_market_metrics_15m_pipeline()
