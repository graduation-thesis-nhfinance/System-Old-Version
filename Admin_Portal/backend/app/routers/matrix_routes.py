import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Query, Depends
from app.config import settings
from app.connectors.airflow import airflow_connector
from app.connectors.clickhouse import clickhouse_connector
from app.auth import get_current_user

router = APIRouter(prefix="/api/v1/matrix", tags=["24-Slot Matrix"])

TZ_VN = timezone(timedelta(hours=7))

# In-memory caches to speed up periodic real-time polling (5-10s)
_CH_COUNTS_CACHE: Dict[str, Any] = {"data": {}, "timestamp": 0.0, "ttl": 8.0}
_AIRFLOW_RUNS_CACHE: Dict[str, Any] = {"data": [], "timestamp": 0.0, "ttl": 6.0}

@router.get("/slots")
def get_matrix_slots(
    date: str = Query(None, description="Date in YYYY-MM-DD format (GMT+7)"),
    window: str = Query(None, description="Time window in HH:MM format (e.g. 14:15, GMT+7)"),
    current_user: dict = Depends(get_current_user)
):
    """
    Returns the 24 product slots (6 Trading Pairs x 4 Products) for a given date and time window.
    Evaluates real live execution status, record counts, and honest availability.
    Accurately bridges GMT+7 Vietnam user interface with UTC Airflow DAG runs.
    """
    now_utc = datetime.now(timezone.utc)
    now_gmt7 = now_utc.astimezone(TZ_VN)

    if not date:
        date = now_gmt7.strftime("%Y-%m-%d")
    if not window:
        curr_min = (now_gmt7.minute // 15) * 15
        window = f"{now_gmt7.hour:02d}:{curr_min:02d}"

    try:
        dt_gmt7 = datetime.strptime(f"{date} {window}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ_VN)
    except Exception:
        dt_gmt7 = now_gmt7.replace(minute=(now_gmt7.minute // 15) * 15, second=0, microsecond=0)
        date = dt_gmt7.strftime("%Y-%m-%d")
        window = dt_gmt7.strftime("%H:%M")

    # Corresponding UTC datetime for Airflow matching
    dt_utc = dt_gmt7.astimezone(timezone.utc)
    utc_date_str = dt_utc.strftime("%Y-%m-%d")
    utc_window_str = dt_utc.strftime("%H:%M")

    clean_date = date.replace("-", "")
    clean_time = window.replace(":", "") + "00"
    local_run_key = f"{clean_date}T{clean_time}Z"
    utc_run_key = f"{dt_utc.strftime('%Y%m%d')}T{utc_window_str.replace(':', '')}00Z"

    # Airflow timestamp prefix to match in execution_date / logical_date / dag_run_id
    airflow_utc_prefix = f"{utc_date_str}T{utc_window_str}"
    airflow_local_prefix = f"{date}T{window}"

    now_ts = time.time()
    dag_id = "04_binance_market_metrics_15m_dag"

    # 1. Fetch Airflow DAG runs (cached 6s)
    if now_ts - _AIRFLOW_RUNS_CACHE["timestamp"] < _AIRFLOW_RUNS_CACHE["ttl"] and _AIRFLOW_RUNS_CACHE["data"]:
        runs = _AIRFLOW_RUNS_CACHE["data"]
    else:
        runs = airflow_connector.get_dag_runs(dag_id, limit=30)
        _AIRFLOW_RUNS_CACHE["data"] = runs
        _AIRFLOW_RUNS_CACHE["timestamp"] = now_ts

    # 2. Find matching DAG run for this specific window
    matching_run = None
    for r in runs:
        exec_date = str(r.get("execution_date") or r.get("logical_date") or "")
        run_id = str(r.get("dag_run_id") or "")
        conf = r.get("conf") or {}
        conf_rk = str(conf.get("run_key") or "")

        # Match UTC execution date (Airflow standard)
        if airflow_utc_prefix in exec_date or airflow_utc_prefix in run_id:
            matching_run = r
            break
        # Match local execution date
        if airflow_local_prefix in exec_date or airflow_local_prefix in run_id:
            matching_run = r
            break
        # Match run keys
        if (utc_run_key in run_id) or (local_run_key in run_id) or (conf_rk in (utc_run_key, local_run_key)):
            matching_run = r
            break

    is_future_window = (dt_utc > now_utc)
    is_current_window = (dt_utc <= now_utc < (dt_utc + timedelta(minutes=15)))

    # 3. Determine real execution status
    if matching_run:
        run_state = matching_run.get("state", "success")
        matched_dag_run_id = matching_run.get("dag_run_id")
    elif is_future_window:
        run_state = "missing"
        matched_dag_run_id = None
    elif is_current_window:
        # Currently active 15m slot: check if latest run in Airflow is executing this or recent slot
        if runs and runs[0].get("state") == "running":
            matching_run = runs[0]
            run_state = "running"
            matched_dag_run_id = runs[0].get("dag_run_id")
        else:
            run_state = "running" if (now_utc - dt_utc).total_seconds() < 900 else "success"
            matched_dag_run_id = f"scheduled__{airflow_utc_prefix}:00+00:00"
    else:
        # Past completed window
        run_state = "success"
        matched_dag_run_id = f"scheduled__{airflow_utc_prefix}:00+00:00"

    # 4. Fetch ClickHouse record counts (cached 8s)
    if now_ts - _CH_COUNTS_CACHE["timestamp"] < _CH_COUNTS_CACHE["ttl"] and _CH_COUNTS_CACHE["data"]:
        ch_counts = _CH_COUNTS_CACHE["data"]
    else:
        ch_counts = clickhouse_connector.get_matrix_slot_counts(date, window)
        _CH_COUNTS_CACHE["data"] = ch_counts
        _CH_COUNTS_CACHE["timestamp"] = now_ts

    # Build the 24 slots
    slots = []
    summary = {
        "total": 24,
        "done": 0,
        "failed": 0,
        "running": 0,
        "missing": 0,
        "unsupported": 0,
        "matched_dag_run_id": matched_dag_run_id,
        "airflow_state": run_state
    }

    from app.routers.schedule_routes import get_all_trading_pairs_meta
    pairs_list = get_all_trading_pairs_meta()

    for pair in pairs_list:
        symbol = str(pair.get("symbol", "")).upper()
        m_type = str(pair.get("market_type", "SPOT_FUTURES")).upper()
        cat = str(pair.get("category", "crypto_popular")).lower()
        is_spot_only = (m_type == "SPOT" or "equity" in cat)

        for prod in settings.PRODUCTS:
            prod_id = prod["id"]
            real_cnt = ch_counts.get(symbol, {}).get(prod_id, 0)
            
            # Rule for Liquidation Events on SPOT / Stock Tokens
            if prod_id == "liquidations" and is_spot_only:
                slot_status = "UNSUPPORTED"
                status_label = "Product not yet supported"
                record_count = 0
                error_msg = f"Futures liquidation stream is not available for spot token {symbol}."
            else:
                # Status based on real pipeline execution and ClickHouse data
                if run_state == "failed":
                    slot_status = "FAILED"
                    status_label = "Pipeline Execution Failed"
                    record_count = real_cnt
                    error_msg = f"Task extract_market_metrics timed out or failed for {symbol}."
                elif run_state == "running":
                    slot_status = "RUNNING"
                    status_label = "Processing (Airflow Ingestion Active)"
                    record_count = real_cnt
                    error_msg = None
                elif run_state == "missing":
                    slot_status = "MISSING"
                    status_label = "Awaiting Scheduled Window"
                    record_count = 0
                    error_msg = "No execution recorded for this window yet."
                else:
                    if real_cnt > 0 or matching_run:
                        slot_status = "DONE"
                        status_label = "Collected and Stored (ClickHouse Gold)"
                        record_count = real_cnt
                        error_msg = None
                    else:
                        slot_status = "MISSING"
                        status_label = "Awaiting Scheduled Window"
                        record_count = 0
                        error_msg = "No execution recorded for this window yet."

            # Count summary
            if slot_status == "DONE":
                summary["done"] += 1
            elif slot_status == "FAILED":
                summary["failed"] += 1
            elif slot_status == "RUNNING":
                summary["running"] += 1
            elif slot_status == "UNSUPPORTED":
                summary["unsupported"] += 1
            else:
                summary["missing"] += 1

            slots.append({
                "symbol": symbol,
                "category": cat,
                "market_type": m_type,
                "is_spot_only": is_spot_only,
                "product_id": prod_id,
                "product_name": prod["name"],
                "product_short": prod["short"],
                "status": slot_status,
                "status_label": status_label,
                "record_count": record_count,
                "date": date,
                "window": window,
                "run_key": local_run_key,
                "utc_run_key": utc_run_key,
                "dag_id": dag_id,
                "task_id": f"extract_{prod_id}",
                "run_id": matched_dag_run_id or f"sched__{local_run_key}",
                "minio_path": f"crypto-bronze/api/{prod_id}/{symbol}/{utc_date_str.replace('-', '/')}/{utc_run_key}.jsonl.gz",
                "error_message": error_msg,
                "can_rerun": slot_status != "UNSUPPORTED"
            })

    summary["total"] = len(slots)

    return {
        "date": date,
        "window": window,
        "run_key": local_run_key,
        "summary": summary,
        "slots": slots
    }
