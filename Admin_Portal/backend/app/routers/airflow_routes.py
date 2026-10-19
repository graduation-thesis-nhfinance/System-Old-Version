from datetime import datetime
from fastapi import APIRouter, Depends, Query
from app.connectors.airflow import airflow_connector
from app.auth import get_current_user
from app.routers.audit_routes import append_audit_log

router = APIRouter(prefix="/api/v1/airflow", tags=["Airflow Orchestrator"])

DAG_METADATA = {
    "01_binance_klines_etl_dag": {
        "tier": "Hourly",
        "tier_label": "Hourly Aggregation",
        "tier_badge": "HOURLY RESAMPLING & ETL",
        "category": "hourly",
        "icon": "fa-clock",
        "color": "blue",
        "description": "Hourly historical resampling and multi-timeframe aggregation: computes technical momentum indicators (RSI, MACD, Bollinger Bands) from Silver partitions.",
        "order": 1
    },
    "02_ml_model_retrain_dag": {
        "tier": "Daily",
        "tier_label": "Daily AI/ML Retrain",
        "tier_badge": "DAILY AI/ML MODEL RETRAIN",
        "category": "daily",
        "icon": "fa-brain",
        "color": "purple",
        "description": "Daily machine learning pipeline: extracts closed D-1 Gold feature vectors and retrains volatility & liquidity prediction models (LSTM / XGBoost).",
        "order": 2
    },
    "03_lakehouse_maintenance_dag": {
        "tier": "Weekly",
        "tier_label": "Weekly Maintenance",
        "tier_badge": "WEEKLY STORAGE & COMPACTION",
        "category": "weekly",
        "icon": "fa-screwdriver-wrench",
        "color": "amber",
        "description": "Weekly Lakehouse housekeeping: executes ClickHouse OPTIMIZE FINAL on partitioned Gold tables, compacts MinIO Bronze/Silver objects, and purges quarantine.",
        "order": 3
    },
    "04_binance_market_metrics_15m_dag": {
        "tier": "15M",
        "tier_label": "15-Min Ingestion",
        "tier_badge": "15-MIN NEAR-REALTIME",
        "category": "ingestion",
        "icon": "fa-bolt",
        "color": "emerald",
        "description": "Continuous 15-minute tumbling window: collects Spot & Futures market feeds (Klines, AggTrades, Depth 20, Liquidations), cross-reconciles with WSIS, and ingests into ClickHouse Gold.",
        "order": 4
    },
    "05_ai_models_hourly_inference_dag": {
        "tier": "Hourly",
        "tier_label": "Hourly AI Inference",
        "tier_badge": "HOURLY AI INFERENCE & RECONCILIATION",
        "category": "hourly",
        "icon": "fa-brain",
        "color": "purple",
        "description": "Automated Hourly AI Pipeline: executes multi-horizon LSTM price forecasting (t+15m, t+60m), Isolation Forest microstructure risk guard, and continuous 1-hour prediction drift reconciliation.",
        "order": 5
    }
}

@router.get("/dags")
def get_dags(current_user: dict = Depends(get_current_user)):
    raw_dags = airflow_connector.list_dags()
    enriched = []

    for d in raw_dags:
        dag_id = d.get("dag_id")
        meta = DAG_METADATA.get(dag_id, {
            "tier": "Batch",
            "tier_label": "Batch Pipeline",
            "tier_badge": "BATCH JOB",
            "category": "batch",
            "icon": "fa-cube",
            "color": "slate",
            "description": d.get("description") or "Airflow automated orchestration DAG.",
            "order": 99
        })

        # Fetch latest execution run
        latest_runs = airflow_connector.get_dag_runs(dag_id, limit=1)
        last_run_info = None
        if latest_runs:
            lr = latest_runs[0]
            start_iso = lr.get("start_date")
            end_iso = lr.get("end_date")
            duration_sec = None
            if start_iso and end_iso:
                try:
                    s_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                    e_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                    duration_sec = round((e_dt - s_dt).total_seconds(), 1)
                except Exception:
                    pass

            last_run_info = {
                "dag_run_id": lr.get("dag_run_id"),
                "state": lr.get("state") or "success",
                "execution_date": lr.get("execution_date") or lr.get("logical_date"),
                "start_date": start_iso,
                "end_date": end_iso,
                "duration_seconds": duration_sec,
                "run_type": lr.get("run_type") or "scheduled"
            }

        sched = d.get("schedule_interval")
        sched_str = sched.get("value") if isinstance(sched, dict) else (str(sched) if sched else "None")

        enriched.append({
            "dag_id": dag_id,
            "description": meta["description"],
            "tier": meta["tier"],
            "tier_label": meta["tier_label"],
            "tier_badge": meta["tier_badge"],
            "category": meta["category"],
            "icon": meta["icon"],
            "color": meta["color"],
            "order": meta["order"],
            "schedule_interval": sched_str,
            "is_paused": d.get("is_paused", False),
            "is_active": d.get("is_active", True),
            "last_run": last_run_info
        })

    enriched.sort(key=lambda x: x["order"])
    return {
        "dags": enriched,
        "total": len(enriched),
        "tiers_summary": {
            "near_realtime_15m": sum(1 for x in enriched if x["category"] == "ingestion"),
            "hourly_etl": sum(1 for x in enriched if x["category"] == "hourly"),
            "daily_ml": sum(1 for x in enriched if x["category"] == "daily"),
            "weekly_maintenance": sum(1 for x in enriched if x["category"] == "weekly")
        }
    }

@router.get("/dags/{dag_id}/runs")
def get_dag_runs(dag_id: str, limit: int = Query(10, ge=1, le=50), current_user: dict = Depends(get_current_user)):
    runs = airflow_connector.get_dag_runs(dag_id, limit=limit)
    return {"dag_runs": runs}

@router.get("/dags/{dag_id}/traces")
def get_dag_traces(dag_id: str, limit: int = Query(8, ge=1, le=30), current_user: dict = Depends(get_current_user)):
    meta = DAG_METADATA.get(dag_id, {
        "tier": "Batch",
        "tier_label": "Batch Pipeline",
        "tier_badge": "BATCH JOB",
        "category": "batch",
        "color": "slate"
    })
    
    raw_runs = airflow_connector.get_dag_runs(dag_id, limit=limit)
    traces = []
    
    for r in raw_runs:
        run_id = r.get("dag_run_id")
        start_iso = r.get("start_date")
        end_iso = r.get("end_date")
        duration_sec = None
        if start_iso and end_iso:
            try:
                s_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                e_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                duration_sec = round((e_dt - s_dt).total_seconds(), 1)
            except Exception:
                pass
        
        tis = airflow_connector.get_task_instances(dag_id, run_id) if run_id else []
        tasks_list = []
        for t in tis:
            tasks_list.append({
                "task_id": t.get("task_id"),
                "state": t.get("state") or "unknown",
                "start_date": t.get("start_date"),
                "end_date": t.get("end_date"),
                "duration": round(float(t.get("duration")), 2) if t.get("duration") is not None else None,
                "try_number": t.get("try_number", 1),
                "operator": t.get("operator", "PythonOperator")
            })
            
        traces.append({
            "dag_id": dag_id,
            "run_id": run_id,
            "state": r.get("state") or "unknown",
            "run_type": r.get("run_type") or "scheduled",
            "execution_date": r.get("execution_date") or r.get("logical_date"),
            "start_date": start_iso,
            "end_date": end_iso,
            "duration_seconds": duration_sec,
            "task_summary": {
                "total": len(tis),
                "success": sum(1 for t in tis if t.get("state") == "success"),
                "running": sum(1 for t in tis if t.get("state") in ("running", "up_for_retry")),
                "failed": sum(1 for t in tis if t.get("state") in ("failed", "upstream_failed")),
                "queued": sum(1 for t in tis if t.get("state") in ("queued", "scheduled")),
                "skipped": sum(1 for t in tis if t.get("state") == "skipped")
            },
            "tasks": tasks_list
        })
        
    return {
        "dag_id": dag_id,
        "tier": meta.get("tier"),
        "tier_badge": meta.get("tier_badge"),
        "color": meta.get("color"),
        "total_runs": len(traces),
        "traces": traces
    }

@router.get("/dags/{dag_id}/runs/{dag_run_id}/details")
def get_dag_run_details(dag_id: str, dag_run_id: str, current_user: dict = Depends(get_current_user)):
    return airflow_connector.get_dag_run_details(dag_id, dag_run_id)

@router.post("/dags/{dag_id}/trigger")
def trigger_dag(dag_id: str, current_user: dict = Depends(get_current_user)):
    user_name = current_user.get("username") or current_user.get("sub", "operator")
    res = airflow_connector.trigger_dag_run(dag_id, conf={"triggered_by": user_name})
    
    append_audit_log({
        "operator": user_name,
        "action": "MANUAL_DAG_TRIGGER",
        "target": dag_id,
        "scope": f"Airflow DAG: {dag_id}",
        "reason": f"Manual trigger dispatched via Admin Portal by {user_name}"
    })
    
    return res


