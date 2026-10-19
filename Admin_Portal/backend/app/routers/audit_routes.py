import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException
from app.connectors.airflow import airflow_connector
from app.auth import get_current_user

router = APIRouter(prefix="/api/v1/audit", tags=["Audit Log"])

AUDIT_LOG_FILE = Path(__file__).resolve().parent.parent / "data" / "audit_log.json"
AUDIT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

def append_audit_log(entry: Dict[str, Any]):
    logs = []
    if AUDIT_LOG_FILE.exists():
        try:
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except Exception:
            logs = []
    
    entry["id"] = f"aud_{int(time.time()*1000)}"
    entry["timestamp"] = datetime.now(timezone.utc).isoformat()
    logs.insert(0, entry)  # Prepend latest
    
    # Keep last 500 audit logs
    logs = logs[:500]
    with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2)

@router.get("/logs")
def get_audit_logs(limit: int = 60, current_user: dict = Depends(get_current_user)):
    logs = []
    if AUDIT_LOG_FILE.exists():
        try:
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except Exception:
            logs = []

    # Merge Airflow automated scheduled runs
    try:
        dag_runs = airflow_connector.get_dag_runs("04_binance_market_metrics_15m_dag", limit=20)
        existing_run_keys = {str(l.get("run_key")) for l in logs if l.get("run_key")}
        existing_ids = {str(l.get("id")) for l in logs}

        for run in dag_runs:
            dag_run_id = str(run.get("dag_run_id", ""))
            clean_key = dag_run_id.replace("scheduled__", "").replace("manual__", "")
            conf = run.get("conf") or {}
            manual_op = conf.get("operator") or conf.get("operator_username")

            # Determine operator:
            # - If manual rerun with credentials, operator is their username
            # - If automated by Airflow scheduler, operator is "API/WSIS system"
            if manual_op:
                operator = manual_op
                action = "MANUAL_RERUN"
                reason = conf.get("reason", f"Manual recovery rerun by {manual_op}")
                is_auto = False
            elif dag_run_id.startswith("manual__"):
                operator = "nhfinanceadmin"
                action = "MANUAL_RERUN"
                reason = "Operator manual recovery dispatch"
                is_auto = False
            else:
                operator = "API/WSIS system"
                action = "AUTO_15M_TUMBLING_RUN"
                reason = "Automated clock-aligned 15-minute tumbling ingestion by Airflow"
                is_auto = True

            # If already logged via manual action, skip duplicate
            if clean_key in existing_run_keys or dag_run_id in existing_run_keys or f"aud_airflow_{dag_run_id}" in existing_ids:
                continue

            exec_date = run.get("execution_date") or run.get("start_date") or datetime.now(timezone.utc).isoformat()
            auto_entry = {
                "id": f"aud_airflow_{dag_run_id}",
                "timestamp": exec_date,
                "operator": operator,
                "action": action,
                "target_dag": "04_binance_market_metrics_15m_dag",
                "run_key": clean_key,
                "scope": "All 6 Pairs (API & WSIS Stream)",
                "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"],
                "products": ["klines", "aggtrade", "depth", "liquidations"],
                "reason": reason,
                "airflow_dispatched": True,
                "airflow_response": {"data": run},
                "is_automated": is_auto
            }
            logs.append(auto_entry)
    except Exception as e:
        pass

    # Sort logs by timestamp descending (newest first)
    logs.sort(key=lambda l: str(l.get("timestamp", "")), reverse=True)
    return {"logs": logs[:limit]}

@router.get("/status/{audit_id}")
def get_audit_airflow_status(audit_id: str, current_user: dict = Depends(get_current_user)):
    logs_resp = get_audit_logs(limit=200, current_user=current_user)
    logs = logs_resp.get("logs", [])

    target_log = next((l for l in logs if l.get("id") == audit_id), None)
    if not target_log:
        raise HTTPException(status_code=404, detail="Audit log entry not found")

    dag_id = target_log.get("target_dag", "04_binance_market_metrics_15m_dag")
    dag_run_id = target_log.get("airflow_response", {}).get("data", {}).get("dag_run_id")

    if not dag_run_id:
        return {
            "audit_id": audit_id,
            "dag_id": dag_id,
            "dag_run_id": None,
            "live_status": "DISPATCHED" if target_log.get("airflow_dispatched") else "QUEUED_LOCAL",
            "state": "unknown",
            "is_paused": False,
            "is_active": True,
            "audit_entry": target_log,
            "live_details": None
        }

    live_details = airflow_connector.get_dag_run_details(dag_id=dag_id, dag_run_id=dag_run_id)
    return {
        "audit_id": audit_id,
        "audit_entry": target_log,
        "live_details": live_details
    }

@router.post("/log-external-visit")
def log_external_visit(body: Dict[str, Any], current_user: dict = Depends(get_current_user)):
    system_code = body.get("system_code", "UNKNOWN")
    system_name = body.get("system_name", "External System")
    target_url = body.get("target_url", "")
    operator = current_user.get("sub", "Unknown Operator")
    entry = {
        "operator": operator,
        "action": "NAVIGATE_SYSTEM_LINK",
        "target_dag": system_code,
        "run_key": system_code,
        "scope": f"External Link: {system_name}",
        "reason": f"Security & Auditing: Operator accessed external system portal [{system_name}] ({target_url})",
        "airflow_dispatched": False,
        "is_automated": False
    }
    append_audit_log(entry)
    return {"status": "success", "entry": entry}

