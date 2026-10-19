import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.config import settings
from app.connectors.airflow import airflow_connector
from app.routers.audit_routes import append_audit_log
from app.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/rerun", tags=["Rerun & Reschedule"])

class RerunRequest(BaseModel):
    target_date: str
    time_window: str
    symbols: List[str]
    products: List[str]
    action: str = "rerun_slot"  # "rerun_slot", "reschedule", "clear_task", "batch_backfill"
    reason: Optional[str] = "Operator manual recovery"

def check_pipeline_permission(current_user: dict):
    role = current_user.get("role", "")
    perms = current_user.get("permissions", [])
    if role not in ["Super Admin", "Administrator"] and "trigger_pipeline" not in perms and "*" not in perms:
        raise HTTPException(status_code=403, detail="Permission denied: Requires 'trigger_pipeline' or Administrator role")

@router.post("/execute")
def execute_rerun(req: RerunRequest, current_user: dict = Depends(get_current_user)):
    """
    Executes a selective Rerun or Reschedule targeting specific trading pairs and products.
    """
    check_pipeline_permission(current_user)
    operator = current_user.get("username") or current_user.get("sub", settings.ADMIN_USERNAME)

    # Resolve symbols
    from app.routers.schedule_routes import get_registered_symbol_set
    registered_pairs = list(get_registered_symbol_set())
    if "all" in req.symbols or not req.symbols:
        resolved_symbols = registered_pairs
    else:
        # Validate symbols
        resolved_symbols = [s for s in req.symbols if s in registered_pairs]
        if not resolved_symbols:
            raise HTTPException(status_code=400, detail="No valid trading pairs specified.")

    # Resolve products
    valid_product_ids = [p["id"] for p in settings.PRODUCTS]
    if "all" in req.products or not req.products:
        resolved_products = valid_product_ids
    else:
        resolved_products = [p for p in req.products if p in valid_product_ids]
        if not resolved_products:
            raise HTTPException(status_code=400, detail="No valid products specified.")

    clean_date = req.target_date.replace("-", "")
    clean_time = req.time_window.replace(":", "") + "00" if ":" in req.time_window else "000000"
    run_key = f"{clean_date}T{clean_time}Z"

    conf = {
        "action": req.action,
        "run_key": run_key,
        "target_date": req.target_date,
        "target_window": req.time_window,
        "symbols": resolved_symbols,
        "products": resolved_products,
        "recollection": True,
        "operator": operator,
        "operator_username": operator,
        "reason": req.reason
    }

    dag_id = "04_binance_market_metrics_15m_dag"
    airflow_result = airflow_connector.trigger_dag_run(dag_id=dag_id, conf=conf)

    sym_summary = "All Pairs" if len(resolved_symbols) == len(settings.TRADING_PAIRS) else ", ".join(resolved_symbols)
    prod_summary = "All Products" if len(resolved_products) == len(valid_product_ids) else ", ".join(resolved_products)
    scope_str = f"{sym_summary} ({prod_summary})"

    # Create audit record
    audit_entry = {
        "operator": operator,
        "action": req.action.upper(),
        "target_dag": dag_id,
        "run_key": run_key,
        "scope": scope_str,
        "symbols": resolved_symbols,
        "products": resolved_products,
        "reason": f"Manual {req.action} by {operator}: {req.reason}",
        "airflow_dispatched": airflow_result.get("success", False),
        "airflow_response": airflow_result
    }
    append_audit_log(audit_entry)

    return {
        "status": "DISPATCHED" if airflow_result.get("success") else "QUEUED_LOCAL",
        "message": f"Successfully triggered {req.action} for {len(resolved_symbols)} symbols and {len(resolved_products)} products.",
        "details": {
            "operator": operator,
            "symbols": resolved_symbols,
            "products": resolved_products,
            "target_window": f"{req.target_date} {req.time_window}",
            "run_key": run_key,
            "airflow_trigger": airflow_result
        }
    }
