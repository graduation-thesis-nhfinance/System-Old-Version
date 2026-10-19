"""
SLA Miss & Pipeline Failure Notification Callback Hooks
"""

import logging
import requests
from typing import Dict, Any, Optional
from sys02_orchestration.config import config

logger = logging.getLogger(__name__)

def send_telegram_alert(message: str, level: str = "CRITICAL") -> bool:
    """Sends notification to Telegram via SYS-06 Bot API dispatcher."""
    token = config.alert.telegram_bot_token
    chat_id = config.alert.telegram_chat_id
    
    if not token or not chat_id or token.startswith("mock_"):
        logger.warning(f"Telegram Bot credentials not configured or in mock mode. Mocking alert [{level}]: {message}")
        return True


    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": f"[{level}] {message}",
        "parse_mode": "Markdown"
    }

    try:
        resp = requests.post(url, json=payload, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"Failed to deliver Telegram notification: {e}")
        return False

def on_task_failure_callback(context: Dict[str, Any]) -> None:
    """Airflow callback function triggered when a task fails. Extracts root-cause file & line number."""
    from sys02_orchestration.utils.error_tracer import ErrorTracer

    summary = ErrorTracer.generate_detailed_error_summary(context)
    logger.error(summary["banner_text"])

    tb = summary["tb_info"]
    msg = (
        f"🔴 *[AIRFLOW TASK FAILURE ROOT-CAUSE]*\n"
        f"• *DAG*: `{summary['dag_id']}`\n"
        f"• *Task*: `{summary['task_id']}`\n"
        f"• *File*: `{tb['file_path']}` (Line `{tb['line_no']}`)\n"
        f"• *Function*: `{tb['func_name']}()`\n"
        f"• *Code Line*: `{tb['code_snippet']}`\n"
        f"• *Exception*: `{tb['error_type']}: {tb['error_message']}`\n"
    )
    if summary.get("drift_report"):
        msg += f"\n{summary['drift_report']}\n"

    msg += f"\n• *Airflow UI*: http://localhost:{config.airflow.webserver_port}"
    
    send_telegram_alert(msg, level="CRITICAL")
    ErrorTracer.persist_error_to_metadata(summary)

def on_sla_miss_callback(dag: Any, task_list: str, blocking_task_list: str, slas: str, blocking_tis: str) -> None:
    """Airflow callback function triggered when an SLA threshold is breached."""
    msg = (
        f"⚠️ *[AIRFLOW SLA MISS WARNING]*\n"
        f"• *DAG*: `{getattr(dag, 'dag_id', 'unknown')}`\n"
        f"• *Tasks Impacted*: `{task_list}`\n"
        f"• *Blocking Task*: `{blocking_task_list}`\n"
        f"• *System SLA*: Exceeded target duration threshold."
    )
    logger.warning(msg)
    send_telegram_alert(msg, level="WARNING")
