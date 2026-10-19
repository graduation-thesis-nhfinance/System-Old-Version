"""
Error Tracer & Value Drift Reporting Utility for Airflow Orchestration.
Captures exact file, line number, code snippet, and value change metrics on task failure.
"""

import sys
import os
import traceback
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class ErrorTracer:
    """Extracts granular error information from Airflow Task execution context."""

    @staticmethod
    def extract_traceback_details(exception: Exception) -> Dict[str, Any]:
        """
        Parses exception traceback, skipping internal Airflow/framework wrappers
        to isolate the exact project file and line number causing the failure.
        """
        if not exception:
            return {
                "file_path": "Unknown",
                "line_no": -1,
                "func_name": "Unknown",
                "code_snippet": "N/A",
                "error_type": "UnknownException",
                "error_message": "No exception object found in context.",
                "clean_traceback": ""
            }

        tb_list = traceback.extract_tb(exception.__traceback__)
        
        # Identify project frames (dags, src, plugins) vs framework internals
        project_frames = []
        for frame in tb_list:
            fn = frame.filename.replace("\\", "/")
            if any(marker in fn for marker in ["/dags/", "/src/", "/plugins/", "01_", "02_", "03_", "04_", "05_"]):
                project_frames.append(frame)

        # Fallback to the last available frame if no project frame matched
        target_frame = project_frames[-1] if project_frames else (tb_list[-1] if tb_list else None)

        if target_frame:
            file_path = target_frame.filename
            line_no = target_frame.lineno
            func_name = target_frame.name
            code_snippet = target_frame.line or "N/A"
        else:
            file_path = "Unknown"
            line_no = -1
            func_name = "Unknown"
            code_snippet = "N/A"

        error_type = type(exception).__name__
        error_message = str(exception).strip()

        # Format clean, readable traceback
        clean_lines = []
        for f in (project_frames if project_frames else tb_list):
            clean_lines.append(f'  File "{f.filename}", line {f.lineno}, in {f.name}\n    {f.line}')
        clean_tb = "\n".join(clean_lines)

        return {
            "file_path": file_path,
            "line_no": line_no,
            "func_name": func_name,
            "code_snippet": code_snippet,
            "error_type": error_type,
            "error_message": error_message,
            "clean_traceback": clean_tb
        }

    @staticmethod
    def format_value_drift_report(
        metrics: List[Dict[str, Any]]
    ) -> str:
        """
        Generates a structured report table for value changes / data drift.
        Each entry in metrics:
        {
            'metric': 'Total Volume',
            'baseline': 105400.0,
            'current': 12300.0,
            'unit': 'USDT',
            'threshold_pct': 15.0
        }
        """
        if not metrics:
            return ""

        report_lines = [
            "\n📊 [DATA DRIFT & VALUE CHANGE REPORT]",
            f"{'Metric':<25} | {'Baseline':<12} | {'Current':<12} | {'Drift %':<10} | {'Status':<10}",
            "-" * 75
        ]

        for m in metrics:
            name = m.get("metric", "Unknown")
            base = float(m.get("baseline", 0.0))
            curr = float(m.get("current", 0.0))
            unit = m.get("unit", "")
            thresh = float(m.get("threshold_pct", 10.0))

            if base != 0:
                drift_pct = ((curr - base) / base) * 100.0
            else:
                drift_pct = 100.0 if curr != 0 else 0.0

            status = "❌ DRIFT" if abs(drift_pct) > thresh else "✅ OK"
            base_str = f"{base:,.2f} {unit}".strip()
            curr_str = f"{curr:,.2f} {unit}".strip()
            drift_str = f"{drift_pct:+.2f}%"

            report_lines.append(f"{name:<25} | {base_str:<12} | {curr_str:<12} | {drift_str:<10} | {status:<10}")

        return "\n".join(report_lines)

    @classmethod
    def generate_detailed_error_summary(
        cls,
        context: Dict[str, Any],
        drift_metrics: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Generates an end-to-end error analysis summary containing root cause,
        line number, code line, and optional value change drift metrics.
        """
        ti = context.get("task_instance")
        dag = context.get("dag")
        exception = context.get("exception")

        task_id = ti.task_id if ti and hasattr(ti, "task_id") else "unknown_task"
        dag_id = dag.dag_id if dag and hasattr(dag, "dag_id") else "unknown_dag"
        exec_date = str(context.get("logical_date", context.get("execution_date", datetime.utcnow())))
        try_number = ti.try_number if ti and hasattr(ti, "try_number") else 1

        tb_info = cls.extract_traceback_details(exception)
        drift_report = cls.format_value_drift_report(drift_metrics) if drift_metrics else ""

        banner = (
            f"\n" + "=" * 80 + "\n"
            f"🚨 AIRFLOW TASK FAILURE ROOT CAUSE REPORT\n"
            f"=" * 80 + "\n"
            f"• DAG ID       : {dag_id}\n"
            f"• Task ID      : {task_id} (Attempt #{try_number})\n"
            f"• Execution At : {exec_date}\n"
            f"-" * 80 + "\n"
            f"📍 ROOT CAUSE LOCATION:\n"
            f"• File Path    : {tb_info['file_path']}\n"
            f"• Line Number  : {tb_info['line_no']}\n"
            f"• Function     : {tb_info['func_name']}()\n"
            f"• Code Snippet : {tb_info['code_snippet']}\n"
            f"• Error Type   : {tb_info['error_type']}\n"
            f"• Error Message: {tb_info['error_message']}\n"
            f"-" * 80 + "\n"
            f"📑 RELEVANT TRACEBACK:\n"
            f"{tb_info['clean_traceback']}\n"
            f"{drift_report}"
            f"=" * 80 + "\n"
        )

        return {
            "dag_id": dag_id,
            "task_id": task_id,
            "exec_date": exec_date,
            "banner_text": banner,
            "tb_info": tb_info,
            "drift_report": drift_report
        }

    @staticmethod
    def persist_error_to_metadata(
        error_summary: Dict[str, Any],
        pair_id: Optional[int] = 1,
        symbol: Optional[str] = "BTCUSDT",
        source: Optional[str] = "v1.0.0_api_klines",
        type_method: Optional[str] = "API"
    ) -> bool:
        """
        Persists the granular error into PostgreSQL public.data_quality_audits.
        """
        try:
            import psycopg2
            from sys02_orchestration.config import config
            
            pg_host = os.getenv("POSTGRES_METADATA_HOST", "127.0.0.1")
            pg_port = int(os.getenv("POSTGRES_METADATA_PORT", "5432"))
            pg_user = os.getenv("POSTGRES_METADATA_USER", "admin123")
            pg_pass = os.getenv("POSTGRES_METADATA_PASS", "admin123")
            pg_db = os.getenv("POSTGRES_METADATA_DB", "enterprise_metadata")

            conn = psycopg2.connect(
                host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db, connect_timeout=5
            )
            cur = conn.cursor()

            tb = error_summary.get("tb_info", {})
            err_msg_clean = f"[{tb.get('error_type')}] {tb.get('file_path')}:{tb.get('line_no')} - {tb.get('error_message')}"
            if len(err_msg_clean) > 500:
                err_msg_clean = err_msg_clean[:497] + "..."

            cur.execute("""
                INSERT INTO public.data_quality_audits (
                    batch_id, dataset_name, pair_id, symbol, layer,
                    check_time, is_passed, total_records, null_count,
                    duplicate_count, timestamp_gaps, error_summary,
                    source, type_method
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    NOW(), FALSE, 0, 0,
                    0, 0, %s,
                    %s, %s
                );
            """, (
                f"{error_summary.get('dag_id')}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                error_summary.get("task_id"),
                pair_id,
                symbol,
                "SILVER",
                err_msg_clean,
                source,
                type_method
            ))
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Successfully recorded task failure trace into public.data_quality_audits.")
            return True
        except Exception as e:
            logger.warning(f"Could not persist error trace to PostgreSQL metadata: {e}")
            return False
