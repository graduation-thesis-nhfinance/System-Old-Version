import logging
import requests
import urllib.parse
from typing import Dict, Any, List, Optional
from app.config import settings

logger = logging.getLogger(__name__)

class AirflowConnector:
    def __init__(self):
        self.base_url = settings.AIRFLOW_API_URL
        self.auth = (settings.AIRFLOW_USER, settings.AIRFLOW_PASSWORD)
        self.timeout = 2.5

    def get_health(self) -> Dict[str, Any]:
        """Check Airflow service health."""
        try:
            url = f"{self.base_url}/health"
            resp = requests.get(url, auth=self.auth, timeout=self.timeout)
            if resp.status_code == 200:
                data = resp.json()
                meta_status = data.get("metadatabase", {}).get("status", "unknown")
                sched_status = data.get("scheduler", {}).get("status", "unknown")
                return {
                    "online": meta_status == "healthy",
                    "status": "HEALTHY" if meta_status == "healthy" else "DEGRADED",
                    "details": data
                }
        except Exception as e:
            logger.debug("Airflow health check failed: %s", e)
        return {"online": False, "status": "OFFLINE", "error": "Cannot reach Airflow Webserver (8085)"}

    def list_dags(self) -> List[Dict[str, Any]]:
        """List all DAGs and their paused/active states."""
        try:
            url = f"{self.base_url}/dags?limit=100"
            resp = requests.get(url, auth=self.auth, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json().get("dags", [])
        except Exception as e:
            logger.error("Failed to list DAGs: %s", e)
        return []

    def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """Get single DAG metadata by dag_id."""
        try:
            url = f"{self.base_url}/dags/{dag_id}"
            resp = requests.get(url, auth=self.auth, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error("Failed to get DAG %s: %s", dag_id, e)
        return {}

    def get_dag_runs(self, dag_id: str, limit: int = 15) -> List[Dict[str, Any]]:
        """Get recent DAG runs for a specific DAG."""
        try:
            url = f"{self.base_url}/dags/{dag_id}/dagRuns?order_by=-execution_date&limit={limit}"
            resp = requests.get(url, auth=self.auth, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json().get("dag_runs", [])
        except Exception as e:
            logger.error("Failed to get dag runs for %s: %s", dag_id, e)
        return []

    def trigger_dag_run(self, dag_id: str, conf: Optional[Dict[str, Any]] = None, logical_date: Optional[str] = None) -> Dict[str, Any]:
        """Trigger a new DAG run with custom parameters (conf)."""
        try:
            url = f"{self.base_url}/dags/{dag_id}/dagRuns"
            payload: Dict[str, Any] = {"conf": conf or {}}
            if logical_date:
                payload["logical_date"] = logical_date
            resp = requests.post(url, auth=self.auth, json=payload, timeout=8)
            if resp.status_code in (200, 201):
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as e:
            logger.error("Failed to trigger DAG run %s: %s", dag_id, e)
            return {"success": False, "error": str(e)}

    def clear_task_instances(self, dag_id: str, task_ids: Optional[List[str]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Clear task instances to force Airflow to rerun them."""
        try:
            url = f"{self.base_url}/dags/{dag_id}/clearTaskInstances"
            payload: Dict[str, Any] = {
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False
            }
            if task_ids:
                payload["task_ids"] = task_ids
            if start_date:
                payload["start_date"] = start_date
            if end_date:
                payload["end_date"] = end_date
            resp = requests.post(url, auth=self.auth, json=payload, timeout=8)
            if resp.status_code == 200:
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as e:
            logger.error("Failed to clear task instances for %s: %s", dag_id, e)
            return {"success": False, "error": str(e)}

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> List[Dict[str, Any]]:
        """Get task instances for a DAG run."""
        try:
            encoded_run_id = urllib.parse.quote(dag_run_id, safe='')
            url = f"{self.base_url}/dags/{dag_id}/dagRuns/{encoded_run_id}/taskInstances"
            resp = requests.get(url, auth=self.auth, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json().get("task_instances", [])
        except Exception as e:
            logger.error("Failed to get task instances: %s", e)
        return []

    def get_dag_run_details(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get comprehensive live execution status for a specific DAG and DAG run."""
        encoded_run_id = urllib.parse.quote(dag_run_id, safe='')
        dag_paused = False
        dag_active = True
        timetable_desc = ""

        # 1. Check if DAG itself is paused
        try:
            dag_resp = requests.get(f"{self.base_url}/dags/{dag_id}", auth=self.auth, timeout=self.timeout)
            if dag_resp.status_code == 200:
                dag_data = dag_resp.json()
                dag_paused = dag_data.get("is_paused", False)
                dag_active = dag_data.get("is_active", True)
                timetable_desc = dag_data.get("timetable_description", "")
        except Exception as e:
            logger.error("Failed to get DAG details for %s: %s", dag_id, e)

        # 2. Query DagRun state
        run_data = {}
        try:
            run_resp = requests.get(f"{self.base_url}/dags/{dag_id}/dagRuns/{encoded_run_id}", auth=self.auth, timeout=self.timeout)
            if run_resp.status_code == 200:
                run_data = run_resp.json()
        except Exception as e:
            logger.error("Failed to get DagRun for %s %s: %s", dag_id, dag_run_id, e)

        # 3. Query Task Instances
        task_instances = []
        try:
            ti_resp = requests.get(f"{self.base_url}/dags/{dag_id}/dagRuns/{encoded_run_id}/taskInstances", auth=self.auth, timeout=self.timeout)
            if ti_resp.status_code == 200:
                task_instances = ti_resp.json().get("task_instances", [])
        except Exception as e:
            logger.error("Failed to get task instances: %s", e)

        state = run_data.get("state", "unknown")
        if dag_paused and state in ("queued", "unknown"):
            live_status = "PAUSED"
        elif state == "running":
            live_status = "IN_PROGRESS"
        elif state == "success":
            live_status = "SUCCESS"
        elif state == "failed":
            live_status = "FAILED"
        elif state == "queued":
            live_status = "QUEUED"
        else:
            live_status = state.upper()

        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "is_paused": dag_paused,
            "is_active": dag_active,
            "timetable_description": timetable_desc,
            "state": state,
            "live_status": live_status,
            "start_date": run_data.get("start_date"),
            "end_date": run_data.get("end_date"),
            "logical_date": run_data.get("logical_date"),
            "execution_date": run_data.get("execution_date"),
            "external_trigger": run_data.get("external_trigger", True),
            "conf": run_data.get("conf", {}),
            "task_summary": {
                "total": len(task_instances),
                "success": sum(1 for t in task_instances if t.get("state") == "success"),
                "running": sum(1 for t in task_instances if t.get("state") in ("running", "up_for_retry")),
                "failed": sum(1 for t in task_instances if t.get("state") in ("failed", "upstream_failed")),
                "queued": sum(1 for t in task_instances if t.get("state") in ("queued", "scheduled")),
                "skipped": sum(1 for t in task_instances if t.get("state") == "skipped")
            },
            "tasks": [
                {
                    "task_id": t.get("task_id"),
                    "state": t.get("state"),
                    "start_date": t.get("start_date"),
                    "end_date": t.get("end_date"),
                    "duration": t.get("duration"),
                    "try_number": t.get("try_number")
                }
                for t in task_instances
            ]
        }

airflow_connector = AirflowConnector()
