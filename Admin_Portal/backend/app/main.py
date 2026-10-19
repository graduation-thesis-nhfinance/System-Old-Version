import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.config import settings
from app.connectors.airflow import airflow_connector
from app.connectors.clickhouse import clickhouse_connector
from app.connectors.minio_client import minio_connector
from app.connectors.wsis import wsis_connector

from app.routers.auth_routes import router as auth_router
from app.routers.matrix_routes import router as matrix_router
from app.routers.rerun_routes import router as rerun_router
from app.routers.airflow_routes import router as airflow_router
from app.routers.lakehouse_routes import router as lakehouse_router
from app.routers.wsis_routes import router as wsis_router
from app.routers.audit_routes import router as audit_router
from app.routers.schedule_routes import router as schedule_router
from app.routers.trading_pair_routes import router as trading_pair_router
from app.routers.catalog_routes import router as catalog_router
from app.routers.profile_routes import router as profile_router
from app.routers.notification_routes import router as notification_router
from app.routers.ai_models_routes import router as ai_models_router

app = FastAPI(
    title="NHFinance Admin Portal - Ecosystem Command Center",
    description="Central Orchestration & Operations Gateway for Airflow, ClickHouse, MinIO, WSIS, and SYS-04 AI Models",
    version="1.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routers
app.include_router(auth_router)
app.include_router(matrix_router)
app.include_router(rerun_router)
app.include_router(airflow_router)
app.include_router(lakehouse_router)
app.include_router(wsis_router)
app.include_router(audit_router)
app.include_router(schedule_router)
app.include_router(trading_pair_router)
app.include_router(catalog_router)
app.include_router(profile_router)
app.include_router(notification_router)
app.include_router(ai_models_router)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "app": "admin-portal"}

@app.get("/readyz")
def readyz():
    ws_health = wsis_connector.check_health()
    ch_health = clickhouse_connector.get_health()
    return {
        "status": "ready",
        "airflow": airflow_connector.get_health(),
        "clickhouse": ch_health,
        "minio": minio_connector.get_health(),
        "wsis": ws_health,
        "ai_models": {"status": "ONLINE" if ch_health.get("online", False) else "OFFLINE", "subsystem": "SYS-04", "count": 3}
    }

import time
import socket
import urllib.request

def _probe_http(url: str, timeout: float = 1.5):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NHFinance-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency = max(1, round((time.time() - t0) * 1000))
            return True, latency
    except Exception:
        latency = round((time.time() - t0) * 1000)
        return False, latency

def _probe_tcp(host: str, port: int, timeout: float = 1.0):
    t0 = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency = max(1, round((time.time() - t0) * 1000))
            return True, latency
    except Exception:
        return False, None

@app.get("/api/v1/system/status")
def get_system_status():
    """Quick scan of all connected subsystems with real health probes."""
    af = airflow_connector.get_health()
    ch = clickhouse_connector.get_health()
    mn = minio_connector.get_health()
    ws_health = wsis_connector.check_health()

    # Realtime probes for all registered system links
    s1_ok, s1_lat = _probe_http("http://host.docker.internal:8501", timeout=0.6)
    if not s1_ok:
        s1_ok, s1_lat = _probe_tcp("host.docker.internal", 8501, timeout=0.4)

    s2_ok = af.get("online", False)
    s2_lat = 25 if s2_ok else None
    if not s2_ok:
        s2_ok, s2_lat = _probe_http("http://sys02_airflow_webserver:8080/health", timeout=0.8)
    if not s2_ok:
        s2_ok, s2_lat = _probe_http("http://host.docker.internal:8085/health", timeout=0.8)
    if not s2_ok:
        s2_ok, s2_lat = _probe_tcp("sys02_airflow_webserver", 8080, timeout=0.5)

    s3_graf_ok, s3_graf_lat = _probe_http("http://host.docker.internal:3000/api/health", timeout=0.6)
    if not s3_graf_ok:
        s3_graf_ok, s3_graf_lat = _probe_tcp("host.docker.internal", 3000, timeout=0.4)

    s3_prom_ok, s3_prom_lat = _probe_http("http://host.docker.internal:9090/-/healthy", timeout=0.6)
    if not s3_prom_ok:
        s3_prom_ok, s3_prom_lat = _probe_tcp("host.docker.internal", 9090, timeout=0.4)

    s4_fastapi_ok, s4_fastapi_lat = _probe_http("http://nhfinance_sys04_ai_models:8000/docs", timeout=0.8)
    if not s4_fastapi_ok:
        s4_fastapi_ok, s4_fastapi_lat = _probe_http("http://host.docker.internal:8000/docs", timeout=0.8)

    s4_mlflow_ok, s4_mlflow_lat = _probe_http("http://host.docker.internal:5000", timeout=0.6)
    if not s4_mlflow_ok:
        s4_mlflow_ok, s4_mlflow_lat = _probe_tcp("host.docker.internal", 5000, timeout=0.4)

    s5_ch_ok = ch.get("online", False)
    s5_ch_lat = 4 if s5_ch_ok else None

    s5_mn_ok = mn.get("online", False)
    s5_mn_lat = 8 if s5_mn_ok else None

    wsis_ok = ws_health.get("online", False)
    wsis_lat = 5 if wsis_ok else None

    s6_ok, s6_lat = _probe_http("http://sys06-alert-dispatcher-v2:8080/health", timeout=0.8)
    if not s6_ok:
        s6_ok, s6_lat = _probe_http("http://host.docker.internal:8080/health", timeout=0.8)

    gw_ok, gw_lat = False, None

    system_links_map = {
        "sys-01": {"online": s1_ok, "latency": s1_lat if s1_ok else None, "status": "ONLINE" if s1_ok else "OFFLINE"},
        "sys-02": {"online": s2_ok, "latency": s2_lat, "status": "ONLINE" if s2_ok else "OFFLINE"},
        "sys-03-grafana": {"online": s3_graf_ok, "latency": s3_graf_lat if s3_graf_ok else None, "status": "ONLINE" if s3_graf_ok else "OFFLINE"},
        "sys-03-prom": {"online": s3_prom_ok, "latency": s3_prom_lat if s3_prom_ok else None, "status": "ONLINE" if s3_prom_ok else "OFFLINE"},
        "sys-04-fastapi": {"online": s4_fastapi_ok, "latency": s4_fastapi_lat if s4_fastapi_ok else None, "status": "ONLINE" if s4_fastapi_ok else "OFFLINE"},
        "sys-04-mlflow": {"online": s4_mlflow_ok, "latency": s4_mlflow_lat if s4_mlflow_ok else None, "status": "ONLINE" if s4_mlflow_ok else "OFFLINE"},
        "sys-05-clickhouse": {"online": s5_ch_ok, "latency": s5_ch_lat, "status": "ONLINE" if s5_ch_ok else "OFFLINE"},
        "sys-05-minio": {"online": s5_mn_ok, "latency": s5_mn_lat, "status": "ONLINE" if s5_mn_ok else "OFFLINE"},
        "sys-wsis": {"online": wsis_ok, "latency": wsis_lat, "status": "ONLINE" if wsis_ok else "OFFLINE"},
        "sys-06": {"online": s6_ok, "latency": s6_lat if s6_ok else None, "status": "ONLINE" if s6_ok else "OFFLINE"},
        "sys-gateway": {"online": gw_ok, "latency": gw_lat, "status": "OFFLINE"}
    }

    return {
        "system_links": system_links_map,
        "services": [
            {
                "id": "airflow",
                "name": "Apache Airflow",
                "system": "SYS-02",
                "port": 8085,
                "type": "Orchestrator",
                "online": s2_ok,
                "url": settings.AIRFLOW_URL
            },
            {
                "id": "clickhouse",
                "name": "ClickHouse OLAP",
                "system": "SYS-05",
                "port": settings.CLICKHOUSE_PORT,
                "type": "Data Warehouse",
                "online": s5_ch_ok,
                "url": f"http://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}"
            },
            {
                "id": "minio",
                "name": "MinIO Lakehouse",
                "system": "SYS-05",
                "port": 9000,
                "type": "Object Storage",
                "online": s5_mn_ok,
                "url": f"http://{settings.MINIO_ENDPOINT}"
            },
            {
                "id": "wsis",
                "name": "WebSocket Ingestion (WSIS)",
                "system": "WSIS",
                "port": settings.WSIS_PORT,
                "type": "Realtime Stream",
                "online": wsis_ok,
                "url": f"http://{settings.WSIS_HOST}:{settings.WSIS_PORT}",
                "status": ws_health.get("status", "OFFLINE")
            },
            {
                "id": "aimodels",
                "name": "AI Models & Governance",
                "system": "SYS-04",
                "port": 8000,
                "type": "ML Inference & Risk",
                "online": s4_fastapi_ok,
                "url": "/api/v1/ai-models/summary"
            },
            {
                "id": "sys06",
                "name": "Operational Alert Dispatcher",
                "system": "SYS-06",
                "port": 8080,
                "type": "Alert & Notification",
                "online": s6_ok,
                "url": "http://sys06-alert-dispatcher-v2:8080"
            }
        ]
    }

# Mount Frontend static files
frontend_candidates = [
    Path(__file__).resolve().parent.parent.parent / "frontend",
    Path("/frontend"),
    Path(__file__).resolve().parent.parent / "frontend",
    Path("frontend")
]
frontend_dir = None
for candidate in frontend_candidates:
    if candidate.exists() and (candidate / "index.html").exists():
        frontend_dir = candidate
        break

if frontend_dir and frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/")
    def serve_index():
        index_file = frontend_dir / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
        return {"message": "Admin Portal backend running. Frontend index.html not found."}
else:
    @app.get("/")
    def serve_fallback():
        return {"message": "Admin Portal API running. Access /docs for OpenAPI documentation."}
