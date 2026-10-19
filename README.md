# 🏛️ Financial Analytics & Fraud Detection Enterprise Platform
### 📦 System-Old-Version — Legacy Architecture & Baseline Prototype

> [!WARNING]
> ### ⚠️ ARCHIVED / LEGACY REPOSITORY NOTICE
> This repository preserves the **initial architectural prototype (V1.0 — Early Iteration)** of the Graduation Thesis.
> 
> This codebase is formally **frozen / archived** for the following academic and engineering purposes:
> 1. **Baseline & Benchmark Reference:** Serves as the quantitative baseline to evaluate performance, ingestion latency, and fault tolerance against the upgraded system.
> 2. **Research Evolution Tracking:** Documents the iterative progression from batch-centric pipelines to real-time streaming and MLOps lifecycle management.
> 3. **Experiment Reproducibility:** Enables independent reproduction of early empirical experiments and initial data lakehouse benchmarks.
>
> 🚀 **Current Production System (Full 6-System Ecosystem + Streaming Engine):** Please refer to the primary active repository: **[`Systems-Graduation-Thesis`](../Systems-Graduation-Thesis)**.

---

## 🧭 Architectural Overview: Version 1.0 (Legacy)

In the initial stage of this research, the platform was centered around high-throughput batch ingestion and hybrid data lakehouse storage, focusing on the historical collection and analytical warehousing of financial transaction data:

```
[External Financial Data Sources]
                │
                ▼ (Batch Fetch / Scheduled Interval)
┌────────────────────────────────────────────────────────┐
│  SYS-02: Data Pipeline & Orchestration                 │
│  - Apache Airflow 2.x (Periodic Extraction DAGs)       │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼ (Raw & Staged Data Load)
┌────────────────────────────────────────────────────────┐
│  SYS-05: Storage & Lakehouse Core                      │
│  - MinIO (S3 Object Storage — Bronze & Silver Buckets) │
│  - ClickHouse (OLAP Columnar Engine for Analytics)     │
│  - PostgreSQL (Relational Metadata & Operational Ops)  │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Admin Portal (Legacy Prototype UI — Port 8090)        │
│  - Basic Service Navigation & Monitoring               │
└────────────────────────────────────────────────────────┘
```

---

## 📂 Repository Contents & Implementation Scope

While early planning documentation conceptualized a full six-subsystem ecosystem, this legacy repository contains only the foundational infrastructure and ingestion components that were physically implemented in V1.0:

| Subsystem / Directory | Component Role | V1.0 Status | Technology Stack |
| :--- | :--- | :--- | :--- |
| [`SYS-02/`](SYS-02/) | **Data Pipeline & Orchestration System**<br>Manages periodic batch DAGs for data extraction and basic transformation. | ✅ Implemented (Batch-Only) | Apache Airflow 2.x, Celery / Local Executor, Python |
| [`SYS-05/`](SYS-05/) | **Lakehouse & Database Management Portal**<br>High-performance columnar OLAP store and object storage backend. | ✅ Implemented | MinIO S3, ClickHouse OLAP, PostgreSQL, Delta Lake |
| [`Admin_Portal/`](Admin_Portal/) | **Unified Ecosystem Portal (V1 Prototype)**<br>Static navigation portal and service dashboard for early local demos. | ✅ Prototype | Fastify / Node.js, Vanilla HTML5/CSS/JavaScript |
| `Scripts (*.ps1)` | Automation scripts for local orchestrations and container health checks. | ✅ Operational | PowerShell, Docker CLI |

> [!NOTE]
> Advanced analytical visualization (**SYS-01**), real-time cluster observability (**SYS-03**), end-to-end MLOps serving (**SYS-04**), and automated webhook alert dispatching (**SYS-06**) existed in this version solely as **preliminary planning specifications**, and were later fully engineered in the V2.0 platform.

---

## ⚖️ Architectural Evolution: Legacy (V1.0) vs. Modern (V2.0)

Identifying the architectural limits of V1.0 motivated the design of the micro-enterprise platform in V2.0:

| Dimension | 📦 Legacy System (`System-Old-Version`) | 🚀 Modern System (`Systems-Graduation-Thesis`) |
| :--- | :--- | :--- |
| **Ecosystem Scope** | 2 implemented subsystems (`SYS-02`, `SYS-05`) + prototype portal. | **6 fully integrated subsystems** (`SYS-01` → `SYS-06`) + **WSIS Engine**. |
| **Ingestion Paradigm** | **Batch Polling (Airflow)**: Latency in minutes/hours; vulnerable to exchange rate limits. | **Low-Latency Streaming (WSIS)**: Binance WebSocket feed (<100ms latency), continuous real-time processing. |
| **Machine Learning & AI** | Offline experimental scripts; lack of automated real-time inference. | **Full MLOps Lifecycle (SYS-04)**: FastAPI serving (<50ms), MLflow tracking, LSTM + LightGBM + Isolation Forest. |
| **Business Analytics UI** | Static web portal without interactive streaming visualizers. | **Interactive Dashboard (SYS-01)**: Streamlit Canvas, real-time Plotly charts, live fraud detection overlays. |
| **Observability & Health** | Manual container inspection via Docker Desktop and CLI logs. | **Centralized OPS Center (SYS-03)**: Prometheus metrics collection, Grafana real-time operational telemetry. |
| **Alerting System** | None; silent failures requiring manual troubleshooting. | **Incident Dispatcher (SYS-06)**: Automated multi-channel anomaly alerts via Telegram Bot & Slack Webhooks. |
| **Network & Deployment** | Localhost-bound; rigid network configurations. | **Secure Cloud Tunnel**: Integrated Cloudflare Tunnel (`cloudflared`), standardized environment secrets. |

---

## 🛠️ Reproduction & Verification Guide (Runbook)

Follow these instructions to spin up the legacy environment for historical benchmarks and comparative evaluations:

### 1. Prerequisites
- **Operating System:** Windows 10/11 (with PowerShell 5.1+)
- **Container Runtime:** Docker Desktop installed and running
- **Hardware Recommendation:** Minimum 8 GB available RAM

### 2. Service Endpoints & Default Credentials
| Service | Local URL / Port | Default Credentials | Description |
| :--- | :--- | :--- | :--- |
| **Admin Portal** | `http://localhost:8090` | `nhfinanceadmin` / `nhathuy123` | Legacy portal UI |
| **Airflow Web UI (SYS-02)** | `http://localhost:8085` | `admin123` / `admin123` | Batch pipeline DAG manager |
| **MinIO Console (SYS-05)** | `http://localhost:9001` | `admin123` / `admin123` | Object storage management |
| **ClickHouse HTTP (SYS-05)** | `http://localhost:8123/play` | `default` / `(no password)` | Columnar query playground |

### 3. Orchestration Commands

Execute the following commands within PowerShell in the `System-Old-Version/` directory:

- **Launch all legacy containers:**
  ```powershell
  .\start_ecosystem.ps1
  ```
- **Inspect container health status:**
  ```powershell
  .\check_health.ps1
  ```
- **Tear down the environment:**
  ```powershell
  .\stop_ecosystem.ps1
  ```

---

## 🎓 Academic Value to the Thesis

The development of `System-Old-Version` contributed foundational engineering findings:
1. **Validation of Hybrid Lakehouse Architecture:** Demonstrated the compression efficiency and query performance advantages of ClickHouse combined with S3 object storage over traditional relational databases for financial time-series data.
2. **Batch Pipeline Baselining:** Established standardized Airflow DAG patterns for extracting and staging large-scale transactional datasets.
3. **Dataset Generation:** Generated the primary historical transaction corpus utilized for training and benchmark evaluations across subsequent machine learning iterations in `SYS-04`.

---

*This document serves as formal engineering documentation for the Graduation Thesis Defense & Evaluation.*
