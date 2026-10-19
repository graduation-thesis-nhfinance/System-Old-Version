# 📖 CONTEXT: SYS-02 Data Pipeline & Orchestration Domain Model

This document defines the **Ubiquitous Language** for **SYS-02 (Data Pipeline & Orchestration System)** within the Financial Analytics & Fraud Detection Enterprise Platform.

---

## 📚 Ubiquitous Language & Terminology

### 1. Airflow Orchestration & Core Concepts
- **Directed Acyclic Graph (DAG)**: A collection of tasks organized with explicit directional dependencies and scheduling rules without loops.
- **TaskInstance**: A specific run of a task within a DAG at a given execution timestamp (`logical_date`).
- **DAG Run**: An execution instance of an entire DAG for a specific schedule interval or manual trigger.
- **Operator**: Template definition for a single task unit (e.g., `PythonOperator`, `ClickHouseOperator`, `MinIOTransformOperator`).
- **Sensor**: A specialized task operator that polls external state (e.g., file arrival in MinIO or database row count) until a condition is met or timeout occurs.

### 2. Pipeline Execution & Scheduling Concepts
- **Schedule Interval**: The cron expression or frequency (`@hourly`, `@daily`, `@weekly`) controlling DAG execution.
- **Backfill**: Re-executing a DAG for historical time windows to populate missing data layers.
- **Catchup**: Airflow feature that executes past unrun intervals when a DAG is turned on. Set to `False` by default for streaming/real-time pipelines.
- **Concurrency & Pool**: Resource boundaries limiting maximum simultaneous task executions across worker processes to protect external databases.
- **Retries & Retry Delay**: Resilience parameters defining how many times a failed task is re-attempted and the exponential backoff delay between attempts.

### 3. Data Flow & Medallion Pipeline Stages
- **Bronze Ingestion Task**: Task extracting raw Binance WebSocket/REST JSON payloads into S3/MinIO (`crypto-bronze`).
- **Silver Cleansing Task**: Task validating schemas, removing duplicate market ticks, and writing parquet files into S3/MinIO (`crypto-silver`).
- **Gold Aggregation Task**: Task generating technical indicators (RSI, MA, VWAP) and loading time-series rows into ClickHouse (`crypto_klines_gold`).
- **Data Quality Check Gate**: Automated validation step ensuring zero null close prices, valid timestamp continuity, and volume sanity before downstream ML consumption.

### 4. Operational Monitoring & Resilience
- **SLA (Service Level Agreement)**: Maximum allowable time duration for a DAG or task to complete.
- **SLA Miss**: Event triggered when a task exceeds its allocated duration threshold, generating an operational alert hook.
- **Task Lineage**: Data dependency tracking mapping source dataset paths to destination tables.
- **Dead Letter Queue (DLQ)**: Quarantine location for corrupted records that fail schema validation rules during ETL execution.

---

## ⚙️ Operational Port Mapping & Isolation
- **Airflow Web UI**: Port `8080` (or `8082` if custom configured)
- **Airflow Metadata Postgres DB**: Internal `5432`, Host Port `5433` *(isolated from SYS-05 Postgres on 5432)*
