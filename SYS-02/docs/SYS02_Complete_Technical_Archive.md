# 📚 SYS-02: COMPLETE TECHNICAL ARCHIVE

## 1. System Architecture Matrix & Lineage

```mermaid
graph TD
    subgraph EXT["Binance Market Stream"]
        BIN["WebSocket / REST API"]
    end

    subgraph SYS02["SYS-02: Apache Airflow 2.8+ Engine"]
        DAG1["01_binance_klines_etl_dag (@hourly)"]
        DAG2["02_ml_model_retrain_dag (@daily)"]
        DAG3["03_lakehouse_maintenance_dag (@weekly)"]
    end

    subgraph SYS05["SYS-05: Lakehouse Storage"]
        MINIO["MinIO S3 (crypto-bronze / silver)"]
        CH["ClickHouse OLAP (crypto_klines_gold)"]
    end

    subgraph SYS04["SYS-04: MLOps Serving"]
        MLF["MLflow Registry (Port 5000)"]
        FAPI["FastAPI Serving (Port 8000)"]
    end

    subgraph SYS06["SYS-06: Operational Alerting"]
        BOT["Telegram Alert Dispatcher"]
    end

    BIN --> DAG1
    DAG1 -->|Extract & Transform| MINIO
    DAG1 -->|Load Gold Tables| CH
    DAG2 -->|Verify Freshness| CH
    DAG2 -->|Trigger Retrain| FAPI
    FAPI -->|Register Model| MLF
    DAG3 -->|Purge Expired Data| MINIO
    DAG1 & DAG2 & DAG3 -.->|SLA Miss / Failure| BOT
```

---

## 2. Component Specifications

### Custom Operators (`src/sys02_orchestration/operators/`)
- `LakehouseTaskOperator`: Handles Parquet conversion, deduplication, and S3 retention purging.
- `ClickHouseTaskOperator`: Handles Time-Series SQL execution and freshness checking.

### Monitoring & Quality Utilities (`src/sys02_orchestration/utils/`)
- `validate_klines_data()`: Inspects records for null close prices or negative volumes.
- `send_telegram_alert()` / `on_task_failure_callback()`: Provides SLA and failure alerting hooks.
