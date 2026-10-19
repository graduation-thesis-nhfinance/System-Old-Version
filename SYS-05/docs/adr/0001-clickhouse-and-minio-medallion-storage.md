# ADR 0001: ClickHouse OLAP & MinIO S3 Medallion Storage Architecture

- **Status**: Accepted
- **Date**: 2026-08-12
- **Deciders**: Enterprise Platform Architecture Team

---

## 🎯 Context and Problem Statement

The platform requires storing and querying millions of real-time financial market data points (K-lines, trade streams, order book depth, liquidations) per day.

Key technical constraints:
1. **Query Performance SLA**: Sub-10ms latency on time-series queries for dashboard and AI inference APIs.
2. **Storage Cost Efficiency**: High compression ratio for multi-year historical data.
3. **Data Integrity & Lineage**: Strict Medallion Architecture (Raw ➔ Cleaned ➔ Features) supporting Time Travel and recovery.

---

## 💡 Decision Drivers

- Need for high-throughput batch writes from streaming workers (Spark / Airflow).
- Fast aggregation queries (Sum, Avg, Count) over millions of rows without full-table scans.
- 100% open-source software running locally with zero cloud subscription fees.

---

## ⚖️ Considered Alternatives

1. **Relational Database Only (PostgreSQL with TimescaleDB extension)**
   - *Pros*: Single database system, full ACID transactions.
   - *Cons*: Slower columnar aggregation performance on 100M+ rows, larger disk storage footprint.
2. **ClickHouse OLAP + MinIO S3 Object Storage (Chosen)**
   - *Pros*: ClickHouse provides 10x compression ratio and sub-10ms OLAP performance; MinIO provides AWS S3 compatibility for Parquet/Delta Lake storage.
   - *Cons*: Managing two distinct storage engines.

---

## 📌 Decision Outcome

Chosen Option: **ClickHouse OLAP Engine combined with MinIO S3 Medallion Storage**.

### Positive Consequences
- **Sub-10ms Query SLA achieved** (`9.59ms` benchmark result).
- **Medallion Data Isolation**: Raw data in `crypto-bronze`, cleaned Parquet in `crypto-silver`, AI features in `crypto-gold`.
- **ReplacingMergeTree Engine** automatically eliminates duplicate K-lines during streaming ingests.
- **SummingMergeTree Materialized Views** compute hourly volume aggregations automatically.

### Negative Consequences
- Data ingestion pipelines must write to both S3 storage and ClickHouse OLAP database.
