# ADR 0001: Selection of Apache Airflow 2.x for Enterprise Pipeline Orchestration

- **Status**: Accepted
- **Date**: 2026-08-13
- **Context**: The financial data platform requires an enterprise-grade scheduler and orchestrator capable of automating multi-stage Medallion ETL pipelines, MLOps model retraining workflows, and database maintenance tasks with SLA monitoring and failure alerting.

## Decision
We select **Apache Airflow 2.8+** as the core orchestration engine for SYS-02.

## Rationale
1. **Python-Native DAG Definitions**: Allows data engineers and ML engineers to write pipeline definitions in clean Python code without complex XML/JSON DSLs.
2. **Rich Ecosystem & Operators**: Built-in support for PostgreSQL, HTTP REST endpoints, MinIO/S3, and custom Python operators.
3. **Resilience & SLA Monitoring**: Provides robust retries, backfill capabilities, SLA tracking, and callback hooks for operational alert dispatchers (SYS-06).
4. **Port Isolation**: Isolated Airflow metadata PostgreSQL to host port `5433` ensuring zero port collision with SYS-05 database services.

## Consequences
- Requires running an Airflow Webserver, Scheduler, and PostgreSQL database instance.
- Memory consumption per container is manageable (~300MB for scheduler, ~250MB for webserver).
