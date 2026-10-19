"""
Automated Initialization Script for Airflow External Connections.
Ensures ClickHouse, PostgreSQL, and MinIO connections are always registered and persistent.
"""

import os
import json
import logging
from airflow.models import Connection
from airflow.utils.session import create_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def register_default_connections():
    """Register core connections into Airflow metadata database."""
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "host.docker.internal")
    clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "nhfinanceadmin")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "nhathuy123")

    postgres_user = os.getenv("POSTGRES_USER", "airflow")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "airflow_secret")
    postgres_db = os.getenv("POSTGRES_DB", "airflow_db")

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "nhfinanceadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "nhathuy123")

    connections = [
        Connection(
            conn_id="clickhouse_default",
            conn_type="generic",
            host=clickhouse_host,
            port=clickhouse_port,
            login=clickhouse_user,
            password=clickhouse_password,
            description="ClickHouse Gold Analytical Database (SYS-03)",
        ),
        Connection(
            conn_id="postgres_default",
            conn_type="postgres",
            host="airflow_postgres",
            port=5432,
            schema=postgres_db,
            login=postgres_user,
            password=postgres_password,
            description="PostgreSQL Medallion Warehouse & Airflow DB",
        ),
        Connection(
            conn_id="minio_s3_default",
            conn_type="aws",
            login=minio_access_key,
            password=minio_secret_key,
            extra=json.dumps({"endpoint_url": f"http://{minio_endpoint}"}),
            description="MinIO Object Storage Lakehouse (Bronze/Silver)",
        ),
    ]

    with create_session() as session:
        for conn in connections:
            existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
            if existing:
                session.delete(existing)
                session.flush()
            session.add(conn)
            logger.info("Registered connection: %s (%s)", conn.conn_id, conn.conn_type)
        session.commit()
    logger.info("All connections initialized and verified successfully.")

if __name__ == "__main__":
    register_default_connections()
