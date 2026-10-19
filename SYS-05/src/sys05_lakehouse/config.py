"""
Configuration Management for SYS-05 (Unified Credentials: admin123 / admin123)
"""
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load .env file from project root or current working directory
load_dotenv()

@dataclass
class SystemConfig:
    # MinIO S3 Settings
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "admin123")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "admin123")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "t")
    
    # Medallion Buckets
    bucket_bronze: str = os.getenv("BUCKET_BRONZE", "crypto-bronze")
    bucket_silver: str = os.getenv("BUCKET_SILVER", "crypto-silver")
    bucket_gold: str = os.getenv("BUCKET_GOLD", "crypto-gold")
    
    # ClickHouse OLAP Settings
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "admin123")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "admin123")
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "default")
    
    # PostgreSQL Settings
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user: str = os.getenv("POSTGRES_USER", "admin123")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "admin123")
    postgres_db: str = os.getenv("POSTGRES_DB", "enterprise_metadata")

def get_config() -> SystemConfig:
    return SystemConfig()
