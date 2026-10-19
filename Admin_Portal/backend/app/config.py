import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env if present
env_paths = [
    Path(__file__).resolve().parent.parent / ".env",
    Path(__file__).resolve().parent.parent.parent.parent / "SYS-02" / ".env"
]
for p in env_paths:
    if p.exists():
        load_dotenv(p)

class Settings:
    # Portal Server
    PORTAL_HOST: str = os.getenv("PORTAL_HOST", "0.0.0.0")
    PORTAL_PORT: int = int(os.getenv("PORTAL_PORT", "8090"))
    SECRET_KEY: str = os.getenv("PORTAL_SECRET_KEY", "nhfinance_admin_portal_secret_key_2026")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "43200"))  # 30 days (persistent session for monitoring cockpits)

    # Admin Credentials (synchronized with Airflow)
    ADMIN_USERNAME: str = os.getenv("AIRFLOW_ADMIN_USERNAME", "nhfinanceadmin")
    ADMIN_PASSWORD: str = os.getenv("AIRFLOW_ADMIN_PASSWORD", "nhathuy123")
    ADMIN_EMAIL: str = os.getenv("AIRFLOW_ADMIN_EMAIL", "admin@admin.nhfinance.com")

    # Airflow SYS-02
    AIRFLOW_URL: str = os.getenv("AIRFLOW_URL", "http://localhost:8085")
    AIRFLOW_API_URL: str = os.getenv("AIRFLOW_API_URL", "http://localhost:8085/api/v1")
    AIRFLOW_USER: str = os.getenv("AIRFLOW_ADMIN_USERNAME", "nhfinanceadmin")
    AIRFLOW_PASSWORD: str = os.getenv("AIRFLOW_ADMIN_PASSWORD", "nhathuy123")

    # ClickHouse SYS-05
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "nhfinanceadmin")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "nhathuy123")
    CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "default")

    # MinIO SYS-05
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "nhfinanceadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "nhathuy123")
    MINIO_SECURE: bool = False
    MINIO_BRONZE_BUCKET: str = "crypto-bronze"
    MINIO_SILVER_BUCKET: str = "crypto-silver"

    # WSIS
    WSIS_HOST: str = os.getenv("WSIS_HOST", "localhost")
    WSIS_PORT: int = int(os.getenv("WSIS_PORT", "8005"))

    # Six Configured Trading Pairs
    TRADING_PAIRS: list = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "AAPLBUSDT",
        "AMZNBUSDT",
        "DELLBUSDT"
    ]

    # Four Configured Data Products
    PRODUCTS: list = [
        {"id": "klines", "name": "Market Price Candles (OHLCV)", "short": "Klines"},
        {"id": "aggtrade", "name": "Trade Activity", "short": "aggTrade"},
        {"id": "depth", "name": "Order Book Depth", "short": "Order Book"},
        {"id": "liquidations", "name": "Liquidation Events", "short": "Future Liquidation"}
    ]

settings = Settings()
