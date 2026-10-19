"""
Configuration Settings for SYS-02 Orchestration Engine
"""

import os
from dataclasses import dataclass
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# Central registry of 6 Trading Pairs based on binance_market_data_reference.md
TRADING_PAIRS: Dict[int, Dict[str, Any]] = {
    1: {"pair_id": 1, "symbol": "BTCUSDT", "base_coin": "BTC", "quote_coin": "USDT", "trading_pair": "BTC/USDT", "market_type": "SPOT_FUTURES", "category": "crypto_popular", "is_futures": True},
    2: {"pair_id": 2, "symbol": "ETHUSDT", "base_coin": "ETH", "quote_coin": "USDT", "trading_pair": "ETH/USDT", "market_type": "SPOT_FUTURES", "category": "crypto_popular", "is_futures": True},
    3: {"pair_id": 3, "symbol": "BNBUSDT", "base_coin": "BNB", "quote_coin": "USDT", "trading_pair": "BNB/USDT", "market_type": "SPOT_FUTURES", "category": "crypto_popular", "is_futures": True},
    4: {"pair_id": 4, "symbol": "AAPLBUSDT", "base_coin": "AAPLB", "quote_coin": "USDT", "trading_pair": "AAPLB/USDT", "market_type": "SPOT", "category": "equity_token", "is_futures": False},
    5: {"pair_id": 5, "symbol": "AMZNBUSDT", "base_coin": "AMZNB", "quote_coin": "USDT", "trading_pair": "AMZNB/USDT", "market_type": "SPOT", "category": "equity_token", "is_futures": False},
    6: {"pair_id": 6, "symbol": "DELLBUSDT", "base_coin": "DELLB", "quote_coin": "USDT", "trading_pair": "DELLB/USDT", "market_type": "SPOT", "category": "equity_token", "is_futures": False},
}

SYMBOL_TO_PAIR_ID: Dict[str, int] = {data["symbol"]: pair_id for pair_id, data in TRADING_PAIRS.items()}

def get_pair_info(symbol_or_id) -> Optional[Dict[str, Any]]:
    """Retrieve pair configuration by integer pair_id or string symbol."""
    if isinstance(symbol_or_id, int):
        return TRADING_PAIRS.get(symbol_or_id)
    if isinstance(symbol_or_id, str):
        pid = SYMBOL_TO_PAIR_ID.get(symbol_or_or_symbol := symbol_or_id.upper())
        return TRADING_PAIRS.get(pid) if pid else None
    return None

@dataclass
class AirflowConfig:
    project_name: str = os.getenv("AIRFLOW_PROJECT_NAME", "sys02_orchestration")
    webserver_port: int = int(os.getenv("AIRFLOW_WEBSERVER_PORT", "8085"))
    postgres_host_port: int = int(os.getenv("POSTGRES_HOST_PORT", "5433"))
    load_examples: bool = os.getenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false").lower() == "true"

@dataclass
class LakehouseConfig:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket: str = "crypto-bronze"
    silver_bucket: str = "crypto-silver"
    gold_bucket: str = "crypto-gold"

@dataclass
class ClickHouseConfig:
    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    database: str = "default"
    # Specific databases
    bronze_db: str = "db_bronze"
    silver_market_db: str = "db_silver_market"
    silver_orderbook_db: str = "db_silver_orderbook"
    silver_derivatives_db: str = "db_silver_derivatives"
    gold_analytics_db: str = "db_gold_analytics"

@dataclass
class AlertConfig:
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    sla_alert_enabled: bool = os.getenv("SLA_ALERT_ENABLED", "true").lower() == "true"

class ConfigManager:
    """Central configuration manager for SYS-02 orchestration tasks."""
    def __init__(self):
        self.airflow = AirflowConfig()
        self.lakehouse = LakehouseConfig()
        self.clickhouse = ClickHouseConfig()
        self.alert = AlertConfig()
        self.trading_pairs = TRADING_PAIRS

config = ConfigManager()
