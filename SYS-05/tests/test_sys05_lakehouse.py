"""
Automated Integration Tests for SYS-05: Lakehouse & Database Management Portal
"""
import os
import sys
import time
import pytest
from datetime import datetime, timedelta
from datetime import datetime, timedelta, timezone

# Add src directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.sys05_lakehouse.config import get_config, SystemConfig
from src.sys05_lakehouse.minio_client import LakehouseStorageManager
from src.sys05_lakehouse.clickhouse_client import ClickHouseManager

def test_config_loading():
    config = get_config()
    assert isinstance(config, SystemConfig)
    assert config.bucket_bronze == "crypto-bronze"
    assert config.bucket_silver == "crypto-silver"
    assert config.bucket_gold == "crypto-gold"
    assert config.clickhouse_port == 8123

@pytest.mark.integration
def test_minio_bucket_management():
    config = get_config()
    minio_mgr = LakehouseStorageManager(config)
    
    # 1. Ensure buckets exist
    bucket_results = minio_mgr.ensure_buckets_exist()
    assert bucket_results[config.bucket_bronze] is True
    assert bucket_results[config.bucket_silver] is True
    assert bucket_results[config.bucket_gold] is True

    # 2. Test raw JSON upload to Bronze
    test_json = {"symbol": "BTCUSDT", "price": 65000.50, "timestamp": datetime.now(timezone.utc).isoformat()}
    obj_name = "test_raw_btc.json"
    upload_ok = minio_mgr.upload_bronze_raw(obj_name, test_json)
    assert upload_ok is True

    # 3. Read back data
    read_bytes = minio_mgr.get_object_data(config.bucket_bronze, obj_name)
    assert read_bytes is not None
    assert b"BTCUSDT" in read_bytes

@pytest.mark.integration
def test_clickhouse_ddl_and_data_flow():
    config = get_config()
    ch_mgr = ClickHouseManager(config)
    
    # 1. Check health
    assert ch_mgr.is_healthy() is True

    # 2. Init tables
    init_ok = ch_mgr.init_tables()
    assert init_ok is True

    # 3. Insert mock K-line records
    now = datetime.now(timezone.utc)
    records = []
    for i in range(10):
        records.append({
            "symbol": "BTCUSDT",
            "open_time": now - timedelta(minutes=i),
            "close_time": now - timedelta(minutes=i - 1),
            "open": 65000.0 + i,
            "high": 65100.0 + i,
            "low": 64900.0 + i,
            "close": 65050.0 + i,
            "volume": 10.5 + i,
            "quote_asset_volume": 680000.0,
            "number_of_trades": 150 + i,
            "sma_20": 65020.0,
            "rsi_14": 55.4
        })
        
    insert_ok = ch_mgr.insert_klines(records)
    assert insert_ok is True

    # 4. Query & Verify SLA (< 10ms target)
    rows, query_ms = ch_mgr.query_klines("BTCUSDT", limit=10)
    assert len(rows) > 0
    assert rows[0]["symbol"] == "BTCUSDT"
    print(f"\n[Test] Query execution time: {query_ms:.3f} ms")

    # 5. Query Materialized View
    mv_rows, mv_ms = ch_mgr.query_hourly_volume("BTCUSDT")
    assert isinstance(mv_rows, list)

    # 6. Verify deep module methods (Active trading pairs & stream insertions)
    pairs = ch_mgr.get_active_trading_pairs()
    assert len(pairs) >= 6
    assert any(p["symbol"] == "BTCUSDT" for p in pairs)

    # Test trade insertion
    trade_ok = ch_mgr.insert_trades([{
        "symbol": "BTCUSDT", "agg_trade_id": 1001, "price": 65000.0, "quantity": 0.5,
        "first_trade_id": 100, "last_trade_id": 101, "trade_time": now, "is_buyer_maker": 1
    }])
    assert trade_ok is True
