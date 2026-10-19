"""
CLI Management Portal for SYS-05 Lakehouse & Database Platform
"""
import sys
import os
import io
import argparse
import logging
from datetime import datetime, timedelta
from tabulate import tabulate

# Ensure UTF-8 output encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from .config import get_config
from .minio_client import LakehouseStorageManager
from .clickhouse_client import ClickHouseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sys05.cli")

def cmd_status(args):
    config = get_config()
    print("=" * 70)
    print(" 💾 SYS-05: LAKEHOUSE & DATABASE MANAGEMENT PORTAL STATUS")
    print("=" * 70)
    print(f"• MinIO Endpoint:    {config.minio_endpoint}")
    print(f"• ClickHouse Host:   {config.clickhouse_host}:{config.clickhouse_port}")
    print(f"• Postgres Host:     {config.postgres_host}:{config.postgres_port}")
    print("-" * 70)

    # 1. MinIO Storage Status
    print("\n📦 1. MINIO MEDALLION BUCKETS STATUS:")
    minio_mgr = LakehouseStorageManager(config)
    try:
        bucket_stats = minio_mgr.get_bucket_status()
        table_data = []
        for b in bucket_stats:
            table_data.append([
                b.get("bucket"),
                "ACTIVE ✅" if b.get("exists") else "FAILED ❌",
                b.get("object_count", 0),
                f"{b.get('total_size_mb', 0.0)} MB"
            ])
        print(tabulate(table_data, headers=["Bucket Name", "Status", "Object Count", "Total Size"], tablefmt="grid"))
    except Exception as e:
        print(f"❌ MinIO Connection Error: {e}")

    # 2. ClickHouse OLAP Status
    print("\n⚡ 2. CLICKHOUSE OLAP ENGINE STATUS:")
    ch_mgr = ClickHouseManager(config)
    if ch_mgr.is_healthy():
        print("• ClickHouse Connection: HEALTHY ✅")
        try:
            stats = ch_mgr.get_table_stats()
            ch_data = [
                ["crypto_klines_gold (Klines OHLCV)", stats.get("crypto_klines_gold_count", 0)],
                ["mv_hourly_volume (Hourly MV)", stats.get("mv_hourly_volume_count", 0)],
                ["crypto_trades_raw (aggTrade Stream)", stats.get("crypto_trades_raw_count", 0)],
                ["crypto_orderbook_depth (Depth Updates)", stats.get("crypto_orderbook_depth_count", 0)],
                ["crypto_liquidations (Futures Panic)", stats.get("crypto_liquidations_count", 0)]
            ]
            print(tabulate(ch_data, headers=["Table / Materialized View", "Row Count"], tablefmt="grid"))
        except Exception as e:
            print(f"❌ ClickHouse Query Error: {e}")
    else:
        print("❌ ClickHouse Connection: UNHEALTHY / DOWN")
    print("\n" + "=" * 70)

def cmd_init(args):
    print("🚀 Initializing SYS-05 Infrastructure Resources...")
    config = get_config()
    
    # 1. Ensure MinIO Buckets
    print("• Provisioning MinIO Medallion Buckets...")
    minio_mgr = LakehouseStorageManager(config)
    bucket_res = minio_mgr.ensure_buckets_exist()
    for bucket, success in bucket_res.items():
        status_str = "CREATED/READY ✅" if success else "FAILED ❌"
        print(f"  - Bucket [{bucket}]: {status_str}")

    # 2. Initialize ClickHouse Schemas
    print("\n• Provisioning ClickHouse SQL DDL Schemas...")
    ch_mgr = ClickHouseManager(config)
    if ch_mgr.init_tables():
        print("  - ClickHouse Tables & Materialized Views: INITIALIZED ✅")
    else:
        print("  - ClickHouse DDL Execution: FAILED ❌")

    print("\n✨ SYS-05 Initialization Complete!")

def cmd_query(args):
    symbol = args.symbol.upper()
    config = get_config()
    ch_mgr = ClickHouseManager(config)
    
    print(f"🔍 Querying K-Line Data for Symbol: {symbol}...")
    try:
        rows, query_time_ms = ch_mgr.query_klines(symbol, limit=args.limit)
        print(f"⏱️ Query Execution Time: {query_time_ms:.2f} ms (SLA Target: < 10.00 ms)")
        if query_time_ms <= 10.0:
            print("🎯 SLA Status: PASSED (Sub-10ms response time)")
        else:
            print("⚠️ SLA Status: SLA EXCEEDED (> 10ms)")

        if rows:
            print(f"\nTop {len(rows)} Records:")
            table_data = []
            for r in rows:
                table_data.append([
                    r.get("symbol"),
                    str(r.get("open_time")),
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r.get("close"),
                    r.get("volume"),
                    r.get("sma_20"),
                    r.get("rsi_14")
                ])
            headers = ["Symbol", "Open Time", "Open", "High", "Low", "Close", "Volume", "SMA(20)", "RSI(14)"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            print(f"No records found for symbol {symbol}.")
    except Exception as e:
        print(f"❌ Query Error: {e}")

def cmd_healthcheck(args):
    print("🏥 Running SYS-05 System Health Check & Performance Benchmark...")
    config = get_config()
    
    # Check MinIO
    minio_mgr = LakehouseStorageManager(config)
    minio_ok = True
    try:
        buckets = minio_mgr.get_bucket_status()
        for b in buckets:
            if not b.get("exists"):
                minio_ok = False
    except Exception:
        minio_ok = False

    # Check ClickHouse
    ch_mgr = ClickHouseManager(config)
    ch_ok = ch_mgr.is_healthy()
    
    # SLA Benchmark (Warm Connection)
    sla_ok = False
    query_ms = 0.0
    cold_ms = 0.0
    if ch_ok:
        try:
            # 1. Cold start query (initial TCP/HTTP connection handshake)
            _, cold_ms = ch_mgr.query_klines("BTCUSDT", limit=1)
            # 2. Warm query (SLA measurement on active connection pool)
            _, query_ms = ch_mgr.query_klines("BTCUSDT", limit=10)
            sla_ok = query_ms <= 10.0
        except Exception:
            sla_ok = False

    print("\n--- HEALTHCHECK SUMMARY ---")
    print(f"1. MinIO Object Storage:   {'HEALTHY ✅' if minio_ok else 'UNHEALTHY ❌'}")
    print(f"2. ClickHouse Engine:      {'HEALTHY ✅' if ch_ok else 'UNHEALTHY ❌'}")
    print(f"3. Cold Start Handshake:   {cold_ms:.2f} ms")
    print(f"4. Single-Key SLA (<10ms): {'PASSED ✅ (' + f'{query_ms:.2f}ms)' if sla_ok else 'FAILED ❌ (' + f'{query_ms:.2f}ms)'}")
    print("----------------------------")

def main():
    parser = argparse.ArgumentParser(description="SYS-05 Lakehouse & Database Management Portal CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Subcommand: status
    parser_status = subparsers.add_parser("status", help="Show system status")
    parser_status.set_defaults(func=cmd_status)

    # Subcommand: init
    parser_init = subparsers.add_parser("init", help="Initialize buckets and ClickHouse DDL")
    parser_init.set_defaults(func=cmd_init)

    # Subcommand: query
    parser_query = subparsers.add_parser("query", help="Run OLAP query for symbol")
    parser_query.add_argument("symbol", nargs="?", default="BTCUSDT", help="Crypto Symbol (e.g. BTCUSDT)")
    parser_query.add_argument("--limit", type=int, default=10, help="Row limit")
    parser_query.set_defaults(func=cmd_query)

    # Subcommand: healthcheck
    parser_hc = subparsers.add_parser("healthcheck", help="Run diagnostic health check & benchmark")
    parser_hc.set_defaults(func=cmd_healthcheck)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
