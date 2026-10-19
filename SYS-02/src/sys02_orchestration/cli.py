"""
Command Line Interface for SYS-02 Data Pipeline Orchestration
"""

import sys
import os
import argparse
import logging

# Ensure src/ directory is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sys02_orchestration.config import config
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sys02_cli")

def list_dags():
    print("==================================================================")
    print(" [SYS-02 AIRFLOW DAGS REGISTRY]")
    print("==================================================================")
    dags = [
        {"id": "01_binance_klines_etl_dag", "schedule": "@hourly", "description": "Market Data Ingestion & Medallion ETL Pipeline"},
        {"id": "02_ml_model_retrain_dag", "schedule": "@daily", "description": "Automated MLOps Model Retraining Trigger"},
        {"id": "03_lakehouse_maintenance_dag", "schedule": "@weekly", "description": "MinIO S3 Purge & ClickHouse Partition Vacuum"}
    ]
    for d in dags:
        print(f" * [{d['id']}] Frequency: {d['schedule']:<10} | {d['description']}")
    print(f"\n Airflow Web UI: http://localhost:{config.airflow.webserver_port}")
    print(f" Postgres Metadata DB Port: {config.airflow.postgres_host_port} (Isolated)")
    print("==================================================================")


def test_etl(symbol: str = "BTCUSDT"):
    logger.info(f"Running manual dry-run ETL test for {symbol}...")
    lake_op = LakehouseTaskOperator()
    ch_op = ClickHouseTaskOperator()
    
    etl_res = lake_op.process_bronze_to_silver(symbol=symbol)
    fresh_res = ch_op.verify_table_freshness("crypto_klines_gold")
    
    print("\n--- ETL DRY RUN TEST SUMMARY ---")
    print(f"Symbol Processed: {etl_res['symbol']}")
    print(f"Records Cleaned:  {etl_res['cleaned_ticks']}")
    print(f"Silver Dataset:   {etl_res['silver_output_path']}")
    print(f"Gold DB Freshness:{fresh_res['is_fresh']} (Rows: {fresh_res['row_count']})")
    print("--------------------------------\n")

def main():
    parser = argparse.ArgumentParser(description="SYS-02 Orchestration CLI Tool")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list-dags", help="List registered Airflow DAGs and status")
    
    etl_parser = subparsers.add_parser("test-etl", help="Run a manual dry-run of the ETL task")
    etl_parser.add_argument("--symbol", default="BTCUSDT", help="Trading symbol (default: BTCUSDT)")

    args = parser.parse_args()

    if args.command == "list-dags":
        list_dags()
    elif args.command == "test-etl":
        test_etl(symbol=args.symbol)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
