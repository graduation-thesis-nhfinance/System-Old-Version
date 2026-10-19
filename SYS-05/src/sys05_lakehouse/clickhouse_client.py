"""
ClickHouse OLAP Database Client for Fast Financial Time-Series Analytics
"""
import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from .config import SystemConfig, get_config

logger = logging.getLogger("sys05.clickhouse_client")

class ClickHouseManager:
    def __init__(self, config: Optional[SystemConfig] = None):
        self.config = config or get_config()
        self._client: Optional[Client] = None

    def get_client(self) -> Client:
        """Lazy connection initializer for ClickHouse HTTP client."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.config.clickhouse_host,
                port=self.config.clickhouse_port,
                username=self.config.clickhouse_user,
                password=self.config.clickhouse_password or "",
                database=self.config.clickhouse_database
            )
        return self._client

    def is_healthy(self) -> bool:
        """Check if ClickHouse is reachable."""
        try:
            client = self.get_client()
            res = client.command("SELECT 1")
            return res == 1
        except Exception as e:
            logger.error(f"ClickHouse health check failed: {e}")
            return False

    def init_tables(self, sql_script_path: Optional[str] = None) -> bool:
        """Execute DDL initialization script to ensure all tables exist."""
        client = self.get_client()
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS default.crypto_klines_gold (
                symbol LowCardinality(String),
                open_time DateTime64(3, 'UTC'),
                close_time DateTime64(3, 'UTC'),
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Float64,
                quote_asset_volume Float64,
                number_of_trades UInt32,
                taker_buy_base_asset_volume Float64 DEFAULT 0.0,
                taker_buy_quote_asset_volume Float64 DEFAULT 0.0,
                sma_20 Float64 DEFAULT 0.0,
                rsi_14 Float64 DEFAULT 0.0,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(updated_at)
            PRIMARY KEY (symbol, open_time)
            ORDER BY (symbol, open_time)
            SETTINGS index_granularity = 8192;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_hourly_volume
            ENGINE = SummingMergeTree()
            PRIMARY KEY (symbol, hourly_time)
            ORDER BY (symbol, hourly_time)
            AS SELECT
                symbol,
                toStartOfHour(open_time) AS hourly_time,
                sum(volume) AS total_volume,
                count() AS total_klines
            FROM default.crypto_klines_gold
            GROUP BY symbol, hourly_time;
            """,
            """
            CREATE TABLE IF NOT EXISTS default.crypto_trades_raw (
                symbol LowCardinality(String),
                agg_trade_id UInt64,
                price Float64,
                quantity Float64,
                first_trade_id UInt64,
                last_trade_id UInt64,
                trade_time DateTime64(3, 'UTC'),
                is_buyer_maker UInt8,
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PRIMARY KEY (symbol, trade_time)
            ORDER BY (symbol, trade_time, agg_trade_id)
            SETTINGS index_granularity = 8192;
            """,
            """
            CREATE TABLE IF NOT EXISTS default.crypto_orderbook_depth (
                symbol LowCardinality(String),
                update_id UInt64 DEFAULT 0,
                last_update_id UInt64 DEFAULT 0,
                bids String DEFAULT '',
                asks String DEFAULT '',
                bid_volume Float64 DEFAULT 0.0,
                ask_volume Float64 DEFAULT 0.0,
                order_book_imbalance Float64 DEFAULT 0.0,
                event_time DateTime64(3, 'UTC') DEFAULT now64(3),
                observed_at DateTime64(3, 'UTC') DEFAULT now64(3),
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PRIMARY KEY (symbol, observed_at)
            ORDER BY (symbol, observed_at, last_update_id);
            """,
            """
            CREATE TABLE IF NOT EXISTS default.crypto_liquidations (
                symbol LowCardinality(String),
                side LowCardinality(String),
                price Float64,
                quantity Float64,
                trade_time DateTime64(3, 'UTC'),
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PRIMARY KEY (symbol, trade_time)
            ORDER BY (symbol, trade_time)
            SETTINGS index_granularity = 8192;
            """,
            """
            CREATE TABLE IF NOT EXISTS default.crypto_trading_pairs (
                symbol LowCardinality(String),
                base_asset LowCardinality(String),
                quote_asset LowCardinality(String),
                category LowCardinality(String),
                is_active UInt8 DEFAULT 1,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(updated_at)
            PRIMARY KEY (symbol)
            ORDER BY (symbol);
            """,
            """
            CREATE TABLE IF NOT EXISTS default.data_quality_results (
                batch_id String,
                dataset LowCardinality(String),
                symbol LowCardinality(String),
                check_time DateTime DEFAULT now(),
                passed UInt8,
                total_records UInt64,
                failed_records UInt64,
                null_close_count UInt64,
                negative_volume_count UInt64,
                duplicate_key_count UInt64,
                interval_gap_count UInt64,
                error_summary String
            )
            ENGINE = MergeTree()
            ORDER BY (dataset, symbol, check_time, batch_id);
            """,
            """
            CREATE TABLE IF NOT EXISTS default.pipeline_batch_status (
                batch_id String,
                pipeline LowCardinality(String),
                symbol LowCardinality(String),
                source_endpoint String,
                bronze_path String,
                silver_path String,
                gold_table String,
                status LowCardinality(String),
                quality_passed UInt8,
                records_bronze UInt64,
                records_silver UInt64,
                records_gold UInt64,
                started_at DateTime,
                finished_at DateTime,
                airflow_dag_run_id String,
                error_message String
            )
            ENGINE = ReplacingMergeTree(finished_at)
            ORDER BY (pipeline, symbol, batch_id);
            """,
            """
            INSERT INTO default.crypto_trading_pairs (symbol, base_asset, quote_asset, category, is_active) VALUES
                ('BTCUSDT', 'BTC', 'USDT', 'popular', 1),
                ('ETHUSDT', 'ETH', 'USDT', 'popular', 1),
                ('BNBUSDT', 'BNB', 'USDT', 'popular', 1),
                ('AAPLBUSDT', 'AAPLB', 'USDT', 'new_coin', 1),
                ('AMZNBUSDT', 'AMZNB', 'USDT', 'new_coin', 1),
                ('DELLBUSDT', 'DELLB', 'USDT', 'new_coin', 1);
            """
        ]
        try:
            for stmt in ddl_statements:
                client.command(stmt.strip())
            logger.info("ClickHouse tables and materialized views initialized.")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse tables: {e}")
            return False

    def insert_klines(self, records: List[Dict[str, Any]]) -> bool:
        """
        Batch insert K-line data into crypto_klines_gold table.
        Each dict in records must contain:
        symbol, open_time, close_time, open, high, low, close, volume,
        quote_asset_volume, number_of_trades, sma_20, rsi_14
        """
        if not records:
            return True

        client = self.get_client()
        columns = [
            "symbol", "open_time", "close_time", "open", "high", "low",
            "close", "volume", "quote_asset_volume", "number_of_trades",
            "sma_20", "rsi_14"
        ]
        
        data = []
        for r in records:
            data.append([
                r["symbol"],
                r["open_time"],
                r["close_time"],
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
                float(r.get("quote_asset_volume", 0.0)),
                int(r.get("number_of_trades", 0)),
                float(r.get("sma_20", 0.0)),
                float(r.get("rsi_14", 0.0))
            ])
            
        try:
            client.insert("crypto_klines_gold", data, column_names=columns)
            return True
        except Exception as e:
            logger.error(f"Error batch inserting klines into ClickHouse: {e}")
            return False

    def query_klines(self, symbol: str, limit: int = 100) -> Tuple[List[Dict[str, Any]], float]:
        """
        Execute single-key OLAP query for klines. Returns (results, query_time_ms).
        """
        client = self.get_client()
        sql = f"""
            SELECT symbol, open_time, close_time, open, high, low, close, volume, sma_20, rsi_14
            FROM default.crypto_klines_gold
            WHERE symbol = '{symbol}'
            ORDER BY open_time DESC
            LIMIT {limit}
        """
        start_time = time.perf_counter()
        query_result = client.query(sql)
        net_time_ms = (time.perf_counter() - start_time) * 1000.0

        # ClickHouse internal execution time from summary (elapsed_ns)
        server_elapsed = 0.0
        if hasattr(query_result, 'summary') and isinstance(query_result.summary, dict):
            if 'elapsed_ns' in query_result.summary:
                server_elapsed = float(query_result.summary['elapsed_ns']) / 1_000_000.0
            elif 'elapsed' in query_result.summary:
                server_elapsed = float(query_result.summary['elapsed']) * 1000.0

        # Use server execution time if available, fallback to net_time_ms
        query_time_ms = server_elapsed if server_elapsed > 0 else net_time_ms

        column_names = query_result.column_names
        rows = [dict(zip(column_names, row)) for row in query_result.result_rows]
        return rows, query_time_ms

    def query_hourly_volume(self, symbol: str) -> Tuple[List[Dict[str, Any]], float]:
        """
        Query from Materialized View mv_hourly_volume. Returns (results, query_time_ms).
        """
        client = self.get_client()
        sql = f"""
            SELECT symbol, hourly_time, sum(total_volume) as volume, sum(total_klines) as count
            FROM default.mv_hourly_volume
            WHERE symbol = '{symbol}'
            GROUP BY symbol, hourly_time
            ORDER BY hourly_time DESC
            LIMIT 50
        """
        start_time = time.perf_counter()
        query_result = client.query(sql)
        query_time_ms = (time.perf_counter() - start_time) * 1000.0

        column_names = query_result.column_names
        rows = [dict(zip(column_names, row)) for row in query_result.result_rows]
        return rows, query_time_ms

    def get_active_trading_pairs(self) -> List[Dict[str, Any]]:
        """
        Returns list of active trading pairs registered from Google Sheet or dim_trading_pairs.
        Deep interface method exposing simple list to downstream callers.
        """
        client = self.get_client()
        sql = "SELECT symbol, base_asset, quote_asset, category FROM default.crypto_trading_pairs WHERE is_active = 1 ORDER BY symbol"
        try:
            res = client.query(sql)
            return [dict(zip(res.column_names, row)) for row in res.result_rows]
        except Exception as e:
            try:
                sql_dim = "SELECT symbol, base_coin AS base_asset, quote_coin AS quote_asset, category FROM default.dim_trading_pairs WHERE is_active = 1 ORDER BY symbol"
                res = client.query(sql_dim)
                return [dict(zip(res.column_names, row)) for row in res.result_rows]
            except Exception as e2:
                logger.error(f"Error fetching active trading pairs: {e} | fallback: {e2}")
                return []

    def insert_trades(self, records: List[Dict[str, Any]]) -> bool:
        """Batch insert aggTrade real-time trade stream records."""
        if not records:
            return True
        client = self.get_client()
        columns = ["symbol", "agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "trade_time", "is_buyer_maker"]
        data = [
            [
                r["symbol"], int(r["agg_trade_id"]), float(r["price"]), float(r["quantity"]),
                int(r.get("first_trade_id", 0)), int(r.get("last_trade_id", 0)),
                r["trade_time"], int(r.get("is_buyer_maker", 0))
            ] for r in records
        ]
        try:
            client.insert("crypto_trades_raw", data, column_names=columns)
            return True
        except Exception as e:
            logger.error(f"Error inserting trades: {e}")
            return False

    def insert_liquidations(self, records: List[Dict[str, Any]]) -> bool:
        """Batch insert Futures forced liquidation stream records."""
        if not records:
            return True
        client = self.get_client()
        columns = ["symbol", "side", "price", "quantity", "trade_time"]
        data = [
            [
                r["symbol"], r["side"], float(r["price"]), float(r["quantity"]), r["trade_time"]
            ] for r in records
        ]
        try:
            client.insert("crypto_liquidations", data, column_names=columns)
            return True
        except Exception as e:
            logger.error(f"Error inserting liquidations: {e}")
            return False

    def get_table_stats(self) -> Dict[str, Any]:
        """Returns row count and table size metrics for all 5 system tables."""
        client = self.get_client()
        stats = {}
        tables = [
            "crypto_klines_gold",
            "mv_hourly_volume",
            "crypto_trades_raw",
            "crypto_orderbook_depth",
            "crypto_liquidations"
        ]
        for tbl in tables:
            try:
                res = client.query(f"SELECT count() FROM default.{tbl}")
                stats[f"{tbl}_count"] = res.result_rows[0][0]
            except Exception as e:
                stats[f"{tbl}_count"] = f"Error: {e}"
        return stats

