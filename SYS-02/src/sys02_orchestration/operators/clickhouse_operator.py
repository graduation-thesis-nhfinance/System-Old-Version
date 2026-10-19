"""Real ClickHouse Gold-layer persistence operations."""

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional

import requests
from sys02_orchestration.config import config, SYMBOL_TO_PAIR_ID


class ClickHouseTaskOperator:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        self.host = host or config.clickhouse.host
        self.port = port or config.clickhouse.port
        self.user = config.clickhouse.user
        self.password = config.clickhouse.password
        self.url = f"http://{self.host}:{self.port}/"

    def execute_sql(self, query: str) -> str:
        try:
            response = requests.post(self.url, params={"query": query}, auth=(self.user, self.password), timeout=15)
            response.raise_for_status()
            return response.text.strip()
        except Exception as exc:
            err_detail = ""
            if hasattr(exc, "response") and exc.response is not None:
                err_detail = f" | ClickHouse response: {exc.response.text.strip()}"
            raise RuntimeError(f"ClickHouse query failed at {self.host}:{self.port}: {exc}{err_detail}") from exc

    def ensure_tables_exist(self) -> None:
        self.execute_sql("""
            CREATE TABLE IF NOT EXISTS crypto_klines_gold (
                pair_id UInt8 DEFAULT 0,
                symbol LowCardinality(String), open_time DateTime64(3, 'UTC'),
                close_time DateTime64(3, 'UTC'), open Float64, high Float64,
                low Float64, close Float64, volume Float64,
                quote_asset_volume Float64 DEFAULT 0, number_of_trades UInt32 DEFAULT 0,
                taker_buy_base_asset_volume Float64 DEFAULT 0,
                taker_buy_quote_asset_volume Float64 DEFAULT 0,
                sma_20 Float64 DEFAULT 0, rsi_14 Float64 DEFAULT 0,
                collected_at DateTime64(3, 'UTC') DEFAULT now64(3),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (symbol, open_time)
        """)

    def insert_gold_klines(self, records: List[Dict[str, Any]], source: str = "v1.0.0_api_klines", type_method: str = "API") -> int:
        if not records:
            raise ValueError("Cannot insert an empty Gold batch")
        self.ensure_tables_exist()
        values = []
        for row in records:
            symbol = str(row["symbol"])
            if not symbol.isalnum():
                raise ValueError(f"Invalid symbol: {symbol}")
            pid = int(row.get("pair_id", 0)) or SYMBOL_TO_PAIR_ID.get(symbol, 0)
            open_time = datetime.fromtimestamp(int(row["timestamp"]), timezone.utc)
            close_time = open_time.replace(second=59, microsecond=999000)
            open_text = open_time.strftime("%Y-%m-%d %H:%M:%S.000")
            close_text = close_time.strftime("%Y-%m-%d %H:%M:%S.999")
            close = float(row["close"])
            volume = float(row["volume"])
            row_source = str(row.get("source", source))
            row_type = str(row.get("type_method", type_method))
            values.append(
                f"({pid},'{symbol}','{open_text}','{close_text}',{float(row['open'])},"
                f"{float(row['high'])},{float(row['low'])},{close},{volume},"
                f"{close * volume},0,0,0,'{row_source}','{row_type}')"
            )
        self.execute_sql(
            "INSERT INTO crypto_klines_gold "
            "(pair_id,symbol,open_time,close_time,open,high,low,close,volume,"
            "quote_asset_volume,number_of_trades,sma_20,rsi_14,source,type_method) VALUES "
            + ",".join(values)
        )
        return len(records)

    def verify_table_freshness(self, table_name: str, max_allowed_delay_seconds: int = 3600) -> Dict[str, Any]:
        row_count = int(self.execute_sql(f"SELECT count() FROM {table_name} WHERE updated_at >= now() - INTERVAL {max_allowed_delay_seconds} SECOND"))
        return {"table_name": table_name, "row_count": row_count, "is_fresh": row_count > 0, "max_delay_seconds": max_allowed_delay_seconds}

    @staticmethod
    def _timestamp_text(value):
        return datetime.fromtimestamp(int(value)/1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    def _insert_json_rows(self, table, rows):
        if not rows: return 0
        normalized=[]
        for row in rows:
            item=dict(row)
            for key in ("open_time","close_time","event_time","trade_time","observed_at","order_time","collected_at"):
                if key in item: item[key]=self._timestamp_text(item[key])
            for key in ("bids","asks"):
                if key in item: item[key]=json.dumps(item[key],separators=(",",":"))
            normalized.append(item)
        try:
            r = requests.post(
                self.url,
                params={"query": f"INSERT INTO {table} SETTINGS input_format_skip_unknown_fields=1 FORMAT JSONEachRow"},
                auth=(self.user, self.password),
                data="\n".join(json.dumps(x, separators=(",", ":")) for x in normalized).encode(),
                timeout=45,
            )
            r.raise_for_status()
        except Exception as exc:
            err_detail = ""
            if hasattr(exc, "response") and exc.response is not None:
                err_detail = f" | ClickHouse response: {exc.response.text.strip()}"
            raise RuntimeError(f"ClickHouse insert failed for {table}: {exc}{err_detail}") from exc
        return len(rows)

    def ensure_market_metric_tables(self):
        self.ensure_tables_exist()
        self.execute_sql("ALTER TABLE crypto_klines_gold ADD COLUMN IF NOT EXISTS taker_buy_base_asset_volume Float64 DEFAULT 0")
        self.execute_sql("ALTER TABLE crypto_klines_gold ADD COLUMN IF NOT EXISTS taker_buy_quote_asset_volume Float64 DEFAULT 0")
        self.execute_sql("CREATE TABLE IF NOT EXISTS crypto_agg_trades_gold (pair_id UInt8 DEFAULT 0, event_type LowCardinality(String),event_time DateTime64(3,'UTC'),symbol LowCardinality(String),aggregate_trade_id UInt64,price Float64,quantity Float64,first_trade_id UInt64,last_trade_id UInt64,trade_time DateTime64(3,'UTC'),buyer_is_market_maker Bool,collected_at DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(collected_at) ORDER BY (symbol,aggregate_trade_id)")
        self.execute_sql("ALTER TABLE crypto_agg_trades_gold ADD COLUMN IF NOT EXISTS pair_id UInt8 DEFAULT 0")
        self.execute_sql("CREATE TABLE IF NOT EXISTS crypto_order_book_depth_gold (pair_id UInt8 DEFAULT 0, symbol LowCardinality(String),last_update_id UInt64,bids String,asks String,bid_volume Float64,ask_volume Float64,order_book_imbalance Float64,observed_at DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(observed_at) ORDER BY (symbol,last_update_id)")
        self.execute_sql("ALTER TABLE crypto_order_book_depth_gold ADD COLUMN IF NOT EXISTS pair_id UInt8 DEFAULT 0")
        self.execute_sql("CREATE TABLE IF NOT EXISTS crypto_futures_liquidations_gold (pair_id UInt8 DEFAULT 0, event_type LowCardinality(String),event_time DateTime64(3,'UTC'),symbol LowCardinality(String),side LowCardinality(String),price Float64,quantity Float64,order_id UInt64,order_type LowCardinality(String),time_in_force LowCardinality(String),order_status LowCardinality(String),average_price Float64,filled_quantity Float64,order_time DateTime64(3,'UTC'),collected_at DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(collected_at) ORDER BY (symbol,order_id,order_time)")
        self.execute_sql("ALTER TABLE crypto_futures_liquidations_gold ADD COLUMN IF NOT EXISTS pair_id UInt8 DEFAULT 0")
    def insert_market_metric_rows(self, dataset, rows):
        tables={"klines":"crypto_klines_gold","agg_trades":"crypto_agg_trades_gold","order_book_depth":"crypto_order_book_depth_gold","futures_liquidations":"crypto_futures_liquidations_gold"}
        if dataset not in tables: raise ValueError(f"Unknown market metrics dataset: {dataset}")
        return self._insert_json_rows(tables[dataset],rows)
