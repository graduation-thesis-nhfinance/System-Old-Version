import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from app.config import settings

logger = logging.getLogger(__name__)

class ClickHouseConnector:
    def __init__(self):
        self.host = settings.CLICKHOUSE_HOST
        self.port = settings.CLICKHOUSE_PORT
        self.user = settings.CLICKHOUSE_USER
        self.password = settings.CLICKHOUSE_PASSWORD
        self.db = settings.CLICKHOUSE_DB
        self.http_url = f"http://{self.host}:{self.port}"
        self.timeout = 2.0

    def get_health(self) -> Dict[str, Any]:
        """Check ClickHouse server ping."""
        try:
            resp = requests.get(f"{self.http_url}/ping", timeout=self.timeout)
            if resp.status_code == 200 and "Ok" in resp.text:
                return {"online": True, "status": "ONLINE", "message": "ClickHouse HTTP server running"}
        except Exception as e:
            logger.debug("ClickHouse ping failed: %s", e)
        return {"online": False, "status": "OFFLINE", "error": "Cannot reach ClickHouse HTTP (8123)"}

    def query(self, query_sql: str) -> List[Dict[str, Any]]:
        """Run SQL query via ClickHouse HTTP interface with JSON output."""
        try:
            sql_formatted = f"{query_sql.strip()} FORMAT JSON"
            resp = requests.post(
                self.http_url,
                params={"user": self.user, "password": self.password, "database": self.db},
                data=sql_formatted.encode("utf-8"),
                timeout=self.timeout
            )
            if resp.status_code == 200:
                result = resp.json()
                return result.get("data", [])
            else:
                logger.debug("ClickHouse query error: %s", resp.text)
        except Exception as e:
            logger.debug("ClickHouse query exception: %s", e)
        return []

    def execute(self, query_sql: str) -> bool:
        """Run SQL DML/DDL statement via ClickHouse HTTP interface."""
        try:
            resp = requests.post(
                self.http_url,
                params={"user": self.user, "password": self.password, "database": self.db},
                data=query_sql.strip().encode("utf-8"),
                timeout=self.timeout
            )
            if resp.status_code == 200:
                return True
            else:
                logger.error("ClickHouse execute error (%s): %s", resp.status_code, resp.text)
                return False
        except Exception as e:
            logger.error("ClickHouse execute exception: %s", e)
            return False

    def get_overview_stats(self) -> Dict[str, Any]:
        """Retrieve record counts and freshness stats for Gold and business tables."""
        health = self.get_health()
        if not health["online"]:
            return {
                "online": False,
                "total_rows": 0,
                "business_rows": 0,
                "tables": {},
                "freshness": "Offline",
                "qc_status": "Offline"
            }

        stats = {
            "online": True,
            "total_rows": 0,
            "business_rows": 0,
            "tables": {},
            "freshness": "Up to date",
            "qc_status": "Passed"
        }

        try:
            # Query business tables specifically
            biz_query = """
            SELECT database, table, sum(rows) as total_rows 
            FROM system.parts 
            WHERE database IN ('default', 'db_gold_analytics', 'db_silver_market', 'db_silver_orderbook', 'db_silver_derivatives', 'db_bronze', 'db_gold_fraud', 'db_bronze_fraud') 
              AND active 
              AND table NOT LIKE '.%'
            GROUP BY database, table
            """
            rows = self.query(biz_query)
            biz_total = 0
            for r in rows:
                t_name = r.get("table", "")
                t_db = r.get("database", "")
                t_count = int(r.get("total_rows", 0))
                display_key = f"{t_db}.{t_name}" if t_db in ('db_gold_fraud', 'db_bronze_fraud') else t_name
                stats["tables"][display_key] = t_count
                biz_total += t_count
            stats["business_rows"] = biz_total
            stats["total_rows"] = biz_total

            # Check latest timestamp for freshness (converted to GMT+7 Vietnam Time)
            fresh_query = """
            SELECT 
                max(close_time) as latest_close, 
                max(open_time) as latest_open,
                max(updated_at) as latest_updated
            FROM crypto_klines_gold
            """
            ts_res = self.query(fresh_query)
            if ts_res and ts_res[0]:
                r = ts_res[0]
                latest_str = r.get("latest_close") or r.get("latest_open") or r.get("latest_updated")
                if latest_str:
                    try:
                        raw_clean = str(latest_str).split(".")[0].replace("T", " ")
                        dt_utc = datetime.strptime(raw_clean, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        dt_gmt7 = dt_utc.astimezone(timezone(timedelta(hours=7)))
                        
                        now_utc = datetime.now(timezone.utc)
                        delta_seconds = max(0, int((now_utc - dt_utc).total_seconds()))
                        mins_ago = delta_seconds // 60

                        time_display = dt_gmt7.strftime("%Y-%m-%d %H:%M")
                        ago_str = "just now" if mins_ago == 0 else f"{mins_ago}m ago"
                        if mins_ago <= 15:
                            stats["freshness"] = f"Real-time (latest: {time_display} GMT+7 • {ago_str})"
                        elif mins_ago < 60:
                            stats["freshness"] = f"Fresh (<1h) (latest: {time_display} GMT+7 • {ago_str})"
                        else:
                            hours_ago = mins_ago // 60
                            stats["freshness"] = f"Delayed (latest: {time_display} GMT+7 • {hours_ago}h ago)"

                        stats["latest_data_time_gmt7"] = dt_gmt7.strftime("%Y-%m-%d %H:%M:%S GMT+7")
                        stats["freshness_minutes_ago"] = mins_ago
                    except Exception as ex:
                        logger.debug("Failed parsing freshness timestamp: %s", ex)
                        stats["freshness"] = f"Real-time (latest: {latest_str} UTC)"
        except Exception as e:
            logger.warning("Error computing ClickHouse overview stats: %s", e)

        return stats

    def get_matrix_slot_counts(self, date_str: Optional[str] = None, window_str: Optional[str] = None) -> Dict[str, Dict[str, int]]:
        """
        Query actual record counts per (symbol, product) from ClickHouse tables.
        Returns: {symbol: {"klines": count, "aggtrade": count, "depth": count, "liquidations": count}}
        """
        counts: Dict[str, Dict[str, int]] = {
            s: {"klines": 0, "aggtrade": 0, "depth": 0, "liquidations": 0}
            for s in settings.TRADING_PAIRS
        }

        try:
            # 1. Klines
            q_klines = "SELECT symbol, count() as cnt FROM crypto_klines_gold GROUP BY symbol"
            for r in self.query(q_klines):
                sym = r.get("symbol")
                if sym in counts:
                    counts[sym]["klines"] = int(r.get("cnt", 0))

            # 2. AggTrades
            q_agg = "SELECT symbol, count() as cnt FROM crypto_agg_trades_gold GROUP BY symbol"
            for r in self.query(q_agg):
                sym = r.get("symbol")
                if sym in counts:
                    counts[sym]["aggtrade"] = int(r.get("cnt", 0))

            # 3. Order Book Depth
            q_depth = "SELECT symbol, count() as cnt FROM crypto_order_book_depth_gold GROUP BY symbol"
            for r in self.query(q_depth):
                sym = r.get("symbol")
                if sym in counts:
                    counts[sym]["depth"] = int(r.get("cnt", 0))

            # 4. Liquidations
            q_liq = "SELECT symbol, count() as cnt FROM crypto_futures_liquidations_gold GROUP BY symbol"
            for r in self.query(q_liq):
                sym = r.get("symbol")
                if sym in counts:
                    counts[sym]["liquidations"] = int(r.get("cnt", 0))

        except Exception as e:
            logger.warning("Error fetching ClickHouse matrix slot counts: %s", e)

        return counts

    def get_sample_preview(self, table_name: str, symbol: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve real sample rows from a ClickHouse table for inspector preview."""
        try:
            where_clause = f"WHERE symbol = '{symbol}'" if symbol else ""
            sql = f"SELECT * FROM {table_name} {where_clause} ORDER BY tuple() LIMIT {limit}"
            return self.query(sql)
        except Exception as e:
            logger.warning("Failed to fetch sample preview for %s: %s", table_name, e)
            return []

    def get_fraud_feature_store_stats(self) -> Dict[str, Any]:
        """Fetch real-time tables, row counts, and schema info for Fraud Feature Store."""
        tables_info = []
        try:
            q = """
            SELECT 
                database, 
                name as table, 
                engine, 
                coalesce(total_rows, 0) as rows, 
                formatReadableSize(coalesce(total_bytes, 0)) as disk_size
            FROM system.tables
            WHERE database IN ('db_gold_fraud', 'db_bronze_fraud')
            ORDER BY database, name
            """
            rows = self.query(q)
            for r in rows:
                tables_info.append({
                    "database": r.get("database"),
                    "table": r.get("table"),
                    "full_name": f"{r.get('database')}.{r.get('table')}",
                    "engine": r.get("engine"),
                    "row_count": int(r.get("rows", 0)),
                    "disk_size": r.get("disk_size", "0 B")
                })
        except Exception as e:
            logger.warning("Failed to fetch fraud feature store stats: %s", e)
        return {
            "databases": ["db_gold_fraud", "db_bronze_fraud"],
            "tables": tables_info,
            "connected": len(tables_info) > 0
        }

    def get_latest_klines(self, symbol: str = "BTCUSDT", limit: int = 60) -> List[Dict[str, Any]]:
        """Fetch latest closed klines for a given symbol for model inference."""
        try:
            q = f"""
            SELECT 
                symbol,
                open_time,
                close_time,
                open as open_price,
                high as high_price,
                low as low_price,
                close as close_price,
                volume,
                quote_asset_volume as quote_volume,
                number_of_trades as trades_count,
                taker_buy_base_asset_volume as taker_buy_base_volume,
                taker_buy_quote_asset_volume as taker_buy_quote_volume
            FROM crypto_klines_gold
            WHERE symbol = '{symbol}'
            ORDER BY close_time DESC
            LIMIT {limit}
            """
            rows = self.query(q)
            # Reverse so it's chronological
            return list(reversed(rows))
        except Exception as e:
            logger.warning("Failed to fetch latest klines for %s: %s", symbol, e)
            return []

clickhouse_connector = ClickHouseConnector()

