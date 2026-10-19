"""Binance market-metrics collection and persistence for the 15-minute DAG."""
from __future__ import annotations
import json, time
from typing import Any, Dict, Iterable, List, Sequence
import requests, websocket
from sys02_orchestration.config import config, SYMBOL_TO_PAIR_ID
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
SPOT_BASE_URL = "https://api.binance.com"
FUTURES_WS_BASE_URL = "wss://fstream.binance.com/stream?streams="

class BinanceMarketMetricsOperator:
    def __init__(self, symbols: Sequence[str], depth_limit: int = 100) -> None:
        self.symbols, self.depth_limit = tuple(s.upper() for s in symbols), depth_limit
    @staticmethod
    def _window_start(timestamp_ms: int) -> int:
        return timestamp_ms - timestamp_ms % 900000
    def _get(self, path: str, params: Dict[str, Any] | None = None) -> Any:
        response = requests.get(f"{SPOT_BASE_URL}{path}", params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    def available_symbols(self) -> tuple[List[str], List[str]]:
        active = {r["symbol"] for r in self._get("/api/v3/exchangeInfo")["symbols"] if r.get("status") == "TRADING"}
        available = [s for s in self.symbols if s in active]
        if not available: raise RuntimeError("None of the configured workbook trading pairs are active on Binance Spot")
        return available, [s for s in self.symbols if s not in active]
    def collect_spot(self, symbols: Iterable[str], collected_at: int) -> Dict[str, List[Dict[str, Any]]]:
        result = {"klines": [], "agg_trades": [], "order_book_depth": []}
        for symbol in symbols:
            pid = SYMBOL_TO_PAIR_ID.get(symbol, 0)
            k = self._get("/api/v3/klines", {"symbol": symbol, "interval": "15m", "limit": 1})[-1]
            result["klines"].append({"pair_id": pid, "symbol": symbol, "open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]), "close_time": int(k[6]), "quote_asset_volume": float(k[7]), "number_of_trades": int(k[8]), "taker_buy_base_asset_volume": float(k[9]), "taker_buy_quote_asset_volume": float(k[10]), "collected_at": collected_at})
            trades = self._get("/api/v3/aggTrades", {"symbol": symbol, "startTime": self._window_start(collected_at), "endTime": collected_at, "limit": 1000})
            result["agg_trades"].extend({"pair_id": pid, "event_type": "aggTrade", "event_time": int(x["T"]), "symbol": symbol, "aggregate_trade_id": int(x["a"]), "price": float(x["p"]), "quantity": float(x["q"]), "first_trade_id": int(x["f"]), "last_trade_id": int(x["l"]), "trade_time": int(x["T"]), "buyer_is_market_maker": bool(x["m"]), "collected_at": collected_at} for x in trades)
            book = self._get("/api/v3/depth", {"symbol": symbol, "limit": self.depth_limit}); bids, asks = book["bids"], book["asks"]
            bv, av = sum(float(x[1]) for x in bids), sum(float(x[1]) for x in asks)
            result["order_book_depth"].append({"pair_id": pid, "symbol": symbol, "last_update_id": int(book["lastUpdateId"]), "bids": bids, "asks": asks, "bid_volume": bv, "ask_volume": av, "order_book_imbalance": (bv-av)/(bv+av) if bv+av else 0.0, "observed_at": collected_at})
        return result
    def collect_liquidations(self, duration_seconds: int, collected_at: int) -> List[Dict[str, Any]]:
        url = FUTURES_WS_BASE_URL + "/".join(f"{s.lower()}@forceOrder" for s in self.symbols[:3]); events = []; deadline = time.monotonic() + max(1, duration_seconds)
        ws = websocket.create_connection(url, timeout=15)
        try:
            while time.monotonic() < deadline:
                ws.settimeout(min(15, max(1, deadline-time.monotonic())))
                try: payload = json.loads(ws.recv()).get("data", {})
                except websocket.WebSocketTimeoutException: continue
                order = payload.get("o", {})
                if order:
                    s = order["s"]
                    pid = SYMBOL_TO_PAIR_ID.get(s, 0)
                    events.append({"pair_id": pid, "event_type": payload.get("e", "forceOrder"), "event_time": int(payload.get("E", collected_at)), "symbol": s, "side": order["S"], "price": float(order["p"]), "quantity": float(order["q"]), "order_id": int(order.get("i", 0)), "order_type": order.get("o", ""), "time_in_force": order.get("f", ""), "order_status": order.get("X", ""), "average_price": float(order.get("ap", 0)), "filled_quantity": float(order.get("z", 0)), "order_time": int(order.get("T", collected_at)), "collected_at": collected_at})
        finally: ws.close()
        return events

class MarketMetricsPersistenceOperator:
    DATASETS = ("klines", "agg_trades", "order_book_depth", "futures_liquidations")
    def __init__(self): self.lakehouse, self.clickhouse = LakehouseTaskOperator(), ClickHouseTaskOperator()
    def write_layer(self, layer, run_key, records):
        bucket = self.lakehouse.bronze_bucket if layer == "bronze" else self.lakehouse.silver_bucket
        return {d: self.lakehouse.write_market_metrics(bucket, d, run_key, records.get(d, [])) for d in self.DATASETS}
    def read_layer(self, paths): return {d: self.lakehouse.read_market_metrics(p) for d, p in paths.items()}
    def load_gold(self, records):
        self.clickhouse.ensure_market_metric_tables()
        return {d: self.clickhouse.insert_market_metric_rows(d, r) for d, r in records.items()}
