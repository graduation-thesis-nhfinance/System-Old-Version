"""
Catalog Product - Lakehouse Data Dictionary & Schema Catalog
Organized by Data Lakehouse Layers (Bronze, Silver, Gold, Governance) and Tables.
Queries ClickHouse system.columns & system.tables with enriched domain descriptions.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
from app.connectors.clickhouse import clickhouse_connector
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/catalog", tags=["catalog"])

# Layer classifications
LAYER_DEFINITIONS = {
    "BRONZE": {
        "id": "bronze",
        "name": "Bronze Layer",
        "badge": "Raw Ingestion",
        "color": "amber",
        "description": "Raw unparsed / semi-structured ingestion payload directly from Binance WebSocket / REST API to MinIO & ClickHouse",
        "match_databases": ["db_bronze"],
        "match_tables": ["crypto_trades_raw", "crypto_orderbook_depth", "crypto_liquidations"]
    },
    "SILVER": {
        "id": "silver",
        "name": "Silver Layer",
        "badge": "Cleaned & Standardized",
        "color": "slate",
        "description": "Deduplicated, schema-validated, and structured financial time series data with standardized timestamps and types",
        "match_databases": ["db_silver_market", "db_silver_orderbook", "db_silver_derivatives"],
        "match_tables": []
    },
    "GOLD": {
        "id": "gold",
        "name": "Gold Layer",
        "badge": "Analytics & Indicators",
        "color": "yellow",
        "description": "Aggregated analytics, technical indicators (RSI, MACD, EMA), orderbook depth metrics, ML features, and dimensions",
        "match_databases": ["db_gold_analytics"],
        "match_tables": [
            "crypto_klines_gold", "crypto_agg_trades_gold", "crypto_order_book_depth_gold",
            "crypto_futures_liquidations_gold", "dim_trading_pairs", "mv_hourly_volume",
            "ai_hourly_inference_results", "ai_forecast_reconciliation"
        ]
    },
    "FEATURE_STORE": {
        "id": "feature_store",
        "name": "Feature Store (SYS-04)",
        "badge": "ML Features & Risk",
        "color": "emerald",
        "description": "Risk scoring feature store, transaction behavioral snapshots, anomaly indicators, and fraud labels for machine learning models (LSTM, Isolation Forest, LightGBM)",
        "match_databases": ["db_gold_fraud", "db_bronze_fraud"],
        "match_tables": ["gold_transaction_risk_features", "fraud_labels", "raw_transactions", "v_lightgbm_fraud_training"]
    },
    "GOVERNANCE": {
        "id": "governance",
        "name": "Governance & Ops",
        "badge": "Quality & Audit",
        "color": "blue",
        "description": "Data quality test results, reconciliation reports, and pipeline execution audit logs",
        "match_databases": [],
        "match_tables": ["data_quality_results", "pipeline_batch_status"]
    }
}

# Domain dictionary descriptions for known financial fields
COLUMN_DICTIONARY: Dict[str, Dict[str, str]] = {
    "pair_id": {"desc": "Internal unique identifier of trading pair (foreign key to dim_trading_pairs)", "example": "1"},
    "symbol": {"desc": "Market trading pair symbol formatted in uppercase", "example": "BTCUSDT"},
    "market_type": {"desc": "Contract specification category: spot, futures, or stock", "example": "spot"},
    "interval": {"desc": "Candle aggregation timeframe interval", "example": "1m, 15m, 1h"},
    "source_type": {"desc": "Source pipeline protocol identifier: rest_api or websocket_wsis", "example": "rest_api"},
    "ingestion_time": {"desc": "UTC timestamp when the event was ingested by the portal pipeline", "example": "2026-09-19 02:00:00.123"},
    "raw_payload": {"desc": "Raw JSON string message received directly from Binance upstream", "example": "{\"e\":\"kline\",...}"},
    "depth_type": {"desc": "Order book snapshot depth limit: depth5, depth10, depth20, or full", "example": "depth20"},
    
    # Prices & Volume
    "open_time": {"desc": "Opening timestamp of candle interval in milliseconds or UTC DateTime", "example": "2026-09-19 02:00:00"},
    "close_time": {"desc": "Closing timestamp of candle interval in milliseconds or UTC DateTime", "example": "2026-09-19 02:14:59"},
    "open_price": {"desc": "First executed trade price within the time window", "example": "64520.50"},
    "high_price": {"desc": "Highest executed trade price reached during the interval", "example": "64890.00"},
    "low_price": {"desc": "Lowest executed trade price reached during the interval", "example": "64310.20"},
    "close_price": {"desc": "Last executed trade price recorded at the close of the interval", "example": "64750.00"},
    "volume": {"desc": "Total traded volume denominated in base asset (e.g. BTC)", "example": "1428.55"},
    "quote_volume": {"desc": "Total traded notional turnover in quote asset (USDT)", "example": "92543180.20"},
    "trades_count": {"desc": "Total count of individual trade executions inside interval", "example": "18450"},
    "taker_buy_base_volume": {"desc": "Base asset volume bought aggressively by market takers", "example": "720.12"},
    "taker_buy_quote_volume": {"desc": "Quote asset turnover bought aggressively by market takers", "example": "46680210.00"},
    "vwap": {"desc": "Volume-Weighted Average Price calculated for this window", "example": "64685.32"},

    # Trade activity
    "agg_trade_id": {"desc": "Unique aggregate trade ID generated by the exchange matching engine", "example": "49823101"},
    "price": {"desc": "Executed unit price of the fill transaction", "example": "64720.10"},
    "quantity": {"desc": "Quantity of base units filled in the transaction", "example": "0.452"},
    "first_trade_id": {"desc": "First individual fill trade identifier included in aggregate trade", "example": "1002341"},
    "last_trade_id": {"desc": "Last individual fill trade identifier included in aggregate trade", "example": "1002348"},
    "transact_time": {"desc": "Exchange matching engine timestamp when trade settled", "example": "2026-09-19 02:00:05"},
    "is_buyer_maker": {"desc": "Order book side: 1 if buyer was maker (ask filled), 0 if seller was maker", "example": "1"},
    "is_best_match": {"desc": "Flag denoting execution at best bid/ask", "example": "1"},

    # Orderbook Depth
    "last_update_id": {"desc": "Monotonically increasing sequence ID of the orderbook state", "example": "1847291032"},
    "bids_price": {"desc": "Array of bid price levels ordered descending from top of book", "example": "[64720.0, 64719.5, ...]"},
    "bids_quantity": {"desc": "Array of liquidity volume quantities at corresponding bid prices", "example": "[2.45, 8.12, ...]"},
    "asks_price": {"desc": "Array of ask price levels ordered ascending from top of book", "example": "[64720.5, 64721.0, ...]"},
    "asks_quantity": {"desc": "Array of liquidity volume quantities at corresponding ask prices", "example": "[3.10, 6.75, ...]"},
    "spread": {"desc": "Absolute price gap between Best Ask and Best Bid (Ask[0] - Bid[0])", "example": "0.50"},
    "spread_bps": {"desc": "Bid-Ask spread expressed in basis points relative to mid price", "example": "0.77"},
    "bid_depth_usd": {"desc": "Aggregated dollar liquidity available on the buy side within top N levels", "example": "450000.00"},
    "ask_depth_usd": {"desc": "Aggregated dollar liquidity available on the sell side within top N levels", "example": "512000.00"},
    "imbalance_ratio": {"desc": "Order book pressure metric: (BidQty - AskQty) / (BidQty + AskQty)", "example": "-0.064"},

    # Liquidations
    "order_id": {"desc": "Forced margin liquidation order reference ID", "example": "8931201"},
    "side": {"desc": "Liquidation forced action: BUY (short liquidated) or SELL (long liquidated)", "example": "SELL"},
    "order_type": {"desc": "Liquidation order fill type (usually LIMIT or MARKET)", "example": "LIMIT"},
    "time_in_force": {"desc": "Execution policy: IOC (Immediate Or Cancel) or GTC", "example": "IOC"},
    "original_quantity": {"desc": "Total position quantity liquidated by the exchange risk engine", "example": "12.50"},
    "accumulated_quantity": {"desc": "Cumulative quantity executed against liquidators", "example": "12.50"},
    "average_price": {"desc": "Average execution fill price of the liquidation", "example": "63920.00"},
    "liquidation_usd": {"desc": "Estimated notional dollar loss value of the liquidated contract", "example": "799000.00"},

    # Technical Indicators & ML
    "ema_9": {"desc": "9-period Exponential Moving Average of close price", "example": "64610.12"},
    "ema_21": {"desc": "21-period Exponential Moving Average for trend detection", "example": "64480.45"},
    "sma_50": {"desc": "50-period Simple Moving Average baseline", "example": "64120.00"},
    "rsi_14": {"desc": "14-period Relative Strength Index momentum oscillator (0-100)", "example": "58.42"},
    "macd": {"desc": "Moving Average Convergence Divergence line (12 EMA - 26 EMA)", "example": "142.10"},
    "macd_signal": {"desc": "9-period EMA signal line of the MACD oscillator", "example": "128.50"},
    "macd_hist": {"desc": "MACD histogram bar showing momentum divergence", "example": "13.60"},
    "bb_upper": {"desc": "Bollinger Band Upper threshold (SMA + 2 StdDev)", "example": "65120.00"},
    "bb_middle": {"desc": "Bollinger Band Middle centerline (20 SMA)", "example": "64500.00"},
    "bb_lower": {"desc": "Bollinger Band Lower threshold (SMA - 2 StdDev)", "example": "63880.00"},
    "atr_14": {"desc": "14-period Average True Range measuring market volatility", "example": "315.40"},
    "feature_vector": {"desc": "Normalized feature embeddings array ready for ML inference", "example": "[0.12, -0.45, ...]"},

    # Dimension & Governance
    "base_coin": {"desc": "Base asset code (e.g. BTC, ETH, SOL, AAPLB)", "example": "BTC"},
    "quote_coin": {"desc": "Counter currency quote asset code (e.g. USDT)", "example": "USDT"},
    "trading_pair": {"desc": "Standard display notation of the currency pair", "example": "BTC/USDT"},
    "category": {"desc": "Asset taxonomy class: crypto, equity, tech_stock, etc.", "example": "crypto"},
    "is_active": {"desc": "Pipeline active ingestion flag: 1 = Active, 0 = Deactivated", "example": "1"},
    "updated_at": {"desc": "Timestamp of most recent record revision in GMT+7", "example": "2026-09-19 02:20:00"},
    "check_name": {"desc": "Name of automated Great Expectations / ClickHouse DQ assertion", "example": "null_check_price"},
    "status": {"desc": "Status outcome of test execution: PASSED, FAILED, WARNING", "example": "PASSED"},
    "batch_id": {"desc": "Airflow DAG execution run batch partition key", "example": "batch_20260919_020000"},

    # AI Models & Governance (SYS-04)
    "predicted_price_60m": {"desc": "Forecasted asset price 60 minutes ahead generated by Attention-Residual LSTM", "example": "85292.25"},
    "predicted_direction": {"desc": "Expected directional movement (UP or DOWN) relative to anchor price", "example": "UP"},
    "predicted_delta_pct": {"desc": "Projected return delta percentage for the horizon window", "example": "0.061"},
    "actual_price": {"desc": "Realized market settlement price at close of evaluation interval", "example": "85240.00"},
    "actual_direction": {"desc": "Observed market directional movement (UP or DOWN)", "example": "UP"},
    "actual_delta_pct": {"desc": "Realized percentage price return across the evaluation window", "example": "0.058"},
    "direction_hit": {"desc": "Binary classification indicator (1 = Predicted direction hit correctly, 0 = Missed)", "example": "1"},
    "absolute_error": {"desc": "Absolute difference in USD between predicted and actual realized price", "example": "12.58"},
    "percentage_error": {"desc": "Relative percentage error (MAPE point contribution) of the forecast", "example": "0.015"},
    "anomaly_score": {"desc": "Calibrated market microstructure stress and anomaly score from Isolation Forest (0.0 to 1.0)", "example": "0.3312"},
    "risk_level": {"desc": "Risk Engine classification tier: NORMAL, ELEVATED, or CRITICAL", "example": "NORMAL"},
    "leverage_limit": {"desc": "Enforced maximum allowable leverage cap under automated Risk Guard filter", "example": "20"},
    "spread_buffer": {"desc": "Dynamic orderbook bid/ask spread widening policy (e.g. Standard, +5 bps, +15 bps)", "example": "Standard"},
    "fraud_decision": {"desc": "3-tier AML gate verdict: APPROVE, MANUAL_REVIEW, or BLOCK_TRANSACTION", "example": "APPROVE"},
    "fraud_risk_score": {"desc": "Predicted probability of fraudulent transaction activity from LightGBM model", "example": "0.124"}
}

def determine_table_layer(database: str, table_name: str) -> Dict[str, Any]:
    """Classifies a table into Bronze, Silver, Gold, or Governance."""
    for layer_key, info in LAYER_DEFINITIONS.items():
        if database in info["match_databases"]:
            return info
        if table_name in info["match_tables"]:
            return info
    if "gold" in table_name or "gold" in database:
        return LAYER_DEFINITIONS["GOLD"]
    if "silver" in table_name or "silver" in database:
        return LAYER_DEFINITIONS["SILVER"]
    if "raw" in table_name or "bronze" in database:
        return LAYER_DEFINITIONS["BRONZE"]
    return LAYER_DEFINITIONS["GOLD"]

@router.get("")
def get_catalog(
    layer: Optional[str] = Query(None, description="Filter by layer: bronze, silver, gold, governance"),
    search: Optional[str] = Query(None, description="Search keyword across tables and columns")
):
    """
    Returns full Data Lakehouse catalog of tables and column definitions across all layers.
    """
    try:
        # 1. Fetch tables metadata
        tables_query = """
        SELECT 
            database, 
            name as table_name, 
            engine, 
            total_rows, 
            comment as table_comment,
            sorting_key,
            partition_key
        FROM system.tables 
        WHERE database IN ('db_bronze', 'db_silver_market', 'db_silver_orderbook', 'db_silver_derivatives', 'db_gold_analytics', 'default')
          AND name NOT LIKE '.%'
        ORDER BY database, name
        """
        raw_tables = clickhouse_connector.query(tables_query)
        
        # 2. Fetch all columns metadata
        cols_query = """
        SELECT 
            database, 
            table as table_name, 
            name as column_name, 
            type as data_type, 
            position,
            is_in_primary_key,
            is_in_sorting_key,
            is_in_partition_key,
            comment as column_comment
        FROM system.columns 
        WHERE database IN ('db_bronze', 'db_silver_market', 'db_silver_orderbook', 'db_silver_derivatives', 'db_gold_analytics', 'default')
          AND table NOT LIKE '.%'
        ORDER BY database, table, position
        """
        raw_columns = clickhouse_connector.query(cols_query)

        # Group columns by (database, table_name)
        cols_by_table: Dict[str, List[Dict[str, Any]]] = {}
        for col in raw_columns:
            key = f"{col['database']}.{col['table_name']}"
            if key not in cols_by_table:
                cols_by_table[key] = []
            
            c_name = col['column_name']
            dict_info = COLUMN_DICTIONARY.get(c_name.lower(), {})
            desc = col.get('column_comment') or dict_info.get("desc") or f"Column {c_name} for table {col['table_name']}"
            example = dict_info.get("example", "--")

            cols_by_table[key].append({
                "column_name": c_name,
                "data_type": col['data_type'],
                "position": col['position'],
                "is_primary_key": bool(col.get('is_in_primary_key', 0)),
                "is_sorting_key": bool(col.get('is_in_sorting_key', 0)),
                "is_partition_key": bool(col.get('is_in_partition_key', 0)),
                "description": desc,
                "example_value": example
            })

        # Assemble catalog response
        catalog_tables = []
        layer_stats = {"bronze": 0, "silver": 0, "gold": 0, "governance": 0}
        total_columns_count = 0

        for t in raw_tables:
            db = t['database']
            t_name = t['table_name']
            key = f"{db}.{t_name}"
            layer_info = determine_table_layer(db, t_name)
            layer_id = layer_info["id"]

            table_cols = cols_by_table.get(key, [])
            total_columns_count += len(table_cols)
            layer_stats[layer_id] = layer_stats.get(layer_id, 0) + 1

            # Filter by layer if specified
            if layer and layer.lower() != "all" and layer_id != layer.lower():
                continue

            # Search keyword match
            if search:
                q = search.lower()
                matches_table = q in t_name.lower() or q in db.lower() or q in layer_info['name'].lower()
                matches_col = any(q in c['column_name'].lower() or q in c['data_type'].lower() or q in c['description'].lower() for c in table_cols)
                if not (matches_table or matches_col):
                    continue

            # Table description
            t_comment = t.get('table_comment') or ""
            if not t_comment:
                if "kline" in t_name:
                    t_comment = "OHLCV Candlestick price time-series with volume and window indicators"
                elif "orderbook" in t_name or "depth" in t_name:
                    t_comment = "Order book L2 liquidity snapshots, bids/asks arrays, and spread telemetry"
                elif "trade" in t_name:
                    t_comment = "Individual and aggregate trade executions with buyer maker flags and VWAP"
                elif "liquidation" in t_name:
                    t_comment = "Forced margin liquidation events from Binance Futures risk engine"
                elif "pair" in t_name:
                    t_comment = "Core dimensional reference catalog for all active and supported trading instruments"
                elif "quality" in t_name:
                    t_comment = "Data quality validation test results and anomaly detection metrics"
                elif "batch" in t_name:
                    t_comment = "Orchestration batch tracking and Airflow pipeline run statuses"
                else:
                    t_comment = f"{layer_info['badge']} dataset for {t_name}"

            catalog_tables.append({
                "database": db,
                "table_name": t_name,
                "full_name": key,
                "layer_id": layer_id,
                "layer_name": layer_info["name"],
                "layer_badge": layer_info["badge"],
                "layer_color": layer_info["color"],
                "layer_desc": layer_info["description"],
                "engine": t.get("engine", "MergeTree"),
                "total_rows": t.get("total_rows") or 0,
                "sorting_key": t.get("sorting_key") or "--",
                "partition_key": t.get("partition_key") or "--",
                "description": t_comment,
                "column_count": len(table_cols),
                "columns": table_cols
            })

        return {
            "status": "success",
            "total_tables": len(catalog_tables),
            "total_columns": total_columns_count,
            "layer_stats": layer_stats,
            "tables": catalog_tables
        }

    except Exception as e:
        logger.error(f"Catalog query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch catalog: {str(e)}")

@router.get("/layers")
def get_layer_metadata():
    """Returns layer taxonomy and architectural definitions."""
    return {
        "status": "success",
        "layers": list(LAYER_DEFINITIONS.values())
    }
