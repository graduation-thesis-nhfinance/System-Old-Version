-- ============================================================================
-- SYS-05: LAKEHOUSE & DATABASE MANAGEMENT PORTAL
-- ClickHouse DDL Script: 3-Layer Medallion Architecture (Bronze, Silver, Gold)
-- Covers 4 Categories: Klines, Trades (aggTrade), Order Book Depth, Liquidations
-- Plus Dimension Trading Pairs with pair_id (1-6) from binance_market_data_reference.md
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. METADATA & DIMENSION TABLE (IN DEFAULT & GLOBAL)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default.dim_trading_pairs (
    pair_id UInt8,
    base_coin LowCardinality(String),
    quote_coin LowCardinality(String),
    trading_pair LowCardinality(String),
    symbol LowCardinality(String),
    market_type LowCardinality(String),
    category LowCardinality(String),
    is_active UInt8 DEFAULT 1,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (pair_id)
ORDER BY (pair_id);

INSERT INTO default.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active) VALUES
    (1, 'BTC', 'USDT', 'BTC/USDT', 'BTCUSDT', 'SPOT_FUTURES', 'crypto_popular', 1),
    (2, 'ETH', 'USDT', 'ETH/USDT', 'ETHUSDT', 'SPOT_FUTURES', 'crypto_popular', 1),
    (3, 'BNB', 'USDT', 'BNB/USDT', 'BNBUSDT', 'SPOT_FUTURES', 'crypto_popular', 1),
    (4, 'AAPLB', 'USDT', 'AAPLB/USDT', 'AAPLBUSDT', 'SPOT', 'equity_token', 1),
    (5, 'AMZNB', 'USDT', 'AMZNB/USDT', 'AMZNBUSDT', 'SPOT', 'equity_token', 1),
    (6, 'DELLB', 'USDT', 'DELLB/USDT', 'DELLBUSDT', 'SPOT', 'equity_token', 1);

-- ----------------------------------------------------------------------------
-- 1. BRONZE LAYER (RAW INGESTION STAGING)
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS db_bronze;

CREATE TABLE IF NOT EXISTS db_bronze.raw_binance_klines (
    pair_id UInt8,
    symbol LowCardinality(String),
    market_type LowCardinality(String),
    interval LowCardinality(String),
    source_type LowCardinality(String),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String
) ENGINE = MergeTree()
PARTITION BY (toYYYYMM(ingestion_time), market_type)
ORDER BY (symbol, interval, ingestion_time);

CREATE TABLE IF NOT EXISTS db_bronze.raw_binance_trades (
    pair_id UInt8,
    symbol LowCardinality(String),
    market_type LowCardinality(String),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingestion_time)
ORDER BY (symbol, ingestion_time);

CREATE TABLE IF NOT EXISTS db_bronze.raw_binance_orderbook (
    pair_id UInt8,
    symbol LowCardinality(String),
    market_type LowCardinality(String),
    depth_type LowCardinality(String),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingestion_time)
ORDER BY (symbol, depth_type, ingestion_time);

CREATE TABLE IF NOT EXISTS db_bronze.raw_binance_liquidations (
    pair_id UInt8,
    symbol LowCardinality(String),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingestion_time)
ORDER BY (symbol, ingestion_time);

-- ----------------------------------------------------------------------------
-- 2. SILVER LAYER (CLEANSED, STRUCTURED, DEDUPLICATED)
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS db_silver_market;
CREATE DATABASE IF NOT EXISTS db_silver_orderbook;
CREATE DATABASE IF NOT EXISTS db_silver_derivatives;

-- Silver Klines (All 12 REST Elements + WebSocket Fields + pair_id)
CREATE TABLE IF NOT EXISTS db_silver_market.silver_crypto_klines (
    pair_id UInt8,
    market_type LowCardinality(String),
    symbol LowCardinality(String),
    interval LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_asset_volume Float64,
    number_of_trades UInt32,
    taker_buy_base_volume Float64,
    taker_buy_quote_volume Float64,
    first_trade_id Int64 DEFAULT -1,
    last_trade_id Int64 DEFAULT -1,
    is_kline_closed UInt8 DEFAULT 1,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (market_type, toYYYYMM(open_time))
PRIMARY KEY (symbol, interval, open_time)
ORDER BY (symbol, interval, open_time);

-- Silver Trades (aggTrade Format + pair_id)
CREATE TABLE IF NOT EXISTS db_silver_market.silver_trades_aggregated (
    pair_id UInt8,
    market_type LowCardinality(String),
    symbol LowCardinality(String),
    agg_trade_id UInt64,
    price Float64,
    quantity Float64,
    first_trade_id UInt64,
    last_trade_id UInt64,
    trade_time DateTime64(3, 'UTC'),
    is_buyer_maker UInt8,
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(trade_time)
PRIMARY KEY (symbol, trade_time, agg_trade_id)
ORDER BY (symbol, trade_time, agg_trade_id);

-- Silver Order Book Snapshots (Top 10/20 Nested Arrays + pair_id)
CREATE TABLE IF NOT EXISTS db_silver_orderbook.silver_orderbook_snapshots (
    pair_id UInt8,
    market_type LowCardinality(String),
    symbol LowCardinality(String),
    last_update_id UInt64,
    event_time DateTime64(3, 'UTC'),
    bids Nested(price Float64, quantity Float64),
    asks Nested(price Float64, quantity Float64),
    depth_level UInt16,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (symbol, event_time, last_update_id);

-- Silver Order Book Delta Updates (diffUpdate + pair_id)
CREATE TABLE IF NOT EXISTS db_silver_orderbook.silver_orderbook_deltas (
    pair_id UInt8,
    symbol LowCardinality(String),
    first_update_id UInt64,
    final_update_id UInt64,
    event_time DateTime64(3, 'UTC'),
    bids_updated Nested(price Float64, quantity Float64),
    asks_updated Nested(price Float64, quantity Float64),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (symbol, event_time, final_update_id);

-- Silver Futures Liquidations (forceOrder + pair_id)
CREATE TABLE IF NOT EXISTS db_silver_derivatives.silver_futures_liquidations (
    pair_id UInt8,
    symbol LowCardinality(String),
    side LowCardinality(String),
    order_type LowCardinality(String),
    time_in_force LowCardinality(String),
    original_quantity Float64,
    trigger_price Float64,
    average_price Float64,
    order_status LowCardinality(String),
    last_filled_quantity Float64,
    accumulated_filled_quantity Float64,
    trade_time DateTime64(3, 'UTC'),
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(trade_time)
PRIMARY KEY (symbol, trade_time)
ORDER BY (symbol, trade_time, side);

-- ----------------------------------------------------------------------------
-- 3. GOLD LAYER (DATA MARTS & ML FEATURE STORE)
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS db_gold_analytics;

-- Gold Technical Indicators Table
CREATE TABLE IF NOT EXISTS db_gold_analytics.gold_klines_technical_indicators (
    pair_id UInt8,
    market_type LowCardinality(String),
    symbol LowCardinality(String),
    interval LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_volume Float64,
    trades_count UInt32,
    taker_buy_ratio Float64,
    vwap Float64,
    sma_20 Float64,
    sma_50 Float64,
    sma_200 Float64,
    ema_12 Float64,
    ema_26 Float64,
    rsi_14 Float64,
    macd Float64,
    macd_signal Float64,
    macd_hist Float64,
    bb_upper Float64,
    bb_middle Float64,
    bb_lower Float64,
    atr_14 Float64,
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY (interval, toYYYYMM(open_time))
PRIMARY KEY (symbol, interval, open_time)
ORDER BY (symbol, interval, open_time);

-- Gold Order Book Imbalance (1m)
CREATE TABLE IF NOT EXISTS db_gold_analytics.gold_orderbook_imbalance_1m (
    pair_id UInt8,
    symbol LowCardinality(String),
    bucket_time DateTime64(3, 'UTC'),
    best_bid Float64,
    best_ask Float64,
    spread_absolute Float64,
    spread_bps Float64,
    bid_depth_qty_top10 Float64,
    ask_depth_qty_top10 Float64,
    orderbook_imbalance Float64,
    total_depth_liquidity_usd Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(bucket_time)
PRIMARY KEY (symbol, bucket_time)
ORDER BY (symbol, bucket_time);

-- Gold Liquidation Stats (Hourly)
CREATE TABLE IF NOT EXISTS db_gold_analytics.gold_liquidation_stats_hourly (
    pair_id UInt8,
    symbol LowCardinality(String),
    hourly_time DateTime64(3, 'UTC'),
    total_liquidations_count UInt32,
    long_liquidated_orders UInt32,
    short_liquidated_orders UInt32,
    total_long_liquidated_usd Float64,
    total_short_liquidated_usd Float64,
    max_single_liquidation_usd Float64,
    long_short_liquidation_ratio Float64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hourly_time)
PRIMARY KEY (symbol, hourly_time)
ORDER BY (symbol, hourly_time);

-- Gold ML Feature Store (1m)
CREATE TABLE IF NOT EXISTS db_gold_analytics.gold_ml_features_1m (
    pair_id UInt8,
    symbol LowCardinality(String),
    feature_timestamp DateTime64(3, 'UTC'),
    log_return Float64,
    volatility_parkinson Float64,
    volume_surge_ratio Float64,
    taker_imbalance Float64,
    orderbook_pressure Float64,
    recent_liquidation_usd Float64,
    rsi_14 Float64,
    target_return_5m_forward Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(feature_timestamp)
PRIMARY KEY (symbol, feature_timestamp)
ORDER BY (symbol, feature_timestamp);

-- ----------------------------------------------------------------------------
-- 4. BACKWARD COMPATIBILITY VIEWS & PIPELINE AUDIT TABLES IN DEFAULT
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default.data_quality_results (
    batch_id String,
    dataset LowCardinality(String),
    symbol LowCardinality(String),
    pair_id UInt8 DEFAULT 0,
    check_time DateTime DEFAULT now(),
    passed UInt8,
    total_records UInt64,
    failed_records UInt64,
    null_close_count UInt64,
    negative_volume_count UInt64,
    duplicate_key_count UInt64,
    interval_gap_count UInt64,
    error_summary String
) ENGINE = MergeTree()
ORDER BY (dataset, symbol, check_time, batch_id);

CREATE TABLE IF NOT EXISTS default.pipeline_batch_status (
    batch_id String,
    pipeline LowCardinality(String),
    symbol LowCardinality(String),
    pair_id UInt8 DEFAULT 0,
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
) ENGINE = ReplacingMergeTree(finished_at)
ORDER BY (pipeline, symbol, batch_id);

-- Backward-compatible default tables and synchronization Materialized Views
CREATE TABLE IF NOT EXISTS default.crypto_klines_gold (
    pair_id UInt8 DEFAULT 0,
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_asset_volume Float64 DEFAULT 0.0,
    number_of_trades UInt32 DEFAULT 0,
    taker_buy_base_asset_volume Float64 DEFAULT 0.0,
    taker_buy_quote_asset_volume Float64 DEFAULT 0.0,
    sma_20 Float64 DEFAULT 0.0,
    rsi_14 Float64 DEFAULT 0.0,
    collected_at DateTime64(3, 'UTC') DEFAULT now64(3),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time);

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

CREATE MATERIALIZED VIEW IF NOT EXISTS db_gold_analytics.mv_gold_klines_sync
TO db_gold_analytics.gold_klines_technical_indicators
AS SELECT
    pair_id,
    'SPOT' AS market_type,
    symbol,
    '1m' AS interval,
    open_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    quote_asset_volume AS quote_volume,
    number_of_trades AS trades_count,
    if(volume > 0, taker_buy_base_asset_volume / volume, 0.0) AS taker_buy_ratio,
    if(volume > 0, quote_asset_volume / volume, close) AS vwap,
    sma_20,
    0.0 AS sma_50,
    0.0 AS sma_200,
    0.0 AS ema_12,
    0.0 AS ema_26,
    rsi_14,
    0.0 AS macd,
    0.0 AS macd_signal,
    0.0 AS macd_hist,
    0.0 AS bb_upper,
    0.0 AS bb_middle,
    0.0 AS bb_lower,
    0.0 AS atr_14,
    now() AS calculated_at
FROM default.crypto_klines_gold;

CREATE TABLE IF NOT EXISTS default.crypto_trades_raw (
    symbol LowCardinality(String),
    agg_trade_id UInt64,
    price Float64,
    quantity Float64,
    first_trade_id UInt64 DEFAULT 0,
    last_trade_id UInt64 DEFAULT 0,
    trade_time DateTime64(3, 'UTC'),
    is_buyer_maker UInt8 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PRIMARY KEY (symbol, trade_time)
ORDER BY (symbol, trade_time, agg_trade_id);

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
) ENGINE = MergeTree()
PRIMARY KEY (symbol, observed_at)
ORDER BY (symbol, observed_at, last_update_id);

CREATE TABLE IF NOT EXISTS default.crypto_liquidations (
    symbol LowCardinality(String),
    side LowCardinality(String),
    price Float64,
    quantity Float64,
    trade_time DateTime64(3, 'UTC'),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PRIMARY KEY (symbol, trade_time)
ORDER BY (symbol, trade_time);

CREATE TABLE IF NOT EXISTS default.crypto_trading_pairs (
    symbol LowCardinality(String),
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    category LowCardinality(String),
    is_active UInt8 DEFAULT 1,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (symbol)
ORDER BY (symbol);

INSERT INTO default.crypto_trading_pairs (symbol, base_asset, quote_asset, category, is_active) VALUES
    ('BTCUSDT', 'BTC', 'USDT', 'popular', 1),
    ('ETHUSDT', 'ETH', 'USDT', 'popular', 1),
    ('BNBUSDT', 'BNB', 'USDT', 'popular', 1),
    ('AAPLBUSDT', 'AAPLB', 'USDT', 'new_coin', 1),
    ('AMZNBUSDT', 'AMZNB', 'USDT', 'new_coin', 1),
    ('DELLBUSDT', 'DELLB', 'USDT', 'new_coin', 1);


