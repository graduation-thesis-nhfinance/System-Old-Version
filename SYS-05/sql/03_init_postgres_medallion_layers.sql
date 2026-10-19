-- ============================================================================
-- SYS-05: PostgreSQL Medallion Architecture Schema Migration
-- Database: enterprise_metadata
-- 1. Schema gold_layer for Gold Layer Data Marts & ML Feature Stores
-- 2. Schema public for Silver Cleansed, Operational, and Pipeline Tables
-- 3. Both schemas contain dim_trading_pairs & data_quality_audits (bi-directional synced)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CREATE GOLD_LAYER SCHEMA & ASSIGN PRIVILEGES
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS gold_layer;
GRANT ALL ON SCHEMA gold_layer TO admin123;
GRANT ALL ON SCHEMA public TO admin123;

-- ----------------------------------------------------------------------------
-- 2. DUAL-SCHEMA PRESENCE FOR EXISTING TABLES
--    (dim_trading_pairs & data_quality_audits present in BOTH public and gold_layer)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_layer.dim_trading_pairs (LIKE public.dim_trading_pairs INCLUDING ALL);
INSERT INTO gold_layer.dim_trading_pairs SELECT * FROM public.dim_trading_pairs ON CONFLICT (pair_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS gold_layer.data_quality_audits (LIKE public.data_quality_audits INCLUDING ALL);
INSERT INTO gold_layer.data_quality_audits SELECT * FROM public.data_quality_audits ON CONFLICT (audit_id) DO NOTHING;

-- Automatic Bi-directional Synchronization Triggers
CREATE OR REPLACE FUNCTION sync_dim_trading_pairs_to_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN RETURN NEW; END IF;
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO gold_layer.dim_trading_pairs VALUES (NEW.*)
        ON CONFLICT (pair_id) DO UPDATE SET
            base_coin = EXCLUDED.base_coin, quote_coin = EXCLUDED.quote_coin,
            trading_pair = EXCLUDED.trading_pair, symbol = EXCLUDED.symbol,
            market_type = EXCLUDED.market_type, category = EXCLUDED.category,
            is_active = EXCLUDED.is_active, updated_at = EXCLUDED.updated_at;
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE gold_layer.dim_trading_pairs SET
            base_coin = NEW.base_coin, quote_coin = NEW.quote_coin,
            trading_pair = NEW.trading_pair, symbol = NEW.symbol,
            market_type = NEW.market_type, category = NEW.category,
            is_active = NEW.is_active, updated_at = NEW.updated_at
        WHERE pair_id = NEW.pair_id;
    ELSIF (TG_OP = 'DELETE') THEN
        DELETE FROM gold_layer.dim_trading_pairs WHERE pair_id = OLD.pair_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_dim_pairs_to_gold ON public.dim_trading_pairs;
CREATE TRIGGER trg_sync_dim_pairs_to_gold
AFTER INSERT OR UPDATE OR DELETE ON public.dim_trading_pairs
FOR EACH ROW EXECUTE FUNCTION sync_dim_trading_pairs_to_gold();

CREATE OR REPLACE FUNCTION sync_dim_trading_pairs_from_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN RETURN NEW; END IF;
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO public.dim_trading_pairs VALUES (NEW.*)
        ON CONFLICT (pair_id) DO UPDATE SET
            base_coin = EXCLUDED.base_coin, quote_coin = EXCLUDED.quote_coin,
            trading_pair = EXCLUDED.trading_pair, symbol = EXCLUDED.symbol,
            market_type = EXCLUDED.market_type, category = EXCLUDED.category,
            is_active = EXCLUDED.is_active, updated_at = EXCLUDED.updated_at;
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE public.dim_trading_pairs SET
            base_coin = NEW.base_coin, quote_coin = NEW.quote_coin,
            trading_pair = NEW.trading_pair, symbol = NEW.symbol,
            market_type = NEW.market_type, category = NEW.category,
            is_active = NEW.is_active, updated_at = NEW.updated_at
        WHERE pair_id = NEW.pair_id;
    ELSIF (TG_OP = 'DELETE') THEN
        DELETE FROM public.dim_trading_pairs WHERE pair_id = OLD.pair_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_dim_pairs_from_gold ON gold_layer.dim_trading_pairs;
CREATE TRIGGER trg_sync_dim_pairs_from_gold
AFTER INSERT OR UPDATE OR DELETE ON gold_layer.dim_trading_pairs
FOR EACH ROW EXECUTE FUNCTION sync_dim_trading_pairs_from_gold();

CREATE OR REPLACE FUNCTION sync_dq_audits_to_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN RETURN NEW; END IF;
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO gold_layer.data_quality_audits VALUES (NEW.*)
        ON CONFLICT (audit_id) DO NOTHING;
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE gold_layer.data_quality_audits SET
            batch_id = NEW.batch_id, dataset_name = NEW.dataset_name,
            pair_id = NEW.pair_id, symbol = NEW.symbol, layer = NEW.layer,
            check_time = NEW.check_time, is_passed = NEW.is_passed,
            total_records = NEW.total_records, null_count = NEW.null_count,
            duplicate_count = NEW.duplicate_count, timestamp_gaps = NEW.timestamp_gaps,
            error_summary = NEW.error_summary
        WHERE audit_id = NEW.audit_id;
    ELSIF (TG_OP = 'DELETE') THEN
        DELETE FROM gold_layer.data_quality_audits WHERE audit_id = OLD.audit_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_dq_audits_to_gold ON public.data_quality_audits;
CREATE TRIGGER trg_sync_dq_audits_to_gold
AFTER INSERT OR UPDATE OR DELETE ON public.data_quality_audits
FOR EACH ROW EXECUTE FUNCTION sync_dq_audits_to_gold();

CREATE OR REPLACE FUNCTION sync_dq_audits_from_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN RETURN NEW; END IF;
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO public.data_quality_audits VALUES (NEW.*)
        ON CONFLICT (audit_id) DO NOTHING;
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE public.data_quality_audits SET
            batch_id = NEW.batch_id, dataset_name = NEW.dataset_name,
            pair_id = NEW.pair_id, symbol = NEW.symbol, layer = NEW.layer,
            check_time = NEW.check_time, is_passed = NEW.is_passed,
            total_records = NEW.total_records, null_count = NEW.null_count,
            duplicate_count = NEW.duplicate_count, timestamp_gaps = NEW.timestamp_gaps,
            error_summary = NEW.error_summary
        WHERE audit_id = NEW.audit_id;
    ELSIF (TG_OP = 'DELETE') THEN
        DELETE FROM public.data_quality_audits WHERE audit_id = OLD.audit_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_dq_audits_from_gold ON gold_layer.data_quality_audits;
CREATE TRIGGER trg_sync_dq_audits_from_gold
AFTER INSERT OR UPDATE OR DELETE ON gold_layer.data_quality_audits
FOR EACH ROW EXECUTE FUNCTION sync_dq_audits_from_gold();

-- ----------------------------------------------------------------------------
-- 3. SILVER LAYER TABLES (LOCATED IN PUBLIC SCHEMA)
-- ----------------------------------------------------------------------------

-- Silver Klines (Normalized REST + WebSocket OHLCV Streams)
CREATE TABLE IF NOT EXISTS public.silver_crypto_klines (
    kline_id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    market_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    interval VARCHAR(10) NOT NULL,
    open_time TIMESTAMP WITH TIME ZONE NOT NULL,
    close_time TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(24, 8) NOT NULL,
    quote_asset_volume NUMERIC(24, 8) DEFAULT 0.0,
    number_of_trades INTEGER DEFAULT 0,
    taker_buy_base_volume NUMERIC(24, 8) DEFAULT 0.0,
    taker_buy_quote_volume NUMERIC(24, 8) DEFAULT 0.0,
    first_trade_id BIGINT DEFAULT -1,
    last_trade_id BIGINT DEFAULT -1,
    is_kline_closed BOOLEAN DEFAULT TRUE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_silver_kline UNIQUE (symbol, interval, open_time)
);
CREATE INDEX IF NOT EXISTS idx_silver_klines_sym_time ON public.silver_crypto_klines(symbol, open_time DESC);

-- Silver Trades (Standardized aggTrade Stream)
CREATE TABLE IF NOT EXISTS public.silver_trades_aggregated (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    market_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    agg_trade_id BIGINT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    quantity NUMERIC(24, 8) NOT NULL,
    first_trade_id BIGINT,
    last_trade_id BIGINT,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_silver_agg_trade UNIQUE (symbol, trade_time, agg_trade_id)
);
CREATE INDEX IF NOT EXISTS idx_silver_trades_sym_time ON public.silver_trades_aggregated(symbol, trade_time DESC);

-- Silver Order Book Snapshots (L2 Depth Snapshots)
CREATE TABLE IF NOT EXISTS public.silver_orderbook_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    market_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    last_update_id BIGINT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    depth_level SMALLINT DEFAULT 20,
    bids_json JSONB NOT NULL,
    asks_json JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_silver_ob_snap_sym_time ON public.silver_orderbook_snapshots(symbol, event_time DESC);

-- Silver Order Book Deltas (diffUpdate L2 Stream)
CREATE TABLE IF NOT EXISTS public.silver_orderbook_deltas (
    delta_id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    first_update_id BIGINT NOT NULL,
    final_update_id BIGINT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    bids_updated_json JSONB NOT NULL,
    asks_updated_json JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_silver_ob_deltas_sym_time ON public.silver_orderbook_deltas(symbol, event_time DESC);

-- Silver Futures Liquidations (forceOrder Stream)
CREATE TABLE IF NOT EXISTS public.silver_futures_liquidations (
    liquidation_id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    order_type VARCHAR(20) DEFAULT 'LIMIT',
    time_in_force VARCHAR(10) DEFAULT 'IOC',
    original_quantity NUMERIC(24, 8) NOT NULL,
    trigger_price NUMERIC(18, 8) NOT NULL,
    average_price NUMERIC(18, 8) NOT NULL,
    order_status VARCHAR(20) DEFAULT 'FILLED',
    last_filled_quantity NUMERIC(24, 8),
    accumulated_filled_quantity NUMERIC(24, 8),
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_silver_liq_sym_time ON public.silver_futures_liquidations(symbol, trade_time DESC);

-- Silver Funding Rates
CREATE TABLE IF NOT EXISTS public.silver_funding_rates (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    funding_rate NUMERIC(12, 8) NOT NULL,
    funding_time TIMESTAMP WITH TIME ZONE NOT NULL,
    mark_price NUMERIC(18, 8) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_silver_funding UNIQUE (symbol, funding_time)
);

-- Silver Open Interest
CREATE TABLE IF NOT EXISTS public.silver_open_interest (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    open_interest NUMERIC(24, 8) NOT NULL,
    open_interest_value NUMERIC(24, 8) NOT NULL,
    snapshot_time TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_silver_oi UNIQUE (symbol, snapshot_time)
);

-- Pipeline Batch Execution Audits
CREATE TABLE IF NOT EXISTS public.pipeline_batch_status (
    batch_id VARCHAR(64) PRIMARY KEY,
    pipeline VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    source_endpoint VARCHAR(150),
    bronze_path VARCHAR(255),
    silver_path VARCHAR(255),
    gold_table VARCHAR(100),
    status VARCHAR(20) NOT NULL,
    quality_passed BOOLEAN DEFAULT TRUE,
    records_bronze BIGINT DEFAULT 0,
    records_silver BIGINT DEFAULT 0,
    records_gold BIGINT DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP WITH TIME ZONE,
    airflow_dag_run_id VARCHAR(100),
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_batch_status_pipeline ON public.pipeline_batch_status(pipeline, symbol, started_at DESC);

-- Data Quality Detailed Test Results
CREATE TABLE IF NOT EXISTS public.data_quality_results (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    dataset VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    pair_id SMALLINT REFERENCES public.dim_trading_pairs(pair_id),
    check_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    passed BOOLEAN NOT NULL,
    total_records BIGINT NOT NULL,
    failed_records BIGINT DEFAULT 0,
    null_close_count BIGINT DEFAULT 0,
    negative_volume_count BIGINT DEFAULT 0,
    duplicate_key_count BIGINT DEFAULT 0,
    interval_gap_count BIGINT DEFAULT 0,
    error_summary TEXT
);
CREATE INDEX IF NOT EXISTS idx_dq_results_batch ON public.data_quality_results(batch_id, symbol, check_time DESC);

-- ----------------------------------------------------------------------------
-- 4. GOLD LAYER TABLES (LOCATED IN GOLD_LAYER SCHEMA)
-- ----------------------------------------------------------------------------

-- Gold Technical Indicators Mart
CREATE TABLE IF NOT EXISTS gold_layer.gold_klines_technical_indicators (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES gold_layer.dim_trading_pairs(pair_id),
    market_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    interval VARCHAR(10) NOT NULL,
    open_time TIMESTAMP WITH TIME ZONE NOT NULL,
    close_time TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(24, 8) NOT NULL,
    quote_volume NUMERIC(24, 8) NOT NULL,
    trades_count INTEGER DEFAULT 0,
    taker_buy_ratio NUMERIC(10, 4) DEFAULT 0.0,
    vwap NUMERIC(18, 8),
    sma_20 NUMERIC(18, 8),
    sma_50 NUMERIC(18, 8),
    sma_200 NUMERIC(18, 8),
    ema_12 NUMERIC(18, 8),
    ema_26 NUMERIC(18, 8),
    rsi_14 NUMERIC(10, 4),
    macd NUMERIC(18, 8),
    macd_signal NUMERIC(18, 8),
    macd_hist NUMERIC(18, 8),
    bb_upper NUMERIC(18, 8),
    bb_middle NUMERIC(18, 8),
    bb_lower NUMERIC(18, 8),
    atr_14 NUMERIC(18, 8),
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_gold_kline_tech UNIQUE (symbol, interval, open_time)
);
CREATE INDEX IF NOT EXISTS idx_gold_tech_sym_time ON gold_layer.gold_klines_technical_indicators(symbol, open_time DESC);

-- Gold Order Book Imbalance Mart (1m)
CREATE TABLE IF NOT EXISTS gold_layer.gold_orderbook_imbalance_1m (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES gold_layer.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    bucket_time TIMESTAMP WITH TIME ZONE NOT NULL,
    best_bid NUMERIC(18, 8) NOT NULL,
    best_ask NUMERIC(18, 8) NOT NULL,
    spread_absolute NUMERIC(18, 8) NOT NULL,
    spread_bps NUMERIC(10, 4) NOT NULL,
    bid_depth_qty_top10 NUMERIC(24, 8) NOT NULL,
    ask_depth_qty_top10 NUMERIC(24, 8) NOT NULL,
    orderbook_imbalance NUMERIC(10, 4) NOT NULL,
    total_depth_liquidity_usd NUMERIC(24, 8) NOT NULL,
    CONSTRAINT uq_gold_ob_imbalance UNIQUE (symbol, bucket_time)
);
CREATE INDEX IF NOT EXISTS idx_gold_ob_imbalance_sym ON gold_layer.gold_orderbook_imbalance_1m(symbol, bucket_time DESC);

-- Gold Liquidation Stats Hourly Mart
CREATE TABLE IF NOT EXISTS gold_layer.gold_liquidation_stats_hourly (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES gold_layer.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    hourly_time TIMESTAMP WITH TIME ZONE NOT NULL,
    total_liquidations_count INTEGER DEFAULT 0,
    long_liquidated_orders INTEGER DEFAULT 0,
    short_liquidated_orders INTEGER DEFAULT 0,
    total_long_liquidated_usd NUMERIC(24, 8) DEFAULT 0.0,
    total_short_liquidated_usd NUMERIC(24, 8) DEFAULT 0.0,
    max_single_liquidation_usd NUMERIC(24, 8) DEFAULT 0.0,
    long_short_liquidation_ratio NUMERIC(10, 4) DEFAULT 1.0,
    CONSTRAINT uq_gold_liq_hourly UNIQUE (symbol, hourly_time)
);
CREATE INDEX IF NOT EXISTS idx_gold_liq_hourly_sym ON gold_layer.gold_liquidation_stats_hourly(symbol, hourly_time DESC);

-- Gold ML Features 1m Mart (SYS-04 LSTM Feature Store)
CREATE TABLE IF NOT EXISTS gold_layer.gold_ml_features_1m (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES gold_layer.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    feature_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    log_return NUMERIC(12, 8),
    volatility_parkinson NUMERIC(12, 8),
    volume_surge_ratio NUMERIC(12, 8),
    taker_imbalance NUMERIC(10, 4),
    orderbook_pressure NUMERIC(10, 4),
    recent_liquidation_usd NUMERIC(24, 8),
    rsi_14 NUMERIC(10, 4),
    target_return_5m_forward NUMERIC(12, 8),
    CONSTRAINT uq_gold_ml_features UNIQUE (symbol, feature_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_gold_ml_sym_time ON gold_layer.gold_ml_features_1m(symbol, feature_timestamp DESC);

-- Gold Transaction Risk Features (SYS-04 LightGBM Fraud Feature Store)
CREATE TABLE IF NOT EXISTS gold_layer.gold_transaction_risk_features (
    transaction_id UUID PRIMARY KEY,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    amount NUMERIC(18, 4) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    user_hash VARCHAR(64) NOT NULL,
    ip_hash VARCHAR(64) NOT NULL,
    device_hash VARCHAR(64) NOT NULL,
    account_age_days INTEGER DEFAULT 0,
    transaction_count_5m INTEGER DEFAULT 0,
    transaction_count_1h INTEGER DEFAULT 0,
    amount_sum_24h NUMERIC(18, 4) DEFAULT 0.0,
    failed_attempts_24h INTEGER DEFAULT 0,
    ip_velocity_1h INTEGER DEFAULT 0,
    device_velocity_24h INTEGER DEFAULT 0,
    market_anomaly_score NUMERIC(10, 4),
    price_volatility_1h NUMERIC(10, 4),
    feature_schema_version VARCHAR(20) DEFAULT 'v2.1',
    feature_calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_gold_tx_features_time ON gold_layer.gold_transaction_risk_features(event_time DESC);

-- Fraud Labels & Ground Truth Confirmed Cases
CREATE TABLE IF NOT EXISTS gold_layer.fraud_labels (
    transaction_id UUID PRIMARY KEY REFERENCES gold_layer.gold_transaction_risk_features(transaction_id) ON DELETE CASCADE,
    is_fraud SMALLINT NOT NULL DEFAULT 0,
    label_source VARCHAR(50) DEFAULT 'AML_GATEWAY',
    label_confirmed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    reviewer_hash VARCHAR(64),
    label_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Training View for LightGBM
CREATE OR REPLACE VIEW gold_layer.v_lightgbm_fraud_training AS
SELECT
    features.*,
    labels.is_fraud,
    labels.label_confirmed_at
FROM gold_layer.gold_transaction_risk_features AS features
INNER JOIN gold_layer.fraud_labels AS labels ON features.transaction_id = labels.transaction_id
WHERE labels.label_confirmed_at >= features.event_time;

-- AI Model Hourly Inference Results & Governance
CREATE TABLE IF NOT EXISTS gold_layer.ai_hourly_inference_results (
    inference_id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(50) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    predicted_price NUMERIC(18, 8),
    actual_price NUMERIC(18, 8),
    mape_error NUMERIC(10, 4),
    anomaly_score NUMERIC(10, 4),
    is_anomaly BOOLEAN DEFAULT FALSE,
    inference_latency_ms NUMERIC(10, 2) DEFAULT 0.0,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_gold_ai_inf_sym_time ON gold_layer.ai_hourly_inference_results(model_name, symbol, timestamp DESC);

-- Consolidated Crypto Klines Gold Mart
CREATE TABLE IF NOT EXISTS gold_layer.crypto_klines_gold (
    id BIGSERIAL PRIMARY KEY,
    pair_id SMALLINT REFERENCES gold_layer.dim_trading_pairs(pair_id),
    symbol VARCHAR(20) NOT NULL,
    open_time TIMESTAMP WITH TIME ZONE NOT NULL,
    close_time TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(24, 8) NOT NULL,
    quote_asset_volume NUMERIC(24, 8) DEFAULT 0.0,
    number_of_trades INTEGER DEFAULT 0,
    taker_buy_base_asset_volume NUMERIC(24, 8) DEFAULT 0.0,
    taker_buy_quote_asset_volume NUMERIC(24, 8) DEFAULT 0.0,
    sma_20 NUMERIC(18, 8) DEFAULT 0.0,
    rsi_14 NUMERIC(10, 4) DEFAULT 0.0,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_crypto_klines_gold UNIQUE (symbol, open_time)
);

-- Materialized Summary: Hourly Volume Aggregation
CREATE TABLE IF NOT EXISTS gold_layer.mv_hourly_volume (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    hourly_time TIMESTAMP WITH TIME ZONE NOT NULL,
    total_volume NUMERIC(28, 8) NOT NULL DEFAULT 0.0,
    total_klines BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_gold_hourly_vol UNIQUE (symbol, hourly_time)
);

-- ============================================================================
-- PHẦN BỔ SUNG: CỘT 'SOURCE' & ĐỒNG BỘ NGUỒN SÀN
-- (Chỉ áp dụng cho các Fact/Stream tables, không áp dụng cho Dim tables như dim_trading_pairs)
-- ============================================================================

-- 2. Thêm cột source vào các bảng Silver Layer (public)
ALTER TABLE public.silver_crypto_klines ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_klines';
ALTER TABLE public.silver_trades_aggregated ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_wsis_aggtrade';
ALTER TABLE public.silver_orderbook_snapshots ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_wsis_depth';
ALTER TABLE public.silver_orderbook_deltas ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_wsis_depth';
ALTER TABLE public.silver_futures_liquidations ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_wsis_liquidations';
ALTER TABLE public.silver_funding_rates ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_funding_rates';
ALTER TABLE public.silver_open_interest ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_open_interest';
ALTER TABLE public.pipeline_batch_status ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_batch_pipeline';
ALTER TABLE public.data_quality_audits ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_data_quality_audits';
ALTER TABLE public.data_quality_results ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_data_quality_results';

-- 3. Thêm cột source vào các bảng Gold Layer (gold_layer)
ALTER TABLE gold_layer.data_quality_audits ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_data_quality_audits';
ALTER TABLE gold_layer.crypto_klines_gold ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_klines';
ALTER TABLE gold_layer.gold_klines_technical_indicators ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_technical_indicators';
ALTER TABLE gold_layer.gold_orderbook_imbalance_1m ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_wsis_depth';
ALTER TABLE gold_layer.gold_liquidation_stats_hourly ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_wsis_liquidations';
ALTER TABLE gold_layer.gold_ml_features_1m ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_ml_features';
ALTER TABLE gold_layer.ai_hourly_inference_results ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_ai_inference';

-- ============================================================================
-- PHẦN BỔ SUNG: HYBRID MEDALLION ARCHITECTURE (TYPE_METHOD & UNICITY)
-- ============================================================================
ALTER TABLE public.silver_crypto_klines ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE public.silver_trades_aggregated ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_orderbook_snapshots ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_orderbook_deltas ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_futures_liquidations ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_funding_rates ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE public.silver_open_interest ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE public.pipeline_batch_status ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

ALTER TABLE public.silver_crypto_klines DROP CONSTRAINT IF EXISTS uq_silver_kline;
ALTER TABLE public.silver_crypto_klines ADD CONSTRAINT uq_silver_kline UNIQUE (symbol, interval, open_time, type_method);

ALTER TABLE public.silver_trades_aggregated DROP CONSTRAINT IF EXISTS uq_silver_agg_trade;
ALTER TABLE public.silver_trades_aggregated ADD CONSTRAINT uq_silver_agg_trade UNIQUE (symbol, trade_time, agg_trade_id, type_method);

ALTER TABLE gold_layer.crypto_klines_gold ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE gold_layer.gold_klines_technical_indicators ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE gold_layer.gold_orderbook_imbalance_1m ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE gold_layer.gold_liquidation_stats_hourly ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE gold_layer.gold_ml_features_1m ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE gold_layer.ai_hourly_inference_results ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

ALTER TABLE gold_layer.ai_hourly_inference_results DROP CONSTRAINT IF EXISTS uq_ai_hourly_inference;
ALTER TABLE gold_layer.ai_hourly_inference_results ADD CONSTRAINT uq_ai_hourly_inference UNIQUE (symbol, timestamp, model_name);

CREATE OR REPLACE VIEW gold_layer.v_stream_reconciliation_klines AS
SELECT 
    w.symbol,
    w.interval,
    w.open_time,
    w.close AS wsis_close_price,
    a.close AS api_close_price,
    ROUND(ABS(w.close - a.close)::numeric, 4) AS price_discrepancy,
    ROUND((ABS(w.close - a.close) / NULLIF(a.close, 0) * 100)::numeric, 4) AS discrepancy_pct,
    w.volume AS wsis_volume,
    a.volume AS api_volume,
    w.source AS wsis_source,
    a.source AS api_source,
    w.ingested_at AS wsis_ingested_at,
    a.ingested_at AS api_ingested_at
FROM public.silver_crypto_klines w
JOIN public.silver_crypto_klines a 
    ON w.symbol = a.symbol 
    AND w.interval = a.interval 
    AND w.open_time = a.open_time
WHERE w.type_method = 'WSIS' AND a.type_method = 'API';

