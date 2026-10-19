-- ============================================================================
-- SYS-05: PostgreSQL Metadata & Data Governance Schema
-- Managing Dimension Tables & Pipeline Quality Gates for Binance Market Data
-- ============================================================================

-- 1. Dimension Table for Trading Pairs
CREATE TABLE IF NOT EXISTS dim_trading_pairs (
    pair_id       SMALLINT PRIMARY KEY,
    base_coin     VARCHAR(10) NOT NULL,
    quote_coin    VARCHAR(10) NOT NULL,
    trading_pair  VARCHAR(20) NOT NULL,
    symbol        VARCHAR(20) NOT NULL UNIQUE,
    market_type   VARCHAR(20) NOT NULL, -- 'SPOT_FUTURES' or 'SPOT'
    category      VARCHAR(20) NOT NULL, -- 'crypto_popular' or 'equity_token'
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed 6 Trading Pairs from binance_market_data_reference.md
INSERT INTO dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active)
VALUES 
    (1, 'BTC', 'USDT', 'BTC/USDT', 'BTCUSDT', 'SPOT_FUTURES', 'crypto_popular', TRUE),
    (2, 'ETH', 'USDT', 'ETH/USDT', 'ETHUSDT', 'SPOT_FUTURES', 'crypto_popular', TRUE),
    (3, 'BNB', 'USDT', 'BNB/USDT', 'BNBUSDT', 'SPOT_FUTURES', 'crypto_popular', TRUE),
    (4, 'AAPLB', 'USDT', 'AAPLB/USDT', 'AAPLBUSDT', 'SPOT', 'equity_token', TRUE),
    (5, 'AMZNB', 'USDT', 'AMZNB/USDT', 'AMZNBUSDT', 'SPOT', 'equity_token', TRUE),
    (6, 'DELLB', 'USDT', 'DELLB/USDT', 'DELLBUSDT', 'SPOT', 'equity_token', TRUE)
ON CONFLICT (pair_id) DO UPDATE 
SET 
    base_coin = EXCLUDED.base_coin,
    quote_coin = EXCLUDED.quote_coin,
    trading_pair = EXCLUDED.trading_pair,
    symbol = EXCLUDED.symbol,
    market_type = EXCLUDED.market_type,
    category = EXCLUDED.category,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

-- 2. Data Quality Audit Log (Gate for Airflow Pipelines)
CREATE TABLE IF NOT EXISTS data_quality_audits (
    audit_id        BIGSERIAL PRIMARY KEY,
    batch_id        VARCHAR(64) NOT NULL,
    dataset_name    VARCHAR(50) NOT NULL, -- 'klines', 'trades', 'orderbook', 'liquidations'
    pair_id         SMALLINT REFERENCES dim_trading_pairs(pair_id),
    symbol          VARCHAR(20) NOT NULL,
    layer           VARCHAR(10) CHECK (layer IN ('BRONZE', 'SILVER', 'GOLD')),
    check_time      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_passed       BOOLEAN NOT NULL,
    total_records   BIGINT NOT NULL,
    null_count      BIGINT DEFAULT 0,
    duplicate_count BIGINT DEFAULT 0,
    timestamp_gaps  BIGINT DEFAULT 0,
    error_summary   TEXT
);

CREATE INDEX IF NOT EXISTS idx_dq_batch_symbol ON data_quality_audits (batch_id, symbol, check_time);
