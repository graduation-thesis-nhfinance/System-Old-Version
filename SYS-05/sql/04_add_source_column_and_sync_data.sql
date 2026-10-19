-- ============================================================================
-- 04_add_source_column_and_sync_data.sql
-- Thêm cột 'source' vào toàn bộ các bảng trong PostgreSQL và cập nhật trigger
-- ============================================================================

-- 1. Bảng Dim Trading Pairs
ALTER TABLE public.dim_trading_pairs ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_dim_trading_pairs';
ALTER TABLE gold_layer.dim_trading_pairs ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'v1.0.0_api_dim_trading_pairs';
UPDATE public.dim_trading_pairs SET source = 'v1.0.0_api_dim_trading_pairs';
UPDATE gold_layer.dim_trading_pairs SET source = 'v1.0.0_api_dim_trading_pairs';

-- Cập nhật Function đồng bộ 2 chiều
CREATE OR REPLACE FUNCTION sync_dim_trading_pairs_to_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO gold_layer.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, source, created_at, updated_at)
        VALUES (NEW.pair_id, NEW.base_coin, NEW.quote_coin, NEW.trading_pair, NEW.symbol, NEW.market_type, NEW.category, NEW.is_active, NEW.source, NEW.created_at, NEW.updated_at)
        ON CONFLICT (pair_id) DO UPDATE
        SET base_coin = EXCLUDED.base_coin, quote_coin = EXCLUDED.quote_coin, trading_pair = EXCLUDED.trading_pair,
            symbol = EXCLUDED.symbol, market_type = EXCLUDED.market_type, category = EXCLUDED.category,
            is_active = EXCLUDED.is_active, source = EXCLUDED.source, updated_at = EXCLUDED.updated_at;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE gold_layer.dim_trading_pairs
        SET base_coin = NEW.base_coin, quote_coin = NEW.quote_coin, trading_pair = NEW.trading_pair,
            symbol = NEW.symbol, market_type = NEW.market_type, category = NEW.category,
            is_active = NEW.is_active, source = NEW.source, updated_at = NEW.updated_at
        WHERE pair_id = NEW.pair_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM gold_layer.dim_trading_pairs WHERE pair_id = OLD.pair_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_dim_trading_pairs_from_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, source, created_at, updated_at)
        VALUES (NEW.pair_id, NEW.base_coin, NEW.quote_coin, NEW.trading_pair, NEW.symbol, NEW.market_type, NEW.category, NEW.is_active, NEW.source, NEW.created_at, NEW.updated_at)
        ON CONFLICT (pair_id) DO UPDATE
        SET base_coin = EXCLUDED.base_coin, quote_coin = EXCLUDED.quote_coin, trading_pair = EXCLUDED.trading_pair,
            symbol = EXCLUDED.symbol, market_type = EXCLUDED.market_type, category = EXCLUDED.category,
            is_active = EXCLUDED.is_active, source = EXCLUDED.source, updated_at = EXCLUDED.updated_at;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE public.dim_trading_pairs
        SET base_coin = NEW.base_coin, quote_coin = NEW.quote_coin, trading_pair = NEW.trading_pair,
            symbol = NEW.symbol, market_type = NEW.market_type, category = NEW.category,
            is_active = NEW.is_active, source = NEW.source, updated_at = NEW.updated_at
        WHERE pair_id = NEW.pair_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM public.dim_trading_pairs WHERE pair_id = OLD.pair_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

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

