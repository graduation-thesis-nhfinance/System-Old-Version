-- ============================================================================
-- 05_remove_source_from_dim_trading_pairs.sql
-- Xóa cột source khỏi bảng chiều tĩnh dim_trading_pairs và cập nhật trigger
-- ============================================================================

-- 1. Cập nhật Trigger Functions đồng bộ 2 chiều (bỏ cột source)
CREATE OR REPLACE FUNCTION sync_dim_trading_pairs_to_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO gold_layer.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, created_at, updated_at)
        VALUES (NEW.pair_id, NEW.base_coin, NEW.quote_coin, NEW.trading_pair, NEW.symbol, NEW.market_type, NEW.category, NEW.is_active, NEW.created_at, NEW.updated_at)
        ON CONFLICT (pair_id) DO UPDATE
        SET base_coin = EXCLUDED.base_coin, quote_coin = EXCLUDED.quote_coin, trading_pair = EXCLUDED.trading_pair,
            symbol = EXCLUDED.symbol, market_type = EXCLUDED.market_type, category = EXCLUDED.category,
            is_active = EXCLUDED.is_active, updated_at = EXCLUDED.updated_at;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE gold_layer.dim_trading_pairs
        SET base_coin = NEW.base_coin, quote_coin = NEW.quote_coin, trading_pair = NEW.trading_pair,
            symbol = NEW.symbol, market_type = NEW.market_type, category = NEW.category,
            is_active = NEW.is_active, updated_at = NEW.updated_at
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
        INSERT INTO public.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, created_at, updated_at)
        VALUES (NEW.pair_id, NEW.base_coin, NEW.quote_coin, NEW.trading_pair, NEW.symbol, NEW.market_type, NEW.category, NEW.is_active, NEW.created_at, NEW.updated_at)
        ON CONFLICT (pair_id) DO UPDATE
        SET base_coin = EXCLUDED.base_coin, quote_coin = EXCLUDED.quote_coin, trading_pair = EXCLUDED.trading_pair,
            symbol = EXCLUDED.symbol, market_type = EXCLUDED.market_type, category = EXCLUDED.category,
            is_active = EXCLUDED.is_active, updated_at = EXCLUDED.updated_at;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE public.dim_trading_pairs
        SET base_coin = NEW.base_coin, quote_coin = NEW.quote_coin, trading_pair = NEW.trading_pair,
            symbol = NEW.symbol, market_type = NEW.market_type, category = NEW.category,
            is_active = NEW.is_active, updated_at = NEW.updated_at
        WHERE pair_id = NEW.pair_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM public.dim_trading_pairs WHERE pair_id = OLD.pair_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 2. Xóa cột source khỏi dim_trading_pairs ở cả 2 schemas
ALTER TABLE public.dim_trading_pairs DROP COLUMN IF EXISTS source;
ALTER TABLE gold_layer.dim_trading_pairs DROP COLUMN IF EXISTS source;
