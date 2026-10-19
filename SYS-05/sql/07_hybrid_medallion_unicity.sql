-- ============================================================================
-- 07_hybrid_medallion_unicity.sql
-- Triển khai Kiến trúc Hybrid Medallion Architecture:
-- 1. Bổ sung cột type_method ('API' / 'WSIS') cho tất cả các bảng dữ liệu
-- 2. Tầng Silver: Phương án B (Unicity kèm type_method - lưu song song 2 luồng để đối soát)
-- 3. Tầng Gold: Phương án A (Unicity không kèm type_method - Golden Record duy nhất cho AI & Dashboard)
-- 4. Tạo View đối soát sai lệch giá và độ trễ: v_stream_reconciliation_klines
-- ============================================================================

-- BƯỚC 1: THÊM CỘT type_method VÀO CÁC BẢNG SILVER (public)
ALTER TABLE public.silver_crypto_klines ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE public.silver_trades_aggregated ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_orderbook_snapshots ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_orderbook_deltas ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_futures_liquidations ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE public.silver_funding_rates ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE public.silver_open_interest ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE public.pipeline_batch_status ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

-- Cập nhật giá trị type_method ban đầu dựa trên source tag
UPDATE public.silver_crypto_klines 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

UPDATE public.silver_trades_aggregated 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

UPDATE public.pipeline_batch_status 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

-- BƯỚC 2: CẬP NHẬT RÀNG BUỘC UNICITY TẦNG SILVER (PHƯƠNG ÁN B: Dual-Stream)
-- Cho phép lưu song song bản ghi của API và WSIS tại cùng mốc thời gian
ALTER TABLE public.silver_crypto_klines DROP CONSTRAINT IF EXISTS uq_silver_kline;
ALTER TABLE public.silver_crypto_klines ADD CONSTRAINT uq_silver_kline UNIQUE (symbol, interval, open_time, type_method);

ALTER TABLE public.silver_trades_aggregated DROP CONSTRAINT IF EXISTS uq_silver_agg_trade;
ALTER TABLE public.silver_trades_aggregated ADD CONSTRAINT uq_silver_agg_trade UNIQUE (symbol, trade_time, agg_trade_id, type_method);

-- BƯỚC 3: THÊM CỘT type_method VÀO CÁC BẢNG GOLD (gold_layer)
ALTER TABLE gold_layer.crypto_klines_gold ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE gold_layer.gold_klines_technical_indicators ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE gold_layer.gold_orderbook_imbalance_1m ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE gold_layer.gold_liquidation_stats_hourly ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'WSIS';
ALTER TABLE gold_layer.gold_ml_features_1m ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';
ALTER TABLE gold_layer.ai_hourly_inference_results ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

UPDATE gold_layer.crypto_klines_gold 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

UPDATE gold_layer.gold_klines_technical_indicators 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

UPDATE gold_layer.gold_orderbook_imbalance_1m 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

UPDATE gold_layer.ai_hourly_inference_results 
SET type_method = CASE WHEN source LIKE '%_wsis_%' THEN 'WSIS' ELSE 'API' END;

-- BƯỚC 4: RÀNG BUỘC UNICITY TẦNG GOLD (PHƯƠNG ÁN A: Single Golden Record)
-- Giữ nguyên tính duy nhất theo Business Keys cốt lõi, không chứa type_method:
-- 1 mốc thời gian = 1 bản ghi duy nhất, bản mới nhất ghi đè bản cũ!
ALTER TABLE gold_layer.ai_hourly_inference_results DROP CONSTRAINT IF EXISTS uq_ai_hourly_inference;
ALTER TABLE gold_layer.ai_hourly_inference_results ADD CONSTRAINT uq_ai_hourly_inference UNIQUE (symbol, timestamp, model_name);

-- BƯỚC 5: TẠO VIEW ĐỐI SOÁT CHÉO SAI LỆCH GIỮA WSIS VÀ API (RECONCILIATION VIEW)
-- Cho phép đối soát độ lệch giá (price drift), chênh lệch khối lượng và độ trễ mạng
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
