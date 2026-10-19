-- ============================================================================
-- 06_create_code_version_audit_logs.sql
-- Bảng quản trị và lưu vết lịch sử thay đổi code theo phiên bản (Code-to-Data Audit)
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.code_version_audit_logs (
    audit_id        SERIAL PRIMARY KEY,
    version         VARCHAR(20) NOT NULL,          -- e.g. 'v1.0.0', 'v1.1.0'
    component       VARCHAR(20) NOT NULL,          -- 'api' hoặc 'wsis'
    metric_type     VARCHAR(50) NOT NULL,          -- 'klines', 'aggtrade', 'depth', 'technical_indicators'
    source_tag      VARCHAR(100) NOT NULL,         -- e.g. 'v1.0.0_api_klines', 'v1.1.0_api_klines'
    file_path       VARCHAR(255) NOT NULL,         -- e.g. 'SYS-02/dags/01_binance_klines_etl_dag.py'
    lines_modified  VARCHAR(150),                  -- e.g. 'Lines 77-85 modified (+3, -1)'
    diff_summary    TEXT,                          -- Nội dung code diff (Unified Diff)
    code_hash       VARCHAR(64) NOT NULL,          -- SHA-256 mã nguồn để đối soát
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_code_ver_comp_metric ON public.code_version_audit_logs(component, metric_type, created_at DESC);

-- Bản mirror ở gold_layer
CREATE TABLE IF NOT EXISTS gold_layer.code_version_audit_logs (
    audit_id        SERIAL PRIMARY KEY,
    version         VARCHAR(20) NOT NULL,
    component       VARCHAR(20) NOT NULL,
    metric_type     VARCHAR(50) NOT NULL,
    source_tag      VARCHAR(100) NOT NULL,
    file_path       VARCHAR(255) NOT NULL,
    lines_modified  VARCHAR(150),
    diff_summary    TEXT,
    code_hash       VARCHAR(64) NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gold_code_ver_comp_metric ON gold_layer.code_version_audit_logs(component, metric_type, created_at DESC);

-- Trigger đồng bộ 2 chiều cho code_version_audit_logs
CREATE OR REPLACE FUNCTION sync_code_version_audits_to_gold()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
        INSERT INTO gold_layer.code_version_audit_logs (audit_id, version, component, metric_type, source_tag, file_path, lines_modified, diff_summary, code_hash, created_at)
        VALUES (NEW.audit_id, NEW.version, NEW.component, NEW.metric_type, NEW.source_tag, NEW.file_path, NEW.lines_modified, NEW.diff_summary, NEW.code_hash, NEW.created_at)
        ON CONFLICT (audit_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_code_ver_to_gold ON public.code_version_audit_logs;
CREATE TRIGGER trg_sync_code_ver_to_gold
AFTER INSERT ON public.code_version_audit_logs
FOR EACH ROW EXECUTE FUNCTION sync_code_version_audits_to_gold();
