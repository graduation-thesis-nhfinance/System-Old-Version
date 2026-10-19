-- 08_standardize_remaining_tables.sql
-- 1. Add type_method to public.data_quality_audits and gold_layer.data_quality_audits
ALTER TABLE public.data_quality_audits 
ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

ALTER TABLE gold_layer.data_quality_audits 
ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

-- 2. Add type_method to public.data_quality_results
ALTER TABLE public.data_quality_results 
ADD COLUMN IF NOT EXISTS type_method VARCHAR(10) DEFAULT 'API';

-- 3. Update trigger sync_dq_audits_to_gold
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
            error_summary = NEW.error_summary,
            source = NEW.source,
            type_method = NEW.type_method
        WHERE audit_id = NEW.audit_id;
    ELSIF (TG_OP = 'DELETE') THEN
        DELETE FROM gold_layer.data_quality_audits WHERE audit_id = OLD.audit_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 4. Update trigger sync_dq_audits_from_gold
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
            error_summary = NEW.error_summary,
            source = NEW.source,
            type_method = NEW.type_method
        WHERE audit_id = NEW.audit_id;
    ELSIF (TG_OP = 'DELETE') THEN
        DELETE FROM public.data_quality_audits WHERE audit_id = OLD.audit_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
