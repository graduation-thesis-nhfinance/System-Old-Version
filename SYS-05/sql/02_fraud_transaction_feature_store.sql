-- SYS-04 LightGBM Fraud: transaction data domain.
-- Execute after init_clickhouse_tables.sql. Hash identifiers before inserting.

CREATE DATABASE IF NOT EXISTS db_bronze_fraud;
CREATE DATABASE IF NOT EXISTS db_gold_fraud;

CREATE TABLE IF NOT EXISTS db_bronze_fraud.raw_transactions (
    transaction_id UUID,
    event_time DateTime64(3, 'UTC'),
    source LowCardinality(String),
    raw_payload String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (transaction_id, event_time, ingested_at);

CREATE TABLE IF NOT EXISTS db_gold_fraud.gold_transaction_risk_features (
    transaction_id UUID,
    event_time DateTime64(3, 'UTC'),
    amount Float64,
    transaction_type LowCardinality(String),
    payment_method LowCardinality(String),
    user_hash String,
    ip_hash String,
    device_hash String,
    account_age_days UInt32,
    transaction_count_5m UInt32,
    transaction_count_1h UInt32,
    amount_sum_24h Float64,
    failed_attempts_24h UInt32,
    ip_velocity_1h UInt32,
    device_velocity_24h UInt32,
    market_anomaly_score Nullable(Float64),
    price_volatility_1h Nullable(Float64),
    feature_schema_version LowCardinality(String),
    feature_calculated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(feature_calculated_at)
PARTITION BY toYYYYMM(event_time)
PRIMARY KEY (transaction_id)
ORDER BY (transaction_id, feature_calculated_at);

CREATE TABLE IF NOT EXISTS db_gold_fraud.fraud_labels (
    transaction_id UUID,
    is_fraud UInt8,
    label_source LowCardinality(String),
    label_confirmed_at DateTime64(3, 'UTC'),
    reviewer_hash Nullable(String),
    label_updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(label_updated_at)
PARTITION BY toYYYYMM(label_confirmed_at)
PRIMARY KEY (transaction_id)
ORDER BY (transaction_id, label_updated_at);

-- Training rows must come only from confirmed outcomes and immutable snapshots.
CREATE VIEW IF NOT EXISTS db_gold_fraud.v_lightgbm_fraud_training AS
SELECT
    features.*,
    labels.is_fraud,
    labels.label_confirmed_at
FROM db_gold_fraud.gold_transaction_risk_features AS features
INNER JOIN db_gold_fraud.fraud_labels AS labels USING (transaction_id)
WHERE labels.label_confirmed_at >= features.event_time;
