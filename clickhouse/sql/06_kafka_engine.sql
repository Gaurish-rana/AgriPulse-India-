-- ============================================================================
-- AgriPulse India — Phase 3: ClickHouse Native Kafka Engine & Materialized View
-- File: clickhouse/sql/06_kafka_engine.sql
-- Description: Zero-dependency, native streaming ingestion from Kafka into ClickHouse
-- ============================================================================

-- 1. Kafka Ingestion Queue Table
CREATE TABLE IF NOT EXISTS agripulse_clean.kafka_mandi_prices_queue (
    state_name String,
    district_name String,
    market_name String,
    commodity_name String,
    variety_name String,
    grade String,
    arrival_date String,
    min_price_inr_quintal Float64,
    max_price_inr_quintal Float64,
    modal_price_inr_quintal Float64,
    processed_at_utc String,
    _message_id String,
    _ingested_at_utc String,
    _is_synthetic UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mandi-prices-clean',
    kafka_group_name = 'clickhouse-mandi-consumer-group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;

-- 2. Materialized View to Stream Queue -> Clean Mandi Table
CREATE MATERIALIZED VIEW IF NOT EXISTS agripulse_clean.mv_kafka_mandi_prices
TO agripulse_clean.clean_mandi_prices AS
SELECT
    state_name,
    district_name,
    market_name,
    commodity_name,
    variety_name,
    grade,
    toDate32(arrival_date) AS arrival_date,
    min_price_inr_quintal,
    max_price_inr_quintal,
    modal_price_inr_quintal,
    toYear(toDate32(arrival_date)) AS arrival_year,
    toMonth(toDate32(arrival_date)) AS arrival_month,
    1 AS is_price_relation_valid,
    'INR' AS currency,
    'INR per quintal' AS price_unit,
    _message_id AS record_hash,
    'KAFKA_STREAM' AS source_channel,
    now() AS loaded_at_utc
FROM agripulse_clean.kafka_mandi_prices_queue;
