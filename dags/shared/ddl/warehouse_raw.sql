-- Tables Definition Language (DDL) for Raw Layer in Clickhouse

-- ============================================================================
-- AGGREGATING TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS telecom.traffic_hourly (
    event_hour       DateTime,
    station_code     LowCardinality(String),
    operator_code    LowCardinality(String),
    region           LowCardinality(String),
    province         LowCardinality(String),
    density_class    LowCardinality(String),
    technology       LowCardinality(String),
    total_bytes      AggregateFunction(sum, UInt64),
    session_count    AggregateFunction(count),
    unique_subs      AggregateFunction(uniq, String),
    avg_latency      AggregateFunction(avg, Float64),
    p95_latency      AggregateFunction(quantile(0.95), Float64),
    avg_packet_loss  AggregateFunction(avg, Float64),
    avg_jitter       AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_hour)
ORDER BY (event_hour, station_code, operator_code);

CREATE TABLE IF NOT EXISTS telecom.traffic_daily (
    event_date       Date,
    station_code     LowCardinality(String),
    operator_code    LowCardinality(String),
    region           LowCardinality(String),
    province         LowCardinality(String),
    density_class    LowCardinality(String),
    technology       LowCardinality(String),
    total_bytes      AggregateFunction(sum, UInt64),
    session_count    AggregateFunction(count),
    unique_subs      AggregateFunction(uniq, String),
    avg_latency      AggregateFunction(avg, Float64),
    p95_latency      AggregateFunction(quantile(0.95), Float64),
    avg_packet_loss  AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, station_code, operator_code);

CREATE TABLE IF NOT EXISTS telecom.operator_daily (
    event_date       Date,
    operator_code    LowCardinality(String),
    region           LowCardinality(String),
    total_bytes      AggregateFunction(sum, UInt64),
    session_count    AggregateFunction(count),
    unique_subs      AggregateFunction(uniq, String),
    active_stations  AggregateFunction(uniq, String),
    avg_latency      AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, operator_code, region);

-- ============================================================================
-- MATERIALIZED VIEWS
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS telecom.mv_traffic_hourly
TO telecom.traffic_hourly
AS
SELECT
    event_hour,
    station_code,
    operator_code,
    region,
    province,
    density_class,
    technology,
    sumState(bytes_total)           AS total_bytes,
    countState()                    AS session_count,
    uniqState(imsi_hash)            AS unique_subs,
    avgState(latency_ms)            AS avg_latency,
    quantileState(0.95)(latency_ms) AS p95_latency,
    avgState(packet_loss_pct)       AS avg_packet_loss,
    avgState(jitter_ms)             AS avg_jitter
FROM telecom.staging_traffic_cleaned
GROUP BY event_hour, station_code, operator_code, region, province, density_class, technology;

CREATE MATERIALIZED VIEW IF NOT EXISTS telecom.mv_traffic_daily
TO telecom.traffic_daily
AS
SELECT
    event_date,
    station_code,
    operator_code,
    region,
    province,
    density_class,
    technology,
    sumState(bytes_total)           AS total_bytes,
    countState()                    AS session_count,
    uniqState(imsi_hash)            AS unique_subs,
    avgState(latency_ms)            AS avg_latency,
    quantileState(0.95)(latency_ms) AS p95_latency,
    avgState(packet_loss_pct)       AS avg_packet_loss
FROM telecom.staging_traffic_cleaned
GROUP BY event_date, station_code, operator_code, region, province, density_class, technology;

CREATE MATERIALIZED VIEW IF NOT EXISTS telecom.mv_operator_daily
TO telecom.operator_daily
AS
SELECT
    event_date,
    operator_code,
    region,
    sumState(bytes_total)    AS total_bytes,
    countState()             AS session_count,
    uniqState(imsi_hash)     AS unique_subs,
    uniqState(station_code)  AS active_stations,
    avgState(latency_ms)     AS avg_latency
FROM telecom.staging_traffic_cleaned
GROUP BY event_date, operator_code, region;