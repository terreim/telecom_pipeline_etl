-- Tables Definition Language (DDL) for Gold Layer in Clickhouse

-- Health Scoring Table
CREATE TABLE IF NOT EXISTS telecom.gold_health_hourly
(
    station_id                    UInt32,
    station_code                  LowCardinality(String),
    hour_start                    DateTime64(3, 'Asia/Ho_Chi_Minh'),
    hour_date                     Date MATERIALIZED toDate(hour_start),
    operator_code                 LowCardinality(String),
    province                      LowCardinality(String),
    region                        LowCardinality(String),
    density_class                 LowCardinality(String),
    technology                    LowCardinality(String),
    session_count                 UInt64,
    unique_subscribers            UInt64,
    total_bytes                   UInt64,
    avg_latency_ms                Float64,
    p95_latency_ms                Float64,
    avg_packet_loss_pct           Float64,
    high_latency_ratio            Float64,
    avg_cpu_pct                   Nullable(Float64),
    max_cpu_pct                   Nullable(Float64),
    avg_memory_pct                Nullable(Float64),
    avg_temperature_celsius       Nullable(Float64),
    max_temperature_celsius       Nullable(Float64),
    avg_uplink_throughput_mbps    Nullable(Float64),
    avg_downlink_throughput_mbps  Nullable(Float64),
    alarm_count                   Nullable(UInt64),
    warning_count                 Nullable(UInt64),
    critical_count                Nullable(UInt64),
    handover_count                Nullable(UInt64),
    incident_active               Nullable(UInt8),
    incident_type                 LowCardinality(Nullable(String)),
    maintenance_active            Nullable(UInt8),

    health_score                  Nullable(Float64) DEFAULT 0,
    health_category               LowCardinality(Nullable(String)),
    loaded_at                     DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(hour_start)
    ORDER BY (station_id, hour_start)
    TTL hour_start + INTERVAL 180 DAY;

    CREATE TABLE IF NOT EXISTS telecom.health_daily
    (
        report_date      Date,
        station_code     LowCardinality(String),
        operator_code    LowCardinality(String),
        region           LowCardinality(String),
        density_class    LowCardinality(String),
        avg_health       AggregateFunction(avg, Nullable(Float64)),
        min_health       AggregateFunction(min, Nullable(Float64)),
        max_health       AggregateFunction(max, Nullable(Float64)),
        hours_critical   AggregateFunction(countIf, Nullable(UInt8)),
        hours_degraded   AggregateFunction(countIf, Nullable(UInt8))
    )
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(report_date)
    ORDER BY (report_date, station_code);

    CREATE MATERIALIZED VIEW IF NOT EXISTS telecom.mv_health_daily
    TO telecom.health_daily
    AS SELECT
        hour_date                                                                  AS report_date,
        station_code,
        operator_code,
        region,
        density_class,
        avgState(health_score)                                                     AS avg_health,
        minState(health_score)                                                     AS min_health,
        maxState(health_score)                                                     AS max_health,
        countIfState(health_category = 'critical')                                 AS hours_critical,
        countIfState(health_category = 'degraded' OR health_category = 'critical') AS hours_degraded
    FROM telecom.gold_health_hourly
    WHERE health_score IS NOT NULL
    GROUP BY hour_date, station_code, operator_code, region, density_class;

-- SALA Compliance Table
CREATE TABLE IF NOT EXISTS telecom.gold_sla_compliance
(
    station_id           UInt32,
    station_code         LowCardinality(String),
    report_date          Date,
    operator_code        LowCardinality(String),
    province             LowCardinality(String),
    region               LowCardinality(String),
    density_class              LowCardinality(String),
    technology           LowCardinality(String),
    total_hours          UInt8,
    active_hours         UInt8,
    down_hours           UInt8,
    maintenance_hours    UInt8,
    degraded_hours       UInt8,
    billable_hours       UInt8,
    available_hours      UInt8,
    uptime_pct           Float64,
    sla_target_pct       Float64,
    sla_met              UInt8,
    sla_breach_hours     Float64,
    avg_health_score     Nullable(Float64),
    min_health_score     Nullable(Float64),
    hours_below_60       UInt8,
    hours_below_30       UInt8,
    incident_count       UInt16,
    total_incident_min   Float64,
    longest_incident_min Float64,
    mttr_min             Float64,
    compliance_status    LowCardinality(String),
    loaded_at            DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, station_code)
TTL report_date + INTERVAL 365 DAY;

-- Anomaly Features Table
CREATE TABLE IF NOT EXISTS telecom.gold_anomaly_features
(
    station_id               UInt32,
    station_code             LowCardinality(String),
    hour_start               DateTime64(3, 'Asia/Ho_Chi_Minh'),
    current_latency_ms       Float64,
    current_packet_loss      Float64,
    current_cpu_pct          Float64,
    current_throughput       Float64,
    current_subscribers      UInt32,
    baseline_latency_mean    Float64,
    baseline_latency_std     Float64,
    baseline_cpu_mean        Float64,
    baseline_cpu_std         Float64,
    baseline_throughput_mean Float64,
    baseline_throughput_std  Float64,
    baseline_subs_mean       Float64,
    baseline_subs_std        Float64,
    z_latency                Float64,
    z_cpu                    Float64,
    z_throughput             Float64,
    z_subscribers            Float64,
    is_anomalous             UInt8,
    anomaly_type             LowCardinality(Nullable(String)),
    neighbor_anomaly_count   UInt8,
    loaded_at                DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(hour_start)
ORDER BY (station_id, hour_start)
TTL hour_start + INTERVAL 90 DAY;

-- Outage Table
CREATE TABLE IF NOT EXISTS telecom.gold_outage_report
(
    station_id             UInt32,
    station_code           LowCardinality(String),
    operator_code          LowCardinality(String),
    province               LowCardinality(String),
    region                 LowCardinality(String),
    technology             LowCardinality(String),
    incident_type          LowCardinality(String),
    severity               Float64,
    incident_start         Nullable(DateTime64(3, 'Asia/Ho_Chi_Minh')),
    outage_date            Date MATERIALIZED toDate(incident_start),
    incident_end           Nullable(DateTime64(3, 'Asia/Ho_Chi_Minh')),
    duration_min           Float64,
    estimated_duration_min Float64,
    affected_subscribers   UInt32,
    traffic_loss_bytes     UInt64,
    hardware_quality       Float64,
    health_score_during    Nullable(Float64),
    loaded_at              DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(assumeNotNull(incident_start))
ORDER BY (assumeNotNull(incident_start), station_id)
TTL assumeNotNull(incident_start) + INTERVAL 365 DAY;

-- Region Daily Summary Table
CREATE TABLE IF NOT EXISTS telecom.gold_region_daily
(
    report_date           Date,
    region                LowCardinality(String),
    province              LowCardinality(String),
    operator_code         LowCardinality(String),
    technology            LowCardinality(String),
    station_count         UInt32,
    active_station_count  UInt32,       
    total_subscribers     UInt64,
    total_sessions        UInt64,
    total_bytes           UInt64,
    avg_health_score      Float64,
    min_health_score      Float64,
    stations_critical     UInt32,
    stations_degraded     UInt32,
    avg_latency_ms        Float64,
    p95_latency_ms        Float64,
    avg_packet_loss_pct   Float64,
    incident_count        UInt32,
    sla_breach_count      UInt32,
    sla_compliance_pct    Float64,
    loaded_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, region, province, operator_code, technology);

-- Maintenance Summary Table
CREATE TABLE IF NOT EXISTS telecom.gold_maintenance_report
(
    station_id              UInt32,
    station_code            LowCardinality(String),
    operator_code           LowCardinality(String),
    province                LowCardinality(String),
    region                  LowCardinality(String),
    technology              LowCardinality(String),
    maintenance_start       DateTime64(3, 'Asia/Ho_Chi_Minh'),
    maintenance_end         Nullable(DateTime64(3, 'Asia/Ho_Chi_Minh')),
    planned_duration_min    UInt16,
    actual_duration_min     Float64,
    overrun_min             Float64,
    tech_upgrade            UInt8,
    sla_excluded            UInt8,
    pre_maintenance_health  Nullable(Float64),
    post_maintenance_health Nullable(Float64),
    loaded_at               DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(maintenance_start)
ORDER BY (maintenance_start, station_id);

-- Handover Summary Table
CREATE TABLE IF NOT EXISTS telecom.gold_handover_daily
(
    report_date             Date,
    source_station_id       UInt32,
    source_station_code     LowCardinality(String),
    target_station_id       UInt32,
    target_station_code     LowCardinality(String),
    source_region           LowCardinality(String),
    target_region           LowCardinality(String),
    handover_count          UInt32,
    unique_subscribers      UInt32,
    avg_latency_before_ms   Float64,
    avg_latency_after_ms    Float64,
    loaded_at               DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, source_station_id, target_station_id);

-- Alarm Summary Table
CREATE TABLE IF NOT EXISTS telecom.gold_alarm_daily
(
    report_date           Date,
    station_id            UInt32,
    station_code          LowCardinality(String),
    operator_code         LowCardinality(String),
    region                LowCardinality(String),
    technology            LowCardinality(String),
    warning_count         UInt32,
    error_count           UInt32,
    critical_count        UInt32,
    total_alarm_count     UInt32,
    alarm_rate_per_hour   Float64,
    top_alarm_description LowCardinality(String),
    health_score_avg      Nullable(Float64),
    loaded_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, station_id);