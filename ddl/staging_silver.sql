-- Tables Definition Language (DDL) for Staging Layer in Clickhouse

-- Traffic
CREATE TABLE telecom.staging_traffic_cleaned
(
	traffic_id              UInt64,
	station_id              UInt32,
	event_time              DateTime64(3, 'Asia/Ho_Chi_Minh'),
	imsi_hash               String,
	tmsi                    String,
	ip_address              String,
	destination_ip          String,
	destination_port        UInt16,
	protocol                String,
	bytes_up                UInt64,
	bytes_down              UInt64,
	packets_up              UInt32,
	packets_down            UInt32,
	latency_ms              Float64,
	jitter_ms               Float64,
	packet_loss_pct         Float64,
	connection_duration_ms  UInt32,
	ingested_at             DateTime64(3, 'Asia/Ho_Chi_Minh'),
	batch_id                String,
	is_valid                Bool,
	quality_issues          String,
	station_code            LowCardinality(String),
	operator_code           LowCardinality(String),
	operator_name           String,
	province                LowCardinality(String),
	district                String,
	region                  LowCardinality(String),
	density                 LowCardinality(String),
	technology              LowCardinality(String),
	dim_match_status        LowCardinality(String),
	bytes_total             UInt64,
	event_date              DateTime,
	event_hour              DateTime64(3, 'Asia/Ho_Chi_Minh'),
	is_high_latency         Bool,
	processed_at            DateTime64(3, 'Asia/Ho_Chi_Minh')
	--	`day`               UInt8, 
	--	`hour`              UInt8, 
	--	`month`             LowCardinality(String), 
	--	`year`              UInt16
	-- Commented out because SETTINGS use_hive_partitioning=0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (station_id, event_time, traffic_id)
TTL event_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Metrics
CREATE TABLE telecom.staging_metrics_cleaned
(
	metric_id               UInt64,
	station_id              UInt32,
	metric_time             DateTime64(3, 'Asia/Ho_Chi_Minh'),
	cpu_util_pct            Float32,
	memory_util_pct         Float32,
	disk_util_pct           Float32,
	temperature_c           Float32,
	active_connections      UInt64,
	throughput_mbps         Float64,
	uptime_seconds          UInt32,
	error_count             UInt64,
	ingested_at             DateTime64(3, 'Asia/Ho_Chi_Minh'),
	batch_id                String,
	is_valid                Bool,
	quality_issues          String,
	station_code            LowCardinality(String),
	operator_code           LowCardinality(String),
	operator_name           String,
	province                LowCardinality(String),
	district                String,
	region                  LowCardinality(String),
	density                 LowCardinality(String),
	technology              LowCardinality(String),
	dim_match_status        LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_time)
ORDER BY (station_id, metric_time, metric_id)
TTL metric_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Events
CREATE TABLE telecom.staging_events_cleaned
(
	event_id                UInt64,
	station_id              UInt32,
	event_time              DateTime64(3, 'Asia/Ho_Chi_Minh'),
	event_type              LowCardinality(String),
	severity                LowCardinality(String),
	description             LowCardinality(String),
	metadata                JSON,
	target_station_id       UInt32 DEFAULT 0,
	ingested_at             DateTime64(3, 'Asia/Ho_Chi_Minh'),
	batch_id                String,
	is_valid                Bool,
	quality_issues          String,
	station_code            LowCardinality(String),
	operator_code           LowCardinality(String),
	operator_name           String,
	province                LowCardinality(String),
	district                String,
	region                  LowCardinality(String),
	density                 LowCardinality(String),
	technology              LowCardinality(String),
	dim_match_status        LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (station_id, event_time, event_id)
TTL event_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Dimension Table for Station Metadata
-- TODO: Add latitude and longitude for future geospatial analysis, and status
CREATE TABLE telecom.dim_station
(
	station_id              UInt32,
	station_code            String,
	operator_code           LowCardinality(String),
	operator_name           LowCardinality(String),
	province                LowCardinality(String),
	district                LowCardinality(String),
	region                  LowCardinality(String),
	density                 LowCardinality(String),
	technology              LowCardinality(String),
	updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY station_id;

CREATE DICTIONARY IF NOT EXISTS telecom.dict_station
(
	station_id              UInt32,
	station_code            String,
	operator_code           String,
	operator_name           String,
	province                String,
	district                String,
	region                  String,
	density                 String,
	technology              String
)
PRIMARY KEY station_id
SOURCE(CLICKHOUSE(TABLE 'telecom.dim_station' DB 'telecom'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(FLAT());
