"""
    PipelineConfig

    A configuration class for managing ETL pipeline settings and database connections.
    This class serves as a centralized configuration container for a telecommunications
    data pipeline, managing connection IDs, storage prefixes, table names, and processing
    parameters across Bronze, Silver, and Gold data layers.

    **Attributes:**
    Connection Management:
        - postgres_conn_id (str): Airflow connection ID for PostgreSQL (OLTP database)
        - s3_conn_id (str): Airflow connection ID for MinIO/S3 storage
        - clickhouse_conn_id (str): Airflow connection ID for ClickHouse data warehouse
    
    Storage Configuration:
        - minio_endpoint (str): MinIO/S3 endpoint URL
        - s3_bucket (str): S3 bucket name for data storage
        - bronze_prefix (str): S3 prefix for raw data layer
        - silver_prefix (str): S3 prefix for cleaned/processed data layer
        - gold_prefix (str): S3 prefix for aggregated/business-ready data layer
        - quarantine_prefix (str): S3 prefix for invalid/problematic data
    
    Schema and Dimension:
        - schema_name (str): Database schema name
        - dim_dict (str): Dimension table name for station dictionary
    
    Table Names (Bronze Layer - Raw):
        - station_st (str): Subscriber traffic raw table
        - station_bs (str): Base station raw table
        - station_pm (str): Performance metrics raw table
        - station_se (str): Station events raw table
        - station_cf (str): Configuration raw table
        - station_lc (str): Location raw table
        - station_op (str): Operator raw table
    
    Table Names (Silver Layer - Cleaned):
        - station_cleaned_st (str): Subscriber traffic cleaned table
        - station_cleaned_pm (str): Performance metrics cleaned table
        - station_cleaned_se (str): Station events cleaned table
    
    Table Names (Silver Layer - Staging):
        - station_staging_st (str): Subscriber traffic staging table
        - station_staging_pm (str): Performance metrics staging table
        - station_staging_se (str): Station events staging table
    
    Table Names (Gold Layer - Business-Ready):
        - station_health (str): Station health hourly aggregation
        - station_slac (str): SLA compliance metrics
        - station_anomaly (str): Anomaly detection features
        - station_outage (str): Outage reports
        - station_region (str): Regional daily metrics
        - station_maintenance (str): Maintenance reports
        - station_handover (str): Handover daily metrics
        - station_alarm (str): Alarm daily metrics
    
    Processing Parameters:
        - temp_dir (str): Temporary directory for staging files during ETL
        - buffer_seconds (int): Buffer time in seconds for event processing (default: 240)
        - lookback_hours (int): Historical lookback window in hours (default: 24)
    
    **Usage Note:**
    Use the provided class methods:
        - `from_env()`: Recommended - Loads configuration from environment variables with sensible defaults
        - `from_airflow_vars()`: Alternative - Loads from Airflow Variables (not yet implemented)
    
    Example:
        config = PipelineConfig.from_env()
        
    The class uses environment variables with fallback defaults, making it suitable for
    containerized deployments and Airflow DAGs.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os

@dataclass(frozen=True)
class PipelineConfig:
    # Temp dir
    temp_dir: str

    # Connetion IDs
    postgres_conn_id: str
    s3_conn_id: str
    clickhouse_conn_id: str

    # Bucket and prefixes
    minio_endpoint: str
    s3_bucket: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    quarantine_prefix: str

    # Schema and table names
    schema_name: str
    dim_dict: str

    # Bronze
    station_st: str
    station_bs: str
    station_pm: str
    station_se: str
    station_cf: str
    station_lc: str
    station_op: str

    # Silver
    station_cleaned_st: str
    station_cleaned_pm: str
    station_cleaned_se: str

    # Staging Silver
    station_staging_st: str
    station_staging_pm: str
    station_staging_se: str

    # Gold
    station_health: str
    station_slac: str
    station_anomaly: str
    station_outage: str
    station_region: str
    station_maintenance: str
    station_handover: str
    station_alarm: str

    # Numeric configs
    buffer_seconds: int = 240
    overlap_seconds: int = 24

    @classmethod
    @lru_cache(maxsize=1)
    def from_env(cls) -> PipelineConfig:
        return cls(
            temp_dir=os.getenv("AIRFLOW_TEMP_DIR", "/opt/airflow/etl_temp"),
            postgres_conn_id=os.environ.get("POSTGRES_CONN_ID", "postgres-oltp"),
            s3_conn_id=os.environ.get("MINIO_CONN_ID", "minio_default"),
            clickhouse_conn_id=os.environ.get("CLICKHOUSE_CONN_ID", "clickhouse_default"),
            minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            s3_bucket=os.environ.get("MINIO_BUCKET", "station-lake"),
            bronze_prefix=os.environ.get("BRONZE_PREFIX", "bronze"),
            silver_prefix=os.environ.get("SILVER_PREFIX", "silver"),
            gold_prefix=os.environ.get("GOLD_PREFIX", "gold"),
            quarantine_prefix=os.environ.get("QUARANTINE_PREFIX", "quarantine"),
            schema_name=os.environ.get("SCHEMA_NAME", "telecom"),
            dim_dict=os.environ.get("DIM_DICT", "dict_station"),
            station_st=os.environ.get("STATION_ST", "subscriber_traffic"),
            station_bs=os.environ.get("STATION_BS", "base_station"),
            station_pm=os.environ.get("STATION_PM", "performance_metrics"),
            station_se=os.environ.get("STATION_SE", "station_events"),
            station_cf=os.environ.get("STATION_CF", "configuration"),
            station_lc=os.environ.get("STATION_LC", "location"),
            station_op=os.environ.get("STATION_OP", "operator"),
            station_cleaned_st=os.environ.get("STATION_CLEANED_ST", "traffic_cleaned"),
            station_cleaned_pm=os.environ.get("STATION_CLEANED_PM", "metrics_cleaned"),
            station_cleaned_se=os.environ.get("STATION_CLEANED_SE", "events_cleaned"),
            station_staging_st=os.environ.get("STATION_STAGING_ST", "staging_traffic_cleaned"),
            station_staging_pm=os.environ.get("STATION_STAGING_PM", "staging_metrics_cleaned"),
            station_staging_se=os.environ.get("STATION_STAGING_SE", "staging_events_cleaned"),
            station_health=os.environ.get("STATION_HEALTH", "gold_health_hourly"),
            station_slac=os.environ.get("STATION_SLAC", "gold_sla_compliance"),
            station_anomaly=os.environ.get("STATION_ANOMALY", "gold_anomaly_features"),
            station_outage=os.environ.get("STATION_OUTAGE", "gold_outage_report"),
            station_region=os.environ.get("STATION_REGION", "gold_region_daily"),
            station_maintenance=os.environ.get("STATION_MAINTENANCE", "gold_maintenance_report"),
            station_handover=os.environ.get("STATION_HANDOVER", "gold_handover_daily"),
            station_alarm=os.environ.get("STATION_ALARM", "gold_alarm_daily")
        )

    @classmethod
    def from_airflow_vars(cls) -> PipelineConfig:
        # This method can be implemented to read from Airflow Variables if needed
        raise NotImplementedError("from_airflow_vars is not implemented yet")

    def __post_init__(self):
        assert self.s3_bucket, "S3_BUCKET must not be empty"
        assert self.buffer_seconds >= 0
        assert self.overlap_seconds >= 0
        assert self.temp_dir, "TEMP_DIR must not be empty"

CFG = PipelineConfig.from_env()
