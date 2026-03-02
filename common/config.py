from dataclasses import dataclass
from typing import Optional
import os

@dataclass
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
    lookback_hours: int = 24

    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        return cls(
            temp_dir=os.getenv("AIRFLOW_TEMP_DIR", "/opt/airflow/etl_temp"),
            postgres_conn_id=os.environ.get("POSTGRES_CONN_ID", "postgres-oltp"),
            s3_conn_id=os.environ.get("MINIO_CONN_ID", "minio_default"),
            clickhouse_conn_id=os.environ.get("CLICKHOUSE_CONN_ID", "clickhouse_default"),
            minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            s3_bucket=os.environ.get("S3_BUCKET", "telecom-data"),
            bronze_prefix=os.environ.get("BRONZE_PREFIX", "bronze"),
            silver_prefix=os.environ.get("SILVER_PREFIX", "silver"),
            gold_prefix=os.environ.get("GOLD_PREFIX", "gold"),
            quarantine_prefix=os.environ.get("QUARANTINE_PREFIX", "quarantine"),
            schema_name=os.environ.get("SCHEMA_NAME", "telecom"),
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
    def from_airflow_vars(cls) -> 'PipelineConfig':
        # This method can be implemented to read from Airflow Variables if needed
        raise NotImplementedError("from_airflow_vars is not implemented yet")

    def __post_init__(self):
        assert self.s3_bucket, "S3_BUCKET must not be empty"
        assert self.buffer_seconds >= 0
        assert self.lookback_hours >= 0
        assert self.temp_dir, "TEMP_DIR must not be empty"


