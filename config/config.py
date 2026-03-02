import os

class PipelineConfig:
    #TODO: Migrate to use Variables or Params of Airflow

    # config.py
    TEMP_DIR = os.getenv("AIRFLOW_TEMP_DIR", "/opt/airflow/etl_temp")

    POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID", "postgres-oltp")
    S3_CONN_ID = os.environ.get("MINIO_CONN_ID", "minio_default")
    CLICKHOUSE_CONN_ID = os.environ.get("CLICKHOUSE_CONN_ID", "clickhouse_default")

    # Bucket and prefixes
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    S3_BUCKET = os.environ.get("MINIO_BUCKET", "station-lake")
    BRONZE_PREFIX = os.environ.get("MINIO_PREFIX_BRONZE", "bronze")
    SILVER_PREFIX = os.environ.get("MINIO_PREFIX_SILVER", "silver")
    GOLD_PREFIX = os.environ.get("MINIO_PREFIX_GOLD", "gold")
    QUARANTINE_PREFIX = os.environ.get("MINIO_PREFIX_QUARANTINE", "quarantine")

    # Schema and table names
    SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "telecom")

    # Bronze
    STATION_ST = os.environ.get("STATION_ST", "subscriber_traffic")
    STATION_BS = os.environ.get("STATION_BS", "base_station")
    STATION_PM = os.environ.get("STATION_PM", "performance_metrics")
    STATION_SE = os.environ.get("STATION_SE", "station_events")
    STATION_CF = os.environ.get("STATION_CF", "configuration")
    STATION_LC = os.environ.get("STATION_LC", "location")
    STATION_OP = os.environ.get("STATION_OP", "operator")

    # Silver 
    STATION_CLEANED_ST = os.environ.get("STATION_CLEANED_ST", "traffic_cleaned")
    STATION_CLEANED_PM = os.environ.get("STATION_CLEANED_PM", "metrics_cleaned")
    STATION_CLEANED_SE = os.environ.get("STATION_CLEANED_SE", "events_cleaned")

    # Staging Silver 
    STATION_STAGING_ST = os.environ.get("STATION_STAGING_ST", "staging_traffic_cleaned")
    STATION_STAGING_PM = os.environ.get("STATION_STAGING_PM", "staging_metrics_cleaned")
    STATION_STAGING_SE = os.environ.get("STATION_STAGING_SE", "staging_events_cleaned")

    # Gold
    STATION_HEALTH = os.environ.get("STATION_HEALTH", "gold_health_hourly")
    STATION_SLAC = os.environ.get("STATION_SLAC", "gold_sla_compliance")
    STATION_ANOMALY = os.environ.get("STATION_ANOMALY", "gold_anomaly_features")
    STATION_OUTAGE = os.environ.get("STATION_OUTAGE", "gold_outage_report")
    STATION_REGION = os.environ.get("STATION_REGION", "gold_region_daily")
    STATION_MAINTENANCE = os.environ.get("STATION_MAINTENANCE", "gold_maintenance_report")
    STATION_HANDOVER = os.environ.get("STATION_HANDOVER", "gold_handover_daily")
    STATION_ALARM = os.environ.get("STATION_ALARM", "gold_alarm_daily")