from airflow.sdk import DAG, task, Asset
from datetime import datetime, timedelta
from pendulum import now
import re

# # from airflow.providers.common.sql.sensors.sql import SqlSensor
# from airflow.sdk.bases.sensor import PokeReturnValue
# from airflow.exceptions import AirflowSkipException
# from util.bronze_extractor import BronzeExtractor, PostgresHook

from common.config import CFG
from common.assets import build_assets
from common.dag_defaults import BRONZE_DEFAULTS
from common.dag_factory import BronzeDag

default_args = BRONZE_DEFAULTS.copy()

# Build Assets
bronze_assets = build_assets(cfg=CFG,option="bronze")
silver_triggers = build_assets(cfg=CFG, option="silvers_triggers")



# with DAG(
#     dag_id="bronze_high_volume",
#     schedule="*/1 * * * *",
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['bronze', 'traffic', 'high-volume']
# ) as dag:
    
#     # Subscriber Traffic Ingestion (High IO)
#     @task(outlets=[bronze_assets['bronze_traffic']])
#     def ingest_traffic(**context):
#         extractor = BronzeExtractor(
#             postgres_conn_id=CFG.POSTGRES_CONN_ID,
#             s3_conn_id=CFG.S3_CONN_ID,
#             s3_bucket=CFG.S3_BUCKET,
#             s3_prefix_base=CFG.BRONZE_PREFIX
#         )

#         return extractor.el(
#             schema=CFG.SCHEMA_NAME,
#             table=CFG.STATION_ST,
#             pk_column="traffic_id",
#             target_columns=[
#                 "traffic_id", "station_id", "event_time", "imsi_hash", 
#                 "tmsi", "ip_address", "destination_ip", "destination_port",
#                 "protocol", "bytes_up", "bytes_down", "packets_up", "packets_down",
#                 "latency_ms", "jitter_ms", "packet_loss_pct", "connection_duration_ms",
#                 "created_at", "updated_at"
#             ],
#             batch_id=f"st_{context['run_id']}",
#             cutoff_time=context['data_interval_start'],
#             time_column="event_time",
#         )
    
#     @task(outlets=[silver_triggers['silver_trigger_traffic']])
#     def signal_silver(**context):
#         interval_end = context['data_interval_end']

#         # Regex match to see if manual triggered run_id
#         if re.match(r"manual__", context['run_id']):
#             return {"station_traffic_manual_run": True}
        
#         if interval_end.minute != 0:
#             raise AirflowSkipException(f"Not hour boundary (minute={interval_end.minute})")
#         return {"hour_completed": interval_end.subtract(hours=1).hour}

#     ingest_traffic() >> signal_silver()


# with DAG(
#     dag_id="bronze_low_volume",
#     schedule="*/5 * * * *",
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['bronze', 'low-volume']
# ) as dag:
    
#     # Sensor
#     @task.sensor(poke_interval=30, timeout=120, mode="reschedule", soft_fail=True)
#     def wait_for_data() -> PokeReturnValue:
#         hook = PostgresHook(postgres_conn_id=CFG.POSTGRES_CONN_ID)
#         result = hook.get_first(f"""
#             SELECT 1 WHERE EXISTS (
#                 SELECT 1 FROM {CFG.SCHEMA_NAME}.{CFG.STATION_SE} 
#                 WHERE extracted_at IS NULL AND ingested_at < NOW() - INTERVAL '10 seconds'
#             ) OR EXISTS (
#                 SELECT 1 FROM {CFG.SCHEMA_NAME}.{CFG.STATION_PM} 
#                 WHERE extracted_at IS NULL AND ingested_at < NOW() - INTERVAL '10 seconds'
#             )
#         """)

#         return PokeReturnValue(is_done=result is not None)

#     # Station Events Ingestion
#     @task(outlets=[bronze_assets['bronze_events']])
#     def ingest_station_events(**context):
#         extractor = BronzeExtractor(
#             postgres_conn_id=CFG.POSTGRES_CONN_ID,
#             s3_conn_id=CFG.S3_CONN_ID,
#             s3_bucket=CFG.S3_BUCKET,
#             s3_prefix_base=CFG.BRONZE_PREFIX
#         )

#         return extractor.el(
#             schema=CFG.SCHEMA_NAME,
#             table=CFG.STATION_SE,
#             pk_column="event_id",
#             target_columns=[
#                 "event_id", "station_id", "event_time", "event_type", "severity",
#                 "description", "metadata", "target_station_id", "created_at", "updated_at"
#             ],
#             batch_id=f"se_{context['run_id']}",
#             cutoff_time=context['data_interval_start'],
#             time_column="event_time",
#         )  
    
#     @task(outlets=[silver_triggers['silver_trigger_events']])
#     def signal_silver_events(**context):
#         interval_end = context['data_interval_end']

#         if re.match(r"manual__", context['run_id']):
#             return {"station_events_manual_run": True}
        
#         if interval_end.minute != 0:
#             raise AirflowSkipException(f"Not hour boundary (minute={interval_end.minute})")
        
#         return {"hour_completed": interval_end.subtract(minutes=1).hour}
    
#     # Performance Metrics Ingestion
#     @task(outlets=[bronze_assets['bronze_metrics']])
#     def ingest_performance_metrics(**context):
#         extractor = BronzeExtractor(
#             postgres_conn_id=CFG.POSTGRES_CONN_ID,
#             s3_conn_id=CFG.S3_CONN_ID,
#             s3_bucket=CFG.S3_BUCKET,
#             s3_prefix_base=CFG.BRONZE_PREFIX
#         )

#         return extractor.el(
#             schema=CFG.SCHEMA_NAME,
#             table=CFG.STATION_PM,
#             pk_column="metric_id",
#             target_columns=[
#                 "metric_id", "station_id", "metric_time", "cpu_usage_pct", 
#                 "memory_usage_pct", "disk_usage_pct", "temperature_celsius", "power_consumption_watts",
#                 "uplink_throughput_mbps", "downlink_throughput_mbps", "active_subscribers",
#                 "signal_strength_dbm", "frequency_band", "channel_utilization_pct", "created_at", "updated_at"
#             ],
#             batch_id=f"pm_{context['run_id']}",
#             cutoff_time=context['data_interval_start'],
#             time_column="metric_time",
#         )
    
#     @task(outlets=[silver_triggers['silver_trigger_metrics']])
#     def signal_silver_metrics(**context):
#         interval_end = context['data_interval_end']

#         if re.match(r"manual__", context['run_id']):
#             return {"station_metrics_manual_run": True}
        
#         if interval_end.minute != 0:
#             raise AirflowSkipException(f"Not hour boundary (minute={interval_end.minute})")
        
#         return {"hour_completed": interval_end.subtract(minutes=1).hour}
    
#     # Flow
#     # Wait_for_data() ─┬──> ingest_station_events() ───────> signal_silver_events()
#     #                  └──> ingest_performance_metrics() ──> signal_silver_metrics()

#     ingested_se = ingest_station_events()
#     ingested_pm = ingest_performance_metrics()

#     wait_for_data() >> [ingested_se, ingested_pm]

#     ingested_se >> signal_silver_events()
#     ingested_pm >> signal_silver_metrics()
