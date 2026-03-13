from shared.common.config import CFG
from shared.common.assets import build_assets
from shared.common.dag_defaults import BRONZE_DEFAULTS
from shared.common.dag_factory import BronzeDag

default_args = BRONZE_DEFAULTS.copy()

# Build Assets
bronze_assets = build_assets(cfg=CFG,option="bronze")
silver_triggers = build_assets(cfg=CFG, option="silvers_triggers")

bronze = BronzeDag(tags=['bronze', 'telecom'])

with bronze.create_dag(dag_id="bronze_high_volume", schedule="*/1 * * * *"):
    ingest_traffic = bronze.create_bronze_task(
        outlets=[bronze_assets['bronze_traffic']],
        table=CFG.station_st,
        pk_column="traffic_id",
        time_column="event_time",
        target_columns=[
                "traffic_id", "station_id", "event_time", "imsi_hash", 
                "tmsi", "ip_address", "destination_ip", "destination_port",
                "protocol", "bytes_up", "bytes_down", "packets_up", "packets_down",
                "latency_ms", "jitter_ms", "packet_loss_pct", "connection_duration_ms",
                "created_at", "updated_at"
            ],
    )
    ingest_traffic()

with bronze.create_dag(dag_id="bronze_low_volume", schedule="*/5 * * * *"):
    ingest_events = bronze.create_bronze_task(
        outlets=[bronze_assets['bronze_events']],
        table=CFG.station_se,
        pk_column="event_id",
        time_column="event_time",
        target_columns=[
                "event_id", "station_id", "event_time", "event_type", "severity",
                "description", "metadata", "target_station_id", "created_at", "updated_at"
            ],
    )
    ingest_metrics = bronze.create_bronze_task(
        outlets=[bronze_assets['bronze_metrics']],
        table=CFG.station_pm,
        pk_column="metric_id",
        time_column="metric_time",
        target_columns=[
                "metric_id", "station_id", "metric_time", "cpu_usage_pct", 
                "memory_usage_pct", "disk_usage_pct", "temperature_celsius", "power_consumption_watts",
                "uplink_throughput_mbps", "downlink_throughput_mbps", "active_subscribers",
                "signal_strength_dbm", "frequency_band", "channel_utilization_pct", "created_at", "updated_at"
            ],
    )

    ingest_events()
    ingest_metrics()