from airflow.sdk import DAG

from shared.common.config import CFG
from shared.common.assets import build_assets
from shared.common.dag_defaults import BRONZE_DEFAULTS
from shared.common.dag_factory import BronzeDag
from shared.common.schema_registry import REGISTRY

default_args = BRONZE_DEFAULTS.copy()
bronze_assets = build_assets(cfg=CFG, option="silvers_triggers")
bronze = BronzeDag(tags=['bronze', 'telecom'])

with bronze.create_dag(
    dag_id="bronze_high_volume", 
    schedule="*/1 * * * *",
    default_args=default_args
) as bronze_high_volume:
    traffic_contract = REGISTRY.get(CFG.station_st)
    ingest_traffic = bronze.create_bronze_task(
        table=CFG.station_st,
        pk_column=traffic_contract.primary_key,
        time_column=traffic_contract.time_column,
        target_columns=traffic_contract.all_source_columns(),
    )
    signal_traffic = bronze.create_signal(
        task_id="signal_silver_traffic",
        outlets=[bronze_assets['silver_trigger_traffic']],
        boundary_type='hourly'
    )

    ingest_traffic() >> signal_traffic()

with bronze.create_dag(
    dag_id="bronze_low_volume", 
    schedule="*/5 * * * *",
    default_args=default_args
) as bronze_low_volume:
    events_contract = REGISTRY.get(CFG.station_se)
    ingest_events = bronze.create_bronze_task(
        table=CFG.station_se,
        pk_column=events_contract.primary_key,
        time_column=events_contract.time_column,
        target_columns=events_contract.all_source_columns(),
    )
    signal_events = bronze.create_signal(
        task_id="signal_silver_events",
        outlets=[bronze_assets['silver_trigger_events']],
        boundary_type='hourly'
    )

    metrics_contract = REGISTRY.get(CFG.station_pm)
    ingest_metrics = bronze.create_bronze_task(
        table=CFG.station_pm,
        pk_column=metrics_contract.primary_key,
        time_column=metrics_contract.time_column,
        target_columns=metrics_contract.all_source_columns(),
    )
    signal_metrics = bronze.create_signal(
        task_id="signal_silver_metrics",
        outlets=[bronze_assets['silver_trigger_metrics']],
        boundary_type='hourly'
    )

    ingest_events() >> signal_events()
    ingest_metrics() >> signal_metrics()