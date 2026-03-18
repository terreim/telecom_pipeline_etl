from airflow.sdk import DAG

import logging

from shared.common.config import CFG
from shared.common.dag_defaults import STAGING_DEFAULTS
from shared.common.assets import build_assets
from shared.common.dag_factory import StagingSilverDag

from silver_lake import silver_assets

logger = logging.getLogger(__name__)

default_args = STAGING_DEFAULTS.copy()
staging_assets = build_assets(cfg=CFG, option="gold")
staging = StagingSilverDag(tags=['staging', 'telecom'])

with staging.create_dag(
    dag_id="staging_silver", 
    schedule=[asset for _, asset in silver_assets.items()],
    default_args=default_args
) as staging_silver:
    
    staging_database = staging.ensure_db()
    staging_tables = staging.ensure_tables()
    staging_dim = staging.create_dim_task()

    list_traffic, load_traffic, collect_traffic = staging.create_staging_task(
        staging_table=CFG.station_staging_st,
        silver_subpath=CFG.station_cleaned_st,
        outlets=[staging_assets['gold_trigger_traffic']]
    )

    list_events, load_events, collect_events = staging.create_staging_task(
        staging_table=CFG.station_staging_se,
        silver_subpath=CFG.station_cleaned_se,
        outlets=[staging_assets['gold_trigger_events']]
    )

    list_metrics, load_metrics, collect_metrics = staging.create_staging_task(
        staging_table=CFG.station_staging_pm,
        silver_subpath=CFG.station_cleaned_pm,
        outlets=[staging_assets['gold_trigger_metrics']]
    )

    schema = staging_database() >> staging_tables() >> staging_dim()

    traffic_batches = list_traffic()
    events_batches = list_events()
    metrics_batches = list_metrics()

    schema >> [traffic_batches, events_batches, metrics_batches]

    traffic_results = load_traffic.expand(batch=traffic_batches)
    events_results = load_events.expand(batch=events_batches)
    metrics_results = load_metrics.expand(batch=metrics_batches)

    collect_traffic(traffic_results)
    collect_events(events_results)
    collect_metrics(metrics_results)
