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
    staging_dim = staging.create_dim_task()

    staging_traffic = staging.create_staging_task(
        staging_table=CFG.station_staging_st,
        silver_subpath=CFG.station_cleaned_st,
        outlets=[staging_assets['gold_trigger_traffic']]
    )

    staging_events = staging.create_staging_task(
        staging_table=CFG.station_staging_se,
        silver_subpath=CFG.station_cleaned_se,
        outlets=[staging_assets['gold_trigger_events']]
    )

    staging_metrics = staging.create_staging_task(
        staging_table=CFG.station_staging_pm,
        silver_subpath=CFG.station_cleaned_pm,
        outlets=[staging_assets['gold_trigger_metrics']]
    )
    
    staging_dim() >> [staging_traffic(), staging_events(), staging_metrics()]
