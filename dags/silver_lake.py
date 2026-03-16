from airflow.sdk import DAG

from shared.common.config import CFG
from shared.common.assets import build_assets
from shared.common.dag_defaults import SILVER_DEFAULTS
from shared.common.dag_factory import SilverDag

default_args = SILVER_DEFAULTS.copy()
staging_trigger_events = build_assets(cfg=CFG, option="staging_silver_triggers")['staging_trigger_events']
staging_trigger_metrics = build_assets(cfg=CFG, option="staging_silver_triggers")['staging_trigger_metrics']
staging_trigger_traffic = build_assets(cfg=CFG, option="staging_silver_triggers")['staging_trigger_traffic']

silver = SilverDag(tags=['silver', 'telecom'])

with silver.create_dag(dag_id="silver_lake", schedule="@hourly") as silver_lake:
    clean_traffic = silver.create_silver_task(
        outlets=[staging_trigger_traffic],
        bronze_table=CFG.station_st,
        silver_subpath=CFG.station_cleaned_st,
        transform_fn=silver.transformer.transform_traffic
    )
    clean_events = silver.create_silver_task(
        outlets=[staging_trigger_events],
        bronze_table=CFG.station_se,
        silver_subpath=CFG.station_cleaned_se,
        transform_fn=silver.transformer.transform_events
    )
    clean_metrics = silver.create_silver_task(
        outlets=[staging_trigger_metrics],
        bronze_table=CFG.station_pm,
        silver_subpath=CFG.station_cleaned_pm,
        transform_fn=silver.transformer.transform_metrics
    )

    clean_traffic()
    clean_events()
    clean_metrics()
