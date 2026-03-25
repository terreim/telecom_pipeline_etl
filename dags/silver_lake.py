from airflow.sdk import DAG

from shared.common.config import CFG
from shared.common.assets import build_assets
from shared.common.dag_defaults import SILVER_DEFAULTS
from shared.common.dag_factory import SilverDag, SparkSilverDag

from bronze_lake import bronze_assets

default_args = SILVER_DEFAULTS.copy()
silver_assets = build_assets(cfg=CFG, option="staging_silver_triggers")
spark_silver = SparkSilverDag(tags=['spark_silver', 'telecom'])

with spark_silver.create_dag(
    dag_id="spark_silver_lake", 
    schedule=[asset for _, asset in bronze_assets.items()],
    default_args=default_args
) as spark_silver_lake:
    clean_traffic = spark_silver.create_spark_silver_task(
        bronze_table=CFG.station_st,
        silver_subpath=CFG.station_cleaned_st,
        transform_method="transform_traffic"
    )
    signal_traffic = spark_silver.create_signal(
        task_id="signal_staging_traffic",
        outlets=[silver_assets['staging_trigger_traffic']]
    )

    clean_events = spark_silver.create_spark_silver_task(
        bronze_table=CFG.station_se,
        silver_subpath=CFG.station_cleaned_se,
        transform_method="transform_events"
    )
    signal_events = spark_silver.create_signal(
        task_id="signal_staging_events",
        outlets=[silver_assets['staging_trigger_events']]
    )

    clean_metrics = spark_silver.create_spark_silver_task(
        bronze_table=CFG.station_pm,
        silver_subpath=CFG.station_cleaned_pm,
        transform_method="transform_metrics"
    )
    signal_metrics = spark_silver.create_signal(
        task_id="signal_staging_metrics",
        outlets=[silver_assets['staging_trigger_metrics']]
    )

    clean_traffic() >> signal_traffic()
    clean_events() >> signal_events()
    clean_metrics() >> signal_metrics()


# silver = SilverDag(tags=['silver', 'telecom'])

# with silver.create_dag(
#     dag_id="silver_lake", 
#     schedule=[asset for _, asset in bronze_assets.items()],
#     default_args=default_args
# ) as silver_lake:
#     clean_traffic = silver.create_silver_task(
#         bronze_table=CFG.station_st,
#         silver_subpath=CFG.station_cleaned_st,
#         transform_fn=silver.transformer.transform_traffic
#     )
#     signal_traffic = silver.create_signal(
#         task_id="signal_staging_traffic",
#         outlets=[silver_assets['staging_trigger_traffic']]
#     )

#     clean_events = silver.create_silver_task(
#         bronze_table=CFG.station_se,
#         silver_subpath=CFG.station_cleaned_se,
#         transform_fn=silver.transformer.transform_events
#     )
#     signal_events = silver.create_signal(
#         task_id="signal_staging_events",
#         outlets=[silver_assets['staging_trigger_events']]
#     )

#     clean_metrics = silver.create_silver_task(
#         bronze_table=CFG.station_pm,
#         silver_subpath=CFG.station_cleaned_pm,
#         transform_fn=silver.transformer.transform_metrics
#     )
#     signal_metrics = silver.create_signal(
#         task_id="signal_staging_metrics",
#         outlets=[silver_assets['staging_trigger_metrics']]
#     )

#     clean_traffic() >> signal_traffic()
#     clean_events() >> signal_events()
#     clean_metrics() >> signal_metrics()
