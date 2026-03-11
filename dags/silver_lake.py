from shared.common.config import CFG
from shared.common.assets import build_assets
from shared.common.dag_defaults import SILVER_DEFAULTS
from shared.common.dag_factory import SilverDag

default_args = SILVER_DEFAULTS.copy()

# staging_trigger_traffic = Asset("signal://staging/traffic")
# staging_trigger_events = Asset("signal://staging/events")
# staging_trigger_metrics = Asset("signal://staging/metrics")

silver = SilverDag(tags=['silver', 'telecom'])

with silver.create_dag(dag_id="silver_lake", schedule="@hourly"):
    clean_traffic = silver.create_silver_task(
        outlets=[build_assets(cfg=CFG, option="staging_silver_triggers")['staging_trigger_traffic']],
        bronze_table=CFG.station_st,
        silver_subpath=CFG.station_cleaned_st,
        transform_fn=silver.transformer.transform_traffic
    )
    clean_events = silver.create_silver_task(
        outlets=[build_assets(cfg=CFG, option="staging_silver_triggers")['staging_trigger_events']],
        bronze_table=CFG.station_se,
        silver_subpath=CFG.station_cleaned_se,
        transform_fn=silver.transformer.transform_events
    )
    clean_metrics = silver.create_silver_task(
        outlets=[build_assets(cfg=CFG, option="staging_silver_triggers")['staging_trigger_metrics']],
        bronze_table=CFG.station_pm,
        silver_subpath=CFG.station_cleaned_pm,
        transform_fn=silver.transformer.transform_metrics
    )

    clean_traffic()
    clean_events()
    clean_metrics()

# with DAG(
#     dag_id="silver_subscribers_traffic",
#     schedule=[silver_trigger_traffic],
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['subscriber', 'traffic', 'silver']
# ) as dag:
    
#     @task
#     def clean_subscribers_traffic(**context):
#         transformer = SilverTransformer(postgres_conn_id=CFG.POSTGRES_CONN_ID)
#         is_manual = context['run_id'].startswith('manual__')

#         pending_hours = transformer.find_unprocessed_hours(
#             bronze_table=CFG.STATION_ST,
#             silver_subpath=CFG.STATION_CLEANED_ST,
#             lookback_hours=720,
#         )

#         results = []
#         for year, month, day, hour in pending_hours:
#             result = transformer.transform_traffic(
#                 table_name=CFG.STATION_ST,
#                 year=year, month=month, day=day, hour=hour,
#                 batch_id=f"st_{context['run_id']}",
#                 manual_run=is_manual,
#             )
#             results.append(result)
#         return results
    
#     @task(outlets=[staging_trigger_traffic])
#     def signal_gold_traffic(results):
#         s3_keys = []
#         for r in results:
#             s3_keys.extend(r.get('silver_keys', []))
#         if not s3_keys:
#             raise AirflowSkipException("No silver keys to signal")
#         hours = [
#             (r['year'], r['month'], r['day'], r['hour'])
#             for r in results if r.get('silver_keys')
#         ]
#         yield Metadata(asset=staging_trigger_traffic, extra={"s3_keys": s3_keys, "hours": hours})

#     results = clean_subscribers_traffic()
#     signal_gold_traffic(results)

# with DAG(
#     dag_id="silver_performance_metrics",
#     schedule=[silver_trigger_metrics],
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['performance', 'metrics', 'silver']
# ) as dag:
    
#     @task
#     def clean_performance_metrics(**context):
#         transformer = SilverTransformer(postgres_conn_id=CFG.POSTGRES_CONN_ID)
#         is_manual = context['run_id'].startswith('manual__')

#         pending_hours = transformer.find_unprocessed_hours(
#             bronze_table=CFG.STATION_PM,
#             silver_subpath=CFG.STATION_CLEANED_PM,
#             lookback_hours=720,
#         )

#         results = []
#         for year, month, day, hour in pending_hours:
#             result = transformer.transform_metrics(
#                 table_name=CFG.STATION_PM,
#                 year=year, month=month, day=day, hour=hour,
#                 batch_id=f"pm_{context['run_id']}",
#                 manual_run=is_manual,
#             )
#             results.append(result)
#         return results
    
#     @task(outlets=[staging_trigger_metrics])
#     def signal_gold_metrics(results):
#         s3_keys = []
#         for r in results:
#             s3_keys.extend(r.get('silver_keys', []))
#         if not s3_keys:
#             raise AirflowSkipException("No silver keys to signal")
#         hours = [
#             (r['year'], r['month'], r['day'], r['hour'])
#             for r in results if r.get('silver_keys')
#         ]
#         yield Metadata(asset=staging_trigger_metrics, extra={"s3_keys": s3_keys, "hours": hours})

#     results = clean_performance_metrics()
#     signal_gold_metrics(results)
        
# with DAG(
#     dag_id="silver_station_events",
#     schedule=[silver_trigger_events],
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['station', 'events', 'silver']
# ) as dag:
    
#     @task
#     def clean_station_events(**context):
#         transformer = SilverTransformer(postgres_conn_id=CFG.POSTGRES_CONN_ID)
#         is_manual = context['run_id'].startswith('manual__')

#         pending_hours = transformer.find_unprocessed_hours(
#             bronze_table=CFG.STATION_SE,
#             silver_subpath=CFG.STATION_CLEANED_SE,
#             lookback_hours=720,
#         )

#         results = []
#         for year, month, day, hour in pending_hours:
#             result = transformer.transform_events(
#                 table_name=CFG.STATION_SE,
#                 year=year, month=month, day=day, hour=hour,
#                 batch_id=f"se_{context['run_id']}",
#                 manual_run=is_manual,
#             )
#             results.append(result)
#         return results

#     @task(outlets=[staging_trigger_events])
#     def signal_gold_events(results):
#         s3_keys = []
#         for r in results:
#             s3_keys.extend(r.get('silver_keys', []))
#         if not s3_keys:
#             raise AirflowSkipException("No silver keys to signal")
#         hours = [
#             (r['year'], r['month'], r['day'], r['hour'])
#             for r in results if r.get('silver_keys')
#         ]
#         yield Metadata(asset=staging_trigger_events, extra={"s3_keys": s3_keys, "hours": hours})
    
#     results = clean_station_events()
#     signal_gold_events(results)

