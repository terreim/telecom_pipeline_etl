from airflow.sdk import DAG, task
from airflow.sdk import Asset, Metadata
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import json

from pendulum import now as pendulum_now

from config.config import PipelineConfig as C

from util.silver_transformer import SilverTransformer
from util.gold_aggregator import GoldAggregator
from util.warehouse_loader import ClickHouseLoader
from silver_lake import staging_trigger_events, staging_trigger_metrics, staging_trigger_traffic

default_args = {
    "owner": "data-team", 
    "retries": 50,
    "retry_delay": timedelta(seconds=60),
    # "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "sla": timedelta(hours=2),
}

gold_traffic_ready = Asset("signal://gold/traffic")
gold_metrics_ready = Asset("signal://gold/metrics")
gold_events_ready = Asset("signal://gold/events")

health_hourly_ready = Asset("signal://gold/health_hourly")
health_daily_ready = Asset("signal://gold/health_daily")
sla_ready = Asset("signal://gold/sla_compliance")


# =============================================================================
# Staging DAGs - Load silver data from S3 to ClickHouse staging tables
# =============================================================================

with DAG(
    dag_id="gold_staging_dim",
    schedule=(staging_trigger_traffic | staging_trigger_metrics | staging_trigger_events),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'dim', 'traffic'],
    default_args=default_args
) as dag:
    
    @task
    def load_dim_to_clickhouse():
        loader = ClickHouseLoader()

        transformer = SilverTransformer(postgres_conn_id=C.POSTGRES_CONN_ID)
        dim_df = transformer._load_station_dimension()
        loader.load_dim(schema=C.SCHEMA_NAME, table_name="dim_station", dict_name="dict_station", dim_df=dim_df)
        return f"Loaded dim_station with {len(dim_df)} records"

    load_dim_to_clickhouse()

with DAG(
    dag_id="gold_staging_traffic_clickhouse",
    schedule=[staging_trigger_traffic],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'silver', 'traffic'],
    default_args=default_args
) as dag:
    
    @task(outlets=[gold_traffic_ready])
    def insert_traffic_to_clickhouse(**context):
        loader = ClickHouseLoader()

        events = context['triggering_asset_events'][staging_trigger_traffic]
        all_hours = []
        loaded = 0
        skipped = 0
        for event in events:
            all_hours.extend(event.extra.get("hours", []))
            for s3_key in event.extra["s3_keys"]:
                if loader._is_loaded(s3_key):
                    skipped += 1
                    continue
                loader.insert_silver_to_clickhouse(
                    s3_key=s3_key,
                    staging_table=C.STATION_STAGING_ST,
                )
                loaded += 1
        unique_hours = list(set(map(tuple, all_hours)))
        yield Metadata(asset=gold_traffic_ready, extra={"hours": unique_hours})
        return {"loaded": loaded, "skipped": skipped}
    
    insert_traffic_to_clickhouse()
    
with DAG(
    dag_id="gold_staging_metrics_clickhouse",
    schedule=[staging_trigger_metrics],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'silver', 'metrics'],
    default_args=default_args
) as dag:
    
    @task(outlets=[gold_metrics_ready])
    def insert_metrics_to_clickhouse(**context):
        loader = ClickHouseLoader()

        events = context['triggering_asset_events'][staging_trigger_metrics]
        all_hours = []
        loaded = 0
        skipped = 0
        for event in events:
            all_hours.extend(event.extra.get("hours", []))
            for s3_key in event.extra["s3_keys"]:
                if loader._is_loaded(s3_key):
                    skipped += 1
                    continue
                loader.insert_silver_to_clickhouse(
                    s3_key=s3_key,
                    staging_table=C.STATION_STAGING_PM,
                )
                loaded += 1
        unique_hours = list(set(map(tuple, all_hours)))
        yield Metadata(asset=gold_metrics_ready, extra={"hours": unique_hours})
        return {"loaded": loaded, "skipped": skipped}
    
    insert_metrics_to_clickhouse()

with DAG(
    dag_id="gold_staging_events_clickhouse",
    schedule=[staging_trigger_events],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'silver', 'events'],
    default_args=default_args
) as dag:
    
    @task(outlets=[gold_events_ready])
    def insert_events_to_clickhouse(**context):
        loader = ClickHouseLoader()

        events = context['triggering_asset_events'][staging_trigger_events]
        all_hours = []
        loaded = 0
        skipped = 0
        for event in events:
            all_hours.extend(event.extra.get("hours", []))
            for s3_key in event.extra["s3_keys"]:
                if loader._is_loaded(s3_key):
                    skipped += 1
                    continue
                loader.insert_silver_to_clickhouse(
                    s3_key=s3_key,
                    staging_table=C.STATION_STAGING_SE,
                )
                loaded += 1
        unique_hours = list(set(map(tuple, all_hours)))
        yield Metadata(asset=gold_events_ready, extra={"hours": unique_hours})
        return {"loaded": loaded, "skipped": skipped}
    
    insert_events_to_clickhouse()

# =============================================================================
# Gold Catchup DAG
# Catches full_ silver files that were never loaded to CH staging
# (e.g. OOM killed the silver task after write but before gold signal fired)
# =============================================================================

with DAG(
    dag_id="gold_staging_catchup",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'catchup', 'recovery'],
    default_args=default_args,
) as dag:

    @task
    def catchup_unloaded_silver():
        loader = ClickHouseLoader()

        # Map silver subpaths to their CH staging tables
        silver_to_staging = {
            C.STATION_CLEANED_ST: C.STATION_STAGING_ST,
            C.STATION_CLEANED_PM: C.STATION_STAGING_PM,
            C.STATION_CLEANED_SE: C.STATION_STAGING_SE,
        }

        total_loaded = 0
        results = {}

        for silver_subpath, staging_table in silver_to_staging.items():
            unloaded = loader.find_unloaded_silver_keys(
                silver_subpath=silver_subpath,
                lookback_hours=24,
            )

            if unloaded:
                batch_result = loader.insert_silver_partition_batch(
                    silver_subpath=silver_subpath,
                    staging_table=staging_table,
                    s3_keys=unloaded,
                )
                total_loaded += batch_result["files"]
                results[silver_subpath] = {
                    "found": len(unloaded),
                    "loaded": batch_result["files"],
                    "partitions": batch_result["partitions"],
                }
            else:
                results[silver_subpath] = {"found": 0, "loaded": 0}

        if total_loaded == 0:
            raise AirflowSkipException("No unloaded silver files found")

        return results

    catchup_unloaded_silver()

# =============================================================================
# Gold Logic DAGs
# Signal Dependency Chain:
#
#  gold_traffic_ready ──┐
#  gold_metrics_ready ──┤──> health_hourly ──> anomaly_features
#  gold_events_ready ───┘        │
#                                │ (fires on hour 23 only)
#                                ▼
#                         health_daily_ready
#                       ┌───┬────┼─────┬────────┐
#                       ▼   ▼    ▼     ▼        ▼
#                      SLA  outage maint alarm  handover
#                       │
#                       ▼
#                   sla_ready
#                       │
#                       ▼
#                 region_daily
# =============================================================================

with DAG(
    dag_id="gold_health_hourly",
    schedule=[gold_traffic_ready, gold_metrics_ready, gold_events_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'health', 'hourly'],
    default_args=default_args,
    params={
        "trigger_time": "",       # ISO datetime e.g. "2026-02-25T14:00:00"
        "lookback_hours": 1,      # hours back from trigger_time (manual only)
    },
) as dag:
    
    @task(outlets=[health_hourly_ready, health_daily_ready])
    def compute_health_hourly(**context):
        aggregator = GoldAggregator()
        
        all_hours = set()

        for asset in [gold_traffic_ready, gold_metrics_ready, gold_events_ready]:
            for event in context['triggering_asset_events'].get(asset, []):
                for h in event.extra.get("hours", []):
                    all_hours.add(tuple(h))

        # If manually triggering gold_health_hourly
        if not all_hours and context['run_id'].startswith('manual__'):
            params = context['params']
            trigger_str = params.get("trigger_time", "")

            if trigger_str:
                anchor = datetime.fromisoformat(trigger_str)
            else:
                anchor = pendulum_now()

            lookback = int(params.get("lookback_hours", 1))
            for i in range(lookback):
                ts = anchor - timedelta(hours=i)
                all_hours.add((ts.year, ts.month, ts.day, ts.hour))

        results = []
        for year, month, day, hour in all_hours:
            result = aggregator.gold_health_hourly(year=year, month=month, day=day, hour=hour)
            results.append(result)
        
        yield Metadata(asset=health_hourly_ready, extra={"hours": [list(h) for h in all_hours]})

        # Collect all unique days that need daily aggregation:
        # - hour 23 means the day is fully complete (streaming mode)
        # - manual backfill: every day in the range
        daily_days = set()
        is_manual = context['run_id'].startswith('manual__')
        for y, m, d, h in all_hours:
            if h == 23 or is_manual:
                daily_days.add((y, m, d))

        if daily_days:
            yield Metadata(asset=health_daily_ready, extra={
                "days": [list(d) for d in sorted(daily_days)],
            })

        return results
    
    compute_health_hourly()

with DAG(
    dag_id="gold_anomaly_features",
    schedule=[health_hourly_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'anomaly', 'hourly'],
    default_args=default_args,
) as dag:
    
    @task
    def compute_anomaly_features(**context):
        aggregator = GoldAggregator()
        hours = set()

        for event in context['triggering_asset_events'].get(health_hourly_ready, []):
            for h in event.extra.get("hours", []):
                hours.add(tuple(h))
        
        results = []
        for year, month, day, hour in hours:
            result = aggregator.gold_anomaly_features(year=year, month=month, day=day, hour=hour)
            results.append(result)
        
        return results

    compute_anomaly_features()

with DAG( 
    dag_id="gold_sla_compliance",
    schedule=[health_daily_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'sla', 'compliance'],
    default_args=default_args,
) as dag:
    
    @task(outlets=[sla_ready])
    def compute_sla_compliance(**context):
        aggregator = GoldAggregator()

        all_days = set()
        for event in context['triggering_asset_events'].get(health_daily_ready, []):
            for d in event.extra.get("days", []):
                all_days.add(tuple(d))

        results = []
        for year, month, day in sorted(all_days):
            result = aggregator.gold_sla_compliance(year=year, month=month, day=day)
            results.append(result)

        yield Metadata(asset=sla_ready, extra={"days": [list(d) for d in sorted(all_days)]})
        return results

    compute_sla_compliance()
    
with DAG(
    dag_id="gold_region_daily",
    schedule=[sla_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'region', 'daily'],
    default_args=default_args,
) as dag:
    
    @task
    def compute_region_daily(**context):
        aggregator = GoldAggregator()

        all_days = set()
        for event in context['triggering_asset_events'].get(sla_ready, []):
            for d in event.extra.get("days", []):
                all_days.add(tuple(d))

        results = []
        for year, month, day in sorted(all_days):
            result = aggregator.gold_region_daily(year=year, month=month, day=day)
            results.append(result)
        return results

    compute_region_daily()

with DAG(
    dag_id="gold_outage_report",
    schedule=[health_daily_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'outage', 'report'],
    default_args=default_args,
) as dag:
    
    @task
    def compute_outage_report(**context):
        aggregator = GoldAggregator()

        all_days = set()
        for event in context['triggering_asset_events'].get(health_daily_ready, []):
            for d in event.extra.get("days", []):
                all_days.add(tuple(d))

        results = []
        for year, month, day in sorted(all_days):
            result = aggregator.gold_outage_report(year=year, month=month, day=day)
            results.append(result)
        return results

    compute_outage_report()
    
with DAG(   
    dag_id="gold_maintenance_report",
    schedule=[health_daily_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'maintenance', 'report'],
    default_args=default_args,
) as dag:
    
    @task
    def compute_maintenance_report(**context):
        aggregator = GoldAggregator()

        all_days = set()
        for event in context['triggering_asset_events'].get(health_daily_ready, []):
            for d in event.extra.get("days", []):
                all_days.add(tuple(d))

        results = []
        for year, month, day in sorted(all_days):
            result = aggregator.gold_maintenance_report(year=year, month=month, day=day)
            results.append(result)
        return results
    
    compute_maintenance_report()

with DAG(
    dag_id="gold_handover_report",
    schedule=[health_daily_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'handover', 'report'],
    default_args=default_args,
) as dag:
    
    @task
    def compute_handover_report(**context):
        aggregator = GoldAggregator()

        all_days = set()
        for event in context['triggering_asset_events'].get(health_daily_ready, []):
            for d in event.extra.get("days", []):
                all_days.add(tuple(d))

        results = []
        for year, month, day in sorted(all_days):
            result = aggregator.gold_handover_report(year=year, month=month, day=day)
            results.append(result)
        return results
    
    compute_handover_report()

with DAG(
    dag_id="gold_alarm_report",
    schedule=[health_daily_ready],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'alarm', 'report'],
    default_args=default_args,
) as dag:
    
    @task
    def compute_alarm_report(**context):
        aggregator = GoldAggregator()

        all_days = set()
        for event in context['triggering_asset_events'].get(health_daily_ready, []):
            for d in event.extra.get("days", []):
                all_days.add(tuple(d))

        results = []
        for year, month, day in sorted(all_days):
            result = aggregator.gold_alarm_report(year=year, month=month, day=day)
            results.append(result)
        return results
    
    compute_alarm_report()