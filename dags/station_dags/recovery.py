"""
Recovery DAGs — manually triggered to clear markers and re-run pipeline stages.

clickhouse_recovery:
    Clear .ch_loaded markers → re-insert silver data into ClickHouse.
    Use when CH data is lost/corrupted but silver parquets in MinIO are fine.

silver_recovery:
    Clear _SUCCESS + .ch_loaded markers → re-transform from bronze.
    Use when a buggy ETL produced incorrect silver data and needs a full redo.

Both DAGs are manual-only (no schedule). Trigger via Airflow UI with optional
config to target specific subpaths or adjust lookback window:

    { "lookback_hours": 168, "subpaths": ["traffic_cleaned"] }
"""

from airflow.sdk import DAG, task
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

from station_dags.config.config import PipelineConfig as C
from station_dags.util.warehouse_loader import ClickHouseLoader
from station_dags.util.gold_aggregator import GoldAggregator

import logging
log = logging.getLogger(__name__)

default_args = {
    "owner": "data-team",
    "retries": 50,
    "retry_delay": timedelta(seconds=60),
    "execution_timeout": timedelta(minutes=60),
}

ALL_SILVER_SUBPATHS = {
    C.STATION_CLEANED_ST: C.STATION_STAGING_ST,
    C.STATION_CLEANED_PM: C.STATION_STAGING_PM,
    C.STATION_CLEANED_SE: C.STATION_STAGING_SE,
}

# =============================================================================
# ClickHouse Recovery — clear .ch_loaded markers, then re-load to CH
# =============================================================================

with DAG(
    dag_id="clickhouse_recovery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "clickhouse"],
    default_args=default_args,
    params={
        "lookback_hours": 168,
        "subpaths": [],
    },
) as dag:

    @task
    def clear_ch_loaded_markers(**context):
        """Delete .ch_loaded markers so staging tasks will re-insert."""
        loader = ClickHouseLoader()
        params = context["params"]
        lookback = int(params.get("lookback_hours", 168))
        target_subpaths = params.get("subpaths") or list(ALL_SILVER_SUBPATHS.keys())

        results = {}
        for subpath in target_subpaths:
            results[subpath] = loader.clear_ch_loaded_markers(
                silver_subpath=subpath,
                lookback_hours=lookback,
            )
        return results

    @task
    def reload_to_clickhouse(clear_results, **context):
        """Re-insert all unloaded silver files into ClickHouse staging.
        
        Uses partition-level glob INSERTs to minimize MergeTree part creation.
        """
        loader = ClickHouseLoader()
        params = context["params"]
        lookback = int(params.get("lookback_hours", 168))
        target_subpaths = params.get("subpaths") or list(ALL_SILVER_SUBPATHS.keys())

        total_loaded = 0
        results = {}

        for subpath in target_subpaths:
            staging_table = ALL_SILVER_SUBPATHS[subpath]
            unloaded = loader.find_unloaded_silver_keys(
                silver_subpath=subpath,
                lookback_hours=lookback,
            )

            if unloaded:
                batch_result = loader.insert_silver_partition_batch(
                    silver_subpath=subpath,
                    staging_table=staging_table,
                    s3_keys=unloaded,
                )
                total_loaded += batch_result["files"]
                results[subpath] = {
                    "found": len(unloaded),
                    "loaded": batch_result["files"],
                    "partitions": batch_result["partitions"],
                }
            else:
                results[subpath] = {"found": 0, "loaded": 0}

        if total_loaded == 0:
            raise AirflowSkipException("No unloaded silver files found after clearing markers")

        return results

    cleared = clear_ch_loaded_markers()
    reload_to_clickhouse(cleared)


# =============================================================================
# Silver Recovery — clear _SUCCESS (+ .ch_loaded) markers, then re-transform
# =============================================================================

with DAG(
    dag_id="silver_recovery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "silver"],
    default_args=default_args,
    params={
        "lookback_hours": 168,
        "subpaths": [],
    },
) as dag:

    @task
    def clear_success_markers(**context):
        """Delete _SUCCESS + .ch_loaded markers so silver re-transforms from bronze."""
        loader = ClickHouseLoader()
        params = context["params"]
        lookback = int(params.get("lookback_hours", 168))
        target_subpaths = params.get("subpaths") or list(ALL_SILVER_SUBPATHS.keys())

        results = {}
        for subpath in target_subpaths:
            results[subpath] = loader.clear_success_markers(
                silver_subpath=subpath,
                lookback_hours=lookback,
            )
        return results

    clear_success_markers()


# =============================================================================
# Gold Recovery — clear gold CH tables, then re-aggregate from staging data
#
# Bypasses the normal signal chain (health_hourly → anomaly → SLA → …) and
# calls every GoldAggregator method directly, one hour / day at a time.
#
# Why not signals?
#   Airflow asset signals are fire-and-forget.  A task that yields Metadata
#   cannot wait for the downstream DAG to finish processing.  So there is no
#   way to "emit signal → wait for downstream → next iteration".  Instead we
#   inline everything and iterate sequentially.  Each hour's DataFrame is
#   ~1 000 rows — no OOM risk.
#
# Retry safety:
#   Each hour / day writes an S3 marker (_DONE) after successful processing.
#   On retry, already-done items are skipped.  Before processing an item,
#   its CH data is deleted per-item so partial inserts don't cause duplicates.
# =============================================================================

with DAG(
    dag_id="gold_recovery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "gold"],
    default_args=default_args,
    params={
        "lookback_hours": 168,
        "skip_daily": False,
    },
) as dag:

    @task
    def discover_periods(**context):
        """Build sorted lists of hours and days that need reprocessing."""
        import pandas as pd

        lookback = int(context["params"].get("lookback_hours", 168))
        now = pd.Timestamp.now("UTC").floor("h")

        hours = []
        days_set = set()
        for i in range(lookback):
            ts = now - pd.Timedelta(hours=i)
            hours.append((ts.year, ts.month, ts.day, ts.hour))
            days_set.add((ts.year, ts.month, ts.day))

        return {
            "hours": sorted(hours),
            "days": sorted(days_set),
        }

    @task
    def clear_gold_markers(periods, **context):
        """Delete all gold _DONE markers for the target range.

        Runs before recompute tasks so every hour/day will be re-processed.
        """
        aggregator = GoldAggregator()
        hours = [tuple(h) for h in periods["hours"]]
        days = [tuple(d) for d in periods["days"]]

        skip_daily = context["params"].get("skip_daily", False)

        h_cleared = aggregator.clear_hourly_markers(hours)
        d_cleared = 0 if skip_daily else aggregator.clear_daily_markers(days)

        return {"hourly_cleared": h_cleared, "daily_cleared": d_cleared}

    @task
    def recompute_hourly(periods, markers_cleared, **context):
        """Re-compute gold_health_hourly and gold_anomaly_features.

        Processes one hour at a time.  Skips hours that already have a
        ``_DONE`` marker (from a previous partial run).  For each new hour:
        delete CH data → recompute → write marker.
        """
        aggregator = GoldAggregator()
        hours = [tuple(h) for h in periods["hours"]]

        processed = 0
        skipped = 0
        for year, month, day, hour in hours:
            if aggregator.is_hour_processed(year, month, day, hour):
                skipped += 1
                continue

            aggregator.delete_gold_hour(year, month, day, hour)
            aggregator.gold_health_hourly(year, month, day, hour)
            aggregator.gold_anomaly_features(year, month, day, hour)
            aggregator.mark_hour_done(year, month, day, hour)

            processed += 1
            if processed % 12 == 0:
                log.info(
                    "Gold hourly recovery: %d processed, %d skipped / %d total",
                    processed, skipped, len(hours),
                )

        return {"processed": processed, "skipped": skipped}

    @task
    def recompute_daily(periods, hourly_done, **context):
        """Re-compute all daily gold tables.

        Must run after hourly (SLA, outage, etc. read from gold_health_hourly).
        Skips days that already have a ``_DONE`` marker.  For each new day:
        delete CH data → recompute all 6 daily tables → write marker.
        """
        skip_daily = context["params"].get("skip_daily", False)
        if skip_daily:
            raise AirflowSkipException("skip_daily=True, skipping daily recompute")

        aggregator = GoldAggregator()
        days = [tuple(d) for d in periods["days"]]

        processed = 0
        skipped = 0
        for year, month, day in days:
            if aggregator.is_day_processed(year, month, day):
                skipped += 1
                continue

            aggregator.delete_gold_day(year, month, day)
            aggregator.gold_sla_compliance(year, month, day)
            aggregator.gold_outage_report(year, month, day)
            aggregator.gold_maintenance_report(year, month, day)
            aggregator.gold_handover_report(year, month, day)
            aggregator.gold_alarm_report(year, month, day)
            aggregator.gold_region_daily(year, month, day)
            aggregator.mark_day_done(year, month, day)

            processed += 1

        return {"processed": processed, "skipped": skipped}

    periods = discover_periods()
    markers_cleared = clear_gold_markers(periods)
    hourly_done = recompute_hourly(periods, markers_cleared)
    recompute_daily(periods, hourly_done)