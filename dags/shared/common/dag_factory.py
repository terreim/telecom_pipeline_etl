from datetime import datetime
from typing import Callable
import re

from airflow.sdk import Asset, Metadata
from airflow.sdk import DAG, task
from airflow.exceptions import AirflowSkipException

from shared.common.config import CFG
from shared.util.bronze_extractor import BronzeExtractor
from shared.util.silver_transformer import SilverTransformer
from shared.util.staging_loader import ClickHouseLoader
from shared.util.gold_aggregator import GoldAggregator


class DagFactory:
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
        ):
        self.start_date = start_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.tags = tags or []

    def create_dag(self, dag_id: str, schedule, default_args: dict = None) -> DAG:
        return DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=self.start_date,
            catchup=self.catchup,
            max_active_runs=self.max_active_runs,
            tags=self.tags,
            default_args=default_args or {},
        )

    def create_signal(
            self,
            task_id: str,
            outlets: list[Asset] | None = None,
            boundary_type: str = 'hourly',
        ) -> Callable:
        """Wake-up signal task — fires an outlet asset when a time boundary is crossed.

        boundary_type:
            'hourly' — fires once per hour (skips non-hour-boundary runs)
            'daily'  — fires once per day  (skips non-midnight runs)
            None     — always fires (asset-triggered DAGs where cron doesn't apply)
        """
        @task(outlets=outlets or [], task_id=task_id)
        def signal(**context):
            if context['run_id'].startswith('manual__'):
                return {"manual": True}

            if boundary_type == 'hourly':
                interval_end = context['data_interval_end']
                if interval_end.minute != 0:
                    raise AirflowSkipException(
                        f"Not hour boundary (minute={interval_end.minute})"
                    )
            elif boundary_type == 'daily':
                interval_end = context['data_interval_end']
                if interval_end.hour != 0 or interval_end.minute != 0:
                    raise AirflowSkipException(
                        f"Not day boundary (hour={interval_end.hour}, minute={interval_end.minute})"
                    )

        return signal


class BronzeDag(DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
        ):
        super().__init__(start_date, catchup, max_active_runs, tags)
        self.extractor = BronzeExtractor()

    def create_bronze_task(
            self,
            table: str,
            pk_column: str,
            time_column: str,
            outlets: list[Asset] | None = None,
            target_columns: list[str] = None,
        ) -> Callable:

        @task(outlets=outlets or [], task_id=f"bronze_{table}")
        def ingest(**context):
            return self.extractor.extract_bronze(
                schema=CFG.schema_name,
                table=table,
                pk_column=pk_column,
                target_columns=target_columns,
                batch_id=f"{table[:2]}_{context['run_id']}",
                time_column=time_column,
            )

        return ingest


class SilverDag(DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
            postgres_conn_id: str = CFG.postgres_conn_id,
        ):
        super().__init__(start_date, catchup, max_active_runs, tags)
        self.transformer = SilverTransformer(postgres_conn_id=postgres_conn_id)

    def create_silver_task(
            self,
            bronze_table: str,
            silver_subpath: str,
            transform_fn: Callable,
            outlets: list[Asset] | None = None,
            lookback_hours: int = 720,
        ) -> Callable:

        @task(outlets=outlets or [], task_id=f"transform_{bronze_table}")
        def transform(**context):
            return transform_fn(
                table_name=bronze_table,
                silver_subpath=silver_subpath,
                batch_id=f"{bronze_table[:2]}_{context['run_id']}",
                lookback_range=lookback_hours,
            )

        return transform


class StagingSilverDag(DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
            postgres_conn_id: str = CFG.postgres_conn_id,
        ):
        super().__init__(start_date, catchup, max_active_runs, tags)
        self.transformer = SilverTransformer(postgres_conn_id=postgres_conn_id)

    def create_dim_task(self, outlets: list[Asset] | None = None) -> Callable:
        """Load the station dimension into ClickHouse."""

        @task(outlets=outlets or [], task_id="staging_dim_station")
        def load_dim():
            loader = ClickHouseLoader()
            dim_df = self.transformer._load_station_dimension()
            result = loader.load_dim(
                schema=CFG.schema_name,
                table_name="dim_station",
                dim_df=dim_df,
            )
            return result

        return load_dim

    def create_staging_task(
            self,
            staging_table: str,
            silver_subpath: str,
            outlets: list[Asset] | None = None,
            lookback_range: int = 720,
        ) -> Callable:
        """Load a silver subpath into a CH staging table.

        Yields Metadata with {"hours": [...]} on the outlet so gold knows
        which partitions are ready to aggregate.
        """

        @task(outlets=outlets or [], task_id=f"staging_{silver_subpath}")
        def load_staging(**context):
            loader = ClickHouseLoader()
            results = loader.load_staging(
                staging_table=staging_table,
                silver_subpath=silver_subpath,
                batch_id=f"staging_{context['run_id']}",
                lookback_range=lookback_range,
            )

            hours = set()
            for r in results:
                partition = r.get("partition", "")
                # year=YYYY/month=MM/day=DD/hour=HH/
                m = re.search(
                    r'year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)',
                    partition,
                )
                if m:
                    hours.add(tuple(int(x) for x in m.groups()))

            if outlets and hours:
                for outlet in outlets: # Should take one outlet at most
                    yield Metadata(
                        asset=outlet,
                        extra={"hours": [list(h) for h in sorted(hours)]},
                    )

            return results

        return load_staging


class GoldDag(DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
        ):
        super().__init__(start_date, catchup, max_active_runs, tags)

    def create_hourly_task(
            self,
            report_name: str,
            outlets: list[Asset] | None = None,
        ) -> Callable:
        """Task for hourly-granularity gold reports (health_hourly, anomaly_features, …)."""

        @task(outlets=outlets or [], task_id=f"compute_{report_name}")
        def run_hourly(**context):
            aggregator = GoldAggregator()
            report = aggregator.reports()[report_name]

            hours = set()
            for events in context['triggering_asset_events'].values():
                for event in events:
                    for h in event.extra.get("hours", []):
                        hours.add(tuple(h))

            return [
                aggregator.aggregate(report, year, month, day, hour)
                for year, month, day, hour in sorted(hours)
            ]

        return run_hourly

    def create_daily_task(
            self,
            report_name: str,
            outlets: list[Asset] | None = None,
        ) -> Callable:
        """Task for daily-granularity gold reports (sla_compliance, outage_report, …)."""

        @task(outlets=outlets or [], task_id=f"compute_{report_name}")
        def run_daily(**context):
            aggregator = GoldAggregator()
            report = aggregator.reports()[report_name]

            days = set()
            for events in context['triggering_asset_events'].values():
                for event in events:
                    for d in event.extra.get("days", []):
                        days.add(tuple(d))

            return [
                aggregator.aggregate(report, year, month, day)
                for year, month, day in sorted(days)
            ]

        return run_daily


class RecoveryDag(DagFactory):
    pass


# =============================================================================
# Gold Catchup DAG
# Catches full_ silver files that were never loaded to CH staging
# (e.g. OOM killed the silver task after write but before gold signal fired)
# =============================================================================

# with DAG(
#     dag_id="gold_staging_catchup",
#     schedule="*/30 * * * *",
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['gold', 'catchup', 'recovery'],
#     default_args=default_args,
# ) as dag:

#     @task
#     def catchup_unloaded_silver():
#         loader = ClickHouseLoader()

#         # Map silver subpaths to their CH staging tables
#         silver_to_staging = {
#             CFG.station_cleaned_st: CFG.station_staging_st,
#             CFG.station_cleaned_pm: CFG.station_staging_pm,
#             CFG.station_cleaned_se: CFG.station_staging_se,
#         }

#         total_loaded = 0
#         results = {}

#         for silver_subpath, staging_table in silver_to_staging.items():
#             unloaded = loader.find_unloaded_silver_keys(
#                 silver_subpath=silver_subpath,
#                 lookback_hours=24,
#             )

#             if unloaded:
#                 batch_result = loader.insert_silver_partition_batch(
#                     silver_subpath=silver_subpath,
#                     staging_table=staging_table,
#                     s3_keys=unloaded,
#                 )
#                 total_loaded += batch_result["files"]
#                 results[silver_subpath] = {
#                     "found": len(unloaded),
#                     "loaded": batch_result["files"],
#                     "partitions": batch_result["partitions"],
#                 }
#             else:
#                 results[silver_subpath] = {"found": 0, "loaded": 0}

#         if total_loaded == 0:
#             raise AirflowSkipException("No unloaded silver files found")

#         return results

#     catchup_unloaded_silver()
