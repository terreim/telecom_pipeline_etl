from datetime import datetime
from typing import Callable
import logging
import re

from pathlib import Path

from airflow.sdk import Asset, Metadata
from airflow.sdk import DAG, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

from shared.common.config import CFG
from shared.util.bronze_extractor import BronzeExtractor
from shared.util.silver_transformer import SilverTransformer
from shared.util.spark_silver_transformer import SparkSilverTransformer
from shared.util.staging_loader import ClickHouseLoader
from shared.util.gold_aggregator import GoldAggregator

import shared.ddl as _ddl_pkg

STAGING_DDL_PATH = Path(_ddl_pkg.__file__).parent / "staging_silver.sql"
GOLD_DDL_PATH = Path(_ddl_pkg.__file__).parent / "gold_logic.sql"

logger = logging.getLogger(__name__)

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

            interval_end = context.get('data_interval_end')
            if interval_end is None:
                return {"asset_triggered": True}

            if boundary_type == 'hourly':
                if interval_end.minute != 0:
                    raise AirflowSkipException(
                        f"Not hour boundary (minute={interval_end.minute})"
                    )
            elif boundary_type == 'daily':
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

class SparkSilverDag(DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
            postgres_conn_id: str = CFG.postgres_conn_id,
        ):
        super().__init__(start_date, catchup, max_active_runs, tags)
        self._postgres_conn_id = postgres_conn_id

    def create_spark_silver_task(
            self,
            bronze_table: str,
            silver_subpath: str,
            transform_method: str,
            outlets: list[Asset] | None = None,
            lookback_hours: int = 720,
            pool: str = "silver_transform_pool",
        ) -> Callable:

        pg_conn_id = self._postgres_conn_id

        @task(outlets=outlets or [], task_id=f"spark_transform_{bronze_table}", pool=pool)
        def transform(**context):
            transformer = SparkSilverTransformer(postgres_conn_id=pg_conn_id)
            fn = getattr(transformer, transform_method)
            return fn(
                table_name=bronze_table,
                silver_subpath=silver_subpath,
                batch_id=f"{bronze_table[:2]}_{context['run_id']}",
                lookback_range=lookback_hours,
            )

        return transform

class SchemaManager:
    def __init__(self, ddl_path: Path | None = None, **kwargs):
        super().__init__(**kwargs)
        self.ddl_path = ddl_path

    def ensure_db(self) -> Callable:
        @task(task_id="ensure_database")
        def create_database():
            loader = ClickHouseLoader()
            loader.ensure_db()

        return create_database

    def ensure_tables(self) -> Callable:
        @task(task_id="ensure_schema")
        def create_schema():
            loader = ClickHouseLoader()
            loader.ensure_tables(ddl_path=self.ddl_path)

        return create_schema

    def drop_tables(self) -> Callable:
        @task(task_id="drop_tables")
        def drop():
            loader = ClickHouseLoader()
            return loader.drop_tables(ddl_path=self.ddl_path)

        return drop


class StagingSilverDag(SchemaManager, DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
            postgres_conn_id: str = CFG.postgres_conn_id,
            ddl_path: Path | None = STAGING_DDL_PATH,
        ):
        super().__init__(start_date=start_date, catchup=catchup, max_active_runs=max_active_runs, tags=tags, ddl_path=ddl_path)
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
            pool: str = "staging_load",
        ) -> tuple[Callable, Callable, Callable]:
        """Dynamically mapped staging: one Airflow task per silver batch.

        Returns (list_task, load_task, collect_task) — wire them as:
            batches = list_task()
            results = load_task.expand(batch=batches)
            collect_task(results)
        """
        _pattern = re.compile(r'year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)')

        @task(task_id=f"list_{silver_subpath}")
        def list_batches():
            loader = ClickHouseLoader()
            return loader.list_unprocessed(
                silver_subpath=silver_subpath,
                lookback_range=lookback_range,
            )

        @task(task_id=f"staging_{silver_subpath}", pool=pool)
        def load_one(batch: dict, **context):
            loader = ClickHouseLoader()
            return loader.load_staging_single(
                staging_table=staging_table,
                silver_subpath=silver_subpath,
                batch_id=f"staging_{context['run_id']}",
                meta_key=batch["meta_key"],
                silver_meta_file=batch["silver_meta"],
            )

        @task(
            task_id=f"collect_{silver_subpath}",
            outlets=outlets or [],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        def collect_signal(results: list[dict]):
            hours = set()
            for r in results:
                if not r:
                    continue
                sources = []
                partition = r.get("partition")
                if partition:
                    sources.append(partition)
                else:
                    sources.extend(r.get("source_silver_keys", []))
                    sources.extend(r.get("late_source_silver_keys", []))

                for src in sources:
                    m = _pattern.search(src)
                    if m:
                        hours.add(tuple(int(x) for x in m.groups()))

            if outlets and hours:
                yield Metadata(
                    asset=outlets[0],
                    extra={"hours": [list(h) for h in sorted(hours)]},
                )

            return {"hours_signaled": len(hours)}

        return list_batches, load_one, collect_signal


class GoldDag(SchemaManager, DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
            ddl_path: Path | None = GOLD_DDL_PATH,
        ):
        super().__init__(start_date=start_date, catchup=catchup, max_active_runs=max_active_runs, tags=tags, ddl_path=ddl_path)

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

            per_signal = []
            for events in context['triggering_asset_events'].values():
                signal_hours = set()
                for event in events:
                    for h in event.extra.get("hours", []):
                        signal_hours.add(tuple(h))
                if signal_hours:
                    per_signal.append(signal_hours)

            hours = set.intersection(*per_signal) if per_signal else set()

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


class RecoveryDag(SchemaManager, DagFactory):
    def __init__(
            self,
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None,
            ddl_path: Path | None = None,
        ):
        super().__init__(start_date=start_date, catchup=catchup, max_active_runs=max_active_runs, tags=tags, ddl_path=ddl_path)
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
