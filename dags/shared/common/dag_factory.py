from datetime import datetime
import re
from typing import Callable

from shared.common.config import CFG
from shared.util.bronze_extractor import BronzeExtractor
from shared.util.silver_transformer import SilverTransformer

from airflow.sdk import DAG, task
from airflow.exceptions import AirflowSkipException

class DagFactory:
    def __init__(
            self, 
            start_date: datetime = datetime(2026, 1, 1), 
            catchup: bool = False, 
            max_active_runs: int = 1, 
            tags: list[str] = None
        ):
        self.start_date = start_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.tags = tags or []

    def create_dag(self, dag_id: str, schedule: str) -> DAG:
        return DAG(
            dag_id=dag_id,
            schedule=schedule,
            start_date=self.start_date,
            catchup=self.catchup,
            max_active_runs=self.max_active_runs,
            tags=self.tags
        )

class BronzeDag(DagFactory):
    def __init__(
            self, 
            start_date: datetime = datetime(2026, 1, 1),
            catchup: bool = False,
            max_active_runs: int = 1,
            tags: list[str] = None
        ):
        super().__init__(start_date, catchup, max_active_runs, tags)
        self.extractor = BronzeExtractor()

    def create_bronze_task(
            self, 
            outlets: list[str],
            table: str,
            pk_column: str,
            time_column: str,
            target_columns: list[str] = None
        ) -> Callable:
        
            @task(outlets=outlets, task_id=f"ingest_{table}")
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

    def create_bronze_signal(
            self,
            table: str,
            outlets: list[str],
            boundary_type: str = 'hourly'
        ) -> Callable:
        
        @task(outlets=outlets, task_id=f"signal_{table}")
        def signal(**context):
            interval_end = context['data_interval_end']

            if re.match(r"manual__", context['run_id']):
                return {f"{table}_manual_run": True}
            
            if boundary_type == 'hourly':
                if interval_end.minute != 0:
                    raise AirflowSkipException(f"Not hour boundary (minute={interval_end.minute})")
                
                return {"hour_completed": interval_end.subtract(minutes=1).hour}
            elif boundary_type == 'daily':
                if interval_end.hour != 0 or interval_end.minute != 0:
                    raise AirflowSkipException(f"Not day boundary (hour={interval_end.hour}, minute={interval_end.minute})")
                
                return {"day_completed": interval_end.subtract(days=1).day}
            
        return signal

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
            outlets: list[str],
            bronze_table: str,
            silver_subpath: str,
            transform_fn: Callable,
            lookback_hours=720
        ) -> Callable:

        @task(outlets=outlets, task_id=f"transform_{bronze_table}")
        def transform(**context):
            pending = self.transformer.find_unprocessed_hours(bronze_table, lookback_hours)
            return [transform_fn(table_name=bronze_table, silver_subpath=silver_subpath,
                                 batch_id=f"{bronze_table[:2]}_{context['run_id']}", lookback_range=lookback_hours)
                    for _, __ in pending]
        
        return transform

class GoldDag(DagFactory):
    pass

class RecoveryDag(DagFactory):
    pass