"""
Recovery DAGs — manually triggered to reset metadata flags and re-run pipeline stages.

All DAGs are schedule=None (manual only). Each follows:

    list_affected (task)
        → ApprovalOperator  (HITL — shows summary, pauses for human approval)
        → execute_recovery  (task — runs only if approved)

silver_recovery
    Reset loaded_to_silver=False on bronze metadata → silver DAG re-processes on next run.
    Optional mode="delete" also removes the silver parquet files from S3.

staging_recovery
    Reset loaded_to_warehouse=False on silver metadata → staging DAG re-loads on next run.

gold_recovery
    Delete CH gold rows → re-run GoldAggregator for the affected hours/days.
    Uses is_processed() to skip already-complete periods (idempotent on retry).

Params
------
All DAGs accept:
    lookback_hours (int)   — how many hours back to scan (default 168 = 7 days)

silver_recovery also accepts:
    table  (str)           — bronze table name, e.g. "subscriber_traffic"
    mode   (str)           — "unmark" (default) or "delete" (also removes silver parquets)

staging_recovery also accepts:
    silver_subpath (str)   — e.g. "traffic_cleaned"

gold_recovery also accepts:
    skip_daily (bool)      — if True, skip daily report recomputation
"""

from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.hitl import ApprovalOperator

from shared.common.config import CFG
from shared.common.dag_defaults import RECOVERY_DEFAULTS
from shared.common.dag_factory import RecoveryDag
from shared.util.recovery_manager import SilverRecovery, StagingRecovery, GoldRecovery

import shared.ddl as _ddl_pkg

import logging
log = logging.getLogger(__name__)


GOLD_DDL_PATH = Path(_ddl_pkg.__file__).parent / "gold_logic.sql"
recovery = RecoveryDag(tags=['recovery', 'gold'], ddl_path=GOLD_DDL_PATH)

# =============================================================================
# Silver Recovery
# =============================================================================

users = None

with DAG(
    dag_id="silver_recovery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "silver"],
    default_args=RECOVERY_DEFAULTS,
    params={
        "lookback_hours": 168,
        "table": CFG.station_st,
        "mode": "unmark",           # "unmark" | "delete"
    },
) as dag:

    @task(task_id="list_affected")
    def list_affected_silver(**context):
        p = context["params"]
        table = p.get("table", CFG.station_st)
        lookback = int(p.get("lookback_hours", 168))
        summary = SilverRecovery().list_affected(table=table, lookback=lookback)
        log.info(f"Silver recovery scope: {summary}")
        if summary["count"] == 0:
            raise AirflowSkipException(f"No loaded_to_silver entries found for {table}")
        return summary

    approve = ApprovalOperator(
        subject="Silver recovery — approve to reset loaded_to_silver flags (and optionally delete silver parquets).",
        task_id="approve",
        assigned_users=users,
    )

    @task(task_id="execute_recovery")
    def execute_silver_recovery(**context):
        p = context["params"]
        table = p.get("table", CFG.station_st)
        lookback = int(p.get("lookback_hours", 168))
        mode = p.get("mode", "unmark")

        mgr = SilverRecovery()
        result = mgr.unmark_all(table=table, lookback=lookback)

        if mode == "delete":
            delete_result = mgr.delete_all(table=table, lookback=lookback)
            result["deleted"] = delete_result["deleted"]
            result["delete_errors"] = delete_result["errors"]

        log.info(f"Silver recovery complete: {result}")
        return result

    summary = list_affected_silver()
    execute = execute_silver_recovery()
    summary >> approve >> execute


# =============================================================================
# Staging Recovery
# =============================================================================

with DAG(
    dag_id="staging_recovery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "staging"],
    default_args=RECOVERY_DEFAULTS,
    params={
        "lookback_hours": 168,
        "silver_subpath": CFG.station_cleaned_st,
    },
) as dag:

    @task(task_id="list_affected")
    def list_affected_staging(**context):
        p = context["params"]
        subpath = p.get("silver_subpath", CFG.station_cleaned_st)
        lookback = int(p.get("lookback_hours", 168))
        summary = StagingRecovery().list_affected(silver_subpath=subpath, lookback=lookback)
        log.info(f"Staging recovery scope: {summary}")
        if summary["count"] == 0:
            raise AirflowSkipException(f"No loaded_to_warehouse entries found for {subpath}")
        return summary

    approve = ApprovalOperator(
        subject="Staging recovery — approve to reset loaded_to_warehouse flags. Staging DAG will re-load on its next run.",
        task_id="approve",
        assigned_users=users,
    )

    @task(task_id="execute_recovery")
    def execute_staging_recovery(**context):
        p = context["params"]
        subpath = p.get("silver_subpath", CFG.station_cleaned_st)
        lookback = int(p.get("lookback_hours", 168))

        result = StagingRecovery().unmark_all(silver_subpath=subpath, lookback=lookback)
        log.info(f"Staging recovery complete: {result}")
        return result

    summary = list_affected_staging()
    execute = execute_staging_recovery()
    summary >> approve >> execute


# =============================================================================
# Gold Recovery
# =============================================================================

with DAG(
    dag_id="gold_recovery",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "gold"],
    default_args=RECOVERY_DEFAULTS,
    params={
        "lookback_hours": 168,
        "skip_daily": False,
    },
) as dag:

    @task(task_id="discover_periods")
    def discover_periods(**context):
        p = context["params"]
        lookback = int(p.get("lookback_hours", 168))
        periods = GoldRecovery.build_periods(lookback)
        summary = {
            "hours": len(periods["hours"]),
            "days": len(periods["days"]),
            "range": periods["range"],
            # Pass the actual lists for downstream tasks
            "_hours": periods["hours"],
            "_days": periods["days"],
        }
        log.info(f"Gold recovery scope: {summary['hours']} hours, {summary['days']} days — {summary['range']}")
        return summary

    approve = ApprovalOperator(
        task_id="approve",
        subject="Gold recovery — approve to DELETE CH gold data and recompute from staging. This is destructive.",
        assigned_users=users,
    )

    @task(task_id="delete_gold")
    def delete_gold(periods):
        hours = [tuple(h) for h in periods["_hours"]]
        days  = [tuple(d) for d in periods["_days"]]
        result = GoldRecovery().delete_all(hours=hours, days=days)
        log.info(f"Gold delete complete: {result}")
        return result

    @task(task_id="recompute_gold")
    def recompute_gold(periods, **context):
        skip_daily = bool(context["params"].get("skip_daily", False))
        hours = [tuple(h) for h in periods["_hours"]]
        days  = [tuple(d) for d in periods["_days"]]
        result = GoldRecovery().recompute_all(hours=hours, days=days, skip_daily=skip_daily)
        log.info(f"Gold recompute complete: {result}")
        return result

    periods = discover_periods()
    deleted = delete_gold(periods)
    recomputed = recompute_gold(periods)
    periods >> approve >> deleted >> recomputed


with recovery.create_dag(
    dag_id="clickhouse_recovery",
    schedule=None,
    default_args=RECOVERY_DEFAULTS,
) as clickhouse_recovery:
    drop_tables = recovery.drop_tables()()
    ensure_db = recovery.ensure_db()()
    ensure_tables = recovery.ensure_tables()()
    drop_tables >> ensure_db >> ensure_tables