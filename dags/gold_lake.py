from datetime import datetime, timedelta
from pendulum import now as pendulum_now

from airflow.sdk import DAG, task, Metadata

from shared.common.config import CFG
from shared.common.dag_defaults import GOLD_DEFAULTS
from shared.common.dag_factory import GoldDag
from shared.common.assets import build_assets

from shared.util.gold_aggregator import GoldAggregator
from staging import staging_assets

default_args = GOLD_DEFAULTS.copy()
gold_assets = build_assets(cfg=CFG, option="gold_triggers")
gold = GoldDag(tags=['gold', 'telecom'])

# =============================================================================
"""
Gold Logic DAGs
    Signal Dependency Chain:

    gold_traffic_ready ──┐
    gold_metrics_ready ──┤──> health_hourly ──> anomaly_features
    gold_events_ready ───┘        │
                                │ (fires on hour 23 only)
                                ▼
                            health_daily_ready
                        ┌───┬────┼─────┬────────┐
                        ▼   ▼    ▼     ▼        ▼
                        SLA  outage maint alarm  handover
                        │
                        ▼
                    sla_ready
                        │
                        ▼
                    region_daily"""
# =============================================================================

# health_hourly is special: manual backfill support + fires health_daily_ready
with DAG(
    dag_id="gold_health_hourly",
    schedule=[asset for _, asset in staging_assets.items()],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'health', 'hourly'],
    default_args=default_args,
    params={
        "trigger_time": "",   # ISO datetime e.g. "2026-02-25T14:00:00"
        "lookback_hours": 1,  # hours back from trigger_time (manual only)
    },
) as dag:

    @task(outlets=[gold_assets["health_hourly_ready"], gold_assets["health_daily_ready"]])
    def compute_health_hourly(**context):
        aggregator = GoldAggregator()
        report = aggregator.reports()["health_hourly"]

        all_hours = set()
        for events in context['triggering_asset_events'].values():
            for event in events:
                for h in event.extra.get("hours", []):
                    all_hours.add(tuple(h))

        if not all_hours and context['run_id'].startswith('manual__'):
            trigger_str = context['params'].get("trigger_time", "")
            anchor = datetime.fromisoformat(trigger_str) if trigger_str else pendulum_now()
            for i in range(int(context['params'].get("lookback_hours", 1))):
                ts = anchor - timedelta(hours=i)
                all_hours.add((ts.year, ts.month, ts.day, ts.hour))

        results = [
            aggregator.aggregate(report, year, month, day, hour)
            for year, month, day, hour in sorted(all_hours)
        ]

        yield Metadata(asset=gold_assets["health_hourly_ready"], extra={"hours": [list(h) for h in all_hours]})

        is_manual = context['run_id'].startswith('manual__')
        daily_days = {(y, m, d) for y, m, d, h in all_hours if h == 23 or is_manual}
        if daily_days:
            yield Metadata(asset=gold_assets["health_daily_ready"], extra={"days": [list(d) for d in sorted(daily_days)]})

        return results
    
    ensure_db = gold.ensure_db()()
    ensure_tables = gold.ensure_tables()()

    ensure_db >> ensure_tables >> compute_health_hourly()


with DAG(
    dag_id="gold_anomaly_features",
    schedule=[gold_assets["health_hourly_ready"]],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'anomaly', 'hourly'],
    default_args=default_args,
) as dag:
    gold.create_hourly_task("anomaly_features")()


with DAG(
    dag_id="gold_sla_compliance",
    schedule=[gold_assets["health_daily_ready"]],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'sla', 'compliance'],
    default_args=default_args,
) as dag:

    @task(outlets=[gold_assets["sla_ready"]])
    def compute_sla_compliance(**context):
        aggregator = GoldAggregator()
        report = aggregator.reports()["sla_compliance"]

        days = set()
        for events in context['triggering_asset_events'].values():
            for event in events:
                for d in event.extra.get("days", []):
                    days.add(tuple(d))

        results = [aggregator.aggregate(report, y, m, d) for y, m, d in sorted(days)]
        yield Metadata(asset=gold_assets["sla_ready"], extra={"days": [list(d) for d in sorted(days)]})
        return results

    compute_sla_compliance()


with DAG(
    dag_id="gold_region_daily",
    schedule=[gold_assets["sla_ready"]],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'region', 'daily'],
    default_args=default_args,
) as dag:
    gold.create_daily_task("region_daily")()


# Daily reports that all fan out from health_daily_ready
for _report_name, _tags in [
    ("outage_report",       ['gold', 'outage',       'report']),
    ("maintenance_report",  ['gold', 'maintenance',  'report']),
    ("handover_daily",      ['gold', 'handover',     'daily']),
    ("alarm_daily",         ['gold', 'alarm',        'daily']),
]:
    with DAG(
        dag_id=f"gold_{_report_name}",
        schedule=[gold_assets["health_daily_ready"]],
        start_date=datetime(2026, 1, 1),
        catchup=False,

        tags=_tags,
        default_args=default_args,
    ) as dag:
        gold.create_daily_task(_report_name)()