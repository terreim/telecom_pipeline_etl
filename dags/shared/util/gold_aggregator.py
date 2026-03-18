import time
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import logging

from shared.common.config import CFG
from shared.common.s3 import S3IO
from shared.common.ch import ClickHouseIO
from shared.common.metadata import MetadataManager
from shared.common.metadata_template import gold_metadata_template
from shared.common.validators import check_gold_health_hourly, check_gold_sla_compliance, check_gold_anomaly_features
from shared.common.connections import get_s3_credentials
from shared.common.sql_builder import (
    sql_gold_health_hourly,
    sql_gold_slac_hourly,
    sql_gold_anomaly_features,
    sql_gold_outage_report,
    sql_gold_region_report,
    sql_gold_maintenance_report,
    sql_gold_handover_report,
    sql_gold_alarm_report,
)

logger = logging.getLogger(__name__)

@dataclass
class GoldReport:
    name: str
    sql_fn: Callable
    columns: list[str]
    s3_prefix: str
    filename: str
    ch_table: str
    granularity: str
    compute_fn: Callable | None = None
    validate_fn: Callable | None = None

class GoldAggregator:
    def __init__(
        self,
        s3_conn_id: str = CFG.s3_conn_id,
        ch_conn_id: str = CFG.clickhouse_conn_id,
        s3_bucket: str = CFG.s3_bucket,
    ):
        self.ch_io = ClickHouseIO(s3_conn_id, ch_conn_id)
        self.s3_io = S3IO(s3_conn_id, s3_bucket)
        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.bucket = s3_bucket
        self.layer = CFG.gold_prefix
        self.s3_access_key, self.s3_secret_key, self.s3_endpoint = get_s3_credentials(conn_id=s3_conn_id)

    # =========================================================================
    # Core dispatch
    # =========================================================================

    def aggregate(
        self,
        report: GoldReport,
        year: int,
        month: int,
        day: int,
        hour: int | None = None,
        batch_id: str | None = None,
    ) -> dict:
        """Run a single GoldReport for the given partition and write metadata."""
        t0 = time.monotonic()

        if report.granularity == "hourly":
            if hour is None:
                raise ValueError(f"Report '{report.name}' requires hour parameter")
            partition = f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}"
            sql_args = (year, month, day, hour)
        else:
            partition = f"year={year:04d}/month={month:02d}/day={day:02d}"
            sql_args = (year, month, day)

        batch_id = batch_id or f"{report.name}__{partition.replace('/', '-').replace("=", "")}"
        meta = gold_metadata_template()
        meta |= {
            "report": report.name,
            "partition": partition,
            "batch_id": batch_id,
            "table": report.ch_table,
        }

        try:
            result = self.ch_io.execute_query(report.sql_fn(*sql_args))

            if not result:
                logger.info(f"[{report.name}] No data for {partition}")
                meta |= {"status": "skipped", "record_count": 0}
                meta["created_at"] = datetime.now(timezone.utc).isoformat()
                meta["processing_duration_seconds"] = round(time.monotonic() - t0, 2)
                self._write_metadata(report.name, partition, batch_id, meta)
                return meta

            df = pd.DataFrame(result, columns=report.columns)

            if report.compute_fn:
                df = report.compute_fn(df)

            if report.validate_fn:
                warnings = [w for w in report.validate_fn(df) if w]
                if warnings:
                    logger.warning(f"[{report.name}] QA warnings: {warnings}")

            s3_key = self.s3_io.create_key(
                prefix=f"{self.layer}/{report.s3_prefix}",
                partition_date=partition,
                filename=report.filename,
                type="medium_key",
            )
            self.s3_io.write_parquet(df, s3_key)

            s3_url = self.s3_io.create_key(
                prefix=f"{self.layer}/{report.s3_prefix}",
                partition_date=partition,
                filename=report.filename,
                type="url",
            )
            self._insert_to_ch(df, s3_url, report.ch_table)

            meta |= {
                "status": "complete",
                "record_count": len(df),
                "data_key": s3_key,
            }
            logger.info(f"[{report.name}] {len(df)} rows → s3://{self.bucket}/{s3_key}")

        except Exception as e:
            logger.error(f"[{report.name}] Failed for {partition}: {e}")
            meta |= {"status": "failed"}
            raise
        finally:
            meta["created_at"] = datetime.now(timezone.utc).isoformat()
            meta["processing_duration_seconds"] = round(time.monotonic() - t0, 2)
            self._write_metadata(report.name, partition, batch_id, meta)

        return meta

    # =========================================================================
    # Helpers
    # =========================================================================

    def _insert_to_ch(self, df: pd.DataFrame, s3_url: str, ch_table: str):
        self.ch_io.insert_with_fallback(df=df, s3_url=s3_url, ch_table=ch_table)

    def _write_metadata(self, report_name: str, partition: str, batch_id: str, meta: dict):
        key = f"metadata/watermark/{self.layer}/{report_name}/{partition}/{batch_id}.json"
        self.meta.write_metadata(key=key, metadata_dict=meta)

    # =========================================================================
    # Recovery helpers
    # =========================================================================

    def _partition_hour(self, year: int, month: int, day: int, hour: int) -> str:
        return f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}"

    def _partition_day(self, year: int, month: int, day: int) -> str:
        return f"year={year:04d}/month={month:02d}/day={day:02d}"

    def is_processed(self, report_name: str, partition: str) -> bool:
        """True if the latest metadata for this report+partition has status='complete'."""
        keys = self.meta.s3_hook.list_keys(
            bucket_name=self.bucket,
            prefix=f"metadata/{self.layer}/{report_name}/{partition}/",
        ) or []
        json_keys = [k for k in keys if k.endswith(".json")]
        if not json_keys:
            return False
        latest = self.meta.read_metadata(sorted(json_keys)[-1])
        return bool(latest and latest.get("status") == "complete")

    def delete_hour(self, year: int, month: int, day: int, hour: int) -> None:
        """Delete all hourly gold CH data for this (y, m, d, h)."""
        self.ch_io.delete_gold_hour(year, month, day, hour)

    def delete_day(self, year: int, month: int, day: int) -> None:
        """Delete all daily gold CH data for this (y, m, d)."""
        self.ch_io.delete_gold_day(year, month, day)

    # =========================================================================
    # Health scoring
    # =========================================================================

    def _compute_health_score(self, df: pd.DataFrame) -> pd.DataFrame:
        def _score_row(row):
            if row.get('maintenance_active'):
                return None, 'maintenance'

            latency_score = max(0, 100 - (row['avg_latency_ms'] - 30) * (100 / 470))
            loss_score = max(0, 100 - row['avg_packet_loss_pct'] * (100 / 10))
            cpu_score = max(0, 100 - max(0, row['avg_cpu_pct'] - 50) * 2)
            temp_score = max(0, 100 - max(0, row['avg_temperature_celsius'] - 45) * (100 / 35))

            alarm_penalty = row.get('warning_count', 0) * 10 + row.get('critical_count', 0) * 30
            error_score = max(0, 100 - alarm_penalty)

            tech_expected = {'2G': 0.01, '3G': 0.1, '4G': 1.0, '5G': 5.0}
            expected = tech_expected.get(row['technology'], 1.0) * 1000
            throughput_score = min(1.0, row['avg_downlink_throughput_mbps'] / max(0.001, expected * 0.3)) * 100

            score = (
                latency_score * 0.25 +
                loss_score    * 0.20 +
                cpu_score     * 0.15 +
                temp_score    * 0.10 +
                error_score   * 0.15 +
                throughput_score * 0.15
            )

            if row.get('incident_active'):
                if row.get('incident_type') == 'failure':
                    score = min(score, 15)
                elif row.get('incident_type') == 'degradation':
                    score = min(score, 50)

            category = (
                'critical'  if score < 30 else
                'degraded'  if score < 60 else
                'warning'   if score < 80 else
                'healthy'
            )
            return round(score, 2), category

        df['health_score'], df['health_category'] = zip(*df.apply(_score_row, axis=1))
        return df

    # =========================================================================
    # Anomaly features
    # =========================================================================

    def _compute_anomaly_features(self, df: pd.DataFrame) -> pd.DataFrame:
        BASELINE_TO_CURRENT = {
            'baseline_latency_mean':    'current_latency_ms',
            'baseline_cpu_mean':        'current_cpu_pct',
            'baseline_throughput_mean': 'current_throughput',
            'baseline_subs_mean':       'current_subscribers',
        }
        for baseline_col, current_col in BASELINE_TO_CURRENT.items():
            df[baseline_col] = df[baseline_col].fillna(df[current_col])

        std_cols = ['baseline_latency_std', 'baseline_cpu_std', 'baseline_throughput_std', 'baseline_subs_std']
        for col in std_cols:
            df[col] = df[col].fillna(0.0)

        def _features(row):
            z_lat  = (row['current_latency_ms']  - row['baseline_latency_mean'])    / max(row['baseline_latency_std'],    1e-6)
            z_cpu  = (row['current_cpu_pct']      - row['baseline_cpu_mean'])        / max(row['baseline_cpu_std'],        1e-6)
            z_tp   = (row['current_throughput']   - row['baseline_throughput_mean']) / max(row['baseline_throughput_std'], 1e-6)
            z_subs = (row['current_subscribers']  - row['baseline_subs_mean'])       / max(row['baseline_subs_std'],       1e-6)

            is_anomalous = int(abs(z_lat) > 3 or abs(z_cpu) > 3 or z_tp < -3 or abs(z_subs) > 3)
            anomaly_types = []
            if abs(z_lat)  > 3: anomaly_types.append("latency")
            if abs(z_cpu)  > 3: anomaly_types.append("cpu")
            if z_tp        < -3: anomaly_types.append("throughput")
            if abs(z_subs) > 3: anomaly_types.append("subscribers")

            return pd.Series({
                'z_latency': z_lat, 'z_cpu': z_cpu,
                'z_throughput': z_tp, 'z_subscribers': z_subs,
                'is_anomalous': is_anomalous,
                'anomaly_type': ",".join(anomaly_types) if anomaly_types else None,
                'neighbor_anomaly_count': 0,  # No lat/long in dim_station
            })

        return pd.concat([df, df.apply(_features, axis=1)], axis=1)

    # =========================================================================
    # Report catalogue
    # =========================================================================

    def reports(self) -> dict[str, GoldReport]:
        """Return all registered GoldReports keyed by name."""
        return {r.name: r for r in [
            GoldReport(
                name="health_hourly",
                sql_fn=sql_gold_health_hourly,
                columns=[
                    'station_id', 'station_code', 'hour_start', 'operator_code', 'province', 'region',
                    'density_class', 'technology', 'session_count', 'unique_subscribers', 'total_bytes',
                    'avg_latency_ms', 'p95_latency_ms', 'avg_packet_loss_pct', 'high_latency_ratio',
                    'avg_cpu_pct', 'max_cpu_pct', 'avg_memory_pct', 'avg_temperature_celsius', 'max_temperature_celsius',
                    'avg_uplink_throughput_mbps', 'avg_downlink_throughput_mbps', 'alarm_count', 'warning_count', 'critical_count',
                    'handover_count', 'incident_active', 'incident_type', 'maintenance_active',
                ],
                s3_prefix="health",
                filename="health_score.parquet",
                ch_table=CFG.station_health,
                granularity="hourly",
                compute_fn=self._compute_health_score,
                validate_fn=check_gold_health_hourly,
            ),
            GoldReport(
                name="sla_compliance",
                sql_fn=sql_gold_slac_hourly,
                columns=[
                    'station_id', 'station_code', 'report_date', 'operator_code', 'province', 'region',
                    'density_class', 'technology', 'total_hours', 'active_hours', 'down_hours',
                    'maintenance_hours', 'degraded_hours', 'billable_hours', 'available_hours',
                    'uptime_pct', 'sla_target_pct', 'sla_met', 'sla_breach_hours', 'avg_health_score',
                    'min_health_score', 'hours_below_60', 'hours_below_30', 'incident_count',
                    'total_incident_min', 'longest_incident_min', 'mttr_min', 'compliance_status',
                ],
                s3_prefix="sla_compliance",
                filename="sla_compliance_report.parquet",
                ch_table=CFG.station_slac,
                granularity="daily",
                validate_fn=check_gold_sla_compliance,
            ),
            GoldReport(
                name="anomaly_features",
                sql_fn=sql_gold_anomaly_features,
                columns=[
                    'station_id', 'station_code', 'hour_start',
                    'current_latency_ms', 'current_packet_loss', 'current_cpu_pct',
                    'current_throughput', 'current_subscribers',
                    'baseline_latency_mean', 'baseline_latency_std',
                    'baseline_cpu_mean', 'baseline_cpu_std',
                    'baseline_throughput_mean', 'baseline_throughput_std',
                    'baseline_subs_mean', 'baseline_subs_std',
                ],
                s3_prefix="anomaly_features",
                filename="anomaly_features_report.parquet",
                ch_table=CFG.station_anomaly,
                granularity="hourly",
                compute_fn=self._compute_anomaly_features,
                validate_fn=check_gold_anomaly_features,
            ),
            GoldReport(
                name="outage_report",
                sql_fn=sql_gold_outage_report,
                columns=[
                    'station_id', 'station_code', 'operator_code', 'province', 'region', 'technology',
                    'incident_type', 'severity', 'incident_start', 'incident_end', 'duration_min',
                    'estimated_duration_min', 'affected_subscribers', 'traffic_loss_bytes',
                    'hardware_quality', 'health_score_during',
                ],
                s3_prefix="outage_report",
                filename="outage_report.parquet",
                ch_table=CFG.station_outage,
                granularity="daily",
            ),
            GoldReport(
                name="region_daily",
                sql_fn=sql_gold_region_report,
                columns=[
                    'report_date', 'region', 'province', 'operator_code', 'technology',
                    'station_count', 'active_station_count', 'total_subscribers', 'total_sessions',
                    'total_bytes', 'avg_health_score', 'min_health_score', 'stations_critical',
                    'stations_degraded', 'avg_latency_ms', 'p95_latency_ms', 'avg_packet_loss_pct',
                    'incident_count', 'sla_breach_count', 'sla_compliance_pct',
                ],
                s3_prefix="region_daily",
                filename="region_daily.parquet",
                ch_table=CFG.station_region,
                granularity="daily",
            ),
            GoldReport(
                name="maintenance_report",
                sql_fn=sql_gold_maintenance_report,
                columns=[
                    'station_id', 'station_code', 'operator_code', 'region', 'technology',
                    'maintenance_start', 'maintenance_end', 'planned_duration_min',
                    'actual_duration_min', 'overrun_min', 'tech_upgrade', 'sla_excluded',
                    'pre_maintenance_health', 'post_maintenance_health',
                ],
                s3_prefix="maintenance_report",
                filename="maintenance_report.parquet",
                ch_table=CFG.station_maintenance,
                granularity="daily",
            ),
            GoldReport(
                name="handover_daily",
                sql_fn=sql_gold_handover_report,
                columns=[
                    'report_date', 'source_station_id', 'source_station_code',
                    'target_station_id', 'target_station_code', 'source_region', 'target_region',
                    'handover_count', 'unique_subscribers', 'avg_latency_before_ms', 'avg_latency_after_ms',
                ],
                s3_prefix="handover_daily",
                filename="handover_daily.parquet",
                ch_table=CFG.station_handover,
                granularity="daily",
            ),
            GoldReport(
                name="alarm_daily",
                sql_fn=sql_gold_alarm_report,
                columns=[
                    'report_date', 'station_id', 'station_code', 'operator_code', 'region', 'technology',
                    'warning_count', 'error_count', 'critical_count', 'total_alarm_count',
                    'alarm_rate_per_hour', 'top_alarm_description', 'health_score_avg',
                ],
                s3_prefix="alarm_daily",
                filename="alarm_daily.parquet",
                ch_table=CFG.station_alarm,
                granularity="daily",
            ),
        ]}
