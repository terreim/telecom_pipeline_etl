import pandas as pd
from datetime import date, timedelta

import logging

from shared.common.config import CFG
from shared.common.s3 import S3IO
from shared.common.ch import ClickHouseIO
from shared.common.metadata import MetadataManager
from shared.common.metadata_template import gold_metadata_template
from shared.common.failure_recovery import write_failure_metadata
from shared.common.watermark import S3WatermarkStore
from shared.common.validators import check_gold_health_hourly, check_gold_sla_compliance, check_gold_anomaly_features
from shared.common.connections import get_s3_hook, get_s3_credentials, get_clickhouse_hook
from shared.common.sql_builder import (
    sql_gold_health_hourly, 
    sql_gold_slac_hourly, 
    sql_gold_anomaly_features, 
    sql_gold_outage_report, 
    sql_gold_region_report, 
    sql_gold_maintenance_report,
    sql_gold_handover_report, 
    sql_gold_alarm_report
)

logger = logging.getLogger(__name__)

class GoldAggregator:
    def __init__(
        self, 
        s3_conn_id: str = CFG.s3_conn_id, 
        ch_conn_id: str = CFG.clickhouse_conn_id,
        s3_bucket: str = CFG.s3_bucket
    ):
        self.ch_hook = get_clickhouse_hook(conn_id=ch_conn_id)
        self.s3_io = S3IO(s3_conn_id, s3_bucket)
        self.ch_io = ClickHouseIO(s3_conn_id, ch_conn_id)
        self.bucket = s3_bucket

        self.s3_access_key, self.s3_secret_key, self.s3_endpoint = get_s3_credentials(conn_id=s3_conn_id)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _generic_write(
            self, 
            prefix: str,
            partition_date: str,
            filename: str,
            df: pd.DataFrame,
            ch_table: str
        ):
            s3_key = self.s3_io.create_key(
                prefix=prefix,
                partition_date=partition_date, 
                filename=filename, 
                type="medium_key")
            
            self.s3_io.write_parquet(df, s3_key)

            s3_url = self.s3_io.create_key(
                prefix=prefix,
                partition_date=partition_date,
                filename=filename,
                type="url"
            )
            
            try:
                self.ch_io.insert_s3(url=s3_url, ch_table=ch_table)
            except Exception as ex1:
                logger.error(f"Error inserting health scores into ClickHouse through S3: {ex1}")
                try:
                    self.ch_io.insert_ch(
                        df=df, 
                        ch_table=ch_table, 
                        columns=df.columns.tolist()
                    )
                except Exception as ex2:
                    logger.error(f"Error inserting health scores into ClickHouse via VALUES: {ex2}")
                    raise ex2

    # =========================================================================
    # Health scoring
    # =========================================================================

    def _compute_health_score(self, row) -> float:
        """
        Composite score: 100 = perfect health, 0 = total failure.
        
        Components (weighted):
        - Latency score (25%):  100 if avg < 30ms, degrades linearly to 0 at 500ms
        - Loss score (20%):     100 if avg < 0.5%, degrades to 0 at 10%
        - CPU score (15%):      100 if avg < 50%, degrades to 0 at 100%
        - Temperature (10%):    100 if avg < 45°C, degrades to 0 at 80°C
        - Error score (15%):    100 if 0 alarms, loses 10 per warning, 30 per critical
        - Throughput (15%):     100 if at expected level for tech, degrades proportionally
        
        Overrides:
        - Incident active (failure): cap at 15
        - Incident active (degradation): cap at 50
        - Maintenance active: set to NULL (excluded from scoring)

        This helper is to be called for each station-hour record, and can be used to compute both hourly and daily health scores.
        """

        if row.get('maintenance_active'):
            return None, 'maintenance'
        
        latency_score = max(0, 100 - (row['avg_latency_ms'] - 30) * (100 / 470))
        loss_score = max(0, 100 - row['avg_packet_loss_pct'] * (100 / 10))
        cpu_score = max(0, 100 - max(0, row['avg_cpu_pct'] - 50) * 2)
        temp_score = max(0, 100 - max(0, row['avg_temperature_c'] - 45) * (100 / 35))
        
        alarm_penalty = row.get('warning_count', 0) * 10 + row.get('critical_count', 0) * 30
        error_score = max(0, 100 - alarm_penalty)
        
        tech_expected = {'2G': 0.01, '3G': 0.1, '4G': 1.0, '5G': 5.0}
        expected = tech_expected.get(row['technology'], 1.0) * 1000  # Mbps
        throughput_ratio = min(1.0, row['avg_throughput_mbps'] / max(0.001, expected * 0.3))
        throughput_score = throughput_ratio * 100

        score = (
            latency_score * 0.25 +
            loss_score * 0.20 +
            cpu_score * 0.15 +
            temp_score * 0.10 +
            error_score * 0.15 +
            throughput_score * 0.15
        )

        # Incident overrides
        if row.get('incident_active'):
            if row.get('incident_type') == 'failure':
                score = min(score, 15)
            elif row.get('incident_type') == 'degradation':
                score = min(score, 50)

        category = (
            'critical' if score < 30 else
            'degraded' if score < 60 else
            'warning' if score < 80 else
            'healthy'
        )
        return round(score, 2), category
        
    def gold_health_hourly(self, year: int, month: int, day: int, hour: int) -> dict:
        """
        Health Scoring joins traffic (latency, packet loss) + metrics (CPU, temperature) + events (alarms, incidents) 
        for the same station-hour and produces a weighted composite score.
        """

        hour_start = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:00:00"
        hour_end   = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:59:59"
        columns = [
            'station_id', 'station_code', 'hour_start', 'operator_code', 'province', 'region', 'density_class', 'technology',
            'session_count', 'unique_subscribers', 'total_bytes', 'avg_latency_ms', 'p95_latency_ms', 'avg_packet_loss_pct', 'high_latency_ratio',
            'avg_cpu_pct', 'max_cpu_pct', 'avg_memory_pct', 'avg_temperature_c', 'max_temperature_c', 'avg_throughput_mbps', 'error_count',
            'alarm_count', 'warning_count', 'critical_count', 'handover_count',
            'incident_active', 'incident_type', 'maintenance_active'
        ]
        
        try:
            result = self.ch_io.execute_query(sql_gold_health_hourly(year, month, day, hour))
            if not result:
                logger.info(f"No data found for {hour_start} to {hour_end}")
                return None
        
            df = pd.DataFrame(result, columns=columns)
            # Compute health score for each station-hour
            df['health_score'], df['health_category'] = zip(*df.apply(self._compute_health_score, axis=1))
            logger.warning(f"{[warning for warning in check_gold_health_hourly(df) if warning]}")

            self._generic_write(
                prefix="gold/health",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}",
                filename="health_score.parquet",
                df=df,
                ch_table=CFG.station_health
            )
            logger.info(f"Hourly health report written for {year:04d}-{month:02d}-{day:02d}-{hour:02d}")
        except Exception as ex:
            logger.error(f"Error inserting health scores into ClickHouse for {hour_start} to {hour_end}: {ex}")
            raise ex
     
    # =========================================================================
    # SLA Compliance
    # =========================================================================

    def gold_sla_compliance(self, year: int, month: int, day: int) -> dict:
        columns = [
            'station_id', 'station_code', 'report_date', 'operator_code', 'province', 'region', 'density_class', 'technology',
            'total_hours', 'active_hours', 'down_hours', 'maintenance_hours', 'degraded_hours',
            'billable_hours', 'available_hours', 'uptime_pct', 'sla_target_pct', 'sla_met', 'sla_breach_hours',
            'avg_health_score', 'min_health_score', 'hours_below_60', 'hours_below_30',
            'incident_count', 'total_incident_min', 'longest_incident_min', 'mttr_min',
            'compliance_status'
        ]
        try:
            result = self.ch_io.execute_query(sql_gold_slac_hourly(year, month, day))
            if not result:
                logger.info(f"No data found for SLA compliance on {year:04d}-{month:02d}-{day:02d}")
                return None
            
            df = pd.DataFrame(result, columns=columns)
            logger.warning(f"{[warning for warning in check_gold_sla_compliance(df) if warning]}")

            self._generic_write(
                prefix="gold/sa_compliance",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}",
                filename="sa_compliance_report.parquet",
                df=df,
                ch_table=CFG.station_slac
            )
            logger.info(f"SLA compliance report written for {year:04d}-{month:02d}-{day:02d}")
        except Exception as ex:
            logger.error(f"Error computing SLA compliance for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex

    # =========================================================================
    # Anomaly Features
    # =========================================================================

    def _compute_anomaly_features(self, row):
        features = {}
        features['z_latency'] = (row['current_latency_ms'] - row['baseline_latency_mean']) / max(row['baseline_latency_std'], 1e-6)
        features['z_cpu'] = (row['current_cpu_pct'] - row['baseline_cpu_mean']) / max(row['baseline_cpu_std'], 1e-6)
        features['z_throughput'] = (row['current_throughput'] - row['baseline_throughput_mean']) / max(row['baseline_throughput_std'], 1e-6)
        features['z_subscribers'] = (row['current_subscribers'] - row['baseline_subs_mean']) / max(row['baseline_subs_std'], 1e-6)
        
        features['is_anomalous'] = int(
            abs(features['z_latency']) > 3 or
            abs(features['z_cpu']) > 3 or
            features['z_throughput'] < -3 or
            abs(features['z_subscribers']) > 3
        )
        features['anomaly_type'] = None
        if features['is_anomalous']:
            anomaly_types = []
            if abs(features['z_latency']) > 3:
                anomaly_types.append("latency")
            if abs(features['z_cpu']) > 3:
                anomaly_types.append("cpu")
            if features['z_throughput'] < -3:
                anomaly_types.append("throughput")
            if abs(features['z_subscribers']) > 3:
                anomaly_types.append("subscribers")
            features['anomaly_type'] = ",".join(anomaly_types)

        features['neighbor_anomaly_count'] = 0  # No lat/long in dim_station.
        return pd.Series(features)

    def gold_anomaly_features(self, year: int, month: int, day: int, hour: int) -> dict:
        columns = [
            'station_id', 'station_code', 'hour_start',
            'current_latency_ms', 'current_packet_loss', 'current_cpu_pct', 'current_throughput', 'current_subscribers',
            'baseline_latency_mean', 'baseline_latency_std',
            'baseline_cpu_mean', 'baseline_cpu_std',
            'baseline_throughput_mean', 'baseline_throughput_std',
            'baseline_subs_mean', 'baseline_subs_std'
        ]

        try:
            result = self.ch_io.execute_query(sql_gold_anomaly_features(year, month, day, hour))
            if not result:
                logger.info(f"No data found for anomaly feature computation on {year:04d}-{month:02d}-{day:02d} {hour:02d}:00")
                return None
            
            df = pd.DataFrame(result, columns=columns)
            # Fill NaN baselines (new stations with no history) with current values to avoid false anomalies
            BASELINE_TO_CURRENT = {
                'baseline_latency_mean': 'current_latency_ms',
                'baseline_cpu_mean': 'current_cpu_pct',
                'baseline_throughput_mean': 'current_throughput',
                'baseline_subs_mean': 'current_subscribers',
            }
            for baseline_col, current_col in BASELINE_TO_CURRENT.items():
                df[baseline_col] = df[baseline_col].fillna(df[current_col])
            
            anomaly_df = df.apply(self._compute_anomaly_features, axis=1)
            result_df = pd.concat([df, anomaly_df], axis=1)

            logger.warning(f"{[warning for warning in check_gold_anomaly_features(result_df) if warning]}")

            self._generic_write(
                prefix="gold/anomaly_features",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}",
                filename="anomaly_features_report.parquet",
                df=result_df,
                ch_table=CFG.station_anomaly
            )
            logger.info(f"Anomaly features report written for {year:04d}-{month:02d}-{day:02d}-{hour:02d}")
        except Exception as ex:
            logger.error(f"Error computing anomaly features for {year:04d}-{month:02d}-{day:02d} {hour:02d}:00: {ex}")
            raise ex

    # =========================================================================
    # Outage Report
    # =========================================================================

    def gold_outage_report(self, year: int, month: int, day: int) -> dict:
        columns = [
            'station_id', 'station_code', 'operator_code', 'province', 'region', 'technology',
            'incident_type', 'severity', 'incident_start', 'incident_end', 'duration_min', 'estimated_duration_min',
            'affected_subscribers', 'traffic_loss_bytes', 'hardware_quality', 'health_score_during'
        ]

        try:
            result = self.ch_io.execute_query(sql_gold_outage_report(year, month, day))
            if not result:
                logger.info(f"No data found for outage report on {year:04d}-{month:02d}-{day:02d}")
                return None
            
            df = pd.DataFrame(result, columns=columns)

            self._generic_write(
                prefix="gold/outage_report",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}",
                filename="outage_report.parquet",
                df=df,
                ch_table=CFG.station_outage
            )
            logger.info(f"Outage report written for {year:04d}-{month:02d}-{day:02d}")
        except Exception as ex:
            logger.error(f"Error computing outage report for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex
    
    # =========================================================================
    # Region Daily Report
    # =========================================================================

    def gold_region_daily(self, year: int, month: int, day: int) -> dict:
        columns = [
            'report_date', 'region', 'province', 'operator_code', 'technology',
            'station_count', 'active_station_count', 'total_subscribers', 'total_sessions', 'total_bytes',
            'avg_health_score', 'min_health_score', 'stations_critical', 'stations_degraded',
            'avg_latency_ms', 'p95_latency_ms', 'avg_packet_loss_pct', 'incident_count',
            'sla_breach_count', 'sla_compliance_pct'
        ]

        try:
            result = self.ch_io.execute_query(sql_gold_region_report(year, month, day))

            if not result:
                logger.info(f"No data found for region daily report on {year:04d}-{month:02d}-{day:02d}")
                return None

            df = pd.DataFrame(result, columns=columns)

            self._generic_write(
                prefix="gold/region_daily",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}",
                filename="region_daily.parquet",
                df=df,
                ch_table=CFG.station_region
            )
            logger.info(f"Region report written for {year:04d}-{month:02d}-{day:02d}")
        except Exception as ex:
            logger.error(f"Error computing region daily report for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex

    
    # =========================================================================
    # Maintenance Report
    # =========================================================================

    def gold_maintenance_report(self, year: int, month: int, day: int) -> dict:
        columns = [
            'station_id', 'station_code', 'operator_code', 'region', 'technology',
            'maintenance_start', 'maintenance_end',
            'planned_duration_min', 'actual_duration_min', 'overrun_min',
            'tech_upgrade', 'sla_excluded',
            'pre_maintenance_health', 'post_maintenance_health'
        ]

        try:
            result = self.ch_io.execute_query(sql_gold_maintenance_report(year, month, day))
            if not result:
                logger.info(f"No data found for maintenance report on {year:04d}-{month:02d}-{day:02d}")
                return None

            df = pd.DataFrame(result, columns=columns)

            self._generic_write(
                prefix="gold/maintenance_report",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}",
                filename="maintenance_report.parquet",
                df=df,
                ch_table=CFG.station_maintenance
            )
            logger.info(f"Maintenance report written for {year:04d}-{month:02d}-{day:02d}")
        except Exception as ex:
            logger.error(f"Error computing maintenance report for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex
    
    # =========================================================================
    # Handover Report
    # =========================================================================

    def gold_handover_report(self, year: int, month: int, day: int) -> dict:
        columns=[
            'report_date', 'source_station_id', 'source_station_code',
            'target_station_id', 'target_station_code',
            'source_region', 'target_region',
            'handover_count', 'unique_subscribers',
            'avg_latency_before_ms', 'avg_latency_after_ms'
        ]
        try:
            result = self.ch_io.execute_query(sql_gold_handover_report(year, month, day))

            if not result:
                logger.info(f"No data found for handover report on {year:04d}-{month:02d}-{day:02d}")
                return None

            df = pd.DataFrame(result, columns=columns)

            self._generic_write(
                prefix="gold/handover_daily",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}",
                filename="handover_daily.parquet",
                df=df,
                ch_table=CFG.station_handover
            )
            logger.info(f"Handover report written for {year:04d}-{month:02d}-{day:02d}")
        except Exception as ex:
            logger.error(f"Error computing handover report for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex

    
    # =========================================================================
    # Alarm Daily Report
    # =========================================================================

    def gold_alarm_report(self, year: int, month: int, day: int) -> dict:
        columns = [
            'report_date', 'station_id', 'station_code', 'operator_code', 'region', 'technology',
            'warning_count', 'error_count', 'critical_count', 'total_alarm_count',
            'alarm_rate_per_hour', 'top_alarm_description', 'health_score_avg'
        ]
        try:
            result = self.ch_io.execute_query(sql_gold_alarm_report(year, month, day))

            if not result:
                logger.info(f"No data found for alarm daily report on {year:04d}-{month:02d}-{day:02d}")
                return None

            df = pd.DataFrame(result, columns=columns)

            self._generic_write(
                prefix="gold/alarm_daily",
                partition_date=f"year={year:04d}/month={month:02d}/day={day:02d}",
                filename="alarm_daily.parquet",
                df=df,
                ch_table=CFG.station_alarm
            )
            logger.info(f"Alarm daily report written for {year:04d}-{month:02d}-{day:02d}")
        except Exception as ex:
            logger.error(f"Error computing alarm daily report for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex