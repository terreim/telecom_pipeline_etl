import pandas as pd
from datetime import date, timedelta

import logging

from station_dags.config.config import PipelineConfig as C
from station_dags.util.s3_parquet import S3ParquetIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

logger = logging.getLogger(__name__)

class GoldAggregator:
    def __init__(
        self, 
        s3_conn_id: str = C.S3_CONN_ID, 
        ch_conn_id: str = C.CLICKHOUSE_CONN_ID,
        s3_bucket: str = C.S3_BUCKET
    ):
        self.s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        self.ch_hook = ClickHouseHook(clickhouse_conn_id=ch_conn_id)
        self.s3_io = S3ParquetIO(self.s3_hook, s3_bucket)
        self.bucket = s3_bucket

        creds = self.s3_hook.get_credentials()
        self.s3_access_key = creds.access_key
        self.s3_secret_key = creds.secret_key
        
        conn = self.s3_hook.get_connection(s3_conn_id)
        self.s3_endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

    def _create_s3_url(self, key: str) -> str:
        return f"{self.s3_endpoint}/{self.bucket}/{key}"

    # =========================================================================
    # Recovery helpers — gold markers + per-hour/day delete
    # =========================================================================

    MARKER_PREFIX = "gold/_markers"

    def _hourly_marker_key(self, y: int, m: int, d: int, h: int) -> str:
        return (
            f"{self.MARKER_PREFIX}/hourly/"
            f"year={y:04d}/month={m:02d}/day={d:02d}/hour={h:02d}/_DONE"
        )

    def _daily_marker_key(self, y: int, m: int, d: int) -> str:
        return (
            f"{self.MARKER_PREFIX}/daily/"
            f"year={y:04d}/month={m:02d}/day={d:02d}/_DONE"
        )

    def is_hour_processed(self, y: int, m: int, d: int, h: int) -> bool:
        return self.s3_hook.check_for_key(
            key=self._hourly_marker_key(y, m, d, h),
            bucket_name=self.bucket,
        )

    def is_day_processed(self, y: int, m: int, d: int) -> bool:
        return self.s3_hook.check_for_key(
            key=self._daily_marker_key(y, m, d),
            bucket_name=self.bucket,
        )

    def mark_hour_done(self, y: int, m: int, d: int, h: int) -> None:
        key = self._hourly_marker_key(y, m, d, h)
        self.s3_hook.load_string(
            string_data="", key=key,
            bucket_name=self.bucket, replace=True,
        )
        logger.info(f"Gold hourly marker written: {key}")

    def mark_day_done(self, y: int, m: int, d: int) -> None:
        key = self._daily_marker_key(y, m, d)
        self.s3_hook.load_string(
            string_data="", key=key,
            bucket_name=self.bucket, replace=True,
        )
        logger.info(f"Gold daily marker written: {key}")

    def clear_hourly_markers(self, hours: list[tuple[int, int, int, int]]) -> int:
        """Delete hourly gold markers for given hours. Returns count deleted."""
        deleted = 0
        for y, m, d, h in hours:
            key = self._hourly_marker_key(y, m, d, h)
            if self.s3_hook.check_for_key(key=key, bucket_name=self.bucket):
                self.s3_hook.delete_objects(bucket=self.bucket, keys=[key])
                deleted += 1
        logger.info(f"Cleared {deleted} gold hourly markers")
        return deleted

    def clear_daily_markers(self, days: list[tuple[int, int, int]]) -> int:
        """Delete daily gold markers for given days. Returns count deleted."""
        deleted = 0
        for y, m, d in days:
            key = self._daily_marker_key(y, m, d)
            if self.s3_hook.check_for_key(key=key, bucket_name=self.bucket):
                self.s3_hook.delete_objects(bucket=self.bucket, keys=[key])
                deleted += 1
        logger.info(f"Cleared {deleted} gold daily markers")
        return deleted

    def delete_gold_hour(self, y: int, m: int, d: int, h: int) -> None:
        """Delete gold CH data for a single hour (health + anomaly + health_daily MV)."""
        hour_start = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:00:00"
        hour_end   = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:59:59"
        report_date = f"{y:04d}-{m:02d}-{d:02d}"

        for table in [C.STATION_HEALTH, C.STATION_ANOMALY]:
            self.ch_hook.execute(f"""
                ALTER TABLE {C.SCHEMA_NAME}.{table}
                DELETE WHERE hour_start >= '{hour_start}'
                        AND hour_start <= '{hour_end}'
                SETTINGS mutations_sync = 1
            """)

        # health_daily MV target — delete the full date so it can be
        # re-aggregated from the surviving + new health_hourly rows.
        self.ch_hook.execute(f"""
            ALTER TABLE {C.SCHEMA_NAME}.health_daily
            DELETE WHERE report_date = '{report_date}'
            SETTINGS mutations_sync = 1
        """)

    def delete_gold_day(self, y: int, m: int, d: int) -> None:
        """Delete gold CH data for a single day (all daily tables)."""
        report_date = f"{y:04d}-{m:02d}-{d:02d}"

        daily_tables = {
            C.STATION_SLAC:        "report_date",
            C.STATION_OUTAGE:      "toDate(incident_start)",
            C.STATION_REGION:      "report_date",
            C.STATION_MAINTENANCE: "toDate(maintenance_start)",
            C.STATION_HANDOVER:    "report_date",
            C.STATION_ALARM:       "report_date",
        }
        for table, date_col in daily_tables.items():
            self.ch_hook.execute(f"""
                ALTER TABLE {C.SCHEMA_NAME}.{table}
                DELETE WHERE {date_col} = '{report_date}'
                SETTINGS mutations_sync = 1
            """)

    # ---- bulk range helpers (kept for non-recovery use) ----

    def clear_gold_hourly_range(self, hours: list[tuple[int, int, int, int]]):
        """Delete gold hourly data (health, anomaly, health_daily MV) for a
        range of hours.  Uses ``mutations_sync=1`` so the deletes complete
        before we start re-inserting.
        """
        if not hours:
            return

        min_ts, max_ts = min(hours), max(hours)
        ts_start = f"{min_ts[0]:04d}-{min_ts[1]:02d}-{min_ts[2]:02d} {min_ts[3]:02d}:00:00"
        ts_end   = f"{max_ts[0]:04d}-{max_ts[1]:02d}-{max_ts[2]:02d} {max_ts[3]:02d}:59:59"

        for table in [C.STATION_HEALTH, C.STATION_ANOMALY]:
            self.ch_hook.execute(f"""
                ALTER TABLE {C.SCHEMA_NAME}.{table}
                DELETE WHERE hour_start >= '{ts_start}'
                        AND hour_start <= '{ts_end}'
                SETTINGS mutations_sync = 1
            """)
            logger.info(f"Cleared {C.SCHEMA_NAME}.{table}  [{ts_start} → {ts_end}]")

        # health_daily is a MV target (AggregatingMergeTree).
        # It does NOT auto-clean when the source (health_hourly) is deleted,
        # so we must explicitly clear the affected dates.
        dates = sorted(set((h[0], h[1], h[2]) for h in hours))
        d_start = f"{dates[0][0]:04d}-{dates[0][1]:02d}-{dates[0][2]:02d}"
        d_end   = f"{dates[-1][0]:04d}-{dates[-1][1]:02d}-{dates[-1][2]:02d}"
        self.ch_hook.execute(f"""
            ALTER TABLE {C.SCHEMA_NAME}.health_daily
            DELETE WHERE report_date >= '{d_start}'
                     AND report_date <= '{d_end}'
            SETTINGS mutations_sync = 1
        """)
        logger.info(f"Cleared {C.SCHEMA_NAME}.health_daily  [{d_start} → {d_end}]")

    def clear_gold_daily_range(self, days: list[tuple[int, int, int]]):
        """Delete gold daily data (SLA, outage, region, maintenance, handover,
        alarm) for a range of days.
        """
        if not days:
            return

        d_start = f"{min(days)[0]:04d}-{min(days)[1]:02d}-{min(days)[2]:02d}"
        d_end   = f"{max(days)[0]:04d}-{max(days)[1]:02d}-{max(days)[2]:02d}"

        daily_tables = {
            C.STATION_SLAC:        "report_date",
            C.STATION_OUTAGE:      "toDate(incident_start)",
            C.STATION_REGION:      "report_date",
            C.STATION_MAINTENANCE: "toDate(maintenance_start)",
            C.STATION_HANDOVER:    "report_date",
            C.STATION_ALARM:       "report_date",
        }

        for table, date_col in daily_tables.items():
            self.ch_hook.execute(f"""
                ALTER TABLE {C.SCHEMA_NAME}.{table}
                DELETE WHERE {date_col} >= '{d_start}'
                         AND {date_col} <= '{d_end}'
                SETTINGS mutations_sync = 1
            """)
            logger.info(f"Cleared {C.SCHEMA_NAME}.{table}  [{d_start} → {d_end}]")

    # =========================================================================
    # Generic write helper
    # =========================================================================

    def _generic_write_to_lake_ch(self, df: pd.DataFrame, s3_key: str, ch_table: str, columns: list[str]):
        self.s3_io.write_parquet(df, s3_key)
        s3_url = self._create_s3_url(s3_key)

        try:
            self.ch_hook.execute(f"""
                INSERT INTO {C.SCHEMA_NAME}.{ch_table}
                SELECT * FROM s3('{s3_url}', '{self.s3_access_key}', '{self.s3_secret_key}', 'Parquet')
                SETTINGS use_hive_partitioning=0
            """)
        except Exception:
            logger.warning(f"S3 insert failed for {ch_table}, falling back to VALUES insert")
            self.ch_hook.execute(
                f"INSERT INTO {C.SCHEMA_NAME}.{ch_table} ({', '.join(columns)}) VALUES",
                params=[tuple(row) for _, row in df.iterrows()]
            )

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
        
        try:
            joined_df = self.ch_hook.execute(f"""
                SELECT 
                    t.station_id AS station_id,
                    t.station_code AS station_code,
                    toStartOfHour(t.event_time) AS hour_start,
                    dictGet('telecom.dict_station', 'operator_code', t.station_id) AS operator_code,
                    dictGet('telecom.dict_station', 'province', t.station_id) AS province,
                    dictGet('telecom.dict_station', 'region', t.station_id) AS region,
                    dictGet('telecom.dict_station', 'density', t.station_id) AS density,
                    dictGet('telecom.dict_station', 'technology', t.station_id) AS technology,
                                
                    count() AS session_count,
                    uniq(t.imsi_hash) AS unique_subscribers,
                    sum(t.bytes_up + t.bytes_down) AS total_bytes,
                    avg(t.latency_ms) AS avg_latency_ms,
                    quantile(0.95)(t.latency_ms) AS p95_latency_ms,
                    avg(t.packet_loss_pct) AS avg_packet_loss_pct,
                    countIf(t.latency_ms > 100) / count() AS high_latency_ratio,
                                
                    m.avg_cpu,
                    m.max_cpu,
                    m.avg_memory,
                    m.avg_temp,
                    m.max_temp,
                    m.avg_throughput,
                    m.error_count,
                                
                    e.alarm_count,
                    e.warning_count,
                    e.critical_count,
                    e.handover_count,
                    e.incident_active,
                    e.incident_type,
                    e.maintenance_active
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_ST} t
                    LEFT JOIN (
                        SELECT
                            station_id,
                            avg(cpu_util_pct) AS avg_cpu,
                            max(cpu_util_pct) AS max_cpu,
                            avg(memory_util_pct) AS avg_memory,
                            avg(temperature_c) AS avg_temp,
                            max(temperature_c) AS max_temp,
                            avg(throughput_mbps) AS avg_throughput,
                            max(error_count) AS error_count
                        FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_PM}
                        WHERE metric_time >= '{hour_start}' AND metric_time <= '{hour_end}'
                        GROUP BY station_id
                    ) m ON t.station_id = m.station_id
                    LEFT JOIN (
                        SELECT
                            station_id,
                            countIf(event_type = 'alarm') AS alarm_count,
                            countIf(severity IN ('warning', 'error', 'critical')) AS warning_count,
                            countIf(severity = 'critical') AS critical_count,
                            countIf(event_type = 'handover') AS handover_count,
                            max(event_type = 'incident_start') AS incident_active,
                            argMaxIf(
                                JSONExtractString(metadata::String, 'incident_type'),
                                event_time,
                                event_type = 'incident_start'
                            ) AS incident_type,
                            max(event_type = 'maintenance_start') AS maintenance_active
                        FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                        WHERE event_time >= '{hour_start}' AND event_time <= '{hour_end}'
                        GROUP BY station_id
                    ) e ON t.station_id = e.station_id

                    WHERE t.event_time >= '{hour_start}' AND t.event_time <= '{hour_end}'
                    GROUP BY
                        station_id, hour_start, station_code, operator_code,
                        province, region, density, technology,
                        m.avg_cpu, m.max_cpu, m.avg_memory, m.avg_temp,
                        m.max_temp, m.avg_throughput, m.error_count,
                        e.alarm_count, e.warning_count, e.critical_count,
                        e.handover_count, e.incident_active, e.incident_type,
                        e.maintenance_active
            """)

            if not joined_df:
                logger.info(f"No data found for {hour_start} to {hour_end}")
                return None
        
            df = pd.DataFrame(joined_df, columns=[
                'station_id', 'station_code', 'hour_start', 'operator_code', 'province', 'region', 'density', 'technology',
                'session_count', 'unique_subscribers', 'total_bytes', 'avg_latency_ms', 'p95_latency_ms', 'avg_packet_loss_pct', 'high_latency_ratio',
                'avg_cpu_pct', 'max_cpu_pct', 'avg_memory_pct', 'avg_temperature_c', 'max_temperature_c', 'avg_throughput_mbps', 'error_count',
                'alarm_count', 'warning_count', 'critical_count', 'handover_count',
                'incident_active', 'incident_type', 'maintenance_active'
            ])

            # Compute health score for each station-hour
            df['health_score'], df['health_category'] = zip(*df.apply(self._compute_health_score, axis=1))

            # Write to S3 as Parquet
            s3_key = f"gold/health/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/health_score.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_HEALTH} 
            #         SELECT * FROM s3(
            #             '{s3_url}', 
            #             '{self.s3_access_key}', 
            #             '{self.s3_secret_key}', 
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0

            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting health scores into ClickHouse for {hour_start} to {hour_end}: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_HEALTH} (
            #                 station_id, hour_start, station_code, operator_code, province, region, density, technology,
            #                 session_count, unique_subscribers, total_bytes, avg_latency_ms, p95_latency_ms, avg_packet_loss_pct, high_latency_ratio,
            #                 avg_cpu_pct, max_cpu_pct, avg_memory_pct, avg_temperature_c, max_temperature_c, avg_throughput_mbps, error_count,
            #                 alarm_count, warning_count, critical_count, handover_count,
            #                 incident_active, incident_type, maintenance_active,
            #                 health_score, health_category
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting health scores into ClickHouse via VALUES for {hour_start} to {hour_end}: {ex2}")
            #         raise ex2

            self._generic_write_to_lake_ch(df, s3_key, C.STATION_HEALTH, df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error inserting health scores into ClickHouse for {hour_start} to {hour_end}: {ex}")
            raise ex
    
    
    # =========================================================================
    # SLA Compliance
    # =========================================================================

    def gold_sla_compliance(self, year: int, month: int, day: int) -> dict:
        report_date = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            joined_df = self.ch_hook.execute(f"""
                WITH health_data AS (
                    SELECT 
                        station_id,
                        station_code,
                        toDate(hour_start) AS report_date,
                        operator_code,
                        province,
                        region,
                        density,
                        technology,
                        count() AS total_hours,
                        countIf(health_category IS NOT NULL) AS active_hours,
                        countIf(health_category = 'critical') AS down_hours,
                        countIf(health_category IS NULL) AS maintenance_hours,
                        countIf(health_category IN ('degraded', 'warning')) AS degraded_hours,
                        avg(health_score) AS avg_health_score,
                        min(health_score) AS min_health_score,
                        countIf(health_score < 60) AS hours_below_60,
                        countIf(health_score < 30) AS hours_below_30
                    FROM {C.SCHEMA_NAME}.{C.STATION_HEALTH}
                    WHERE toDate(hour_start) = '{report_date}'
                    GROUP BY station_id, station_code, report_date, operator_code, province, region, density, technology
                ),
                sla_calc AS (
                    SELECT 
                        ha.*,
                        24 - ha.maintenance_hours AS billable_hours,
                        (24 - ha.maintenance_hours) - ha.down_hours AS available_hours,
                        if(ha.maintenance_hours >= 24, 100.0,
                            ((24 - ha.maintenance_hours - ha.down_hours) / (24 - ha.maintenance_hours)) * 100
                        ) AS uptime_pct,
                        CASE ha.technology
                            WHEN '5G' THEN 99.99 WHEN '4G' THEN 99.9 WHEN '3G' THEN 99.5 ELSE 99.0
                        END AS sla_target_pct
                    FROM health_data ha
                )
                SELECT 
                    sc.station_id,
                    sc.station_code,
                    sc.report_date,
                    sc.operator_code,
                    sc.province,
                    sc.region,
                    sc.density,
                    sc.technology,
                    sc.total_hours,
                    sc.active_hours,
                    sc.down_hours,
                    sc.maintenance_hours,
                    sc.degraded_hours,
                    sc.billable_hours,
                    sc.available_hours,
                    sc.uptime_pct,
                    sc.sla_target_pct,
                    if(sc.uptime_pct >= sc.sla_target_pct, 1, 0) AS sla_met,
                    greatest(0,
                        (sc.sla_target_pct / 100) * sc.billable_hours - sc.available_hours
                    ) AS sla_breach_hours,
                    sc.avg_health_score,
                    sc.min_health_score,
                    sc.hours_below_60,
                    sc.hours_below_30,
                    e.incident_count,
                    e.total_incident_min,
                    e.longest_incident_min,
                    e.mttr_min,
                    CASE
                        WHEN sc.uptime_pct >= sc.sla_target_pct THEN 'compliant'
                        WHEN sc.uptime_pct <= 95 THEN 'critical_breach'
                        ELSE 'minor_breach'
                    END AS compliance_status
                FROM sla_calc sc
                LEFT JOIN (
                    SELECT
                        station_id,
                        toDate(event_time) AS event_date,
                        countIf(event_type = 'incident_start') AS incident_count,
                        sumIf(JSONExtractFloat(metadata::String, 'duration_min'), event_type = 'incident_end') AS total_incident_min,
                        maxIf(JSONExtractFloat(metadata::String, 'duration_min'), event_type = 'incident_end') AS longest_incident_min,
                        avgIf(JSONExtractFloat(metadata::String, 'duration_min'), event_type = 'incident_end') AS mttr_min
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                    WHERE toDate(event_time) = '{report_date}'
                    GROUP BY station_id, event_date
                ) e ON sc.station_id = e.station_id AND e.event_date = sc.report_date
                """ )
            
            if not joined_df:
                logger.info(f"No data found for SLA compliance on {year:04d}-{month:02d}-{day:02d}")
                return None
            
            df = pd.DataFrame(joined_df, columns=[
                'station_id', 'station_code', 'report_date', 'operator_code', 'province', 'region', 'density', 'technology',
                'total_hours', 'active_hours', 'down_hours', 'maintenance_hours', 'degraded_hours',
                'billable_hours', 'available_hours', 'uptime_pct', 'sla_target_pct', 'sla_met', 'sla_breach_hours',
                'avg_health_score', 'min_health_score', 'hours_below_60', 'hours_below_30',
                'incident_count', 'total_incident_min', 'longest_incident_min', 'mttr_min',
                'compliance_status'
            ])

            s3_key = f"gold/sa_compliance/year={year:04d}/month={month:02d}/day={day:02d}/sa_compliance_report.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_SLAC} 
            #         SELECT * FROM s3(
            #             '{s3_url}', 
            #             '{self.s3_access_key}', 
            #             '{self.s3_secret_key}', 
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting SLA compliance report into ClickHouse for {year:04d}-{month:02d}-{day:02d}: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_SLAC} (
            #                 station_id, station_code, report_date, operator_code, province, region, density, technology,
            #                 total_hours, active_hours, down_hours, maintenance_hours, degraded_hours,
            #                 billable_hours, available_hours, uptime_pct, sla_target_pct, sla_met, sla_breach_hours,
            #                 avg_health_score, min_health_score, hours_below_60, hours_below_30,
            #                 incident_count, total_incident_min, longest_incident_min, mttr_min,
            #                 compliance_status
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting SLA compliance report into ClickHouse via VALUES for {year:04d}-{month:02d}-{day:02d}: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(df, s3_key, C.STATION_SLAC, df.columns.tolist())
            
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
        target_date = date(year, month, day)
        baseline_start = (target_date - timedelta(days=7)).isoformat()
        report_date = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            joined_df = self.ch_hook.execute(f"""
                WITH current_data AS (
                    SELECT
                        sh.station_id,
                        sh.station_code,
                        toStartOfHour(sh.hour_start) AS hour_start,
                        sh.avg_latency_ms AS current_latency_ms,
                        sh.avg_packet_loss_pct AS current_packet_loss,
                        sh.avg_cpu_pct AS current_cpu_pct,
                        sh.avg_throughput_mbps AS current_throughput,
                        sh.unique_subscribers AS current_subscribers
                    FROM {C.SCHEMA_NAME}.{C.STATION_HEALTH} sh
                    WHERE toDate(sh.hour_start) = '{report_date}'
                      AND toHour(sh.hour_start) = {hour}
                ),
                baseline AS (
                    SELECT
                        station_id,
                        avg(avg_latency_ms) AS baseline_latency_mean,
                        stddevPop(avg_latency_ms) AS baseline_latency_std,
                        avg(avg_cpu_pct) AS baseline_cpu_mean,
                        stddevPop(avg_cpu_pct) AS baseline_cpu_std,
                        avg(avg_throughput_mbps) AS baseline_throughput_mean,
                        stddevPop(avg_throughput_mbps) AS baseline_throughput_std,
                        avg(unique_subscribers) AS baseline_subs_mean,
                        stddevPop(unique_subscribers) AS baseline_subs_std
                    FROM {C.SCHEMA_NAME}.{C.STATION_HEALTH}
                    WHERE toHour(hour_start) = {hour}
                      AND toDate(hour_start) >= '{baseline_start}'
                      AND toDate(hour_start) < '{report_date}'
                    GROUP BY station_id
                )
                SELECT
                    c.station_id,
                    c.station_code,
                    c.hour_start,
                    c.current_latency_ms,
                    c.current_packet_loss,
                    c.current_cpu_pct,
                    c.current_throughput,
                    c.current_subscribers,
                    b.baseline_latency_mean,
                    b.baseline_latency_std,
                    b.baseline_cpu_mean,
                    b.baseline_cpu_std,
                    b.baseline_throughput_mean,
                    b.baseline_throughput_std,
                    b.baseline_subs_mean,
                    b.baseline_subs_std
                FROM current_data c
                LEFT JOIN baseline b ON c.station_id = b.station_id
            """)

            if not joined_df:
                logger.info(f"No data found for anomaly feature computation on {year:04d}-{month:02d}-{day:02d} {hour:02d}:00")
                return None
            
            df = pd.DataFrame(joined_df, columns=[
                'station_id', 'station_code', 'hour_start',
                'current_latency_ms', 'current_packet_loss', 'current_cpu_pct', 'current_throughput', 'current_subscribers',
                'baseline_latency_mean', 'baseline_latency_std',
                'baseline_cpu_mean', 'baseline_cpu_std',
                'baseline_throughput_mean', 'baseline_throughput_std',
                'baseline_subs_mean', 'baseline_subs_std'
            ])

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

            s3_key = f"gold/anomaly_features/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/anomaly_features_report.parquet"
            # self.s3_io.write_parquet(result_df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_ANOMALY} 
            #         SELECT * FROM s3(
            #             '{s3_url}', 
            #             '{self.s3_access_key}', 
            #             '{self.s3_secret_key}', 
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting anomaly features into ClickHouse for {year:04d}-{month:02d}-{day:02d}: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_ANOMALY} (
            #                 station_id, station_code, hour_start,
            #                 current_latency_ms, current_packet_loss, current_cpu_pct, current_throughput, current_subscribers,
            #                 baseline_latency_mean, baseline_latency_std,
            #                 baseline_cpu_mean, baseline_cpu_std,
            #                 baseline_throughput_mean, baseline_throughput_std,
            #                 baseline_subs_mean, baseline_subs_std,
            #                 z_latency, z_cpu, z_throughput, z_subscribers,
            #                 is_anomalous, anomaly_type, neighbor_anomaly_count
            #             )
            #             VALUES""", params=[tuple(row) for _, row in result_df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting anomaly features into ClickHouse via VALUES for {year:04d}-{month:02d}-{day:02d}: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(result_df, s3_key, C.STATION_ANOMALY, result_df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error computing anomaly features for {year:04d}-{month:02d}-{day:02d} {hour:02d}:00: {ex}")
            raise ex

    # =========================================================================
    # Outage Report
    # =========================================================================

    def gold_outage_report(self, year: int, month: int, day: int) -> dict:
        
        target_date = date(year, month, day)
        report_date = target_date.isoformat()
        baseline_start = (target_date - timedelta(days=7)).isoformat()

        try:
            joined_df = self.ch_hook.execute(f"""
                WITH 
                -- Step 1: Pair incident_start → incident_end using row_number
                -- (simulator guarantees no overlapping incidents per station)
                starts AS (
                    SELECT
                        station_id,
                        event_time AS incident_start,
                        metadata AS start_meta,
                        row_number() OVER (PARTITION BY station_id ORDER BY event_time) AS rn
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                    WHERE event_type = 'incident_start'
                      AND toDate(event_time) = '{report_date}'
                ),
                ends AS (
                    SELECT
                        station_id,
                        event_time AS incident_end,
                        metadata AS end_meta,
                        row_number() OVER (PARTITION BY station_id ORDER BY event_time) AS rn
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                    WHERE event_type = 'incident_end'
                      AND toDate(event_time) >= '{report_date}'
                      AND toDate(event_time) <= toDate('{report_date}') + 1
                ),
                incident_pairs AS (
                    SELECT
                        s.station_id,
                        s.incident_start,
                        e.incident_end,
                        s.start_meta,
                        e.end_meta
                    FROM starts s
                    LEFT JOIN ends e ON s.station_id = e.station_id AND s.rn = e.rn
                ),

                -- Step 2: Aggregate traffic during each incident window
                incident_traffic AS (
                    SELECT
                        ip.station_id,
                        ip.incident_start,
                        uniq(st.imsi_hash) AS affected_subscribers,
                        sum(st.bytes_up + st.bytes_down) AS incident_bytes
                    FROM incident_pairs ip
                    LEFT JOIN {C.SCHEMA_NAME}.{C.STATION_STAGING_ST} st
                        ON ip.station_id = st.station_id
                        AND st.event_time >= ip.incident_start
                        AND st.event_time <= coalesce(ip.incident_end, now())
                    WHERE toDate(st.event_time) >= '{report_date}'
                    GROUP BY ip.station_id, ip.incident_start
                ),

                -- Step 3: Average health score during the incident hours
                incident_health AS (
                    SELECT
                        ip.station_id,
                        ip.incident_start,
                        avg(h.health_score) AS health_score_during
                    FROM incident_pairs ip
                    LEFT JOIN {C.SCHEMA_NAME}.{C.STATION_HEALTH} h
                        ON ip.station_id = h.station_id
                        AND h.hour_start >= toStartOfHour(ip.incident_start)
                        AND h.hour_start <= toStartOfHour(coalesce(ip.incident_end, now()))
                    GROUP BY ip.station_id, ip.incident_start
                ),

                -- Step 4: Baseline = avg hourly bytes for same station+hour over past 7 days
                baseline AS (
                    SELECT
                        station_id,
                        toHour(event_time) AS hour_of_day,
                        sum(bytes_up + bytes_down) / 7 AS avg_hourly_bytes
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_ST}
                    WHERE toDate(event_time) >= '{baseline_start}'
                      AND toDate(event_time) < '{report_date}'
                    GROUP BY station_id, hour_of_day
                )

                SELECT
                    ip.station_id,
                    dictGet('telecom.dict_station', 'station_code', ip.station_id) AS station_code,
                    dictGet('telecom.dict_station', 'operator_code', ip.station_id) AS operator_code,
                    dictGet('telecom.dict_station', 'province', ip.station_id) AS province,
                    dictGet('telecom.dict_station', 'region', ip.station_id) AS region,
                    dictGet('telecom.dict_station', 'technology', ip.station_id) AS technology,

                    JSONExtractString(ip.start_meta::String, 'incident_type') AS incident_type,
                    JSONExtractFloat(ip.start_meta::String, 'severity') AS severity,
                    ip.incident_start,
                    ip.incident_end,
                    JSONExtractFloat(ip.end_meta::String, 'duration_min') AS duration_min,
                    JSONExtractFloat(ip.start_meta::String, 'estimated_duration_min') AS estimated_duration_min,
                    
                    it.affected_subscribers,
                    greatest(0,
                        coalesce(bl.avg_hourly_bytes, 0) 
                            * (JSONExtractFloat(ip.end_meta::String, 'duration_min') / 60)
                            - coalesce(it.incident_bytes, 0)
                    ) AS traffic_loss_bytes,
                    JSONExtractFloat(ip.start_meta::String, 'hardware_quality') AS hardware_quality,
                    ih.health_score_during

                FROM incident_pairs ip
                LEFT JOIN incident_traffic it 
                    ON ip.station_id = it.station_id AND ip.incident_start = it.incident_start
                LEFT JOIN incident_health ih 
                    ON ip.station_id = ih.station_id AND ip.incident_start = ih.incident_start
                LEFT JOIN baseline bl 
                    ON ip.station_id = bl.station_id AND toHour(ip.incident_start) = bl.hour_of_day
            """)

            if not joined_df:
                logger.info(f"No data found for outage report on {year:04d}-{month:02d}-{day:02d}")
                return None
            
            df = pd.DataFrame(joined_df, columns=[
                'station_id', 'station_code', 'operator_code', 'province', 'region', 'technology',
                'incident_type', 'severity', 'incident_start', 'incident_end', 'duration_min', 'estimated_duration_min',
                'affected_subscribers', 'traffic_loss_bytes', 'hardware_quality', 'health_score_during'
            ])

            s3_key = f"gold/outage_report/year={year:04d}/month={month:02d}/day={day:02d}/outage_report.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_OUTAGE} 
            #         SELECT * FROM s3(
            #             '{s3_url}', 
            #             '{self.s3_access_key}', 
            #             '{self.s3_secret_key}', 
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting outage report into ClickHouse for {year:04d}-{month:02d}-{day:02d}: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_OUTAGE} (
            #                 station_id, station_code, operator_code, province, region, technology,
            #                 incident_type, severity, incident_start, incident_end, duration_min, estimated_duration_min,
            #                 affected_subscribers, traffic_loss_bytes, hardware_quality, health_score_during
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting outage report into ClickHouse via VALUES for {year:04d}-{month:02d}-{day:02d}: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(df, s3_key, C.STATION_OUTAGE, df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error computing outage report for {year:04d}-{month:02d}-{day:02d}: {ex}")
            raise ex
    
    # =========================================================================
    # Region Daily Report
    # =========================================================================

    def gold_region_daily(self, year: int, month: int, day: int) -> dict:
        report_date = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            joined_df = self.ch_hook.execute(f"""
                WITH health_agg AS (
                    SELECT
                        toDate(hour_start) AS report_date,
                        region,
                        province,
                        operator_code,
                        technology,
                        uniq(station_id) AS station_count,
                        uniqIf(station_id, session_count > 0) AS active_station_count,
                        sum(unique_subscribers) AS total_subscribers,
                        sum(session_count) AS total_sessions,
                        sum(total_bytes) AS total_bytes,
                        avg(health_score) AS avg_health_score,
                        min(health_score) AS min_health_score,
                        uniqIf(station_id, health_score < 30) AS stations_critical,
                        uniqIf(station_id, health_score >= 30 AND health_score < 60) AS stations_degraded,
                        avg(avg_latency_ms) AS avg_latency_ms,
                        quantile(0.95)(p95_latency_ms) AS p95_latency_ms,
                        avg(avg_packet_loss_pct) AS avg_packet_loss_pct,
                        countIf(incident_active = 1) AS incident_count
                    FROM {C.SCHEMA_NAME}.{C.STATION_HEALTH}
                    WHERE toDate(hour_start) = '{report_date}'
                    GROUP BY report_date, region, province, operator_code, technology
                )
                SELECT
                    ha.*,
                    coalesce(sla.sla_breach_count, 0) AS sla_breach_count,
                    if(sla.total_stations > 0,
                        (sla.total_stations - coalesce(sla.sla_breach_count, 0)) / sla.total_stations * 100,
                        100.0
                    ) AS sla_compliance_pct
                FROM health_agg ha
                LEFT JOIN (
                    SELECT
                        region,
                        province,
                        operator_code,
                        technology,
                        count() AS total_stations,
                        countIf(sla_met = 0) AS sla_breach_count
                    FROM {C.SCHEMA_NAME}.{C.STATION_SLAC}
                    WHERE report_date = '{report_date}'
                    GROUP BY region, province, operator_code, technology
                ) sla ON ha.region = sla.region 
                     AND ha.province = sla.province 
                     AND ha.operator_code = sla.operator_code 
                     AND ha.technology = sla.technology
            """)

            if not joined_df:
                logger.info(f"No data found for region daily report on {report_date}")
                return None

            df = pd.DataFrame(joined_df, columns=[
                'report_date', 'region', 'province', 'operator_code', 'technology',
                'station_count', 'active_station_count', 'total_subscribers', 'total_sessions', 'total_bytes',
                'avg_health_score', 'min_health_score', 'stations_critical', 'stations_degraded',
                'avg_latency_ms', 'p95_latency_ms', 'avg_packet_loss_pct', 'incident_count',
                'sla_breach_count', 'sla_compliance_pct'
            ])

            s3_key = f"gold/region_daily/year={year:04d}/month={month:02d}/day={day:02d}/region_daily.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_REGION}
            #         SELECT * FROM s3(
            #             '{s3_url}',
            #             '{self.s3_access_key}',
            #             '{self.s3_secret_key}',
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting region daily report into ClickHouse: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_REGION} (
            #                 report_date, region, province, operator_code, technology,
            #                 station_count, active_station_count, total_subscribers, total_sessions, total_bytes,
            #                 avg_health_score, min_health_score, stations_critical, stations_degraded,
            #                 avg_latency_ms, p95_latency_ms, avg_packet_loss_pct, incident_count,
            #                 sla_breach_count, sla_compliance_pct
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting region daily via VALUES: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(df, s3_key, C.STATION_REGION, df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error computing region daily report for {report_date}: {ex}")
            raise ex

    
    # =========================================================================
    # Maintenance Report
    # =========================================================================

    def gold_maintenance_report(self, year: int, month: int, day: int) -> dict:
        report_date = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            joined_df = self.ch_hook.execute(f"""
                WITH 
                maint_starts AS (
                    SELECT
                        station_id,
                        event_time AS maintenance_start,
                        metadata AS start_meta,
                        row_number() OVER (PARTITION BY station_id ORDER BY event_time) AS rn
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                    WHERE event_type = 'maintenance_start'
                      AND toDate(event_time) = '{report_date}'
                ),
                maint_ends AS (
                    SELECT
                        station_id,
                        event_time AS maintenance_end,
                        metadata AS end_meta,
                        row_number() OVER (PARTITION BY station_id ORDER BY event_time) AS rn
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                    WHERE event_type = 'maintenance_end'
                      AND toDate(event_time) >= '{report_date}'
                      AND toDate(event_time) <= toDate('{report_date}') + 1
                ),
                maint_pairs AS (
                    SELECT
                        s.station_id,
                        s.maintenance_start,
                        e.maintenance_end,
                        s.start_meta,
                        e.end_meta
                    FROM maint_starts s
                    LEFT JOIN maint_ends e ON s.station_id = e.station_id AND s.rn = e.rn
                )

                SELECT
                    mp.station_id,
                    dictGet('telecom.dict_station', 'station_code', mp.station_id) AS station_code,
                    dictGet('telecom.dict_station', 'operator_code', mp.station_id) AS operator_code,
                    dictGet('telecom.dict_station', 'region', mp.station_id) AS region,
                    dictGet('telecom.dict_station', 'technology', mp.station_id) AS technology,
                    mp.maintenance_start,
                    mp.maintenance_end,
                    JSONExtractInt(mp.start_meta::String, 'planned_duration_min') AS planned_duration_min,
                    dateDiff('minute', mp.maintenance_start, coalesce(mp.maintenance_end, now())) AS actual_duration_min,
                    dateDiff('minute', mp.maintenance_start, coalesce(mp.maintenance_end, now()))
                        - JSONExtractInt(mp.start_meta::String, 'planned_duration_min') AS overrun_min,
                    JSONExtractBool(mp.start_meta::String, 'tech_upgrade') AS tech_upgrade,
                    JSONExtractBool(mp.start_meta::String, 'sla_excluded') AS sla_excluded,
                    h_before.health_score AS pre_maintenance_health,
                    h_after.health_score AS post_maintenance_health
                FROM maint_pairs mp
                LEFT JOIN {C.SCHEMA_NAME}.{C.STATION_HEALTH} h_before
                    ON mp.station_id = h_before.station_id
                    AND h_before.hour_start = toStartOfHour(mp.maintenance_start) - INTERVAL 1 HOUR
                LEFT JOIN {C.SCHEMA_NAME}.{C.STATION_HEALTH} h_after
                    ON mp.station_id = h_after.station_id
                    AND h_after.hour_start = toStartOfHour(coalesce(mp.maintenance_end, now())) + INTERVAL 1 HOUR
            """)

            if not joined_df:
                logger.info(f"No data found for maintenance report on {report_date}")
                return None

            df = pd.DataFrame(joined_df, columns=[
                'station_id', 'station_code', 'operator_code', 'region', 'technology',
                'maintenance_start', 'maintenance_end',
                'planned_duration_min', 'actual_duration_min', 'overrun_min',
                'tech_upgrade', 'sla_excluded',
                'pre_maintenance_health', 'post_maintenance_health'
            ])

            s3_key = f"gold/maintenance_report/year={year:04d}/month={month:02d}/day={day:02d}/maintenance_report.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_MAINTENANCE}
            #         SELECT * FROM s3(
            #             '{s3_url}',
            #             '{self.s3_access_key}',
            #             '{self.s3_secret_key}',
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting maintenance report into ClickHouse: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_MAINTENANCE} (
            #                 station_id, station_code, operator_code, region, technology,
            #                 maintenance_start, maintenance_end,
            #                 planned_duration_min, actual_duration_min, overrun_min,
            #                 tech_upgrade, sla_excluded,
            #                 pre_maintenance_health, post_maintenance_health
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting maintenance report via VALUES: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(df, s3_key, C.STATION_MAINTENANCE, df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error computing maintenance report for {report_date}: {ex}")
            raise ex

    
    # =========================================================================
    # Handover Report
    # =========================================================================

    def gold_handover_report(self, year: int, month: int, day: int) -> dict:
        report_date = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            joined_df = self.ch_hook.execute(f"""
                SELECT
                    toDate(e.event_time) AS report_date,
                    e.station_id AS source_station_id,
                    dictGet('telecom.dict_station', 'station_code', e.station_id) AS source_station_code,
                    e.target_station_id AS target_station_id,
                    dictGet('telecom.dict_station', 'station_code', e.target_station_id) AS target_station_code,
                    dictGet('telecom.dict_station', 'region', e.station_id) AS source_region,
                    dictGet('telecom.dict_station', 'region', e.target_station_id) AS target_region,
                    count() AS handover_count,
                    uniq(
                        JSONExtractString(e.metadata::String, 'imsi_hash')
                    ) AS unique_subscribers,
                    src_traffic.avg_latency AS avg_latency_before_ms,
                    tgt_traffic.avg_latency AS avg_latency_after_ms
                FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE} e
                LEFT JOIN (
                    SELECT
                        station_id,
                        toDate(event_time) AS traffic_date,
                        avg(latency_ms) AS avg_latency
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_ST}
                    WHERE toDate(event_time) = '{report_date}'
                    GROUP BY station_id, traffic_date
                ) src_traffic 
                    ON e.station_id = src_traffic.station_id
                    AND toDate(e.event_time) = src_traffic.traffic_date
                LEFT JOIN (
                    SELECT
                        station_id,
                        toDate(event_time) AS traffic_date,
                        avg(latency_ms) AS avg_latency
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_ST}
                    WHERE toDate(event_time) = '{report_date}'
                    GROUP BY station_id, traffic_date
                ) tgt_traffic 
                    ON e.target_station_id = tgt_traffic.station_id
                    AND toDate(e.event_time) = tgt_traffic.traffic_date
                WHERE e.event_type = 'handover'
                  AND toDate(e.event_time) = '{report_date}'
                  AND e.target_station_id IS NOT NULL
                  AND e.target_station_id != 0
                GROUP BY 
                    report_date, source_station_id, source_station_code,
                    target_station_id, target_station_code,
                    source_region, target_region,
                    src_traffic.avg_latency, tgt_traffic.avg_latency
            """)

            if not joined_df:
                logger.info(f"No data found for handover report on {report_date}")
                return None

            df = pd.DataFrame(joined_df, columns=[
                'report_date', 'source_station_id', 'source_station_code',
                'target_station_id', 'target_station_code',
                'source_region', 'target_region',
                'handover_count', 'unique_subscribers',
                'avg_latency_before_ms', 'avg_latency_after_ms'
            ])

            s3_key = f"gold/handover_daily/year={year:04d}/month={month:02d}/day={day:02d}/handover_daily.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_HANDOVER}
            #         SELECT * FROM s3(
            #             '{s3_url}',
            #             '{self.s3_access_key}',
            #             '{self.s3_secret_key}',
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting handover report into ClickHouse: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_HANDOVER} (
            #                 report_date, source_station_id, source_station_code,
            #                 target_station_id, target_station_code,
            #                 source_region, target_region,
            #                 handover_count, unique_subscribers,
            #                 avg_latency_before_ms, avg_latency_after_ms
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting handover report via VALUES: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(df, s3_key, C.STATION_HANDOVER, df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error computing handover report for {report_date}: {ex}")
            raise ex

    
    # =========================================================================
    # Alarm Daily Report
    # =========================================================================

    def gold_alarm_report(self, year: int, month: int, day: int) -> dict:
        report_date = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            joined_df = self.ch_hook.execute(f"""
                WITH alarm_agg AS (
                    SELECT
                        toDate(event_time) AS report_date,
                        station_id,
                        countIf(severity = 'warning') AS warning_count,
                        countIf(severity = 'error') AS error_count,
                        countIf(severity = 'critical') AS critical_count,
                        count() AS total_alarm_count,
                        topK(1)(description)[1] AS top_alarm_description
                    FROM {C.SCHEMA_NAME}.{C.STATION_STAGING_SE}
                    WHERE event_type = 'alarm'
                      AND toDate(event_time) = '{report_date}'
                    GROUP BY report_date, station_id
                )
                SELECT
                    a.report_date,
                    a.station_id,
                    dictGet('telecom.dict_station', 'station_code', a.station_id) AS station_code,
                    dictGet('telecom.dict_station', 'operator_code', a.station_id) AS operator_code,
                    dictGet('telecom.dict_station', 'region', a.station_id) AS region,
                    dictGet('telecom.dict_station', 'technology', a.station_id) AS technology,
                    a.warning_count,
                    a.error_count,
                    a.critical_count,
                    a.total_alarm_count,
                    if(h.active_hours > 0,
                        (a.total_alarm_count / h.active_hours)::Float64,
                        a.total_alarm_count::Float64
                    ) AS alarm_rate_per_hour,
                    a.top_alarm_description,
                    h.avg_health_score
                FROM alarm_agg a
                LEFT JOIN (
                    SELECT
                        station_id,
                        countIf(health_category IS NOT NULL) AS active_hours,
                        avg(health_score)::Float64 AS avg_health_score
                    FROM {C.SCHEMA_NAME}.{C.STATION_HEALTH}
                    WHERE toDate(hour_start) = '{report_date}'
                    GROUP BY station_id
                ) h ON a.station_id = h.station_id
            """)

            if not joined_df:
                logger.info(f"No data found for alarm daily report on {report_date}")
                return None

            df = pd.DataFrame(joined_df, columns=[
                'report_date', 'station_id', 'station_code', 'operator_code', 'region', 'technology',
                'warning_count', 'error_count', 'critical_count', 'total_alarm_count',
                'alarm_rate_per_hour', 'top_alarm_description', 'health_score_avg'
            ])

            s3_key = f"gold/alarm_daily/year={year:04d}/month={month:02d}/day={day:02d}/alarm_daily.parquet"
            # self.s3_io.write_parquet(df, s3_key)
            # s3_url = self._create_s3_url(s3_key)

            # try:
            #     self.ch_hook.execute(f"""
            #         INSERT INTO {C.SCHEMA_NAME}.{C.STATION_ALARM}
            #         SELECT * FROM s3(
            #             '{s3_url}',
            #             '{self.s3_access_key}',
            #             '{self.s3_secret_key}',
            #             'Parquet')
            #         SETTINGS use_hive_partitioning=0
            #     """)
            # except Exception as ex1:
            #     logger.error(f"Error inserting alarm daily report into ClickHouse: {ex1}")
            #     try:
            #         self.ch_hook.execute(f"""
            #             INSERT INTO {C.SCHEMA_NAME}.{C.STATION_ALARM} (
            #                 report_date, station_id, station_code, operator_code, region, technology,
            #                 warning_count, error_count, critical_count, total_alarm_count,
            #                 alarm_rate_per_hour, top_alarm_description, health_score_avg
            #             )
            #             VALUES""", params=[tuple(row) for _, row in df.iterrows()])
            #     except Exception as ex2:
            #         logger.error(f"Error inserting alarm daily via VALUES: {ex2}")
            #         raise ex2
            self._generic_write_to_lake_ch(df, s3_key, C.STATION_ALARM, df.columns.tolist())
        except Exception as ex:
            logger.error(f"Error computing alarm daily report for {report_date}: {ex}")
            raise ex