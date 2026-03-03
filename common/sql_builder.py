"""
Parameterized SQL templates for the telecom ETL pipeline.

This module provides functions that generate parameterized SQL query strings
for each layer of the data pipeline (Bronze, Silver, Gold) as well as
dimension lookups, recovery/catchup operations, and various Gold-layer
analytical reports.

Functions:

    **Dimension:**
    sql_dim_station() -> str
        Generates a SELECT query joining base station, operator, and location
        dimension tables to produce a unified station dimension view.
    
    **Bronze Layer:**
    sql_bronze_extractor(table_name, columns) -> str
        Generates a parameterized SELECT for incremental extraction from a
        source table, filtered by `updated_at` timestamps (expects %s placeholders).
    
    sql_silver_staging(table_name, s3_endpoint, s3_bucket, s3_key, s3_access_key, s3_secret_key) -> str
        Generates an INSERT ... SELECT from an S3-backed Parquet file into a
        ClickHouse staging table.
    
    **Recovery/Catchup:**
    sql_generic_clear_range(table_name, date_column, start, end) -> str
        Generates an ALTER TABLE DELETE for a date range (inclusive),
        used for recovery/backfill scenarios.
    
    sql_generic_clear_date(table_name, date_column, target_date) -> str
        Generates an ALTER TABLE DELETE for a single date, used for
        idempotent re-processing.
    
    **Gold Layer Analytical Reports:*
    sql_gold_health_hourly(year, month, day, hour) -> str
        Generates a complex analytical query that joins session traffic,
        performance metrics, and station events to produce an hourly
        health snapshot per station. Uses ClickHouse dictGet() for
        dimension enrichment.
    
    sql_gold_slac_hourly(year, month, day) -> str
        Generates a daily SLA compliance report per station, computing
        uptime percentage, SLA breach hours, and compliance status
        based on technology-specific targets.
    
    sql_gold_anomaly_features(year, month, day, hour) -> str
        Generates anomaly detection feature vectors by comparing current
        hourly metrics against a 7-day rolling baseline (mean and stddev).
    
    sql_gold_outage_report(year, month, day) -> str
        Generates a detailed outage/incident report by pairing incident_start
        and incident_end events, computing affected subscribers, traffic loss,
        and health scores during each incident window.
    
    sql_gold_region_report(year, month, day) -> str
        Generates a daily regional performance summary aggregating health
        scores, traffic volumes, and SLA compliance across region/province/
        operator/technology dimensions.
    
    sql_gold_handover_report(year, month, day) -> str
        Generates a daily handover analysis report showing handover counts
        between source and target stations, with latency comparisons.
    
    sql_gold_alarm_report(year, month, day) -> str
        Generates a daily alarm summary per station, broken down by severity,
        with alarm rate per active hour and correlation to health scores."""

from datetime import date, timedelta
from common.config import CFG

# ══════════════════════════════════════════════════════════════════════════════
# Dimension template
# ══════════════════════════════════════════════════════════════════════════════

def sql_dim_station() -> str:
    return f"""
        SELECT 
                bs.station_id,
                bs.station_code,
                op.operator_code, 
                op.operator_name, 
                lc.province, 
                lc.district,
                lc.region, 
                lc.density,
                bs.technology
            FROM {CFG.schema_name}.{CFG.station_bs} bs
            LEFT JOIN {CFG.schema_name}.{CFG.station_op} op ON bs.operator_id = op.operator_id
            LEFT JOIN {CFG.schema_name}.{CFG.station_lc} lc ON bs.location_id = lc.location_id
    """


# ══════════════════════════════════════════════════════════════════════════════
# Bronze layer template
# ══════════════════════════════════════════════════════════════════════════════

def sql_bronze_extractor(table_name: str, columns: list[str], overlap: int, buffer: int, pk_column: str) -> str:
    return f"""
        SELECT {', '.join(columns)}
        FROM {CFG.schema_name}.{table_name}
        WHERE updated_at > %(nominal_from)s - INTERVAL '{overlap}s'
            AND updated_at <= %(max_updated_at)s - INTERVAL '{buffer}s'
        ORDER BY updated_at ASC, {pk_column} ASC
        """

# ══════════════════════════════════════════════════════════════════════════════
# Silver staging templates
# ══════════════════════════════════════════════════════════════════════════════

def sql_silver_staging(table_name: str, s3_endpoint: str, s3_bucket: str, s3_key: str, s3_access_key: str, s3_secret_key: str) -> str:
    return f"""
        INSERT INTO {CFG.schema_name}.{table_name}
        SELECT * FROM s3(
            '{s3_endpoint}/{s3_bucket}/{s3_key}',
            '{s3_access_key}',
            '{s3_secret_key}',
            'Parquet'
        )
        SETTINGS use_hive_partitioning=0"""

# ══════════════════════════════════════════════════════════════════════════════
# Recovery and Catchup templates
# ══════════════════════════════════════════════════════════════════════════════

def sql_generic_clear_range(table_name: str, date_column: str, start: str, end: str) -> str:
    return f"""
        ALTER TABLE {CFG.schema_name}.{table_name}
        DELETE WHERE {date_column} >= '{start}'
                AND {date_column} <= '{end}'
        SETTINGS mutations_sync = 1
        """

def sql_generic_clear_date(table_name: str, date_column: str, target_date: str) -> str:
    return f"""
        ALTER TABLE {CFG.schema_name}.{table_name}
        DELETE WHERE {date_column} = '{target_date}'
        SETTINGS mutations_sync = 1
        """

# ══════════════════════════════════════════════════════════════════════════════
# Gold layer templates
# ══════════════════════════════════════════════════════════════════════════════

def sql_gold_health_hourly(year: int, month: int, day: int, hour: int) -> str:
    hour_start = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:00:00"
    hour_end = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:59:59"

    return f"""
        SELECT 
            t.station_id AS station_id,
            t.station_code AS station_code,
            toStartOfHour(t.event_time) AS hour_start,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'operator_code', t.station_id) AS operator_code,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'province', t.station_id) AS province,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'region', t.station_id) AS region,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'density', t.station_id) AS density,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'technology', t.station_id) AS technology,
                        
            count() AS session_count,
            uniq(t.imsi_hash) AS unique_subscribers,
            sum(t.bytes_up + t.bytes_down) AS total_bytes,
            avg(t.latency_ms) AS avg_latency_ms,
            quantile(0.95)(t.latency_ms) AS p95_latency_ms,
            avg(t.packet_loss_pct) AS avg_packet_loss_pct,
            countIf(t.latency_ms > 100) / count() AS high_latency_ratio,
                        
            m.avg_cpu AS avg_cpu_pct,
            m.max_cpu AS max_cpu_pct,
            m.avg_memory AS avg_memory_pct,
            m.avg_temp AS avg_temperature_c,
            m.max_temp AS max_temperature_c,
            m.avg_throughput AS avg_throughput_mbps,
            m.error_count AS error_count,
                        
            e.alarm_count,
            e.warning_count,
            e.critical_count,
            e.handover_count,
            e.incident_active,
            e.incident_type,
            e.maintenance_active
            FROM {CFG.schema_name}.{CFG.station_staging_st} t
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
                FROM {CFG.schema_name}.{CFG.station_staging_pm}
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
                FROM {CFG.schema_name}.{CFG.station_staging_se}
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
    """

def sql_gold_slac_hourly(year: int, month: int, day: int) -> str:
    report_date = f"{year:04d}-{month:02d}-{day:02d}"

    return f"""
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
            FROM {CFG.schema_name}.{CFG.station_health}
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
            FROM {CFG.schema_name}.{CFG.station_staging_se}
            WHERE toDate(event_time) = '{report_date}'
            GROUP BY station_id, event_date
        ) e ON sc.station_id = e.station_id AND e.event_date = sc.report_date
    """

def sql_gold_anomaly_features(year: int, month: int, day: int, hour: int) -> str:
    target_date = date(year, month, day)
    baseline_start = (target_date - timedelta(days=7)).isoformat()
    report_date = f"{year:04d}-{month:02d}-{day:02d}"

    return f"""
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
            FROM {CFG.schema_name}.{CFG.station_health} sh
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
            FROM {CFG.schema_name}.{CFG.station_health}
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
    """

def sql_gold_outage_report(year: int, month: int, day: int) -> str:
    target_date = date(year, month, day)
    report_date = target_date.isoformat()
    baseline_start = (target_date - timedelta(days=7)).isoformat()

    return f"""
        WITH 
        -- Step 1: Pair incident_start → incident_end using row_number
        -- (simulator guarantees no overlapping incidents per station)
        starts AS (
            SELECT
                station_id,
                event_time AS incident_start,
                metadata AS start_meta,
                row_number() OVER (PARTITION BY station_id ORDER BY event_time) AS rn
            FROM {CFG.schema_name}.{CFG.station_staging_se}
            WHERE event_type = 'incident_start'
                AND toDate(event_time) = '{report_date}'
        ),
        ends AS (
            SELECT
                station_id,
                event_time AS incident_end,
                metadata AS end_meta,
                row_number() OVER (PARTITION BY station_id ORDER BY event_time) AS rn
            FROM {CFG.schema_name}.{CFG.station_staging_se}
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
            LEFT JOIN {CFG.schema_name}.{CFG.station_staging_st} st
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
            LEFT JOIN {CFG.schema_name}.{CFG.station_health} h
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
            FROM {CFG.schema_name}.{CFG.station_staging_st}
            WHERE toDate(event_time) >= '{baseline_start}'
                AND toDate(event_time) < '{report_date}'
            GROUP BY station_id, hour_of_day
        )

        SELECT
            ip.station_id,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'station_code', ip.station_id) AS station_code,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'operator_code', ip.station_id) AS operator_code,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'province', ip.station_id) AS province,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'region', ip.station_id) AS region,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'technology', ip.station_id) AS technology,

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
    """

def sql_gold_region_report(year: int, month: int, day: int) -> str:
    report_date = f"{year:04d}-{month:02d}-{day:02d}"

    return f"""
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
            FROM {CFG.schema_name}.{CFG.station_health}
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
            FROM {CFG.schema_name}.{CFG.station_slac}
            WHERE report_date = '{report_date}'
            GROUP BY region, province, operator_code, technology
        ) sla ON ha.region = sla.region 
                AND ha.province = sla.province 
                AND ha.operator_code = sla.operator_code 
                AND ha.technology = sla.technology
    """

def sql_gold_handover_report(year: int, month: int, day: int) -> str:
    report_date = f"{year:04d}-{month:02d}-{day:02d}"

    return f"""
        SELECT
            toDate(e.event_time) AS report_date,
            e.station_id AS source_station_id,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'station_code', e.station_id) AS source_station_code,
            e.target_station_id AS target_station_id,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'station_code', e.target_station_id) AS target_station_code,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'region', e.station_id) AS source_region,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'region', e.target_station_id) AS target_region,
            count() AS handover_count,
            uniq(
                JSONExtractString(e.metadata::String, 'imsi_hash')
            ) AS unique_subscribers,
            src_traffic.avg_latency AS avg_latency_before_ms,
            tgt_traffic.avg_latency AS avg_latency_after_ms
        FROM {CFG.schema_name}.{CFG.station_staging_se} e
        LEFT JOIN (
            SELECT
                station_id,
                toDate(event_time) AS traffic_date,
                avg(latency_ms) AS avg_latency
            FROM {CFG.schema_name}.{CFG.station_staging_st}
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
            FROM {CFG.schema_name}.{CFG.station_staging_st}
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
    """

def sql_gold_alarm_report(year: int, month: int, day: int) -> str:
    report_date = f"{year:04d}-{month:02d}-{day:02d}"

    return f"""
        WITH alarm_agg AS (
            SELECT
                toDate(event_time) AS report_date,
                station_id,
                countIf(severity = 'warning') AS warning_count,
                countIf(severity = 'error') AS error_count,
                countIf(severity = 'critical') AS critical_count,
                count() AS total_alarm_count,
                topK(1)(description)[1] AS top_alarm_description
            FROM {CFG.schema_name}.{CFG.station_staging_se}
            WHERE event_type = 'alarm'
                AND toDate(event_time) = '{report_date}'
            GROUP BY report_date, station_id
        )
        SELECT
            a.report_date,
            a.station_id,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'station_code', a.station_id) AS station_code,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'operator_code', a.station_id) AS operator_code,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'region', a.station_id) AS region,
            dictGet('{CFG.schema_name}.{CFG.dim_dict}', 'technology', a.station_id) AS technology,
            a.warning_count,
            a.error_count,
            a.critical_count,
            a.total_alarm_count,
            if(h.active_hours > 0,
                (a.total_alarm_count / h.active_hours)::Float64,
                a.total_alarm_count::Float64
            ) AS alarm_rate_per_hour,
            a.top_alarm_description,
            h.avg_health_score AS health_score_avg
        FROM alarm_agg a
        LEFT JOIN (
            SELECT
                station_id,
                countIf(health_category IS NOT NULL) AS active_hours,
                avg(health_score)::Float64 AS avg_health_score
            FROM {CFG.schema_name}.{CFG.station_health}
            WHERE toDate(hour_start) = '{report_date}'
            GROUP BY station_id
        ) h ON a.station_id = h.station_id
    """