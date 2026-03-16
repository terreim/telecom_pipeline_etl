"""
Provides utilities for reading/writing to ClickHouse using Airflow's ClickhouseHook.
"""

import json
import logging
import pandas as pd

from shared.common.config import CFG
from shared.common.connections import get_s3_credentials, get_clickhouse_hook

logger = logging.getLogger(__name__)

class ClickHouseIO:
    def __init__(
        self, 
        s3_conn_id: str = CFG.s3_conn_id, 
        ch_conn_id: str = CFG.clickhouse_conn_id,
    ):
        self.ch_hook = get_clickhouse_hook(conn_id=ch_conn_id)
        self.s3_access_key, self.s3_secret_key, self.s3_endpoint = get_s3_credentials(conn_id=s3_conn_id)

    def execute_query(self, query: str):
        """Executes a raw SQL query against ClickHouse."""
        self.ch_hook.execute(query)
    
    def insert_s3(self, url: str, ch_table: str):
        """Inserts into a ClickHouse table through S3 URL."""
        self.ch_hook.execute(f"""
                INSERT INTO {CFG.schema_name}.{ch_table}
                SELECT * FROM s3('{url}', '{self.s3_access_key}', '{self.s3_secret_key}', 'Parquet')
                SETTINGS use_hive_partitioning=0
            """)
        
    def insert_ch(self, df: pd.DataFrame, ch_table: str, columns: list[str]):
        """Inserts a DataFrame into a ClickHouse table."""
        self.ch_hook.execute(
            f"INSERT INTO {CFG.schema_name}.{ch_table} ({', '.join(columns)}) VALUES",
            params=[tuple(row) for _, row in df.iterrows()]
        )

    def insert_with_fallback(
        self,
        df: pd.DataFrame,
        s3_url: str,
        ch_table: str,
    ) -> None:
        """INSERT via S3 function, falling back to VALUES if S3 insert fails.

        Args:
            df:        DataFrame to insert (used only for the VALUES fallback).
            s3_url:    Full S3 URL already written by the caller (used for primary path).
            ch_table:  ClickHouse table name (without schema prefix).
        """
        try:
            self.insert_s3(url=s3_url, ch_table=ch_table)
        except Exception as ex1:
            logger.warning(f"S3 insert into {ch_table} failed ({ex1}), falling back to VALUES insert")
            try:
                self.insert_ch(df=df, ch_table=ch_table, columns=df.columns.tolist())
            except Exception as ex2:
                logger.error(f"VALUES insert into {ch_table} also failed: {ex2}")
                raise ex2

    # def delete_gold_hour(self, y: int, m: int, d: int, h: int) -> None:
        #     """Delete gold CH data for a single hour (health + anomaly + health_daily MV)."""
        #     hour_start = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:00:00"
        #     hour_end   = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:59:59"
        #     report_date = f"{y:04d}-{m:02d}-{d:02d}"

        #     for table in [CFG.station_health, CFG.station_anomaly]:
        #         self.ch_hook.execute(f"""
        #             ALTER TABLE {CFG.schema_name}.{table}
        #             DELETE WHERE hour_start >= '{hour_start}'
        #                     AND hour_start <= '{hour_end}'
        #             SETTINGS mutations_sync = 1
        #         """)

        #     # health_daily MV target — delete the full date so it can be
        #     # re-aggregated from the surviving + new health_hourly rows.
        #     self.ch_hook.execute(f"""
        #         ALTER TABLE {CFG.schema_name}.health_daily
        #         DELETE WHERE report_date = '{report_date}'
        #         SETTINGS mutations_sync = 1
        #     """)

        # def delete_gold_day(self, y: int, m: int, d: int) -> None:
        #     """Delete gold CH data for a single day (all daily tables)."""
        #     report_date = f"{y:04d}-{m:02d}-{d:02d}"

        #     daily_tables = {
        #         CFG.station_slac:        "report_date",
        #         CFG.station_outage:      "toDate(incident_start)",
        #         CFG.station_region:      "report_date",
        #         CFG.station_maintenance: "toDate(maintenance_start)",
        #         CFG.station_handover:    "report_date",
        #         CFG.station_alarm:       "report_date",
        #     }
        #     for table, date_col in daily_tables.items():
        #         self.ch_hook.execute(f"""
        #             ALTER TABLE {CFG.schema_name}.{table}
        #             DELETE WHERE {date_col} = '{report_date}'
        #             SETTINGS mutations_sync = 1
        #         """)

        # # ---- bulk range helpers (kept for non-recovery use) ----

        # def clear_gold_hourly_range(self, hours: list[tuple[int, int, int, int]]):
        #     """Delete gold hourly data (health, anomaly, health_daily MV) for a
        #     range of hours.  Uses ``mutations_sync=1`` so the deletes complete
        #     before we start re-inserting.
        #     """
        #     if not hours:
        #         return

        #     min_ts, max_ts = min(hours), max(hours)
        #     ts_start = f"{min_ts[0]:04d}-{min_ts[1]:02d}-{min_ts[2]:02d} {min_ts[3]:02d}:00:00"
        #     ts_end   = f"{max_ts[0]:04d}-{max_ts[1]:02d}-{max_ts[2]:02d} {max_ts[3]:02d}:59:59"

        #     for table in [CFG.station_health, CFG.station_anomaly]:
        #         self.ch_hook.execute(f"""
        #             ALTER TABLE {CFG.schema_name}.{table}
        #             DELETE WHERE hour_start >= '{ts_start}'
        #                     AND hour_start <= '{ts_end}'
        #             SETTINGS mutations_sync = 1
        #         """)
        #         logger.info(f"Cleared {CFG.schema_name}.{table}  [{ts_start} → {ts_end}]")

        #     # health_daily is a MV target (AggregatingMergeTree).
        #     # It does NOT auto-clean when the source (health_hourly) is deleted,
        #     # so we must explicitly clear the affected dates.
        #     dates = sorted(set((h[0], h[1], h[2]) for h in hours))
        #     d_start = f"{dates[0][0]:04d}-{dates[0][1]:02d}-{dates[0][2]:02d}"
        #     d_end   = f"{dates[-1][0]:04d}-{dates[-1][1]:02d}-{dates[-1][2]:02d}"
        #     self.ch_hook.execute(f"""
        #         ALTER TABLE {CFG.schema_name}.health_daily
        #         DELETE WHERE report_date >= '{d_start}'
        #                  AND report_date <= '{d_end}'
        #         SETTINGS mutations_sync = 1
        #     """)
        #     logger.info(f"Cleared {CFG.schema_name}.health_daily  [{d_start} → {d_end}]")

        # def clear_gold_daily_range(self, days: list[tuple[int, int, int]]):
        #     """Delete gold daily data (SLA, outage, region, maintenance, handover,
        #     alarm) for a range of days.
        #     """
        #     if not days:
        #         return

        #     d_start = f"{min(days)[0]:04d}-{min(days)[1]:02d}-{min(days)[2]:02d}"
        #     d_end   = f"{max(days)[0]:04d}-{max(days)[1]:02d}-{max(days)[2]:02d}"

        #     daily_tables = {
        #         CFG.station_slac:        "report_date",
        #         CFG.station_outage:      "toDate(incident_start)",
        #         CFG.station_region:      "report_date",
        #         CFG.station_maintenance: "toDate(maintenance_start)",
        #         CFG.station_handover:    "report_date",
        #         CFG.station_alarm:       "report_date",
        #     }

        #     for table, date_col in daily_tables.items():
        #         self.ch_hook.execute(f"""
        #             ALTER TABLE {CFG.schema_name}.{table}
        #             DELETE WHERE {date_col} >= '{d_start}'
        #                      AND {date_col} <= '{d_end}'
        #             SETTINGS mutations_sync = 1
        #         """)
        #         logger.info(f"Cleared {CFG.schema_name}.{table}  [{d_start} → {d_end}]")
