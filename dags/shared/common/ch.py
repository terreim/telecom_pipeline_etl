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
        return self.ch_hook.execute(query)
    
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

    def delete_gold_hour(self, y: int, m: int, d: int, h: int) -> None:
        """Delete gold CH data for a single hour (health + anomaly + health_daily MV)."""
        hour_start = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:00:00"
        hour_end   = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:59:59"
        report_date = f"{y:04d}-{m:02d}-{d:02d}"

        for table in [CFG.station_health, CFG.station_anomaly]:
            self.ch_hook.execute(f"""
                ALTER TABLE {CFG.schema_name}.{table}
                DELETE WHERE hour_start >= '{hour_start}'
                        AND hour_start <= '{hour_end}'
                SETTINGS mutations_sync = 1
            """)

        # health_daily MV target — delete the full date so it can be
        # re-aggregated from the surviving + new health_hourly rows.
        self.ch_hook.execute(f"""
            ALTER TABLE {CFG.schema_name}.health_daily
            DELETE WHERE report_date = '{report_date}'
            SETTINGS mutations_sync = 1
        """)
        logger.info(f"Deleted gold hour {y:04d}-{m:02d}-{d:02d} {h:02d}:xx from CH")

    def delete_gold_day(self, y: int, m: int, d: int) -> None:
        """Delete gold CH data for a single day (all daily tables)."""
        report_date = f"{y:04d}-{m:02d}-{d:02d}"

        daily_tables = {
            CFG.station_slac:        "report_date",
            CFG.station_outage:      "toDate(incident_start)",
            CFG.station_region:      "report_date",
            CFG.station_maintenance: "toDate(maintenance_start)",
            CFG.station_handover:    "report_date",
            CFG.station_alarm:       "report_date",
        }
        for table, date_col in daily_tables.items():
            self.ch_hook.execute(f"""
                ALTER TABLE {CFG.schema_name}.{table}
                DELETE WHERE {date_col} = '{report_date}'
                SETTINGS mutations_sync = 1
            """)
        logger.info(f"Deleted gold day {report_date} from CH")
