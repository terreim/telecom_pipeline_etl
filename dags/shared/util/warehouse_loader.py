import logging
from typing import Optional
import pandas as pd

from shared.common.config import CFG
from shared.common.s3 import S3IO

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

class ClickHouseLoader:
    def __init__(
        self,
        ch_conn_id: str = CFG.clickhouse_conn_id,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
        silver_prefix: str = CFG.silver_prefix,
    ):
        self.ch_hook = ClickHouseHook(clickhouse_conn_id=ch_conn_id)
        self.s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        self.s3_bucket = s3_bucket

        creds = self.s3_hook.get_credentials()
        self.s3_access_key = creds.access_key
        self.s3_secret_key = creds.secret_key
        
        conn = self.s3_hook.get_connection(s3_conn_id)
        self.s3_endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

        self.silver_prefix = silver_prefix

    def _create_s3_key(self, prefix: str, year: int, month: int, day: int, hour: int) -> str:
        return f"{prefix}/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data.parquet"

    # =========================================================================
    # Dimension loading with change detection
    # =========================================================================  
  
    def _has_dim_changes(self, schema: str, table_name: str, dim_df: pd.DataFrame) -> bool:
        """Check if dimension data has changed compared to existing table."""
        if dim_df is None or dim_df.empty:
            return False
        
        existing_count = self.ch_hook.execute(f"SELECT count() FROM {schema}.{table_name} FINAL")
        if existing_count[0][0] != len(dim_df):
            return True
        
        # Compare checksums of key columns
        new_checksum = hash(tuple(sorted(dim_df['station_id'].tolist())))
        existing_ids = self.ch_hook.execute(f"SELECT station_id FROM {schema}.{table_name} FINAL")
        existing_checksum = hash(tuple(sorted([r[0] for r in existing_ids])))
        
        return new_checksum != existing_checksum
        
    def load_dim_dict(self, schema, table_name, dict_name) -> dict:
        self.ch_hook.execute(f"""
            CREATE DICTIONARY IF NOT EXISTS {schema}.{dict_name}
            (
                `station_id` UInt32,
                `station_code` String,
                `operator_code` String,
                `operator_name` String,
                `province` String,
                `district` String,
                `region` String,
                `density` String,
                `technology` String
            )
            PRIMARY KEY station_id
            SOURCE(CLICKHOUSE(TABLE '{table_name}' DB '{schema}'))
            LIFETIME(MIN 300 MAX 600)
            LAYOUT(FLAT());
            """)
        return {"status": "success"}

    def load_dim(self, schema, table_name, dict_name, dim_df: pd.DataFrame, force_reload: bool = False) -> dict:
        """Load dimension data into ClickHouse and refresh the dictionary.
        
        Args:
            dim_df: Denormalized station dimension DataFrame 
                    (station_id, station_code, operator_code, etc.)
        """
        # Skip if no changes detected and not forced
        if not force_reload and not self._has_dim_changes(schema, table_name, dim_df):
            logger.info(f"No dimension changes detected, skipping load for {table_name}")
            return {"status": "skipped", "table": table_name, "reason": "no_changes"}
        
        records = dim_df.to_dict(orient='records')
        current_timestamp = pd.Timestamp.now(tz='UTC')
        records = [(r['station_id'], r['station_code'], r['operator_code'], r['operator_name'], 
                    r['province'], r['district'], r['region'], r['density_class'], r['technology'], current_timestamp) 
                   for r in records]

        self.ch_hook.execute(f"""
                INSERT INTO {schema}.{table_name} 
                    (station_id, station_code, operator_code, operator_name, 
                    province, district, region, density_class, technology, updated_at)
                VALUES""", params=records)
        
        self.ch_hook.execute(f"OPTIMIZE TABLE {schema}.{table_name} FINAL")
        count = self.ch_hook.execute(f"SELECT count() FROM {schema}.{table_name} FINAL")
        logger.info(f"Loaded {count} dimension records")

        self.load_dim_dict(schema, table_name, dict_name)
        
        return {"status": "success", "table": table_name, "count": count[0][0]}
    
    # =========================================================================
    # Loaded marker helpers  (S3 sidecar .ch_loaded files)
    # =========================================================================

    def _loaded_marker_key(self, s3_key: str) -> str:
        """Derive a marker key from a silver parquet key.
        
        e.g. silver/traffic_cleaned/.../full_c60_batch.parquet
          -> silver/traffic_cleaned/.../.ch_loaded_full_c60_batch
        """
        parts = s3_key.rsplit('/', 1)
        filename = parts[-1].replace('.parquet', '')
        prefix = parts[0] if len(parts) > 1 else ''
        return f"{prefix}/.ch_loaded_{filename}"

    def _mark_as_loaded(self, s3_key: str) -> None:
        """Write a zero-byte marker after successful CH insert."""
        marker_key = self._loaded_marker_key(s3_key)
        self.s3_hook.load_string(
            string_data='',
            key=marker_key,
            bucket_name=self.s3_bucket,
            replace=True,
        )
        logger.info(f"Wrote CH loaded marker: {marker_key}")

    def _is_loaded(self, s3_key: str) -> bool:
        """Check if a silver file has already been loaded to CH."""
        marker_key = self._loaded_marker_key(s3_key)
        return self.s3_hook.check_for_key(key=marker_key, bucket_name=self.s3_bucket)

    # =========================================================================
    # Silver -> CH staging
    # =========================================================================

    def insert_silver_to_clickhouse(self, s3_key: str, staging_table: str) -> dict:
        self.ch_hook.execute(f"""
            INSERT INTO {CFG.schema_name}.{staging_table}
            SELECT * FROM s3(
                '{self.s3_endpoint}/{self.s3_bucket}/{s3_key}',
                '{self.s3_access_key}',
                '{self.s3_secret_key}',
                'Parquet'
            )
            SETTINGS use_hive_partitioning=0
        """)

        self._mark_as_loaded(s3_key)
        logger.info(f"Inserted data from {s3_key} into ClickHouse staging_{staging_table}")
        return {"status": "completed", "count": None, "s3_key": s3_key}

    def insert_silver_partition_batch(
        self,
        silver_subpath: str,
        staging_table: str,
        s3_keys: list[str],
    ) -> dict:
        """Insert multiple silver parquets in one INSERT using s3() glob.

        Groups *s3_keys* by their hour-partition prefix, issues one
        ``INSERT ... FROM s3('<prefix>/*.parquet')`` per partition, then
        writes ``.ch_loaded_*`` markers for every key.

        This creates far fewer MergeTree parts than individual INSERTs.
        """
        from collections import defaultdict

        # Group keys by partition prefix
        partition_keys: dict[str, list[str]] = defaultdict(list)
        for key in s3_keys:
            prefix = key.rsplit('/', 1)[0] + '/'
            partition_keys[prefix].append(key)

        total_inserted = 0
        for prefix, keys in partition_keys.items():
            glob_pattern = f"{prefix}*.parquet"
            self.ch_hook.execute(f"""
                INSERT INTO {CFG.schema_name}.{staging_table}
                SELECT * FROM s3(
                    '{self.s3_endpoint}/{self.s3_bucket}/{glob_pattern}',
                    '{self.s3_access_key}',
                    '{self.s3_secret_key}',
                    'Parquet'
                )
                SETTINGS use_hive_partitioning=0
            """)
            logger.info(
                f"Batch-inserted {len(keys)} files from {prefix} "
                f"into {CFG.schema_name}.{staging_table}"
            )

            # Mark each individual key so idempotency checks still work
            for key in keys:
                self._mark_as_loaded(key)

            total_inserted += len(keys)

        return {"partitions": len(partition_keys), "files": total_inserted}

    # =========================================================================
    # Gold catchup — find full_ silver files not yet loaded to CH
    # =========================================================================

    def find_unloaded_silver_keys(self, silver_subpath: str, lookback_hours: int = 24) -> list[str]:
        """Scan silver partitions for parquet files missing a .ch_loaded marker.

        Only considers partitions with a ``_SUCCESS`` marker, meaning the
        silver transform completed for that hour.  Returns every parquet file
        in those partitions that has not yet been loaded to ClickHouse.
        """
        s3_io = S3IO(self.s3_hook, self.s3_bucket)

        now_utc = pd.Timestamp.now('UTC')
        start = now_utc.floor('h')

        unloaded: list[str] = []
        for i in range(lookback_hours):
            ts = start - pd.Timedelta(hours=i)
            partition = (
                f"year={ts.year:04d}/month={ts.month:02d}/"
                f"day={ts.day:02d}/hour={ts.hour:02d}/"
            )
            prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"

            # Only consider complete partitions
            if not s3_io.has_success_marker(prefix):
                continue

            silver_files = s3_io.list_parquet_keys(prefix)

            for s3_key in silver_files:
                if not self._is_loaded(s3_key):
                    unloaded.append(s3_key)

        return sorted(unloaded)

    # =========================================================================
    # Recovery helpers — clear markers so tasks can re-run
    # =========================================================================

    def clear_ch_loaded_markers(
        self, silver_subpath: str, lookback_hours: int = 168
    ) -> dict:
        """Delete all ``.ch_loaded_*`` markers for *silver_subpath*.

        Use this when ClickHouse data is lost and you need the staging
        tasks to re-insert everything from silver parquets.
        """
        s3_io = S3IO(self.s3_hook, self.s3_bucket)
        now_utc = pd.Timestamp.now("UTC")
        start = now_utc.floor("h")

        deleted = 0
        for i in range(lookback_hours):
            ts = start - pd.Timedelta(hours=i)
            partition = (
                f"year={ts.year:04d}/month={ts.month:02d}/"
                f"day={ts.day:02d}/hour={ts.hour:02d}/"
            )
            prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"
            all_keys = self.s3_hook.list_keys(
                bucket_name=self.s3_bucket, prefix=prefix
            ) or []
            markers = [k for k in all_keys if ".ch_loaded_" in k]
            if markers:
                self.s3_hook.delete_objects(bucket=self.s3_bucket, keys=markers)
                deleted += len(markers)
                logger.info(
                    f"Deleted {len(markers)} .ch_loaded markers in {partition}"
                )

        logger.info(
            f"Cleared {deleted} .ch_loaded markers for {silver_subpath} "
            f"(lookback={lookback_hours}h)"
        )
        return {"silver_subpath": silver_subpath, "deleted": deleted}

    def clear_success_markers(
        self, silver_subpath: str, lookback_hours: int = 168
    ) -> dict:
        """Delete all ``_SUCCESS`` markers for *silver_subpath*.

        Use this when a bad ETL batch produced incorrect silver data and
        you need ``find_unprocessed_hours`` to re-transform from bronze.
        Also clears the corresponding ``.ch_loaded_*`` markers so the
        new silver output will be re-loaded to ClickHouse.
        """
        s3_io = S3IO(self.s3_hook, self.s3_bucket)
        now_utc = pd.Timestamp.now("UTC")
        start = now_utc.floor("h")

        success_deleted = 0
        ch_deleted = 0
        for i in range(lookback_hours):
            ts = start - pd.Timedelta(hours=i)
            partition = (
                f"year={ts.year:04d}/month={ts.month:02d}/"
                f"day={ts.day:02d}/hour={ts.hour:02d}/"
            )
            prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"

            if s3_io.delete_success_marker(prefix):
                success_deleted += 1

            # Also force-clear .ch_loaded so stale data is re-loaded after
            # the corrected silver files are produced.
            all_keys = self.s3_hook.list_keys(
                bucket_name=self.s3_bucket, prefix=prefix
            ) or []
            markers = [k for k in all_keys if ".ch_loaded_" in k]
            if markers:
                self.s3_hook.delete_objects(bucket=self.s3_bucket, keys=markers)
                ch_deleted += len(markers)

        logger.info(
            f"Cleared {success_deleted} _SUCCESS + {ch_deleted} .ch_loaded "
            f"markers for {silver_subpath} (lookback={lookback_hours}h)"
        )
        return {
            "silver_subpath": silver_subpath,
            "success_deleted": success_deleted,
            "ch_loaded_deleted": ch_deleted,
        }
    
    # def delete_and_reload_hour(self, year: int, month: int, day: int, hour: int) -> dict:
        
    #     # Current source tables : silver_traffic and traffic_wide
    #     start_time = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:00:00"
    #     end_time = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:59:59"
        
    #     self.ch_hook.execute(f"""
    #         ALTER TABLE telecom.traffic_wide
    #         DELETE WHERE event_hour = toStartOfHour(toDateTime('{start_time}'))
    #     """)

    #     self.ch_hook.execute(f"""
    #         ALTER TABLE telecom.silver_traffic
    #         DELETE WHERE event_time >= '{start_time}' AND event_time <= '{end_time}'
    #     """)

    #     #TODO: Uncomment when moving to replication
    #     # self.ch_hook.execute(f"""
    #     #     SYSTEM SYNC REPLICA telecom.traffic_wide
    #     # """)

    #     # self.ch_hook.execute(f"""
    #     #     SYSTEM SYNC REPLICA telecom.silver_traffic
    #     # """)

    #     logger.info(f"Deleted data for {start_time} - ready for reload")
        
    #     self.load_silver_traffic(year, month, day, hour)
    #     self.load_traffic_wide(schema="telecom", dict_name="dict_station", source_table_name="silver_traffic", target_table_name="traffic_wide", year=year, month=month, day=day, hour=hour)
        
    #     return {"status": "success", "action": "delete_and_reload"}
    
    # def rebuild_aggregate(
    #     self,
    #     year: int,
    #     month: int,
    #     day: int,
    #     hour: int,
    # ):
        
    #     # Current aggregate table: traffic_hourly, traffic_daily and operator_daily
    #     event_date = f"{year:04d}-{month:02d}-{day:02d}"

    #     self.ch_hook.execute(f"""
    #         ALTER TABLE telecom.traffic_hourly 
    #         DELETE WHERE toDate(event_hour) = '{event_date}'
    #     """)

    #     self.ch_hook.execute(f"""
    #         ALTER TABLE telecom.traffic_daily
    #         DELETE WHERE event_date = '{event_date}'
    #     """)

    #     self.ch_hook.execute(f"""
    #         ALTER TABLE telecom.operator_daily
    #         DELETE WHERE event_date = '{event_date}'
    #     """)

    #     self.ch_hook.execute(f"""
    #         INSERT INTO telecom.traffic_hourly
    #         SELECT
    #             event_hour,
    #             station_code,
    #             operator_code,
    #             region,
    #             province,
    #             technology,
    #             sumState(bytes_total) AS total_bytes,
    #             countState() AS session_count,
    #             uniqState(imsi_hash) AS unique_subscribers,
    #             avgState(latency_ms) AS avg_latency
    #         FROM telecom.gold_traffic_wide
    #         WHERE event_date = '{event_date}'
    #         GROUP BY event_hour, station_code, operator_code, region, province, technology
    #     """)

    #     self.ch_hook.execute(f"""
    #         INSERT INTO telecom.traffic_daily
    #         SELECT
    #             event_date,
    #             station_code,
    #             operator_code,
    #             region,
    #             province,
    #             technology,
    #             sumState(bytes_total) AS total_bytes,
    #             countState() AS session_count,
    #             uniqState(imsi_hash) AS unique_subscribers,
    #             avgState(latency_ms) AS avg_latency
    #         FROM telecom.gold_traffic_wide
    #         WHERE event_date = '{event_date}'
    #         GROUP BY event_date, station_code, operator_code, region, province, technology
    #     """)

    #     self.ch_hook.execute(f"""
    #         INSERT INTO telecom.gold_operator_daily
    #         SELECT
    #             event_date,
    #             operator_code,
    #             sumState(bytes_total) AS total_bytes,
    #             countState() AS session_count,
    #             uniqState(imsi_hash) AS unique_subscribers,
    #             uniqState(station_code) AS active_stations,
    #             avgState(latency_ms) AS avg_latency
    #         FROM telecom.gold_traffic_wide
    #         WHERE event_date = '{event_date}'
    #         GROUP BY event_date, operator_code
    #     """)

    #     logger.info(f"Rebuilt aggregates for {event_date}")
        
    #     return {"status": "success", "action": "rebuild_aggregates", "date": event_date}
