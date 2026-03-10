import pandas as pd

import logging
from typing import Optional

from shared.common.s3 import S3IO
from shared.common.config import CFG
from shared.common.connections import get_s3_hook, get_postgres_hook
from shared.common.sql_builder import sql_dim_station
from shared.common.validators import validate_traffic, validate_events, validate_metrics

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import re

logger = logging.getLogger(__name__)

class SilverTransformer:
    def __init__(
        self,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
        bronze_prefix: str = CFG.bronze_prefix,
        silver_prefix: str = CFG.silver_prefix,
        quarantine_prefix: str = CFG.quarantine_prefix,
        postgres_conn_id: Optional[str] = None,
    ):
        self.s3_hook = get_s3_hook(conn_id=s3_conn_id)
        self.s3_io = S3IO(s3_conn_id, s3_bucket)
        self.bucket = s3_bucket
        self.bronze_prefix = bronze_prefix
        self.silver_prefix = silver_prefix
        self.quarantine_prefix = quarantine_prefix
        self.pg_hook = get_postgres_hook(conn_id=postgres_conn_id) if postgres_conn_id else None
        self._station_dim: Optional[pd.DataFrame] = None

    # =========================================================================
    # Load Dimensions
    # =========================================================================

    def _load_station_dimension(self) -> pd.DataFrame:
        if self._station_dim is not None:
            return self._station_dim
        
        if not self.pg_hook:
            raise ValueError("Postgres connection is required to load station dimension.")

        try:
            self._station_dim = self.pg_hook.get_pandas_df(sql=sql_dim_station())
            logger.info(f"Loaded {len(self._station_dim)} station dimension records")
            return self._station_dim
        
        except Exception as e:
            logger.error(f"Failed to load station dimension: {e}")
            raise

    # =========================================================================
    # Data Quality Validation
    # =========================================================================

    def _validate_traffic(self, df: pd.DataFrame) -> pd.DataFrame: return validate_traffic(df)
    def _validate_metrics(self, df: pd.DataFrame) -> pd.DataFrame: return validate_metrics(df)
    def _validate_events(self, df: pd.DataFrame) -> pd.DataFrame:  return validate_events(df)

    # =========================================================================
    # Transformations
    # =========================================================================

    def _enrich_with_dimensions(self, df: pd.DataFrame) -> pd.DataFrame:
        station_dim = self._load_station_dimension()
        
        df = df.merge(station_dim, on='station_id', how='left', indicator='dim_match_status')
        df['dim_match_status'] = df['dim_match_status'].map({'both': 'matched', 'left_only': 'station_missing'})
        
        missing_count = (df['dim_match_status'] == 'station_missing').sum()
        if missing_count > 0:
            logger.warning(f"{missing_count} records missing station dimension match")
        
        return df

    def _add_derived_columns_traffic(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df['bytes_total'] = df['bytes_up'].fillna(0) + df['bytes_down'].fillna(0)
        df['event_date'] = df['event_time'].dt.date
        df['event_hour'] = df['event_time'].dt.floor('h')
        df['is_high_latency'] = df['latency_ms'] > 100
        df['transformed_at'] = pd.Timestamp.now('UTC')
        return df
    
    def _add_derived_columns_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['transformed_at'] = pd.Timestamp.now('UTC')
        return df

    def _add_derived_columns_events(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['transformed_at'] = pd.Timestamp.now('UTC')
        return df   

    # =========================================================================
    # S3 Operations
    # =========================================================================

    # def _list_bronze_files(self, table: str, year: int, month: int, day: int, hour: int) -> list:
    #     prefix = f"{self.bronze_prefix}/{table}/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"

    #     logger.info(f"Listing Bronze files with prefix: {prefix}")
    #     return self.s3_io.list_parquet_keys(prefix)
    
    # def _read_parquet_from_s3(self, s3_key: str) -> pd.DataFrame:
    #     return self.s3_io.read_parquet(s3_key)

    # def _read_parquet_chunks(self, s3_key: str, chunk_size: int = 50_000):
    #     """Yield DataFrames of at most *chunk_size* rows from a bronze file."""
    #     return self.s3_io.read_parquet_chunked(s3_key, chunk_size)

    # def _write_parquet_to_s3(self, df: pd.DataFrame, s3_key: str) -> str:
    #     return self.s3_io.write_parquet(df, s3_key)

    # def _cleanup_old_batch_files(
    #     self,
    #     silver_subpath: str,
    #     partition: str,
    #     current_batch_id: str,
    #     exclude_keys: list[str] | None = None,
    # ) -> list[str]:
    #     """Delete silver files from previous batches in a partition.

    #     After a successful streaming write, old files from earlier batch_ids
    #     are no longer needed.  Files belonging to *current_batch_id* and any
    #     explicitly listed *exclude_keys* are kept.
    #     Returns list of deleted keys.
    #     """
    #     prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"
    #     existing = self.s3_io.list_parquet_keys(prefix)
    #     exclude_set = set(exclude_keys or [])

    #     old_files = [
    #         k for k in existing
    #         if current_batch_id not in k and k not in exclude_set
    #     ]

    #     for key in old_files:
    #         self.s3_hook.delete_objects(bucket=self.bucket, keys=[key])
    #         logger.info(f"Cleaned up old batch file: {key}")

    #     return old_files
    
    # =========================================================================
    # Helper Methods
    # =========================================================================

    def find_unprocessed_hours(
        self, bronze_table: str, silver_subpath: str, lookback_hours: int = 24
    ) -> list[tuple[int, int, int, int]]:
        
        now_utc = pd.Timestamp.now("UTC")
        start = now_utc.floor("h")

        unprocessed = []
        for i in range(lookback_hours):
            ts = start - pd.Timedelta(hours=i)
            partition = (
                f"year={ts.year:04d}/month={ts.month:02d}/"
                f"day={ts.day:02d}/hour={ts.hour:02d}/"
            )

            bronze_files = self.s3_io.list_parquet_keys(
                f"{self.bronze_prefix}/{bronze_table}/{partition}"
            )
            if not bronze_files:
                continue

            silver_prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"
            if self.s3_io.has_success_marker(silver_prefix):
                # Silver transform done.  Only skip if every silver file
                # has a .ch_loaded marker (= fully delivered to ClickHouse).
                silver_files = self.s3_io.list_parquet_keys(silver_prefix)
                all_keys = (
                    self.s3_hook.list_keys(
                        bucket_name=self.bucket, prefix=silver_prefix
                    ) or []
                )
                ch_loaded_count = sum(1 for k in all_keys if '.ch_loaded_' in k)
                if ch_loaded_count >= len(silver_files) and silver_files:
                    continue  # fully processed AND loaded
                # else: needs re-signaling — fall through

            unprocessed.append((ts.year, ts.month, ts.day, ts.hour))

        return sorted(unprocessed)

    # =========================================================================
    # Main Transform Methods
    # =========================================================================

    def _transform_generic(
        self,
        table_name: str,
        silver_subpath: str,
        validate_fn,
        enrich: bool,
        add_derived_fn,
        year: int,
        month: int,
        day: int,
        hour: int,
        batch_id: str,
        manual_run: bool = False,
    ) -> dict:
        """Generic transform pipeline.

        Processes each bronze file **individually** — read, validate, enrich,
        and write to silver immediately — so that memory never holds more than
        one file's worth of data.  A ``_SUCCESS`` marker is written only after
        every bronze file in the hour has been processed, giving downstream
        consumers (catchup DAG, ``find_unprocessed_hours``) a reliable
        completion signal.

        On manual runs the marker is **skipped** so that the next scheduled
        run will still pick up any additional bronze files that arrive later.
        """

        time_desc = f"{year}-{month:02d}-{day:02d} hour={hour:02d}"
        logger.info(f"Transforming {table_name}: {time_desc}")
        bronze_files = self._list_bronze_files(table_name, year, month, day, hour)

        if not bronze_files:
            logger.info(f"No Bronze files found for {table_name}")
            return {"status": "skipped", "count": 0}

        partition = f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"

        # Fast-path: if _SUCCESS exists the silver data is already written.
        # Return the existing keys so the signal task can re-fire for the
        # staging DAG without re-doing the expensive transform work.
        marker_prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"
        if self.s3_io.has_success_marker(marker_prefix):
            existing_keys = self.s3_io.list_parquet_keys(marker_prefix)
            logger.info(
                f"Hour {time_desc} already processed — "
                f"returning {len(existing_keys)} existing silver keys for re-signaling"
            )
            return {
                "status": "success",
                "batch_id": batch_id,
                "total_records": 0,
                "valid_records": 0,
                "invalid_records": 0,
                "silver_keys": existing_keys,
                "quarantine_keys": [],
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
            }

        total_records = 0
        total_valid = 0
        total_invalid = 0
        written_silver_keys: list[str] = []
        written_quarantine_keys: list[str] = []

        part_counter = 0  # monotonic counter across files & chunks

        for idx, file_key in enumerate(bronze_files):
            for chunk_idx, df in enumerate(self._read_parquet_chunks(file_key)):
                logger.info(
                    f"Read {len(df)} records from {file_key} "
                    f"(chunk {chunk_idx})"
                )

                # Transform
                df = validate_fn(df)

                if enrich:
                    df = self._enrich_with_dimensions(df)

                if add_derived_fn:
                    df = add_derived_fn(df)

                valid_df = df[df['is_valid']].copy()
                invalid_df = df[~df['is_valid']].copy()

                total_records += len(df)
                total_valid += len(valid_df)
                total_invalid += len(invalid_df)

                del df

                # ---- write immediately per chunk (no accumulation) ----
                if len(valid_df) > 0:
                    silver_key = (
                        f"{self.silver_prefix}/{silver_subpath}/{partition}"
                        f"{batch_id}_p{part_counter}.parquet"
                    )
                    self._write_parquet_to_s3(valid_df, silver_key)
                    written_silver_keys.append(silver_key)
                del valid_df

                if len(invalid_df) > 0:
                    quarantine_key = (
                        f"{self.quarantine_prefix}/{silver_subpath}/{partition}"
                        f"{batch_id}_p{part_counter}.parquet"
                    )
                    self._write_parquet_to_s3(invalid_df, quarantine_key)
                    written_quarantine_keys.append(quarantine_key)
                    logger.warning(
                        f"Quarantined {len(invalid_df)} invalid records "
                        f"from file {idx} chunk {chunk_idx}"
                    )
                del invalid_df

                part_counter += 1

        logger.info(
            f"Processed {len(bronze_files)} files: "
            f"{total_records} total, {total_valid} valid, {total_invalid} invalid"
        )

        # Write _SUCCESS marker only after ALL files have been processed.
        # If OOM kills the task midway, the marker is absent and the hour
        # will be retried on the next run.
        # Skip on manual runs — more bronze data may still arrive for this hour.
        if not manual_run:
            marker_prefix = f"{self.silver_prefix}/{silver_subpath}/{partition}"
            self.s3_io.write_success_marker(marker_prefix)
        else:
            logger.info(f"Manual run — skipping _SUCCESS marker for {partition}")

        # Clean up silver files from previous batches
        cleaned = self._cleanup_old_batch_files(
            silver_subpath, partition, batch_id, exclude_keys=written_silver_keys
        )
        if cleaned:
            logger.info(f"Removed {len(cleaned)} old batch file(s) in {partition}")

        return {
            "status": "success",
            "batch_id": batch_id,
            "total_records": total_records,
            "valid_records": total_valid,
            "invalid_records": total_invalid,
            "silver_keys": written_silver_keys,
            "quarantine_keys": written_quarantine_keys,
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
        }

    def transform_traffic(self, table_name: str, year: int, month: int, day: int, hour: int, batch_id: str, manual_run: bool = False) -> dict:
        return self._transform_generic(
            table_name=table_name,
            silver_subpath=CFG.STATION_CLEANED_ST,
            validate_fn=self._validate_traffic,
            enrich=True,
            add_derived_fn=self._add_derived_columns_traffic,
            year=year,
            month=month,
            day=day,
            hour=hour,
            batch_id=batch_id,
            manual_run=manual_run,
        )

    def transform_metrics(self, table_name: str, year: int, month: int, day: int, hour: int, batch_id: str, manual_run: bool = False) -> dict:
        return self._transform_generic(
            table_name=table_name,
            silver_subpath=CFG.STATION_CLEANED_PM,
            validate_fn=self._validate_metrics,
            enrich=True,
            add_derived_fn=self._add_derived_columns_metrics,
            year=year,
            month=month,
            day=day,
            hour=hour,
            batch_id=batch_id,
            manual_run=manual_run,
        )

    def transform_events(self, table_name: str, year: int, month: int, day: int, hour: int, batch_id: str, manual_run: bool = False) -> dict:
        return self._transform_generic(
            table_name=table_name,
            silver_subpath=CFG.STATION_CLEANED_SE,
            validate_fn=self._validate_events,
            enrich=True,
            add_derived_fn=self._add_derived_columns_events,
            year=year,
            month=month,
            day=day,
            hour=hour,
            batch_id=batch_id,
            manual_run=manual_run,
        )
