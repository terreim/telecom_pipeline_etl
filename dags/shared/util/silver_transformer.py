"""
String doc goes here.

SilverTransformer iterates over the metadata partition of each bronze tables, identifies unprocessed time windows, and transforms the data into silver layer.
    1. List metadata/watermark/bronze/{table}/*.json  (skip _latest.json)
    2. For each: is loaded_to_silver == False?
    3. Yes → read data_keys → download those parquets → transform → write to silver
    4. Mark loaded_to_silver = True on that metadata file
    5. Write the silver metadata file 
"""


import pandas as pd
from datetime import datetime
import time
import re

import logging
from typing import Optional

from shared.common.s3 import S3IO
from shared.common.config import CFG
from shared.common.connections import get_postgres_hook
from shared.common.sql_builder import sql_dim_station
from shared.common.metadata import MetadataManager
from shared.common.metadata_template import silver_metadata_template
from shared.common.failure_recovery import write_failure_metadata
from shared.common.watermark import S3WatermarkStore
from shared.common.validators import validate_traffic, validate_events, validate_metrics, validate_station_dimension

logger = logging.getLogger(__name__)

class SilverTransformer:
    def __init__(
        self,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
        layer: str = CFG.silver_prefix,
        postgres_conn_id: Optional[str] = None,
    ):
        self.s3_io = S3IO(s3_conn_id, s3_bucket)
        self.bucket = s3_bucket
        self.pg_hook = get_postgres_hook(conn_id=postgres_conn_id) if postgres_conn_id else None
        self._station_dim: Optional[pd.DataFrame] = None
        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.watermark_store = S3WatermarkStore(self.meta)
        self.layer = layer

        self.silver_prefix = CFG.silver_prefix
        self.quarantine_prefix = CFG.quarantine_prefix
        self.metadata_prefix = CFG.metadata_prefix

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
    def _validate_station_dimension(self, df: pd.DataFrame) -> pd.DataFrame: return validate_station_dimension(df)
    
    # =========================================================================
    # Transformations
    # =========================================================================

    def _enrich_with_dimensions(self, df: pd.DataFrame) -> pd.DataFrame:
        station_dim = self._load_station_dimension()
        
        df = df.merge(station_dim, on='station_id', how='left', indicator='dim_match_status')
        df = self._validate_station_dimension(df)
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
    # Helper Methods
    # =========================================================================

    def find_unprocessed_hours(
            self, 
            bronze_table: str,
            lookback_range: int | None = '',
        ) -> list[tuple[str, dict]]:

        metadata_keys = self.s3_io.list_json_keys(
            prefix=f"{self.metadata_prefix}/watermark/bronze/{bronze_table}/"
        )

        logger.info(f"Found {len(metadata_keys)} metadata files for bronze table {bronze_table}")

        unprocessed = []
        for key in metadata_keys[-lookback_range:]:
            if key.endswith('_latest.json'):
                continue
            metadata = self.s3_io.read_json(key)
            if metadata.get('loaded_to_silver', False):
                continue

            # Skip bronze batches that were skipped or have no data_keys —
            # mark them loaded_to_silver so they don't reappear on every scan.
            if metadata.get('status') == 'skipped' or not metadata.get('data_keys'):
                logger.info(f"Marking empty/skipped bronze batch as loaded_to_silver: {key}")
                metadata['loaded_to_silver'] = True
                self.meta.write_metadata(key=key, metadata_dict=metadata)
                continue

            unprocessed.append((key, metadata))

        logger.info(f"Identified {len(unprocessed)} unprocessed metadata entries for bronze table {bronze_table}")
        return unprocessed
    
    def _extract_partition(self, s3_key: str) -> str:
        match = re.search(r'(year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2}/)', s3_key)
        return match.group(1) if match else ""

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
        batch_id: str,
        lookback_range: int = 720,
    ) -> dict:
        """Generic transform pipeline.

        Processes each bronze file **individually** — read, validate, enrich,
        and write to silver immediately — so that memory never holds more than
        one file's worth of data. The bronze file is located by checking unprocessed metadata entries, which point to the bronze parquet keys to read.
        """
        all_results = []

        for meta_key, bronze_meta in self.find_unprocessed_hours(bronze_table=table_name, lookback_range=lookback_range):
            t0 = time.monotonic()
            silver_meta = silver_metadata_template()
            silver_meta['table'] = silver_subpath
            silver_meta['batch_id'] = batch_id
            
            total_records = 0
            total_valid = 0
            total_invalid = 0
            written_silver_keys: list[str] = []
            written_quarantine_keys: list[str] = []
            partition = ""
            
            for data_key in bronze_meta.get("data_keys", []):
                partition = self._extract_partition(data_key)
                logger.info(partition)

                try:
                    df = self.s3_io.read_parquet(data_key)
                except Exception as e:
                    failure_key = write_failure_metadata(
                        mm=self.meta,
                        metadata_prefix=self.metadata_prefix,
                        layer="silver",
                        table_name=table_name,
                        batch_id=batch_id,
                        stage="read_bronze",
                        source_key=data_key,
                        source_metadata_key=meta_key,
                        error=e,
                        s3_hook=self.s3_io.s3_hook,
                        s3_bucket=self.bucket,
                        subpath=silver_subpath,
                    )
                    logger.error(
                        f"Skipping unreadable bronze file {data_key}: {e}. "
                        f"Failure metadata written to {failure_key}"
                    )
                    continue
                logger.info(f"Read {len(df)} records from {data_key}")

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

                bronze_filename = data_key.rsplit('/', 1)[-1].replace('.parquet', '')

                if len(valid_df) > 0:
                    silver_key = (
                        f"{self.silver_prefix}/{silver_subpath}/{partition}{bronze_filename}.parquet"
                    )
                    self.s3_io.write_parquet(valid_df, silver_key)
                    written_silver_keys.append(silver_key)
                del valid_df

                if len(invalid_df) > 0:
                    q_key = (
                        f"{self.quarantine_prefix}/{silver_subpath}/{partition}{bronze_filename}.parquet"
                    )
                    self.s3_io.write_parquet(invalid_df, q_key)
                    written_quarantine_keys.append(q_key)
                    logger.warning(
                        f"Quarantined {len(invalid_df)} invalid records "
                    )
                del invalid_df
            
            silver_meta["status"] = "silver_complete"
            silver_meta["partition"] = partition # last partition processed
            silver_meta["source_bronze_keys"] = bronze_meta.get("data_keys", [])
            silver_meta["record_count"] = total_valid
            silver_meta["quarantine_count"] = total_invalid
            silver_meta["data_keys"] = written_silver_keys
            silver_meta["quarantine_keys"] = written_quarantine_keys
            silver_meta["created_at"] = datetime.utcnow().isoformat()
            silver_meta["processing_duration_seconds"] = round(time.monotonic() - t0, 2)

            self.meta.write_metadata(
                key=f"{self.metadata_prefix}/watermark/{self.layer}/{silver_subpath}/{batch_id}.json",
                metadata_dict=silver_meta,
            )   

            bronze_meta["loaded_to_silver"] = True
            self.meta.write_metadata(key=meta_key, metadata_dict=bronze_meta)

            all_results.append(silver_meta)

        return all_results

    def transform_traffic(self, table_name: str, silver_subpath: str, batch_id: str, lookback_range: int = 720) -> dict:
        return self._transform_generic(
            table_name=table_name,
            silver_subpath=silver_subpath,
            validate_fn=self._validate_traffic,
            enrich=True,
            add_derived_fn=self._add_derived_columns_traffic,
            batch_id=batch_id,
            lookback_range=lookback_range
        )

    def transform_metrics(self, table_name: str, silver_subpath: str, batch_id: str, lookback_range: int = 720) -> dict:
        return self._transform_generic(
            table_name=table_name,
            silver_subpath=silver_subpath,
            validate_fn=self._validate_metrics,
            enrich=True,
            add_derived_fn=self._add_derived_columns_metrics,
            batch_id=batch_id,
            lookback_range=lookback_range
        )

    def transform_events(self, table_name: str, silver_subpath: str, batch_id: str, lookback_range: int = 720) -> dict:
        return self._transform_generic(
            table_name=table_name,
            silver_subpath=silver_subpath,
            validate_fn=self._validate_events,
            enrich=True,
            add_derived_fn=self._add_derived_columns_events,
            batch_id=batch_id,
            lookback_range=lookback_range
        )
