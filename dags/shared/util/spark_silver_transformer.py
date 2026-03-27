"""
String doc goes here.

SilverTransformer iterates over the metadata partition of each bronze tables, identifies unprocessed time windows, and transforms the data into silver layer.
    1. List metadata/watermark/bronze/{table}/*.json  (skip _latest.json)
    2. For each: is loaded_to_silver == False?
    3. Yes → read data_keys → download those parquets → transform → write to silver
    4. Mark loaded_to_silver = True on that metadata file
    5. Write the silver metadata file 
"""

from datetime import datetime
import time
import re
from uuid import uuid4

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    to_date,
    date_trunc,
    current_timestamp,
)
from py4j.protocol import Py4JJavaError, Py4JNetworkError

from shared.common.config import CFG
from shared.common.spark import get_spark_session
from shared.common.connections import get_postgres_hook
from shared.common.sql_builder import sql_dim_station
from shared.common.metadata import MetadataManager
from shared.common.metadata_template import silver_metadata_template
from shared.common.failure_recovery import write_failure_metadata
from shared.common.watermark import S3WatermarkStore
from shared.common.spark_validators import validate_traffic, validate_events, validate_metrics, validate_station_dimension

logger = logging.getLogger(__name__)

class SparkSilverTransformer:
    def __init__(
        self,
        spark: SparkSession | None = None,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
        layer: str = CFG.silver_prefix,
        postgres_conn_id: Optional[str] = None
    ):
        self.spark = spark or get_spark_session("silver_transformer")
        self.s3_bucket = s3_bucket
        self.pg_hook = get_postgres_hook(conn_id=postgres_conn_id) if postgres_conn_id else None
        self._station_dim: Optional[DataFrame] = None
        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.watermark_store = S3WatermarkStore(self.meta)
        self.layer = layer

        self.silver_prefix = CFG.silver_prefix
        self.quarantine_prefix = CFG.quarantine_prefix
        self.metadata_prefix = CFG.metadata_prefix

    # =========================================================================
    # Load Dimensions
    # =========================================================================

    def _load_station_dimension(self) -> Optional[DataFrame]:
        if self._station_dim is not None:
            return self._station_dim
        
        if not self.pg_hook:
            raise ValueError("Postgres connection is required to load station dimension.")

        try:
            pandas_df = self.pg_hook.get_pandas_df(sql=sql_dim_station())
            self._station_dim = self.spark.createDataFrame(pandas_df)
            logger.info(f"Loaded station dimension records")
            return self._station_dim
        
        except Exception as e:
            logger.error(f"Failed to load station dimension: {e}")
            raise

    # =========================================================================
    # Data Quality Validation
    # =========================================================================

    def _validate_traffic(self, df: DataFrame) -> DataFrame: return validate_traffic(df)
    def _validate_metrics(self, df: DataFrame) -> DataFrame: return validate_metrics(df)
    def _validate_events(self, df: DataFrame) -> DataFrame:  return validate_events(df)
    def _validate_station_dimension(self, df: DataFrame) -> DataFrame: return validate_station_dimension(df)
    
    # =========================================================================
    # Transformations
    # =========================================================================

    def _enrich_with_dimensions(self, df: DataFrame) -> DataFrame:
        station_dim = self._load_station_dimension()

        df = (
            df
            .join(station_dim, on='station_id', how='left')
            .withColumn(
                'dim_match_status',
                when(col('station_id').isNotNull(), lit('matched')).otherwise(lit('station_missing'))
            )
        )

        is_missing = df.filter(col('dim_match_status') == 'station_missing')
        if is_missing.rdd.isEmpty() is False:
            logger.warning(f"Data contains records missing station dimension match")

        return df
    
    def _add_derived_columns_traffic(self, df: DataFrame) -> DataFrame:
        df = (
            df
            .withColumn('bytes_total', coalesce(col('bytes_up'), lit(0)) + coalesce(col('bytes_down'), lit(0)))
            .withColumn('event_date', to_date(col('event_time')))
            .withColumn('event_hour', date_trunc('hour', col('event_time')))
            .withColumn('is_high_latency', col('latency_ms') > 100)
            .withColumn('transformed_at', current_timestamp())
        )
        return df

    def _add_derived_columns_metrics(self, df: DataFrame) -> DataFrame:
      df = df.withColumn('transformed_at', current_timestamp())
      return df

    def _add_derived_columns_events(self, df: DataFrame) -> DataFrame:
        df = df.withColumn('transformed_at', current_timestamp())
        return df

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def find_unprocessed_bronze(
            self,
            bronze_table: str,
            lookback_range: int | None = None,
        ) -> list[tuple[str, dict]]:
        """Scan bronze metadata for json files with unmarked `loaded_to_silver`."""
        prefix = f"{self.metadata_prefix}/watermark/bronze/{bronze_table}/"
        candidates = self.meta.find_unprocessed(
            prefix=prefix,
            flag="loaded_to_silver",
            lookback=lookback_range,
        )

        # Skip bronze batches that were skipped or have no data_keys —
        # mark them loaded_to_silver so they don't reappear on every scan.
        unprocessed = []
        for key, metadata in candidates:
            if metadata.get('status') == 'skipped' or not metadata.get('data_keys'):
                logger.info(f"Marking empty/skipped bronze batch as loaded_to_silver: {key}")
                self.meta.mark_loaded(key, metadata, "loaded_to_silver")
                continue
            unprocessed.append((key, metadata))

        logger.info(f"Identified {len(unprocessed)} unprocessed bronze entries for {bronze_table}")
        return unprocessed
    
    def _extract_partition(self, s3_key: str) -> str:
        match = re.search(r'(year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2}/)', s3_key)
        return match.group(1) if match else ""

    def _write_single_parquet(self, df: DataFrame, output_key: str) -> None:
        """Write Spark DataFrame as a single parquet object at the exact S3 key."""
        temp_key = f"{output_key}.__tmp__{uuid4().hex}/"
        temp_uri = f"s3a://{self.s3_bucket}/{temp_key}"
        output_uri = f"s3a://{self.s3_bucket}/{output_key}"

        # Write one-part parquet to a temp prefix, then promote part file to final key.
        df.coalesce(1).write.mode('overwrite').parquet(temp_uri)

        jvm = self.spark.sparkContext._jvm
        jsc = self.spark.sparkContext._jsc
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(temp_uri), jsc.hadoopConfiguration())
        temp_path = jvm.org.apache.hadoop.fs.Path(temp_uri)
        output_path = jvm.org.apache.hadoop.fs.Path(output_uri)

        try:
            part_path = None
            for status in fs.listStatus(temp_path):
                name = status.getPath().getName()
                if name.startswith('part-') and name.endswith('.parquet'):
                    part_path = status.getPath()
                    break

            if part_path is None:
                raise RuntimeError(f"No parquet part file found in temp path: {temp_uri}")

            if fs.exists(output_path):
                fs.delete(output_path, True)

            if not fs.rename(part_path, output_path):
                raise RuntimeError(f"Failed to move parquet part into final key: {output_uri}")
        finally:
            fs.delete(temp_path, True)

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
    ) -> list[dict]:
        """Generic transform pipeline.

        Processes each bronze file **individually** — read, validate, enrich,
        and write to silver immediately — so that memory never holds more than
        one file's worth of data. The bronze file is located by checking unprocessed metadata entries, which point to the bronze parquet keys to read.
        """
        try:
            all_results = []
            for meta_key, bronze_meta in self.find_unprocessed_bronze(bronze_table=table_name, lookback_range=lookback_range):
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
                
                data_keys = bronze_meta.get("data_keys", [])
                if not data_keys:
                    continue

                # Extract partition from first key
                partition = self._extract_partition(data_keys[0])
                logger.info(f"Processing partition: {partition} with {len(data_keys)} files")

                try:
                    # Read all bronze files at once
                    paths = [f"s3a://{self.s3_bucket}/{k}" for k in data_keys]
                    df = self.spark.read.parquet(*paths)
                except Py4JNetworkError:
                    raise
                except Exception as e:
                    failure_key = write_failure_metadata(
                        mm=self.meta,
                        metadata_prefix=self.metadata_prefix,
                        layer="silver",
                        table_name=table_name,
                        batch_id=batch_id,
                        stage="read_bronze",
                        source_key=",".join(data_keys),
                        source_metadata_key=meta_key,
                        error=e,
                        s3_hook=None,
                        s3_bucket=self.s3_bucket,
                        subpath=silver_subpath,
                    )
                    logger.error(
                        f"Skipping unreadable bronze batch with {len(data_keys)} files: {e}. "
                        f"Failure metadata written to {failure_key}"
                    )
                    continue

                df = validate_fn(df)
                if enrich:
                    df = self._enrich_with_dimensions(df)
                if add_derived_fn:
                    df = add_derived_fn(df)

                valid_df = df.filter(col('is_valid') == True)
                invalid_df = df.filter(col('is_valid') == False)

                total_valid = valid_df.count()
                total_invalid = invalid_df.count()
                total_records = total_valid + total_invalid

                if total_valid > 0:
                    silver_key = f"{self.silver_prefix}/{silver_subpath}/{partition}batch.parquet"
                    self._write_single_parquet(valid_df, silver_key)
                    written_silver_keys.append(silver_key)

                if total_invalid > 0:
                    q_key = f"{self.quarantine_prefix}/{silver_subpath}/{partition}batch.parquet"
                    self._write_single_parquet(invalid_df, q_key)
                    written_quarantine_keys.append(q_key)
                    logger.warning(f"Quarantined {total_invalid} invalid records from batch")

                silver_meta["status"] = "silver_complete"
                silver_meta["partition"] = partition
                silver_meta["source_bronze_keys"] = bronze_meta.get("data_keys", [])
                silver_meta["record_count"] = total_valid
                silver_meta["quarantine_count"] = total_invalid
                silver_meta["data_keys"] = written_silver_keys
                silver_meta["quarantine_keys"] = written_quarantine_keys
                silver_meta["created_at"] = datetime.utcnow().isoformat()
                silver_meta["processing_duration_seconds"] = round(time.monotonic() - t0, 2)

                self.meta.write_metadata(
                    key=f"{self.metadata_prefix}/watermark/{self.layer}/{silver_subpath}/{partition.replace('/', '_')}.json",
                    metadata_dict=silver_meta,
                )   

                self.meta.mark_loaded(meta_key, bronze_meta, "loaded_to_silver")

                all_results.append(silver_meta)

            return all_results
        except (Py4JNetworkError, ConnectionRefusedError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error during transformation: {e}")
            raise

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