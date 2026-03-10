"""
Provides utilities for reading/writing Parquet files to S3 using Airflow's S3Hook.
"""

import os
import tempfile
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from shared.common.config import CFG
from shared.common.connections import get_s3_hook, get_s3_credentials


logger = logging.getLogger(__name__)

class S3IO:
    def __init__(self, conn_id: str, bucket: str):
        self.s3_hook = get_s3_hook(conn_id=conn_id)
        self.access_key, self.secret_key, self.endpoint = get_s3_credentials(conn_id=conn_id)
        self.bucket = bucket

    def ensure_bucket(self):
        if not self.s3_hook.check_for_bucket(self.bucket):
            logger.info(f"Creating bucket: {self.bucket}")
            self.s3_hook.create_bucket(bucket_name=self.bucket)

    def create_key(self, **kwargs) -> str:
        """Helper to create a full S3 key from prefix and filename, accepting various parameters.
        
        Expects arguments:

            prefix: str - the S3 prefix (e.g. "bronze/subscriber_traffic")
            filename: str - the filename (e.g. "xxx.parquet")
            cutoff_time: datetime - the cutoff time for partitioning
            zone: str - the zone (e.g. "bronze", "silver", "gold")
            table: str - the table name (e.g. "station_st")
            type: str - the type of key to generate ("full", "url", "key", "watermark")
        """
        prefix = kwargs.get("prefix", "")
        filename = kwargs.get("filename", "")
        cutoff_time = kwargs.get("cutoff_time", "")
        type = kwargs.get("type", "")

        if type == "full":
            return f"{self.endpoint}/{self.bucket}/{prefix}/{filename}"
        
        elif type == "url":
            return f"{self.endpoint}/{self.bucket}/{prefix}/year={cutoff_time.year:04d}/month={cutoff_time.month:02d}/day={cutoff_time.day:02d}/hour={cutoff_time.hour:02d}/{filename}"
        
        elif type == "key":
            return f"{prefix}/year={cutoff_time.year:04d}/month={cutoff_time.month:02d}/day={cutoff_time.day:02d}/hour={cutoff_time.hour:02d}/{filename}"
        
    def list_parquet_keys(self, prefix: str) -> list[str]:
        keys = self.s3_hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []
        return [k for k in keys if k.endswith('.parquet')]
    
    def upload_parquet(self, tmp_path: str, s3_key: str):
        self.s3_hook.load_file(
            filename=tmp_path, key=s3_key,
            bucket_name=self.bucket, replace=True,
        )
        logger.info(f"Uploaded parquet to s3://{self.bucket}/{s3_key}")

    def read_parquet(self, s3_key: str) -> pd.DataFrame:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = self.s3_hook.download_file(
                key=s3_key, bucket_name=self.bucket, local_path=tmp_dir
            )
            return pd.read_parquet(local_path)

    def read_parquet_chunked(
        self, s3_key: str, chunk_size: int = 30_000
    ):
        """Yield DataFrames of at most *chunk_size* rows from a parquet file.

        Uses PyArrow's ``iter_batches`` so that only one chunk lives in
        memory at a time.  The downloaded temp file is kept alive for the
        entire iteration.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = self.s3_hook.download_file(
                key=s3_key, bucket_name=self.bucket, local_path=tmp_dir
            )
            pf = pq.ParquetFile(local_path)
            for batch in pf.iter_batches(batch_size=chunk_size):
                yield batch.to_pandas()

    def write_parquet(self, df: pd.DataFrame, s3_key: str) -> str:
        with tempfile.NamedTemporaryFile(dir=CFG.temp_dir, suffix=".parquet", delete=False) as tmp_file:
            tmp_path = tmp_file.name
        try:
            table_pa = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table_pa, tmp_path, compression="SNAPPY")
            self.s3_hook.load_file(
                filename=tmp_path, key=s3_key,
                bucket_name=self.bucket, replace=True,
            )
            logger.info(f"Wrote {len(df)} rows to s3://{self.bucket}/{s3_key}")
            return s3_key
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # def write_success_marker(self, prefix: str) -> str:
    #     """Write a _SUCCESS marker file (Spark/Hive convention) to indicate
    #     that all files in a partition have been written successfully."""
    #     marker_key = f"{prefix}_SUCCESS"
    #     self.s3_hook.load_string(
    #         string_data='',
    #         key=marker_key,
    #         bucket_name=self.bucket,
    #         replace=True,
    #     )
    #     logger.info(f"Wrote _SUCCESS marker: {marker_key}")
    #     return marker_key

    # def has_success_marker(self, prefix: str) -> bool:
    #     """Check if a _SUCCESS marker exists under the given prefix."""
    #     marker_key = f"{prefix}_SUCCESS"
    #     return self.s3_hook.check_for_key(key=marker_key, bucket_name=self.bucket)

    # def delete_success_marker(self, prefix: str) -> bool:
    #     """Delete the _SUCCESS marker if it exists. Returns True if deleted."""
    #     marker_key = f"{prefix}_SUCCESS"
    #     if self.s3_hook.check_for_key(key=marker_key, bucket_name=self.bucket):
    #         self.s3_hook.delete_objects(bucket=self.bucket, keys=[marker_key])
    #         logger.info(f"Deleted _SUCCESS marker: {marker_key}")
    #         return True
    #     return False