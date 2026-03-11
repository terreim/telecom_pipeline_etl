"""
Provides utilities for reading/writing Parquet files to S3 using Airflow's S3Hook.
"""

import json
import os
import tempfile
import logging
import shutil
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
        
    # =========================================================================
    # Parquet
    # =========================================================================

    def list_parquet_keys(self, prefix: str) -> list[str]:
        keys = self.s3_hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []
        return [k for k in keys if k.endswith('.parquet')]
    
    def upload_parquet(self, tmp_path: str, s3_key: str):
        if os.path.getsize(tmp_path) == 0:
            raise ValueError(f"Refusing to upload 0-byte parquet to {s3_key}")
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
            logger.debug(f"download_file returned path: {local_path}, size: {os.path.getsize(local_path)}, dir contents: {os.listdir(tmp_dir)}")
            if os.path.getsize(local_path) == 0:
                raise ValueError(f"Downloaded parquet is 0 bytes: s3://{self.bucket}/{s3_key} -> {local_path}")
            return pd.read_parquet(local_path)

    def read_parquet_chunked(
        self, s3_key: str, chunk_size: int = 30_000
    ):
        """Yield DataFrames of at most *chunk_size* rows from a parquet file.

        Uses PyArrow's ``iter_batches`` so that only one chunk lives in
        memory at a time.  The downloaded temp file is kept alive for the
        entire iteration.
        TODO: verify if tmp_dir is being deleted before caller can call for the next one, make tmp_dir outlives the generator.
        """
        tmp_dir = tempfile.mkdtemp()
        try:
            local_path = self.s3_hook.download_file(
                key=s3_key, bucket_name=self.bucket, local_path=tmp_dir
            )
            pf = pq.ParquetFile(local_path)
            for batch in pf.iter_batches(batch_size=chunk_size):
                yield batch.to_pandas()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def write_parquet(self, df: pd.DataFrame, s3_key: str) -> str:
        with tempfile.NamedTemporaryFile(dir=CFG.temp_dir, suffix=".parquet", delete=False) as tmp_file:
            tmp_path = tmp_file.name
        try:
            table_pa = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table_pa, tmp_path, compression="SNAPPY")
            if os.path.getsize(tmp_path) == 0:
                raise ValueError(f"Parquet serialized to 0 bytes for key {s3_key} — df shape: {df.shape}, dtypes: {df.dtypes.to_dict()}")
            self.s3_hook.load_file(
                filename=tmp_path, key=s3_key,
                bucket_name=self.bucket, replace=True,
            )
            logger.info(f"Wrote {len(df)} rows to s3://{self.bucket}/{s3_key}")
            return s3_key
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # =========================================================================
    # Json Metadata
    # =========================================================================

    def list_json_keys(self, prefix: str) -> list[str]:
        keys = self.s3_hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []
        return [k for k in keys if k.endswith('.json')]
    
    def read_json(self, s3_key: str) -> dict:
        obj = self.s3_hook.get_key(key=s3_key, bucket_name=self.bucket)
        return json.loads(obj.get()['Body'].read())