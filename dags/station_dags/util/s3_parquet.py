import os
import tempfile
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

class S3ParquetIO:
    def __init__(self, s3_hook: S3Hook, bucket: str):
        self.s3_hook = s3_hook
        self.bucket = bucket

    def ensure_bucket(self):
        if not self.s3_hook.check_for_bucket(self.bucket):
            logger.info(f"Creating bucket: {self.bucket}")
            self.s3_hook.create_bucket(bucket_name=self.bucket)

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
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
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

    def list_parquet_keys(self, prefix: str) -> list[str]:
        keys = self.s3_hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []
        return [k for k in keys if k.endswith('.parquet')]

    def write_success_marker(self, prefix: str) -> str:
        """Write a _SUCCESS marker file (Spark/Hive convention) to indicate
        that all files in a partition have been written successfully."""
        marker_key = f"{prefix}_SUCCESS"
        self.s3_hook.load_string(
            string_data='',
            key=marker_key,
            bucket_name=self.bucket,
            replace=True,
        )
        logger.info(f"Wrote _SUCCESS marker: {marker_key}")
        return marker_key

    def has_success_marker(self, prefix: str) -> bool:
        """Check if a _SUCCESS marker exists under the given prefix."""
        marker_key = f"{prefix}_SUCCESS"
        return self.s3_hook.check_for_key(key=marker_key, bucket_name=self.bucket)

    def delete_success_marker(self, prefix: str) -> bool:
        """Delete the _SUCCESS marker if it exists. Returns True if deleted."""
        marker_key = f"{prefix}_SUCCESS"
        if self.s3_hook.check_for_key(key=marker_key, bucket_name=self.bucket):
            self.s3_hook.delete_objects(bucket=self.bucket, keys=[marker_key])
            logger.info(f"Deleted _SUCCESS marker: {marker_key}")
            return True
        return False