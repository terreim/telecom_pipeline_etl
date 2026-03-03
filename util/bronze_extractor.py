import os
import tempfile
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from contextlib import contextmanager

import logging

from util.s3_parquet import S3ParquetIO
from config.config import PipelineConfig as C

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

@contextmanager
def _get_cursor(pg_hook):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        yield conn, cursor
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

class BronzeExtractor:
    def __init__(
        self, 
        postgres_conn_id: str = C.POSTGRES_CONN_ID,
        s3_conn_id: str = C.S3_CONN_ID, 
        s3_bucket: str = C.S3_BUCKET, 
        s3_prefix_base: str = C.BRONZE_PREFIX
    ):
        self.pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        self.s3_io = S3ParquetIO(self.s3_hook, s3_bucket)
        self.bucket = s3_bucket
        self.prefix_base = s3_prefix_base

    def _build_s3_key(self, table: str, cutoff_time, batch_id: str) -> str:
        return (
            f"{self.prefix_base}/"
            f"{table}/"
            f"year={cutoff_time.year:04d}/"
            f"month={cutoff_time.month:02d}/"
            f"day={cutoff_time.day:02d}/"
            f"hour={cutoff_time.hour:02d}/"
            f"batch_id={batch_id}.parquet"
        )
    
    def _ensure_bucket(self):
        self.s3_io.ensure_bucket()

    def _unify_schema(self, pyarrow_schema: pa.Schema) -> pa.Schema:
        """Normalize decimal precisions to max(38,s) so chunks never conflict."""
        fields = []
        for field in pyarrow_schema:
            if pa.types.is_decimal(field.type):
                # Widen to max precision, keep original scale
                fields.append(pa.field(field.name, pa.decimal128(38, field.type.scale), nullable=True))
            else:
                fields.append(pa.field(field.name, field.type, nullable=True))
        return pa.schema(fields)
            
    def _serialize_jsonb_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Serialize any dict/JSONB columns to JSON strings."""
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
                )
        return df

    def el(
        self,
        schema: str,
        table: str,
        pk_column: str,
        target_columns: list,
        batch_id: str,
        cutoff_time,
        batch_limit: int = 70000,
        chunk_size: int = 2000,
        time_column: str | None = None,
    ) -> dict | list[dict]:
        """Extract-Load from Postgres to S3 as Parquet.

        When *time_column* is provided, records are partitioned by their
        actual event/metric hour instead of the extraction *cutoff_time*.
        This produces correct bronze partitions for backfilled data.
        Returns a **list** of result dicts (one per hour) when
        *time_column* is set; a single dict otherwise.
        """
        
        cols_formatted = ", ".join([f"t.{col}" for col in target_columns])

        sql = f"""
            WITH batch AS (
                SELECT {pk_column}
                FROM {schema}.{table}
                WHERE extracted_at IS NULL and ingested_at < %s
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {schema}.{table} t
            SET extracted_at = NOW(), batch_id = %s
            FROM batch b
            WHERE t.{pk_column} = b.{pk_column}
            RETURNING {cols_formatted}
        """

        logger.info(f"Extracting {schema}.{table} | batch_id={batch_id} | cutoff={cutoff_time}")

        with _get_cursor(self.pg_hook) as (conn, cursor):
            try:
                cursor.execute(sql, (cutoff_time, batch_limit, batch_id))

                if not cursor.description:
                    conn.commit()
                    logger.info(f"No data to extract from {schema}.{table}")
                    result = {"batch_id": batch_id, "count": 0, "status": "skipped"}
                    return [result] if time_column else result
                
                columns = [desc[0] for desc in cursor.description]

                if time_column:
                    return self._el_partitioned(
                        conn, cursor, columns, table, batch_id,
                        cutoff_time, chunk_size, time_column,
                    )
                else:
                    return self._el_single(
                        conn, cursor, columns, table, batch_id,
                        cutoff_time, chunk_size,
                    )
            except Exception as e:
                logger.error(f"Error during extraction process: {e}")
                raise

    # -----------------------------------------------------------------
    # Single-file path (original behaviour, cutoff_time partitioning)
    # -----------------------------------------------------------------
    def _el_single(
        self, conn, cursor, columns, table, batch_id, cutoff_time, chunk_size,
    ) -> dict:
        total_rows = 0

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            writer = None
            parquet_schema = None

            while True:
                records = cursor.fetchmany(chunk_size)
                if not records:
                    break

                df = pd.DataFrame(records, columns=columns)
                df = self._serialize_jsonb_columns(df)

                table_pa = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    parquet_schema = self._unify_schema(table_pa.schema)
                    writer = pq.ParquetWriter(tmp_path, parquet_schema, compression="SNAPPY")

                table_pa = table_pa.cast(parquet_schema)
                writer.write_table(table_pa)
                total_rows += len(records)

                del df, records, table_pa
            
            if writer:
                writer.close()
            
            if total_rows == 0:
                conn.commit()
                logger.info(f"No records extracted from {table}.")
                return {"batch_id": batch_id, "count": 0, "status": "skipped"}
            
            s3_key = self._build_s3_key(table, cutoff_time, batch_id)
            self._ensure_bucket()

            self.s3_hook.load_file(
                filename=tmp_path,
                key=s3_key,
                bucket_name=self.bucket,
                replace=True,
            )

            conn.commit()
            logger.info(f"Success: {total_rows} rows → s3://{self.bucket}/{s3_key}")
            
            return {"batch_id": batch_id, "count": total_rows, "status": "success", "s3_key": s3_key}
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed extracting {table}: {e}")
            raise
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # -----------------------------------------------------------------
    # Per-hour path (event-time partitioning for correct backfill)
    # -----------------------------------------------------------------
    def _el_partitioned(
        self, conn, cursor, columns, table, batch_id,
        cutoff_time, chunk_size, time_column,
    ) -> list[dict]:
        """Stream records from the cursor into per-hour Parquet writers,
        then upload each temp file to S3 under the correct hour partition.

        Records whose *time_column* is NULL or unparseable are placed in
        the *cutoff_time* partition as a fallback.
        """
        writers: dict[tuple, dict] = {}   # (y,m,d,h) → {writer, tmp_path, schema, count}
        tmp_paths: list[str] = []

        try:
            while True:
                records = cursor.fetchmany(chunk_size)
                if not records:
                    break

                df = pd.DataFrame(records, columns=columns)
                df = self._serialize_jsonb_columns(df)

                # Determine partition hour from the time column
                ts = pd.to_datetime(df[time_column], errors='coerce')
                partition_hour = ts.dt.floor('h')

                # Fallback for NaT: use cutoff_time's hour
                fallback = pd.Timestamp(cutoff_time).floor('h')
                partition_hour = partition_hour.fillna(fallback)

                for hour_ts, group_df in df.groupby(partition_hour):
                    key = (hour_ts.year, hour_ts.month, hour_ts.day, hour_ts.hour)

                    # Drop the partition helper if it leaked in (it shouldn't)
                    group_df = group_df.drop(columns=[c for c in group_df.columns if c.startswith('_partition')], errors='ignore')

                    table_pa = pa.Table.from_pandas(group_df, preserve_index=False)

                    if key not in writers:
                        tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
                        tmp_path = tmp_file.name
                        tmp_file.close()
                        tmp_paths.append(tmp_path)
                        parquet_schema = self._unify_schema(table_pa.schema)
                        writers[key] = {
                            'writer': pq.ParquetWriter(tmp_path, parquet_schema, compression="SNAPPY"),
                            'tmp_path': tmp_path,
                            'schema': parquet_schema,
                            'count': 0,
                        }

                    entry = writers[key]
                    table_pa = table_pa.cast(entry['schema'])
                    entry['writer'].write_table(table_pa)
                    entry['count'] += len(group_df)

                del df, records

            # Close all writers
            for entry in writers.values():
                entry['writer'].close()

            if not writers:
                conn.commit()
                logger.info(f"No records extracted from {table}.")
                return [{"batch_id": batch_id, "count": 0, "status": "skipped"}]

            # Upload each hour partition
            self._ensure_bucket()
            results: list[dict] = []

            for (y, m, d, h), entry in writers.items():
                partition_ts = pd.Timestamp(year=y, month=m, day=d, hour=h)
                s3_key = self._build_s3_key(table, partition_ts, batch_id)

                self.s3_hook.load_file(
                    filename=entry['tmp_path'],
                    key=s3_key,
                    bucket_name=self.bucket,
                    replace=True,
                )

                logger.info(
                    f"Success: {entry['count']} rows "
                    f"→ s3://{self.bucket}/{s3_key}"
                )
                results.append({
                    "batch_id": batch_id,
                    "count": entry['count'],
                    "status": "success",
                    "s3_key": s3_key,
                })

            conn.commit()
            total = sum(r['count'] for r in results)
            logger.info(f"Partitioned {total} rows across {len(results)} hour(s)")
            return results

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed extracting {table}: {e}")
            raise
        finally:
            for path in tmp_paths:
                if os.path.exists(path):
                    os.unlink(path)