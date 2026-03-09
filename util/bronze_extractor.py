import os
import tempfile
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass, field

import logging

from common.s3 import S3IO
from common.metadata_template import metadata_template
from common.config import CFG
from common.schema import unify_schema, serialize_jsonb_columns
from common.connections import get_postgres_hook, pg_cursor
from common.metadata import MetadataManager
from common.watermark import S3WatermarkStore
from common.sql_builder import sql_bronze_extractor

logger = logging.getLogger(__name__)

@dataclass
class DelayAccumulator:
    max_updated_at: datetime | None = None
    max_propagation_s: float = 0.0      # max(created_at - event_time)
    max_clock_offset_s: float = 0.0     # max(updated_at - created_at)
    propagation_values: list = field(default_factory=list)  # for p99
    clock_offset_values: list = field(default_factory=list) # for p99
    late_arrival_count: int = 0         # created_at - event_time > threshold
    update_count: int = 0              # updated_at != created_at (beyond threshold)

class BronzeExtractor:
    def __init__(
        self, 
        postgres_conn_id: str = CFG.postgres_conn_id,
        s3_conn_id: str = CFG.s3_conn_id, 
        s3_bucket: str = CFG.s3_bucket, 
        zone: str = CFG.bronze_prefix
    ):
        self.pg_hook = get_postgres_hook(postgres_conn_id=postgres_conn_id)
        self.s3_io = S3IO(conn_id=s3_conn_id, bucket=s3_bucket)
        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.watermark_store = S3WatermarkStore(self.meta)
        self.bucket = s3_bucket
        self.zone = zone

    def _get_initial_watermark(self, schema: str, table: str) -> datetime | None:
        sql = f"SELECT min(updated_at) FROM {schema}.{table}"
        with pg_cursor(self.pg_hook) as (conn, cur):
            cur.execute(sql)
            row = cur.fetchone()
            conn.commit()
            return row[0] if row and row[0] else None

    def _accumulate_delays(
            self, 
            acc: DelayAccumulator, 
            df: pd.DataFrame,
            time_column: str | None, 
            buffer_seconds: int) -> None:
        
        if 'updated_at' not in df.columns or 'created_at' not in df.columns:
            return
        
        ua = pd.to_datetime(df['updated_at'])
        ca = pd.to_datetime(df['created_at'])

        chunk_max = ua.max()
        if acc.max_updated_at is None or chunk_max > acc.max_updated_at:
            acc.max_updated_at = chunk_max

        if time_column and time_column in df.columns:
            et = pd.to_datetime(df[time_column], errors='coerce')
            prop = (ca - et).dt.total_seconds()
            acc.max_propagation_s = max(acc.max_propagation_s, prop.max())
            acc.propagation_values.extend(prop.dropna().tolist())
            acc.late_arrival_count += int((prop > buffer_seconds).sum())
        
        offset = (ua - ca).dt.total_seconds()
        acc.max_clock_offset_s = max(acc.max_clock_offset_s, offset.max())
        acc.clock_offset_values.extend(offset.dropna().tolist())
        acc.update_count += int((offset > 1.0).sum())

    def _finalize_delays(self, acc: DelayAccumulator) -> dict:
        return {
            "actual_max_updated_at": acc.max_updated_at.isoformat() if acc.max_updated_at else None,
            "max_station_propagation_seconds": round(acc.max_propagation_s, 1),
            "max_clock_offset_seconds": round(acc.max_clock_offset_s, 1),
            "p99_station_propagation_seconds": round(
                pd.Series(acc.propagation_values).quantile(0.99), 1
            ) if acc.propagation_values else 0.0,
            "p99_clock_offset_seconds": round(
                pd.Series(acc.clock_offset_values).quantile(0.99), 1
            ) if acc.clock_offset_values else 0.0,
            "late_arrival_count": acc.late_arrival_count,
            "update_count": acc.update_count,
        }

    def extract_bronze(
            self,
            table: str,
            schema: str,
            pk_column: str,
            target_columns: list,
            batch_id: str,
            time_column: str
        ):
        """
        Iteratively extract bronze data from Postgres to S3, advancing the
        watermark each iteration until we've caught up to "now - buffer".
        """
        windows_processed = 0
        initial_wm = self._get_initial_watermark(schema, table)

        while True:
            meta = metadata_template()
            meta |= {"layer": "bronze", "table": table, "batch_id": batch_id, "dedup_key": [pk_column]}

            fr, n_fr, to, first, prev_bid = self.watermark_store.get_watermark(
                zone=self.zone,
                table=table,
                elt_now=datetime.utcnow(),
                buffer_seconds=CFG.buffer_seconds,
                overlap_seconds=CFG.overlap_seconds,
                initial_watermark=initial_wm,
            )

            if fr >= datetime.utcnow() - timedelta(seconds=CFG.buffer_seconds):
                logger.info(f"Watermark caught up for {schema}.{table}, stopping.")
                break

            meta["previous_batch_id"] = prev_bid
            meta["source_watermark"] = self.watermark_store.build_watermark(fr, n_fr, to, CFG.buffer_seconds, 0 if first else CFG.overlap_seconds)

            el_result = self.el(
                schema=schema,
                table=table,
                pk_column=pk_column,
                target_columns=target_columns,
                batch_id=batch_id,
                fr=fr,
                to=to,
                time_column=time_column,
            )

            meta |= el_result 
            meta["created_at"] = datetime.utcnow().isoformat()
            self.watermark_store.set_watermark(
                zone=self.zone,
                table=table,
                metadata=meta,
            )

            windows_processed += 1

            if el_result.get("status") == "skipped":
                break
        
        return {"status": "backfill_complete" if windows_processed > 1 else "complete", 
                    "windows_processed": windows_processed}

    def el(
        self,
        schema: str,
        table: str,
        pk_column: str,
        target_columns: list,
        batch_id: str,
        fr,
        to,
        batch_limit: int = 70000,
        chunk_size: int = 2000,
        time_column: str | None = None,
    ) -> dict | list[dict]:
        """Extract-Load from Postgres to S3 as Parquet.

        If *time_column* is provided, it will be used for event-time partitioning
        into hourly Parquet files. Records with NULL/unparseable *time_column* will
        be placed in the cutoff_time's hour partition as a fallback. If not provided,
        all records will be placed in a single Parquet file.
        """

        sql = sql_bronze_extractor(
            table=table,
            columns=target_columns,
            overlap=CFG.overlap_seconds,
            buffer=CFG.buffer_seconds,
            pk_column=pk_column,
        )

        with pg_cursor(self.pg_hook) as (conn, cursor):
            try:
                cursor.execute(sql, {"nominal_from": fr, "max_updated_at": to})

                if not cursor.description:
                    conn.commit()
                    logger.info(f"No data to extract from {schema}.{table}")
                    result = {"batch_id": batch_id, "record_count": 0, "status": "skipped"}
                    return [result] if time_column else result
                
                columns = [desc[0] for desc in cursor.description]

                if time_column:
                    return self._el_partitioned(
                        conn, cursor, columns, table, batch_id,
                        fr, chunk_size, time_column,
                    )
                else:
                    return self._el_single(
                        conn, cursor, columns, table, batch_id,
                        fr, chunk_size,
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
        acc = DelayAccumulator()

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
                
                self._accumulate_delays(acc, df, None, CFG.buffer_seconds)
                
                df = serialize_jsonb_columns(df)
                
                table_pa = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    parquet_schema = unify_schema(table_pa.schema)
                    writer = pq.ParquetWriter(tmp_path, parquet_schema, compression="SNAPPY")

                table_pa = table_pa.cast(parquet_schema)
                writer.write_table(table_pa)
                total_rows += len(records)

                del df, records, table_pa
            
            observed = self._finalize_delays(acc)

            if writer:
                writer.close()
            
            if total_rows == 0:
                conn.commit()
                logger.info(f"No records extracted from {table}.")
                return {
                    "status": "skipped",
                    "record_count": 0, 
                    "data_keys": [],
                    "observed_delays": observed
                }
            
            is_chunked = total_rows > chunk_size
            
            s3_key = self.s3_io.create_key(prefix=self.zone, table=table, cutoff_time=cutoff_time, filename=f"{batch_id}.parquet", type="key")
            self.s3_io.ensure_bucket()

            self.s3_io.upload_parquet(tmp_path, s3_key)

            conn.commit()
            logger.info(f"Success: {total_rows} rows → s3://{self.bucket}/{s3_key}")
            
            return {
                    "status": "bronze_complete",
                    "record_count": total_rows,
                    "data_keys": [s3_key],
                    "is_chunked": is_chunked,
                    "observed_delays": observed
                }
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
        acc = DelayAccumulator()

        try:
            while True:
                records = cursor.fetchmany(chunk_size)
                if not records:
                    break

                df = pd.DataFrame(records, columns=columns)

                self._accumulate_delays(acc, df, time_column, CFG.buffer_seconds)

                df = serialize_jsonb_columns(df)

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
                        parquet_schema = unify_schema(table_pa.schema)
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

            observed = self._finalize_delays(acc)

            # Close all writers
            for entry in writers.values():
                entry['writer'].close()

            if not writers:
                conn.commit()
                logger.info(f"No records extracted from {table}.")
                return {
                        "status": "skipped",
                        "record_count": 0,
                        "data_keys": [],
                        "observed_delays": observed
                    }
                

            # Upload each hour partition
            self.s3_io.ensure_bucket()
            results: list[dict] = []

            for (y, m, d, h), entry in writers.items():
                partition_ts = pd.Timestamp(year=y, month=m, day=d, hour=h)
                s3_key = self.s3_io.create_key(prefix=self.zone, table=table, cutoff_time=partition_ts, filename=f"{batch_id}.parquet", type="key")

                self.s3_io.upload_parquet(entry['tmp_path'], s3_key)

                logger.info(
                    f"Success: {entry['count']} rows "
                    f"→ s3://{self.bucket}/{s3_key}"
                )
                results.append({
                    "status": "bronze_complete",
                    "record_count": entry['count'],
                    "s3_key": s3_key,
                    "observed_delays": observed
                })

            conn.commit()
            total = sum(r['record_count'] for r in results)
            logger.info(f"Partitioned {total} rows across {len(results)} hour(s)")
            return {
                "status": "bronze_complete",
                "record_count": total,
                "data_keys": [r['s3_key'] for r in results],
                "is_chunked": len(writers) > 1,
                "observed_delays": observed
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed extracting {table}: {e}")
            raise
        finally:
            for path in tmp_paths:
                if os.path.exists(path):
                    os.unlink(path)