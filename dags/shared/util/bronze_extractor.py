"""
Provides the BronzeExtractor class and supporting utilities for incrementally
extracting data from a PostgreSQL source and loading it into an S3 data lake
as Parquet files (the "bronze" layer in a medallion architecture).

Classes
-------
    DelayAccumulator
        A dataclass that accumulates delay metrics across multiple data chunks
        during an extraction run. Tracks:
        - Maximum and P99 propagation delay (created_at - event_time)
        - Maximum and P99 clock offset (updated_at - created_at)
        - Count of late arrivals and updated records
    BronzeExtractor
        Main class responsible for the Extract-Load (EL) pipeline from Postgres
        to S3. Supports:
        - Watermark-based incremental extraction with configurable buffer and
        overlap windows
        - Automatic backfill via iterative watermark advancement
        - Single-file extraction (cutoff_time partitioning)
        - Per-hour event-time partitioned extraction for correct backfill
        - Delay and propagation metric collection per batch
        - Schema unification and JSONB column serialization
    Methods
    -------
    BronzeExtractor._get_initial_watermark(schema, table, mode, initial_wm)
        Queries Postgres for the minimum updated_at value, either from the
        beginning of the table ("first_run") or after a given point ("catch_up").
    BronzeExtractor._accumulate_delays(acc, df, time_column, buffer_seconds)
        Updates a DelayAccumulator with propagation and clock offset metrics
        from a DataFrame chunk.
    BronzeExtractor._finalize_delays(acc)
        Converts a DelayAccumulator into a serializable metrics dictionary,
        including P99 values computed via pandas quantile.
    BronzeExtractor.extract_bronze(table, schema, pk_column, target_columns,
                                batch_id, time_column)
        Orchestrates the incremental extraction loop. Advances the watermark
        window-by-window until caught up to "now - buffer". Handles skipped
        windows by jumping forward to the next available data point, and
        returns a summary of windows processed.
    BronzeExtractor.el(schema, table, pk_column, target_columns, batch_id,
                    fr, to, batch_limit, chunk_size, time_column)
        Executes the SQL query for a single watermark window and delegates to
        either _el_single (no time_column) or _el_partitioned (with time_column).
        Returns a metadata dict (or list of dicts) describing the output.
    BronzeExtractor._el_single(conn, cursor, columns, table, batch_id,
                            cutoff_time, chunk_size)
        Streams query results into a single Parquet file, uploads it to S3
        under a cutoff_time-based key, and returns extraction metadata.
    BronzeExtractor._el_partitioned(conn, cursor, columns, table, batch_id,
                                    cutoff_time, chunk_size, time_column)
        Streams query results into per-hour Parquet writers keyed by the
        event time column. Records with null/unparseable event times fall
        back to the cutoff_time hour. Uploads each partition file to S3
        and returns aggregated extraction metadata.
    Usage
    -----
    Instantiate BronzeExtractor and call extract_bronze() from an Airflow DAG
    task or any orchestration context:
        extractor = BronzeExtractor()
        result = extractor.extract_bronze(
            table="my_table",
            schema="public",
            pk_column="id",
            target_columns=["id", "event_time", "created_at", "updated_at", "value"],
            batch_id="dag_run_2024_01_01",
            time_column="event_time",
    Dependencies
    ------------
    - shared.common.s3.S3IO
    - shared.common.metadata_template.bronze_metadata_template
    - shared.common.config.CFG
    - shared.common.schema.unify_schema, serialize_jsonb_columns
    - shared.common.connections.get_postgres_hook, pg_cursor
    - shared.common.metadata.MetadataManager
    - shared.common.watermark.S3WatermarkStore
    - shared.common.sql_builder.sql_bronze_extractor
    - shared.common.validators.check_bronze_schema

"""
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass, field

import logging

from shared.common.s3 import S3IO
from shared.common.metadata_template import bronze_metadata_template
from shared.common.config import CFG
from shared.common.schema import unify_schema, serialize_jsonb_columns
from shared.common.connections import get_postgres_hook, pg_cursor
from shared.common.metadata import MetadataManager
from shared.common.watermark import S3WatermarkStore
from shared.common.sql_builder import sql_bronze_extractor
from shared.common.validators import check_bronze_schema

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
        self.pg_hook = get_postgres_hook(conn_id=postgres_conn_id)
        self.s3_io = S3IO(conn_id=s3_conn_id, bucket=s3_bucket)
        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.watermark_store = S3WatermarkStore(self.meta)
        self.bucket = s3_bucket
        self.zone = zone

    def _get_initial_watermark(self, schema: str, table: str, mode: str, initial_wm: datetime | None = None) -> datetime | None:
        """Get the initial watermark for the table.
        
        Args:
            mode: "first_run" to get table's minimum updated_at, "catch_up" to find next non-null value
            initial_wm: Starting point for catch_up mode
        """
        if mode == "first_run":
            sql = f"SELECT min(updated_at) FROM {schema}.{table} WHERE updated_at IS NOT NULL"
        elif mode == "catch_up" and initial_wm is not None:
            sql = f"SELECT min(updated_at) FROM {schema}.{table} WHERE updated_at > %s"
        else:
            return None
        
        with pg_cursor(self.pg_hook) as (conn, cur):
            params = () if mode == "first_run" else (initial_wm,)
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            
            result = row[0] if row and row[0] else None
            
            if mode == "catch_up" and result and result > initial_wm:
                logger.info(f"Jumping from {initial_wm} → {result}")
            
            return result

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
        initial_wm = self._get_initial_watermark(schema=schema, table=table, initial_wm=None, mode="first_run")
        elt_now=datetime.utcnow().replace(tzinfo=timezone.utc)

        while True:
            if windows_processed >= CFG.max_window_per_run:
                logger.info(f"Hit max windows ({CFG.max_window_per_run}), yielding to next DAG run.")
                break

            t0 = time.monotonic()
            batch_id_windowed = f"{batch_id}_w{windows_processed}"
            meta = bronze_metadata_template()
            meta |= {"layer": "bronze", "table": table, "batch_id": batch_id_windowed, "dedup_key": [pk_column]}

            fr, n_fr, to, first, prev_bid = self.watermark_store.get_watermark(
                zone=self.zone,
                table=table,
                elt_now=elt_now,
                buffer_seconds=CFG.buffer_seconds,
                overlap_seconds=CFG.overlap_seconds,
                initial_watermark=initial_wm,
            )

            if to <= n_fr:
                logger.info(f"Window collapsed (to <= nominal_from), caught up.")
                break

            logger.info(f"""
                Info:
                - Window {windows_processed}: [{fr} → {to}] (nominal_from={n_fr})
                - First window: {first}
                - Previous batch_id: {prev_bid}
                - Initial watermark: {initial_wm}
                - ELT now: {elt_now}
                - Buffer seconds: {CFG.buffer_seconds}
            """)

            if n_fr >= elt_now - timedelta(seconds=CFG.buffer_seconds):
                logger.info(f"[{n_fr} >= {elt_now - timedelta(seconds=CFG.buffer_seconds)}] Watermark caught up for {schema}.{table}, stopping.")
                logger.info(f"Next window would be [{fr} → {to}] with nominal_from={n_fr} and buffer={CFG.buffer_seconds}s")
                break

            meta["previous_batch_id"] = prev_bid
            meta["source_watermark"] = self.watermark_store.build_watermark(fr, n_fr, to, CFG.buffer_seconds, 0 if first else CFG.overlap_seconds)

            el_result = self.el(
                schema=schema,
                table=table,
                pk_column=pk_column,
                target_columns=target_columns,
                batch_id=batch_id_windowed,
                fr=fr,
                to=to,
                time_column=time_column,
            )

            meta |= el_result 
            meta["created_at"] = datetime.utcnow().isoformat()
            meta["processing_duration_seconds"] = round(time.monotonic() - t0, 2)
            self.watermark_store.set_watermark(
                zone=self.zone,
                table=table,
                metadata=meta,
            )

            windows_processed += 1

            if el_result.get("status") == "skipped":
                next_data = self._get_initial_watermark(
                    schema=schema, table=table, 
                    mode="catch_up", initial_wm=to
                )
            
                if next_data is None:
                    logger.info(f"No data exists after {to}, stopping.")
                    meta["source_watermark"] = self.watermark_store.build_watermark(
                        fr, n_fr, to, CFG.buffer_seconds, 0 if first else CFG.overlap_seconds
                    )
                    meta["status"] = "skipped"
                    meta["created_at"] = datetime.utcnow().isoformat()
                    self.watermark_store.set_watermark(zone=self.zone, table=table, metadata=meta)
                    break
                
                logger.info(f"No data in [{fr} → {to}], jumping to {next_data}")
                meta["source_watermark"] = self.watermark_store.build_watermark(
                    fr, n_fr, next_data, CFG.buffer_seconds, 0 if first else CFG.overlap_seconds
                )
                meta["status"] = "skipped"
                meta["created_at"] = datetime.utcnow().isoformat()
                self.watermark_store.set_watermark(zone=self.zone, table=table, metadata=meta)
                windows_processed += 1
                continue
        
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
            table_name=table,
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
                check_bronze_schema(df, table=table)
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
            
            s3_key = self.s3_io.create_key(prefix=f"{self.zone}/{table}", table=table, cutoff_time=cutoff_time, filename=f"{batch_id}.parquet", type="key")
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
                    check_bronze_schema(df, table=table)

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
                s3_key = self.s3_io.create_key(prefix=f"{self.zone}/{table}", cutoff_time=partition_ts, filename=f"{batch_id}.parquet", type="key")

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