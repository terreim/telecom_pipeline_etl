import logging
import time
import re
from datetime import datetime, timezone
import pandas as pd

from shared.common.config import CFG
from shared.common.s3 import S3IO
from shared.common.ch import ClickHouseIO
from shared.common.metadata import MetadataManager
from shared.common.metadata_template import staging_metadata_template
from shared.common.connections import get_clickhouse_hook, get_s3_credentials
from shared.common.sql_builder import sql_ch_dim_station, sql_ch_dim_dict

logger = logging.getLogger(__name__)

# Quality flags that are safe to re-admit: the row has a valid PK, station_id,
# and event_time — only a numeric measurement was out of range.
_SOFT_FAILURE_FLAGS = {"jitter_invalid", "duration_invalid", "pkt_loss_invalid", "latency_invalid"}


class ClickHouseLoader:
    def __init__(
        self,
        ch_conn_id: str = CFG.clickhouse_conn_id,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
        silver_prefix: str = CFG.silver_prefix,
    ):
        self.ch_hook = get_clickhouse_hook(ch_conn_id)
        self.s3_io = S3IO(conn_id=s3_conn_id, bucket=s3_bucket)
        self.ch_io = ClickHouseIO(s3_conn_id=s3_conn_id, ch_conn_id=ch_conn_id)
        self.s3_bucket = s3_bucket
        self.silver_prefix = silver_prefix
        self.metadata_prefix = CFG.metadata_prefix
        self.quarantine_prefix = CFG.quarantine_prefix

        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.s3_access_key, self.s3_secret_key, self.s3_endpoint = get_s3_credentials(s3_conn_id)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _has_dim_changes(self, schema: str, table_name: str, dim_df: pd.DataFrame) -> bool:
        """Check if dimension data has changed compared to existing table."""
        if dim_df is None or dim_df.empty:
            return False

        existing_count = self.ch_hook.execute(f"SELECT count() FROM {schema}.{table_name} FINAL")
        if existing_count[0][0] != len(dim_df):
            return True

        new_checksum = hash(tuple(sorted(dim_df['station_id'].tolist())))
        existing_ids = self.ch_hook.execute(f"SELECT station_id FROM {schema}.{table_name} FINAL")
        existing_checksum = hash(tuple(sorted([r[0] for r in existing_ids])))

        return new_checksum != existing_checksum

    def _split_quarantine(self, q_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split a quarantine DataFrame into (recoverable, unrecoverable).

        Recoverable rows failed only on soft numeric-range checks — they have
        valid PKs, station_id, and event_time, so they can be merged into
        staging with data_quality='quarantined'.

        Unrecoverable rows have hard failures (null event_time, null station_id,
        bad protocol, etc.) and must stay in quarantine only.
        """
        issues = q_df.get("quality_issues", pd.Series("", index=q_df.index)).fillna("")

        def _is_soft_only(issue_str: str) -> bool:
            flags = {f.strip() for f in issue_str.split(",") if f.strip()}
            return bool(flags) and flags.issubset(_SOFT_FAILURE_FLAGS)

        soft_mask = issues.apply(_is_soft_only)
        return q_df[soft_mask].copy(), q_df[~soft_mask].copy()
    
    def ensure_db(self, db_name: str = CFG.schema_name) -> None:
        """Ensure the ClickHouse database/schema exists."""
        try:
            self.ch_io.execute_query(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            logger.info(f"Ensured ClickHouse database exists: {db_name}")
            
            return {"status": "success", "database": db_name}
        
        except Exception as e:
            logger.error(f"Failed to ensure ClickHouse database {db_name}: {e}")
            raise

    def ensure_tables(self, ddl_path: str) -> dict:
        try:
            with open(ddl_path, 'r') as f:
                ddl_sql = f.read()

            statements = [s.strip() for s in ddl_sql.split(";") if s.strip()]
            for stmt in statements:
                name_match = re.search(
                    r'CREATE\s+(?:TABLE|DICTIONARY)\s+IF\s+NOT\s+EXISTS\s+(\S+)',
                    stmt, re.IGNORECASE
                )
                label = name_match.group(1) if name_match else "unknown"
                logger.info(f"Ensuring ClickHouse table/dictionary exists: {label}")
                self.ch_io.execute_query(stmt)

            return {"status": "success", "details": f"Executed {len(statements)} DDL statements from {ddl_path}"}

        except Exception as e:
            logger.error(f"Failed to ensure ClickHouse staging DDL from {ddl_path}: {e}")
            raise

    def drop_tables(self, ddl_path: str, db_name: str = CFG.schema_name) -> dict:
        """Drop all tables/views/dictionaries defined in a DDL file."""
        try:
            with open(ddl_path, 'r') as f:
                ddl_sql = f.read()

            objects = re.findall(
                r'CREATE\s+((?:MATERIALIZED\s+VIEW|TABLE|DICTIONARY))\s+IF\s+NOT\s+EXISTS\s+(\S+)',
                ddl_sql, re.IGNORECASE
            )

            dropped = []
            for _, name in reversed(objects):
                # Qualify with db_name if no schema prefix present
                fqn = name if '.' in name else f"{db_name}.{name}"
                stmt = f"DROP TABLE IF EXISTS {fqn}"
                logger.info(f"Dropping: {stmt}")
                self.ch_io.execute_query(stmt)
                dropped.append(fqn)

            return {"status": "success", "dropped": dropped}

        except Exception as e:
            logger.error(f"Failed to drop tables from DDL {ddl_path}: {e}")
            raise

    def load_dim(self, schema, table_name, dim_df: pd.DataFrame, force_reload: bool = False) -> dict:
        """Load dimension data into ClickHouse and refresh the dictionary."""
        if not force_reload and not self._has_dim_changes(schema, table_name, dim_df):
            logger.info(f"No dimension changes detected, skipping load for {table_name}")
            return {"status": "skipped", "table": table_name, "reason": "no_changes"}

        df = dim_df.copy()
        df['updated_at'] = pd.Timestamp.now(tz='UTC')

        self.ch_io.execute_query(sql_ch_dim_station())
        self.ch_io.insert_ch(df=df, ch_table=table_name, columns=df.columns.tolist())

        self.ch_io.execute_query(f"OPTIMIZE TABLE {schema}.{table_name} FINAL")
        count = self.ch_io.execute_query(f"SELECT count() FROM {schema}.{table_name} FINAL")
        logger.info(f"Loaded {count[0][0]} dimension records")

        self.ch_io.execute_query(sql_ch_dim_dict())
        return {"status": "success", "table": table_name, "count": count[0][0]}

    def find_unprocessed_silver(
            self,
            silver_table: str,
            lookback_range: int | None = None,
        ) -> list[tuple[str, dict]]:
        """Scan silver metadata for json files with unmarked `loaded_to_warehouse`."""
        prefix = f"{self.metadata_prefix}/watermark/silver/{silver_table}/"
        results = self.meta.find_unprocessed(
            prefix=prefix,
            flag="loaded_to_warehouse",
            lookback=lookback_range,
        )
        logger.info(f"Identified {len(results)} unprocessed silver entries for {silver_table}")
        return results

    # =========================================================================
    # Main
    # =========================================================================

    def list_unprocessed(
        self,
        silver_subpath: str,
        lookback_range: int = 720,
    ) -> list[dict]:
        """Return serialisable descriptors for each unprocessed silver batch."""
        return [
            {"meta_key": meta_key, "silver_meta": silver_meta}
            for meta_key, silver_meta in self.find_unprocessed_silver(
                silver_table=silver_subpath, lookback_range=lookback_range
            )
        ]

    def load_staging_single(
        self,
        staging_table: str,
        silver_subpath: str,
        batch_id: str,
        meta_key: str,
        silver_meta_file: dict,
        include_recoverable_quarantine: bool = True,
    ) -> dict:
        """Load ONE silver batch into ClickHouse staging.

        Steps:
          1. Read all silver (valid) parquets for this batch.
          2. Optionally re-admit soft-fail quarantine rows.
          3. Concat and INSERT into CH staging.
          4. Mark the silver metadata as loaded_to_warehouse=True.
        """
        t0 = time.monotonic()
        staging_meta = staging_metadata_template()
        staging_meta['table'] = staging_table
        staging_meta['batch_id'] = batch_id

        silver_keys = silver_meta_file.get("data_keys", [])
        partition = silver_meta_file.get("partition")
        quarantine_keys = silver_meta_file.get("quarantine_keys", [])

        frames: list[pd.DataFrame] = []
        recovered_count = 0
        source_silver_keys = []
        late_source_keys = []

        for s3_key in silver_keys:
            try:
                df = self.s3_io.read_parquet(s3_key)
                df['data_quality'] = 'valid'
                frames.append(df)
                source_silver_keys.append(s3_key)
            except Exception as e:
                logger.error(f"Failed to read silver parquet {s3_key}: {e} — skipping")

        # ── 2. Recoverable quarantine records ─────────────────────────
        if include_recoverable_quarantine and quarantine_keys:
            for q_key in quarantine_keys:
                try:
                    q_df = self.s3_io.read_parquet(q_key)
                except Exception as e:
                    logger.warning(f"Failed to read quarantine parquet {q_key}: {e} — skipping")
                    continue

                recoverable, unrecoverable = self._split_quarantine(q_df)

                if not recoverable.empty:
                    recoverable['data_quality'] = 'quarantined'
                    frames.append(recoverable)
                    recovered_count += len(recoverable)
                    late_source_keys.append(q_key)
                    logger.info(
                        f"Re-admitted {len(recoverable)} soft-fail records from quarantine: {q_key}"
                    )

                if not unrecoverable.empty:
                    logger.debug(
                        f"Left {len(unrecoverable)} hard-fail records in quarantine: {q_key}"
                    )

        partition_slug = partition.replace('/', '-').replace("=", "") if partition else "no_partition"

        if not frames:
            logger.info(f"No data to load for silver batch {meta_key}, marking done.")
            staging_meta['status'] = 'skipped'
            staging_meta['created_at'] = datetime.now(timezone.utc).isoformat()
            staging_meta['processing_duration_seconds'] = round(time.monotonic() - t0, 2)
            silver_meta_file['loaded_to_warehouse'] = True

            self._write_staging_metadata(silver_subpath, partition_slug, staging_meta)
            self._mark_silver_loaded(meta_key, silver_meta_file)
            return staging_meta

        # ── 3. Combine and insert ─────────────────────────────────────
        combined = pd.concat(frames, ignore_index=True)
        del frames

        try:
            s3_key = self._insert_to_ch(combined, staging_table, silver_subpath, batch_id, partition=partition or "")
            staging_meta['status'] = 'staging_complete'
            staging_meta['data_key'] = s3_key
        except Exception as e:
            logger.error(f"Failed to load staging for {meta_key}: {e}")
            staging_meta['status'] = 'failed'
            staging_meta['created_at'] = datetime.now(timezone.utc).isoformat()
            staging_meta['processing_duration_seconds'] = round(time.monotonic() - t0, 2)
            self._write_staging_metadata(silver_subpath, partition_slug, staging_meta)
            raise

        staging_meta['record_count'] = len(combined) - recovered_count
        staging_meta['partition'] = partition
        staging_meta['late_record_count'] = recovered_count
        staging_meta['source_silver_keys'] = source_silver_keys
        staging_meta['late_source_silver_keys'] = late_source_keys
        staging_meta['is_reopened'] = recovered_count > 0
        staging_meta['data_quality'] = 'mixed' if recovered_count > 0 else 'valid'
        staging_meta['created_at'] = datetime.now(timezone.utc).isoformat()
        staging_meta['processing_duration_seconds'] = round(time.monotonic() - t0, 2)
        del combined

        self._write_staging_metadata(silver_subpath, partition_slug, staging_meta)
        self._mark_silver_loaded(meta_key, silver_meta_file)

        logger.info(
            f"Staged {staging_meta['record_count']} valid + {recovered_count} recovered "
            f"→ {staging_table}"
        )
        return staging_meta

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _insert_to_ch(
        self,
        df: pd.DataFrame,
        staging_table: str,
        silver_subpath: str,
        batch_id: str,
        partition: str = "",
    ) -> str | None:
        """Write df to a temp S3 parquet and INSERT INTO CH via insert_with_fallback."""
        partition_slug = partition.replace("/", "-").replace("=", "") if partition else "no_partition"
        s3_key = f"{self.metadata_prefix}/staging_tmp/{silver_subpath}/{partition_slug}/{batch_id}.parquet"
        written_key = self.s3_io.write_parquet(df, s3_key)
        s3_url = f"{self.s3_endpoint}/{self.s3_bucket}/{written_key}"
        self.ch_io.insert_with_fallback(df=df, s3_url=s3_url, ch_table=staging_table)
        return written_key

    def _write_staging_metadata(self, silver_subpath: str, batch_id: str, meta: dict) -> None:
        self.meta.write_metadata(
            key=f"{self.metadata_prefix}/watermark/staging/{silver_subpath}/{batch_id}.json",
            metadata_dict=meta,
        )

    def _mark_silver_loaded(self, meta_key: str, silver_meta: dict) -> None:
        self.meta.mark_loaded(meta_key, silver_meta, "loaded_to_warehouse")
