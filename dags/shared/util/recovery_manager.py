"""
RecoveryManager — flag reset and targeted deletion utilities for pipeline recovery.

Classes
-------
RecoveryManager
    Base class. Wraps MetadataManager to provide unmark_one / unmark_all, and
    builds partition-range summaries for HITL display.

SilverRecovery(RecoveryManager)
    Operates on bronze metadata (flag: loaded_to_silver).
    unmark_all  → bronze metadata reset; normal silver DAG re-processes on next run.
    delete_all  → deletes actual silver parquet files from S3.

StagingRecovery(RecoveryManager)
    Operates on silver metadata (flag: loaded_to_warehouse).
    unmark_all  → silver metadata reset; normal staging DAG re-loads on next run.

GoldRecovery(RecoveryManager)
    No metadata flag. Uses GoldAggregator.is_processed and CH delete methods.
    delete_all      → deletes CH rows for each hour/day.
    recompute_all   → re-runs GoldAggregator.aggregate() for each period.
"""

import logging
import re

import pandas as pd

from shared.common.config import CFG
from shared.common.metadata import MetadataManager
from shared.common.s3 import S3IO

logger = logging.getLogger(__name__)


class RecoveryManager:
    def __init__(
        self,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
    ):
        self.meta = MetadataManager(s3_bucket, conn_id=s3_conn_id)
        self.s3_io = S3IO(s3_conn_id, s3_bucket)
        self.bucket = s3_bucket

    # ─── Shared helpers ───────────────────────────────────────────────────────

    def _list_processed(
        self,
        prefix: str,
        flag: str,
        lookback: int | None = None,
        skip_latest: bool = True,
    ) -> list[tuple[str, dict]]:
        """Return (key, metadata) pairs where *flag* is True (already processed).

        Inverse of MetadataManager.find_unprocessed — used to find candidates for
        unmarking in recovery scenarios.
        """
        keys = self.meta.s3_hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []
        keys = [k for k in keys if k.endswith(".json")]
        if skip_latest:
            keys = [k for k in keys if not k.endswith("_latest.json")]
        if lookback:
            keys = keys[-lookback:]

        processed = []
        for key in keys:
            metadata = self.meta.read_metadata(key)
            if metadata is None:
                continue
            if metadata.get(flag, False):
                processed.append((key, metadata))
        return processed

    def _summarise(self, keys: list[tuple[str, dict]], flag: str) -> dict:
        """Build a human-readable summary dict for HITL display."""
        if not keys:
            return {"count": 0, "partition_range": None, "flag": flag}

        partitions = []
        for key, _ in keys:
            m = re.search(r'year=(\d+)/month=(\d+)/day=(\d+)', key)
            if m:
                partitions.append(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")

        partitions = sorted(set(partitions))
        return {
            "count": len(keys),
            "flag": flag,
            "partition_range": f"{partitions[0]} → {partitions[-1]}" if partitions else "unknown",
            "sample_keys": [k for k, _ in keys[:5]],
        }

    def unmark_one(self, key: str, metadata: dict, flag: str) -> None:
        """Reset *flag* to False and persist back to S3."""
        metadata[flag] = False
        self.meta.write_metadata(key=key, metadata_dict=metadata)
        logger.info(f"Unmarked {flag} on {key}")

    def _unmark_all(self, keys: list[tuple[str, dict]], flag: str) -> int:
        """Reset *flag* on all provided (key, metadata) pairs. Returns count reset."""
        for key, metadata in keys:
            self.unmark_one(key, metadata, flag)
        logger.info(f"Unmarked {flag} on {len(keys)} metadata entries")
        return len(keys)


class SilverRecovery(RecoveryManager):
    """Recovery for the bronze → silver transition.

    After unmark_all, the normal silver DAG picks up the bronze entries on its
    next scheduled run via SilverTransformer.find_unprocessed_bronze().
    """

    FLAG = "loaded_to_silver"

    def list_affected(self, table: str, lookback: int | None = None) -> dict:
        """Summary of bronze entries already marked loaded_to_silver (recovery candidates)."""
        prefix = f"{CFG.metadata_prefix}/watermark/bronze/{table}/"
        keys = self._list_processed(prefix, self.FLAG, lookback)
        summary = self._summarise(keys, self.FLAG)
        summary["table"] = table
        return summary

    def unmark_all(self, table: str, lookback: int | None = None) -> dict:
        """Reset loaded_to_silver=False on bronze metadata for *table*.

        Silver DAG will re-process these entries on its next run.
        """
        prefix = f"{CFG.metadata_prefix}/watermark/bronze/{table}/"
        keys = self._list_processed(prefix, self.FLAG, lookback)
        count = self._unmark_all(keys, self.FLAG)
        logger.info(f"[SilverRecovery] Unmarked {count} bronze entries for {table}")
        return {"table": table, "unmarked": count}

    def delete_all(self, table: str, lookback: int | None = None) -> dict:
        """Delete actual silver parquet files from S3 for *table*.

        Reads data_keys from each already-processed bronze metadata entry and
        deletes the corresponding silver parquets. Does NOT reset the bronze flag
        — call unmark_all separately if you also want the silver DAG to re-run.
        """
        prefix = f"{CFG.metadata_prefix}/watermark/bronze/{table}/"
        keys = self._list_processed(prefix, self.FLAG, lookback)

        deleted = errors = 0
        for _, metadata in keys:
            for silver_key in metadata.get("data_keys", []):
                try:
                    self.s3_io.s3_hook.delete_objects(bucket=self.bucket, keys=[silver_key])
                    deleted += 1
                    logger.info(f"Deleted silver parquet: {silver_key}")
                except Exception as e:
                    logger.warning(f"Failed to delete {silver_key}: {e}")
                    errors += 1

        return {"table": table, "deleted": deleted, "errors": errors}


class StagingRecovery(RecoveryManager):
    """Recovery for the silver → ClickHouse staging transition.

    After unmark_all, the normal staging DAG picks up the silver entries on its
    next scheduled run via ClickHouseLoader.find_unprocessed_silver().
    """

    FLAG = "loaded_to_warehouse"

    def list_affected(self, silver_subpath: str, lookback: int | None = None) -> dict:
        """Summary of silver entries already marked loaded_to_warehouse."""
        prefix = f"{CFG.metadata_prefix}/watermark/silver/{silver_subpath}/"
        keys = self._list_processed(prefix, self.FLAG, lookback)
        summary = self._summarise(keys, self.FLAG)
        summary["silver_subpath"] = silver_subpath
        return summary

    def unmark_all(self, silver_subpath: str, lookback: int | None = None) -> dict:
        """Reset loaded_to_warehouse=False on silver metadata for *silver_subpath*.

        Staging DAG will re-load these entries on its next run.
        """
        prefix = f"{CFG.metadata_prefix}/watermark/silver/{silver_subpath}/"
        keys = self._list_processed(prefix, self.FLAG, lookback)
        count = self._unmark_all(keys, self.FLAG)
        logger.info(f"[StagingRecovery] Unmarked {count} silver entries for {silver_subpath}")
        return {"silver_subpath": silver_subpath, "unmarked": count}


class GoldRecovery(RecoveryManager):
    """Recovery for gold ClickHouse aggregates.

    No metadata flag — uses GoldAggregator.is_processed() and CH delete methods.
    delete_all    → issues ALTER TABLE DELETE for each hour/day.
    recompute_all → re-runs GoldAggregator.aggregate() for each period, skipping
                    any period already marked complete (idempotent on retry).
    """

    def __init__(
        self,
        s3_conn_id: str = CFG.s3_conn_id,
        s3_bucket: str = CFG.s3_bucket,
        ch_conn_id: str = CFG.clickhouse_conn_id,
    ):
        super().__init__(s3_conn_id, s3_bucket)
        from shared.util.gold_aggregator import GoldAggregator
        self.aggregator = GoldAggregator(
            s3_conn_id=s3_conn_id, ch_conn_id=ch_conn_id, s3_bucket=s3_bucket
        )

    @staticmethod
    def build_periods(lookback_hours: int) -> dict:
        """Build sorted lists of (y,m,d,h) hours and (y,m,d) days."""
        now = pd.Timestamp.now("UTC").floor("h")
        hours, days_set = [], set()
        for i in range(lookback_hours):
            ts = now - pd.Timedelta(hours=i)
            hours.append((ts.year, ts.month, ts.day, ts.hour))
            days_set.add((ts.year, ts.month, ts.day))
        periods = {"hours": sorted(hours), "days": sorted(days_set)}
        periods["range"] = f"{periods['hours'][0]} → {periods['hours'][-1]}"
        return periods

    def list_affected(self, lookback_hours: int) -> dict:
        """Summary of periods that would be deleted/recomputed."""
        periods = self.build_periods(lookback_hours)
        return {
            "hours": len(periods["hours"]),
            "days": len(periods["days"]),
            "range": periods["range"],
        }

    def delete_all(self, hours: list[tuple], days: list[tuple]) -> dict:
        """Delete CH gold data for all *hours* and *days*."""
        h_deleted = d_deleted = errors = 0

        for y, m, d, h in hours:
            try:
                self.aggregator.delete_hour(y, m, d, h)
                h_deleted += 1
            except Exception as e:
                logger.warning(f"delete_hour({y},{m},{d},{h}) failed: {e}")
                errors += 1

        for y, m, d in days:
            try:
                self.aggregator.delete_day(y, m, d)
                d_deleted += 1
            except Exception as e:
                logger.warning(f"delete_day({y},{m},{d}) failed: {e}")
                errors += 1

        logger.info(
            f"[GoldRecovery] Deleted {h_deleted} hours, {d_deleted} days ({errors} errors)"
        )
        return {"hours_deleted": h_deleted, "days_deleted": d_deleted, "errors": errors}

    def recompute_all(
        self,
        hours: list[tuple],
        days: list[tuple],
        skip_daily: bool = False,
    ) -> dict:
        """Re-run gold aggregation for all periods. Idempotent — skips completed ones."""
        reports = self.aggregator.reports()
        hourly_reports = [r for r in reports.values() if r.granularity == "hourly"]
        daily_reports  = [r for r in reports.values() if r.granularity == "daily"]

        h_processed = h_skipped = d_processed = d_skipped = 0

        for y, m, d, h in hours:
            partition = self.aggregator._partition_hour(y, m, d, h)
            for report in hourly_reports:
                if self.aggregator.is_processed(report.name, partition):
                    h_skipped += 1
                    continue
                self.aggregator.aggregate(report, y, m, d, h)
                h_processed += 1
            if h_processed > 0 and h_processed % 12 == 0:
                logger.info(
                    f"[GoldRecovery] Hourly: {h_processed} processed, {h_skipped} skipped"
                )

        if not skip_daily:
            for y, m, d in days:
                partition = self.aggregator._partition_day(y, m, d)
                for report in daily_reports:
                    if self.aggregator.is_processed(report.name, partition):
                        d_skipped += 1
                        continue
                    self.aggregator.aggregate(report, y, m, d)
                    d_processed += 1

        logger.info(
            f"[GoldRecovery] Done — hourly: {h_processed} new / {h_skipped} skipped, "
            f"daily: {d_processed} new / {d_skipped} skipped"
        )
        return {
            "hours_processed": h_processed, "hours_skipped": h_skipped,
            "days_processed": d_processed,  "days_skipped": d_skipped,
        }
