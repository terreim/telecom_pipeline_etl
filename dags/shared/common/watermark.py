"""
Manages watermarks for ELT batches.

Each table has one current watermark file at:
    metadata/watermark/{layer}/{table}/_metadata.json

Three scenarios on get_watermark:
    1. No data in source yet          → initial_watermark=None, is_first_run=True
    2. Data exists, no metadata yet   → initial_watermark=<min created_at>, is_first_run=True
    3. Normal run / gap recovery      → reads previous batch's `to`, caps window to prevent OOM
"""

import logging
from datetime import datetime, timedelta, timezone
from shared.common.metadata import MetadataManager

logger = logging.getLogger(__name__)

class S3WatermarkStore:
    def __init__(self, metadata_manager: MetadataManager):
        self.mm = metadata_manager

    # ── Key helper ────────────────────────────────────────────────────────────

    def _watermark_key(self, layer: str, table: str) -> str:
        return f"metadata/watermark/{layer}/{table}/_latest.json"

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_watermark(
        self,
        layer: str,
        table: str,
        elt_now: datetime,
        buffer_seconds: int,
        overlap_seconds: int,
        initial_watermark: datetime | None,
        max_window_seconds: int = 3600,
    ) -> tuple[datetime, datetime, datetime, bool, str | None]:
        """
        Returns (from_wm, nominal_from, to, is_first_run, prev_batch_id).

        is_first_run=True signals the caller to route to chunked backfill,
        not the normal single-batch extractor.

        max_window_seconds caps the extraction window to prevent OOM
        on gap recovery after outages.
        """
        prev_batch = self.mm.read_metadata(self._watermark_key(layer, table))
        # Ensure elt_now is timezone-aware (UTC)
        if elt_now.tzinfo is None:
            elt_now = elt_now.replace(tzinfo=timezone.utc)
        to_uncapped = elt_now - timedelta(seconds=buffer_seconds)

        if prev_batch is None:
            if initial_watermark is None:
                # Scenario 1: fresh source, nothing exists yet
                logger.info(f"[{table}] First run, no initial watermark — starting from elt_now")
                return elt_now, elt_now, to_uncapped, True, None
            else:
                # Scenario 2: data exists but no metadata — caller must run chunked backfill
                logger.info(f"[{table}] First run with initial_watermark={initial_watermark.isoformat()}")
                # Ensure initial_watermark is timezone-aware (UTC)
                if initial_watermark.tzinfo is None:
                    initial_watermark = initial_watermark.replace(tzinfo=timezone.utc)
                to = min(to_uncapped, initial_watermark + timedelta(seconds=max_window_seconds))
                return initial_watermark, initial_watermark, to, True, None

        # Scenario 3: normal run or gap recovery
        nominal_from = datetime.fromisoformat(prev_batch["source_watermark"]["to"])
        # Ensure nominal_from is timezone-aware (UTC)
        if nominal_from.tzinfo is None:
            nominal_from = nominal_from.replace(tzinfo=timezone.utc)
        from_wm = nominal_from - timedelta(seconds=overlap_seconds)

        # Cap to prevent OOM on large gaps
        to = min(to_uncapped, nominal_from + timedelta(seconds=max_window_seconds))

        if to < to_uncapped:
            gap_minutes = (to_uncapped - nominal_from).total_seconds() / 60
            logger.warning(
                f"[{table}] Gap detected ({gap_minutes:.0f} min) — "
                f"capping window to {max_window_seconds}s. Will catch up over multiple batches."
            )

        return from_wm, nominal_from, to, False, prev_batch.get("batch_id")

    # ── Build ─────────────────────────────────────────────────────────────────

    def build_watermark(self, from_wm, nominal_from, to, buffer_seconds, overlap_seconds):
        return {
            "column": "updated_at",
            "nominal_from": nominal_from.isoformat(),
            "from": from_wm.isoformat(),
            "to": to.isoformat(),
            "buffer_seconds": buffer_seconds,
            "overlap_seconds": overlap_seconds
        }
    
    # ── Write ─────────────────────────────────────────────────────────────────

    def set_watermark(
        self,
        layer: str,
        table: str,
        metadata: dict,
    ) -> None:
        """
        Persists the watermark metadata for a given layer and table.
        """

        # Duplicate another key but with timestaped history for audit
        self.mm.write_metadata(
            key=f"metadata/watermark/{layer}/{table}/{metadata['batch_id']}.json",
            metadata_dict=metadata,
        )

        # Write _latest.json
        self.mm.write_metadata(
            key=self._watermark_key(layer, table),
            metadata_dict=metadata,
        )