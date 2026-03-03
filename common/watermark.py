"""
Manages watermarks for ELT batches, building and parsing watermark information from metadata in S3 via MetadataManager.
Each watermark corresponds to an hour-partition of a specific table in bronze/silver/gold.

Basically a common wrapper for metadata with more specific methods for getting/setting watermarks.
NOTE: Data Lake should have Versioning enabled, else the _metadata.json is lost on overwrite and cannot be used for auditing or recovery.

"""

from datetime import datetime, timedelta
from common.metadata import MetadataManager

class S3WatermarkStore:
    def __init__(self, metadata_manager: MetadataManager):
        self.metadata_manager = metadata_manager

    def _watermark_key(self, zone: str, table: str) -> str:
        return f"metadata/watermark/{zone}/{table}/_metadata.json"

    def get_watermark(
        self,
        zone: str, 
        table: str,
        elt_now: datetime,
        buffer_seconds: int,
        overlap_seconds: int,
        initial_watermark: datetime
    ) -> tuple[datetime, datetime, datetime, bool]:
        
        """
        Returns (from, nominal_from, to, is_first_run).
        """
        prev_batch = self.metadata_manager.read_metadata(self._watermark_key(zone, table))
        to = elt_now - timedelta(seconds=buffer_seconds)

        if prev_batch is None:
            # First run
            return initial_watermark, initial_watermark, to, True

        nominal_from = datetime.fromisoformat(prev_batch["source_watermark"]["to"])
        from_wm = nominal_from - timedelta(seconds=overlap_seconds)

        return from_wm, nominal_from, to, False
    
    def build_watermark(self, 
        from_wm: datetime, 
        nominal_from: datetime, 
        to: datetime, 
        buffer_seconds: int, 
        overlap_seconds: int,
        is_first_run: bool = False
    ) -> dict:
        return {
            "column": "updated_at",
            "nominal_from": nominal_from.isoformat(),
            "from": from_wm.isoformat(),
            "to": to.isoformat(),
            "buffer_seconds": buffer_seconds,
            "overlap_seconds": 0 if is_first_run else overlap_seconds,
        }
    
    def set_watermark(
        self,
        dict_metadata: dict,
        watermark: dict,
        zone: str,
        table: str,
    ):
        metadata = {**dict_metadata, "source_watermark": watermark}
        self.metadata_manager.write_metadata(self._watermark_key(zone, table), metadata)