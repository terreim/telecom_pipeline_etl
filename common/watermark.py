import json
from datetime import datetime
from metadata import MetadataManager


class S3WatermarkStore:
    """
    Manages watermarks for ELT batches, building and parsing watermark information from metadata in S3 via MetadataManager.
    Each watermark corresponds to an hour-partition of a specific table in bronze/silver/gold.
    """

    def __init__(self, metadata_manager: MetadataManager):
        self.metadata_manager = metadata_manager

    def get_watermark(self, table_name: str) -> datetime | None:
        meta = self.metadata_manager.read_metadata(f"watermarks/{table_name}")
        return meta["source_watermark"] if meta else None
    
    def set_watermark(self, table_name: str, value: datetime) -> None:
        self.metadata_manager.write_metadata(f"watermarks/{table_name}", {
            "source_watermark": value.isoformat() # Placeholder until schema is defined
        })
    