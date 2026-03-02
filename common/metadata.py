import json
import connections
from config import PipelineConfig as C

class MetadataManager:
    """
    Manages metadata for ELT batches, storing and retrieving JSON metadata files in S3.
    Each metadata file corresponds to an hour-partition of a specific table in bronze/silver/gold.

    Proposed schema:
    `{
        "schema_version": 1,
        "status": "complete",
        "layer": "bronze",
        "table": "subscriber_traffic",
        "batch_id": "st_scheduled__2026-03-02T09:00:00+00:00",
        "previous_batch_id": "st_scheduled__2026-03-02T08:59:00+00:00",
        "dedup_key": ["traffic_id", "updated_at"],
        "change_detection_column": "updated_at",
        "record_count": 14832,
        "quarantine_count": 0,
        "source_watermark": {
            "column": "updated_at",
            "from": "2026-03-02T08:52:00+00:00",
            "nominal_from": "2026-03-02T08:54:00+00:00",
            "to": "2026-03-02T09:11:00+00:00",
            "buffer_seconds": 240,
            "overlap_seconds": 120
        },
        "observed_delays": {
            "max_station_propagation_seconds": 18.4,
            "max_clock_offset_seconds": 62.1,
            "p99_station_propagation_seconds": 12.7,
            "p99_clock_offset_seconds": 61.3,
            "late_arrival_count": 43,
            "update_count": 127
        },
        "data_keys": [
            "bronze/subscriber_traffic/year=2026/month=03/day=02/hour=09/st_scheduled__2026-03-02T09:00:00+00:00.parquet"
        ],
        "created_at": "2026-03-02T09:15:23+00:00",
        "processing_duration_seconds": 12.4
    }`
    """
    def __init__(self, s3_bucket: str):
        self.s3_hook = connections.get_s3_hook(conn_id=C.S3_CONN_ID)
        self.s3_bucket = s3_bucket

    def read_metadata(self, prefix: str):
        """
        Reads metadata content from S3. Returns the content as a string.
        If the key does not exist, returns None.
        """
        try:
            return self.s3_hook.read_key(bucket=self.s3_bucket, key=prefix)
        except Exception:
            return None
    
    def write_metadata(self, prefix: str, metadata_dict: dict):
        """
        Writes metadata content to S3 in JSON. 
        If the key already exists, it will be overwritten.
        """
        try:
            self.s3_hook.load_string(
                string_data=json.dumps(metadata_dict, indent=2),
                bucket=self.s3_bucket,
                key=prefix
            )
        except Exception:
            raise