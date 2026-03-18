def bronze_metadata_template() -> dict:
    """
    Returns a template for metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "layer": None,
        "table": None,
        "batch_id": None,
        "previous_batch_id": None,
        "status": None,
        "dedup_key": [],
        "change_detection_column": "updated_at",
        "is_chunked": False,
        "loaded_to_silver": False,
        "record_count": 0,
        "quarantine_count": 0,
        # "source_watermark": {
        #     "column": "updated_at",
        #     "nominal_from": None,
        #     "from": None,
        #     "to": None,
        #     "buffer_seconds": 0,
        #     "overlap_seconds": 0
        # }, # to be filled by watermark builder
        # "observed_delays": {
        #     "actual_max_updated_at": None,
        #     "max_station_propagation_seconds": 0,
        #     "max_clock_offset_seconds": 0,
        #     "p99_station_propagation_seconds": 0,
        #     "p99_clock_offset_seconds": 0,
        #     "late_arrival_count": 0,
        #     "update_count": 0
        # },
        # "data_keys": [
        #     "bronze/subscriber_traffic/year=2026/month=03/day=02/hour=09/st_scheduled__2026-03-02T09:00:00+00:00.parquet"
        # ], # to be filled during el()
        "created_at": None,  # simple ISO format string, to be filled by Airflow
        "processing_duration_seconds": 0.0,  # to be filled by Airflow
    }


def silver_metadata_template() -> dict:
    """
    Returns a template for metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "layer": "silver",
        "table": None,
        "partition": None,
        "batch_id": None,
        "status": None,
        "source_bronze_keys": [],  # lineage back to bronze parquets
        "data_keys": [],
        "quarantine_keys": [],
        "loaded_to_warehouse": False,
        "record_count": 0,
        "quarantine_count": 0,
        "created_at": None,
        "processing_duration_seconds": 0.0,
    }

def staging_metadata_template() -> dict:
    """
    Returns a template for metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "layer": "staging",
        "table": None,
        "partition": None,
        "batch_id": None,
        "status": None,
        "source_silver_keys": [],
        "data_key": None,
        "record_count": 0,
        "data_quality": None,
        "is_reopened": False,
        "late_record_count": 0,
        "late_source_silver_keys": [],
        "created_at": None,
        "processing_duration_seconds": 0.0,
    }


def gold_metadata_template() -> dict:
    return {
        "schema_version": 1,
        "layer": "gold",
        "table": None,
        "report": None,
        "partition": None,
        "batch_id": None,
        "status": None,
        "data_key": None,  # single S3 key (gold writes one file per partition)
        "record_count": 0,
        "created_at": None,
        "processing_duration_seconds": 0.0,
    }


def failure_metadata_template() -> dict:
    """
    Returns a template for failure metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "layer": None,
        "table": None,
        "silver_subpath": None,
        "gold_subpath": None,
        "batch_id": None,
        "stage": None,
        "status": "failed",
        "source_key": None,
        "bronze_metadata_key": None,
        "silver_metadata_key": None,
        "remote_size_bytes": None,
        "error_type": None,
        "error_message": None,
        "traceback": None,
        "created_at": None,
    }