def bronze_metadata_template() -> dict:
    """
    Returns a template for metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "status": None,
        "layer": None,
        "table": None,
        "batch_id": None,
        "previous_batch_id": None,
        "dedup_key": [],
        "change_detection_column": "updated_at",
        "record_count": 0,
        "quarantine_count": 0,
        "loaded_to_silver": False,
        "is_chunked": False,
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
        "created_at": None, # simple ISO format string, to be filled by Airflow 
        "processing_duration_seconds": 0.0 # to be filled by Airflow
    }

def silver_metadata_template() -> dict:
    """
    Returns a template for metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "status": None,
        "layer": "silver",
        "table": None,
        "partition": None,
        "batch_id": None,
        "source_bronze_keys": [],   # lineage back to bronze parquets
        "record_count": 0,
        "quarantine_count": 0,
        "loaded_to_warehouse": False,
        "data_keys": [],
        "quarantine_keys": [],
        "created_at": None,
        "processing_duration_seconds": 0.0,
    }

def gold_metadata_template() -> dict:
    pass

def failure_metadata_template() -> dict:
    """
    Returns a template for failure metadata dicts. Callers should not mutate this directly;
    import and call this function to get a fresh dict, then customize and write it.
    """
    return {
        "schema_version": 1,
        "layer": None,
        "status": "failed",
        "table": None,
        "silver_subpath": None,
        "gold_subpath": None,
        "batch_id": None,
        "stage": None,
        "source_key": None,
        "bronze_metadata_key": None,
        "silver_metadata_key": None,
        "error_type": None,
        "error_message": None,
        "traceback": None,
        "remote_size_bytes": None,
        "created_at": None,
    }