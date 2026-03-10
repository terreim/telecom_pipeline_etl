def metadata_template() -> dict:
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
        "loaded_to_warehouse": False,
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