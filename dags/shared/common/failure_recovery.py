import traceback
from datetime import datetime

from shared.common.metadata import MetadataManager
from shared.common.metadata_template import failure_metadata_template


def write_failure_metadata(
    *,
    mm: MetadataManager,
    metadata_prefix: str,
    layer: str,
    table_name: str,
    batch_id: str,
    stage: str,
    source_key: str,
    source_metadata_key: str | None,
    error: Exception,
    s3_hook,
    s3_bucket: str,
    subpath: str | None = None,
) -> str:
    """Write structured failure metadata for a failed pipeline unit of work."""
    ts = datetime.utcnow().isoformat()
    failure_key = f"{metadata_prefix}/quarantine/{layer}/{table_name}/{batch_id}_{ts}.json"

    remote_size = None
    try:
        obj = s3_hook.get_key(key=source_key, bucket_name=s3_bucket)
        remote_size = int(getattr(obj, "content_length", 0) or 0)
    except Exception:
        # Best effort only; failure metadata should still be persisted.
        pass

    payload = failure_metadata_template()
    payload["layer"] = layer
    payload["table"] = table_name
    payload["batch_id"] = batch_id
    payload["stage"] = stage
    payload["source_key"] = source_key
    payload["error_type"] = type(error).__name__
    payload["error_message"] = str(error)
    payload["traceback"] = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    payload["remote_size_bytes"] = remote_size
    payload["created_at"] = datetime.utcnow().isoformat()

    if layer == "silver":
        payload["silver_subpath"] = subpath
        payload["bronze_metadata_key"] = source_metadata_key
    elif layer == "gold":
        payload["gold_subpath"] = subpath
        payload["silver_metadata_key"] = source_metadata_key

    mm.write_metadata(key=failure_key, metadata_dict=payload)
    return failure_key

def list_failure_metadata(
    *,
    mm: MetadataManager,
    metadata_prefix: str,
    layer: str | None = None,
    table_name: str | None = None,
) -> list[dict]:
    """List failure metadata dicts from S3, optionally filtered by layer and/or table."""
    prefix = f"{metadata_prefix}/quarantine/"
    if layer:
        prefix += f"{layer}/"
        if table_name:
            prefix += f"{table_name}/"

    keys = mm.s3_hook.list_keys(bucket_name=mm.s3_bucket, prefix=prefix) or []
    failure_metadata_list = []
    for key in keys:
        if key.endswith('.json'):
            metadata = mm.read_metadata(key=key)
            if metadata:
                failure_metadata_list.append(metadata)
    return failure_metadata_list

def normalized_quarantined_parquet(
    key: str,
) -> None:
    pass