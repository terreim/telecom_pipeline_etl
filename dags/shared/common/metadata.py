"""
Manages metadata for ELT batches, storing and retrieving JSON metadata files in S3.
Each metadata file corresponds to an hour-partition of a specific table in bronze/silver/gold.
"""

import json
from shared.common.connections import get_s3_hook
from shared.common.config import CFG

class MetadataManager:
    def __init__(self, s3_bucket: str, conn_id: str = CFG.s3_conn_id):
        self.s3_hook = get_s3_hook(conn_id=conn_id)
        self.s3_bucket = s3_bucket

    @staticmethod
    def _is_not_found(e: Exception) -> bool:
        msg = str(e).lower()
        return "nosuchkey" in msg or "404" in msg or "not found" in msg

    def check_metadata_exists(self, prefix: str) -> bool:
        keys = self.s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=prefix) or []
        return any(k.endswith('.json') for k in keys)

    def read_metadata(self, key: str) -> dict | None:
        """
        Reads metadata from S3. Returns None only if the key genuinely does not exist.
        Raises on network errors, permission errors, or corrupted JSON; callers
        must not interpret those as first-run signals.
        """
        try:
            content = self.s3_hook.read_key(bucket_name=self.s3_bucket, key=key)
        except Exception as e:
            if self._is_not_found(e):
                return None
            raise

        if content is None:
            return None

        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Corrupted metadata at s3://{self.s3_bucket}/{key}") from e
    
    def write_metadata(self, key: str, metadata_dict: dict) -> None:
        """
        Writes metadata to S3 as JSON.
        Bucket versioning preserves history on overwrite.
        """
        self.s3_hook.load_string(
            string_data=json.dumps(metadata_dict, indent=2, default=str),
            bucket_name=self.s3_bucket,
            key=key,
            replace=True,
        )
    
    def build_metadata(self, base: dict, **overrides) -> dict:
        """
        Merges base dict with overrides. Overrides win on key collision.
        Stateless — callers own their base dicts.
        """
        return {**base, **overrides}

    def find_unprocessed(
        self,
        prefix: str,
        flag: str,
        lookback: int | None = None,
        skip_latest: bool = True,
    ) -> list[tuple[str, dict]]:
        """Scan a metadata prefix and return (key, metadata) pairs where
        *flag* is falsy (False, missing, or None).

        Args:
            prefix:       S3 key prefix to scan (e.g. "metadata/watermark/bronze/my_table/")
            flag:         Metadata field to check (e.g. "loaded_to_silver")
            lookback:     If set, only inspect the last N keys (by S3 list order).
            skip_latest:  Skip keys ending with '_latest.json' (watermark pointers).
        """
        keys = self.s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=prefix) or []
        keys = [k for k in keys if k.endswith(".json")]
        if skip_latest:
            keys = [k for k in keys if not k.endswith("_latest.json")]
        if lookback:
            keys = keys[-lookback:]

        unprocessed = []
        for key in keys:
            metadata = self.read_metadata(key)
            if metadata is None:
                continue
            if not metadata.get(flag, False):
                unprocessed.append((key, metadata))
        return unprocessed

    def mark_loaded(self, key: str, metadata: dict, flag: str) -> None:
        """Set *flag* = True on *metadata* and persist it back to S3."""
        metadata[flag] = True
        self.write_metadata(key=key, metadata_dict=metadata)
