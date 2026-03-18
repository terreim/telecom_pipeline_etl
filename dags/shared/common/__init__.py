"""Common utilities for the telecom data pipeline ETL."""

# common/__init__.py
from shared.common.config import PipelineConfig, CFG
from shared.common.connections import get_s3_hook, get_s3_credentials, pg_cursor
from shared.common.dag_defaults import BRONZE_DEFAULTS, SILVER_DEFAULTS, GOLD_DEFAULTS, RECOVERY_DEFAULTS
from shared.common.assets import build_assets
from shared.common.metadata import MetadataManager
from shared.common.validators import validate_traffic, validate_metrics, validate_events

__all__ = [
    "PipelineConfig",
    "CFG",
    "get_s3_hook",
    "get_s3_credentials",
    "pg_cursor",
    "BRONZE_DEFAULTS",
    "SILVER_DEFAULTS",
    "GOLD_DEFAULTS",
    "RECOVERY_DEFAULTS",
    "build_assets",
    "MetadataManager",
    "validate_traffic",
    "validate_metrics",
    "validate_events",
]

