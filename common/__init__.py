"""Common utilities for the telecom data pipeline ETL."""

# common/__init__.py
from common.config import PipelineConfig, CFG
from common.connections import get_s3_hook, get_s3_credentials, pg_cursor
from common.dag_defaults import BRONZE_DEFAULTS, SILVER_DEFAULTS, GOLD_DEFAULTS, RECOVERY_DEFAULTS
from common.assets import build_assets
from common.metadata import MetadataManager
from common.validators import validate_traffic, validate_metrics, validate_events

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

