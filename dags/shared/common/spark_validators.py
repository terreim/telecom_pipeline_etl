"""Cross-layer data validation for the telecom ETL pipeline.

Silver  → data-quality   (range, domain, nullability) → appends is_valid + quality_issues

Usage
-----
    from common.spark_validators import (
        DEFAULT_PROFILE,
        validate_traffic,
        validate_metrics,
        validate_events,
    )

The silver_transformer delegates to validate_* instead of embedding rules
inline.  Thresholds are configurable via QualityProfile — swap in a stricter
or looser profile per environment without changing code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import reduce as py_reduce
from typing import Optional

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    lit,
    col,
    concat_ws,
    when,
    to_utc_timestamp,
    current_timestamp,
    timestamp_add,
)

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Domain Constants — single source of truth across all layers
# ══════════════════════════════════════════════════════════════════════════════

VALID_EVENT_TYPES: frozenset[str] = frozenset({
    # Core radio events
    "handover",
    "attach",
    "detach",
    "paging",
    "alarm",
    "config_change",
    # Incidents
    "incident_start",
    "incident_end",
    # Maintenance
    "maintenance_start",
    "maintenance_end",
    # Station lifecycle
    "station_commissioned",
    "station_testing",
    "station_go_live",
    "station_decommission",
    "station_retired",
    # Cluster outage
    "cluster_outage_start",
    "cluster_outage_end",
    # Weather
    "weather_degradation",
    "weather_clear",
    # Mass gatherings
    "mass_event_start",
    "mass_event_end",
    # Firmware
    "firmware_bug_start",
    "firmware_reboot",
    "firmware_hotfix",
})

VALID_SEVERITIES: frozenset[str] = frozenset({
    "debug", "info", "warning", "error", "critical",
})

VALID_PROTOCOLS: frozenset[str] = frozenset({
    "TCP", "UDP", "ICMP", "OTHER",
})

VALID_TECHNOLOGIES: frozenset[str] = frozenset({
    "2G", "3G", "4G", "5G",
})

VALID_FREQUENCY_BANDS: frozenset[str] = frozenset({
    # 2G
    "GSM-900", "GSM-1800",
    # 3G
    "UMTS-2100", "UMTS-900",
    # 4G
    "Band-3", "Band-7", "Band-20",
    # 5G
    "n78", "n258", "n77",
    # Fallback
    "unknown",
})

VALID_STATION_STATUSES: frozenset[str] = frozenset({
    "planned", "provisioning", "testing", "active",
    "degraded", "maintenance", "down",
    "decommissioning", "retired",
})

VALID_DENSITIES: frozenset[str] = frozenset({
    "dense_urban", "urban_core", "urban", "suburban", "rural", "highway",
})

# ══════════════════════════════════════════════════════════════════════════════
# Silver Quality Profile
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class QualityProfile:
    """Configurable thresholds for silver-layer data-quality checks.

    Every range is (min, max) inclusive.  Swap in a custom profile for
    dev/staging/prod without changing validation logic.
    """

    # ── Traffic thresholds ─────────────────────────────────────────────
    latency_range: tuple[float, float] = (0.0, 10_000.0)
    jitter_range: tuple[float, float] = (0.0, 5_000.0)
    packet_loss_range: tuple[float, float] = (0.0, 100.0)
    bytes_min: int = 0
    connection_duration_range: tuple[int, int] = (0, 86_400_000)  # 0 – 24h in ms
    valid_protocols: frozenset[str] = VALID_PROTOCOLS

    # ── Metrics thresholds ─────────────────────────────────────────────
    cpu_range: tuple[float, float] = (0.0, 100.0)
    memory_range: tuple[float, float] = (0.0, 100.0)
    disk_range: tuple[float, float] = (0.0, 100.0)
    temperature_range: tuple[float, float] = (-20.0, 95.0)
    power_range: tuple[float, float] = (0.0, 10_000.0)
    throughput_range: tuple[float, float] = (0.0, 100_000.0)  # Mbps
    signal_strength_range: tuple[float, float] = (-120.0, -30.0)
    channel_util_range: tuple[float, float] = (0.0, 100.0)
    valid_frequency_bands: frozenset[str] = VALID_FREQUENCY_BANDS

    # ── Events thresholds ──────────────────────────────────────────────
    valid_event_types: frozenset[str] = VALID_EVENT_TYPES
    valid_severities: frozenset[str] = VALID_SEVERITIES

    # ── Cross-table ────────────────────────────────────────────────────
    max_future_tolerance_seconds: int = 300  # allow 5 min clock skew
    timezone_source: str = "Asia/Ho_Chi_Minh"  # TZ of raw values from client


DEFAULT_PROFILE = QualityProfile()


# Stricter profile for production alerting (example)
STRICT_PROFILE = QualityProfile(
    latency_range=(0.0, 5_000.0),
    jitter_range=(0.0, 1_000.0),
    temperature_range=(-10.0, 85.0),
    max_future_tolerance_seconds=120,
)


# ══════════════════════════════════════════════════════════════════════════════
# Internal Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _build_quality_string(conditions: dict[str, Column]) -> Column:
    """Concatenate condition labels into a comma-separated quality_issues column.

    Each *conditions* value is a boolean Column where True = issue present.
    The result is a string like ``"bytes_up<0,latency_invalid"`` or ``""``
    when the row is clean. Requires all Column to have the same length
    """
    if not conditions:
        return lit("")
    
    label_cols = [
        when(mask == True, lit(label))  # noqa: E712
        for label, mask in conditions.items()
    ]

    return concat_ws(",", *label_cols)


def _normalize_event_time(
    column: Column,
    tz_source: str,
) -> Column:
    """Parse event_time, localise as *tz_source*, then convert to UTC.

    PostgreSQL exports timestamps that carry the *value* of the local clock
    but may arrive either tz-naïve or mislabelled as UTC.  This helper
    ensures they end up as proper UTC-aware values.
    """
    ts = column.cast("timestamp")
    return to_utc_timestamp(ts, tz_source)

def _check_range(
    df: DataFrame,
    column: str,
    low: float,
    high: float,
    label: Optional[str] = None,
) -> Optional[Column]:
    """Return a boolean mask for out-of-range values, or None if column absent."""
    if column not in df.columns:
        return None
    return ~df[column].between(low, high)


# ══════════════════════════════════════════════════════════════════════════════
# Silver Validation Functions
# ══════════════════════════════════════════════════════════════════════════════

def validate_traffic(
    df: DataFrame,
    profile: QualityProfile = DEFAULT_PROFILE,
) -> DataFrame:
    """Validate & normalise a bronze traffic DataFrame for silver.

    Appends columns: ``is_valid`` (bool), ``quality_issues`` (str).

    Steps
    -----
    1. Normalise event_time → UTC.
    2. Check numeric ranges (bytes, latency, jitter, packet_loss, duration).
    3. Check domain values (protocol).
    4. Null checks on required columns.
    5. Future-timestamp check.
    """

    # ── 1. Timezone normalisation ──────────────────────────────────────
    df = df.withColumn("event_time", _normalize_event_time(col("event_time"), profile.timezone_source))
    now = current_timestamp()
    future_cutoff = timestamp_add("SECOND", lit(profile.max_future_tolerance_seconds), now)

    # ── 2-5. Quality conditions ────────────────────────────────────────

    # Required timestamp
    conditions: dict[str, Column] = {}
    conditions["event_time_null"] = col("event_time").isNull()
    conditions["event_future"] = col("event_time") > future_cutoff

    # Byte counts
    if "bytes_up" in df.columns:
        conditions["bytes_up<0"] = col("bytes_up") < profile.bytes_min
    if "bytes_down" in df.columns:
        conditions["bytes_down<0"] = col("bytes_down") < profile.bytes_min

    # Latency
    mask = _check_range(df, "latency_ms", *profile.latency_range, "latency_invalid")
    if mask is not None:
        conditions["latency_invalid"] = mask

    # Jitter
    mask = _check_range(df, "jitter_ms", *profile.jitter_range, "jitter_invalid")
    if mask is not None:
        conditions["jitter_invalid"] = mask

    # Packet loss
    mask = _check_range(
        df, "packet_loss_pct", *profile.packet_loss_range, "pkt_loss_invalid",
    )
    if mask is not None:
        conditions["pkt_loss_invalid"] = mask

    # Connection duration
    mask = _check_range(
        df, "connection_duration_ms",
        float(profile.connection_duration_range[0]),
        float(profile.connection_duration_range[1]),
        "duration_invalid",
    )
    if mask is not None:
        conditions["duration_invalid"] = mask

    # Protocol domain
    if "protocol" in df.columns:
        conditions["protocol_invalid"] = (
            col("protocol").isNull()
            | ~col("protocol").isin(list(profile.valid_protocols))
        )

    # Station reference
    if "station_id" in df.columns:
        conditions["station_null"] = col("station_id").isNull()

    # IMSI hash
    if "imsi_hash" in df.columns:
        conditions["imsi_null"] = col("imsi_hash").isNull() | (col("imsi_hash") == "")

    # Created_at/updated_at timestamps
    if "created_at" in df.columns and "updated_at" in df.columns:
        created = _normalize_event_time(col("created_at"), profile.timezone_source)
        updated = _normalize_event_time(col("updated_at"), profile.timezone_source)
        conditions["updated_before_created"] = updated < created

    # ── Compose ────────────────────────────────────────────────────────
    if conditions:
        df = df.withColumn("is_valid", ~py_reduce(lambda a, b: a | b, conditions.values()))
    else:
        df = df.withColumn("is_valid", lit(True))

    df = df.withColumn("quality_issues", _build_quality_string(conditions))

    invalid_count = df.filter(~col("is_valid")).count()

    if invalid_count:
        logger.info(
            "Traffic validation: %d rows invalid", invalid_count
        )

    return df


def validate_metrics(
    df: DataFrame,
    profile: QualityProfile = DEFAULT_PROFILE,
) -> DataFrame:
    """Validate a bronze metrics DataFrame for silver.

    Column names follow the PostgreSQL source schema (cpu_usage_pct,
    memory_usage_pct, etc.).  The staging DDL may rename them later.
    The checks are defensive — missing columns are silently skipped.
    """

    # Normalise timestamp
    time_col = "metric_time" if "metric_time" in df.columns else "event_time"
    if time_col in df.columns:
        df = df.withColumn(time_col, _normalize_event_time(col(time_col), profile.timezone_source))

    conditions: dict[str, Column] = {}

    # ── Null checks ────────────────────────────────────────────────────
    if time_col in df.columns:
        conditions["time_null"] = col(time_col).isNull()
    if "station_id" in df.columns:
        conditions["station_null"] = col("station_id").isNull()

    # ── Range checks (postgres column names) ───────────────────────────
    range_checks = [
        ("cpu_usage_pct",            profile.cpu_range,             "cpu_invalid"),
        ("memory_usage_pct",         profile.memory_range,          "memory_invalid"),
        ("disk_usage_pct",           profile.disk_range,            "disk_invalid"),
        ("temperature_celsius",      profile.temperature_range,     "temp_invalid"),
        ("power_consumption_watts",  profile.power_range,           "power_invalid"),
        ("uplink_throughput_mbps",   profile.throughput_range,      "uplink_invalid"),
        ("downlink_throughput_mbps", profile.throughput_range,       "downlink_invalid"),
        ("signal_strength_dbm",      profile.signal_strength_range,  "signal_invalid"),
        ("channel_utilization_pct",  profile.channel_util_range,    "channel_invalid"),
        # Staging column names (if renaming already happened)
        ("cpu_util_pct",             profile.cpu_range,             "cpu_invalid"),
        ("memory_util_pct",          profile.memory_range,          "memory_invalid"),
        ("disk_util_pct",            profile.disk_range,            "disk_invalid"),
        ("temperature_c",            profile.temperature_range,     "temp_invalid"),
    ]

    for range_col, (lo, hi), label in range_checks:
        if range_col in df.columns and label not in conditions:
            mask = _check_range(df, range_col, lo, hi, label)
            if mask is not None:
                conditions[label] = mask

    # ── Domain checks ──────────────────────────────────────────────────
    if "frequency_band" in df.columns:
        conditions["band_invalid"] = (
            col("frequency_band").isNull()
            | ~col("frequency_band").isin(list(profile.valid_frequency_bands))
        )

    # Active subscribers can't be negative
    for neg_col in ("active_subscribers", "active_connections"):
        if neg_col in df.columns:
            conditions["subscribers_negative"] = col(neg_col) < 0
            break  # only one will exist

    # ── Compose ────────────────────────────────────────────────────────
    if conditions:
        df = df.withColumn("is_valid", ~py_reduce(lambda a, b: a | b, conditions.values()))
    else:
        df = df.withColumn("is_valid", lit(True))

    df = df.withColumn("quality_issues", _build_quality_string(conditions))

    invalid_count = df.filter(~col("is_valid")).count()
    
    if invalid_count:
        logger.info(
            "Metrics validation: %d rows invalid", invalid_count
        )

    return df


def validate_events(
    df: DataFrame,
    profile: QualityProfile = DEFAULT_PROFILE,
) -> DataFrame:
    """Validate a bronze events DataFrame for silver."""

    # Normalise timestamp
    if "event_time" in df.columns:
        df = df.withColumn("event_time", _normalize_event_time(col("event_time"), profile.timezone_source))

    conditions: dict[str, Column] = {}

    # ── Required columns ───────────────────────────────────────────────
    conditions["event_time_null"] = col("event_time").isNull()

    if "station_id" in df.columns:
        conditions["station_null"] = col("station_id").isNull()

    # ── Domain checks ──────────────────────────────────────────────────
    if "event_type" in df.columns:
        conditions["event_type_invalid"] = (
            col("event_type").isNull()
            | ~col("event_type").isin(list(profile.valid_event_types))
        )

    if "severity" in df.columns:
        conditions["severity_invalid"] = (
            col("severity").isNull()
            | ~col("severity").isin(list(profile.valid_severities))
        )

    # ── Future-timestamp guard ─────────────────────────────────────────
    if "event_time" in df.columns:
        now = current_timestamp()
        future_cutoff = timestamp_add("SECOND", lit(profile.max_future_tolerance_seconds), now)
        conditions["event_future"] = col("event_time") > future_cutoff

    # ── Compose ────────────────────────────────────────────────────────
    if conditions:
        df = df.withColumn("is_valid", ~py_reduce(lambda a, b: a | b, conditions.values()))
    else:
        df = df.withColumn("is_valid", lit(True))

    df = df.withColumn("quality_issues", _build_quality_string(conditions))

    invalid_count = df.filter(~col("is_valid")).count()

    if invalid_count:
        logger.info(
            "Events validation: %d rows invalid", invalid_count
        )

    return df


# ══════════════════════════════════════════════════════════════════════════════
# Dimension Validators (for base_station, operator, location tables)
# ══════════════════════════════════════════════════════════════════════════════

def validate_station_dimension(df: DataFrame) -> list[str]:
    """Check a base_station dimension snapshot for consistency.

    Useful as a pre-flight check before silver enrichment.
    """
    warnings: list[str] = []

    if df.isEmpty():
        warnings.append("Station dimension is empty")
        return warnings

    if "technology" in df.columns:
        invalid_tech = set(row[0] for row in df.select("technology").dropna().distinct().collect()) - VALID_TECHNOLOGIES
        if invalid_tech:
            warnings.append(f"Unknown technologies: {sorted(invalid_tech)}")

    if "status" in df.columns:
        invalid_status = set(row[0] for row in df.select("status").dropna().distinct().collect()) - VALID_STATION_STATUSES
        if invalid_status:
            warnings.append(f"Unknown statuses: {sorted(invalid_status)}")

    if "station_code" in df.columns:
        dupes = df.groupBy("station_code").count().where(col("count") > 1).count()
        if dupes:
            warnings.append(f"{dupes} duplicate station_code values")

    if "latitude" in df.columns and "longitude" in df.columns:
        # Vietnam bounding box (rough)
        out_lat = ~col("latitude").between(8.0, 24.0)
        out_lon = ~col("longitude").between(102.0, 110.0)
        out_of_bounds = df.filter(out_lat | out_lon).count()
        if out_of_bounds:
            warnings.append(
                f"{out_of_bounds} stations outside Vietnam bounding box"
            )

    return warnings