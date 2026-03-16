"""Cross-layer data validation for the telecom ETL pipeline.

Bronze  → schema checks  (expected columns present in Parquet)
Silver  → data-quality   (range, domain, nullability) → appends is_valid + quality_issues
Gold    → sanity checks   (post-aggregation audit)

Usage
-----
    from common.validators import (
        BRONZE_SCHEMA,
        DEFAULT_PROFILE,
        validate_traffic,
        validate_metrics,
        validate_events,
        check_bronze_schema,
    )

The silver_transformer delegates to validate_* instead of embedding rules
inline.  Thresholds are configurable via QualityProfile — swap in a stricter
or looser profile per environment without changing code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

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
# Bronze Schema Validator
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class BronzeSchema:
    """Expected columns per table in bronze Parquet files.

    All columns are listed as they come out of the PostgreSQL source.
    The check is *subset*-based: extra columns are tolerated (e.g. Hive
    partition columns added by the extractor), but missing ones are flagged.
    """

    traffic_columns: frozenset[str] = frozenset({
        "traffic_id", "station_id", "event_time", "imsi_hash",
        "tmsi", "ip_address", "destination_ip", "destination_port",
        "protocol", "bytes_up", "bytes_down", "packets_up", "packets_down",
        "latency_ms", "jitter_ms", "packet_loss_pct", "connection_duration_ms", 
        "is_deleted", "created_at", "updated_at",
    })

    metrics_columns: frozenset[str] = frozenset({
        "metric_id", "station_id", "metric_time",
        "cpu_usage_pct", "memory_usage_pct", "disk_usage_pct",
        "temperature_celsius", "power_consumption_watts",
        "uplink_throughput_mbps", "downlink_throughput_mbps",
        "active_subscribers", "signal_strength_dbm",
        "frequency_band", "channel_utilization_pct", 
        "is_deleted","created_at", "updated_at",
    })

    events_columns: frozenset[str] = frozenset({
        "event_id", "station_id", "event_time", "event_type", "severity",
        "description", "metadata", "target_station_id", 
        "is_deleted", "created_at", "updated_at",
    })

    def columns_for(self, table: str) -> frozenset[str]:
        """Return expected columns for a table name."""
        mapping = {
            "subscriber_traffic": self.traffic_columns,
            "performance_metrics": self.metrics_columns,
            "station_events": self.events_columns,
        }
        result = mapping.get(table)
        if result is None:
            raise ValueError(
                f"Unknown table '{table}'. "
                f"Expected one of: {', '.join(sorted(mapping))}"
            )
        return result


BRONZE_SCHEMA = BronzeSchema()


def check_bronze_schema(
    df: pd.DataFrame,
    table: str,
    schema: BronzeSchema = BRONZE_SCHEMA,
) -> tuple[bool, set[str]]:
    """Verify a bronze DataFrame has the required columns.

    Returns
    -------
    (is_valid, missing_columns)
        is_valid is True when all expected columns are present.
        missing_columns is the set of columns that are absent.
    """
    expected = schema.columns_for(table)
    actual = frozenset(df.columns)
    missing = expected - actual
    if missing:
        logger.warning(
            "Bronze schema check failed for %s — missing: %s",
            table, sorted(missing),
        )
    return len(missing) == 0, missing


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

def _build_quality_string(conditions: dict[str, pd.Series], n_rows: int) -> pd.Series:
    """Concatenate condition labels into a comma-separated quality_issues column.

    Each *conditions* value is a boolean Series where True = issue present.
    The result is a string like ``"bytes_up<0,latency_invalid"`` or ``""``
    when the row is clean.
    """
    if not conditions:
        return pd.Series([""] * n_rows, dtype="string")

    label_series = [
        mask.map({True: label, False: ""})
        for label, mask in conditions.items()
    ]
    result = label_series[0].str.cat(label_series[1:], sep=",")
    return result.str.replace(r",+", ",", regex=True).str.strip(",")


def _normalize_event_time(
    series: pd.Series,
    tz_source: str,
) -> pd.Series:
    """Parse event_time, localise as *tz_source*, then convert to UTC.

    PostgreSQL exports timestamps that carry the *value* of the local clock
    but may arrive either tz-naïve or mislabelled as UTC.  This helper
    ensures they end up as proper UTC-aware values.
    """
    s = pd.to_datetime(series, errors="coerce")
    if s.dt.tz is not None:
        s = s.dt.tz_localize(None)
    s = s.dt.tz_localize(tz_source).dt.tz_convert("UTC")
    return s


def _check_range(
    df: pd.DataFrame,
    column: str,
    low: float,
    high: float,
    label: str,
) -> Optional[pd.Series]:
    """Return a boolean mask for out-of-range values, or None if column absent."""
    if column not in df.columns:
        return None
    return ~df[column].between(low, high)


# ══════════════════════════════════════════════════════════════════════════════
# Silver Validation Functions
# ══════════════════════════════════════════════════════════════════════════════

def validate_traffic(
    df: pd.DataFrame,
    profile: QualityProfile = DEFAULT_PROFILE,
) -> pd.DataFrame:
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
    df = df.copy()

    # ── 1. Timezone normalisation ──────────────────────────────────────
    df["event_time"] = _normalize_event_time(df["event_time"], profile.timezone_source)
    now = pd.Timestamp.now(tz="UTC")
    future_cutoff = now + pd.Timedelta(seconds=profile.max_future_tolerance_seconds)

    # ── 2-5. Quality conditions ────────────────────────────────────────
    conditions: dict[str, pd.Series] = {}

    # Required timestamp
    conditions["event_null"] = df["event_time"].isnull()
    conditions["event_future"] = df["event_time"] > future_cutoff

    # Byte counts
    if "bytes_up" in df.columns:
        conditions["bytes_up<0"] = df["bytes_up"] < profile.bytes_min
    if "bytes_down" in df.columns:
        conditions["bytes_down<0"] = df["bytes_down"] < profile.bytes_min

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
            df["protocol"].isnull()
            | ~df["protocol"].isin(profile.valid_protocols)
        )

    # Station reference
    if "station_id" in df.columns:
        conditions["station_null"] = df["station_id"].isnull()

    # IMSI hash
    if "imsi_hash" in df.columns:
        conditions["imsi_null"] = df["imsi_hash"].isnull() | (df["imsi_hash"] == "")

    # Created_at/updated_at timestamps
    if "created_at" in df.columns and "updated_at" in df.columns:
        created = _normalize_event_time(df["created_at"], profile.timezone_source)
        updated = _normalize_event_time(df["updated_at"], profile.timezone_source)
        conditions["updated_before_created"] = updated < created

    # ── Compose ────────────────────────────────────────────────────────
    df["is_valid"] = ~pd.concat(conditions.values(), axis=1).any(axis=1)
    df["quality_issues"] = _build_quality_string(conditions, len(df))

    invalid_count = (~df["is_valid"]).sum()
    if invalid_count:
        logger.info(
            "Traffic validation: %d / %d rows invalid (%.1f%%)",
            invalid_count, len(df), invalid_count / max(len(df), 1) * 100,
        )

    return df


def validate_metrics(
    df: pd.DataFrame,
    profile: QualityProfile = DEFAULT_PROFILE,
) -> pd.DataFrame:
    """Validate a bronze metrics DataFrame for silver.

    Column names follow the PostgreSQL source schema (cpu_usage_pct,
    memory_usage_pct, etc.).  The staging DDL may rename them later.
    The checks are defensive — missing columns are silently skipped.
    """
    df = df.copy()

    # Normalise timestamp
    time_col = "metric_time" if "metric_time" in df.columns else "event_time"
    if time_col in df.columns:
        df[time_col] = _normalize_event_time(df[time_col], profile.timezone_source)

    conditions: dict[str, pd.Series] = {}

    # ── Null checks ────────────────────────────────────────────────────
    if time_col in df.columns:
        conditions["time_null"] = df[time_col].isnull()
    if "station_id" in df.columns:
        conditions["station_null"] = df["station_id"].isnull()

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

    for col, (lo, hi), label in range_checks:
        if col in df.columns and label not in conditions:
            mask = _check_range(df, col, lo, hi, label)
            if mask is not None:
                conditions[label] = mask

    # ── Domain checks ──────────────────────────────────────────────────
    if "frequency_band" in df.columns:
        conditions["band_invalid"] = (
            df["frequency_band"].isnull()
            | ~df["frequency_band"].isin(profile.valid_frequency_bands)
        )

    # Active subscribers can't be negative
    for col in ("active_subscribers", "active_connections"):
        if col in df.columns:
            conditions["subscribers_negative"] = df[col] < 0
            break  # only one will exist

    # ── Compose ────────────────────────────────────────────────────────
    if conditions:
        df["is_valid"] = ~pd.concat(conditions.values(), axis=1).any(axis=1)
    else:
        df["is_valid"] = True
    df["quality_issues"] = _build_quality_string(conditions, len(df))

    invalid_count = (~df["is_valid"]).sum()
    if invalid_count:
        logger.info(
            "Metrics validation: %d / %d rows invalid (%.1f%%)",
            invalid_count, len(df), invalid_count / max(len(df), 1) * 100,
        )

    return df


def validate_events(
    df: pd.DataFrame,
    profile: QualityProfile = DEFAULT_PROFILE,
) -> pd.DataFrame:
    """Validate a bronze events DataFrame for silver."""
    df = df.copy()

    # Normalise timestamp
    if "event_time" in df.columns:
        df["event_time"] = _normalize_event_time(df["event_time"], profile.timezone_source)

    conditions: dict[str, pd.Series] = {}

    # ── Required columns ───────────────────────────────────────────────
    conditions["event_time_null"] = df["event_time"].isnull()

    if "station_id" in df.columns:
        conditions["station_null"] = df["station_id"].isnull()

    # ── Domain checks ──────────────────────────────────────────────────
    if "event_type" in df.columns:
        conditions["event_type_invalid"] = (
            df["event_type"].isnull()
            | ~df["event_type"].isin(profile.valid_event_types)
        )

    if "severity" in df.columns:
        conditions["severity_invalid"] = (
            df["severity"].isnull()
            | ~df["severity"].isin(profile.valid_severities)
        )

    # ── Future-timestamp guard ─────────────────────────────────────────
    if "event_time" in df.columns:
        now = pd.Timestamp.now(tz="UTC")
        future_cutoff = now + pd.Timedelta(seconds=profile.max_future_tolerance_seconds)
        conditions["event_future"] = df["event_time"] > future_cutoff

    # ── Compose ────────────────────────────────────────────────────────
    df["is_valid"] = ~pd.concat(conditions.values(), axis=1).any(axis=1)
    df["quality_issues"] = _build_quality_string(conditions, len(df))

    invalid_count = (~df["is_valid"]).sum()
    if invalid_count:
        logger.info(
            "Events validation: %d / %d rows invalid (%.1f%%)",
            invalid_count, len(df), invalid_count / max(len(df), 1) * 100,
        )

    return df


# ══════════════════════════════════════════════════════════════════════════════
# Gold Sanity Checks
# ══════════════════════════════════════════════════════════════════════════════

def check_gold_health_hourly(df: pd.DataFrame) -> list[str]:
    """Post-aggregation sanity checks on gold_health_hourly.

    Returns a list of warning strings (empty = all good).
    Use after the gold aggregator writes to ClickHouse or Parquet.
    """
    warnings: list[str] = []

    if df.empty:
        warnings.append("gold_health_hourly is empty")
        return warnings

    if "avg_cpu_pct" in df.columns:
        bad = df[~df["avg_cpu_pct"].between(0, 100)]
        if len(bad):
            warnings.append(f"{len(bad)} rows with avg_cpu_pct outside [0,100]")

    if "avg_latency_ms" in df.columns:
        bad = df[df["avg_latency_ms"] < 0]
        if len(bad):
            warnings.append(f"{len(bad)} rows with negative avg_latency_ms")

    if "total_bytes" in df.columns:
        bad = df[df["total_bytes"] < 0]
        if len(bad):
            warnings.append(f"{len(bad)} rows with negative total_bytes")

    if "station_count" in df.columns:
        bad = df[df["station_count"] <= 0]
        if len(bad):
            warnings.append(f"{len(bad)} rows with station_count <= 0")

    return warnings


def check_gold_sla_compliance(df: pd.DataFrame) -> list[str]:
    """Sanity checks on gold_sla_compliance."""
    warnings: list[str] = []

    if df.empty:
        warnings.append("gold_sla_compliance is empty")
        return warnings

    for col in ("availability_pct", "uptime_pct"):
        if col in df.columns:
            bad = df[~df[col].between(0, 100)]
            if len(bad):
                warnings.append(f"{len(bad)} rows with {col} outside [0,100]")

    return warnings


def check_gold_anomaly_features(df: pd.DataFrame) -> list[str]:
    """Sanity checks on gold_anomaly_features."""
    warnings: list[str] = []

    if df.empty:
        warnings.append("gold_anomaly_features is empty")
        return warnings

    for col in ("z_cpu", "z_latency", "z_throughput"):
        if col in df.columns:
            extreme = df[df[col].abs() > 10]
            if len(extreme):
                warnings.append(
                    f"{len(extreme)} rows with |{col}| > 10 — check for data issues"
                )

    return warnings


# ══════════════════════════════════════════════════════════════════════════════
# Dimension Validators (for base_station, operator, location tables)
# ══════════════════════════════════════════════════════════════════════════════

def validate_station_dimension(df: pd.DataFrame) -> list[str]:
    """Check a base_station dimension snapshot for consistency.

    Useful as a pre-flight check before silver enrichment.
    """
    warnings: list[str] = []

    if df.empty:
        warnings.append("Station dimension is empty")
        return warnings

    if "technology" in df.columns:
        invalid_tech = set(df["technology"].dropna().unique()) - VALID_TECHNOLOGIES
        if invalid_tech:
            warnings.append(f"Unknown technologies: {sorted(invalid_tech)}")

    if "status" in df.columns:
        invalid_status = set(df["status"].dropna().unique()) - VALID_STATION_STATUSES
        if invalid_status:
            warnings.append(f"Unknown statuses: {sorted(invalid_status)}")

    if "station_code" in df.columns:
        dupes = df["station_code"].duplicated().sum()
        if dupes:
            warnings.append(f"{dupes} duplicate station_code values")

    if "latitude" in df.columns and "longitude" in df.columns:
        # Vietnam bounding box (rough)
        out_lat = ~df["latitude"].between(8.0, 24.0)
        out_lon = ~df["longitude"].between(102.0, 110.0)
        out_of_bounds = (out_lat | out_lon).sum()
        if out_of_bounds:
            warnings.append(
                f"{out_of_bounds} stations outside Vietnam bounding box"
            )

    return warnings