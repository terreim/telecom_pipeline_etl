"""
Telecom Station Simulator v5
=============================
Production-ready simulator with realistic operational scenarios.

Key improvements over v4:
─────────────────────────────────────────────────────────────────
• CLEAN SCHEMA  — No extracted_at / batch_id on source tables.
                  The ETL pipeline must never write to the client DB;
                  extraction is watermark-based on created_at only.
• Station lifecycle — PLANNED → PROVISIONING → TESTING → ACTIVE →
                      (DEGRADED | MAINTENANCE | DOWN) → DECOMMISSIONING → RETIRED
• New-station commissioning — Stations come online mid-simulation
                              (provisioning phase, testing traffic, go-live event)
• Station decommissioning  — Graceful shutdown: migrate subscribers,
                              reduce capacity, retire
• Cluster failures          — Regional power-outages / fiber-cuts knock
                              out multiple co-located stations simultaneously
• Weather events            — Rain / storm degrades signal across a region
                              (latency ↑, throughput ↓, packet-loss ↑)
• Mass gatherings           — Concerts, sports, festivals surge local traffic;
                              can overload stations causing degradation
• Firmware bugs             — Software rollout introduces periodic reboots
                              on a random subset of stations
• Late-arriving data        — Some records have deliberately delayed
                              created_at, simulating network-buffered uploads
• Data-quality anomalies    — Occasional null fields, out-of-range values
                              that test pipeline robustness

Data model (PostgreSQL `telecom` schema):
  telecom.subscriber_traffic  (high-volume fact)
  telecom.performance_metrics (medium-volume fact)
  telecom.station_events      (low-volume fact)
  telecom.base_station        (dimension, updated_at for SCD)
  telecom.operator            (dimension)
  telecom.location            (dimension)
  telecom.configuration       (SCD Type-2 dimension)

Usage:
    # Streaming mode — continuous, feeds the live pipeline
    python telecom_simulator_v5.py stream --stations 30 --duration 3600

    # Backfill mode — generate N days of historical data
    python telecom_simulator_v5.py backfill --stations 50 --start 2026-01-01 --days 7

    # With custom connection & scenario tuning
    python telecom_simulator_v5.py stream --host localhost --port 5433 \\
        --incident-prob 0.10 --cluster-event-prob 0.02

    # Run scenarios on top of existing data (append)
    python telecom_simulator_v5.py backfill --start 2026-01-05 --days 3 \\
        --mass-event-prob 0.05 --skip-init
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import signal as signal_module
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date, timezone
from enum import Enum
from typing import Optional, Generator

from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import pool


# ═════════════════════════════════════════════════════════════════════════════
# Constants & Configuration
# ═════════════════════════════════════════════════════════════════════════════

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")   # UTC+7

# ── Operators (realistic Vietnam market share) ──────────────────────────────
OPERATORS = [
    {"id": 1, "code": "VTL", "name": "Viettel"},
    {"id": 2, "code": "VNP", "name": "Vinaphone"},
    {"id": 3, "code": "MBF", "name": "MobiFone"},
    {"id": 4, "code": "FPT", "name": "FPT Telecom"},
    {"id": 5, "code": "ITL", "name": "iTelecom"},
    {"id": 6, "code": "VNM", "name": "Vietnamobile"},
]
OPERATOR_WEIGHTS = [0.45, 0.20, 0.18, 0.05, 0.04, 0.08]

# ── Locations with density & GPS centre ────────────────────────────────────
# Each location also has an approximate lat/lon centre for neighbour-finding.
LOCATIONS = [
    # Urban cores — high density
    {"id": 1,  "province": "Ha Noi",      "district": "Hoan Kiem",    "region": "North",   "density": "urban_core", "lat": 21.0285, "lon": 105.8542},
    {"id": 2,  "province": "Ha Noi",      "district": "Ba Dinh",      "region": "North",   "density": "urban_core", "lat": 21.0340, "lon": 105.8190},
    {"id": 3,  "province": "Ha Noi",      "district": "Cau Giay",     "region": "North",   "density": "urban_core", "lat": 21.0320, "lon": 105.7840},
    {"id": 4,  "province": "Ho Chi Minh", "district": "Quan 1",       "region": "South",   "density": "urban_core", "lat": 10.7769, "lon": 106.7009},
    {"id": 5,  "province": "Ho Chi Minh", "district": "Quan 3",       "region": "South",   "density": "urban_core", "lat": 10.7835, "lon": 106.6862},
    {"id": 6,  "province": "Ho Chi Minh", "district": "Binh Thanh",   "region": "South",   "density": "urban_core", "lat": 10.8108, "lon": 106.7091},
    {"id": 7,  "province": "Da Nang",     "district": "Hai Chau",     "region": "Central", "density": "urban_core", "lat": 16.0471, "lon": 108.2068},
    # Urban fringe
    {"id": 8,  "province": "Ha Noi",      "district": "Long Bien",    "region": "North",   "density": "urban",      "lat": 21.0456, "lon": 105.8904},
    {"id": 9,  "province": "Ha Noi",      "district": "Hoang Mai",    "region": "North",   "density": "urban",      "lat": 20.9770, "lon": 105.8430},
    {"id": 10, "province": "Ho Chi Minh", "district": "Quan 7",       "region": "South",   "density": "urban",      "lat": 10.7340, "lon": 106.7220},
    {"id": 11, "province": "Ho Chi Minh", "district": "Thu Duc",      "region": "South",   "density": "urban",      "lat": 10.8510, "lon": 106.7530},
    {"id": 12, "province": "Da Nang",     "district": "Son Tra",      "region": "Central", "density": "urban",      "lat": 16.1050, "lon": 108.2530},
    {"id": 13, "province": "Hai Phong",   "district": "Le Chan",      "region": "North",   "density": "urban",      "lat": 20.8499, "lon": 106.6881},
    {"id": 14, "province": "Can Tho",     "district": "Ninh Kieu",    "region": "South",   "density": "urban",      "lat": 10.0341, "lon": 105.7876},
    # Suburban
    {"id": 15, "province": "Binh Duong",  "district": "Thu Dau Mot",  "region": "South",   "density": "suburban",   "lat": 11.0063, "lon": 106.6525},
    {"id": 16, "province": "Dong Nai",    "district": "Bien Hoa",     "region": "South",   "density": "suburban",   "lat": 10.9574, "lon": 106.8429},
    {"id": 17, "province": "Bac Ninh",    "district": "Bac Ninh",     "region": "North",   "density": "suburban",   "lat": 21.1868, "lon": 106.0763},
    {"id": 18, "province": "Quang Ninh",  "district": "Ha Long",      "region": "North",   "density": "suburban",   "lat": 20.9590, "lon": 107.0448},
    {"id": 19, "province": "Khanh Hoa",   "district": "Nha Trang",    "region": "Central", "density": "suburban",   "lat": 12.2388, "lon": 109.1967},
    {"id": 20, "province": "Lam Dong",    "district": "Da Lat",       "region": "Central", "density": "suburban",   "lat": 11.9404, "lon": 108.4583},
    # Rural
    {"id": 21, "province": "Ha Giang",    "district": "Ha Giang",     "region": "North",   "density": "rural",      "lat": 22.8233, "lon": 104.9839},
    {"id": 22, "province": "Lai Chau",    "district": "Lai Chau",     "region": "North",   "density": "rural",      "lat": 22.3964, "lon": 103.4593},
    {"id": 23, "province": "Dak Nong",    "district": "Gia Nghia",    "region": "Central", "density": "rural",      "lat": 12.0030, "lon": 107.6903},
    {"id": 24, "province": "Ca Mau",      "district": "Ca Mau",       "region": "South",   "density": "rural",      "lat":  9.1765, "lon": 105.1524},
    {"id": 25, "province": "Kon Tum",     "district": "Kon Tum",      "region": "Central", "density": "rural",      "lat": 14.3545, "lon": 108.0072},
]

# ── Density profiles ───────────────────────────────────────────────────────
DENSITY_PROFILES = {
    "urban_core": {"traffic": 8.0, "subs": 400, "events_per_min": 0.50, "metric_noise": 0.15},
    "urban":      {"traffic": 4.0, "subs": 200, "events_per_min": 0.30, "metric_noise": 0.10},
    "suburban":   {"traffic": 2.0, "subs":  80, "events_per_min": 0.15, "metric_noise": 0.08},
    "rural":      {"traffic": 0.5, "subs":  20, "events_per_min": 0.05, "metric_noise": 0.05},
}

# ── Technology profiles ────────────────────────────────────────────────────
TECHNOLOGIES = {
    "2G": {"throughput_mult": 0.01, "latency_base": 150, "latency_std": 50,  "max_subs": 50},
    "3G": {"throughput_mult": 0.10, "latency_base": 80,  "latency_std": 30,  "max_subs": 150},
    "4G": {"throughput_mult": 1.00, "latency_base": 30,  "latency_std": 10,  "max_subs": 500},
    "5G": {"throughput_mult": 5.00, "latency_base": 10,  "latency_std": 3,   "max_subs": 1000},
}
TECH_WEIGHTS = [0.03, 0.07, 0.40, 0.50]
TECH_NAMES = ["2G", "3G", "4G", "5G"]

# ── Time-of-Day patterns (Vietnam local time, UTC+7) ──────────────────────
HOURLY_TRAFFIC_MULT = {
    0: 0.15,  1: 0.10,  2: 0.08,  3: 0.08,  4: 0.10,  5: 0.25,
    6: 0.45,  7: 0.70,  8: 0.80,  9: 0.65, 10: 0.60, 11: 0.65,
    12: 0.85, 13: 0.80, 14: 0.60, 15: 0.60, 16: 0.65, 17: 0.85,
    18: 0.90, 19: 0.95, 20: 1.00, 21: 0.95, 22: 0.70, 23: 0.45,
}
DAY_OF_WEEK_MULT = {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.05, 4: 1.10, 5: 0.85, 6: 0.80}
WEEKEND_HOUR_ADJ = {6: 0.5, 7: 0.6, 8: 0.7, 9: 0.85, 21: 1.1, 22: 1.15, 23: 1.05}

# ── Application traffic profiles (what kind of data subs generate) ─────────
APP_PROFILES = {
    "streaming":    {"bytes_up_mult": 0.1,  "bytes_down_mult": 5.0,  "duration_ms": (30_000, 600_000), "weight": 0.25},
    "social_media": {"bytes_up_mult": 1.5,  "bytes_down_mult": 3.0,  "duration_ms": (5_000, 120_000),  "weight": 0.30},
    "gaming":       {"bytes_up_mult": 0.8,  "bytes_down_mult": 1.2,  "duration_ms": (60_000, 1_800_000), "weight": 0.10},
    "voip":         {"bytes_up_mult": 0.3,  "bytes_down_mult": 0.3,  "duration_ms": (30_000, 900_000),  "weight": 0.10},
    "browsing":     {"bytes_up_mult": 0.2,  "bytes_down_mult": 1.0,  "duration_ms": (1_000, 60_000),   "weight": 0.20},
    "iot":          {"bytes_up_mult": 0.05, "bytes_down_mult": 0.05, "duration_ms": (100, 5_000),      "weight": 0.05},
}
APP_NAMES = list(APP_PROFILES.keys())
APP_WEIGHTS = [APP_PROFILES[n]["weight"] for n in APP_NAMES]

# ═════════════════════════════════════════════════════════════════════════════
# ★ TUNABLE SCENARIO KNOBS ★
# ═════════════════════════════════════════════════════════════════════════════

# ── Basic incidents (same as v4 but with knob names) ───────────────────────
INCIDENT_HOURLY_PROB       = 0.005
INCIDENT_TYPE_WEIGHTS      = [0.7, 0.3]          # [degradation, failure]

DEGRADATION_SEVERITY_RANGE = (0.3, 0.7)
DEGRADATION_DURATION_RANGE = (15, 180)            # minutes
FAILURE_SEVERITY_RANGE     = (0.8, 1.0)
FAILURE_DURATION_RANGE     = (5, 60)              # minutes
CRITICAL_SEVERITY_THRESHOLD = 0.7

# ── Maintenance ────────────────────────────────────────────────────────────
MAINTENANCE_DAILY_PROB     = 0.02
MAINTENANCE_HOUR_RANGE     = (1, 5)
MAINTENANCE_DURATION_RANGE = (30, 120)            # minutes
TECH_UPGRADE_PROB          = 0.20

# ── Traffic incident effects ──────────────────────────────────────────────
DEGRADATION_THROUGHPUT_FACTOR = 0.5
DEGRADATION_LATENCY_FACTOR    = 5.0
DEGRADATION_LOSS_FACTOR       = 10.0
FAILURE_THROUGHPUT_FACTOR     = 0.05
FAILURE_LATENCY_MULT          = 10.0
FAILURE_LOSS_MULT             = 20.0
HIGH_PACKET_LOSS_PROB         = 0.15

# ── Performance metric effects ────────────────────────────────────────────
DEGRADATION_CPU_SPIKE          = 30
FAILURE_CPU_RANGE              = (95, 100)
FAILURE_THROUGHPUT_PCT         = 0.01
DEGRADATION_THROUGHPUT_REDUCTION = 0.6

# ── NEW: Cluster failure knobs ────────────────────────────────────────────
CLUSTER_EVENT_HOURLY_PROB  = 0.003    # Probability a cluster event starts per hour
CLUSTER_EVENT_TYPES        = ["power_outage", "fiber_cut", "cooling_failure"]
CLUSTER_EVENT_TYPE_WEIGHTS = [0.50, 0.35, 0.15]
CLUSTER_POWER_DURATION     = (30, 240)            # minutes
CLUSTER_FIBER_DURATION     = (60, 480)            # minutes
CLUSTER_COOLING_DURATION   = (60, 180)            # minutes

# ── NEW: Weather event knobs ─────────────────────────────────────────────
WEATHER_EVENT_HOURLY_PROB  = 0.004
WEATHER_SEVERITY_RANGE     = (0.1, 0.6)           # relative signal degradation
WEATHER_DURATION_RANGE     = (60, 360)            # minutes
WEATHER_REGIONS            = ["North", "Central", "South"]

# ── NEW: Mass gathering knobs ────────────────────────────────────────────
MASS_EVENT_DAILY_PROB      = 0.03     # Per-location probability
MASS_EVENT_SUB_MULT        = (3.0, 8.0)           # subscriber surge multiplier
MASS_EVENT_DURATION        = (120, 360)           # minutes
MASS_EVENT_DENSITIES       = ["urban_core", "urban"]   # only these attract events

# ── NEW: Firmware bug knobs ──────────────────────────────────────────────
FIRMWARE_ROLLOUT_PROB      = 0.01     # Per-day probability of a firmware rollout
FIRMWARE_AFFECTED_PCT      = (0.10, 0.40)         # % of stations affected
FIRMWARE_REBOOT_INTERVAL   = (60, 240)            # minutes between reboots
FIRMWARE_REBOOT_DURATION   = (2, 5)               # minutes per reboot
FIRMWARE_BUG_LIFETIME      = (6, 48)              # hours before hotfix deployed

# ── NEW: Station lifecycle knobs ─────────────────────────────────────────
NEW_STATION_DAILY_PROB     = 0.02     # Per-day probability a new station is commissioned
DECOMMISSION_DAILY_PROB    = 0.005    # Per-day probability an old station is retired
PROVISIONING_HOURS         = (2, 8)               # Time in provisioning state
TESTING_HOURS              = (4, 24)              # Time in testing state

# ── NEW: Data-quality anomaly knobs ──────────────────────────────────────
LATE_ARRIVAL_PROB          = 0.02     # % of records that arrive late
LATE_ARRIVAL_DELAY         = (60, 900)            # seconds of delay
NULL_FIELD_PROB            = 0.005    # % of records with a random null field
DUPLICATE_RECORD_PROB      = 0.003    # % of records that get inserted twice

# ── Organic events ───────────────────────────────────────────────────────
ORGANIC_EVENT_WEIGHTS      = [0.30, 0.25, 0.20, 0.10, 0.15]
ALARM_SEVERITY_WEIGHTS     = [0.70, 0.25, 0.05]

# ── Subscribers ──────────────────────────────────────────────────────────
SUBSCRIBER_ACTIVE_FACTOR   = 0.6
TRAFFIC_SAMPLE_RATE        = 0.1
MOBILITY_GO_HOME_PROB      = 0.3
MOBILITY_BASE_RATE         = 0.02
COMMUTE_MOBILITY_RATES     = {7: 0.15, 8: 0.20, 9: 0.10, 17: 0.15, 18: 0.20, 19: 0.10}

# ── Network ──────────────────────────────────────────────────────────────
PROTOCOLS = ["TCP", "UDP", "ICMP", "OTHER"]
PROTOCOL_WEIGHTS = [0.70, 0.25, 0.03, 0.02]
COMMON_PORTS = [80, 443, 8080, 53, 3478, 5060, 1935, 554, 8443, 3000]

FREQUENCY_BANDS = {
    "2G": ["GSM-900", "GSM-1800"],
    "3G": ["UMTS-2100", "UMTS-900"],
    "4G": ["Band-3", "Band-7", "Band-20"],
    "5G": ["n78", "n258", "n77"],
}


# ═════════════════════════════════════════════════════════════════════════════
# Enums
# ═════════════════════════════════════════════════════════════════════════════

class EventType(Enum):
    HANDOVER            = "handover"
    ATTACH              = "attach"
    DETACH              = "detach"
    PAGING              = "paging"
    ALARM               = "alarm"
    CONFIG_CHANGE       = "config_change"
    MAINTENANCE_START   = "maintenance_start"
    MAINTENANCE_END     = "maintenance_end"
    INCIDENT_START      = "incident_start"
    INCIDENT_END        = "incident_end"
    # v5 additions
    STATION_COMMISSIONED = "station_commissioned"
    STATION_TESTING      = "station_testing"
    STATION_GO_LIVE      = "station_go_live"
    STATION_DECOMMISSION = "station_decommission"
    STATION_RETIRED      = "station_retired"
    CLUSTER_OUTAGE_START = "cluster_outage_start"
    CLUSTER_OUTAGE_END   = "cluster_outage_end"
    WEATHER_DEGRADATION  = "weather_degradation"
    WEATHER_CLEAR        = "weather_clear"
    MASS_EVENT_START     = "mass_event_start"
    MASS_EVENT_END       = "mass_event_end"
    FIRMWARE_BUG_START   = "firmware_bug_start"
    FIRMWARE_REBOOT      = "firmware_reboot"
    FIRMWARE_HOTFIX      = "firmware_hotfix"


class Severity(Enum):
    DEBUG    = "debug"
    INFO     = "info"
    WARNING  = "warning"
    ERROR    = "error"
    CRITICAL = "critical"


class StationStatus(Enum):
    PLANNED         = "planned"
    PROVISIONING    = "provisioning"
    TESTING         = "testing"
    ACTIVE          = "active"
    DEGRADED        = "degraded"
    MAINTENANCE     = "maintenance"
    DOWN            = "down"
    DECOMMISSIONING = "decommissioning"
    RETIRED         = "retired"


def get_time_multiplier(local_now: datetime) -> float:
    """Return a traffic intensity multiplier for the current time."""
    hour = local_now.hour
    dow = local_now.weekday()
    m = HOURLY_TRAFFIC_MULT.get(hour, 0.5) * DAY_OF_WEEK_MULT.get(dow, 1.0)
    if dow >= 5:
        m *= WEEKEND_HOUR_ADJ.get(hour, 1.0)
    return m


# ═════════════════════════════════════════════════════════════════════════════
# Station Personality — deterministic behavioural fingerprint
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class StationPersonality:
    hardware_quality: float       # 0–1, how reliable
    idle_cpu: float               # % CPU when idle
    temp_base: float              # base temperature °C
    temp_sensitivity: float       # how much temp rises with load
    alarm_tendency: float         # multiplier for organic event rate
    capacity_threshold: float     # fraction of max before degradation

    @staticmethod
    def from_seed(station_code: str) -> StationPersonality:
        rng = random.Random(hashlib.sha256(station_code.encode()).hexdigest())
        hw = rng.betavariate(3, 1.5)
        return StationPersonality(
            hardware_quality=hw,
            idle_cpu=rng.uniform(5, 25),
            temp_base=rng.uniform(30, 45),
            temp_sensitivity=rng.uniform(0.5, 2.0),
            alarm_tendency=rng.uniform(0.3, 3.0),
            capacity_threshold=0.6 + hw * 0.3,
        )


# ═════════════════════════════════════════════════════════════════════════════
# Station Runtime State
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class StationRuntime:
    station_id: int
    station_code: str
    operator: dict
    location: dict
    technology: str
    latitude: float
    longitude: float
    install_date: datetime
    personality: StationPersonality
    status: StationStatus = StationStatus.ACTIVE

    # Lifecycle timestamps
    commissioned_at: Optional[datetime] = None
    go_live_at: Optional[datetime] = None
    decommission_at: Optional[datetime] = None
    retired_at: Optional[datetime] = None

    # Subscriber tracking
    active_subscribers: set = field(default_factory=set)

    # Performance counters
    error_count: int = 0
    uptime_start: Optional[datetime] = None

    # Incident state
    incident_active: bool = False
    incident_type: Optional[str] = None
    incident_start: Optional[datetime] = None
    incident_severity: float = 0.0
    incident_end: Optional[datetime] = None

    # Maintenance state
    maintenance_active: bool = False
    maintenance_end: Optional[datetime] = None
    pre_maintenance_status: Optional[StationStatus] = None

    # Cluster-event state
    cluster_event_active: bool = False
    cluster_event_type: Optional[str] = None
    cluster_event_end: Optional[datetime] = None

    # Weather degradation
    weather_severity: float = 0.0
    weather_end: Optional[datetime] = None

    # Mass gathering
    mass_event_active: bool = False
    mass_event_sub_mult: float = 1.0
    mass_event_end: Optional[datetime] = None

    # Firmware bug
    firmware_bug_active: bool = False
    firmware_next_reboot: Optional[datetime] = None
    firmware_reboot_end: Optional[datetime] = None
    firmware_bug_end: Optional[datetime] = None

    # Neighbour refs
    neighbors: list = field(default_factory=list)

    def __post_init__(self):
        if self.uptime_start is None:
            self.uptime_start = datetime.now(timezone.utc)

    @property
    def is_operational(self) -> bool:
        """Returns True if this station can serve traffic (even degraded)."""
        return self.status in (
            StationStatus.ACTIVE,
            StationStatus.DEGRADED,
            StationStatus.TESTING,
        )


# ═════════════════════════════════════════════════════════════════════════════
# Subscriber Pool
# ═════════════════════════════════════════════════════════════════════════════

class SubscriberPool:
    """
    Persistent subscriber identities that move between stations.
    Each subscriber has a home station but can roam to neighbours.
    """

    def __init__(self, pool_size: int, stations: list[StationRuntime], rng: random.Random):
        self.rng = rng
        self.subscribers: dict[str, dict] = {}

        operational = [s for s in stations if s.is_operational]
        if not operational:
            return

        weights = []
        for s in operational:
            density = s.location.get("density", "urban")
            weights.append(DENSITY_PROFILES[density]["subs"])
        total_w = sum(weights) or 1

        for i in range(pool_size):
            imsi_hash = hashlib.sha256(f"sub-{i}".encode()).hexdigest()
            # Pick home station proportional to density
            r = rng.random() * total_w
            cumul = 0
            chosen = operational[0]
            for s, w in zip(operational, weights):
                cumul += w
                if r <= cumul:
                    chosen = s
                    break
            self.subscribers[imsi_hash] = {
                "home_station": chosen.station_code,
                "current_station": chosen.station_code,
                "app_preference": rng.choices(APP_NAMES, weights=APP_WEIGHTS)[0],
            }

    def get_active_subscribers(
        self,
        station: StationRuntime,
        hour: int,
        time_mult: float,
        rng: random.Random,
    ) -> list[str]:
        """Return list of imsi hashes currently at *station* and considered active."""
        at_station = [
            imsi for imsi, info in self.subscribers.items()
            if info["current_station"] == station.station_code
        ]
        active_frac = min(1.0, SUBSCRIBER_ACTIVE_FACTOR * time_mult)
        n_active = max(1, int(len(at_station) * active_frac))
        return rng.sample(at_station, min(n_active, len(at_station))) if at_station else []

    def simulate_mobility(
        self,
        stations: list[StationRuntime],
        hour: int,
        rng: random.Random,
    ):
        """Move some subscribers between stations."""
        mobility_rate = COMMUTE_MOBILITY_RATES.get(hour, MOBILITY_BASE_RATE)
        operational = [s for s in stations if s.is_operational]
        if not operational:
            return

        for imsi, info in self.subscribers.items():
            if rng.random() > mobility_rate:
                continue

            current = info["current_station"]
            home = info["home_station"]

            # Higher chance to go home
            if current != home and rng.random() < MOBILITY_GO_HOME_PROB:
                info["current_station"] = home
                continue

            # Find neighbouring operational stations in the same region
            current_station = next((s for s in stations if s.station_code == current), None)
            if current_station is None:
                continue

            neighbours = [
                s for s in operational
                if s.station_code != current
                and s.location.get("region") == current_station.location.get("region")
            ]
            if neighbours:
                info["current_station"] = rng.choice(neighbours).station_code

    def migrate_subscribers(
        self,
        from_station: StationRuntime,
        to_stations: list[StationRuntime],
        rng: random.Random,
    ):
        """Move all subscribers from *from_station* to operational neighbours."""
        targets = [s for s in to_stations if s.is_operational and s.station_code != from_station.station_code]
        if not targets:
            return
        for imsi, info in self.subscribers.items():
            if info["current_station"] == from_station.station_code:
                info["current_station"] = rng.choice(targets).station_code

    def add_subscribers(self, count: int, stations: list[StationRuntime], rng: random.Random):
        """Add new subscribers to the pool (e.g. when a new station comes online)."""
        operational = [s for s in stations if s.is_operational]
        if not operational:
            return
        base = len(self.subscribers)
        for i in range(count):
            imsi_hash = hashlib.sha256(f"sub-{base + i}".encode()).hexdigest()
            chosen = rng.choice(operational)
            self.subscribers[imsi_hash] = {
                "home_station": chosen.station_code,
                "current_station": chosen.station_code,
                "app_preference": rng.choices(APP_NAMES, weights=APP_WEIGHTS)[0],
            }


# ═════════════════════════════════════════════════════════════════════════════
# Regional Cluster Manager
# ═════════════════════════════════════════════════════════════════════════════

class ClusterManager:
    """
    Groups stations into regional clusters.
    Cluster events (power outage, fiber cut) affect all stations in a cluster.
    """

    def __init__(self, stations: list[StationRuntime]):
        # Cluster by (province, district)
        self.clusters: dict[str, list[StationRuntime]] = {}
        for s in stations:
            key = f"{s.location['province']}_{s.location['district']}"
            self.clusters.setdefault(key, []).append(s)

        # Also build region-level groupings for weather events
        self.regions: dict[str, list[StationRuntime]] = {}
        for s in stations:
            region = s.location.get("region", "unknown")
            self.regions.setdefault(region, []).append(s)

    def add_station(self, station: StationRuntime):
        key = f"{station.location['province']}_{station.location['district']}"
        self.clusters.setdefault(key, []).append(station)
        region = station.location.get("region", "unknown")
        self.regions.setdefault(region, []).append(station)

    def get_cluster_for(self, station: StationRuntime) -> list[StationRuntime]:
        key = f"{station.location['province']}_{station.location['district']}"
        return self.clusters.get(key, [station])

    def get_cluster_keys(self) -> list[str]:
        return list(self.clusters.keys())

    def get_cluster(self, key: str) -> list[StationRuntime]:
        return self.clusters.get(key, [])


# ═════════════════════════════════════════════════════════════════════════════
# Scenario Engine — v5 extended
# ═════════════════════════════════════════════════════════════════════════════

class ScenarioEngine:
    """
    Manages all scenarios:
     1) Basic incidents (degradation, failure)
     2) Planned maintenance
     3) Cluster events (power outage, fiber cut, cooling failure)
     4) Weather events (regional signal degradation)
     5) Mass gatherings (local traffic surge)
     6) Firmware bugs (periodic reboots after SW update)
     7) Station lifecycle (commissioning, decommissioning)
    """

    def __init__(
        self,
        stations: list[StationRuntime],
        cluster_manager: ClusterManager,
        rng: random.Random,
    ):
        self.stations = stations
        self.cluster_mgr = cluster_manager
        self.rng = rng

        # Active cluster events: {cluster_key: {type, end_time}}
        self.active_cluster_events: dict[str, dict] = {}

        # Active weather events: {region: {severity, end_time}}
        self.active_weather_events: dict[str, dict] = {}

        # Stations queued for commissioning: [{station, phase, transition_time}]
        self.lifecycle_queue: list[dict] = []

    # ── 1) Basic Incidents ─────────────────────────────────────────────────

    def maybe_trigger_incident(
        self, station: StationRuntime, now: datetime, rng: Optional[random.Random] = None,
    ) -> Optional[dict]:
        rng = rng or self.rng
        if station.incident_active or not station.is_operational:
            return None
        if station.status == StationStatus.TESTING:
            # Testing stations have higher incident rate
            prob = INCIDENT_HOURLY_PROB * 3.0
        else:
            prob = INCIDENT_HOURLY_PROB * (2.0 - station.personality.hardware_quality)

        # Convert hourly prob to per-second for streaming
        per_sec = prob / 3600.0
        if rng.random() > per_sec:
            return None

        typ = rng.choices(["degradation", "failure"], weights=INCIDENT_TYPE_WEIGHTS)[0]
        if typ == "degradation":
            severity = rng.uniform(*DEGRADATION_SEVERITY_RANGE)
            duration = rng.randint(*DEGRADATION_DURATION_RANGE)
            station.status = StationStatus.DEGRADED
        else:
            severity = rng.uniform(*FAILURE_SEVERITY_RANGE)
            duration = rng.randint(*FAILURE_DURATION_RANGE)
            station.status = StationStatus.DOWN

        station.incident_active = True
        station.incident_type = typ
        station.incident_start = now
        station.incident_severity = severity
        station.incident_end = now + timedelta(minutes=duration)
        station.error_count += 1

        return {
            "event_type": EventType.INCIDENT_START.value,
            "severity": Severity.CRITICAL.value if severity > CRITICAL_SEVERITY_THRESHOLD else Severity.ERROR.value,
            "description": f"Incident: {typ} (severity={severity:.2f})",
            "metadata": json.dumps({
                "incident_type": typ,
                "severity": round(severity, 3),
                "estimated_duration_min": duration,
            }),
        }

    def maybe_resolve_incident(
        self, station: StationRuntime, now: datetime
    ) -> Optional[dict]:
        if not station.incident_active or station.incident_end is None:
            return None
        if now < station.incident_end:
            return None

        old_type = station.incident_type
        old_severity = station.incident_severity

        station.status = StationStatus.ACTIVE
        station.incident_active = False
        station.incident_type = None
        station.incident_start = None
        station.incident_severity = 0.0
        station.incident_end = None

        return {
            "event_type": EventType.INCIDENT_END.value,
            "severity": Severity.INFO.value,
            "description": f"Incident resolved: {old_type}",
            "metadata": json.dumps({
                "resolved_type": old_type,
                "peak_severity": round(old_severity, 3),
            }),
        }

    # ── 2) Planned Maintenance ─────────────────────────────────────────────

    def schedule_maintenance(
        self,
        station: StationRuntime,
        start_time: datetime,
        duration_min: int,
        upgrade: bool = False,
    ) -> list[dict]:
        events = []

        station.maintenance_active = True
        station.pre_maintenance_status = station.status
        station.status = StationStatus.MAINTENANCE
        station.maintenance_end = start_time + timedelta(minutes=duration_min)

        events.append({
            "event_type": EventType.MAINTENANCE_START.value,
            "severity": Severity.INFO.value,
            "description": f"Planned maintenance ({duration_min}min)"
                           + (" — includes tech upgrade" if upgrade else ""),
            "metadata": json.dumps({
                "type": "planned",
                "duration_min": duration_min,
                "sla_excluded": True,
                "upgrade": upgrade,
            }),
        })
        return events

    def maybe_end_maintenance(
        self, station: StationRuntime, now: datetime
    ) -> Optional[dict]:
        if not station.maintenance_active or station.maintenance_end is None:
            return None
        if now < station.maintenance_end:
            return None

        station.maintenance_active = False
        station.status = station.pre_maintenance_status or StationStatus.ACTIVE
        station.pre_maintenance_status = None
        station.maintenance_end = None

        return {
            "event_type": EventType.MAINTENANCE_END.value,
            "severity": Severity.INFO.value,
            "description": "Maintenance completed — station back online",
            "metadata": json.dumps({"type": "planned", "result": "success"}),
        }

    # ── 3) Cluster Events ─────────────────────────────────────────────────

    def maybe_trigger_cluster_event(self, now: datetime) -> list[dict]:
        """Check if a new cluster-wide event should fire. Returns event dicts."""
        per_sec = CLUSTER_EVENT_HOURLY_PROB / 3600.0
        if self.rng.random() > per_sec:
            return []

        # Pick a random cluster not already affected
        available = [
            k for k in self.cluster_mgr.get_cluster_keys()
            if k not in self.active_cluster_events
        ]
        if not available:
            return []

        cluster_key = self.rng.choice(available)
        stations_in_cluster = self.cluster_mgr.get_cluster(cluster_key)

        event_type = self.rng.choices(CLUSTER_EVENT_TYPES, weights=CLUSTER_EVENT_TYPE_WEIGHTS)[0]
        if event_type == "power_outage":
            duration = self.rng.randint(*CLUSTER_POWER_DURATION)
        elif event_type == "fiber_cut":
            duration = self.rng.randint(*CLUSTER_FIBER_DURATION)
        else:
            duration = self.rng.randint(*CLUSTER_COOLING_DURATION)

        end_time = now + timedelta(minutes=duration)
        self.active_cluster_events[cluster_key] = {
            "type": event_type,
            "end_time": end_time,
            "started_at": now,
        }

        events = []
        for s in stations_in_cluster:
            if s.status in (StationStatus.RETIRED, StationStatus.DECOMMISSIONING):
                continue
            s.cluster_event_active = True
            s.cluster_event_type = event_type
            s.cluster_event_end = end_time

            if event_type in ("power_outage", "fiber_cut"):
                s.pre_maintenance_status = s.status
                s.status = StationStatus.DOWN
            else:  # cooling_failure → degraded performance
                s.pre_maintenance_status = s.status
                s.status = StationStatus.DEGRADED

            events.append({
                "station": s,
                "event_type": EventType.CLUSTER_OUTAGE_START.value,
                "severity": Severity.CRITICAL.value,
                "description": f"Cluster {event_type}: {cluster_key} ({len(stations_in_cluster)} stations)",
                "metadata": json.dumps({
                    "cluster": cluster_key,
                    "outage_type": event_type,
                    "affected_stations": [st.station_code for st in stations_in_cluster],
                    "estimated_duration_min": duration,
                }),
            })

        return events

    def maybe_resolve_cluster_events(self, now: datetime) -> list[dict]:
        """Check if any active cluster events have ended."""
        resolved = []
        ended_keys = []

        for key, info in self.active_cluster_events.items():
            if now < info["end_time"]:
                continue
            ended_keys.append(key)
            for s in self.cluster_mgr.get_cluster(key):
                if not s.cluster_event_active:
                    continue
                s.cluster_event_active = False
                s.cluster_event_type = None
                s.cluster_event_end = None
                s.status = s.pre_maintenance_status or StationStatus.ACTIVE
                s.pre_maintenance_status = None

                resolved.append({
                    "station": s,
                    "event_type": EventType.CLUSTER_OUTAGE_END.value,
                    "severity": Severity.INFO.value,
                    "description": f"Cluster {info['type']} resolved: {key}",
                    "metadata": json.dumps({
                        "cluster": key,
                        "outage_type": info["type"],
                        "duration_min": int((now - info["started_at"]).total_seconds() / 60),
                    }),
                })

        for k in ended_keys:
            del self.active_cluster_events[k]

        return resolved

    # ── 4) Weather Events ─────────────────────────────────────────────────

    def maybe_trigger_weather(self, now: datetime) -> list[dict]:
        per_sec = WEATHER_EVENT_HOURLY_PROB / 3600.0
        if self.rng.random() > per_sec:
            return []

        available_regions = [
            r for r in WEATHER_REGIONS if r not in self.active_weather_events
        ]
        if not available_regions:
            return []

        region = self.rng.choice(available_regions)
        severity = self.rng.uniform(*WEATHER_SEVERITY_RANGE)
        duration = self.rng.randint(*WEATHER_DURATION_RANGE)
        end_time = now + timedelta(minutes=duration)

        self.active_weather_events[region] = {
            "severity": severity,
            "end_time": end_time,
            "started_at": now,
        }

        events = []
        for s in self.cluster_mgr.regions.get(region, []):
            if s.status in (StationStatus.RETIRED, StationStatus.DECOMMISSIONING):
                continue
            s.weather_severity = severity
            s.weather_end = end_time

            events.append({
                "station": s,
                "event_type": EventType.WEATHER_DEGRADATION.value,
                "severity": Severity.WARNING.value,
                "description": f"Weather degradation in {region} (severity={severity:.2f})",
                "metadata": json.dumps({
                    "region": region,
                    "weather_severity": round(severity, 3),
                    "estimated_duration_min": duration,
                }),
            })

        return events

    def maybe_resolve_weather(self, now: datetime) -> list[dict]:
        resolved = []
        ended = []

        for region, info in self.active_weather_events.items():
            if now < info["end_time"]:
                continue
            ended.append(region)
            for s in self.cluster_mgr.regions.get(region, []):
                if s.weather_severity <= 0:
                    continue
                s.weather_severity = 0.0
                s.weather_end = None
                resolved.append({
                    "station": s,
                    "event_type": EventType.WEATHER_CLEAR.value,
                    "severity": Severity.INFO.value,
                    "description": f"Weather cleared in {region}",
                    "metadata": json.dumps({"region": region}),
                })

        for r in ended:
            del self.active_weather_events[r]

        return resolved

    # ── 5) Mass Gatherings ────────────────────────────────────────────────

    def maybe_trigger_mass_event(
        self, station: StationRuntime, now: datetime, rng: Optional[random.Random] = None,
    ) -> Optional[dict]:
        rng = rng or self.rng
        if station.mass_event_active:
            return None
        if station.location.get("density") not in MASS_EVENT_DENSITIES:
            return None

        per_sec = MASS_EVENT_DAILY_PROB / 86400.0
        if rng.random() > per_sec:
            return None

        mult = rng.uniform(*MASS_EVENT_SUB_MULT)
        duration = rng.randint(*MASS_EVENT_DURATION)

        station.mass_event_active = True
        station.mass_event_sub_mult = mult
        station.mass_event_end = now + timedelta(minutes=duration)

        event_names = ["Concert", "Football match", "Festival", "Public rally", "Night market"]
        event_name = rng.choice(event_names)

        return {
            "event_type": EventType.MASS_EVENT_START.value,
            "severity": Severity.WARNING.value,
            "description": f"{event_name} near {station.station_code} — "
                           f"subscriber surge ×{mult:.1f}",
            "metadata": json.dumps({
                "event_name": event_name,
                "subscriber_multiplier": round(mult, 2),
                "estimated_duration_min": duration,
            }),
        }

    def maybe_end_mass_event(
        self, station: StationRuntime, now: datetime
    ) -> Optional[dict]:
        if not station.mass_event_active or station.mass_event_end is None:
            return None
        if now < station.mass_event_end:
            return None

        station.mass_event_active = False
        station.mass_event_sub_mult = 1.0
        station.mass_event_end = None

        return {
            "event_type": EventType.MASS_EVENT_END.value,
            "severity": Severity.INFO.value,
            "description": f"Mass event ended near {station.station_code}",
            "metadata": json.dumps({"station": station.station_code}),
        }

    # ── 6) Firmware Bugs ─────────────────────────────────────────────────

    def trigger_firmware_rollout(
        self, stations: list[StationRuntime], now: datetime
    ) -> list[dict]:
        """
        Simulate a firmware rollout that introduces a bug on a subset of stations.
        Affected stations will periodically reboot until a hotfix is deployed.
        """
        n_affected = max(1, int(
            len(stations) * self.rng.uniform(*FIRMWARE_AFFECTED_PCT)
        ))
        affected = self.rng.sample(
            [s for s in stations if s.is_operational],
            min(n_affected, len([s for s in stations if s.is_operational])),
        )
        lifetime_hours = self.rng.randint(*FIRMWARE_BUG_LIFETIME)
        bug_end = now + timedelta(hours=lifetime_hours)

        firmware_version = f"2.{self.rng.randint(1,9)}.{self.rng.randint(0,99)}"

        events = []
        for s in affected:
            reboot_interval = self.rng.randint(*FIRMWARE_REBOOT_INTERVAL)
            s.firmware_bug_active = True
            s.firmware_next_reboot = now + timedelta(minutes=reboot_interval)
            s.firmware_bug_end = bug_end

            events.append({
                "station": s,
                "event_type": EventType.FIRMWARE_BUG_START.value,
                "severity": Severity.WARNING.value,
                "description": f"Firmware v{firmware_version} "
                               f"deployed — known bug affects {n_affected} stations",
                "metadata": json.dumps({
                    "firmware_version": firmware_version,
                    "affected_count": n_affected,
                    "reboot_interval_min": reboot_interval,
                    "hotfix_eta_hours": lifetime_hours,
                }),
            })

        return events

    def maybe_firmware_reboot(
        self, station: StationRuntime, now: datetime, rng: Optional[random.Random] = None,
    ) -> Optional[dict]:
        rng = rng or self.rng
        if not station.firmware_bug_active:
            return None

        # Check if hotfix deployed
        if station.firmware_bug_end and now >= station.firmware_bug_end:
            station.firmware_bug_active = False
            station.firmware_next_reboot = None
            station.firmware_reboot_end = None
            station.firmware_bug_end = None
            return {
                "event_type": EventType.FIRMWARE_HOTFIX.value,
                "severity": Severity.INFO.value,
                "description": f"Firmware hotfix applied — {station.station_code} stable",
                "metadata": json.dumps({"station": station.station_code}),
            }

        # Check if reboot is due
        if station.firmware_next_reboot and now >= station.firmware_next_reboot:
            if station.firmware_reboot_end and now < station.firmware_reboot_end:
                return None   # Still rebooting

            if station.firmware_reboot_end and now >= station.firmware_reboot_end:
                # Come back online
                station.status = StationStatus.ACTIVE
                station.firmware_reboot_end = None
                # Schedule next reboot
                next_interval = rng.randint(*FIRMWARE_REBOOT_INTERVAL)
                station.firmware_next_reboot = now + timedelta(minutes=next_interval)
                return None

            # Start reboot
            reboot_dur = rng.randint(*FIRMWARE_REBOOT_DURATION)
            station.firmware_reboot_end = now + timedelta(minutes=reboot_dur)
            station.status = StationStatus.DOWN
            station.error_count += 1

            return {
                "event_type": EventType.FIRMWARE_REBOOT.value,
                "severity": Severity.ERROR.value,
                "description": f"Firmware bug reboot ({reboot_dur}min) — {station.station_code}",
                "metadata": json.dumps({
                    "reboot_duration_min": reboot_dur,
                    "station": station.station_code,
                }),
            }

        return None

    # ── 7) Cascading Load ─────────────────────────────────────────────────

    def trigger_cascading_load(
        self,
        failed_station: StationRuntime,
        subscriber_pool: SubscriberPool,
        rng: Optional[random.Random] = None,
    ):
        """When a station goes down, move its subscribers to neighbours."""
        rng = rng or self.rng
        neighbours = [
            s for s in self.stations
            if s.is_operational and s.station_code != failed_station.station_code
            and s.location.get("region") == failed_station.location.get("region")
        ]
        subscriber_pool.migrate_subscribers(failed_station, neighbours, rng)

    # ── 8) Station Lifecycle ──────────────────────────────────────────────

    def maybe_commission_new_station(
        self,
        now: datetime,
        db: "DatabaseManager",
        all_stations: list[StationRuntime],
        subscriber_pool: SubscriberPool,
        next_station_id: int,
        rng: random.Random,
    ) -> Optional[tuple[StationRuntime, list[dict]]]:
        """
        Possibly commission a new station. Returns (station, events) or None.
        The station starts in PROVISIONING, moves to TESTING, then ACTIVE.
        """
        per_sec = NEW_STATION_DAILY_PROB / 86400.0
        if rng.random() > per_sec:
            return None

        location = rng.choice(LOCATIONS)
        operator = rng.choices(OPERATORS, weights=OPERATOR_WEIGHTS)[0]
        technology = rng.choices(TECH_NAMES, weights=TECH_WEIGHTS)[0]

        station_code = f"{operator['code']}-{location['province'][:3].upper()}-{next_station_id:04d}"
        personality = StationPersonality.from_seed(station_code)

        lat = location["lat"] + rng.uniform(-0.02, 0.02)
        lon = location["lon"] + rng.uniform(-0.02, 0.02)

        provision_hours = rng.randint(*PROVISIONING_HOURS)
        testing_hours = rng.randint(*TESTING_HOURS)

        station = StationRuntime(
            station_id=next_station_id,
            station_code=station_code,
            operator=operator,
            location=location,
            technology=technology,
            latitude=lat,
            longitude=lon,
            install_date=now,
            personality=personality,
            status=StationStatus.PROVISIONING,
            commissioned_at=now,
        )

        # Queue lifecycle transitions
        testing_at = now + timedelta(hours=provision_hours)
        go_live_at = testing_at + timedelta(hours=testing_hours)

        self.lifecycle_queue.append({
            "station": station,
            "phase": "testing",
            "transition_time": testing_at,
        })
        self.lifecycle_queue.append({
            "station": station,
            "phase": "go_live",
            "transition_time": go_live_at,
        })

        events = [{
            "event_type": EventType.STATION_COMMISSIONED.value,
            "severity": Severity.INFO.value,
            "description": f"New station {station_code} commissioned (provisioning {provision_hours}h)",
            "metadata": json.dumps({
                "station_code": station_code,
                "operator": operator["code"],
                "technology": technology,
                "location": f"{location['province']}/{location['district']}",
                "provisioning_hours": provision_hours,
                "testing_hours": testing_hours,
            }),
        }]

        return station, events

    def maybe_decommission_station(
        self,
        station: StationRuntime,
        now: datetime,
        subscriber_pool: SubscriberPool,
        all_stations: list[StationRuntime],
        rng: Optional[random.Random] = None,
    ) -> Optional[dict]:
        """Possibly start decommissioning an old station."""
        rng = rng or self.rng
        if station.status in (
            StationStatus.RETIRED,
            StationStatus.DECOMMISSIONING,
            StationStatus.PROVISIONING,
            StationStatus.TESTING,
        ):
            return None

        per_sec = DECOMMISSION_DAILY_PROB / 86400.0
        if rng.random() > per_sec:
            return None

        station.status = StationStatus.DECOMMISSIONING
        station.decommission_at = now

        # Migrate subscribers away
        subscriber_pool.migrate_subscribers(station, all_stations, rng)

        # Schedule retirement (24h later)
        self.lifecycle_queue.append({
            "station": station,
            "phase": "retire",
            "transition_time": now + timedelta(hours=24),
        })

        return {
            "event_type": EventType.STATION_DECOMMISSION.value,
            "severity": Severity.WARNING.value,
            "description": f"Station {station.station_code} decommissioning started — "
                           f"subscribers migrated",
            "metadata": json.dumps({
                "station_code": station.station_code,
                "age_days": (now - station.install_date).days if station.install_date else None,
            }),
        }

    def process_lifecycle_queue(self, now: datetime) -> list[dict]:
        """Check lifecycle transitions that are due."""
        events = []
        remaining = []

        for item in self.lifecycle_queue:
            if now < item["transition_time"]:
                remaining.append(item)
                continue

            station = item["station"]
            phase = item["phase"]

            if phase == "testing":
                station.status = StationStatus.TESTING
                events.append({
                    "station": station,
                    "event_type": EventType.STATION_TESTING.value,
                    "severity": Severity.INFO.value,
                    "description": f"Station {station.station_code} entering testing phase",
                    "metadata": json.dumps({"station_code": station.station_code}),
                })

            elif phase == "go_live":
                station.status = StationStatus.ACTIVE
                station.go_live_at = now
                events.append({
                    "station": station,
                    "event_type": EventType.STATION_GO_LIVE.value,
                    "severity": Severity.INFO.value,
                    "description": f"Station {station.station_code} is now ACTIVE",
                    "metadata": json.dumps({"station_code": station.station_code}),
                })

            elif phase == "retire":
                station.status = StationStatus.RETIRED
                station.retired_at = now
                events.append({
                    "station": station,
                    "event_type": EventType.STATION_RETIRED.value,
                    "severity": Severity.INFO.value,
                    "description": f"Station {station.station_code} retired",
                    "metadata": json.dumps({"station_code": station.station_code}),
                })

        self.lifecycle_queue = remaining
        return events


# ═════════════════════════════════════════════════════════════════════════════
# Data Generators
# ═════════════════════════════════════════════════════════════════════════════

def generate_traffic_event(
    station: StationRuntime,
    imsi_hash: str,
    event_time: datetime,
    rng: random.Random,
    subscriber_pool: Optional[SubscriberPool] = None,
) -> dict:
    """Generate a single subscriber traffic record."""
    tech = TECHNOLOGIES[station.technology]
    density = DENSITY_PROFILES[station.location.get("density", "urban")]

    # Pick application profile for this subscriber
    app = "browsing"
    if subscriber_pool and imsi_hash in subscriber_pool.subscribers:
        app = subscriber_pool.subscribers[imsi_hash].get("app_preference", "browsing")
    app_prof = APP_PROFILES[app]

    base_bytes = int(density["traffic"] * tech["throughput_mult"] * 1_000_000)
    bytes_up = max(1, int(
        rng.gauss(base_bytes * app_prof["bytes_up_mult"], base_bytes * 0.3)
    ))
    bytes_down = max(1, int(
        rng.gauss(base_bytes * app_prof["bytes_down_mult"], base_bytes * 0.5)
    ))

    latency = max(1.0, rng.gauss(tech["latency_base"], tech["latency_std"]))
    jitter = max(0.0, rng.gauss(latency * 0.1, latency * 0.05))
    loss = max(0.0, rng.gauss(0.5, 0.3))

    if rng.random() < HIGH_PACKET_LOSS_PROB:
        loss = rng.uniform(2.0, 15.0)

    duration_ms = rng.randint(*app_prof["duration_ms"])

    # ── Apply incident / weather / mass-event effects ──────────────────
    if station.status == StationStatus.DEGRADED:
        sev = station.incident_severity if station.incident_active else 0.3
        bytes_up = int(bytes_up * (1.0 - sev * DEGRADATION_THROUGHPUT_FACTOR))
        bytes_down = int(bytes_down * (1.0 - sev * DEGRADATION_THROUGHPUT_FACTOR))
        latency *= 1.0 + sev * DEGRADATION_LATENCY_FACTOR
        loss *= 1.0 + sev * DEGRADATION_LOSS_FACTOR

    elif station.status == StationStatus.DOWN:
        bytes_up = int(bytes_up * FAILURE_THROUGHPUT_FACTOR)
        bytes_down = int(bytes_down * FAILURE_THROUGHPUT_FACTOR)
        latency *= FAILURE_LATENCY_MULT
        loss *= FAILURE_LOSS_MULT

    # Weather degradation stacks on top
    if station.weather_severity > 0:
        ws = station.weather_severity
        latency *= 1.0 + ws * 2.0
        loss = min(loss + ws * 5.0, 100.0)
        bytes_down = int(bytes_down * (1.0 - ws * 0.3))

    # Mass event causes congestion effects
    if station.mass_event_active:
        latency *= 1.0 + (station.mass_event_sub_mult - 1.0) * 0.3
        loss += (station.mass_event_sub_mult - 1.0) * 0.5

    loss = min(max(loss, 0.0), 100.0)

    packets_up = max(1, bytes_up // rng.randint(500, 1500))
    packets_down = max(1, bytes_down // rng.randint(500, 1500))

    protocol = rng.choices(PROTOCOLS, weights=PROTOCOL_WEIGHTS)[0]

    record = {
        "station_id": station.station_id,
        "event_time": event_time,
        "imsi_hash": imsi_hash,
        "tmsi": rng.randint(100_000, 999_999),
        "ip_address": f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
        "destination_ip": f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
        "destination_port": rng.choice(COMMON_PORTS),
        "protocol": protocol,
        "bytes_up": max(0, bytes_up),
        "bytes_down": max(0, bytes_down),
        "packets_up": packets_up,
        "packets_down": packets_down,
        "latency_ms": round(latency, 2),
        "jitter_ms": round(jitter, 2),
        "packet_loss_pct": round(loss, 4),
        "connection_duration_ms": duration_ms,
    }

    # ── Data-quality anomalies ─────────────────────────────────────────
    if rng.random() < NULL_FIELD_PROB:
        nullable = ["destination_ip", "destination_port", "jitter_ms", "connection_duration_ms"]
        record[rng.choice(nullable)] = None

    return record


def generate_performance_metrics(
    station: StationRuntime,
    timestamp: datetime,
    active_subscribers: int,
    rng: random.Random,
) -> dict:
    """Generate a performance-metrics snapshot for a station."""
    tech = TECHNOLOGIES[station.technology]
    density = DENSITY_PROFILES[station.location.get("density", "urban")]
    p = station.personality
    noise = density["metric_noise"]

    # CPU
    load_frac = active_subscribers / max(1, tech["max_subs"])
    cpu = p.idle_cpu + load_frac * (100 - p.idle_cpu)
    cpu += rng.gauss(0, noise * 10)

    # Temperature
    temp = p.temp_base + load_frac * p.temp_sensitivity * 20
    temp += rng.gauss(0, 2)

    # Throughput
    max_throughput = tech["throughput_mult"] * 1000
    throughput = max_throughput * (0.7 + rng.gauss(0, noise)) * (1.0 - load_frac * 0.3)

    # Signal strength
    signal_strength = -50 - load_frac * 30 + rng.gauss(0, 5)

    band = rng.choice(FREQUENCY_BANDS.get(station.technology, ["unknown"]))

    # ── Status effects ────────────────────────────────────────────────
    if station.status == StationStatus.DEGRADED:
        sev = station.incident_severity if station.incident_active else 0.3
        cpu += sev * DEGRADATION_CPU_SPIKE
        throughput *= (1.0 - sev * DEGRADATION_THROUGHPUT_REDUCTION)
    elif station.status == StationStatus.DOWN:
        cpu = rng.uniform(*FAILURE_CPU_RANGE)
        throughput *= FAILURE_THROUGHPUT_PCT
    elif station.status == StationStatus.MAINTENANCE:
        cpu = rng.uniform(0, 5)
        throughput = 0
        active_subscribers = 0

    # Weather
    if station.weather_severity > 0:
        signal_strength -= station.weather_severity * 15
        throughput *= (1.0 - station.weather_severity * 0.2)

    return {
        "station_id": station.station_id,
        "metric_time": timestamp,
        "cpu_usage_pct": round(min(100, max(0, cpu)), 2),
        "memory_usage_pct": round(min(100, max(0, 40 + load_frac * 40 + rng.gauss(0, 5))), 2),
        "disk_usage_pct": round(min(100, max(0, 30 + rng.gauss(0, 3))), 2),
        "temperature_celsius": round(min(95, max(15, temp)), 1),
        "power_consumption_watts": round(max(50, 200 + load_frac * 300 + rng.gauss(0, 20)), 1),
        "uplink_throughput_mbps": round(max(0, throughput * 0.3), 2),
        "downlink_throughput_mbps": round(max(0, throughput), 2),
        "active_subscribers": max(0, active_subscribers),
        "signal_strength_dbm": round(min(-30, max(-120, signal_strength)), 1),
        "frequency_band": band,
        "channel_utilization_pct": round(min(100, max(0, load_frac * 100 + rng.gauss(0, noise * 20))), 2),
    }


def generate_station_event(
    station: StationRuntime,
    all_stations: list[StationRuntime],
    event_time: datetime,
    rng: random.Random,
    override: Optional[dict] = None,
) -> Optional[dict]:
    """
    Generate a station event. If *override* is provided, use its fields
    directly (for incident / maintenance / lifecycle events).
    Otherwise, generate an organic event.
    """
    if override:
        target = None
        if override.get("event_type") == EventType.HANDOVER.value:
            neighbours = [
                s for s in all_stations
                if s.is_operational and s.station_code != station.station_code
                and s.location.get("region") == station.location.get("region")
            ]
            target = rng.choice(neighbours).station_id if neighbours else None

        return {
            "station_id": station.station_id,
            "event_time": event_time,
            "event_type": override["event_type"],
            "severity": override.get("severity", Severity.INFO.value),
            "description": override.get("description", ""),
            "metadata": override.get("metadata", json.dumps({})),
            "target_station_id": target,
        }

    # ── Organic events ─────────────────────────────────────────────────
    if station.status in (
        StationStatus.DOWN, StationStatus.MAINTENANCE,
        StationStatus.PROVISIONING, StationStatus.RETIRED,
        StationStatus.DECOMMISSIONING,
    ):
        return None

    # Rate: density × personality × time ~1 event per 2–5 organic calls
    density = DENSITY_PROFILES[station.location.get("density", "urban")]
    if rng.random() > density["events_per_min"] * station.personality.alarm_tendency * 0.5:
        return None

    evt_type_options = [
        EventType.HANDOVER, EventType.ATTACH, EventType.DETACH,
        EventType.ALARM, EventType.PAGING,
    ]
    evt = rng.choices(evt_type_options, weights=ORGANIC_EVENT_WEIGHTS)[0]

    target_station = None
    metadata = {}

    if evt == EventType.HANDOVER:
        neighbours = [
            s for s in all_stations
            if s.is_operational and s.station_code != station.station_code
            and s.location.get("region") == station.location.get("region")
        ]
        if neighbours:
            target = rng.choice(neighbours)
            target_station = target.station_id
            metadata = {
                "source_tech": station.technology,
                "target_tech": target.technology,
                "reason": rng.choice(["signal_quality", "load_balance", "mobility"]),
            }
        else:
            return None

    elif evt == EventType.ATTACH:
        metadata = {"rat_type": station.technology, "attach_type": rng.choice(["initial", "re-attach"])}

    elif evt == EventType.DETACH:
        metadata = {"rat_type": station.technology, "reason": rng.choice(["normal", "timeout", "auth_failure"])}

    elif evt == EventType.ALARM:
        alarm_sev = rng.choices(
            [Severity.WARNING, Severity.ERROR, Severity.CRITICAL],
            weights=ALARM_SEVERITY_WEIGHTS,
        )[0]
        alarm_types = [
            "high_cpu", "high_temperature", "link_flap", "disk_space_low",
            "power_fluctuation", "antenna_misalignment", "backhaul_congestion",
            "hardware_watchdog", "clock_drift", "license_expiring",
        ]
        return {
            "station_id": station.station_id,
            "event_time": event_time,
            "event_type": EventType.ALARM.value,
            "severity": alarm_sev.value,
            "description": rng.choice(alarm_types),
            "metadata": json.dumps({"alarm_type": rng.choice(alarm_types)}),
            "target_station_id": None,
        }

    elif evt == EventType.PAGING:
        metadata = {"page_type": rng.choice(["mt_call", "sms", "data_notification"])}

    severity = Severity.INFO.value
    if station.status == StationStatus.DEGRADED:
        severity = Severity.WARNING.value

    return {
        "station_id": station.station_id,
        "event_time": event_time,
        "event_type": evt.value,
        "severity": severity,
        "description": f"{evt.value} at {station.station_code}",
        "metadata": json.dumps(metadata),
        "target_station_id": target_station,
    }


# ═════════════════════════════════════════════════════════════════════════════
# Database Manager
# ═════════════════════════════════════════════════════════════════════════════

@contextmanager
def _pg_conn(db: "DatabaseManager"):
    conn = db.pool.getconn()
    try:
        yield conn
    finally:
        db.pool.putconn(conn)


class DatabaseManager:
    def __init__(self, host, port, dbname, user, password, minconn=2, maxconn=10):
        self.dsn = dict(host=host, port=port, dbname=dbname, user=user, password=password)
        self.minconn = minconn
        self.maxconn = maxconn
        self.pool: Optional[pool.ThreadedConnectionPool] = None

    def initialize(self):
        self.pool = pool.ThreadedConnectionPool(self.minconn, self.maxconn, **self.dsn)

    def close(self):
        if self.pool:
            self.pool.closeall()


# ═════════════════════════════════════════════════════════════════════════════
# Schema — CLEAN: No extracted_at / batch_id
#
# The ETL pipeline uses watermarks on created_at to extract data.
# Dimension tables have updated_at for SCD change detection.
# ═════════════════════════════════════════════════════════════════════════════

INIT_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS telecom;

-- ── Dimension: Operator ────────────────────────────────────────────────────
-- (created first for FK references)
CREATE TABLE IF NOT EXISTS telecom.operator (
    operator_id SERIAL PRIMARY KEY,
    operator_code VARCHAR(10) UNIQUE NOT NULL,
    operator_name VARCHAR(100) NOT NULL,
    is_deleted BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_op_updated_at
    ON telecom.operator(updated_at);


-- ── Dimension: Location ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telecom.location (
    location_id SERIAL PRIMARY KEY,
    province VARCHAR(50) NOT NULL,
    district VARCHAR(50) NOT NULL,
    region VARCHAR(20) NOT NULL,
    density_class VARCHAR(20),
    is_deleted BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(province, district)
);

CREATE INDEX IF NOT EXISTS idx_loc_updated_at
    ON telecom.location(updated_at);


-- ── Dimension: Base Station ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telecom.base_station (
    station_id SERIAL PRIMARY KEY,
    station_code VARCHAR(30) UNIQUE NOT NULL,
    station_name VARCHAR(100),
    operator_id INTEGER NOT NULL REFERENCES telecom.operator(operator_id),
    location_id INTEGER NOT NULL REFERENCES telecom.location(location_id),
    technology VARCHAR(10) NOT NULL,
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    install_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    is_deleted BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bs_updated_at
    ON telecom.base_station(updated_at);


-- ── Fact: Subscriber Traffic ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telecom.subscriber_traffic (
    traffic_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES telecom.base_station(station_id),
    event_time TIMESTAMPTZ NOT NULL,
    imsi_hash VARCHAR(64) NOT NULL,
    tmsi INTEGER,
    ip_address VARCHAR(45),
    destination_ip VARCHAR(45),
    destination_port INTEGER,
    protocol VARCHAR(10),
    bytes_up BIGINT NOT NULL DEFAULT 0,
    bytes_down BIGINT NOT NULL DEFAULT 0,
    packets_up INTEGER NOT NULL DEFAULT 0,
    packets_down INTEGER NOT NULL DEFAULT 0,
    latency_ms NUMERIC(10,2) NOT NULL,
    jitter_ms NUMERIC(10,2),
    packet_loss_pct NUMERIC(6,4) NOT NULL DEFAULT 0,
    connection_duration_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for watermark-based extraction (ETL reads WHERE created_at > watermark)
CREATE INDEX IF NOT EXISTS idx_st_created_at
    ON telecom.subscriber_traffic(created_at);

-- Index for analytical queries by event time
CREATE INDEX IF NOT EXISTS idx_st_event_time
    ON telecom.subscriber_traffic(event_time);

-- Composite index for station + time range queries
CREATE INDEX IF NOT EXISTS idx_st_station_event_time
    ON telecom.subscriber_traffic(station_id, event_time);


-- ── Fact: Performance Metrics ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telecom.performance_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES telecom.base_station(station_id),
    metric_time TIMESTAMPTZ NOT NULL,
    cpu_usage_pct NUMERIC(5,2),
    memory_usage_pct NUMERIC(5,2),
    disk_usage_pct NUMERIC(5,2),
    temperature_celsius NUMERIC(5,1),
    power_consumption_watts NUMERIC(8,1),
    uplink_throughput_mbps NUMERIC(10,2),
    downlink_throughput_mbps NUMERIC(10,2),
    active_subscribers INTEGER,
    signal_strength_dbm NUMERIC(6,1),
    frequency_band VARCHAR(20),
    channel_utilization_pct NUMERIC(5,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pm_created_at
    ON telecom.performance_metrics(created_at);

CREATE INDEX IF NOT EXISTS idx_pm_metric_time
    ON telecom.performance_metrics(metric_time);

CREATE INDEX IF NOT EXISTS idx_pm_station_metric_time
    ON telecom.performance_metrics(station_id, metric_time);


-- ── Fact: Station Events ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telecom.station_events (
    event_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES telecom.base_station(station_id),
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    description TEXT,
    metadata JSONB,
    target_station_id INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_se_created_at
    ON telecom.station_events(created_at);

CREATE INDEX IF NOT EXISTS idx_se_event_time
    ON telecom.station_events(event_time);

CREATE INDEX IF NOT EXISTS idx_se_event_type
    ON telecom.station_events(event_type);

CREATE INDEX IF NOT EXISTS idx_se_station_event_time
    ON telecom.station_events(station_id, event_time);


-- ── Dimension: Configuration (SCD Type-2) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS telecom.configuration (
    config_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES telecom.base_station(station_id),
    config_key VARCHAR(100) NOT NULL,
    config_value TEXT,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to TIMESTAMPTZ,
    is_current BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cfg_station_current
    ON telecom.configuration(station_id, is_current);

CREATE INDEX IF NOT EXISTS idx_cfg_updated_at
    ON telecom.configuration(updated_at);
"""


def initialize_schema(db: DatabaseManager):
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            cur.execute(INIT_SCHEMA_SQL)
        conn.commit()


def seed_dimensions(db: DatabaseManager):
    """Insert operators and locations (idempotent)."""
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            for op in OPERATORS:
                cur.execute("""
                    INSERT INTO telecom.operator (operator_id, operator_code, operator_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (operator_code) DO NOTHING
                """, (op["id"], op["code"], op["name"]))

            for loc in LOCATIONS:
                cur.execute("""
                    INSERT INTO telecom.location (location_id, province, district, region, density_class)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (province, district) DO NOTHING
                """, (loc["id"], loc["province"], loc["district"], loc["region"], loc["density"]))
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Station Creation & Insertion Helpers
# ═════════════════════════════════════════════════════════════════════════════

def create_stations(
    db: DatabaseManager,
    count: int,
    rng: random.Random,
    base_date: Optional[datetime] = None,
) -> list[StationRuntime]:
    """
    Create *count* stations and persist them to the DB.
    Returns list of StationRuntime.
    """
    if base_date is None:
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    stations: list[StationRuntime] = []

    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            for i in range(count):
                operator = rng.choices(OPERATORS, weights=OPERATOR_WEIGHTS)[0]
                location = rng.choice(LOCATIONS)
                technology = rng.choices(TECH_NAMES, weights=TECH_WEIGHTS)[0]

                station_code = f"{operator['code']}-{location['province'][:3].upper()}-{i+1:04d}"
                install_days_ago = rng.randint(30, 1500)
                install_date = base_date - timedelta(days=install_days_ago)

                lat = location["lat"] + rng.uniform(-0.02, 0.02)
                lon = location["lon"] + rng.uniform(-0.02, 0.02)

                personality = StationPersonality.from_seed(station_code)

                cur.execute("""
                    INSERT INTO telecom.base_station
                        (station_code, station_name, operator_id, location_id,
                         technology, latitude, longitude, install_date, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_code) DO UPDATE SET
                        status = EXCLUDED.status,
                        updated_at = NOW()
                    RETURNING station_id
                """, (
                    station_code,
                    f"Station {station_code}",
                    operator["id"],
                    location["id"],
                    technology,
                    round(lat, 7),
                    round(lon, 7),
                    install_date.date(),
                    StationStatus.ACTIVE.value,
                ))
                station_id = cur.fetchone()[0]

                runtime = StationRuntime(
                    station_id=station_id,
                    station_code=station_code,
                    operator=operator,
                    location=location,
                    technology=technology,
                    latitude=lat,
                    longitude=lon,
                    install_date=install_date,
                    personality=personality,
                    status=StationStatus.ACTIVE,
                )
                stations.append(runtime)

                # Seed initial configuration
                configs = {
                    "max_tx_power_dbm": str(rng.randint(30, 46)),
                    "frequency_band": rng.choice(FREQUENCY_BANDS.get(technology, ["unknown"])),
                    "antenna_tilt_deg": str(rng.randint(2, 15)),
                    "max_connected_ues": str(TECHNOLOGIES[technology]["max_subs"]),
                }
                for key, val in configs.items():
                    cur.execute("""
                        INSERT INTO telecom.configuration
                            (station_id, config_key, config_value, effective_from, is_current)
                        VALUES (%s, %s, %s, %s, true)
                        ON CONFLICT DO NOTHING
                    """, (station_id, key, val, install_date))

        conn.commit()

    # Build neighbour lists (same region)
    for s in stations:
        s.neighbors = [
            n for n in stations
            if n.station_code != s.station_code
            and n.location.get("region") == s.location.get("region")
        ]

    return stations


def insert_station_to_db(db: DatabaseManager, station: StationRuntime):
    """Insert a single newly-commissioned station."""
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO telecom.base_station
                    (station_code, station_name, operator_id, location_id,
                     technology, latitude, longitude, install_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_code) DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING station_id
            """, (
                station.station_code,
                f"Station {station.station_code}",
                station.operator["id"],
                station.location["id"],
                station.technology,
                round(station.latitude, 7),
                round(station.longitude, 7),
                station.install_date.date() if station.install_date else date.today(),
                station.status.value,
            ))
            station.station_id = cur.fetchone()[0]
        conn.commit()


def update_station_status(db: DatabaseManager, station: StationRuntime):
    """Update a station's status in the DB (for lifecycle changes)."""
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE telecom.base_station
                SET status = %s, updated_at = NOW()
                WHERE station_id = %s
            """, (station.status.value, station.station_id))
        conn.commit()


# ── Batch insert helpers ───────────────────────────────────────────────────

def insert_traffic_batch(
    db: DatabaseManager,
    records: list[dict],
    late_arrival_rng: Optional[random.Random] = None,
):
    """Insert traffic records. Some may have late created_at."""
    if not records:
        return
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            for r in records:
                params = [
                    r["station_id"], r["event_time"], r["imsi_hash"],
                    r.get("tmsi"), r.get("ip_address"), r.get("destination_ip"),
                    r.get("destination_port"), r.get("protocol"),
                    r.get("bytes_up"), r.get("bytes_down"),
                    r.get("packets_up"), r.get("packets_down"),
                    r.get("latency_ms"), r.get("jitter_ms"),
                    r.get("packet_loss_pct"), r.get("connection_duration_ms"),
                ]

                if late_arrival_rng and late_arrival_rng.random() < LATE_ARRIVAL_PROB:
                    # Late arrival: record was generated at event_time but didn't
                    # reach the OLTP until event_time + delay seconds later.
                    delay = late_arrival_rng.randint(*LATE_ARRIVAL_DELAY)
                    late_params = params + [r["event_time"], delay]
                    cur.execute("""
                        INSERT INTO telecom.subscriber_traffic
                            (station_id, event_time, imsi_hash, tmsi, ip_address,
                             destination_ip, destination_port, protocol,
                             bytes_up, bytes_down, packets_up, packets_down,
                             latency_ms, jitter_ms, packet_loss_pct,
                             connection_duration_ms, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s + make_interval(secs => %s))
                    """, late_params)
                else:
                    cur.execute("""
                        INSERT INTO telecom.subscriber_traffic
                            (station_id, event_time, imsi_hash, tmsi, ip_address,
                             destination_ip, destination_port, protocol,
                             bytes_up, bytes_down, packets_up, packets_down,
                             latency_ms, jitter_ms, packet_loss_pct,
                             connection_duration_ms)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, params)

                # Maybe insert a duplicate
                if late_arrival_rng and late_arrival_rng.random() < DUPLICATE_RECORD_PROB:
                    cur.execute("""
                        INSERT INTO telecom.subscriber_traffic
                            (station_id, event_time, imsi_hash, tmsi, ip_address,
                             destination_ip, destination_port, protocol,
                             bytes_up, bytes_down, packets_up, packets_down,
                             latency_ms, jitter_ms, packet_loss_pct,
                             connection_duration_ms)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, params[:16])

        conn.commit()


def insert_metrics(db: DatabaseManager, record: dict):
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO telecom.performance_metrics
                    (station_id, metric_time, cpu_usage_pct, memory_usage_pct,
                     disk_usage_pct, temperature_celsius, power_consumption_watts,
                     uplink_throughput_mbps, downlink_throughput_mbps,
                     active_subscribers, signal_strength_dbm,
                     frequency_band, channel_utilization_pct)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                record["station_id"], record["metric_time"],
                record["cpu_usage_pct"], record["memory_usage_pct"],
                record["disk_usage_pct"], record["temperature_celsius"],
                record["power_consumption_watts"],
                record["uplink_throughput_mbps"], record["downlink_throughput_mbps"],
                record["active_subscribers"], record["signal_strength_dbm"],
                record["frequency_band"], record["channel_utilization_pct"],
            ))
        conn.commit()


def insert_event(db: DatabaseManager, record: Optional[dict]):
    if not record:
        return
    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO telecom.station_events
                    (station_id, event_time, event_type, severity,
                     description, metadata, target_station_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                record["station_id"], record["event_time"],
                record["event_type"], record["severity"],
                record.get("description"), record.get("metadata"),
                record.get("target_station_id"),
            ))
        conn.commit()


def insert_config_change(db: DatabaseManager, station: StationRuntime, ts: datetime, rng: random.Random):
    """Record an SCD-2 config change (tech upgrade)."""
    new_tech = station.technology
    new_band = rng.choice(FREQUENCY_BANDS.get(new_tech, ["unknown"]))
    new_max_ues = str(TECHNOLOGIES[new_tech]["max_subs"])

    with _pg_conn(db) as conn:
        with conn.cursor() as cur:
            # Close out all current configs for this station
            for key in ("frequency_band", "max_connected_ues"):
                cur.execute("""
                    UPDATE telecom.configuration
                    SET is_current = false, effective_to = %s, updated_at = NOW()
                    WHERE station_id = %s AND config_key = %s AND is_current = true
                """, (ts, station.station_id, key))

            # Insert new configs
            cur.execute("""
                INSERT INTO telecom.configuration
                    (station_id, config_key, config_value, effective_from, is_current)
                VALUES (%s, 'frequency_band', %s, %s, true)
            """, (station.station_id, new_band, ts))
            cur.execute("""
                INSERT INTO telecom.configuration
                    (station_id, config_key, config_value, effective_from, is_current)
                VALUES (%s, 'max_connected_ues', %s, %s, true)
            """, (station.station_id, new_max_ues, ts))
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Streaming Mode
# ═════════════════════════════════════════════════════════════════════════════

class StationWorker(threading.Thread):
    """Worker thread processing a group of stations."""

    def __init__(
        self,
        stations: list[StationRuntime],
        all_stations: list[StationRuntime],
        db: DatabaseManager,
        subscriber_pool: SubscriberPool,
        scenario_engine: ScenarioEngine,
        stop_event: threading.Event,
        stats: dict,
        rng_seed: int,
        traffic_batch_size: int = 50,
    ):
        super().__init__(daemon=True)
        self.stations = stations
        self.all_stations = all_stations
        self.db = db
        self.subscriber_pool = subscriber_pool
        self.scenario_engine = scenario_engine
        self.stop_event = stop_event
        self.stats = stats
        self.rng = random.Random(rng_seed)
        self.traffic_batch_size = traffic_batch_size

        for s in stations:
            self.stats[s.station_code] = {
                "traffic": 0, "metrics": 0, "events": 0, "errors": 0,
            }

    def run(self):
        last_metric_time = time.time()
        last_mobility_time = time.time()
        last_scenario_time = time.time()
        metric_interval = 60
        mobility_interval = 300
        scenario_interval = 10       # Check cluster/weather every 10s
        traffic_buffer: list[dict] = []

        while not self.stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                local_now = now.astimezone(TIMEZONE)
                time_mult = get_time_multiplier(local_now)

                for station in self.stations:
                    # Skip non-operational stations (but check for resolutions)
                    if not station.is_operational:
                        if station.status in (
                            StationStatus.RETIRED, StationStatus.DECOMMISSIONING,
                            StationStatus.PROVISIONING,
                        ):
                            continue

                        # Check incident / maintenance / cluster / firmware resolution
                        resolved = self.scenario_engine.maybe_resolve_incident(station, now)
                        if resolved:
                            insert_event(self.db, generate_station_event(
                                station, self.all_stations, now, self.rng, resolved
                            ))
                            self.stats[station.station_code]["events"] += 1

                        maint_end = self.scenario_engine.maybe_end_maintenance(station, now)
                        if maint_end:
                            insert_event(self.db, generate_station_event(
                                station, self.all_stations, now, self.rng, maint_end
                            ))
                            self.stats[station.station_code]["events"] += 1

                        fw = self.scenario_engine.maybe_firmware_reboot(station, now, self.rng)
                        if fw:
                            insert_event(self.db, generate_station_event(
                                station, self.all_stations, now, self.rng, fw
                            ))
                            self.stats[station.station_code]["events"] += 1

                        continue

                    # ── Traffic ─────────────────────────────────────────────
                    active_subs = self.subscriber_pool.get_active_subscribers(
                        station, local_now.hour, time_mult, self.rng,
                    )

                    # Mass-event subscriber surge
                    effective_mult = time_mult
                    if station.mass_event_active:
                        effective_mult *= station.mass_event_sub_mult

                    sample_size = max(1, int(len(active_subs) * effective_mult * TRAFFIC_SAMPLE_RATE))
                    sampled = self.rng.sample(active_subs, min(sample_size, len(active_subs))) if active_subs else []

                    for imsi_hash in sampled:
                        traffic = generate_traffic_event(
                            station, imsi_hash, now, self.rng, self.subscriber_pool,
                        )
                        traffic_buffer.append(traffic)
                        self.stats[station.station_code]["traffic"] += 1

                    if len(traffic_buffer) >= self.traffic_batch_size:
                        insert_traffic_batch(self.db, traffic_buffer, self.rng)
                        traffic_buffer.clear()

                    # ── Scenarios ───────────────────────────────────────────
                    incident = self.scenario_engine.maybe_trigger_incident(station, now, self.rng)
                    if incident:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, incident
                        ))
                        self.stats[station.station_code]["events"] += 1
                        if station.status == StationStatus.DOWN:
                            self.scenario_engine.trigger_cascading_load(station, self.subscriber_pool, self.rng)

                    resolved = self.scenario_engine.maybe_resolve_incident(station, now)
                    if resolved:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, resolved
                        ))
                        self.stats[station.station_code]["events"] += 1

                    # Mass event
                    me = self.scenario_engine.maybe_trigger_mass_event(station, now, self.rng)
                    if me:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, me
                        ))
                        self.stats[station.station_code]["events"] += 1

                    me_end = self.scenario_engine.maybe_end_mass_event(station, now)
                    if me_end:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, me_end
                        ))
                        self.stats[station.station_code]["events"] += 1

                    # Firmware
                    fw = self.scenario_engine.maybe_firmware_reboot(station, now, self.rng)
                    if fw:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, fw
                        ))
                        self.stats[station.station_code]["events"] += 1

                    # Decommission check
                    decom = self.scenario_engine.maybe_decommission_station(
                        station, now, self.subscriber_pool, self.all_stations, self.rng,
                    )
                    if decom:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, decom
                        ))
                        update_station_status(self.db, station)
                        self.stats[station.station_code]["events"] += 1

                    # ── Organic events ─────────────────────────────────────
                    organic = generate_station_event(station, self.all_stations, now, self.rng)
                    if organic:
                        insert_event(self.db, organic)
                        self.stats[station.station_code]["events"] += 1

                # ── Periodic: metrics ──────────────────────────────────────
                if time.time() - last_metric_time >= metric_interval:
                    for station in self.stations:
                        if station.status in (StationStatus.RETIRED, StationStatus.PROVISIONING):
                            continue
                        active_count = sum(
                            1 for s in self.subscriber_pool.subscribers.values()
                            if s["current_station"] == station.station_code
                        )
                        metrics = generate_performance_metrics(station, now, active_count, self.rng)
                        insert_metrics(self.db, metrics)
                        self.stats[station.station_code]["metrics"] += 1
                    last_metric_time = time.time()

                # ── Periodic: subscriber mobility ─────────────────────────
                if time.time() - last_mobility_time >= mobility_interval:
                    self.subscriber_pool.simulate_mobility(
                        self.all_stations, local_now.hour, self.rng,
                    )
                    last_mobility_time = time.time()

                # Flush remaining traffic
                if traffic_buffer:
                    insert_traffic_batch(self.db, traffic_buffer, self.rng)
                    traffic_buffer.clear()

                time.sleep(self.rng.uniform(0.8, 1.2))

            except Exception as e:
                for s in self.stations:
                    self.stats[s.station_code]["errors"] += 1
                print(f"Error in worker: {e}")
                time.sleep(2)


class GlobalScenarioThread(threading.Thread):
    """
    Dedicated thread for global-scope scenarios:
    cluster events, weather, firmware rollouts, station lifecycle.
    These must not be per-worker to avoid duplicate triggers.
    """

    def __init__(
        self,
        db: DatabaseManager,
        stations: list[StationRuntime],
        scenario_engine: ScenarioEngine,
        cluster_manager: ClusterManager,
        subscriber_pool: SubscriberPool,
        stop_event: threading.Event,
        stats: dict,
        rng: random.Random,
    ):
        super().__init__(daemon=True)
        self.db = db
        self.stations = stations
        self.scenario_engine = scenario_engine
        self.cluster_mgr = cluster_manager
        self.subscriber_pool = subscriber_pool
        self.stop_event = stop_event
        self.stats = stats
        self.rng = rng
        self.next_station_id = max(s.station_id for s in stations) + 1 if stations else 1
        self.last_firmware_check = time.time()

    def run(self):
        while not self.stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)

                # ── Cluster events ─────────────────────────────────────────
                for ev in self.scenario_engine.maybe_trigger_cluster_event(now):
                    station = ev.pop("station", None)
                    if station:
                        insert_event(self.db, generate_station_event(
                            station, self.stations, now, self.rng, ev
                        ))
                        if station.station_code in self.stats:
                            self.stats[station.station_code]["events"] += 1

                for ev in self.scenario_engine.maybe_resolve_cluster_events(now):
                    station = ev.pop("station", None)
                    if station:
                        insert_event(self.db, generate_station_event(
                            station, self.stations, now, self.rng, ev
                        ))
                        update_station_status(self.db, station)

                # ── Weather events ─────────────────────────────────────────
                for ev in self.scenario_engine.maybe_trigger_weather(now):
                    station = ev.pop("station", None)
                    if station:
                        insert_event(self.db, generate_station_event(
                            station, self.stations, now, self.rng, ev
                        ))

                for ev in self.scenario_engine.maybe_resolve_weather(now):
                    station = ev.pop("station", None)
                    if station:
                        insert_event(self.db, generate_station_event(
                            station, self.stations, now, self.rng, ev
                        ))

                # ── Lifecycle queue (testing → go_live, retire) ────────────
                for ev in self.scenario_engine.process_lifecycle_queue(now):
                    station = ev.pop("station", None)
                    if station:
                        insert_event(self.db, generate_station_event(
                            station, self.stations, now, self.rng, ev
                        ))
                        update_station_status(self.db, station)
                        # Add to stats if new
                        if station.station_code not in self.stats:
                            self.stats[station.station_code] = {
                                "traffic": 0, "metrics": 0, "events": 0, "errors": 0,
                            }

                # ── New station commissioning ──────────────────────────────
                result = self.scenario_engine.maybe_commission_new_station(
                    now, self.db, self.stations, self.subscriber_pool,
                    self.next_station_id, self.rng,
                )
                if result:
                    new_station, events = result
                    insert_station_to_db(self.db, new_station)
                    self.stations.append(new_station)
                    self.cluster_mgr.add_station(new_station)
                    self.next_station_id += 1

                    for ev in events:
                        insert_event(self.db, generate_station_event(
                            new_station, self.stations, now, self.rng, ev
                        ))
                    self.stats[new_station.station_code] = {
                        "traffic": 0, "metrics": 0, "events": 0, "errors": 0,
                    }
                    print(f"  📡 New station commissioned: {new_station.station_code}")

                # ── Firmware rollout (daily check) ─────────────────────────
                if time.time() - self.last_firmware_check > 3600:
                    if self.rng.random() < FIRMWARE_ROLLOUT_PROB * 24:
                        events = self.scenario_engine.trigger_firmware_rollout(self.stations, now)
                        for ev in events:
                            station = ev.pop("station", None)
                            if station:
                                insert_event(self.db, generate_station_event(
                                    station, self.stations, now, self.rng, ev
                                ))
                        if events:
                            print(f"  🔧 Firmware rollout affecting {len(events)} stations")
                    self.last_firmware_check = time.time()

                time.sleep(5)

            except Exception as e:
                print(f"Error in global scenario thread: {e}")
                time.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
# Backfill Mode
# ═════════════════════════════════════════════════════════════════════════════

def run_backfill(
    db: DatabaseManager,
    stations: list[StationRuntime],
    subscriber_pool: SubscriberPool,
    scenario_engine: ScenarioEngine,
    cluster_manager: ClusterManager,
    start_date: date,
    num_days: int,
    rng: random.Random,
    events_per_station_per_hour: int = 20,
    maintenance_probability: float = MAINTENANCE_DAILY_PROB,
):
    """Generate hourly historical data with full scenario simulation."""
    print(f"\n{'='*60}")
    print(f"Backfill: {start_date} → {start_date + timedelta(days=num_days)}")
    print(f"Stations: {len(stations)}, Events/station/hour: ~{events_per_station_per_hour}")
    est_traffic = len(stations) * events_per_station_per_hour * 24 * num_days
    print(f"Estimated traffic records: ~{est_traffic:,}")
    print(f"{'='*60}\n")

    total_traffic = 0
    total_metrics = 0
    total_events = 0
    next_station_id = max(s.station_id for s in stations) + 1 if stations else 1

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        day_start = datetime.combine(current_date, datetime.min.time(), tzinfo=TIMEZONE)
        print(f"{current_date}: ", end="", flush=True)

        day_traffic = 0
        day_events = 0

        # ── Daily: maintenance scheduling ──────────────────────────────────
        for station in stations:
            if station.status in (StationStatus.RETIRED, StationStatus.DECOMMISSIONING):
                continue
            if rng.random() < maintenance_probability and not station.maintenance_active:
                maint_hour = rng.randint(*MAINTENANCE_HOUR_RANGE)
                maint_start = day_start + timedelta(hours=maint_hour)
                maint_duration = rng.randint(*MAINTENANCE_DURATION_RANGE)
                upgrade = rng.random() < TECH_UPGRADE_PROB

                maint_events = scenario_engine.schedule_maintenance(
                    station, maint_start, maint_duration, upgrade,
                )
                for me in maint_events:
                    insert_event(db, generate_station_event(
                        station, stations, maint_start, rng, me,
                    ))
                    day_events += 1

                if upgrade:
                    insert_config_change(db, station, maint_start, rng)

        # ── Daily: maybe firmware rollout ──────────────────────────────────
        if rng.random() < FIRMWARE_ROLLOUT_PROB:
            fw_events = scenario_engine.trigger_firmware_rollout(stations, day_start)
            for ev in fw_events:
                station = ev.pop("station", None)
                if station:
                    insert_event(db, generate_station_event(
                        station, stations, day_start, rng, ev,
                    ))
                    day_events += 1
            if fw_events:
                print(f"[firmware:{len(fw_events)}]", end=" ", flush=True)

        # ── Daily: maybe commission new station ────────────────────────────
        if rng.random() < NEW_STATION_DAILY_PROB:
            commission_time = day_start + timedelta(hours=rng.randint(6, 18))
            location = rng.choice(LOCATIONS)
            operator = rng.choices(OPERATORS, weights=OPERATOR_WEIGHTS)[0]
            technology = rng.choices(TECH_NAMES, weights=TECH_WEIGHTS)[0]

            station_code = f"{operator['code']}-{location['province'][:3].upper()}-{next_station_id:04d}"
            personality = StationPersonality.from_seed(station_code)
            lat = location["lat"] + rng.uniform(-0.02, 0.02)
            lon = location["lon"] + rng.uniform(-0.02, 0.02)

            provision_hours = rng.randint(*PROVISIONING_HOURS)
            testing_hours = rng.randint(*TESTING_HOURS)

            new_station = StationRuntime(
                station_id=next_station_id,
                station_code=station_code,
                operator=operator,
                location=location,
                technology=technology,
                latitude=lat,
                longitude=lon,
                install_date=commission_time,
                personality=personality,
                status=StationStatus.PROVISIONING,
                commissioned_at=commission_time,
            )

            # Queue lifecycle transitions
            testing_at = commission_time + timedelta(hours=provision_hours)
            go_live_at = testing_at + timedelta(hours=testing_hours)
            scenario_engine.lifecycle_queue.append({
                "station": new_station, "phase": "testing", "transition_time": testing_at,
            })
            scenario_engine.lifecycle_queue.append({
                "station": new_station, "phase": "go_live", "transition_time": go_live_at,
            })

            insert_station_to_db(db, new_station)
            stations.append(new_station)
            cluster_manager.add_station(new_station)
            next_station_id += 1

            ev = {
                "event_type": EventType.STATION_COMMISSIONED.value,
                "severity": Severity.INFO.value,
                "description": f"New station {station_code} commissioned (provisioning {provision_hours}h)",
                "metadata": json.dumps({
                    "station_code": station_code, "operator": operator["code"],
                    "technology": technology,
                    "location": f"{location['province']}/{location['district']}",
                    "provisioning_hours": provision_hours, "testing_hours": testing_hours,
                }),
            }
            insert_event(db, generate_station_event(new_station, stations, commission_time, rng, ev))
            day_events += 1
            print(f"[new:{new_station.station_code}]", end=" ", flush=True)

        # ── Daily: maybe decommission old station ──────────────────────────
        for station in list(stations):
            if rng.random() < DECOMMISSION_DAILY_PROB:
                decom = scenario_engine.maybe_decommission_station(
                    station, day_start, subscriber_pool, stations,
                )
                if decom:
                    insert_event(db, generate_station_event(
                        station, stations, day_start, rng, decom,
                    ))
                    update_station_status(db, station)
                    day_events += 1
                    print(f"[decom:{station.station_code}]", end=" ", flush=True)

        # ── Hourly processing ──────────────────────────────────────────────
        for hour in range(24):
            hour_start = day_start + timedelta(hours=hour)
            time_mult = get_time_multiplier(hour_start)

            subscriber_pool.simulate_mobility(stations, hour, rng)
            traffic_buffer: list[dict] = []

            # ── Cluster event check (once per hour) ────────────────────────
            cluster_prob = CLUSTER_EVENT_HOURLY_PROB
            if rng.random() < cluster_prob:
                available = [
                    k for k in cluster_manager.get_cluster_keys()
                    if k not in scenario_engine.active_cluster_events
                ]
                if available:
                    cluster_key = rng.choice(available)
                    event_type = rng.choices(CLUSTER_EVENT_TYPES, weights=CLUSTER_EVENT_TYPE_WEIGHTS)[0]
                    dur_range = {
                        "power_outage": CLUSTER_POWER_DURATION,
                        "fiber_cut": CLUSTER_FIBER_DURATION,
                        "cooling_failure": CLUSTER_COOLING_DURATION,
                    }[event_type]
                    duration = rng.randint(*dur_range)
                    end_time = hour_start + timedelta(minutes=duration)

                    scenario_engine.active_cluster_events[cluster_key] = {
                        "type": event_type, "end_time": end_time, "started_at": hour_start,
                    }

                    for s in cluster_manager.get_cluster(cluster_key):
                        if s.status in (StationStatus.RETIRED, StationStatus.DECOMMISSIONING):
                            continue
                        s.cluster_event_active = True
                        s.cluster_event_type = event_type
                        s.cluster_event_end = end_time
                        s.pre_maintenance_status = s.status
                        s.status = StationStatus.DOWN if event_type != "cooling_failure" else StationStatus.DEGRADED

                        ev = {
                            "event_type": EventType.CLUSTER_OUTAGE_START.value,
                            "severity": Severity.CRITICAL.value,
                            "description": f"Cluster {event_type}: {cluster_key}",
                            "metadata": json.dumps({
                                "cluster": cluster_key, "outage_type": event_type,
                                "estimated_duration_min": duration,
                            }),
                        }
                        insert_event(db, generate_station_event(s, stations, hour_start, rng, ev))
                        day_events += 1

                    print(f"[cluster:{event_type[:5]}]", end=" ", flush=True)

            # ── Resolve cluster events ─────────────────────────────────────
            resolved_events = scenario_engine.maybe_resolve_cluster_events(hour_start)
            for ev in resolved_events:
                station = ev.pop("station", None)
                if station:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, ev))
                    update_station_status(db, station)
                    day_events += 1

            # ── Weather check ──────────────────────────────────────────────
            if rng.random() < WEATHER_EVENT_HOURLY_PROB:
                available_regions = [
                    r for r in WEATHER_REGIONS if r not in scenario_engine.active_weather_events
                ]
                if available_regions:
                    region = rng.choice(available_regions)
                    severity = rng.uniform(*WEATHER_SEVERITY_RANGE)
                    duration = rng.randint(*WEATHER_DURATION_RANGE)
                    end_time = hour_start + timedelta(minutes=duration)

                    scenario_engine.active_weather_events[region] = {
                        "severity": severity, "end_time": end_time, "started_at": hour_start,
                    }

                    for s in cluster_manager.regions.get(region, []):
                        if s.status in (StationStatus.RETIRED, StationStatus.DECOMMISSIONING):
                            continue
                        s.weather_severity = severity
                        s.weather_end = end_time

                        ev = {
                            "event_type": EventType.WEATHER_DEGRADATION.value,
                            "severity": Severity.WARNING.value,
                            "description": f"Weather degradation in {region} (severity={severity:.2f})",
                            "metadata": json.dumps({
                                "region": region, "weather_severity": round(severity, 3),
                                "estimated_duration_min": duration,
                            }),
                        }
                        insert_event(db, generate_station_event(s, stations, hour_start, rng, ev))
                        day_events += 1

                    print(f"[weather:{region[:5]}]", end=" ", flush=True)

            # Resolve weather
            for ev in scenario_engine.maybe_resolve_weather(hour_start):
                station = ev.pop("station", None)
                if station:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, ev))
                    day_events += 1

            # ── Lifecycle queue ────────────────────────────────────────────
            for ev in scenario_engine.process_lifecycle_queue(hour_start):
                station = ev.pop("station", None)
                if station:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, ev))
                    update_station_status(db, station)
                    day_events += 1

            # ── Per-station hourly processing ──────────────────────────────
            for station in stations:
                if station.status in (StationStatus.RETIRED, StationStatus.PROVISIONING):
                    continue

                # Maintenance resolution
                maint_end = scenario_engine.maybe_end_maintenance(station, hour_start)
                if maint_end:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, maint_end))
                    update_station_status(db, station)
                    day_events += 1

                # Incident resolution
                resolved = scenario_engine.maybe_resolve_incident(station, hour_start)
                if resolved:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, resolved))
                    day_events += 1

                # Firmware reboot
                fw = scenario_engine.maybe_firmware_reboot(station, hour_start)
                if fw:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, fw))
                    day_events += 1

                if station.status in (StationStatus.MAINTENANCE, StationStatus.DOWN, StationStatus.DECOMMISSIONING):
                    # Still generate metrics showing station is down
                    metrics = generate_performance_metrics(station, hour_start, 0, rng)
                    insert_metrics(db, metrics)
                    total_metrics += 1
                    continue

                # ── Traffic for this station this hour ─────────────────────
                active_subs = subscriber_pool.get_active_subscribers(station, hour, time_mult, rng)

                effective_mult = time_mult
                if station.mass_event_active:
                    effective_mult *= station.mass_event_sub_mult

                n_events = max(1, int(events_per_station_per_hour * effective_mult))

                # Mass event check
                me = scenario_engine.maybe_trigger_mass_event(station, hour_start)
                if me:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, me))
                    day_events += 1

                me_end = scenario_engine.maybe_end_mass_event(station, hour_start)
                if me_end:
                    insert_event(db, generate_station_event(station, stations, hour_start, rng, me_end))
                    day_events += 1

                for j in range(n_events):
                    event_time = hour_start + timedelta(seconds=rng.randint(0, 3599))
                    imsi = rng.choice(active_subs) if active_subs else hashlib.sha256(
                        f"anon-{station.station_id}-{j}".encode()
                    ).hexdigest()

                    traffic = generate_traffic_event(station, imsi, event_time, rng, subscriber_pool)
                    traffic_buffer.append(traffic)

                # ── Incident check (hourly in backfill) ────────────────────
                hourly_prob = INCIDENT_HOURLY_PROB * (2.0 - station.personality.hardware_quality)
                if rng.random() < hourly_prob and not station.incident_active:
                    incident_time = hour_start + timedelta(minutes=rng.randint(0, 59))
                    typ = rng.choices(["degradation", "failure"], weights=INCIDENT_TYPE_WEIGHTS)[0]

                    if typ == "degradation":
                        severity = rng.uniform(*DEGRADATION_SEVERITY_RANGE)
                        duration = rng.randint(*DEGRADATION_DURATION_RANGE)
                        station.status = StationStatus.DEGRADED
                    else:
                        severity = rng.uniform(*FAILURE_SEVERITY_RANGE)
                        duration = rng.randint(*FAILURE_DURATION_RANGE)
                        station.status = StationStatus.DOWN

                    station.incident_active = True
                    station.incident_type = typ
                    station.incident_start = incident_time
                    station.incident_severity = severity
                    station.incident_end = incident_time + timedelta(minutes=duration)
                    station.error_count += 1

                    ev = {
                        "event_type": EventType.INCIDENT_START.value,
                        "severity": Severity.CRITICAL.value if severity > CRITICAL_SEVERITY_THRESHOLD else Severity.ERROR.value,
                        "description": f"Incident: {typ} (severity={severity:.2f})",
                        "metadata": json.dumps({
                            "incident_type": typ, "severity": round(severity, 3),
                            "estimated_duration_min": duration,
                        }),
                    }
                    insert_event(db, generate_station_event(station, stations, incident_time, rng, ev))
                    day_events += 1

                    if station.status == StationStatus.DOWN:
                        scenario_engine.trigger_cascading_load(station, subscriber_pool)

                # ── Organic events ─────────────────────────────────────────
                density = DENSITY_PROFILES[station.location.get("density", "urban")]
                n_organic = int(density["events_per_min"] * 60 * station.personality.alarm_tendency * time_mult)
                for _ in range(max(0, n_organic)):
                    evt_time = hour_start + timedelta(seconds=rng.randint(0, 3599))
                    ev = generate_station_event(station, stations, evt_time, rng)
                    if ev:
                        insert_event(db, ev)
                        day_events += 1

                # ── Metrics ────────────────────────────────────────────────
                active_count = sum(
                    1 for s in subscriber_pool.subscribers.values()
                    if s["current_station"] == station.station_code
                )
                metrics = generate_performance_metrics(station, hour_start, active_count, rng)
                insert_metrics(db, metrics)
                total_metrics += 1

            # Flush traffic
            if traffic_buffer:
                insert_traffic_batch(db, traffic_buffer, rng)
                day_traffic += len(traffic_buffer)
                traffic_buffer.clear()

            if (hour + 1) % 6 == 0:
                print(f"[h{hour+1}: {day_traffic:,}t]", end=" ", flush=True)

        total_traffic += day_traffic
        total_events += day_events

        # Status summary
        active = sum(1 for s in stations if s.status == StationStatus.ACTIVE)
        degraded = sum(1 for s in stations if s.status == StationStatus.DEGRADED)
        down = sum(1 for s in stations if s.status == StationStatus.DOWN)
        maint = sum(1 for s in stations if s.status == StationStatus.MAINTENANCE)
        testing = sum(1 for s in stations if s.status == StationStatus.TESTING)
        retired = sum(1 for s in stations if s.status == StationStatus.RETIRED)

        print(f"→ {day_traffic:,}t {day_events}e | "
              f"active={active} degraded={degraded} down={down} maint={maint} "
              f"testing={testing} retired={retired} total={len(stations)}")

    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Traffic records: {total_traffic:,}")
    print(f"  Metric records:  {total_metrics:,}")
    print(f"  Event records:   {total_events:,}")
    print(f"  Final stations:  {len(stations)}")
    print(f"{'='*60}")


# ═════════════════════════════════════════════════════════════════════════════
# Stats
# ═════════════════════════════════════════════════════════════════════════════

def print_stats(stats: dict, start_time: float):
    elapsed = time.time() - start_time
    if elapsed == 0:
        return

    total_traffic = sum(s["traffic"] for s in stats.values())
    total_metrics = sum(s["metrics"] for s in stats.values())
    total_events = sum(s["events"] for s in stats.values())
    total_errors = sum(s["errors"] for s in stats.values())

    print(f"\n{'='*60}")
    print(f"Elapsed: {elapsed:.0f}s | Traffic: {total_traffic:,} ({total_traffic/elapsed:.1f}/s) | "
          f"Metrics: {total_metrics:,} | Events: {total_events:,} | Errors: {total_errors}")
    print(f"Stations: {len(stats)}")
    print(f"{'='*60}")

    sorted_stats = sorted(stats.items(), key=lambda x: x[1]["traffic"], reverse=True)[:5]
    for code, s in sorted_stats:
        print(f"  {code}: traffic={s['traffic']:,}, metrics={s['metrics']}, "
              f"events={s['events']}, errors={s['errors']}")


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Telecom Station Simulator v5")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_db_args(p):
        p.add_argument("--host", default="localhost")
        p.add_argument("--port", type=int, default=5433)
        p.add_argument("--db", default="station_oltp")
        p.add_argument("--user", default="station")
        p.add_argument("--password", default="station")
        p.add_argument("--stations", type=int, default=20)
        p.add_argument("--subscribers", type=int, default=2000)
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--skip-init", action="store_true",
                        help="Skip schema/dimension init (append to existing)")

    def add_scenario_args(p):
        p.add_argument("--incident-prob", type=float, default=INCIDENT_HOURLY_PROB,
                        help="Hourly incident probability per station")
        p.add_argument("--cluster-event-prob", type=float, default=CLUSTER_EVENT_HOURLY_PROB,
                        help="Hourly cluster-event probability")
        p.add_argument("--weather-prob", type=float, default=WEATHER_EVENT_HOURLY_PROB,
                        help="Hourly weather-event probability")
        p.add_argument("--mass-event-prob", type=float, default=MASS_EVENT_DAILY_PROB,
                        help="Daily mass-event probability per location")
        p.add_argument("--firmware-prob", type=float, default=FIRMWARE_ROLLOUT_PROB,
                        help="Daily firmware-rollout probability")
        p.add_argument("--new-station-prob", type=float, default=NEW_STATION_DAILY_PROB,
                        help="Daily new-station probability")
        p.add_argument("--decommission-prob", type=float, default=DECOMMISSION_DAILY_PROB,
                        help="Daily decommission probability")
        p.add_argument("--late-arrival-prob", type=float, default=LATE_ARRIVAL_PROB,
                        help="Fraction of records with late created_at")
        p.add_argument("--null-prob", type=float, default=NULL_FIELD_PROB,
                        help="Fraction of records with null fields")
        p.add_argument("--duplicate-prob", type=float, default=DUPLICATE_RECORD_PROB,
                        help="Fraction of records that are duplicated")

    # ── Stream command ─────────────────────────────────────────────────────
    sp = subparsers.add_parser("stream", help="Continuous streaming mode")
    add_db_args(sp)
    add_scenario_args(sp)
    sp.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    sp.add_argument("--stats-interval", type=int, default=30)
    sp.add_argument("--workers", type=int, default=4)
    sp.add_argument("--batch-size", type=int, default=50)

    # ── Backfill command ───────────────────────────────────────────────────
    bp = subparsers.add_parser("backfill", help="Generate historical data")
    add_db_args(bp)
    add_scenario_args(bp)
    bp.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    bp.add_argument("--days", type=int, default=7)
    bp.add_argument("--events-per-hour", type=int, default=20)
    bp.add_argument("--maintenance-prob", type=float, default=MAINTENANCE_DAILY_PROB)

    args = parser.parse_args()

    # ── Apply scenario knob overrides ──────────────────────────────────────
    global INCIDENT_HOURLY_PROB, CLUSTER_EVENT_HOURLY_PROB, WEATHER_EVENT_HOURLY_PROB
    global MASS_EVENT_DAILY_PROB, FIRMWARE_ROLLOUT_PROB, NEW_STATION_DAILY_PROB
    global DECOMMISSION_DAILY_PROB, LATE_ARRIVAL_PROB, NULL_FIELD_PROB, DUPLICATE_RECORD_PROB

    INCIDENT_HOURLY_PROB      = args.incident_prob
    CLUSTER_EVENT_HOURLY_PROB = args.cluster_event_prob
    WEATHER_EVENT_HOURLY_PROB = args.weather_prob
    MASS_EVENT_DAILY_PROB     = args.mass_event_prob
    FIRMWARE_ROLLOUT_PROB     = args.firmware_prob
    NEW_STATION_DAILY_PROB    = args.new_station_prob
    DECOMMISSION_DAILY_PROB   = args.decommission_prob
    LATE_ARRIVAL_PROB         = args.late_arrival_prob
    NULL_FIELD_PROB           = args.null_prob
    DUPLICATE_RECORD_PROB     = args.duplicate_prob

    # Deterministic seeding
    rng = random.Random(args.seed)
    random.seed(args.seed)

    db = DatabaseManager(args.host, args.port, args.db, args.user, args.password)

    try:
        db.initialize()

        if not args.skip_init:
            initialize_schema(db)
            seed_dimensions(db)

        print(f"\nCreating {args.stations} stations (seed={args.seed})...")
        stations = create_stations(db, args.stations, rng)

        print(f"Creating subscriber pool ({args.subscribers} subscribers)...")
        subscriber_pool = SubscriberPool(args.subscribers, stations, rng)
        print(f"  {len(subscriber_pool.subscribers)} subscribers distributed")

        cluster_manager = ClusterManager(stations)
        scenario_engine = ScenarioEngine(stations, cluster_manager, rng)

        print(f"  Clusters: {len(cluster_manager.clusters)}")
        print(f"  Regions:  {list(cluster_manager.regions.keys())}")

        if args.command == "stream":
            stop_event = threading.Event()
            stats: dict = {}
            workers: list[StationWorker] = []
            n_workers = min(args.workers, len(stations))
            chunk_size = max(1, len(stations) // n_workers)

            for i in range(n_workers):
                chunk = stations[i * chunk_size : (i + 1) * chunk_size]
                if not chunk:
                    continue
                worker = StationWorker(
                    stations=chunk,
                    all_stations=stations,
                    db=db,
                    subscriber_pool=subscriber_pool,
                    scenario_engine=scenario_engine,
                    stop_event=stop_event,
                    stats=stats,
                    rng_seed=args.seed + i,
                    traffic_batch_size=args.batch_size,
                )
                workers.append(worker)
                worker.start()

            # Start global scenario thread
            global_thread = GlobalScenarioThread(
                db=db,
                stations=stations,
                scenario_engine=scenario_engine,
                cluster_manager=cluster_manager,
                subscriber_pool=subscriber_pool,
                stop_event=stop_event,
                stats=stats,
                rng=rng,
            )
            global_thread.start()

            def sig_handler(sig, frame):
                print("\n\nShutting down...")
                stop_event.set()

            signal_module.signal(signal_module.SIGINT, sig_handler)

            print(f"\nStreaming with {n_workers} workers, {len(stations)} stations. "
                  f"Ctrl+C to stop.\n")
            start_time = time.time()

            while not stop_event.is_set():
                time.sleep(args.stats_interval)
                if stop_event.is_set():
                    break
                print_stats(stats, start_time)
                if args.duration > 0 and (time.time() - start_time) >= args.duration:
                    print(f"\nDuration {args.duration}s reached.")
                    stop_event.set()

            for w in workers:
                w.join(timeout=3)
            global_thread.join(timeout=3)
            print_stats(stats, start_time)
            print("\nSimulation complete!")

        elif args.command == "backfill":
            start_date_val = date.fromisoformat(args.start)
            run_backfill(
                db=db,
                stations=stations,
                subscriber_pool=subscriber_pool,
                scenario_engine=scenario_engine,
                cluster_manager=cluster_manager,
                start_date=start_date_val,
                num_days=args.days,
                rng=rng,
                events_per_station_per_hour=args.events_per_hour,
                maintenance_probability=args.maintenance_prob,
            )

    except KeyboardInterrupt:
        print("\nInterrupted.")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    main()
