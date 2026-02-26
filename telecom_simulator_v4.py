"""
Telecom Station Simulator v4
=============================
Generates structurally complex cell station data into PostgreSQL OLTP.

Key improvements over v1/v3:
- Station personalities (urban/suburban/rural with different traffic profiles)
- Persistent subscriber pool with mobility across stations
- Incident simulation (degradation, failure, cascading overload)
- Planned maintenance windows with SLA exclusion metadata
- Correlated failures (neighbor stations absorb handover load)
- Configuration changes (SCD Type 2 in the config table)
- Time-of-day / day-of-week realistic patterns (Vietnam UTC+7)
- Deterministic seeding for reproducible runs
- Both streaming (live pipeline) and backfill (benchmark) modes

Data lands in the same schema as your existing pipeline:
  telecom.subscriber_traffic  (high-volume fact)
  telecom.performance_metrics (medium-volume fact)
  telecom.station_events      (low-volume fact)
  telecom.base_station        (dimension)
  telecom.operator            (dimension)
  telecom.location            (dimension)
  telecom.configuration       (SCD Type 2 dimension)

Usage:
    # Streaming mode — continuous, feeds the live pipeline
    python telecom_simulator_v4.py stream --stations 20 --duration 3600

    # Backfill mode — generate N days of historical data for benchmarking
    python telecom_simulator_v4.py backfill --stations 50 --start 2026-01-01 --days 7

    # With custom Postgres connection
    python telecom_simulator_v4.py stream --host localhost --port 5433 --db station_oltp
"""

import argparse
import hashlib
import json
import math
import os
import random
import signal
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

# =============================================================================
# Constants & Configuration
# =============================================================================

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")  # UTC+7

# --- Operators (realistic Vietnam market share) ---
OPERATORS = [
    {"id": 1, "code": "VTL", "name": "Viettel"},
    {"id": 2, "code": "VNP", "name": "Vinaphone"},
    {"id": 3, "code": "MBF", "name": "MobiFone"},
    {"id": 4, "code": "FPT", "name": "FPT Telecom"},
    {"id": 5, "code": "ITL", "name": "iTelecom"},
    {"id": 6, "code": "VNM", "name": "Vietnamobile"},
]
OPERATOR_WEIGHTS = [0.45, 0.20, 0.18, 0.05, 0.04, 0.08]

# --- Locations with urban classification ---
LOCATIONS = [
    # Urban cores — high density, high traffic
    {"id": 1,  "province": "Ha Noi",      "district": "Hoan Kiem",    "region": "North",   "density": "urban_core"},
    {"id": 2,  "province": "Ha Noi",      "district": "Ba Dinh",      "region": "North",   "density": "urban_core"},
    {"id": 3,  "province": "Ha Noi",      "district": "Cau Giay",     "region": "North",   "density": "urban_core"},
    {"id": 4,  "province": "Ho Chi Minh", "district": "Quan 1",       "region": "South",   "density": "urban_core"},
    {"id": 5,  "province": "Ho Chi Minh", "district": "Quan 3",       "region": "South",   "density": "urban_core"},
    {"id": 6,  "province": "Ho Chi Minh", "district": "Binh Thanh",   "region": "South",   "density": "urban_core"},
    {"id": 7,  "province": "Da Nang",     "district": "Hai Chau",     "region": "Central", "density": "urban_core"},
    # Urban fringe — medium density
    {"id": 8,  "province": "Ha Noi",      "district": "Long Bien",    "region": "North",   "density": "urban"},
    {"id": 9,  "province": "Ha Noi",      "district": "Hoang Mai",    "region": "North",   "density": "urban"},
    {"id": 10, "province": "Ho Chi Minh", "district": "Quan 7",       "region": "South",   "density": "urban"},
    {"id": 11, "province": "Ho Chi Minh", "district": "Thu Duc",      "region": "South",   "density": "urban"},
    {"id": 12, "province": "Da Nang",     "district": "Son Tra",      "region": "Central", "density": "urban"},
    {"id": 13, "province": "Hai Phong",   "district": "Le Chan",      "region": "North",   "density": "urban"},
    {"id": 14, "province": "Can Tho",     "district": "Ninh Kieu",    "region": "South",   "density": "urban"},
    # Suburban
    {"id": 15, "province": "Binh Duong",  "district": "Thu Dau Mot",  "region": "South",   "density": "suburban"},
    {"id": 16, "province": "Dong Nai",    "district": "Bien Hoa",     "region": "South",   "density": "suburban"},
    {"id": 17, "province": "Bac Ninh",    "district": "Bac Ninh",     "region": "North",   "density": "suburban"},
    {"id": 18, "province": "Quang Ninh",  "district": "Ha Long",      "region": "North",   "density": "suburban"},
    {"id": 19, "province": "Khanh Hoa",   "district": "Nha Trang",    "region": "Central", "density": "suburban"},
    {"id": 20, "province": "Lam Dong",    "district": "Da Lat",       "region": "Central", "density": "suburban"},
    # Rural
    {"id": 21, "province": "Ha Giang",    "district": "Ha Giang",     "region": "North",   "density": "rural"},
    {"id": 22, "province": "Lai Chau",    "district": "Lai Chau",     "region": "North",   "density": "rural"},
    {"id": 23, "province": "Dak Nong",    "district": "Gia Nghia",    "region": "Central", "density": "rural"},
    {"id": 24, "province": "Ca Mau",      "district": "Ca Mau",       "region": "South",   "density": "rural"},
    {"id": 25, "province": "Kon Tum",     "district": "Kon Tum",      "region": "Central", "density": "rural"},
]

# Density-based traffic multipliers
DENSITY_PROFILES = {
    #                  base_traffic  base_subscribers  event_rate  metric_variation
    "urban_core":     {"traffic": 8.0,  "subs": 400, "events_per_min": 0.5,  "metric_noise": 0.15},
    "urban":          {"traffic": 4.0,  "subs": 200, "events_per_min": 0.3,  "metric_noise": 0.10},
    "suburban":       {"traffic": 2.0,  "subs": 80,  "events_per_min": 0.15, "metric_noise": 0.08},
    "rural":          {"traffic": 0.5,  "subs": 20,  "events_per_min": 0.05, "metric_noise": 0.05},
}

# Technology profiles
TECHNOLOGIES = {
    "2G": {"throughput_mult": 0.01, "latency_base": 150, "latency_std": 50, "max_subs": 50},
    "3G": {"throughput_mult": 0.10, "latency_base": 80,  "latency_std": 30, "max_subs": 150},
    "4G": {"throughput_mult": 1.00, "latency_base": 30,  "latency_std": 10, "max_subs": 500},
    "5G": {"throughput_mult": 5.00, "latency_base": 10,  "latency_std": 3,  "max_subs": 1000},
}
TECH_WEIGHTS = [0.03, 0.07, 0.40, 0.50]
TECH_NAMES = ["2G", "3G", "4G", "5G"]

# --- Time-of-Day Patterns (Vietnam local time) ---
HOURLY_TRAFFIC_MULT = {
    0: 0.15, 1: 0.10, 2: 0.08, 3: 0.08, 4: 0.10, 5: 0.25,
    6: 0.45, 7: 0.70, 8: 0.80, 9: 0.65, 10: 0.60, 11: 0.65,
    12: 0.85, 13: 0.80, 14: 0.60, 15: 0.60, 16: 0.65, 17: 0.85,
    18: 0.90, 19: 0.95, 20: 1.00, 21: 0.95, 22: 0.70, 23: 0.45,
}
DAY_OF_WEEK_MULT = {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.05, 4: 1.10, 5: 0.85, 6: 0.80}

# Weekend adjustments (later wake, later sleep)
WEEKEND_HOUR_ADJ = {
    6: 0.5, 7: 0.6, 8: 0.7, 9: 0.85, 21: 1.1, 22: 1.15, 23: 1.05,
}

# =============================================================================
# ★ TUNABLE SCENARIO KNOBS — adjust these to control incident/event behavior ★
# =============================================================================
#
# TIP: For testing incidents in streaming mode, increase INCIDENT_HOURLY_PROB
#      to 0.10–0.30 so you see failures within minutes instead of hours.
#      In backfill mode the probability is checked once per hour per station,
#      so even the default 0.005 produces incidents over multi-day runs.
#

# --- Incident Probabilities ---
INCIDENT_HOURLY_PROB = 0.005          # Base probability of incident per hour per station
                                       # Modified by hardware quality: actual = prob * (2.0 - hw_quality)
                                       # Default 0.005 = ~0.5%/hour. Set to 0.10+ for testing.

INCIDENT_TYPE_WEIGHTS = [0.7, 0.3]    # [degradation, failure] — weight ratio
                                       # Higher failure weight = more full outages

# --- Degradation Incident Parameters ---
DEGRADATION_SEVERITY_RANGE = (0.3, 0.7)   # min/max severity (0=mild, 1=total)
DEGRADATION_DURATION_RANGE = (15, 180)     # min/max duration in minutes

# --- Failure Incident Parameters ---
FAILURE_SEVERITY_RANGE = (0.8, 1.0)        # min/max severity
FAILURE_DURATION_RANGE = (5, 60)           # min/max duration in minutes

# --- Severity Classification Threshold ---
CRITICAL_SEVERITY_THRESHOLD = 0.7          # Above this → CRITICAL, below → ERROR

# --- Maintenance (backfill mode) ---
MAINTENANCE_DAILY_PROB = 0.02              # Per-station daily probability of planned maintenance
MAINTENANCE_HOUR_RANGE = (1, 5)            # Hour window for scheduling maintenance (low-traffic)
MAINTENANCE_DURATION_RANGE = (30, 120)     # Duration range in minutes
TECH_UPGRADE_PROB = 0.20                   # Probability that maintenance includes a tech upgrade

# --- Traffic Incident Effects ---
DEGRADATION_THROUGHPUT_FACTOR = 0.5        # Throughput reduction: bytes *= (1 - severity * factor)
DEGRADATION_LATENCY_FACTOR = 5.0          # Latency increase: latency *= (1 + severity * factor)
DEGRADATION_LOSS_FACTOR = 10.0            # Packet loss increase: loss *= (1 + severity * factor)
FAILURE_THROUGHPUT_FACTOR = 0.05           # During failure: throughput drops to 5% of normal
FAILURE_LATENCY_MULT = 10.0               # During failure: latency × this
FAILURE_LOSS_MULT = 20.0                  # During failure: packet loss × this
HIGH_PACKET_LOSS_PROB = 0.15              # Chance of elevated base packet loss per traffic event

# --- Performance Metric Effects ---
DEGRADATION_CPU_SPIKE = 30                 # CPU increase during degradation: +severity * this
FAILURE_CPU_RANGE = (95, 100)              # CPU pegged at max during failure
FAILURE_THROUGHPUT_PCT = 0.01              # Throughput fraction during failure
DEGRADATION_THROUGHPUT_REDUCTION = 0.6     # Throughput *= (1 - severity * this)

# --- Organic Event Type Weights ---
ORGANIC_EVENT_WEIGHTS = [0.30, 0.25, 0.20, 0.10, 0.15]
#                        handover attach  detach alarm  paging

# --- Alarm Severity Weights ---
ALARM_SEVERITY_WEIGHTS = [0.70, 0.25, 0.05]
#                         warning error  critical

# --- Subscriber Behavior ---
SUBSCRIBER_ACTIVE_FACTOR = 0.6            # Fraction of subscribers active: traffic_mult * this
TRAFFIC_SAMPLE_RATE = 0.1                 # Fraction of active subs sampled per tick (streaming)
MOBILITY_GO_HOME_PROB = 0.3               # Probability a mobile subscriber returns home vs roams
MOBILITY_BASE_RATE = 0.02                 # Base hourly mobility rate (non-commute hours)
COMMUTE_MOBILITY_RATES = {                 # Hour → mobility probability during commute
    7: 0.15, 8: 0.20, 9: 0.10,
    17: 0.15, 18: 0.20, 19: 0.10,
}

# =============================================================================

# --- Subscriber Pool ---
PROTOCOLS = ["TCP", "UDP", "ICMP", "OTHER"]
PROTOCOL_WEIGHTS = [0.70, 0.25, 0.03, 0.02]
COMMON_PORTS = [80, 443, 8080, 53, 3478, 5060, 1935, 554, 8443, 3000]

# --- Event Configuration ---
class EventType(Enum):
    HANDOVER = "handover"
    ATTACH = "attach"
    DETACH = "detach"
    PAGING = "paging"
    ALARM = "alarm"
    CONFIG_CHANGE = "config_change"
    MAINTENANCE_START = "maintenance_start"
    MAINTENANCE_END = "maintenance_end"
    INCIDENT_START = "incident_start"
    INCIDENT_END = "incident_end"

class Severity(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class StationStatus(Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    DOWN = "down"
    INACTIVE = "inactive"

FREQUENCY_BANDS = {
    "2G": ["GSM-900", "GSM-1800"],
    "3G": ["UMTS-2100", "UMTS-900"],
    "4G": ["Band-3", "Band-7", "Band-20"],
    "5G": ["n78", "n258", "n77"],
}


# =============================================================================
# Station Personality — gives each station a unique behavioral fingerprint
# =============================================================================

@dataclass
class StationPersonality:
    """Deterministic per-station characteristics that affect its behavior."""
    # Hardware quality: 0.0 (old/flaky) to 1.0 (new/reliable)
    hardware_quality: float
    # Base CPU load even when idle (some stations have background processes)
    idle_cpu: float
    # Temperature sensitivity (some stations in hot enclosures)
    temp_base: float
    temp_sensitivity: float
    # How often this station generates alarms (some are noisier)
    alarm_tendency: float
    # Capacity headroom — fraction of max before performance degrades
    capacity_threshold: float

    @staticmethod
    def from_seed(station_code: str) -> "StationPersonality":
        """Deterministic personality from station code."""
        rng = random.Random(hashlib.sha256(station_code.encode()).hexdigest())
        hardware_quality = rng.betavariate(3, 1.5)  # skewed toward good
        return StationPersonality(
            hardware_quality=hardware_quality,
            idle_cpu=rng.uniform(5, 25),
            temp_base=rng.uniform(30, 45),
            temp_sensitivity=rng.uniform(0.5, 2.0),
            alarm_tendency=rng.uniform(0.3, 3.0),
            capacity_threshold=0.6 + hardware_quality * 0.3,  # good hardware → higher threshold
        )


# =============================================================================
# Station Runtime State
# =============================================================================

@dataclass
class StationRuntime:
    """Mutable state tracked during simulation."""
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

    # Subscriber tracking
    active_subscribers: set = field(default_factory=set)

    # Performance counters
    error_count: int = 0
    uptime_start: datetime = None

    # Incident state
    incident_active: bool = False
    incident_type: str = None         # "degradation" | "failure" | "overload"
    incident_start: datetime = None
    incident_severity: float = 0.0    # 0.0 (mild) to 1.0 (total failure)
    incident_end: datetime = None

    # Maintenance state
    maintenance_active: bool = False
    maintenance_end: datetime = None
    pre_maintenance_status: StationStatus = None

    # Neighbor references (for cascading failures)
    neighbors: list = field(default_factory=list)

    def __post_init__(self):
        if self.uptime_start is None:
            self.uptime_start = datetime.now(timezone.utc)


# =============================================================================
# Subscriber Pool — persistent subscribers that move between stations
# =============================================================================

class SubscriberPool:
    """
    Maintains a pool of subscriber identities that persist across time.
    Subscribers have "home" stations but can roam to neighbors.
    """

    def __init__(self, pool_size: int, stations: list["StationRuntime"], rng: random.Random):
        self.rng = rng
        self.subscribers = {}  # imsi_hash → {"home_station": code, "current_station": code}

        # Distribute subscribers across stations proportional to density profile
        station_weights = []
        for s in stations:
            density = s.location.get("density", "urban")
            base_subs = DENSITY_PROFILES[density]["subs"]
            station_weights.append(base_subs)

        total_weight = sum(station_weights) or 1
        for i, s in enumerate(stations):
            n_subs = max(1, int(pool_size * station_weights[i] / total_weight))
            for _ in range(n_subs):
                imsi = f"452{rng.randint(1, 7):02d}{rng.randint(0, 9999999999):010d}"
                imsi_hash = hashlib.sha256(imsi.encode()).hexdigest()
                self.subscribers[imsi_hash] = {
                    "home_station": s.station_code,
                    "current_station": s.station_code,
                    "mobility": rng.uniform(0.0, 1.0),  # how likely to roam
                }

    def get_active_subscribers(
        self, station: "StationRuntime", hour: int, traffic_mult: float, rng: random.Random
    ) -> list[str]:
        """Return subscriber hashes currently at this station."""
        result = []
        for imsi_hash, sub in self.subscribers.items():
            if sub["current_station"] != station.station_code:
                continue
            # Not all subscribers are active at every hour
            if rng.random() < traffic_mult * SUBSCRIBER_ACTIVE_FACTOR:
                result.append(imsi_hash)
        return result

    def simulate_mobility(self, stations: list["StationRuntime"], hour: int, rng: random.Random):
        """
        Move some subscribers between stations.
        Morning/evening commute hours have more mobility.
        """
        base_mobility = COMMUTE_MOBILITY_RATES.get(hour, MOBILITY_BASE_RATE)

        station_map = {s.station_code: s for s in stations}

        for imsi_hash, sub in self.subscribers.items():
            if rng.random() > base_mobility * sub["mobility"]:
                continue

            current = station_map.get(sub["current_station"])
            if not current or not current.neighbors:
                continue

            # Move to a neighbor or back home
            if rng.random() < MOBILITY_GO_HOME_PROB and sub["current_station"] != sub["home_station"]:
                sub["current_station"] = sub["home_station"]  # go home
            else:
                neighbor = rng.choice(current.neighbors)
                if station_map.get(neighbor) and station_map[neighbor].status == StationStatus.ACTIVE:
                    sub["current_station"] = neighbor


# =============================================================================
# Scenario Engine — incidents, maintenance, cascading failures
# =============================================================================

class ScenarioEngine:
    """Generates and manages time-based scenarios across stations."""

    def __init__(self, stations: list[StationRuntime], rng: random.Random):
        self.stations = {s.station_code: s for s in stations}
        self.rng = rng
        self.scheduled_events = []  # (datetime, callback, args)

    def maybe_trigger_incident(self, station: StationRuntime, current_time: datetime) -> Optional[dict]:
        """
        Probabilistically trigger an incident. Flaky hardware = more incidents.
        Returns an event dict if triggered, None otherwise.
        """
        if station.incident_active or station.maintenance_active:
            return None
        if station.status != StationStatus.ACTIVE:
            return None

        # Base probability modified by hardware quality
        hourly_prob = INCIDENT_HOURLY_PROB * (2.0 - station.personality.hardware_quality)
        # Convert to per-second probability for streaming mode
        per_second_prob = hourly_prob / 3600

        if self.rng.random() > per_second_prob:
            return None

        # Choose incident type weighted by station characteristics
        incident_type = self.rng.choices(
            ["degradation", "failure"],
            weights=INCIDENT_TYPE_WEIGHTS
        )[0]

        if incident_type == "degradation":
            # Gradual degradation: latency increases, packet loss rises
            severity = self.rng.uniform(*DEGRADATION_SEVERITY_RANGE)
            duration_minutes = self.rng.randint(*DEGRADATION_DURATION_RANGE)
            station.status = StationStatus.DEGRADED
        else:
            # Full failure: traffic drops to near-zero
            severity = self.rng.uniform(*FAILURE_SEVERITY_RANGE)
            duration_minutes = self.rng.randint(*FAILURE_DURATION_RANGE)
            station.status = StationStatus.DOWN

        station.incident_active = True
        station.incident_type = incident_type
        station.incident_start = current_time
        station.incident_severity = severity
        station.incident_end = current_time + timedelta(minutes=duration_minutes)
        station.error_count += 1

        return {
            "event_type": EventType.INCIDENT_START.value,
            "severity": Severity.CRITICAL.value if severity > CRITICAL_SEVERITY_THRESHOLD else Severity.ERROR.value,
            "description": f"Incident: {incident_type} (severity={severity:.2f}, est. duration={duration_minutes}min)",
            "metadata": json.dumps({
                "incident_type": incident_type,
                "severity": round(severity, 3),
                "estimated_duration_min": duration_minutes,
                "hardware_quality": round(station.personality.hardware_quality, 3),
            }),
        }

    def maybe_resolve_incident(self, station: StationRuntime, current_time: datetime) -> Optional[dict]:
        """Check if an active incident should resolve."""
        if not station.incident_active:
            return None
        if current_time < station.incident_end:
            return None

        station.incident_active = False
        old_type = station.incident_type
        station.incident_type = None
        station.incident_severity = 0.0
        station.status = StationStatus.ACTIVE
        station.uptime_start = current_time

        return {
            "event_type": EventType.INCIDENT_END.value,
            "severity": Severity.INFO.value,
            "description": f"Incident resolved: {old_type}",
            "metadata": json.dumps({
                "incident_type": old_type,
                "duration_min": round((current_time - station.incident_start).total_seconds() / 60, 1),
            }),
        }

    def trigger_cascading_load(self, failed_station: StationRuntime, subscriber_pool: SubscriberPool):
        """
        When a station goes down, its subscribers try to connect to neighbors.
        This increases load on neighboring stations.
        """
        if not failed_station.neighbors:
            return

        for imsi_hash, sub in subscriber_pool.subscribers.items():
            if sub["current_station"] != failed_station.station_code:
                continue
            # Subscriber tries to connect to a neighbor
            for neighbor_code in failed_station.neighbors:
                neighbor = self.stations.get(neighbor_code)
                if neighbor and neighbor.status in (StationStatus.ACTIVE, StationStatus.DEGRADED):
                    sub["current_station"] = neighbor_code
                    break

    def schedule_maintenance(
        self, station: StationRuntime, start_time: datetime, duration_minutes: int, upgrade_tech: bool = False
    ) -> list[dict]:
        """Schedule a maintenance window. Returns start events."""
        station.maintenance_active = True
        station.maintenance_end = start_time + timedelta(minutes=duration_minutes)
        station.pre_maintenance_status = station.status
        station.status = StationStatus.MAINTENANCE

        events = [{
            "event_type": EventType.MAINTENANCE_START.value,
            "severity": Severity.INFO.value,
            "description": f"Planned maintenance started (duration={duration_minutes}min"
                          + (", tech upgrade" if upgrade_tech else "") + ")",
            "metadata": json.dumps({
                "planned_duration_min": duration_minutes,
                "tech_upgrade": upgrade_tech,
                "sla_excluded": True,
            }),
        }]

        if upgrade_tech:
            tech_idx = TECH_NAMES.index(station.technology)
            if tech_idx < len(TECH_NAMES) - 1:
                old_tech = station.technology
                station.technology = TECH_NAMES[tech_idx + 1]
                events.append({
                    "event_type": EventType.CONFIG_CHANGE.value,
                    "severity": Severity.INFO.value,
                    "description": f"Technology upgrade: {old_tech} → {station.technology}",
                    "metadata": json.dumps({
                        "change_type": "tech_upgrade",
                        "old_value": old_tech,
                        "new_value": station.technology,
                        "during_maintenance": True,
                    }),
                })
        return events

    def maybe_end_maintenance(self, station: StationRuntime, current_time: datetime) -> Optional[dict]:
        """Check if maintenance should end."""
        if not station.maintenance_active:
            return None
        if current_time < station.maintenance_end:
            return None

        station.maintenance_active = False
        station.status = StationStatus.ACTIVE
        station.uptime_start = current_time

        return {
            "event_type": EventType.MAINTENANCE_END.value,
            "severity": Severity.INFO.value,
            "description": "Planned maintenance completed",
            "metadata": json.dumps({
                "actual_duration_min": round(
                    (current_time - (station.maintenance_end - timedelta(minutes=30))).total_seconds() / 60, 1
                ),
                "sla_excluded": True,
            }),
        }


# =============================================================================
# Traffic Generators — one per fact table
# =============================================================================

def get_time_multiplier(local_time: datetime) -> float:
    """Get traffic multiplier based on Vietnam local time."""
    hour = local_time.hour
    weekday = local_time.weekday()

    mult = HOURLY_TRAFFIC_MULT.get(hour, 0.5)
    dow = DAY_OF_WEEK_MULT.get(weekday, 1.0)

    if weekday >= 5:
        mult *= WEEKEND_HOUR_ADJ.get(hour, 1.0)

    return min(1.0, mult * dow)


def generate_traffic_event(
    station: StationRuntime,
    imsi_hash: str,
    event_time: datetime,
    rng: random.Random,
) -> dict:
    """Generate a single subscriber traffic record."""

    tech = TECHNOLOGIES[station.technology]
    density = DENSITY_PROFILES[station.location.get("density", "urban")]
    personality = station.personality

    # Base throughput from tech × density
    base_bytes = int(density["traffic"] * tech["throughput_mult"] * rng.uniform(500, 50000))

    # Incident degradation: reduce throughput, increase latency
    latency_mult = 1.0
    loss_mult = 1.0
    if station.incident_active:
        sev = station.incident_severity
        if station.incident_type == "degradation":
            base_bytes = int(base_bytes * (1.0 - sev * DEGRADATION_THROUGHPUT_FACTOR))
            latency_mult = 1.0 + sev * DEGRADATION_LATENCY_FACTOR
            loss_mult = 1.0 + sev * DEGRADATION_LOSS_FACTOR
        elif station.incident_type == "failure":
            base_bytes = int(base_bytes * FAILURE_THROUGHPUT_FACTOR)
            latency_mult = FAILURE_LATENCY_MULT
            loss_mult = FAILURE_LOSS_MULT

    # Hardware quality affects jitter and packet loss
    quality_factor = 0.5 + personality.hardware_quality * 0.5

    latency = max(1.0, rng.gauss(
        tech["latency_base"] * latency_mult,
        tech["latency_std"] * latency_mult
    ))
    jitter = max(0.0, rng.gauss(latency * 0.1, latency * 0.05))

    base_loss = 0.5 if rng.random() > HIGH_PACKET_LOSS_PROB else rng.uniform(1.0, 5.0)
    packet_loss = min(100.0, base_loss * loss_mult / quality_factor)

    bytes_up = int(base_bytes * rng.uniform(0.1, 0.4))
    bytes_down = int(base_bytes * rng.uniform(0.5, 0.9))

    return {
        "station_id": station.station_id,
        "event_time": event_time,
        "imsi_hash": imsi_hash,
        "tmsi": f"TMSI{rng.randint(10000000, 99999999)}",
        "ip_address": _random_ip(rng),
        "destination_ip": _random_ip(rng),
        "destination_port": rng.choice(COMMON_PORTS),
        "protocol": rng.choices(PROTOCOLS, weights=PROTOCOL_WEIGHTS)[0],
        "bytes_up": max(0, bytes_up),
        "bytes_down": max(0, bytes_down),
        "packets_up": max(1, int(bytes_up / rng.uniform(100, 1500))),
        "packets_down": max(1, int(bytes_down / rng.uniform(100, 1500))),
        "latency_ms": round(latency, 3),
        "jitter_ms": round(jitter, 3),
        "packet_loss_pct": round(packet_loss, 2),
        "connection_duration_ms": rng.randint(50, 120000),
    }


def generate_performance_metrics(
    station: StationRuntime,
    metric_time: datetime,
    active_sub_count: int,
    rng: random.Random,
) -> dict:
    """Generate performance metrics reflecting station state."""

    tech = TECHNOLOGIES[station.technology]
    personality = station.personality
    uptime = int((metric_time - station.uptime_start).total_seconds())

    # Load factor: how close to capacity
    max_subs = tech["max_subs"]
    load_factor = min(1.0, active_sub_count / max(1, max_subs))

    # CPU: idle base + load-proportional + noise
    cpu_base = personality.idle_cpu + load_factor * (70 - personality.idle_cpu)
    cpu_noise = rng.gauss(0, personality.idle_cpu * 0.1)

    # Incident effects on metrics
    if station.incident_active:
        sev = station.incident_severity
        if station.incident_type == "degradation":
            cpu_base += sev * DEGRADATION_CPU_SPIKE
        elif station.incident_type == "failure":
            cpu_base = rng.uniform(*FAILURE_CPU_RANGE)

    cpu = min(100, max(0, cpu_base + cpu_noise))

    # Memory correlates with CPU but with lag
    memory = min(100, max(0, personality.idle_cpu * 1.5 + load_factor * 50 + rng.gauss(0, 5)))

    # Temperature: base + load-driven heat + ambient variation
    hour = metric_time.hour
    ambient_adj = math.sin((hour - 6) * math.pi / 12) * 5  # peaks at noon
    temp = personality.temp_base + load_factor * personality.temp_sensitivity * 10 + ambient_adj + rng.gauss(0, 1)

    # Throughput
    max_throughput = tech["throughput_mult"] * 1000  # Mbps
    throughput_pct = load_factor * rng.uniform(0.3, 0.9)
    if station.incident_active and station.incident_type == "failure":
        throughput_pct *= FAILURE_THROUGHPUT_PCT
    elif station.incident_active and station.incident_type == "degradation":
        throughput_pct *= (1.0 - station.incident_severity * DEGRADATION_THROUGHPUT_REDUCTION)

    return {
        "station_id": station.station_id,
        "metric_time": metric_time,
        "cpu_util_pct": round(cpu, 2),
        "memory_util_pct": round(memory, 2),
        "disk_util_pct": round(rng.uniform(15, 55) + load_factor * 10, 2),
        "temperature_c": round(temp, 2),
        "active_connections": active_sub_count,
        "throughput_mbps": round(max_throughput * throughput_pct, 3),
        "uptime_seconds": max(0, uptime),
        "error_count": station.error_count,
    }


def generate_station_event(
    station: StationRuntime,
    all_stations: list[StationRuntime],
    event_time: datetime,
    rng: random.Random,
    event_override: dict = None,
) -> Optional[dict]:
    """
    Generate a station event. If event_override is provided, use it
    (for scenario-driven events). Otherwise, generate organic events.
    """
    if event_override:
        return {
            "station_id": station.station_id,
            "event_time": event_time,
            "event_type": event_override["event_type"],
            "severity": event_override["severity"],
            "description": event_override.get("description"),
            "metadata": event_override.get("metadata"),
            "target_station_id": event_override.get("target_station_id"),
        }

    # Organic events — probability modulated by station personality
    density = DENSITY_PROFILES[station.location.get("density", "urban")]
    base_prob = density["events_per_min"] / 60  # per-second probability
    alarm_prob = base_prob * station.personality.alarm_tendency

    if station.maintenance_active or station.status == StationStatus.DOWN:
        return None  # no organic events during maintenance or outage

    if rng.random() > alarm_prob:
        return None

    event_type = rng.choices(
        [EventType.HANDOVER, EventType.ATTACH, EventType.DETACH, EventType.ALARM, EventType.PAGING],
        weights=ORGANIC_EVENT_WEIGHTS,
    )[0]

    event = {
        "station_id": station.station_id,
        "event_time": event_time,
        "event_type": event_type.value,
        "severity": Severity.INFO.value,
        "description": None,
        "metadata": None,
        "target_station_id": None,
    }

    if event_type == EventType.HANDOVER:
        if station.neighbors:
            target_code = rng.choice(station.neighbors)
            target = next((s for s in all_stations if s.station_code == target_code), None)
            if target:
                event["target_station_id"] = target.station_id
                signal = rng.randint(-110, -65)
                event["description"] = f"Handover to {target_code}"
                event["metadata"] = json.dumps({
                    "reason": rng.choice(["signal_quality", "load_balance", "coverage_gap"]),
                    "signal_strength_dbm": signal,
                    "source_load_pct": rng.randint(30, 95),
                })

    elif event_type == EventType.ATTACH:
        event["description"] = "UE attached to network"
        event["metadata"] = json.dumps({"attach_type": rng.choice(["initial", "re-attach", "handover"])})

    elif event_type == EventType.DETACH:
        event["description"] = "UE detached from network"
        event["metadata"] = json.dumps({"reason": rng.choice(["user_initiated", "network_initiated", "timeout", "power_off"])})

    elif event_type == EventType.ALARM:
        severity = rng.choices(
            [Severity.WARNING, Severity.ERROR, Severity.CRITICAL],
            weights=ALARM_SEVERITY_WEIGHTS,
        )[0]
        event["severity"] = severity.value

        alarm_defs = [
            ("High CPU utilization", "cpu_high"),
            ("Temperature warning", "temp_warning"),
            ("Link degradation detected", "link_degraded"),
            ("Power supply fluctuation", "power_issue"),
            ("Disk space low", "disk_low"),
            ("Backhaul latency elevated", "backhaul_latency"),
            ("Antenna VSWR out of range", "vswr_alarm"),
        ]
        alarm = rng.choice(alarm_defs)
        event["description"] = alarm[0]
        event["metadata"] = json.dumps({
            "alarm_code": alarm[1],
            "threshold_exceeded": True,
            "current_value": round(rng.uniform(80, 100), 1),
        })

        if severity in (Severity.ERROR, Severity.CRITICAL):
            station.error_count += 1

    elif event_type == EventType.PAGING:
        event["description"] = "Paging request"
        event["metadata"] = json.dumps({"paging_cause": rng.choice(["voice_call", "sms", "data", "emergency"])})

    return event


# =============================================================================
# Helpers
# =============================================================================

def _random_ip(rng: random.Random) -> str:
    return f"{rng.randint(1, 255)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def assign_neighbors(stations: list[StationRuntime]):
    """
    Assign 2-4 neighbors per station based on same location or nearby locations.
    Stations in the same province are potential neighbors.
    """
    by_province = {}
    for s in stations:
        prov = s.location["province"]
        by_province.setdefault(prov, []).append(s)

    for s in stations:
        prov = s.location["province"]
        candidates = [c.station_code for c in by_province[prov] if c.station_code != s.station_code]

        # Also add a few from same region
        region_candidates = [
            c.station_code
            for loc_stations in by_province.values()
            for c in loc_stations
            if c.location.get("region") == s.location.get("region")
            and c.station_code != s.station_code
            and c.station_code not in candidates
        ]
        candidates.extend(region_candidates[:3])

        s.neighbors = random.sample(candidates, min(len(candidates), random.randint(2, 4)))


# =============================================================================
# Database Manager (same interface as original)
# =============================================================================

class DatabaseManager:
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.connection_params = {
            "host": host, "port": port, "database": database,
            "user": user, "password": password,
        }
        self._pool = None

    def initialize(self):
        self._pool = pool.ThreadedConnectionPool(minconn=2, maxconn=15, **self.connection_params)
        print(f"✓ Connected to PostgreSQL at {self.connection_params['host']}:{self.connection_params['port']}")

    def close(self):
        if self._pool:
            self._pool.closeall()

    @contextmanager
    def get_connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def execute(self, query: str, params: tuple = None):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def execute_returning(self, query: str, params: tuple = None):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def execute_batch(self, query: str, params_list: list[tuple], page_size: int = 100):
        """Batch insert using execute_values for efficiency."""
        from psycopg2.extras import execute_values
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, params_list, page_size=page_size)


# =============================================================================
# Schema & Seeding (unchanged from your original + minor additions)
# =============================================================================

INIT_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS telecom;

CREATE TABLE IF NOT EXISTS telecom.operator (
    operator_id SERIAL PRIMARY KEY,
    operator_code VARCHAR(10) UNIQUE NOT NULL,
    operator_name VARCHAR(100) NOT NULL,
    founded_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telecom.location (
    location_id SERIAL PRIMARY KEY,
    province VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    density VARCHAR(20) DEFAULT 'urban',
    UNIQUE(province, district)
);

CREATE TABLE IF NOT EXISTS telecom.base_station (
    station_id SERIAL PRIMARY KEY,
    station_code VARCHAR(50) UNIQUE NOT NULL,
    operator_id INTEGER,
    location_id INTEGER,
    technology VARCHAR(10) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    install_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telecom.configuration (
    config_id SERIAL PRIMARY KEY,
    station_id INTEGER,
    frequency_band VARCHAR(20),
    bandwidth_mhz INTEGER,
    max_power_dbm DECIMAL(5,2),
    antenna_height_m DECIMAL(5,2),
    azimuth_degrees INTEGER,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telecom.subscriber_traffic (
    traffic_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    imsi_hash VARCHAR(64),
    tmsi VARCHAR(20),
    ip_address INET,
    destination_ip INET,
    destination_port INTEGER,
    protocol VARCHAR(10),
    bytes_up BIGINT DEFAULT 0,
    bytes_down BIGINT DEFAULT 0,
    packets_up INTEGER DEFAULT 0,
    packets_down INTEGER DEFAULT 0,
    latency_ms DECIMAL(10,3),
    jitter_ms DECIMAL(10,3),
    packet_loss_pct DECIMAL(5,2),
    connection_duration_ms INTEGER,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    extracted_at TIMESTAMPTZ,
    batch_id VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_traffic_extraction
    ON telecom.subscriber_traffic(extracted_at, ingested_at)
    WHERE extracted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_traffic_station_time
    ON telecom.subscriber_traffic(station_id, event_time);

CREATE TABLE IF NOT EXISTS telecom.performance_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL,
    metric_time TIMESTAMPTZ NOT NULL,
    cpu_util_pct DECIMAL(5,2),
    memory_util_pct DECIMAL(5,2),
    disk_util_pct DECIMAL(5,2),
    temperature_c DECIMAL(5,2),
    active_connections INTEGER,
    throughput_mbps DECIMAL(10,3),
    uptime_seconds BIGINT,
    error_count INTEGER DEFAULT 0,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    extracted_at TIMESTAMPTZ,
    batch_id VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_metrics_extraction
    ON telecom.performance_metrics(extracted_at, ingested_at)
    WHERE extracted_at IS NULL;

CREATE TABLE IF NOT EXISTS telecom.station_events (
    event_id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) DEFAULT 'info',
    description TEXT,
    metadata JSONB,
    target_station_id INTEGER,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    extracted_at TIMESTAMPTZ,
    batch_id VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_events_extraction
    ON telecom.station_events(extracted_at, ingested_at)
    WHERE extracted_at IS NULL;
"""


def initialize_schema(db: DatabaseManager):
    print("Initializing schema...")
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(INIT_SCHEMA_SQL)
    print("✓ Schema initialized")


def seed_dimensions(db: DatabaseManager):
    print("Seeding dimensions...")
    for op in OPERATORS:
        db.execute("""
            INSERT INTO telecom.operator (operator_id, operator_code, operator_name)
            VALUES (%s, %s, %s) ON CONFLICT (operator_code) DO NOTHING
        """, (op["id"], op["code"], op["name"]))

    for loc in LOCATIONS:
        db.execute("""
            INSERT INTO telecom.location (location_id, province, district, region, density)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (province, district) DO NOTHING
        """, (loc["id"], loc["province"], loc["district"], loc["region"], loc["density"]))

    print("✓ Dimensions seeded")


# =============================================================================
# Station Factory
# =============================================================================

def create_stations(
    db: DatabaseManager, count: int, rng: random.Random
) -> list[StationRuntime]:
    """Create stations with assigned personalities and density-appropriate locations."""
    stations = []

    for i in range(count):
        operator = rng.choices(OPERATORS, weights=OPERATOR_WEIGHTS)[0]
        location = rng.choice(LOCATIONS)
        technology = rng.choices(TECH_NAMES, weights=TECH_WEIGHTS)[0]

        station_code = f"{operator['code']}-{location['district'][:3].upper()}-{i+1:04d}"

        # Deterministic coordinates near the location's province
        seed = int(hashlib.md5(station_code.encode()).hexdigest()[:8], 16)
        coord_rng = random.Random(seed)
        lat = 10.0 + coord_rng.uniform(-2, 12)   # Vietnam latitude range
        lon = 104.0 + coord_rng.uniform(-2, 5.5)  # Vietnam longitude range

        install_date = datetime.now() - timedelta(days=rng.randint(30, 365 * 5))

        result = db.execute_returning("""
            INSERT INTO telecom.base_station
                (station_code, operator_id, location_id, technology, latitude, longitude, install_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_code) DO UPDATE SET updated_at = NOW()
            RETURNING station_id
        """, (
            station_code, operator["id"], location["id"],
            technology, lat, lon, install_date.date(), "active",
        ))

        station_id = result[0][0]

        # Insert initial configuration
        freq_band = rng.choice(FREQUENCY_BANDS[technology])
        bandwidth = rng.choice([10, 15, 20, 100] if technology == "5G" else [5, 10, 15, 20])
        db.execute("""
            INSERT INTO telecom.configuration
                (station_id, frequency_band, bandwidth_mhz, max_power_dbm, antenna_height_m, azimuth_degrees)
            SELECT %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM telecom.configuration WHERE station_id = %s AND effective_to IS NULL
            )
        """, (
            station_id, freq_band, bandwidth,
            round(rng.uniform(40, 46), 2), round(rng.uniform(20, 50), 2),
            rng.choice([0, 120, 240]),
            station_id,
        ))

        personality = StationPersonality.from_seed(station_code)

        station = StationRuntime(
            station_id=station_id,
            station_code=station_code,
            operator=operator,
            location=location,
            technology=technology,
            latitude=lat,
            longitude=lon,
            install_date=install_date,
            personality=personality,
        )
        stations.append(station)
        print(f"  ✓ {station_code} (ID:{station_id}, {technology}, {location['density']}, hw={personality.hardware_quality:.2f})")

    # Assign neighbor relationships
    assign_neighbors(stations)
    print(f"✓ {len(stations)} stations created with neighbor assignments")

    return stations


# =============================================================================
# Insert Functions — batched for efficiency
# =============================================================================

def insert_traffic_batch(db: DatabaseManager, records: list[dict]):
    """Batch insert traffic records."""
    if not records:
        return
    params = [
        (r["station_id"], r["event_time"], r["imsi_hash"], r["tmsi"],
         r["ip_address"], r["destination_ip"], r["destination_port"],
         r["protocol"], r["bytes_up"], r["bytes_down"], r["packets_up"],
         r["packets_down"], r["latency_ms"], r["jitter_ms"],
         r["packet_loss_pct"], r["connection_duration_ms"])
        for r in records
    ]
    db.execute_batch("""
        INSERT INTO telecom.subscriber_traffic
            (station_id, event_time, imsi_hash, tmsi, ip_address, destination_ip,
             destination_port, protocol, bytes_up, bytes_down, packets_up, packets_down,
             latency_ms, jitter_ms, packet_loss_pct, connection_duration_ms)
        VALUES %s
    """, params)


def insert_metrics(db: DatabaseManager, metrics: dict):
    db.execute("""
        INSERT INTO telecom.performance_metrics
            (station_id, metric_time, cpu_util_pct, memory_util_pct, disk_util_pct,
             temperature_c, active_connections, throughput_mbps, uptime_seconds, error_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        metrics["station_id"], metrics["metric_time"], metrics["cpu_util_pct"],
        metrics["memory_util_pct"], metrics["disk_util_pct"], metrics["temperature_c"],
        metrics["active_connections"], metrics["throughput_mbps"],
        metrics["uptime_seconds"], metrics["error_count"],
    ))


def insert_event(db: DatabaseManager, event: dict):
    db.execute("""
        INSERT INTO telecom.station_events
            (station_id, event_time, event_type, severity, description, metadata, target_station_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        event["station_id"], event["event_time"], event["event_type"],
        event["severity"], event["description"], event["metadata"],
        event.get("target_station_id"),
    ))


def insert_config_change(db: DatabaseManager, station: StationRuntime, change_time: datetime, new_band: str = None):
    """SCD Type 2: close current config, open new one."""
    # Close current
    db.execute("""
        UPDATE telecom.configuration SET effective_to = %s
        WHERE station_id = %s AND effective_to IS NULL
    """, (change_time, station.station_id))

    # Open new
    freq_band = new_band or random.choice(FREQUENCY_BANDS[station.technology])
    db.execute("""
        INSERT INTO telecom.configuration
            (station_id, frequency_band, bandwidth_mhz, max_power_dbm, antenna_height_m, azimuth_degrees, effective_from)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        station.station_id, freq_band,
        random.choice([10, 15, 20, 100] if station.technology == "5G" else [5, 10, 15, 20]),
        round(random.uniform(40, 46), 2), round(random.uniform(20, 50), 2),
        random.choice([0, 120, 240]), change_time,
    ))


# =============================================================================
# Streaming Mode — feeds the live pipeline
# =============================================================================

class StationWorker(threading.Thread):
    """Worker thread for a group of stations (not 1:1 — saves thread count)."""

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
            self.stats[s.station_code] = {"traffic": 0, "metrics": 0, "events": 0, "errors": 0}

    def run(self):
        last_metric_time = time.time()
        last_mobility_time = time.time()
        metric_interval = 60
        mobility_interval = 300  # subscriber mobility every 5 min
        traffic_buffer = []

        while not self.stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                local_now = now.astimezone(TIMEZONE)
                time_mult = get_time_multiplier(local_now)

                for station in self.stations:
                    if station.status in (StationStatus.MAINTENANCE, StationStatus.DOWN, StationStatus.INACTIVE):
                        # Still check for incident/maintenance resolution
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
                        continue

                    # --- Traffic generation ---
                    active_subs = self.subscriber_pool.get_active_subscribers(
                        station, local_now.hour, time_mult, self.rng
                    )

                    # Generate traffic for a sample of active subscribers (not all, to control volume)
                    sample_size = max(1, int(len(active_subs) * time_mult * TRAFFIC_SAMPLE_RATE))
                    sampled = self.rng.sample(active_subs, min(sample_size, len(active_subs))) if active_subs else []

                    for imsi_hash in sampled:
                        traffic = generate_traffic_event(station, imsi_hash, now, self.rng)
                        traffic_buffer.append(traffic)
                        self.stats[station.station_code]["traffic"] += 1

                    # Flush traffic buffer when full
                    if len(traffic_buffer) >= self.traffic_batch_size:
                        insert_traffic_batch(self.db, traffic_buffer)
                        traffic_buffer.clear()

                    # --- Scenario checks ---
                    incident = self.scenario_engine.maybe_trigger_incident(station, now)
                    if incident:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, incident
                        ))
                        self.stats[station.station_code]["events"] += 1
                        # Cascade: move subscribers to neighbors
                        if station.status == StationStatus.DOWN:
                            self.scenario_engine.trigger_cascading_load(station, self.subscriber_pool)

                    resolved = self.scenario_engine.maybe_resolve_incident(station, now)
                    if resolved:
                        insert_event(self.db, generate_station_event(
                            station, self.all_stations, now, self.rng, resolved
                        ))
                        self.stats[station.station_code]["events"] += 1

                    # --- Organic events ---
                    event = generate_station_event(station, self.all_stations, now, self.rng)
                    if event:
                        insert_event(self.db, event)
                        self.stats[station.station_code]["events"] += 1

                # --- Periodic: metrics (every 60s) ---
                if time.time() - last_metric_time >= metric_interval:
                    for station in self.stations:
                        if station.status == StationStatus.INACTIVE:
                            continue
                        active_count = sum(
                            1 for s in self.subscriber_pool.subscribers.values()
                            if s["current_station"] == station.station_code
                        )
                        metrics = generate_performance_metrics(station, now, active_count, self.rng)
                        insert_metrics(self.db, metrics)
                        self.stats[station.station_code]["metrics"] += 1
                    last_metric_time = time.time()

                # --- Periodic: subscriber mobility (every 5 min) ---
                if time.time() - last_mobility_time >= mobility_interval:
                    self.subscriber_pool.simulate_mobility(self.all_stations, local_now.hour, self.rng)
                    last_mobility_time = time.time()

                # Flush any remaining traffic
                if traffic_buffer:
                    insert_traffic_batch(self.db, traffic_buffer)
                    traffic_buffer.clear()

                # Sleep with jitter
                time.sleep(self.rng.uniform(0.8, 1.2))

            except Exception as e:
                for s in self.stations:
                    self.stats[s.station_code]["errors"] += 1
                print(f"Error in worker: {e}")
                time.sleep(2)


# =============================================================================
# Backfill Mode — generate historical data for benchmarking
# =============================================================================

def run_backfill(
    db: DatabaseManager,
    stations: list[StationRuntime],
    subscriber_pool: SubscriberPool,
    scenario_engine: ScenarioEngine,
    start_date: date,
    num_days: int,
    rng: random.Random,
    events_per_station_per_hour: int = 20,
    maintenance_probability: float = 0.02,
):
    """
    Generate historical data hour-by-hour.
    More controlled than streaming — writes in hourly batches.
    """
    print(f"\n{'='*60}")
    print(f"Backfill: {start_date} → {start_date + timedelta(days=num_days)}")
    print(f"Stations: {len(stations)}, Events/station/hour: ~{events_per_station_per_hour}")
    est_traffic = len(stations) * events_per_station_per_hour * 24 * num_days
    print(f"Estimated traffic records: ~{est_traffic:,}")
    print(f"{'='*60}\n")

    total_traffic = 0
    total_metrics = 0
    total_events = 0

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        day_start = datetime.combine(current_date, datetime.min.time(), tzinfo=TIMEZONE)
        print(f"{current_date}: ", end="", flush=True)

        day_traffic = 0
        day_events = 0

        # --- Daily: maybe schedule maintenance for some stations ---
        for station in stations:
            if rng.random() < maintenance_probability and not station.maintenance_active:
                maint_hour = rng.randint(*MAINTENANCE_HOUR_RANGE)
                maint_start = day_start + timedelta(hours=maint_hour)
                maint_duration = rng.randint(*MAINTENANCE_DURATION_RANGE)
                upgrade = rng.random() < TECH_UPGRADE_PROB

                maint_events = scenario_engine.schedule_maintenance(
                    station, maint_start, maint_duration, upgrade
                )
                for me in maint_events:
                    ev = generate_station_event(station, stations, maint_start, rng, me)
                    insert_event(db, ev)
                    day_events += 1

                if upgrade:
                    insert_config_change(db, station, maint_start)

        # --- Hourly processing ---
        for hour in range(24):
            hour_start = day_start + timedelta(hours=hour)
            local_time = hour_start
            time_mult = get_time_multiplier(local_time)

            # Subscriber mobility at the start of each hour
            subscriber_pool.simulate_mobility(stations, hour, rng)

            traffic_buffer = []

            for station in stations:
                # Check maintenance/incident resolution
                maint_end = scenario_engine.maybe_end_maintenance(station, hour_start)
                if maint_end:
                    ev = generate_station_event(station, stations, hour_start, rng, maint_end)
                    insert_event(db, ev)
                    day_events += 1

                resolved = scenario_engine.maybe_resolve_incident(station, hour_start)
                if resolved:
                    ev = generate_station_event(station, stations, hour_start, rng, resolved)
                    insert_event(db, ev)
                    day_events += 1

                if station.status in (StationStatus.MAINTENANCE, StationStatus.DOWN, StationStatus.INACTIVE):
                    # Still generate metrics showing the station is down
                    metrics = generate_performance_metrics(station, hour_start, 0, rng)
                    insert_metrics(db, metrics)
                    total_metrics += 1
                    continue

                # --- Traffic for this station this hour ---
                active_subs = subscriber_pool.get_active_subscribers(station, hour, time_mult, rng)
                n_events = max(1, int(events_per_station_per_hour * time_mult))

                for j in range(n_events):
                    event_time = hour_start + timedelta(seconds=rng.randint(0, 3599))
                    imsi = rng.choice(active_subs) if active_subs else hashlib.sha256(
                        f"anon-{station.station_id}-{j}".encode()
                    ).hexdigest()

                    traffic = generate_traffic_event(station, imsi, event_time, rng)
                    traffic_buffer.append(traffic)

                # --- Maybe trigger incident ---
                # Higher probability in backfill since we check once per hour, not once per second
                hourly_incident_prob = INCIDENT_HOURLY_PROB * (2.0 - station.personality.hardware_quality)
                if rng.random() < hourly_incident_prob and not station.incident_active:
                    # Manually trigger since we're not calling per-second
                    incident_time = hour_start + timedelta(minutes=rng.randint(0, 59))
                    incident_type = rng.choices(["degradation", "failure"], weights=INCIDENT_TYPE_WEIGHTS)[0]

                    if incident_type == "degradation":
                        severity = rng.uniform(*DEGRADATION_SEVERITY_RANGE)
                        duration = rng.randint(*DEGRADATION_DURATION_RANGE)
                        station.status = StationStatus.DEGRADED
                    else:
                        severity = rng.uniform(*FAILURE_SEVERITY_RANGE)
                        duration = rng.randint(*FAILURE_DURATION_RANGE)
                        station.status = StationStatus.DOWN

                    station.incident_active = True
                    station.incident_type = incident_type
                    station.incident_start = incident_time
                    station.incident_severity = severity
                    station.incident_end = incident_time + timedelta(minutes=duration)
                    station.error_count += 1

                    ev_data = {
                        "event_type": EventType.INCIDENT_START.value,
                        "severity": Severity.CRITICAL.value if severity > CRITICAL_SEVERITY_THRESHOLD else Severity.ERROR.value,
                        "description": f"Incident: {incident_type} (severity={severity:.2f})",
                        "metadata": json.dumps({
                            "incident_type": incident_type, "severity": round(severity, 3),
                            "estimated_duration_min": duration,
                        }),
                    }
                    ev = generate_station_event(station, stations, incident_time, rng, ev_data)
                    insert_event(db, ev)
                    day_events += 1

                    if station.status == StationStatus.DOWN:
                        scenario_engine.trigger_cascading_load(station, subscriber_pool)

                # --- Organic events ---
                density = DENSITY_PROFILES[station.location.get("density", "urban")]
                n_organic = int(density["events_per_min"] * 60 * station.personality.alarm_tendency * time_mult)
                for _ in range(max(0, n_organic)):
                    evt_time = hour_start + timedelta(seconds=rng.randint(0, 3599))
                    ev = generate_station_event(station, stations, evt_time, rng)
                    if ev:
                        insert_event(db, ev)
                        day_events += 1

                # --- Metrics (once per hour per station) ---
                active_count = sum(
                    1 for s in subscriber_pool.subscribers.values()
                    if s["current_station"] == station.station_code
                )
                metrics = generate_performance_metrics(station, hour_start, active_count, rng)
                insert_metrics(db, metrics)
                total_metrics += 1

            # Flush traffic buffer for this hour
            if traffic_buffer:
                insert_traffic_batch(db, traffic_buffer)
                day_traffic += len(traffic_buffer)
                traffic_buffer.clear()

            # Progress indicator every 6 hours
            if (hour + 1) % 6 == 0:
                print(f"[h{hour+1}: {day_traffic:,}t]", end=" ", flush=True)

        total_traffic += day_traffic
        total_events += day_events
        active = sum(1 for s in stations if s.status == StationStatus.ACTIVE)
        degraded = sum(1 for s in stations if s.status == StationStatus.DEGRADED)
        down = sum(1 for s in stations if s.status == StationStatus.DOWN)
        maint = sum(1 for s in stations if s.status == StationStatus.MAINTENANCE)
        print(f"→ {day_traffic:,} traffic, {day_events} events | "
              f"active={active} degraded={degraded} down={down} maint={maint}")

    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Traffic records: {total_traffic:,}")
    print(f"  Metric records:  {total_metrics:,}")
    print(f"  Event records:   {total_events:,}")
    print(f"{'='*60}")


# =============================================================================
# Stats Printing
# =============================================================================

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
    print(f"{'='*60}")

    # Show top 5 stations by traffic
    sorted_stats = sorted(stats.items(), key=lambda x: x[1]["traffic"], reverse=True)[:5]
    for code, s in sorted_stats:
        print(f"  {code}: traffic={s['traffic']:,}, metrics={s['metrics']}, events={s['events']}, errors={s['errors']}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Telecom Station Simulator v4")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Shared DB args ---
    def add_db_args(p):
        p.add_argument("--host", default="localhost")
        p.add_argument("--port", type=int, default=5433)
        p.add_argument("--db", default="station_oltp")
        p.add_argument("--user", default="station")
        p.add_argument("--password", default="station")
        p.add_argument("--stations", type=int, default=20)
        p.add_argument("--subscribers", type=int, default=2000, help="Subscriber pool size")
        p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")

    # Stream mode
    sp = subparsers.add_parser("stream", help="Continuous streaming mode for live pipeline testing")
    add_db_args(sp)
    sp.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    sp.add_argument("--stats-interval", type=int, default=30)
    sp.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    sp.add_argument("--batch-size", type=int, default=50, help="Traffic insert batch size")

    # Backfill mode
    bp = subparsers.add_parser("backfill", help="Generate historical data for benchmarking")
    add_db_args(bp)
    bp.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    bp.add_argument("--days", type=int, default=7)
    bp.add_argument("--events-per-hour", type=int, default=20, help="Traffic events per station per hour")
    bp.add_argument("--maintenance-prob", type=float, default=MAINTENANCE_DAILY_PROB, help="Daily maintenance probability per station")

    args = parser.parse_args()

    # Deterministic seeding
    rng = random.Random(args.seed)
    random.seed(args.seed)

    # Setup database
    db = DatabaseManager(args.host, args.port, args.db, args.user, args.password)

    try:
        db.initialize()
        initialize_schema(db)
        seed_dimensions(db)

        print(f"\nCreating {args.stations} stations (seed={args.seed})...")
        stations = create_stations(db, args.stations, rng)

        print(f"Creating subscriber pool ({args.subscribers} subscribers)...")
        subscriber_pool = SubscriberPool(args.subscribers, stations, rng)
        print(f"✓ {len(subscriber_pool.subscribers)} subscribers distributed across stations")

        scenario_engine = ScenarioEngine(stations, rng)

        if args.command == "stream":
            # Split stations across workers
            stop_event = threading.Event()
            stats = {}
            workers = []
            n_workers = min(args.workers, len(stations))
            chunk_size = max(1, len(stations) // n_workers)

            for i in range(n_workers):
                chunk = stations[i * chunk_size: (i + 1) * chunk_size]
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

            def signal_handler(sig, frame):
                print("\n\nShutting down...")
                stop_event.set()

            signal.signal(signal.SIGINT, signal_handler)

            print(f"\n🚀 Streaming with {n_workers} workers, {len(stations)} stations. Ctrl+C to stop.\n")
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
            print_stats(stats, start_time)
            print("\n✓ Simulation complete!")

        elif args.command == "backfill":
            start_date = date.fromisoformat(args.start)
            run_backfill(
                db=db,
                stations=stations,
                subscriber_pool=subscriber_pool,
                scenario_engine=scenario_engine,
                start_date=start_date,
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
