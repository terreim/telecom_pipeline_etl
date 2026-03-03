# DEVELOPMENT.md — Refactoring Progress

**Date:** 2026-03-02  
**Branch:** `refactor_dev`

---

## 1. What Was Completed Today

### 1.1 Simulator Time Model Fix (`telecom_simulator_v5.py`)

The simulator's clock model was semantically wrong. `created_at` was being computed
as `event_time + propagation + offset`, which pushed it up to **19 minutes ahead**
of the ETL's `NOW()`. In reality, `created_at` is just the client's wall clock at
INSERT time — it should only be ~60 seconds ahead of ETL (the clock offset between
the two servers).

**Changes made:**

| Item | Status |
|---|---|
| Added `ETL_CLIENT_JITTER_RANGE = (0, 5)` constant | ✅ Done |
| Rewrote `CLIENT_CLOCK_OFFSET_SECONDS` comment block with full time model docs | ✅ Done |
| Added `_compute_times()` helper — central function for `(event_time, created_at)` | ✅ Done |
| Rewrote `insert_traffic_batch()` — uses `_compute_times()`, accepts `is_backfill` | ✅ Done |
| Rewrote `insert_metrics()` — uses `_compute_times()`, accepts `is_backfill`, `rng` | ✅ Done |
| Rewrote `insert_event()` — uses `_compute_times()`, accepts `is_backfill`, `rng` | ✅ Done |
| Added `_bf_insert_event`, `_bf_insert_metrics`, `_bf_insert_traffic` wrappers in `run_backfill()` | ✅ Done |
| Replaced all 16 `insert_event(db, ...)` calls in backfill → `_bf_insert_event(db, ...)` | ✅ Done |
| Replaced 2 `insert_metrics(db, ...)` calls in backfill → `_bf_insert_metrics(db, ...)` | ✅ Done |
| Replaced 1 `insert_traffic_batch(db, ...)` call in backfill → `_bf_insert_traffic(db, ...)` | ✅ Done |
| Streaming callers unchanged (defaults `is_backfill=False`) | ✅ Verified |
| `py_compile` passes | ✅ Verified |
| Backfill test run (`--days 1 --events-per-hour 823`) | ✅ Passed |

**Corrected time model:**

```
Streaming:
  client_now  = ETL_NOW + offset (60s) - jitter (0-5s)
  created_at  = client_now                              ← always ~55-60s ahead of ETL
  event_time  = client_now - propagation [- late_extra]  ← pushed into past

Backfill:
  event_time  = synthetic past time (given)
  created_at  = event_time + propagation + offset       ← forward computation
```

**Impact on ETL buffer:** was 240s, can now be reduced to ~90s (offset + margin).

### 1.2 `/common` Module Implementation

| File | Status | Notes |
|---|---|---|
| `config.py` | ✅ Done | Frozen dataclass, `from_env()`, `from_airflow_vars()` stub |
| `connections.py` | ✅ Done | `get_s3_hook()`, `get_s3_credentials()`, `pg_cursor()` context manager |
| `dag_defaults.py` | ✅ Done | `_BASE`, `BRONZE_DEFAULTS`, `SILVER_DEFAULTS`, `GOLD_DEFAULTS`, `RECOVERY_DEFAULTS` |
| `assets.py` | ✅ Done | `build_assets(cfg, option)` function returning dicts by layer |
| `validators.py` | ✅ Done | Full cross-layer validation (see below) |
| `metadata.py` | 🔧 Partial | `MetadataManager` with `read_metadata()` / `write_metadata()`, schema defined in docstring |
| `watermark.py` | 🔧 Partial | `S3WatermarkStore` thin wrapper, needs `json.loads` on read |
| `sql_builder.py` | 🔧 Partial | `sql_bronze_extractor()` and `sql_check_new_data()` templates written, more to add |
| `dag_factory.py` | ❌ Empty | Not started |
| `__init__.py` | ❌ Empty | Not started |

### 1.3 `validators.py` — Full Implementation

- **Domain constants:** `VALID_EVENT_TYPES` (all 24 v5 types), `VALID_SEVERITIES`, `VALID_PROTOCOLS`, `VALID_TECHNOLOGIES`, `VALID_FREQUENCY_BANDS`, `VALID_STATION_STATUSES`, `VALID_DENSITIES`
- **Bronze:** `BronzeSchema` (frozen dataclass) + `check_bronze_schema(df, table)` — subset-based column check
- **Silver:** `QualityProfile` (frozen dataclass with configurable thresholds) + `validate_traffic()`, `validate_metrics()`, `validate_events()` — these are drop-in replacements for `silver_transformer._validate_*`
- **Gold:** `check_gold_health_hourly()`, `check_gold_sla_compliance()`, `check_gold_anomaly_features()` — return `list[str]` warnings
- **Dimensions:** `validate_station_dimension()` — technology/status domain, duplicate codes, Vietnam bounding box
- **Profiles:** `DEFAULT_PROFILE` and `STRICT_PROFILE` provided; configurable per environment

---

## 2. Open Design Decisions

### 2.1 `wait_for_data` Sensor and `SELECT 1` Query

**Current state:** `bronze_low_volume` DAG has a `wait_for_data` sensor task that
runs a `SELECT 1 WHERE EXISTS (... WHERE extracted_at IS NULL ...)` query. This
couples to the old `extracted_at` / `batch_id` write-back pattern on the client DB.

**Problem:** The refactoring goal is to **stop writing to the client's PostgreSQL**.
The new extraction model is:

```sql
-- Old (write-back):
UPDATE ... SET extracted_at = NOW(), batch_id = %s ... RETURNING ...

-- New (read-only, watermark-based):
SELECT ... WHERE updated_at > %s AND updated_at <= %s
```

With the watermark model, there's no `extracted_at IS NULL` to check. The sensor
needs rethinking:

**Options:**
1. **Remove the sensor entirely.** The DAG runs on a `*/5 * * * *` cron. If the
   watermark query returns 0 rows, the task just does nothing and succeeds. The
   sensor adds unnecessary Airflow worker slots when poking.
2. **Replace with a watermark-based check.** `SELECT 1 WHERE EXISTS (SELECT 1 FROM ... WHERE updated_at > %s)` where `%s` is the last watermark. This still hits the client DB but is read-only.
3. **Replace with an S3-based check.** Check if new bronze files have landed since
   the last silver processing. No client DB hit at all, but adds S3 API latency.

**Recommendation:** Option 1 (remove sensor). The 5-minute cron is already a
natural debounce. The extractor itself should handle the "no new data" case
gracefully by returning early.

### 2.2 `sql_builder.py` — `sql_check_new_data()` Still References `extracted_at`

The partly-written `sql_check_new_data()` in `common/sql_builder.py` still has
`WHERE extracted_at IS NULL`. This needs to be rewritten once the sensor decision
(§2.1) is resolved.

### 2.3 Metadata Construction Location

**Decision made:** The `metadata_dict` is constructed in `/util` (bronze_extractor,
silver_transformer, etc.) because each utility knows its own record counts,
watermark boundaries, processing duration, and data keys. It then calls
`MetadataManager.write_metadata()` to persist to S3.

### 2.4 `buffer_seconds` Value

Now that `created_at` is only ~60s ahead of ETL (not 19 min), the buffer can be
reduced from 240s to **~90s** (offset + margin). The `config.py` default is still
`buffer_seconds=240` — update once the simulator fix is validated in a full
multi-day run.

---

## 3. What Needs To Be Done

### 3.1 High Priority — Bronze Extractor Rewrite

The core blocker. Everything downstream depends on this.

- [ ] **Rewrite `bronze_extractor.el()`** to use watermark-based `SELECT` instead of
  `UPDATE...SET extracted_at`. Remove `FOR UPDATE SKIP LOCKED`, `extracted_at`,
  `batch_id` write-back. Accept `(watermark_from, watermark_to)` parameters.
- [ ] **Update `bronze_lake.py` DAGs** to pass watermark boundaries from config/metadata
  instead of `cutoff_time`. Remove `extracted_at`-dependent sensor.
- [ ] **Write `_metadata.json` in bronze_extractor** — after successful S3 upload,
  construct the metadata dict and call `MetadataManager.write_metadata()`.

### 3.2 High Priority — Finish `/common` Modules

- [ ] **`metadata.py`** — Add `json.loads()` in `read_metadata()` to return a dict. <= DONE
  Fix import paths (`from common.config import ...` not `from config import ...`).  <= DONE
- [ ] **`watermark.py`** — Add `json.loads()` on read. Wire watermark path to
  `metadata/watermarks/{table_name}.json`. Consider merging into MetadataManager
  or keeping as a thin convenience wrapper.                                         <= DONE, NOT MERGED
- [ ] **`sql_builder.py`** — Add remaining templates: silver staging INSERT,
  gold aggregation queries, recovery/catchup queries.                               <= MAYBE DONE
- [ ] **`dag_factory.py`** — Factory functions per layer: `create_bronze_dag()`,
  `create_silver_dag()`, etc. Reduce boilerplate in DAG files.
- [ ] **`__init__.py`** — Expose key public API for clean imports.

### 3.3 Medium Priority — Wire `/common` Into Existing Code

- [ ] **`silver_transformer.py`** — Replace inline `_validate_traffic()`,
  `_validate_metrics()`, `_validate_events()` with calls to
  `from common.validators import validate_traffic, validate_metrics, validate_events`.
  Update the hardcoded `valid_event_types` set (only 10 types → 24 types now in
  `validators.py`).
- [ ] **`silver_transformer.py`** — Replace direct S3Hook/credentials creation with
  `common.connections` helpers.
- [ ] **`gold_aggregator.py`** — Dedup S3 credential lookups, fix hardcoded `'telecom'`
  in `dictGet`, use `common.connections` and `common.config`.
- [ ] **All DAGs** — Replace inline `default_args` dicts with imports from
  `common.dag_defaults`. Replace inline `Asset(...)` declarations with imports
  from `common.assets`.

### 3.4 Medium Priority — Recovery / Catchup DAG

- [ ] **`dags/recovery.py`** — Implement automated catchup for the ~1% of records
  that fall outside the buffer window (late arrivals that the watermark overlap
  didn't catch). On a longer schedule (hourly or daily), re-scan a wider window
  and reconcile.

### 3.5 Lower Priority — Validation in Production

- [ ] **Gold sanity checks** — Wire `check_gold_health_hourly()` etc. into
  gold DAG as post-aggregation audit tasks.
- [ ] **Dimension pre-flight** — Call `validate_station_dimension()` before
  silver enrichment to catch bad dimension data early.
- [ ] **`STRICT_PROFILE`** — Set up an Airflow Variable or env var to toggle
  between `DEFAULT_PROFILE` and `STRICT_PROFILE` per environment.

### 3.6 Lower Priority — Cleanup

- [ ] Remove `extracted_at`, `batch_id`, `ingested_at` columns from PostgreSQL DDL
  (they're simulator/MVP artifacts that won't exist on a real client DB).
- [ ] Update `telecom_simulator_v5.py` — remove the `extracted_at`/`batch_id`
  columns from `CREATE TABLE` statements and INSERT queries.
- [ ] Update `buffer_seconds` default in config.py from 240 → 90 after validation.
- [ ] Add docstrings to `common/__init__.py` listing the public API.

---

## 4. Architecture Reference

```
Client PostgreSQL (read-only)
  │
  │  SELECT ... WHERE updated_at > (prev_watermark - overlap)
  │                   AND updated_at <= (NOW() - buffer)
  │
  ▼
Bronze (S3/MinIO Parquet)  ──→  _metadata.json (watermarks, record counts, delays)
  │
  │  Asset trigger
  ▼
Silver (S3/MinIO Parquet)  ──→  Validated, enriched, partitioned
  │
  │  Asset trigger
  ▼
Staging (ClickHouse)  ──→  MergeTree tables, dedup via ReplacingMergeTree
  │
  │  Asset trigger
  ▼
Gold (ClickHouse)  ──→  Aggregated materialized views / tables
```

**Three clocks:**
| Clock | Owner | Value |
|---|---|---|
| `event_time` | Station | When the telecom event actually occurred |
| `created_at` | Client PostgreSQL | `client_NOW()` ≈ `ETL_NOW + 60s` |
| `NOW()` | ETL / Airflow | The ETL server's wall clock |

**Watermark extraction window:**
```
FROM = previous_watermark − overlap_seconds (120s)
TO   = NOW() − buffer_seconds (90s)
```
