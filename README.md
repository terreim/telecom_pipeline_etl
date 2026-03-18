# Telecom Station Data Pipeline

Pipeline xử lý dữ liệu viễn thông theo kiến trúc **Medallion** (Bronze → Silver → Gold), sử dụng
Apache Airflow 3.x để điều phối, MinIO (S3) làm data lake, và ClickHouse làm analytics warehouse.

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Simulator](#2-simulator)
3. [Mô hình thời gian & CDC](#3-mô-hình-thời-gian--cdc)
4. [Cấu trúc DAG](#4-cấu-trúc-dag)
5. [Module `shared/common`](#5-module-sharedcommon)
6. [Module `shared/util`](#6-module-sharedutil)
7. [DDL & Schema](#7-ddl--schema)
8. [Hạ tầng Docker](#8-hạ-tầng-docker)
9. [Cấu trúc thư mục](#9-cấu-trúc-thư-mục)
10. [Vấn đề đã biết & TODO](#10-vấn-đề-đã-biết--todo)

---

## 1. Tổng quan kiến trúc

```
PostgreSQL (OLTP nguồn)
  │
  │  Watermark-based CDC (chỉ đọc, không ghi ngược)
  ▼
Bronze (S3 Parquet — dữ liệu thô)
  │
  │  Asset trigger theo ranh giới giờ
  ▼
Silver (S3 Parquet — đã validate & làm giàu)
  │
  │  Asset trigger
  ▼
Staging (ClickHouse MergeTree — denormalized, TTL 90 ngày)
  │
  │  Asset trigger, dynamic task mapping + Pool
  ▼
Gold (ClickHouse — 8 báo cáo phân tích: health, SLA, anomaly, outage, ...)
```

**Ba đồng hồ trong hệ thống:**

| Đồng hồ | Chủ sở hữu | Ý nghĩa |
|---|---|---|
| `event_time` | Trạm (station) | Thời điểm sự kiện xảy ra trên thiết bị |
| `created_at` | Client PostgreSQL | `client_NOW()` ≈ `ETL_NOW + 60s` (clock offset) |
| `updated_at` | Client PostgreSQL | Lần sửa cuối cùng (corrections, enrichment) |

---

## 2. Simulator

File: `telecom_simulator_v5.py` (~174 KB)

### Dữ liệu sinh ra

Simulator tạo dữ liệu viễn thông thực tế trên PostgreSQL schema `telecom`:

- **`subscriber_traffic`** — bảng fact dung lượng cao (CDR-like): traffic_id, station_id, event_time, imsi_hash, bytes_up/down, latency_ms, jitter_ms, packet_loss_pct, ...
- **`performance_metrics`** — metrics hạ tầng: cpu_usage_pct, memory_usage_pct, temperature_celsius, uplink/downlink_throughput_mbps, active_subscribers, ...
- **`station_events`** — sự kiện vận hành: event_type (24 loại), severity, description, metadata (JSONB)
- **`base_station`, `operator`, `location`, `configuration`** — dimension tables (SCD Type-2)

### Kịch bản mô phỏng

| Kịch bản | Mô tả |
|---|---|
| Station lifecycle | PLANNED → PROVISIONING → TESTING → ACTIVE → DEGRADED/DOWN → RETIRED |
| Cluster failure | Power outage, fiber cut, cooling failure ảnh hưởng nhiều trạm cùng vị trí |
| Weather events | Mưa/bão làm suy giảm tín hiệu theo vùng |
| Mass gathering | Concert, thể thao gây traffic surge |
| Firmware rollout | Firmware mới gây reboot ngẫu nhiên trên subset trạm |
| Late arrivals | Record đến trễ (created_at bị delay thêm) |
| Soft deletes | GDPR erasure, operator decommission (`is_deleted = true`) |
| Duplicates | Insert 2 lần cùng record |
| Row updates | CDR reconciliation, event enrichment, metric recalibration |

### Mô hình độ trễ (station → client → ETL)

```
Streaming (live):
  client_now   = ETL_NOW + 60s (clock offset) - 0~5s (jitter)
  created_at   = client_now
  event_time   = client_now - propagation_delay (1~180s tùy density)

Backfill (historical):
  event_time   = thời điểm quá khứ (given)
  created_at   = event_time + propagation_delay + offset
  Late arrival: created_at += late_extra_delay
```

**Density profiles:** Urban Core (8x traffic, 400 subs), Urban (4x, 200), Suburban (2x, 80), Rural (0.5x, 20)

**Technology:** 2G (1%), 3G (7%), 4G (40%), 5G (50%)

**Vùng địa lý:** 25 địa điểm Việt Nam, 6 nhà mạng (Viettel 45%, Vinaphone 20%, MobiFone 18%, FPT 5%, iTelecom 4%, Vietnamobile 8%)

### Sử dụng

```bash
# Streaming liên tục
python telecom_simulator_v5.py stream --stations 30 --duration 3600

# Backfill lịch sử
python telecom_simulator_v5.py backfill --stations 50 --start 2026-01-01 --days 7

# Tùy chỉnh xác suất sự cố
python telecom_simulator_v5.py stream --incident-prob 0.10 --late-arrival-prob 0.05
```

---

## 3. Mô hình thời gian & CDC

### Watermark-based CDC

Pipeline sử dụng CDC dựa trên watermark mà **không ghi ngược** vào PostgreSQL nguồn.

```sql
-- Truy vấn trích xuất (chỉ đọc)
SELECT ... WHERE updated_at > %(from_wm)s AND updated_at <= %(to_wm)s
```

**Lifecycle mỗi lần chạy:**

1. **Lần đầu:** Tìm `min(updated_at)` → khởi tạo watermark → tiến dần theo `max_window_seconds`
2. **Lần thường:** `from = prev_watermark - overlap_seconds`, `to = NOW() - buffer_seconds`
3. **Khoảng trống:** Nhảy tới `updated_at` tiếp theo nếu không có data trong window

**Tham số CDC (config.py):**

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `buffer_seconds` | 240s | Lag phía sau `NOW()` để tránh record đang in-flight |
| `overlap_seconds` | 24s | Chồng lấn với window trước để bắt late arrivals |
| `max_window_per_run` | 100 | Giới hạn vòng lặp/DAG run (tránh chạy vô hạn) |

### Delay metrics thu thập

Bronze extractor tích lũy các chỉ số delay mỗi batch:

- `max_propagation_seconds` = max(created_at - event_time) — độ trễ station → client
- `max_clock_offset_seconds` = max(updated_at - created_at) — clock skew client vs ETL
- `p99_propagation_seconds`, `p99_clock_offset_seconds` — percentile 99
- `late_arrival_count` — số record đến trễ hơn buffer
- `update_count` — số record có update (updated_at > created_at + threshold)

### Partitioning theo event_time

Bronze extractor phân vùng dữ liệu theo giờ của `event_time` (không phải `updated_at`).
Điều này đảm bảo record late-arriving vẫn nằm đúng partition lịch sử.

---

## 4. Cấu trúc DAG

### 4.1 `bronze_lake.py` — 2 DAG

| DAG | Schedule | Bảng | Đặc điểm |
|---|---|---|---|
| `bronze_high_volume` | `*/1 * * * *` | `subscriber_traffic` | Dung lượng cao, 1 phút/lần |
| `bronze_low_volume` | `*/5 * * * *` | `station_events`, `performance_metrics` | Dung lượng thấp, 5 phút/lần |

Mỗi DAG gửi signal (Asset outlet) khi vượt ranh giới giờ → đánh thức Silver.

### 4.2 `silver_lake.py` — 1 DAG

**`silver_lake`** — triggered bởi Bronze signals.

3 task song song:
- `clean_traffic()` → validate, enrich với dimension, ghi Silver parquet
- `clean_events()` → tương tự
- `clean_metrics()` → tương tự

**Data quality:**
- Soft failures (range violation) → đánh dấu `quality_issues`, vẫn đi tiếp
- Hard failures (null PK, station_id, event_time) → quarantine riêng
- Quarantine records có thể recover tại staging nếu chỉ có soft failures

### 4.3 `staging.py` — 1 DAG, dynamic task mapping

**`staging_silver`** — triggered bởi Silver signals.

```
ensure_db → ensure_tables → staging_dim
                                ↓
              ┌─────────────────┼─────────────────┐
         list_traffic      list_events       list_metrics
              ↓                 ↓                  ↓
    load_traffic.expand()  load_events.expand()  load_metrics.expand()
       (pool=staging_load)    (pool=staging_load)   (pool=staging_load)
              ↓                 ↓                  ↓
       collect_traffic     collect_events     collect_metrics
       (ALL_DONE, 1 signal)                   (ALL_DONE, 1 signal)
```

- **Dynamic mapping:** Mỗi batch Silver → 1 task riêng (tránh OOM khi concat tất cả)
- **Pool `staging_load`:** Giới hạn concurrent CH inserts (khuyến nghị 3-5 slots)
- **`collect_signal`:** `trigger_rule=ALL_DONE` — tổng hợp hours từ tất cả mapped tasks, gửi **1 Metadata signal** duy nhất → Gold
- **Chống mất signal:** Nếu 1 task OOM, collect vẫn chạy và gửi signal cho các hours thành công

### 4.4 `gold_lake.py` — 7 DAG

**Dependency graph:**
```
 bronze_high_volume ─── traffic_subscriber ──┐
                      ┌ performance_metrics ─┼──┐ 
 bronze_low_volume ───┴── station_events ────┘  │
    ┌───── (fires on hour boundary only) ───────┘ 
    ▼
 silver_lake ┬── traffic_cleaned ─┐
             ├── metrics_cleaned ─┼──┐
             └── events_cleaned ──┘  │
    ┌────────────────────────────────┘
    ▼                
 staging_silver ┬── staging_traffic ──┐
                ├── staging_metrics ──┤──> health_hourly ─────> anomaly_features
                └── staging_events ───┘        │
                                               │ (fires on hour 23 only)
                                               ▼
                                        health_daily_ready
                            ┌─────────┬────────┼─────────┬──────────┐
                            ▼         ▼        ▼         ▼          ▼
                           SLA      outage    maint     alarm    handover
                            │
                            ▼
                        region_daily
```

**8 báo cáo Gold:**

| Báo cáo | Granularity | Mô tả |
|---|---|---|
| `health_hourly` | Hourly | Health score tổng hợp (latency 25%, packet_loss 20%, cpu 15%, temp 10%, alarm 15%, throughput 15%) |
| `anomaly_features` | Hourly | Z-score 7 ngày rolling, phát hiện anomaly (z > 3) |
| `sla_compliance` | Daily | Uptime %, SLA breach hours, compliance status |
| `outage_report` | Daily | Incident start/end pairing, duration, affected subscribers |
| `region_daily` | Daily | Tổng hợp theo vùng/tỉnh/nhà mạng/công nghệ |
| `maintenance_report` | Daily | Planned vs actual duration, overrun, pre/post health |
| `handover_daily` | Daily | Source→target handover, unique subscribers, latency |
| `alarm_daily` | Daily | Alarm count theo severity, alarm rate, top description |

### 4.5 `recovery.py` — 3 DAG (manual trigger)

| DAG | Chức năng |
|---|---|
| `silver_recovery` | Reset `loaded_to_silver` trên Bronze metadata, tùy chọn xóa Silver parquet |
| `staging_recovery` | Reset `loaded_to_warehouse` trên Silver metadata, force re-load CH |
| `gold_recovery` | Xóa rows ClickHouse theo hour/day, re-aggregate |

Tất cả có HITL approval gate, trigger bằng tay với params `lookback_hours`, `table`, `mode`.

---

## 5. Module `shared/common`

| File | Chức năng |
|---|---|
| `config.py` | `PipelineConfig` frozen dataclass — tất cả connection IDs, bucket, prefix, table names, tham số CDC |
| `connections.py` | Helper lấy S3Hook, ClickHouseHook, PostgreSQL cursor |
| `schema.py` | Unify Decimal128 precision, serialize JSONB → JSON string cho Parquet |
| `sql_builder.py` | SQL templates tham số hóa: bronze extraction, dimension, staging insert, gold aggregation |
| `validators.py` | Validate cross-layer: Bronze schema check, Silver range check + quality profile, Gold sanity, Dimension domain |
| `watermark.py` | `S3WatermarkStore` — đọc/ghi watermark JSON trên S3 |
| `metadata.py` | `MetadataManager` — đọc/ghi/scan metadata JSON, đánh dấu loaded |
| `metadata_template.py` | Template dict cho mỗi layer (bronze, silver, staging) |
| `dag_defaults.py` | Default args per layer: retry, timeout, email |
| `dag_factory.py` | Factory classes: `DagFactory`, `BronzeDag`, `SilverDag`, `StagingSilverDag`, `GoldDag`, `RecoveryDag`, `SchemaManager` mixin |
| `assets.py` | `build_assets()` — tạo Asset dict cho inter-DAG signaling |
| `s3.py` | `S3IO` wrapper: read/write parquet, list keys, delete |
| `ch.py` | `ClickHouseIO` wrapper: execute query, insert with S3 fallback |
| `failure_recovery.py` | Error handling utilities |

---

## 6. Module `shared/util`

| File | Chức năng |
|---|---|
| `bronze_extractor.py` | `BronzeExtractor` — watermark-based CDC, per-hour partitioning, delay accumulation |
| `silver_transformer.py` | `SilverTransformer` — validate, enrich với dim_station, split valid/quarantine, ghi Silver parquet |
| `staging_loader.py` | `ClickHouseLoader` — list unprocessed, load single batch, ensure DB/tables, drop tables, load dimension |
| `gold_aggregator.py` | `GoldAggregator` — 8 report definitions, health score computation, anomaly Z-score, idempotent insert |
| `recovery_manager.py` | `SilverRecovery`, `StagingRecovery`, `GoldRecovery` — reset metadata, delete CH rows, re-aggregate |
| `reconciliation.py` | Data reconciliation helpers |

---

## 7. DDL & Schema

### Staging (`staging_silver.sql`)

3 bảng MergeTree, partitioned by month, TTL 90 ngày:
- `staging_traffic_cleaned` (42 cột) — bao gồm enriched: bytes_total, is_valid, quality_issues, station_code, operator_code, province, region, density_class, technology, dim_match_status, is_deleted
- `staging_metrics_cleaned` (30 cột)
- `staging_events_cleaned` (22 cột)

### Gold (`gold_logic.sql`)

- `gold_health_hourly` — MergeTree, TTL 180 ngày
- `health_daily` + `mv_health_daily` — AggregatingMergeTree + Materialized View (avgState, minState, maxState)
- `gold_sla_compliance` — MergeTree, TTL 365 ngày
- `gold_anomaly_features`, `gold_outage_report`, `gold_region_daily`, `gold_maintenance_report`, `gold_handover_daily`, `gold_alarm_daily`

### Raw aggregation (`warehouse_raw.sql`)

- `traffic_hourly` + `mv_traffic_hourly` — AggregatingMergeTree real-time rollup
- `traffic_daily` + `mv_traffic_daily`
- `operator_daily` + `mv_operator_daily`

---

## 8. Hạ tầng Docker

File: `docker-compose.yaml`

| Service | Port | Vai trò |
|---|---|---|
| `airflow-webserver` | 8080 | UI quản lý DAG |
| `airflow-scheduler` | — | Lập lịch DAG |
| `airflow-api-server` | — | REST API |
| `airflow-worker` | — | CeleryExecutor worker |
| `redis` | 6379 | Celery broker |
| `postgres-oltp` | 5433 | PostgreSQL nguồn + Airflow metadata |
| `minio` | 9000 | S3-compatible data lake |
| `clickhouse` | 9000/8123 | Analytics warehouse |
| `flower` | 5555 | Celery monitoring |

**Executor:** CeleryExecutor với Redis broker.

**Pool khuyến nghị:** `staging_load` = 3-5 slots (Admin → Pools).

---

## 9. Cấu trúc thư mục

```
telecom_pipeline_etl/
├── docker-compose.yaml
├── Dockerfile
├── requirement.txt
├── .env
├── telecom_simulator_v5.py          # Simulator (~174 KB)
├── config/
│   ├── airflow.cfg
│   └── clickhouse_init.sql
├── dags/
│   ├── bronze_lake.py                # 2 DAG: high_volume, low_volume
│   ├── silver_lake.py                # 1 DAG: silver_lake
│   ├── staging.py                    # 1 DAG: staging_silver (dynamic mapping)
│   ├── gold_lake.py                  # 7 DAG: health, SLA, anomaly, outage, ...
│   ├── recovery.py                   # 3 DAG: silver/staging/gold recovery
│   └── shared/
│       ├── common/                   # Infrastructure & config
│       │   ├── config.py
│       │   ├── connections.py
│       │   ├── schema.py
│       │   ├── sql_builder.py
│       │   ├── validators.py
│       │   ├── watermark.py
│       │   ├── metadata.py
│       │   ├── metadata_template.py
│       │   ├── dag_defaults.py
│       │   ├── dag_factory.py
│       │   ├── assets.py
│       │   ├── s3.py
│       │   ├── ch.py
│       │   └── failure_recovery.py
│       ├── util/                     # Business logic
│       │   ├── bronze_extractor.py
│       │   ├── silver_transformer.py
│       │   ├── staging_loader.py
│       │   ├── gold_aggregator.py
│       │   ├── recovery_manager.py
│       │   └── reconciliation.py
│       └── ddl/                      # ClickHouse DDL
│           ├── staging_silver.sql
│           ├── gold_logic.sql
│           └── warehouse_raw.sql
├── init-scripts/
│   └── 01-create-airflow-db.sql
├── logs/
└── plugins/
```

---

## 10. Vấn đề đã biết & TODO

### Vấn đề timezone (ưu tiên thấp, đã documented)

Simulator sinh dữ liệu theo giờ ICT (UTC+7), nhưng:
- **Bronze landing:** Dữ liệu ngày 10 giờ 00:00 ICT được lưu vào partition ngày 9 giờ 17:00 UTC (do trừ 7 giờ). Partition sai ngày nhưng không mất data.
- **Silver → Staging:** `_normalize_event_time` giữ naïve ICT wall-clock. Khi ClickHouse đọc parquet qua `s3()`, nó diễn giải timestamp naïve là UTC → `toDate(event_time)` ở timezone ICT bị lệch 7 giờ so với silver.
- **Khuyến nghị:** Bỏ `DateTime64(3, 'Asia/Ho_Chi_Minh')` trong DDL, thay bằng `DateTime64(3)` (không timezone), hoặc convert về UTC trước khi insert.

### Schema evolution (ưu tiên trung bình)

Pipeline hiện rất yếu trước thay đổi schema:
- `sql_builder.py` hardcode column list — thêm/bớt cột phải sửa nhiều nơi
- DDL và code phải đồng bộ thủ công
- Không có migration framework

**TODO:**
- [ ] Schema registry hoặc auto-detect columns từ parquet
- [ ] Migration framework cho ClickHouse DDL (versioned SQL files)

### Cần comment/document thêm

- [ ] Docstrings cho `shared/common/__init__.py` — expose public API
- [ ] Inline comments cho `gold_aggregator.py` health score formula
- [ ] Inline comments cho `bronze_extractor.py` watermark gap recovery logic
- [ ] Type hints cho `sql_builder.py` SQL template functions

### Minor fixes

- [ ] `buffer_seconds` vẫn 240s — có thể giảm xuống ~90s sau khi validate simulator fix
- [ ] `sql_check_new_data()` trong `sql_builder.py` vẫn reference `extracted_at` cũ
- [ ] Gold `health_daily` Materialized View nên filter `WHERE health_score IS NOT NULL AND health_score <> 0`
- [ ] Xem xét thêm `max_active_tasks` per DAG nếu worker concurrency bị tranh chấp
- [ ] `uplink_throughput_mbps` / `downlink_throughput_mbps` — schema mới tách đôi, schema cũ gộp thành `throughput_mbps`. Cần thống nhất ở sql_builder

### Tính năng tương lai

- [ ] Catchup DAG tự động: query CH staging so với gold, aggregate hours bị thiếu
- [ ] Grafana dashboard cho health score, SLA compliance, anomaly detection
- [ ] Alerting integration (Slack/email) khi health_score < 30 hoặc SLA breach
- [ ] Dimension SCD Type-2 tracking trong ClickHouse (hiện chỉ latest snapshot)
