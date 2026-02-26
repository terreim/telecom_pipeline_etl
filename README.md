# Telecom Station Data Pipeline

Một luồng data cho dữ liệu thu thập từ trạm di động, dựa trên cấu trúc **Medallion** (Bronze → Silver → Gold). Luồng ingest dữ liệu từ Postgres, chuyển qua trung gian dưới dạng Parquet ở MinIo và đẩy thành dữ liệu aggregated lên Clickhouse.

> **Trạng thái:** MVP — pipeline chạy được, nhưng cần cross-check dữ liệu và chạy dưới load. Máy của author quá yếu để có thể chạy hoàn chỉnh.

## Architecture

```
┌──────────────┐    ┌────────────┐    ┌───────────┐    ┌─────────────┐    ┌────────────┐
│  Simulator   │───>│ PostgreSQL │───>│   MinIO   │───>│  ClickHouse │───>│    Gold    │
│              │    │  (OLTP)    │    │   (S3)    │    │  (Staging)  │    │ (Analytics │
│              │    │            │    │  Parquet  │    │             │    │   Tables)  │
└──────────────┘    └────────────┘    └───────────┘    └─────────────┘    └────────────┘
                           │                │                 │                  │
                           └────────────────┴─────────────────┴──────────────────┘
                                              Apache Airflow 3.x
```

### Các lớp Medallion

| Layer | Storage | Format | Purpose |
|-------|---------|--------|---------|
| **Bronze** | MinIO `bronze/` | Parquet | dữ liệu thô lấy từ PostgreSQL, phân mảng theo `year/month/day/hour` |
| **Silver** | MinIO `silver/` | Parquet | Làm sạch, validate, mở rộng với dữ liệu dim của từ trạm. Dữ liệu xấu được đưa về `quarantine/` |
| **Gold** | ClickHouse | Các bảng loại MergeTree | Hourly health scores, SLA compliance, anomaly features, outage/maintenance/alarm/handover reports, regional summaries |

### Luồng tín hiệu của Airflow

DAGs được lên lịch và chạy nhờ Airflow **Assets** (tín hiệu). Từng lớp gửi tín hiệu khi chạy xong:

```
Bronze (1min / 5min cron)
  │
  ├─ subscriber_traffic ──> signal://silver/trigger_traffic
  ├─ performance_metrics ─> signal://silver/trigger_metrics
  └─ station_events ──────> signal://silver/trigger_events
                                │
Silver (asset-triggered)        │
  │                             │
  ├─ traffic_cleaned ─────> signal://staging/trigger_traffic
  ├─ metrics_cleaned ─────> signal://staging/trigger_metrics
  └─ events_cleaned ──────> signal://staging/trigger_events
                                │
Gold Staging (asset-triggered)  │
  │                             │
  ├─ gold_traffic_ready ──┐
  ├─ gold_metrics_ready ──┤──> gold_health_hourly ──> gold_anomaly_features
  └─ gold_events_ready ───┘         │
                                    │ (hour 23 or manual)
                                    ▼
                             health_daily_ready
                           ┌─────┬────────┼──────┬────────┐
                           ▼     ▼        ▼      ▼        ▼
                          SLA   outage  maint  alarm  handover
                           │
                           ▼
                        sla_ready ──> region_daily
```

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestrator | Apache Airflow | 3.x (SDK-style DAGs) |
| Executor | CeleryExecutor + Redis | |
| Source DB | PostgreSQL | 16 |
| Object Store | MinIO (S3-compatible) | RELEASE.2025-01-20 |
| OLAP Warehouse | ClickHouse | latest |
| Data Format | Parquet (Snappy) | qua PyArrow |
| Container Runtime | Docker Compose | |

## Project Structure

```
data_pipeline_lab/
├── docker-compose.yaml          # Full infrastructure stack
├── Dockerfile                   # Custom Airflow image
├── .env                         # AIRFLOW_UID
├── telecom_simulator_v4.py      # Data generator (stream + backfill modes)
├── config/
│   ├── airflow.cfg
│   └── clickhouse_config.xml
├── dags/station_dags/
│   ├── bronze_lake.py           # Bronze DAGs (2): high-volume + low-volume
│   ├── silver_lake.py           # Silver DAGs (3): traffic, metrics, events
│   ├── gold_lake.py             # Gold DAGs (13): staging, aggregation, reports
│   ├── recovery.py              # Recovery DAGs (3): CH, silver, gold recovery
│   ├── warehouse.py             # (reserved)
│   ├── config/
│   │   └── config.py            # PipelineConfig — centralized env-var config
│   ├── ddl/
│   │   ├── warehouse_raw.sql    # Bronze/raw ClickHouse tables
│   │   ├── staging_silver.sql   # Silver staging tables in ClickHouse
│   │   └── gold_logic.sql       # Gold analytics tables + materialized views
│   └── util/
│       ├── s3_parquet.py        # S3ParquetIO — read/write/list/chunk parquets
│       ├── bronze_extractor.py  # BronzeExtractor — Postgres → S3 EL
│       ├── silver_transformer.py# SilverTransformer — clean, enrich, quarantine
│       ├── warehouse_loader.py  # ClickHouseLoader — silver → CH staging
│       └── gold_aggregator.py   # GoldAggregator — CH SQL → gold tables + S3

```

## Quick Start

### Tiên quyết

- Docker & Docker Compose
- ~16 GB RAM available for containers (máy author chỉ có 8 GB RAM cho Docker)

### 1. Bắt đầu

```bash
docker compose up -d
```

Docker sẽ chạy **14 services**: PostgreSQL, Redis, Airflow ecosystem (apiserver, scheduler, dag-processor, worker, triggerer), MinIO + init, ZooKeeper, ClickHouse.

Đợi đến khi `airflow-init` chạy xong (tạo admin user, chạy DB migrations).

### 2. Truy cập UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| ClickHouse HTTP | http://localhost:8123 | (no auth by default) |
| Flower (Celery) | http://localhost:5555 | (enable with `--profile flower`) |

### 3. Set Up ClickHouse Schema

Chạy scripts DDL qua Clickhouse để tạo bảng OLAP:

```bash
# From host — run each DDL file
docker compose exec clickhouse clickhouse-client --multiquery < dags/station_dags/ddl/warehouse_raw.sql
docker compose exec clickhouse clickhouse-client --multiquery < dags/station_dags/ddl/staging_silver.sql
docker compose exec clickhouse clickhouse-client --multiquery < dags/station_dags/ddl/gold_logic.sql
```

### 4. Chạy Simulator để mock dữ liệu trạm di động vào Postgres

**Stream mode** (live data, ~1 batch/minute):
```bash
docker compose exec airflow-worker python /opt/airflow/telecom_simulator_v4.py stream
```

**Backfill mode** (generate historical data):
```bash
docker compose exec airflow-worker python /opt/airflow/telecom_simulator_v4.py backfill
```

### 5. Kích hoạt DAGs

Trong Airflow UI, kích hoạt lần lượt:
1. `bronze_high_volume`, `bronze_low_volume`
2. `silver_subscribers_traffic`, `silver_performance_metrics`, `silver_station_events`
3. Tất cả `gold_staging_*` DAGs
4. `gold_health_hourly`, rồi các `gold_*` còn lại

Luồng tín hiệu sẽ lần lượt kích hoạt các DAGs khi đến lượt

## DAGs Reference

### Bronze (Ingestion)

| DAG | Schedule | Description |
|-----|----------|-------------|
| `bronze_high_volume` | `* * * * *` (1 min) | Trích `subscriber_traffic` từ Postgres → MinIO Parquet |
| `bronze_low_volume` | `*/5 * * * *` (5 min) | Trích `station_events` + `performance_metrics` |

- Dùng `FOR UPDATE SKIP LOCKED` để trích dữ liệu liên tục
- Phân mảng theo **event_time** (không phải thời gian trích extracted_at) để backfill chính xác
- Bắn tín hiệu lên silver chỉ khi sang giờ mới (hoặc khi manual)

### Silver (Transformation)

| DAG | Trigger | Description |
|-----|---------|-------------|
| `silver_subscribers_traffic` | `signal://silver/trigger_traffic` | Làm sạch dữ liệu traffic, thêm dim của trạm |
| `silver_performance_metrics` | `signal://silver/trigger_metrics` | Làm sạch dữ liệu metrics |
| `silver_station_events` | `signal://silver/trigger_events` | Làm sạch dữ liệu events |

- **Streaming per-chunk processing**: đọc bronze file 30k-row chunks qua PyArrow `iter_batches()` — author làm thế để không OOM khi dữ liệu theo giờ quá lớn
- Viết byte `_SUCCESS` theo phân mảng giờ (hour=*) khi xong
- `find_unprocessed_hours()` giúp tìm phân mảng giờ chưa được đẩy lên luồng
- Dữ liệu xấu được đểy về `quarantine/` prefix

### Gold (Analytics)

| DAG | Trigger | Description |
|-----|---------|-------------|
| `gold_staging_dim` | trigger mỗi khi staging_ bắn | Đẩy dữ liệu dim lên Clickhouse dictionary |
| `gold_staging_traffic_clickhouse` | staging_trigger_traffic | Silver traffic → CH staging |
| `gold_staging_metrics_clickhouse` | staging_trigger_metrics | Silver metrics → CH staging |
| `gold_staging_events_clickhouse` | staging_trigger_events | Silver events → CH staging |
| `gold_staging_catchup` | `*/30 * * * *` | Phát hiện silver chưa được đẩy lên (crash recovery) -- Chỉ khi dữ liệu được mark |
| `gold_health_hourly` | all 3 gold_ready | Health theo giờ |
| `gold_anomaly_features` | health_hourly_ready | Z-score phát hiện vấn đề (Theo baseline 7 ngày trước) |
| `gold_sla_compliance` | health_daily_ready | SLA uptime % theo ngày và loại tech của trạm |
| `gold_outage_report` | health_daily_ready | Track các trường hợp xảy ra và metric loss |
| `gold_maintenance_report` | health_daily_ready | Downtime |
| `gold_handover_report` | health_daily_ready | Handover giữa các trạm với nhau |
| `gold_alarm_report` | health_daily_ready |  Tần số xảy ra báo động và report ảnh hưởng |
| `gold_region_daily` | sla_ready | Báo cáo theo từng vùng + SLA |

### Recovery (Manual)

| DAG | Description |
|-----|-------------|
| `clickhouse_recovery` | Xóa `.ch_loaded` markers → Re-insert silver về CH staging. Dùng khi mất dữ liệu CH nhưng còn silver trên MinIO. |
| `silver_recovery` | Xóa `_SUCCESS` + `.ch_loaded` markers → Bắt MinIO silver lấy lại dữ liệu từ bronze. Dùng khi ETL logic lỗi. |
| `gold_recovery` | Xóa gold `_DONE` markers → Xóa và load lại từng gold aggregations theo ngày/giờ. |

Tất cả các recovery DAGs đều có thể chỉnh params qua Airflow UI:
```json
// clickhouse_recovery / silver_recovery
{ "lookback_hours": 168, "subpaths": ["traffic_cleaned"] }

// gold_recovery
{ "lookback_hours": 168, "skip_daily": false }
```

## Marker File System

Luồng dùng zero-byte S3 marker cho files để đảm bảo tính toàn vẹn và phục hồi sau crash:

| Marker | Location | Purpose |
|--------|----------|---------|
| `_SUCCESS` | `silver/<subpath>/year=.../hour=.../` | Biến đổi silver thành công cho giờ này |
| `.ch_loaded_<file>` | `silver/<subpath>/year=.../hour=.../` | File Parquet này đã được load lên CH |
| `_DONE` | `gold/_markers/hourly/year=.../hour=.../` | Gold aggregation theo giờ hoàn tất |
| `_DONE` | `gold/_markers/daily/year=.../day=.../` | Gold aggregation theo ngày hoàn tất |

## Mô hình Dữ liệu 

### Source Tables (PostgreSQL)

| Table | Volume | Description |
|-------|--------|-------------|
| `subscriber_traffic` | ~1000 rows/min | Dữ liệu traffic theo session (latency, bytes, packet loss) |
| `performance_metrics` | ~50 rows/5min | Metric của hardware theo từng trạm (CPU, memory, temperature) |
| `station_events` | ~200 rows/5min | Alarms, incidents, maintenance, handovers |
| `base_station` | Static dim | Dữ liệu không thay đổi/đổi chậm của từng trạm |
| `operator` | Static dim | Thông tin Nhà mạng |
| `location` | Static dim | Thông tin Tỉnh thành |
| `configuration` | SCD Type 2 | Dữ liệu config của từng chạm |

### Gold Tables (ClickHouse)

| Table | Engine | Grain | Key Metrics |
|-------|--------|-------|-------------|
| `gold_health_hourly` | MergeTree | station × hour | Health score (0–100), loại (healthy/warning/degraded/critical) |
| `health_daily` | AggregatingMergeTree | station × day | MV từ health_hourly |
| `gold_sla_compliance` | MergeTree | station × day | Uptime %, SLA target, breach hours, MTTR |
| `gold_anomaly_features` | MergeTree | station × hour | Z-scores cho latency/CPU/throughput/subscribers |
| `gold_outage_report` | MergeTree | incident | Thời gian xảy ra, số lượng subscribers bị ảnh hưởng, traffic loss |
| `gold_region_daily` | ReplacingMergeTree | region × day | Regional rollup với SLA compliance % |
| `gold_maintenance_report` | MergeTree | maintenance event | Thời gian được dự đoán và được ghi lại, health trước/sau |
| `gold_handover_daily` | SummingMergeTree | station pair × day | Số lượng Handover, ảnh hưởng của độ trễ |
| `gold_alarm_daily` | ReplacingMergeTree | station × day | Số lượng báo động theo độ nghiêm trọng, tần suất xảy ra |

## Health Score Algorithm

Health score (0–100) được tính theo station-hour như sau:

| Component | Weight | Score Logic |
|-----------|--------|-------------|
| Latency | 25% | 100 if < 30ms, degrades linearly to 0 at 500ms |
| Packet Loss | 20% | 100 if < 0.5%, degrades to 0 at 10% |
| CPU | 15% | 100 if < 50%, degrades to 0 at 100% |
| Temperature | 10% | 100 if < 45°C, degrades to 0 at 80°C |
| Errors/Alarms | 15% | 100 if 0, loses 10 per warning, 30 per critical |
| Throughput | 15% | % of tech-expected throughput (2G: 10 Mbps, 5G: 5000 Mbps) |

**Overrides:**
- Active incident (failure) → cap at 15
- Active incident (degradation) → cap at 50
- Active maintenance → score excluded (NULL)

## Simulator

`telecom_simulator_v4.py` stream dữ liệu gần giống với thực tế:

- **20 base stations** của 5 nhà mạng, 3 vùng, nhiều loại mạng (2G/3G/4G/5G)
- **Station personalities** — trạm gần thành phố có nhiều traffic hơn, trạm vùng miền dễ xảy ra vấn đề hơn
- **Time-of-day patterns** — Cao điểm theo khoảng giờ (Vietnam UTC+7)
- **Correlated failures** — Vấn đề có thể ảnh hưởng sang các trạm khác <- chưa implement.
- **Planned maintenance** với dữ liệu SLA đặt sẵn
- **Persistent subscriber pool** mô phỏng đi lại giữa các trạm

### Modes

```bash
# Live streaming — generates data at real-time pace
python telecom_simulator_v4.py stream

# Backfill — generates N days of historical data as fast as possible
python telecom_simulator_v4.py backfill --days 7
```

## Configuration

Toàn bộ config của luồng được lưu lại trong `PipelineConfig` ([config.py](dags/station_dags/config/config.py)):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_CONN_ID` | `postgres-oltp` | Airflow connection ID for PostgreSQL |
| `MINIO_CONN_ID` | `minio_default` | Airflow connection ID for MinIO |
| `CLICKHOUSE_CONN_ID` | `clickhouse_default` | Airflow connection ID for ClickHouse |
| `MINIO_BUCKET` | `station-lake` | S3 bucket name |
| `MINIO_PREFIX_BRONZE` | `bronze` | Bronze layer prefix |
| `MINIO_PREFIX_SILVER` | `silver` | Silver layer prefix |
| `MINIO_PREFIX_GOLD` | `gold` | Gold layer prefix |
| `SCHEMA_NAME` | `telecom` | ClickHouse schema name |

Connections của Airflow được setup sẵn trong `docker-compose.yaml` qua `AIRFLOW_CONN_*` environment variables — không cần setup manual.

## Memory / OOM Considerations

Luồng này chạy theo spec máy của author nên bị optimize, runtime có thể bị lâu (như Airflow worker bị túm còn 2 GB không đủ chạy load):

- **Bronze**: Batch theo phút (1 min / 5 min intervals)
- **Silver**: Chunk lại để stream qua PyArrow `iter_batches(batch_size=30_000)` — stream xong xóa.
- **Gold staging**:  INSERT theo từng file có check `_is_loaded` để đảm bảo độ toàn vẹn cũng như recovery khi retry sau crash. Batch recovery dùng S3 glob patterns để tóm Parquet, hạn chế số lượng INSERTs về CH, tránh nhảy TOO_MANY_PARTS.
- **Gold aggregation**: Tính theo từng ngày/giờ và viết luôn
- **Recovery**: Marker — DAG OOMs ở đâu thì retry tại đó

## Known Limitations

- Bảng Gold dùng `MergeTree` (không phải `ReplacingMergeTree`) —INSERTs lại mà không DELETE khiến cho lặp bản ghi. Recovery DAGs hạn chế điều này bằng việc MUTATE bảng trước khi INSERT lại. Chưa test đây là Heavy Mutation hay Lightweight Mutation, nhưng cứ Mutation là nặng.
- `health_daily` là MV của `gold_health_hourly`, không auto-clean khi lỗi xảy ra, recovery xóa trực tiếp bảng này, hơi hardcode
- Chưa test dữ liệu — mới chỉ test cấu trúc của luồng.
- Chưa dùng ReplicatedMergeTree do chưa setup replication.

## License
Dự án cá nhân. Credits author khi sử dụng.
