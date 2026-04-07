# Telecom Pipeline — AWS Migration Guide

A chronological record of migrating the local Docker-based telecom ETL pipeline to AWS.
Covers every phase from initial VPC setup through debugging to final Terraform IaC deployment.

**Region:** `ap-southeast-2` (Sydney)
**Date:** April 2026
**Architecture:** Medallion (Bronze → Silver → Staging → Gold)
**Budget target:** ~$10–20/month (free tier where possible)

---

## Table of Contents

1. [Phase 0 — Prerequisites](#phase-0--prerequisites)
2. [Phase 1 — VPC & Networking](#phase-1--vpc--networking)
3. [Phase 2 — Security Groups](#phase-2--security-groups)
4. [Phase 3 — S3 Bucket](#phase-3--s3-bucket)
5. [Phase 4 — RDS PostgreSQL](#phase-4--rds-postgresql)
6. [Phase 5 — ECR Repositories](#phase-5--ecr-repositories)
7. [Phase 6 — ECS Fargate (Airflow)](#phase-6--ecs-fargate-airflow)
8. [Phase 7 — EFS & ClickHouse](#phase-7--efs--clickhouse)
9. [Phase 8 — Code Changes for AWS](#phase-8--code-changes-for-aws)
10. [Phase 9 — Debugging & Lessons Learned](#phase-9--debugging--lessons-learned)
11. [Phase 10 — Terraform IaC](#phase-10--terraform-iac)
12. [Phase 11 — Next Steps](#phase-11--next-steps)
13. [Cost Breakdown](#cost-breakdown)
14. [Architecture Diagram](#architecture-diagram)

---

## Phase 0 — Prerequisites

### AWS Account Setup
1. Created an **IAM user** (`{youraccounthere}`) in a `DataEngineers` group — never use the root account.
2. Attached the `AdministratorAccess` policy to the group (for learning; tighten in production).
3. Generated **Access Key + Secret Key** for CLI access.
4. Installed and configured the AWS CLI:
   ```bash
   aws configure
   # Access Key: <your-key>
   # Secret Key: <your-secret>
   # Region: ap-southeast-2
   # Output: json
   ```

### Terraform Setup
1. Installed Terraform CLI (v1.x).
2. **Corporate firewall blocked `registry.terraform.io`** — had to manually download the AWS provider:
   - Downloaded `terraform-provider-aws_6.x` from the Terraform releases page.
   - Placed it in `./providers/registry.terraform.io/hashicorp/aws/6.x/<platform>/`.
   - Ran `terraform init -plugin-dir=./providers` instead of normal init.

---

## Phase 1 — VPC & Networking

### What was created
| Resource | Name | Details |
|---|---|---|
| VPC | `telecom-vpc` | CIDR `10.0.0.0/16`, DNS support + hostnames enabled |
| Public Subnet | `telecom-public-1` | `10.0.1.0/24`, AZ `ap-southeast-2a` |
| Private Subnet 1 | `telecom-private-1` | `10.0.2.0/24`, AZ `ap-southeast-2a` |
| Private Subnet 2 | `telecom-private-2` | `10.0.3.0/24`, AZ `ap-southeast-2b` |
| Internet Gateway | `telecom-igw` | Attached to VPC |
| Public Route Table | `telecom-public-rt` | `0.0.0.0/0` → IGW |
| Private Route Table | `telecom-private-rt` | Local only (+ S3 endpoint route) |
| S3 VPC Endpoint | `telecom-s3-endpoint` | Gateway type, free, both route tables |

### Console steps (done first to learn, then codified in Terraform)
1. **VPC** → Create VPC → `telecom-vpc`, CIDR `10.0.0.0/16`.
2. Enable **DNS resolution** and **DNS hostnames** on the VPC (critical for EFS later).
3. Create 3 subnets in the VPC (public, private-1, private-2) across two AZs.
4. Create an **Internet Gateway**, attach it to the VPC.
5. Create two **Route Tables** (public, private).
6. Add a route `0.0.0.0/0 → IGW` to the public route table.
7. **Associate** public subnet with public RT, both private subnets with private RT.
8. Create a **VPC Endpoint** for S3 (Gateway type) — associates with both route tables.

### Key concepts learned
- **Subnets** are "public" only if their route table has an internet gateway route.
- The **local route** (`10.0.0.0/16 → local`) is created automatically — never declare it.
- **S3 is a global service**, not inside any VPC. The VPC Gateway Endpoint gives private subnets a route to S3 without NAT.
- **VPC DNS support + hostnames** must be enabled for EFS DNS resolution (`fs-xxx.efs.region.amazonaws.com`).
- Subnets not explicitly associated with a route table use the **main route table** (the VPC default).

---

## Phase 2 — Security Groups

### Three security groups

**Public SG** (`telecom-public-sg`) — for Airflow webserver:
- Inbound: HTTP (80), Airflow UI (8080), SSH (22) from your IP.
- Inbound: SSH (22) from EC2 Instance Connect range (`13.239.158.0/29` for ap-southeast-2).
- Inbound: Self-reference (all traffic) — allows tasks in the same SG to talk to each other.
- Outbound: All traffic to `0.0.0.0/0`.

**Private SG** (`telecom-private-sg`) — for ClickHouse, EFS, VPC endpoints:
- Inbound: All traffic from public-sg (so Airflow can reach ClickHouse).
- Inbound: Self-reference (all traffic) — internal services can communicate freely.
- Outbound: All traffic to `0.0.0.0/0`.

**RDS SG** (`telecom-rds-sg`) — for PostgreSQL:
- Inbound: Port 5432 only from public-sg and private-sg (scoped, not all traffic).
- Outbound: All traffic.

### Key concepts learned
- Used **separate `aws_vpc_security_group_ingress_rule` resources** instead of inline `ingress {}` blocks. Inline blocks cause Terraform to delete and recreate rules on every change; separate resources are additive.
- **Self-referencing SG**: `referenced_security_group_id = self` means "any resource tagged with this SG can talk to any other resource tagged with the same SG." Essential for container-to-container communication within ECS.
- EC2 Instance Connect has a **fixed IP range per region** — add it to SSH rules as an alternative to direct SSH.
- **Dynamic IP gotcha**: Work network IPs rotate frequently. Had to update SG rules multiple times when IP changed.

---

## Phase 3 — S3 Bucket

### Created
- **`telecom-pipeline-{youraccounthere}`** — stores pipeline data (bronze/, silver/, logs/).
- **`telecom-tfstate-{youraccounthere}`** — stores Terraform remote state (created manually, versioning enabled).

### Console steps
1. S3 → Create bucket → name must be globally unique.
2. All defaults (Block Public Access enabled, no versioning needed for data bucket).

### Gotcha
- Bucket names ending in `-an` trigger AWS **directory bucket** validation (S3 Express One Zone). Use a different suffix.

### IAM policy for S3 access
Created a scoped policy `TelecomS3Access` allowing only `GetObject`, `PutObject`, `DeleteObject`, `ListBucket` on the pipeline bucket. Attached to the ECS task role (not the execution role).

---

## Phase 4 — RDS PostgreSQL

### Created
| Setting | Value |
|---|---|
| Identifier | `telecom-oltp` |
| Engine | PostgreSQL 17 |
| Instance class | `db.t3.micro` (free tier) |
| Storage | 20 GB gp3 (free tier) |
| Database name | `airflow` |
| Username | `postgres` |
| Subnet group | `telecom-db-subnet-group` (private-1, private-2) |
| Security group | `telecom-rds-sg` |
| Publicly accessible | No |

### Console steps
1. RDS → Create database → Standard → PostgreSQL.
2. Free tier template.
3. Set master password (save it — needed for `terraform.tfvars`).
4. Connectivity: select the VPC, create a **DB subnet group** with both private subnets.
5. Assign the RDS security group, disable public access.

### Key concepts learned
- **DB subnet group** requires subnets in at least **2 different AZs** (for Multi-AZ failover support, even if you don't enable it).
- `db_name` creates the initial database. Airflow's metadata DB is named `airflow`, with the pipeline data in a `telecom` schema.
- Changing `db_name` in Terraform **forces RDS recreation** — be careful.
- `publicly_accessible = false` means it's only reachable from within the VPC.
- gp3 storage is included in free tier (20 GB).

---

## Phase 5 — ECR Repositories

### Created
- `telecom/airflow` — custom Airflow image with providers and DAGs baked in.
- `telecom/clickhouse` — ClickHouse server image.

### Push workflow
```bash
# Authenticate Docker to ECR
aws ecr get-login-password --region ap-southeast-2 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.ap-southeast-2.amazonaws.com

# Build and push Airflow image
docker build -t telecom/airflow .
docker tag telecom/airflow:latest <account-id>.dkr.ecr.ap-southeast-2.amazonaws.com/telecom/airflow:latest
docker push <account-id>.dkr.ecr.ap-southeast-2.amazonaws.com/telecom/airflow:latest

# Build and push ClickHouse image
docker build -f Dockerfile.clickhouse -t telecom/clickhouse .
docker tag telecom/clickhouse:latest <account-id>.dkr.ecr.ap-southeast-2.amazonaws.com/telecom/clickhouse:latest
docker push <account-id>.dkr.ecr.ap-southeast-2.amazonaws.com/telecom/clickhouse:latest
```

---

## Phase 6 — ECS Fargate (Airflow)

### Cluster
- **`telecom-cluster`** — Fargate capacity provider (no EC2 instances to manage).

### Airflow Task Definition
Two containers in one task (share `localhost` networking):

| Container | Image | Command | Ports |
|---|---|---|---|
| `airflow-webserver` | `telecom/airflow:latest` | `airflow db migrate && airflow api-server` | 8080 |
| `airflow-dagscheduler` | `telecom/airflow:latest` | `mkdir -p /opt/airflow/etl_temp && airflow dag-processor & airflow scheduler & airflow triggerer & wait` | — |

**Task resources:** 2 vCPU, 6 GB memory (needed for DAG parsing + task execution).

### Running the task
```bash
aws ecs run-task \
  --cluster telecom-cluster \
  --task-definition telecom-airflow \
  --launch-type FARGATE \
  --network-configuration '{
    "awsvpcConfiguration": {
      "subnets": ["<public-subnet-id>"],
      "securityGroups": ["<public-sg-id>"],
      "assignPublicIp": "ENABLED"
    }
  }'
```

### Key concepts learned
- **Fargate** = serverless containers. No EC2 to patch or scale. You define CPU/memory per task.
- **Two containers in one task** share the same network namespace (localhost). This is how we solved the Airflow api-server ↔ scheduler communication issue.
- `assignPublicIp: ENABLED` is required in public subnets for Fargate tasks to reach the internet (no NAT gateway).
- **Task Execution Role**: pulls images from ECR, sends logs to CloudWatch.
- **Task Role**: what the running container can access (S3, RDS, etc.).
- `&&` blocks subsequent commands (api-server never exits). Use `&` to background processes.
- CloudWatch log groups: one per container, `/ecs/<project>-<container>` naming convention.

### Environment variables (shared by both containers)
```
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN    = postgresql+psycopg2://postgres:<pw>@<rds-endpoint>:5432/airflow
AIRFLOW__API_AUTH__JWT_SECRET           = <shared-jwt-secret>
AIRFLOW__CORE__INTERNAL_API_JWT_SECRET  = <same-jwt-secret>
AIRFLOW__CORE__EXECUTOR                = LocalExecutor
AIRFLOW__CORE__DAGS_FOLDER             = /opt/airflow/dags
AIRFLOW__CORE__FERNET_KEY              = <persistent-fernet-key>
AIRFLOW__LOGGING__REMOTE_LOGGING       = True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER = s3://<bucket>/logs
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID   = aws_default
MINIO_CONN_ID                          = aws_default
MINIO_BUCKET                           = telecom-pipeline-{youraccounthere}
```

---

## Phase 7 — EFS & ClickHouse

### EFS (Elastic File System)
- `telecom-clickhouse-data` — encrypted, general purpose, bursting throughput.
- Mount targets in both private subnets (private-1, private-2), attached to private-sg.
- Backups disabled (to save cost).

### ClickHouse Task Definition
| Container | Image | Ports | Volume |
|---|---|---|---|
| `clickhouse` | `telecom/clickhouse:latest` | 8123 (HTTP), 9000 (native) | EFS → `/var/lib/clickhouse` |

**Task resources:** 1 vCPU, 2 GB memory.

### VPC Interface Endpoints (for private subnet)
ClickHouse runs in the private subnet (no internet access), so it needs VPC endpoints to reach AWS services:

| Endpoint | Service | Cost |
|---|---|---|
| ECR API | `com.amazonaws.ap-southeast-2.ecr.api` | ~$0.01/hr/AZ |
| ECR DKR | `com.amazonaws.ap-southeast-2.ecr.dkr` | ~$0.01/hr/AZ |
| CloudWatch Logs | `com.amazonaws.ap-southeast-2.logs` | ~$0.01/hr/AZ |

**Total cost warning:** 3 endpoints × 2 AZs × $0.01/hr ≈ **$43/month**. These were deleted during non-use and only created when ClickHouse needs to run. The Terraform still defines them, but you can comment them out or use `terraform destroy -target` to remove them.

---

## Phase 8 — Code Changes for AWS

### Dockerfile
Removed PySpark and JDK (Spark would run on EMR, not in Airflow container):
```dockerfile
FROM apache/airflow:3.1.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon==9.19.0 \
    airflow-clickhouse-plugin==1.6.0 \
    clickhouse-driver==0.2.10

RUN mkdir -p /opt/airflow/etl_temp

COPY ./dags/ /opt/airflow/dags/
COPY ./telecom_simulator_v5.py /opt/airflow/telecom_simulator_v5.py
```

### connections.py
Changed S3 endpoint default from local MinIO to AWS:
```python
# Before (local Docker)
s3_endpoint = conn.extra_dejson.get("endpoint_url", "http://minio:9000")

# After (AWS)
s3_endpoint = conn.extra_dejson.get("endpoint_url", "https://s3.ap-southeast-2.amazonaws.com")
```

### dag_factory.py
Moved `SparkSilverTransformer` import inside the task function (lazy import) to avoid PySpark loading at DAG parse time, which caused 30s timeout on the small Fargate container:
```python
# Before — top-level import
from shared.util.spark_silver_transformer import SparkSilverTransformer

# After — lazy import inside task
@task(...)
def transform(**context):
    from shared.util.spark_silver_transformer import SparkSilverTransformer
    transformer = SparkSilverTransformer(...)
```

---

## Phase 9 — Debugging & Lessons Learned

### Timeline of issues encountered

#### 1. SSH Connection Timeout to EC2
- **Symptom:** `ssh -i key.pem ec2-user@<ip>` hangs indefinitely.
- **Cause:** Corporate firewall blocks outbound port 22.
- **Fix:** Used **EC2 Instance Connect** (browser-based SSH) instead. Added the EC2 IC IP range `13.239.158.0/29` to the public security group.

#### 2. Terraform Registry Blocked
- **Symptom:** `terraform init` fails with connection timeout to `registry.terraform.io`.
- **Cause:** Corporate firewall.
- **Fix:** Manually downloaded the AWS provider binary and used `terraform init -plugin-dir=./providers`.

#### 3. S3 Bucket Name Validation Error
- **Symptom:** `InvalidBucketName` error when creating a bucket ending in `-an`.
- **Cause:** AWS interprets `-an` suffix as a directory bucket (S3 Express One Zone) naming pattern.
- **Fix:** Changed bucket name to `telecom-pipeline-{youraccounthere}`.

#### 4. DAGs Not Showing in Airflow UI
- **Symptom:** Airflow webserver loads, but no DAGs visible.
- **Cause:** Only running `airflow api-server`, not `dag-processor` or `scheduler`.
- **Fix:** Bundled all processes: `airflow dag-processor & airflow scheduler & airflow triggerer & wait`.

#### 5. DagBag Import Timeout (30 seconds)
- **Symptom:** `DagBag import timeout exceeded` errors in scheduler logs.
- **Cause:** PySpark imports at parse time (via `SparkSilverTransformer`) on a 0.5 vCPU container.
- **Fix:** (a) Removed PySpark from Dockerfile entirely. (b) Moved the import inside the task function.

#### 6. Fernet Key / Connections Page 500 Error
- **Symptom:** Airflow connections page returns Internal Server Error.
- **Cause:** Connections encrypted with a Fernet key that changes on every container restart (Airflow generates a random one if none is set).
- **Fix:** Set a persistent `AIRFLOW__CORE__FERNET_KEY` env var. Deleted corrupted connections with a Python one-liner in the startup command.

#### 7. JWT Signature Verification Failed (Task Execution)
- **Symptom:** `SignatureVerificationError` when scheduler tries to report task status.
- **Cause:** Airflow 3.x scheduler and api-server need a shared JWT secret for the execution API.
- **Fix:** Set `AIRFLOW__API_AUTH__JWT_SECRET` to the same value on both containers. (Note: `AIRFLOW__CORE__INTERNAL_API_JWT_SECRET` alone was not sufficient.)

#### 8. `AIRFLOW_CONN_POSTGRES_OLTP` Env Var Not Working
- **Symptom:** `conn_id 'postgres_oltp' not found` despite env var being set.
- **Cause:** Unknown — even JSON format didn't work.
- **Fix:** Created the connection via the Airflow UI instead.

#### 9. Connection Refused Between Split Airflow Tasks
- **Symptom:** Scheduler's LocalExecutor calls api-server at `localhost:8080`, but api-server is in a different Fargate task.
- **Cause:** Separate Fargate tasks have separate network namespaces. `localhost` doesn't cross task boundaries.
- **Fix:** Bundled both containers into a single task definition (two-container pattern). This way they share `localhost`.

#### 10. EFS Mount Failure
- **Symptom:** `Failed to resolve fs-xxx.efs.ap-southeast-2.amazonaws.com`.
- **Cause:** VPC DNS support and hostnames were not enabled.
- **Fix:** `aws ec2 modify-vpc-attribute --enable-dns-support` and `--enable-dns-hostnames` (now in Terraform as `enable_dns_support = true` / `enable_dns_hostnames = true`).

#### 11. ECR Image Pull Timeout in Private Subnet
- **Symptom:** ClickHouse task stuck at `PROVISIONING` then fails.
- **Cause:** Private subnet has no internet access, can't reach ECR.
- **Fix:** Created VPC Interface endpoints for ECR API and ECR DKR.

#### 12. CloudWatch Logs Failure in Private Subnet
- **Symptom:** Task fails with `ResourceNotFoundException` for log group.
- **Cause:** Same as above — no endpoint for CloudWatch Logs.
- **Fix:** Created VPC Interface endpoint for CloudWatch Logs.

#### 13. ClickHouse `s3()` Function Wrong URL
- **Symptom:** ClickHouse can't read S3 files.
- **Cause:** Code still had `http://minio:9000` as default endpoint and `station-lake` as bucket name.
- **Fix:** Changed `connections.py` default to `https://s3.ap-southeast-2.amazonaws.com`. Set `MINIO_BUCKET=telecom-pipeline-{youraccounthere}` env var.

#### 14. `/opt/airflow/etl_temp` Not Found
- **Symptom:** SilverTransformer fails writing temp files.
- **Cause:** Directory doesn't exist in the Fargate container.
- **Fix:** Added `RUN mkdir -p /opt/airflow/etl_temp` to Dockerfile.

#### 15. IP Address Changed
- **Symptom:** Can't access Airflow UI from browser.
- **Cause:** Work network rotates IP addresses.
- **Fix:** Updated security group with new IP. (This happened multiple times.)

#### 16. Terraform Destroy Stuck
- **Symptom:** `terraform destroy` hangs on private subnets and security groups.
- **Cause:** Manually-created resources (EFS mount targets, running ECS tasks) still using them.
- **Fix:** Manually stopped ECS tasks, deleted EFS mount targets, then destroy completed.

---

## Phase 10 — Terraform IaC

### File structure
```
infra/
  provider.tf      — AWS provider, Terraform backend (S3 remote state)
  variables.tf     — All input variables (region, project, secrets)
  vpc.tf           — VPC, subnets, IGW, route tables, VPC endpoints
  security.tf      — 3 security groups with separate ingress/egress rules
  s3.tf            — S3 pipeline bucket
  rds.tf           — DB subnet group + RDS instance
  ecr.tf           — 2 ECR repositories
  efs.tf           — EFS file system + backup policy + mount targets
  iam.tf           — Task execution role, task role, S3 access policy
  ecs.tf           — ECS cluster, Airflow task def, ClickHouse task def, log groups
  outputs.tf       — VPC ID, S3 bucket, RDS endpoint, ECR URLs, etc.
```

### Secrets management
- `terraform.tfvars` holds all sensitive values (gitignored):
  ```hcl
  db_password    = "..."
  bucket         = "telecom-pipeline-{youraccounthere}"
  jwt_secret     = "..."
  aws_access_key = "..."
  aws_secret_key = "..."
  ```
- Variables marked `sensitive = true` in `variables.tf` are masked in Terraform output.

### Remote state
- State stored in `telecom-tfstate-{youraccounthere}` S3 bucket (created manually with versioning enabled).
- Config in `provider.tf`:
  ```hcl
  backend "s3" {
    bucket = "telecom-tfstate-{youraccounthere}"
    key    = "infra/terraform.tfstate"
    region = "ap-southeast-2"
  }
  ```

### Deploy commands
```bash
cd infra/

# First time (with offline provider)
terraform init -plugin-dir=./providers

# Plan and apply
terraform plan -out=plan.out
terraform show plan.out          # review the plan (binary file, can't cat directly)
terraform apply plan.out

# Destroy everything
terraform destroy
```

### Final result
```
Apply complete! Resources: 50 added, 0 changed, 0 destroyed.
```

---

## Phase 11 — Next Steps

### Before running the pipeline again
1. **Push Docker images** to the new ECR repos (Terraform recreated them with new URIs).
2. **Create VPC Interface endpoints** if running ClickHouse (or uncomment them in `vpc.tf`). Beware of ~$43/month cost.
3. **Set `postgres_oltp` connection** in the Airflow UI (env var approach didn't work).
4. **Update security group** with your current IP address.

### Future improvements
- **Two-container Airflow task** is now in Terraform but hasn't been fully tested.
- **Cloud Map service discovery** for proper multi-task Airflow (if splitting webserver and scheduler into separate tasks).
- **MWAA** (Managed Airflow) — too expensive at ~$350/month, but ideal for production.
- **EMR Serverless** for Spark jobs (replace local PySpark path).
- **NAT Gateway** — alternative to VPC endpoints for private subnet internet access (~$32/month).
- `gold_health_hourly` DAG needs idempotent re-aggregation redesign.

---

## Cost Breakdown

| Resource | Monthly Cost | Notes |
|---|---|---|
| RDS db.t3.micro | Free (750 hrs/mo) | Free tier, first 12 months |
| S3 (< 5 GB) | Free | Free tier |
| ECS Fargate | ~$0.05/hr per task | Only charged while running |
| EFS | ~$0.30/GB/mo | Minimal data stored |
| ECR | Free (< 500 MB) | Free tier |
| VPC Endpoints (Interface) | ~$43/mo if all 3 active | Delete when not in use |
| S3 VPC Endpoint (Gateway) | Free | Always keep |
| CloudWatch Logs | ~$0.50/GB ingested | Minimal with 7-day retention |

**Strategy:** Only run Fargate tasks when needed. Delete VPC Interface endpoints when not running ClickHouse. Stay in free tier for RDS/S3/ECR.

---

## Architecture Diagram

```
                        Internet
                           |
                    [Internet Gateway]
                           |
         ┌─────────────────┴─────────────────────┐
         │              telecom-vpc               │
         │            10.0.0.0/16                 │
         │                                        │
         │  ┌──────────────────────┐              │
         │  │  public-1 (2a)       │              │
         │  │  10.0.1.0/24         │              │
         │  │                      │              │
         │  │  ┌────────────────┐  │              │
         │  │  │ ECS Fargate    │  │              │
         │  │  │ Airflow Task   │  │              │
         │  │  │ - webserver    │  │              │
         │  │  │ - scheduler    │  │              │
         │  │  │ - dag-processor│  │              │
         │  │  │ - triggerer    │  │              │
         │  │  └───────┬────────┘  │              │
         │  │          │           │              │
         │  └──────────┼───────────┘              │
         │             │                          │
         │  ┌──────────┼──────────────────────┐   │
         │  │  private-1 (2a) & private-2 (2b)│   │
         │  │  10.0.2.0/24    10.0.3.0/24     │   │
         │  │          │                      │   │
         │  │  ┌───────┴────────┐  ┌───────┐  │   │
         │  │  │ ECS Fargate    │  │  RDS   │  │   │
         │  │  │ ClickHouse     │  │ PG 17  │  │   │
         │  │  │ + EFS volume   │  │        │  │   │
         │  │  └────────────────┘  └───────┘  │   │
         │  │                                 │   │
         │  └─────────────────────────────────┘   │
         │                                        │
         │  [S3 Gateway Endpoint] ──── S3 Bucket  │
         │  [ECR/Logs Interface Endpoints]        │
         └────────────────────────────────────────┘
```
