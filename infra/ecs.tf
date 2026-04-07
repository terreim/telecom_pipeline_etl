# ──────────────────────────────────────
# Cluster
# ──────────────────────────────────────
resource "aws_ecs_cluster" "main" {
  name = "${var.project}-cluster"

  tags = {
    Name = "${var.project}-cluster"
  }
}

# ──────────────────────────────────────
# Shared env vars for all Airflow containers
# ──────────────────────────────────────
locals {
  airflow_common_env = [
    { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",       value = "postgresql+psycopg2://postgres:${var.db_password}@${aws_db_instance.telecom_oltp.address}:5432/airflow" },
    { name = "AIRFLOW__API_AUTH__JWT_SECRET",             value = var.jwt_secret },
    { name = "AIRFLOW__CORE__INTERNAL_API_JWT_SECRET",    value = var.jwt_secret },
    { name = "AIRFLOW__CORE__DAGS_FOLDER",                value = "/opt/airflow/dags" },
    { name = "AIRFLOW__CORE__EXECUTOR",                   value = "LocalExecutor" },
    { name = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT",      value = "120" },
    { name = "AIRFLOW__CORE__FERNET_KEY" ,                value = "" },
    { name = "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS", value = "True" },
    { name = "AIRFLOW__LOGGING__BASE_LOG_FOLDER",         value = "/opt/airflow/logs" },
    { name = "AIRFLOW__LOGGING__REMOTE_LOGGING",          value = "True" },
    { name = "AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER",  value = "s3://${var.bucket}/logs" },
    { name = "AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID",      value = "aws_default" },
    { name = "MINIO_CONN_ID",                             value = "aws_default" },
    { name = "MINIO_BUCKET",                              value = var.bucket },
    { name = "AIRFLOW_CONN_POSTGRES_OLTP",                value = jsonencode({
      conn_type = "postgres",
      host      = aws_db_instance.telecom_oltp.address,
      login     = "postgres",
      password  = var.db_password,
    })},
    { name = "AIRFLOW_CONN_AWS_DEFAULT",                  value = jsonencode({
      conn_type = "aws"
      login     = var.aws_access_key
      password  = var.aws_secret_key
      extra     = { region_name = var.region }
    })},
  ]
}

# ──────────────────────────────────────
# Airflow Task Definition (two containers)
# ──────────────────────────────────────
resource "aws_ecs_task_definition" "airflow" {
  family                   = "${var.project}-airflow"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "6144"

  task_role_arn      = aws_iam_role.ecs_task.arn
  execution_role_arn = aws_iam_role.ecs_task_execution.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "airflow-webserver"
      image     = "${aws_ecr_repository.airflow.repository_url}:latest"
      essential = true
      command   = ["sh", "-c", "airflow db migrate && airflow api-server"]

      portMappings = [{
        containerPort = 8080
        hostPort      = 8080
        protocol      = "tcp"
      }]

      environment = local.airflow_common_env

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-create-group"  = "true"
          "awslogs-group"         = "/ecs/${var.project}-airflow-webserver"
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    },
    {
      name      = "airflow-dagscheduler"
      image     = "${aws_ecr_repository.airflow.repository_url}:latest"
      essential = true
      command   = ["sh", "-c", "mkdir -p /opt/airflow/etl_temp && airflow dag-processor & airflow scheduler & airflow triggerer & wait"]

      environment = local.airflow_common_env

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-create-group"  = "true"
          "awslogs-group"         = "/ecs/${var.project}-airflow-dagscheduler"
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

# ──────────────────────────────────────
# CloudWatch Log Groups
# ──────────────────────────────────────
resource "aws_cloudwatch_log_group" "airflow_webserver" {
  name              = "/ecs/${var.project}-airflow-webserver"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "airflow_dagscheduler" {
  name              = "/ecs/${var.project}-airflow-dagscheduler"
  retention_in_days = 7
}

# ──────────────────────────────────────
# ClickHouse Task Definition
# ──────────────────────────────────────
resource "aws_ecs_task_definition" "clickhouse" {
  family                   = "${var.project}-clickhouse"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"

  task_role_arn      = aws_iam_role.ecs_task.arn
  execution_role_arn = aws_iam_role.ecs_task_execution.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  volume {
    name = "ch-data"
    efs_volume_configuration {
      file_system_id = aws_efs_file_system.clickhouse_data.id
      root_directory = "/"
    }
  }

  container_definitions = jsonencode([
    {
      name      = "clickhouse"
      image     = "${aws_ecr_repository.clickhouse.repository_url}:latest"
      essential = true

      portMappings = [
        { containerPort = 8123, hostPort = 8123, protocol = "tcp" },
        { containerPort = 9000, hostPort = 9000, protocol = "tcp" }
      ]

      mountPoints = [{
        sourceVolume  = "ch-data"
        containerPath = "/var/lib/clickhouse"
      }]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-create-group"  = "true"
          "awslogs-group"         = "/ecs/${var.project}-clickhouse"
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_log_group" "clickhouse" {
  name              = "/ecs/${var.project}-clickhouse"
  retention_in_days = 7
}

