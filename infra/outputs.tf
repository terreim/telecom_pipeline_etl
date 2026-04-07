output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "s3_bucket_name" {
  description = "S3 bucket name for pipeline data"
  value       = aws_s3_bucket.main.bucket
}

output "rds_endpoint" {
  description = "RDS endpoint (host:port)"
  value       = aws_db_instance.telecom_oltp.endpoint
}

output "ecr_airflow_url" {
  description = "ECR repository URL for Airflow image"
  value       = aws_ecr_repository.airflow.repository_url
}

output "ecr_clickhouse_url" {
  description = "ECR repository URL for ClickHouse image"
  value       = aws_ecr_repository.clickhouse.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "efs_file_system_id" {
  description = "EFS file system ID for ClickHouse data"
  value       = aws_efs_file_system.clickhouse_data.id
}

output "airflow_task_definition" {
  description = "Airflow task definition ARN (use to run tasks)"
  value       = aws_ecs_task_definition.airflow.arn
}

output "clickhouse_task_definition" {
  description = "ClickHouse task definition ARN"
  value       = aws_ecs_task_definition.clickhouse.arn
}