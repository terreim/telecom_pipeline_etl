resource "aws_ecr_repository" "airflow" {
  name                 = "telecom/airflow"
  image_tag_mutability = "MUTABLE"
}

resource "aws_ecr_repository" "clickhouse" {
  name                 = "telecom/clickhouse"
  image_tag_mutability = "MUTABLE"
}

