output "rds_endpoint" {
    description = "RDS endpoint for the Telecom pipeline"
    value       = aws_db_instance.telecom_oltp.endpoint
}

output "s3_bucket_name" {
    description = "S3 bucket name for the Telecom pipeline"
    value       = aws_s3_bucket.main.bucket
}

output "vpc_id" {
    description = "VPC ID for the Telecom pipeline"
    value       = aws_vpc.main.id
}