variable "region" {
    description = "AWS region"
    type        = string
    default     = "ap-southeast-2"
}

variable "project" {
    description = "Project name prefix for all resources"
    type        = string
    default     = "telecom"
}

variable "vpc_cidr" {
    description = "VPC CIDR block"
    type        = string
    default     = "10.0.0.0/16"
}

variable "db_password" {
    description = "RDS master password"
    type        = string
    sensitive   = true
}

variable "bucket" {
    description = "S3 bucket name for the Telecom pipeline (must be globally unique)"
    type        = string
}

variable "jwt_secret" {
    description = "Shared JWT secret for Airflow internal API auth"
    type        = string
    sensitive   = true
}

variable "aws_access_key" {
    description = "AWS access key for Airflow S3 connection"
    type        = string
    sensitive   = true
}

variable "aws_secret_key" {
    description = "AWS secret key for Airflow S3 connection"
    type        = string
    sensitive   = true
}