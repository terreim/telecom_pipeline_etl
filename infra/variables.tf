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