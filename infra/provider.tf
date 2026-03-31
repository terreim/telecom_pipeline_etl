terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  backend "s3" {
    bucket = "telecom-tfstate-duylt10"
    key    = "infra/terraform.tfstate"
    region = "ap-southeast-2"
  }
}

provider "aws" {
    region = var.region
}