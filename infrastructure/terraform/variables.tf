# infrastructure/terraform/variables.tf

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "ap-southeast-2" 
}

variable "environment" {
  description = "Deployment environment: dev | prod"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be 'dev' or 'prod'."
  }
}

variable "redshift_db_name" {
  description = "Redshift database name"
  type        = string
  default     = "ecom_db"
}

variable "redshift_admin_user" {
  description = "Redshift admin username"
  type        = string
  default     = "admin"
}

variable "redshift_admin_password" {
  description = "Redshift admin password (min 8 chars, mixed case + number)"
  type        = string
  sensitive   = true
}

variable "vpc_id" {
  description = "VPC ID where Redshift will be deployed"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block for security group ingress rule"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_ids" {
  description = "List of private subnet IDs for Redshift Serverless workgroup"
  type        = list(string)
}
