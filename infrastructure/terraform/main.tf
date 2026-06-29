# infrastructure/terraform/main.tf
# 
# Provisions all AWS resources for the Olist E-Commerce data pipeline:
#   - S3 buckets (raw + processed)
#   - Redshift Serverless (workgroup + namespace)
#   - IAM roles for Redshift ↔ S3 access
#   - Glue Crawler for automatic schema discovery
# 

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Uncomment to store state in S3:
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "ecom-pipeline/terraform.tfstate"
  #   region = "ap-southeast-2"
  # }
}

provider "aws" {
  region = var.aws_region
}

locals {
  project     = "ecom-pipeline"
  environment = var.environment
  tags = {
    Project     = local.project
    Environment = local.environment
    ManagedBy   = "terraform"
    Owner       = "data-engineering"
  }
}

# S3 BUCKETS
resource "aws_s3_bucket" "raw" {
  bucket = "${local.project}-raw-${local.environment}"
  tags   = merge(local.tags, { Layer = "bronze" })
}

resource "aws_s3_bucket" "processed" {
  bucket = "${local.project}-processed-${local.environment}"
  tags   = merge(local.tags, { Layer = "silver" })
}

resource "aws_s3_bucket_versioning" "raw_versioning" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "archive-old-raw-data"
    status = "Enabled"
    filter { prefix = "olist/" }
    transition {
      days          = 90
      storage_class = "INTELLIGENT_TIERING"
    }
    expiration {
      days = 365
    }
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM — Redshift S3 Access Role

data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3_role" {
  name               = "${local.project}-redshift-s3-${local.environment}"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json
  tags               = local.tags
}

resource "aws_iam_policy" "redshift_s3_policy" {
  name = "${local.project}-redshift-s3-policy-${local.environment}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3" {
  role       = aws_iam_role.redshift_s3_role.name
  policy_arn = aws_iam_policy.redshift_s3_policy.arn
}

# REDSHIFT SERVERLESS

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${local.project}-ns-${local.environment}"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_role.arn]
  tags                = local.tags
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${local.project}-wg-${local.environment}"
  base_capacity  = 8   # 8 RPUs — cost-efficient for dev; scale to 32+ for prod
  publicly_accessible = false
  subnet_ids     = var.subnet_ids
  security_group_ids = [aws_security_group.redshift.id]
  tags           = local.tags
}

# VPC SECURITY GROUP FOR REDSHIFT 

resource "aws_security_group" "redshift" {
  name        = "${local.project}-redshift-sg-${local.environment}"
  description = "Allow inbound to Redshift from within VPC"
  vpc_id      = var.vpc_id
  tags        = local.tags

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Redshift port from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# GLUE CRAWLER (auto-discover schema from S3)

resource "aws_iam_role" "glue_crawler" {
  name = "${local.project}-glue-crawler-${local.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_catalog_database" "olist" {
  name = "olist_raw_${local.environment}"
}

resource "aws_glue_crawler" "olist_raw" {
  name          = "${local.project}-raw-crawler-${local.environment}"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.olist.name
  schedule      = "cron(30 2 * * ? *)"   # 02:30 UTC daily

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/olist/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = local.tags
}

# OUTPUTS

output "s3_raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "s3_processed_bucket" {
  value = aws_s3_bucket.processed.bucket
}

output "redshift_endpoint" {
  value     = aws_redshiftserverless_workgroup.main.endpoint[0].address
  sensitive = true
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift_s3_role.arn
}
