provider "aws" {
  region = var.region
}

##############################
# S3 - BUCKET DE BACKEND
##############################

resource "aws_s3_bucket" "tf_state" {
  bucket = var.tf_state_bucket_name

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name = "terraform-state"
  }
}

resource "aws_s3_bucket_versioning" "tf_state_versioning" {
  bucket = aws_s3_bucket.tf_state.id

  versioning_configuration {
    status = "Disabled"
  }
}

##############################
# DYNAMODB - LOCK
##############################

resource "aws_dynamodb_table" "tf_lock" {
  name         = var.tf_lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name = "terraform-lock"
  }
}

##############################
# DATA LAKE BUCKETS
##############################

resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
}

resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
}

resource "aws_s3_bucket" "gold" {
  bucket = var.gold_bucket_name
}

##############################
# VERSIONAMENTO 
##############################

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_versioning" "silver_versioning" {
  bucket = aws_s3_bucket.silver.id

  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_versioning" "gold_versioning" {
  bucket = aws_s3_bucket.gold.id

  versioning_configuration {
    status = "Disabled"
  }
}

##############################
# IAM ROLE PARA DATABRICKS
##############################

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "databricks_role" {
  name = "databricks-s3-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "*" # para o case não vou me preocupar com segurança agora
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "databricks_s3_policy" {
  name = "databricks-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          "arn:aws:s3:::${var.bronze_bucket_name}",
          "arn:aws:s3:::${var.bronze_bucket_name}/*",
          "arn:aws:s3:::${var.silver_bucket_name}",
          "arn:aws:s3:::${var.silver_bucket_name}/*",
          "arn:aws:s3:::${var.gold_bucket_name}",
          "arn:aws:s3:::${var.gold_bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.databricks_role.name
  policy_arn = aws_iam_policy.databricks_s3_policy.arn
}

output "databricks_role_arn" {
  value = aws_iam_role.databricks_role.arn
}