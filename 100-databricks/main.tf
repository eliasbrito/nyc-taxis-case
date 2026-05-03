resource "databricks_schema" "silver" {
  name         = "silver-nyc-taxi-case"
  catalog_name = "workspace"
  comment      = "Camada Silver - dados tratados"
}

resource "databricks_schema" "gold" {
  name         = "gold-nyc-taxi-case"
  catalog_name = "workspace"
  comment      = "Camada Gold - dados analíticos"
}

resource "databricks_storage_credential" "s3_credential" {
  name = "nyc_taxi_credential"

  aws_iam_role {
    role_arn = "arn:aws:iam::080105255163:role/databricks-s3-access-role"
  }

  comment = "Credencial para acesso ao S3 (NYC Taxi)"
}

resource "databricks_external_location" "bronze" {
  name            = "bronze_location"
  url             = "s3://${var.bronze_bucket_name}"
  credential_name = databricks_storage_credential.s3_credential.name
  comment         = "External location bronze"
}


resource "databricks_external_location" "silver" {
  name            = "silver_location"
  url             = "s3://${var.silver_bucket_name}"
  credential_name = databricks_storage_credential.s3_credential.name
  comment         = "External location silver"
}

resource "databricks_external_location" "gold" {
  name            = "gold_location"
  url             = "s3://${var.gold_bucket_name}"
  credential_name = databricks_storage_credential.s3_credential.name
  comment         = "External location gold"
}

########################################
# SECRET SCOPE (AWS CREDENTIALS)
########################################

resource "databricks_secret_scope" "aws" {
  name = "aws-secrets"
}

resource "databricks_secret" "access_key" {
  key          = "access_key"
  string_value = var.aws_access_key
  scope        = databricks_secret_scope.aws.name
}

resource "databricks_secret" "secret_key" {
  key          = "secret_key"
  string_value = var.aws_secret_key
  scope        = databricks_secret_scope.aws.name
}
