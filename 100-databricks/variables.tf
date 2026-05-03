variable "databricks_host" {
    default = "https://dbc-0a068d4f-2beb.cloud.databricks.com"
}
variable "databricks_token" {
    default = "dapi478f39808d4aa569c9ee77ad13b57018" # está aqui para fins didáticos
}

variable "bronze_bucket_name" {
  default = "bronze-nyc-taxis-case"
}
variable "silver_bucket_name" {
  default = "silver-nyc-taxis-case"
}
variable "gold_bucket_name" {
  default = "gold-nyc-taxis-case"
}

variable "aws_access_key" {
  description = "AWS Access Key"
  type        = string
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS Secret Key"
  type        = string
  sensitive   = true
}