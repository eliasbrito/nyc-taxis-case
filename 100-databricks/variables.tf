variable "databricks_host" {
    default = <sua_url>
}
variable "databricks_token" {
    default = <seu_token> # está aqui para fins didáticos
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