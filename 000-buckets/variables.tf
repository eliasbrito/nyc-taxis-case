variable "region" {
  default = "us-east-2"
}

variable "tf_state_bucket_name" {
  description = "Bucket do Terraform state"
  default = "state-nyc-taxis-case"
}

variable "tf_lock_table_name" {
  default = "terraform-lock"
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