terraform {
  backend "s3" {
    bucket         = "state-nyc-taxis-case"
    key            = "global/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "terraform-lock"
  }
}