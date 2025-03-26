variable "aws_region" {
  default = "eu-north-1"
}

variable "glue_database_name" {
  default = "retailinsights_catalog"
}

variable "s3_base_path" {
  default = "s3://retail-insights-bucket/extracted_data"
}
