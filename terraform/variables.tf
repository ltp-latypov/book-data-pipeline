variable "project" {
  description = "Google Cloud project ID"
  default     = "books-pipeline-491409"
}

variable "region" {
  description = "Compute region for resources"
  default     = "europe-west10"
}

variable "location" {
  description = "Location for data resources (GCS, BigQuery)"
  default     = "europe-west10"
}

variable "bq_dataset_cleaned" {
  description = "BigQuery Dataset for Books, Users, Ratings after PySpark cleaning"
  default     = "books_cleaned"
}


variable "bq_dataset_analytics" {
  description = "BigQuery Dataset for dbt modeling (Books, Users, Ratings)"
  default     = "books_analytics"
}


variable "gcs_bucket_name" {
  description = "Storage Bucket for Books Data"
  default     = "kestra-books-bucket-latypov"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My Google Cloud Account credentials"
  default     = "../service-account.json"
}


variable "kaggle_username" {
  description = "Kaggle API Username"
  sensitive   = true
}

variable "kaggle_key" {
  description = "Kaggle API Key"
  sensitive   = true
}