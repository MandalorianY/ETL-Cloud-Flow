variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources into"
  type        = string
}

variable "gcp_credentials_path" {
  description = "Path to the GCP credentials JSON file"
  type        = string
}
