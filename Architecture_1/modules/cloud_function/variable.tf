variable "region" {
  description = "The GCP region to deploy resources into"
  type        = string
}

variable "source_dir" {
  description = "The directory containing the source code for the Cloud Function"
  type        = string
}

variable "output_dir" {
  description = "The directory to output the Cloud Function source code zip to"
  type        = string
}

variable "function_name" {
  description = "The name of the Cloud Function"
  type        = string

}
variable "bucket_name" {
  description = "The name of the bucket"
  type        = string
}

variable "project_id" {
  description = "The GCP project ID to deploy resources into"
  type        = string
}

variable "google_pubsub_topic_id" {
  description = "The ID of the Google Pub/Sub topic to trigger the Cloud Function"
  type        = string

}

variable "entry_point" {
  description = "The entry point for the Cloud Function"
  type        = string
}
variable "trigger_bucket" {
  description = "The name of the bucket"
  type        = string

}

variable "trigger_type" {
  description = "The type of trigger to use for the Cloud Function"
  type        = string
}
