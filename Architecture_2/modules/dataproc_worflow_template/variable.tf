variable "region" {
  description = "The GCP region to deploy resources into"
  type        = string
}

variable "machine_type" {
  description = "The machine type to use for the Dataproc cluster"
  type        = string
}


variable "main_python_file_uri" {
  description = "The URI of the main Python file to use for the Dataproc cluster"
  type        = string
}


variable "name" {
  description = "The name of the Dataproc workflow template"
  type        = string

}
