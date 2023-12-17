resource "google_storage_bucket" "function_bucket" {
  name     = var.storage_name
  location = var.region
}

