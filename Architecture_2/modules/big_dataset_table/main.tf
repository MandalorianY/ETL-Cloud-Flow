resource "google_bigquery_dataset" "default" {
  dataset_id                  = var.name
  friendly_name               = var.name
  description                 = "Crime dataset"
  location                    = var.region
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}
