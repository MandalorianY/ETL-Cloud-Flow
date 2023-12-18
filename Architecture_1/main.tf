provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.gcp_credentials_path)
}

## PubSub

module "pubsub_start" {
  source = "./modules/pubsub"
  region = var.region
  name   = "pubsub-start"
}



## Storage

module "storage_cloud_function" {
  source       = "./modules/cloud_storage"
  storage_name = "storage_cloud_function"
  region       = var.region

}

module "storage_dataproc_function" {
  source       = "./modules/cloud_storage"
  storage_name = "storage_dataproc-function"
  region       = var.region

}

resource "google_storage_bucket_object" "object" {
  name   = "jobs.py"
  bucket = module.storage_dataproc_function.bucket_name
  source = "./src/jobs/spark/jobs.py"
}

module "storage_crime_processed" {
  source       = "./modules/cloud_storage"
  storage_name = "crime_processed_data"
  region       = var.region

}

## Cloud Function

module "dataflow_function" {
  bucket_name            = module.storage_cloud_function.bucket_name
  function_name          = "dataflow-function"
  source                 = "./modules/cloud_function"
  output_dir             = "./tmp/function_dataflow/function.zip"
  source_dir             = "./src/function/src_dataflow"
  project_id             = var.project_id
  region                 = var.region
  google_pubsub_topic_id = module.pubsub_start.google_pubsub_topic_id
  entry_point            = "dataproc_activation"
  trigger_type           = "pubsub"
  trigger_bucket         = module.storage_crime_processed.bucket_name
}


module "upload_to_bigquery_function" {
  bucket_name            = module.storage_cloud_function.bucket_name
  function_name          = "upload-to-bigquery-function"
  source                 = "./modules/cloud_function"
  output_dir             = "./tmp/function_upload_to_bigquery/function.zip"
  source_dir             = "./src/function/src_upload_to_bigquery"
  project_id             = v.project_id
  region                 = var.region
  google_pubsub_topic_id = module.pubsub_start.google_pubsub_topic_id
  entry_point            = "upload_parquet_to_bigquery"
  trigger_type           = "storage"
  trigger_bucket         = module.storage_crime_processed.bucket_name
}


## DataProc template

module "dataproc_workflow_function" {
  source               = "./modules/dataproc_worflow_template"
  region               = "europe-west3"
  machine_type         = "n1-standard-4"
  main_python_file_uri = "gs://storage_dataproc-function/jobs.py"
}


## BigQuery


module "big_querry_dataset" {
  source = "./modules/big_dataset_table"
  region = var.region
  name   = "crime_dataset"
}
