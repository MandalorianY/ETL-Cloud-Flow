resource "google_dataproc_workflow_template" "template" {
  name     = var.name
  location = var.region
  placement {
    managed_cluster {
      cluster_name = "crime-cluster-bigquery"
      config {
        gce_cluster_config {
          zone            = "${var.region}-b"
          tags            = ["foo", "crime"]
          service_account = "terraform@nodale.iam.gserviceaccount.com"
        }
        master_config {
          num_instances = 1
          machine_type  = var.machine_type
          disk_config {
            boot_disk_type    = "pd-ssd"
            boot_disk_size_gb = 50
          }
        }
        worker_config {
          num_instances = 2
          machine_type  = var.machine_type
          disk_config {
            boot_disk_size_gb = 50
            num_local_ssds    = 2
          }
        }
        software_config {
          image_version = "2.0.35-debian10"
          properties = {
            "spark:spark.jars.packages" = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0"
          }
        }
      }
    }
  }
  jobs {
    step_id = "process-crime-data-architecture-2"
    pyspark_job {
      main_python_file_uri = var.main_python_file_uri
      properties = {
        "spark.logConf" = "true"
      }
    }
  }

}
