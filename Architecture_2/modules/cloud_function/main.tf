# Generates an archive of the source code compressed as a .zip file.
data "archive_file" "source" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = var.output_dir

}

# Add source code zip to the Cloud Function's bucket
resource "google_storage_bucket_object" "zip" {
  source       = data.archive_file.source.output_path
  content_type = "application/zip"

  # Append to the MD5 checksum of the files's content
  # to force the zip to be updated as soon as a change occurs
  name   = "src-${data.archive_file.source.output_md5}.zip"
  bucket = var.bucket_name

}

resource "google_cloudfunctions2_function" "function" {
  name        = var.function_name
  location    = var.region
  description = "Triggered by a HTTP event"
  build_config {
    runtime     = "python311"
    entry_point = var.entry_point
    source {
      storage_source {
        bucket = var.bucket_name
        object = google_storage_bucket_object.zip.name
      }
    }
  }
  dynamic "event_trigger" {
    for_each = var.trigger_type == "pubsub" ? [1] : []
    content {
      trigger_region = var.region
      event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
      pubsub_topic   = var.google_pubsub_topic_id
      retry_policy   = "RETRY_POLICY_RETRY"
    }
  }

  dynamic "event_trigger" {
    for_each = var.trigger_type == "storage" ? [1] : []
    content {
      trigger_region = var.region
      event_type     = "google.cloud.storage.object.v1.finalized"
      retry_policy   = "RETRY_POLICY_RETRY"
      event_filters {
        attribute = "bucket"
        value     = var.trigger_bucket
      }
    }
  }

}

