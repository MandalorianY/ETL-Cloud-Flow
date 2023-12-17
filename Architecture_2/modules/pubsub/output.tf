output "google_pubsub_topic_id" {
  value       = google_pubsub_topic.topic.id
  description = "The ID of the Google Pub/Sub topic to trigger the Cloud Function"
}
