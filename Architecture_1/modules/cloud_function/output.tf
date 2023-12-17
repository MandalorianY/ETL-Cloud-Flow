output "cloud_function_url" {
  value       = google_cloudfunctions2_function.function.url
  description = "The URL of the deployed Cloud Function"
}

output "cloud_function_name" {
  value       = google_cloudfunctions2_function.function.name
  description = "The name of the deployed Cloud Function"
}
