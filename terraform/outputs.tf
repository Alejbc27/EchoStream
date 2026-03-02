# outputs.tf
#
# WHY OUTPUTS?
# After `terraform apply`, outputs print useful values to your terminal.
# They're also used by other Terraform modules that depend on this one.
# Think of them as return values for your infrastructure.

output "raw_bucket_name" {
  description = "Name of the Raw layer GCS bucket"
  value       = google_storage_bucket.raw.name
}

output "bronze_bucket_name" {
  description = "Name of the Bronze layer GCS bucket"
  value       = google_storage_bucket.bronze.name
}

output "silver_bucket_name" {
  description = "Name of the Silver layer GCS bucket"
  value       = google_storage_bucket.silver.name
}

output "gold_bucket_name" {
  description = "Name of the Gold layer GCS bucket"
  value       = google_storage_bucket.gold.name
}

output "raw_bucket_url" {
  description = "gs:// URL for the Raw bucket — use this in Python code"
  value       = "gs://${google_storage_bucket.raw.name}"
}

# ── Cloud Run outputs ───────────────────────────────────────────────────────

output "artifact_registry_url" {
  description = "Docker registry URL — use this in docker push commands"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.echostream.repository_id}"
}

output "cloud_run_job_name" {
  description = "Name of the Cloud Run extraction job"
  value       = google_cloud_run_v2_job.extractor.name
}

output "extractor_service_account" {
  description = "Email of the service account used by the extractor job"
  value       = google_service_account.extractor.email
}
