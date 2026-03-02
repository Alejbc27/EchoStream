# cloud_run.tf
#
# Cloud Run Job + Cloud Scheduler for automated Spotify extraction.
#
# ARCHITECTURE:
#   Cloud Scheduler (cron: every 2h)
#       → invokes Cloud Run Job via HTTP
#           → runs the Docker container (src/echostream/main.py)
#               → extracts recent + top tracks → saves to GCS Raw
#
# WHY Cloud Run Job (not Cloud Run Service)?
#   - A "Service" stays running and waits for HTTP requests (web server).
#   - A "Job" runs once and exits — perfect for batch extraction.
#   - Jobs don't cost money when idle (pay only per execution).
#   - Retry logic is built-in: if the job fails, Cloud Run retries automatically.
#
# WHY Cloud Scheduler (not Cloud Functions + Pub/Sub)?
#   - Simpler: one cron expression, one target. No Pub/Sub topic to manage.
#   - Cloud Scheduler can invoke Cloud Run Jobs directly via the API.
#   - Easier to debug: you can see the schedule and last execution in the console.

# ── Enable required GCP APIs ────────────────────────────────────────────────
# These APIs must be enabled before Terraform can create the resources.
# Terraform handles this automatically — no manual console clicks needed.

resource "google_project_service" "cloud_run" {
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloud_scheduler" {
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifact_registry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secret_manager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

# ── Artifact Registry ───────────────────────────────────────────────────────
# Docker images must be stored in Artifact Registry (GCR is deprecated).
# This is where `docker push` sends the EchoStream container image.

resource "google_artifact_registry_repository" "echostream" {
  location      = var.region
  repository_id = "echostream"
  format        = "DOCKER"
  description   = "Docker images for EchoStream Cloud Run jobs"

  depends_on = [google_project_service.artifact_registry]
}

# ── Service Account ─────────────────────────────────────────────────────────
# The Cloud Run Job runs as this service account.
# Principle of least privilege: it can ONLY write to the Raw GCS bucket
# and read Spotify secrets — nothing else.

resource "google_service_account" "extractor" {
  account_id   = "echostream-extractor"
  display_name = "EchoStream Spotify Extractor"
  description  = "Service account for Cloud Run extraction job"
}

# Grant write access to the Raw GCS bucket only
resource "google_storage_bucket_iam_member" "extractor_raw_writer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.extractor.email}"
}

# ── Secret Manager (Spotify credentials) ────────────────────────────────────
# Secrets are stored in Secret Manager, NOT in environment variables.
# Cloud Run mounts them as env vars at runtime — they never appear in
# Terraform state, Docker images, or Cloud Logging.

resource "google_secret_manager_secret" "spotify_client_id" {
  secret_id = "spotify-client-id"

  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}

resource "google_secret_manager_secret" "spotify_client_secret" {
  secret_id = "spotify-client-secret"

  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}

resource "google_secret_manager_secret" "spotify_cache_token" {
  secret_id = "spotify-cache-token"

  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}

# Grant the extractor service account access to read these secrets
resource "google_secret_manager_secret_iam_member" "extractor_reads_client_id" {
  secret_id = google_secret_manager_secret.spotify_client_id.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.extractor.email}"
}

resource "google_secret_manager_secret_iam_member" "extractor_reads_client_secret" {
  secret_id = google_secret_manager_secret.spotify_client_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.extractor.email}"
}

resource "google_secret_manager_secret_iam_member" "extractor_reads_cache_token" {
  secret_id = google_secret_manager_secret.spotify_cache_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.extractor.email}"
}

# ── Cloud Run Job ───────────────────────────────────────────────────────────
# The actual job definition. Uses the Docker image from Artifact Registry.
# Environment variables point to GCS bucket; secrets come from Secret Manager.

resource "google_cloud_run_v2_job" "extractor" {
  name     = "echostream-extractor"
  location = var.region

  template {
    template {
      service_account = google_service_account.extractor.email

      containers {
        # Image tag — you update this after each `docker push`
        # Format: <region>-docker.pkg.dev/<project>/<repo>/<image>:<tag>
        image = "${var.region}-docker.pkg.dev/${var.project_id}/echostream/extractor:latest"

        resources {
          limits = {
            # Extraction is lightweight: small HTTP calls + small JSON uploads.
            # 512Mi RAM and 1 CPU is more than enough.
            cpu    = "1"
            memory = "512Mi"
          }
        }

        # Non-secret environment variables
        env {
          name  = "GCS_RAW_BUCKET"
          value = google_storage_bucket.raw.name
        }

        env {
          name  = "SPOTIFY_OPEN_BROWSER"
          value = "false"
        }

        env {
          name  = "SPOTIFY_CACHE_PATH"
          value = "/tmp/.spotify_cache"
        }

        env {
          name  = "SPOTIFY_REDIRECT_URI"
          value = "http://localhost:8888/callback"
        }

        # Secrets mounted as environment variables at runtime
        env {
          name = "SPOTIFY_CLIENT_ID"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.spotify_client_id.secret_id
              version = "latest"
            }
          }
        }

        env {
          name = "SPOTIFY_CLIENT_SECRET"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.spotify_client_secret.secret_id
              version = "latest"
            }
          }
        }
      }

      # Max execution time: 5 minutes. Our extraction takes ~10 seconds,
      # but we leave headroom for Spotify API slowness or GCS retries.
      timeout = "300s"

      # Retry up to 3 times if the job fails (e.g. transient network error)
      max_retries = 3
    }
  }

  depends_on = [
    google_project_service.cloud_run,
    google_artifact_registry_repository.echostream,
  ]

  lifecycle {
    # Don't destroy and recreate the job when only the image tag changes.
    # We update the image via `gcloud run jobs update` after each deploy.
    ignore_changes = [template[0].template[0].containers[0].image]
  }
}

# ── Cloud Scheduler ─────────────────────────────────────────────────────────
# Triggers the Cloud Run Job every 2 hours.
# Cron: "0 */2 * * *" = at minute 0, every 2nd hour, every day.

resource "google_service_account" "scheduler_invoker" {
  account_id   = "echostream-scheduler"
  display_name = "EchoStream Scheduler Invoker"
  description  = "Service account for Cloud Scheduler to invoke Cloud Run jobs"
}

# Grant the scheduler permission to invoke (run) the Cloud Run Job
resource "google_cloud_run_v2_job_iam_member" "scheduler_can_invoke" {
  name     = google_cloud_run_v2_job.extractor.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}

resource "google_cloud_scheduler_job" "extractor_trigger" {
  name        = "echostream-extractor-trigger"
  description = "Triggers Spotify extraction every 2 hours"
  schedule    = "0 */2 * * *"
  time_zone   = "Europe/Madrid"
  region      = var.region

  retry_config {
    retry_count = 1
  }

  http_target {
    # Cloud Scheduler invokes the Cloud Run Job via its execution API
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.extractor.name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler_invoker.email
    }
  }

  depends_on = [
    google_project_service.cloud_scheduler,
    google_cloud_run_v2_job_iam_member.scheduler_can_invoke,
  ]
}
