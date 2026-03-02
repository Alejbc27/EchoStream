# storage.tf
#
# WHY THIS FILE?
# This defines the GCS (Google Cloud Storage) buckets that form our
# Medallion architecture:
#
#   Raw  → exact copy of Spotify API responses (JSON) — never modified
#   Bronze → cleaned, partitioned Parquet files
#   Silver → deduplicated, enriched data
#   Gold   → aggregated tables ready for dashboards
#
# WHY MEDALLION?
# Each layer is append-only and independently queryable.
# If a bug corrupts Bronze, you re-process from Raw without re-calling the API.
# If Silver has a schema problem, re-process from Bronze. You never lose data.
#
# LIFECYCLE RULES:
# Raw data is kept indefinitely (it's your source of truth).
# Processed layers could have lifecycle rules to delete old versions, but
# for now we keep everything — storage is cheap at this scale.

# ── Raw layer ────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-echostream-raw-${var.environment}"
  location = var.region

  # Uniform bucket-level access: uses IAM only (no per-object ACLs).
  # This is the modern, recommended approach — simpler and more auditable.
  uniform_bucket_level_access = true

  # Versioning: keeps old versions of objects when overwritten.
  # Useful as a safety net during early development.
  versioning {
    enabled = true
  }

  force_destroy = var.environment == "dev" ? true : false
  # force_destroy = true means `terraform destroy` can delete a non-empty bucket.
  # Safe for dev, but you'd set this to false in production.
}

# ── Bronze layer ──────────────────────────────────────────────────────────────
resource "google_storage_bucket" "bronze" {
  name                        = "${var.project_id}-echostream-bronze-${var.environment}"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  force_destroy = var.environment == "dev" ? true : false
}

# ── Silver layer ──────────────────────────────────────────────────────────────
resource "google_storage_bucket" "silver" {
  name                        = "${var.project_id}-echostream-silver-${var.environment}"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  force_destroy = var.environment == "dev" ? true : false
}

# ── Gold layer ────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "gold" {
  name                        = "${var.project_id}-echostream-gold-${var.environment}"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  force_destroy = var.environment == "dev" ? true : false
}
