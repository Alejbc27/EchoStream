# backend.tf
#
# WHY A SEPARATE BACKEND FILE?
# We separate backend config from providers.tf so you can have
# different backend configs per environment without touching provider settings.
#
# THE CHICKEN-AND-EGG PROBLEM:
# Terraform stores its state in a GCS bucket — but you need Terraform to create
# GCS buckets. So for the state bucket only, we do a ONE-TIME manual step:
#
#   gcloud storage buckets create gs://echostream-terraform-state \
#     --project=YOUR_PROJECT_ID \
#     --location=europe-west1 \
#     --uniform-bucket-level-access
#
# After that bucket exists, run `terraform init` and Terraform will store
# all future state there automatically.
#
# NOTE: The actual backend block lives in providers.tf.
# This file documents the bootstrap process and holds any backend-related locals.

locals {
  # Centralize names so they're consistent across all resources
  state_bucket_name = "echostream-terraform-state"
  state_prefix      = "terraform/state"
}
