# providers.tf
#
# WHY THIS FILE EXISTS:
# Terraform needs to know which cloud provider plugins ("providers") to download.
# This file pins the exact versions so the team always uses the same binaries.
#
# WHAT HAPPENS WHEN YOU RUN `terraform init`:
# - Terraform reads this file and downloads the Google provider plugin
# - It writes a .terraform.lock.hcl file (commit that to git — it's like package-lock.json)
# - After init, `terraform plan` shows you WHAT WOULD change (no real changes yet)
# - Only `terraform apply` actually creates resources in GCP

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"  # "~> 5.0" means "5.x but not 6.x"
    }
  }

  # Remote state backend — stores terraform.tfstate in GCS instead of locally.
  # WHY: if state is only on your laptop and your laptop dies, Terraform
  # can no longer manage your infrastructure safely.
  # NOTE: The bucket must exist BEFORE running `terraform init` (chicken-and-egg).
  # See README for the one-time bootstrap command.
  backend "gcs" {
    bucket = "echostream-terraform-state"  # override with var in backend config file
    prefix = "terraform/state"
  }
}

# The Google provider — configures how Terraform talks to GCP.
# Project and region are read from variables (defined in variables.tf).
provider "google" {
  project = var.project_id
  region  = var.region
}
