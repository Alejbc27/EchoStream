# variables.tf
#
# WHY VARIABLES?
# Hard-coding values like project IDs and bucket names directly in resource
# blocks makes code non-reusable. Variables let you:
# - Use the same Terraform code for dev and prod (different var values)
# - Keep secrets out of source code (sensitive = true)
# - See at a glance what's configurable without reading every resource block
#
# HOW TO PROVIDE VALUES:
# Option 1 (recommended): Create a file called `terraform.tfvars` (gitignored):
#   project_id = "my-gcp-project-id"
#   region     = "europe-west1"
#
# Option 2: Pass on the CLI:
#   terraform apply -var="project_id=my-gcp-project-id"

variable "project_id" {
  description = "GCP project ID — find this in your GCP console under 'Project info'"
  type        = string
  # No default: Terraform will ask you interactively if not provided
}

variable "region" {
  description = "GCP region for all resources (e.g. europe-west1 for Spain/EU)"
  type        = string
  default     = "europe-west1"
  # europe-west1 = Belgium — low latency from Spain, data stays in EU (GDPR-friendly)
}

variable "environment" {
  description = "Deployment environment label (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "tf_state_bucket" {
  description = "Name of the GCS bucket that stores Terraform state (must be created manually first)"
  type        = string
  default     = "echostream-terraform-state"
}
