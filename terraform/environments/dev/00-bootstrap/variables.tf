variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "asia-south1"
  description = "GCP region"
}

variable "github_repo" {
  type        = string
  description = "GitHub repository as owner/name"
}

variable "state_bucket" {
  type = string
}