terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  backend "gcs" {
    prefix = "00-bootstrap" # bucket is passed at init time
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Data bucket: raw CSVs and the Iceberg warehouse
resource "google_storage_bucket" "data" {
  name                        = "ray-platform-data"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true # demo project only

  versioning {
    enabled = true
  }
}

# Registry for the pipeline image
resource "google_artifact_registry_repository" "images" {
  repository_id = "ray-images"
  location      = var.region
  format        = "DOCKER"
}