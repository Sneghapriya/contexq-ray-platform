terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  backend "gcs" {
    prefix = "dev/01-network" # bucket is passed at init time
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "network" {
  source        = "../../../modules/network"
  name          = var.name
  region        = var.region
  subnet_cidr   = var.subnet_cidr
  pods_cidr     = var.pods_cidr
  services_cidr = var.services_cidr
}