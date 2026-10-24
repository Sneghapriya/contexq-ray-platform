terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  backend "gcs" {
    prefix = "dev/02-cluster" # bucket is passed at init time
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Read the network layer's published outputs
data "terraform_remote_state" "network" {
  backend = "gcs"
  config = {
    bucket = "ray-platform-tfstate"
    prefix = "dev/01-network"
  }
}

module "gke" {
  source = "../../../modules/gke-cluster"

  project_id          = var.project_id
  name                = var.name
  zone                = var.zone
  network_id          = data.terraform_remote_state.network.outputs.network_id
  subnet_id           = data.terraform_remote_state.network.outputs.subnet_id
  pods_range_name     = data.terraform_remote_state.network.outputs.pods_range_name
  services_range_name = data.terraform_remote_state.network.outputs.services_range_name
  master_cidr         = var.master_cidr
  authorized_cidrs    = var.authorized_cidrs

  system_machine_type = var.system_machine_type
  system_node_count   = var.system_node_count
  worker_machine_type = var.worker_machine_type
  worker_min_nodes    = var.worker_min_nodes
  worker_max_nodes    = var.worker_max_nodes
  disk_size_gb        = var.disk_size_gb
}