terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.17"
    }
  }

  backend "gcs" {
    prefix = "dev/03-platform" # bucket is passed at init time
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Read the cluster layer's outputs
data "terraform_remote_state" "cluster" {
  backend = "gcs"
  config = {
    bucket = "ray-platform-tfstate"
    prefix = "dev/02-cluster"
  }
}

data "google_client_config" "default" {}

data "google_container_cluster" "this" {
  name     = data.terraform_remote_state.cluster.outputs.cluster_name
  location = data.terraform_remote_state.cluster.outputs.cluster_location
}

provider "kubernetes" {
  host                   = "https://${data.google_container_cluster.this.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(data.google_container_cluster.this.master_auth[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${data.google_container_cluster.this.endpoint}"
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(data.google_container_cluster.this.master_auth[0].cluster_ca_certificate)
  }
}

module "kuberay" {
  source = "../../../modules/kuberay"

  namespace = var.namespace
  version_  = var.kuberay_version
}

module "workload_identity" {
  source = "../../../modules/workload-identity"

  project_id  = var.project_id
  namespace   = module.kuberay.namespace
  ksa_name    = var.ksa_name
  data_bucket = "ray-platform-data"
}