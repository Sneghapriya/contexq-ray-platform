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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
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

resource "random_password" "catalog" {
  length  = 24
  special = false # keeps the connection URI simple
}

resource "kubernetes_secret" "iceberg_catalog" {
  metadata {
    name      = "iceberg-catalog"
    namespace = module.kuberay.namespace
  }
  data = {
    POSTGRES_USER     = "iceberg"
    POSTGRES_DB       = "iceberg"
    POSTGRES_PASSWORD = random_password.catalog.result
  }
}

resource "google_secret_manager_secret" "llm_api_key" {
  secret_id = "llm-api-key"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "ray_reads_llm_key" {
  secret_id = google_secret_manager_secret.llm_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${module.workload_identity.gsa_email}"
}

module "monitoring" {
  source        = "../../../modules/monitoring"
  namespace     = var.monitoring_namespace
  chart_version = var.monitoring_chart_version
}