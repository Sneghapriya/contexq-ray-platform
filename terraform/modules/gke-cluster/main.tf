resource "google_service_account" "nodes" {
  account_id   = "${var.name}-nodes"
  display_name = "GKE node service account (${var.name})"
}

resource "google_project_iam_member" "nodes" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/artifactregistry.reader",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.nodes.email}"
}

resource "google_container_cluster" "this" {
  name       = "${var.name}-gke"
  location   = var.zone
  network    = var.network_id
  subnetwork = var.subnet_id

  # We manage our own node pools, so drop the default one
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false # demo only

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = var.pods_range_name
    services_secondary_range_name = var.services_range_name
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = var.master_cidr
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.authorized_cidrs
      content {
        cidr_block   = cidr_blocks.value.cidr
        display_name = cidr_blocks.value.name
      }
    }
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }
}

# Head, KubeRay operator and monitoring run here (stable, on-demand)
resource "google_container_node_pool" "system" {
  name       = "system"
  cluster    = google_container_cluster.this.name
  location   = var.zone
  node_count = var.system_node_count

  node_config {
    machine_type    = var.system_machine_type
    disk_size_gb    = var.disk_size_gb
    disk_type       = "pd-balanced"
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { pool = "system" }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Ray workers: Spot, autoscaling, tainted so only Ray worker pods land here
resource "google_container_node_pool" "workers" {
  name               = "ray-workers"
  cluster            = google_container_cluster.this.name
  location           = var.zone
  initial_node_count = var.worker_min_nodes

  autoscaling {
    min_node_count = var.worker_min_nodes
    max_node_count = var.worker_max_nodes
  }

  node_config {
    spot            = true
    machine_type    = var.worker_machine_type
    disk_size_gb    = var.disk_size_gb
    disk_type       = "pd-balanced"
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { pool = "ray-worker" }

    taint {
      key    = "ray-worker"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}