output "cluster_name" {
  value = google_container_cluster.this.name
}

output "cluster_location" {
  value = google_container_cluster.this.location
}

output "workload_pool" {
  value = "${var.project_id}.svc.id.goog"
}

output "node_service_account" {
  value = google_service_account.nodes.email
}