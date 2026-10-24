output "cluster_name" {
  value = module.gke.cluster_name
}

output "cluster_location" {
  value = module.gke.cluster_location
}

output "workload_pool" {
  value = module.gke.workload_pool
}

output "node_service_account" {
  value = module.gke.node_service_account
}