output "namespace" {
  value = module.kuberay.namespace
}

output "ray_service_account_email" {
  value = module.workload_identity.gsa_email
}