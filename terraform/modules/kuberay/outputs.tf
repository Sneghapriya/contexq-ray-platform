output "namespace" {
  value = kubernetes_namespace.ray.metadata[0].name
}