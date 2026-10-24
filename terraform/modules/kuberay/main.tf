resource "kubernetes_namespace" "ray" {
  metadata {
    name = var.namespace
  }
}

resource "helm_release" "kuberay_operator" {
  name       = "kuberay-operator"
  repository = "https://ray-project.github.io/kuberay-helm/"
  chart      = "kuberay-operator"
  version    = var.version_
  namespace  = kubernetes_namespace.ray.metadata[0].name

  # Keep the operator on the stable system pool, never on Spot workers
  values = [yamlencode({
    nodeSelector = { pool = "system" }
  })]
}