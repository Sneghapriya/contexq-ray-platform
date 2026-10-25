resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = var.namespace
  }
}

resource "random_password" "grafana" {
  length  = 20
  special = false
}

resource "helm_release" "stack" {
  name       = "kube-prometheus-stack"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = var.chart_version
  namespace  = kubernetes_namespace.monitoring.metadata[0].name
  timeout    = 900

  values = [yamlencode({
    defaultRules = { create = false }
    alertmanager = { enabled = false }
    # GKE manages its control plane, so these cannot be scraped
    kubeControllerManager = { enabled = false }
    kubeScheduler         = { enabled = false }
    kubeEtcd              = { enabled = false }
    kubeProxy             = { enabled = false }

    prometheus = {
      prometheusSpec = {
        retention = "24h" # no disk, so nothing left behind after destroy
        # Pick up PodMonitors from any namespace, not only ones with the chart's label
        podMonitorSelectorNilUsesHelmValues     = false
        serviceMonitorSelectorNilUsesHelmValues = false
        resources = {
          requests = { cpu = "200m", memory = "1Gi" }
          limits   = { memory = "1536Mi" }
        }
      }
    }
    grafana = {
      resources = {
        requests = { cpu = "100m", memory = "256Mi" }
        limits   = { memory = "512Mi" }
      }
    }
    prometheusOperator = {
      resources = {
        requests = { cpu = "100m", memory = "128Mi" }
        limits   = { memory = "256Mi" }
      }
    }
    "kube-state-metrics" = {
      resources = {
        requests = { cpu = "50m", memory = "64Mi" }
        limits   = { memory = "128Mi" }
      }
    }
    "prometheus-node-exporter" = {
      resources = {
        requests = { cpu = "50m", memory = "32Mi" }
        limits   = { memory = "64Mi" }
      }
    }
  })]

  set_sensitive {
    name  = "grafana.adminPassword"
    value = random_password.grafana.result
  }
}