# The Google identity Ray pods will act as
resource "google_service_account" "ray" {
  account_id   = "ray-workload"
  display_name = "Ray workload identity"
}

# Least privilege: read/write on the data bucket only
resource "google_storage_bucket_iam_member" "data" {
  bucket = var.data_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ray.email}"
}

# The Kubernetes service account the pods use
resource "kubernetes_service_account" "ray" {
  metadata {
    name      = var.ksa_name
    namespace = var.namespace
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.ray.email
    }
  }
}

# Allow that Kubernetes account to impersonate the Google one
resource "google_service_account_iam_member" "wi" {
  service_account_id = google_service_account.ray.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.namespace}/${var.ksa_name}]"
}