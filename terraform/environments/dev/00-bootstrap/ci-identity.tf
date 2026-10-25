resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  # Only tokens issued for THIS repository are accepted
  attribute_condition = "assertion.repository == \"${var.github_repo}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "gha_plan" {
  account_id   = "gha-tf-plan"
  display_name = "GitHub Actions: terraform plan (read-only)"
}

resource "google_service_account" "gha_deploy" {
  account_id   = "gha-deploy"
  display_name = "GitHub Actions: build image and run RayJob"
}

# Plan identity: any workflow run from this repo, pull requests included
resource "google_service_account_iam_member" "plan_wif" {
  service_account_id = google_service_account.gha_plan.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

# Deploy identity: only pushes to main
resource "google_service_account_iam_member" "deploy_wif" {
  service_account_id = google_service_account.gha_deploy.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/subject/repo:${var.github_repo}:ref:refs/heads/main"
}

# Plan SA: read-only access
resource "google_project_iam_member" "plan_read" {
  for_each = toset(["roles/viewer", "roles/iam.securityReviewer"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.gha_plan.email}"
}

resource "google_storage_bucket_iam_member" "plan_state" {
  bucket = var.state_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.gha_plan.email}"
}

# Deploy SA: Kubernetes objects on the cluster, and push images to our registry only
resource "google_project_iam_member" "deploy_gke" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = "serviceAccount:${google_service_account.gha_deploy.email}"
}

resource "google_artifact_registry_repository_iam_member" "deploy_push" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.images.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.gha_deploy.email}"
}

output "wif_provider" {
  value = google_iam_workload_identity_pool_provider.github.name
}

output "plan_sa" {
  value = google_service_account.gha_plan.email
}

output "deploy_sa" {
  value = google_service_account.gha_deploy.email
}