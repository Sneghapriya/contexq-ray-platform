export PROJECT_ID=$(gcloud config get-value project)
export TF_VAR_project_id=$PROJECT_ID
export REGION=asia-south1
export GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)
export IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/ray-images/ray-pipeline:v2
export JOB_NAME=entity-resolution