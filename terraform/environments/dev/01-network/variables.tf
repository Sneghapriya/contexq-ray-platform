variable "project_id" {
  type        = string
  description = "GCP project ID (set via TF_VAR_project_id)"
}

variable "region" {
  type = string
}

variable "name" {
  type = string
}

variable "subnet_cidr" {
  type = string
}

variable "pods_cidr" {
  type = string
}

variable "services_cidr" {
  type = string
}