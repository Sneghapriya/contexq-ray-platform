variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
}

variable "name" {
  type = string
}

variable "master_cidr" {
  type = string
}

variable "authorized_cidrs" {
  type = list(object({
    cidr = string
    name = string
  }))
}

variable "system_machine_type" {
  type = string
}

variable "system_node_count" {
  type = number
}

variable "worker_machine_type" {
  type = string
}

variable "worker_min_nodes" {
  type = number
}

variable "worker_max_nodes" {
  type = number
}

variable "disk_size_gb" {
  type = number
}