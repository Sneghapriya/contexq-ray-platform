variable "project_id" {
  type = string
}

variable "name" {
  type = string
}

variable "zone" {
  type = string
}

variable "network_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "pods_range_name" {
  type = string
}

variable "services_range_name" {
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