variable "name" {
  type        = string
  description = "Prefix for network resources"
}

variable "region" {
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