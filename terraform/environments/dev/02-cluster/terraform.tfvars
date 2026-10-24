region              = "asia-south1"
zone                = "asia-south1-a"
name                = "ray-dev"
master_cidr         = "172.16.0.0/28"
system_machine_type = "e2-standard-4"
system_node_count   = 1
worker_machine_type = "e2-standard-4"
worker_min_nodes    = 0
worker_max_nodes    = 2
disk_size_gb        = 50

# DEV ONLY: open to all so Codespaces and CI can reach the API server.
# Production: restrict to known ranges or use a bastion/connect gateway.
authorized_cidrs = [
  { cidr = "0.0.0.0/0", name = "dev-open" }
]