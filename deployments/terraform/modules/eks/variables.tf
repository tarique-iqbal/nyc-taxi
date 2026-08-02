variable "cluster_name" {
  description = "EKS cluster name, e.g. nyc-taxi-dev."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.29"
}

variable "cluster_role_arn" {
  description = "IAM role ARN for the EKS control plane (from the iam module)."
  type        = string
}

variable "node_role_arn" {
  description = "IAM role ARN for EKS worker nodes (from the iam module)."
  type        = string
}

variable "cluster_subnet_ids" {
  description = "Subnet IDs for the EKS control plane's ENIs (public + private)."
  type        = list(string)
}

variable "node_subnet_ids" {
  description = "Subnet IDs for worker nodes -- private subnets only."
  type        = list(string)
}

variable "node_instance_types" {
  description = "EC2 instance types for the managed node group."
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 5
}

variable "node_capacity_type" {
  description = "ON_DEMAND or SPOT. SPOT is fine for a portfolio cluster's node pool."
  type        = string
  default     = "SPOT"
}

variable "tags" {
  type    = map(string)
  default = {}
}
