output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "kubeconfig_command" {
  value = module.eks.kubeconfig_command
}

output "vpc_id" {
  value = module.networking.vpc_id
}

output "alb_controller_role_arn" {
  value = module.eks.alb_controller_role_arn
}
