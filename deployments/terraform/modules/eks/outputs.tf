output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  value = aws_eks_cluster.this.certificate_authority[0].data
}

output "oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.eks.arn
}

output "kubeconfig_command" {
  description = "Run this to configure kubectl against the new cluster."
  value       = "aws eks update-kubeconfig --name ${aws_eks_cluster.this.name} --region ${data.aws_region.current.name}"
}
