locals {
  name = "nyc-taxi-${var.environment}"

  common_tags = {
    Project     = "nyc-taxi"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

module "networking" {
  source = "./modules/networking"

  name                 = local.name
  vpc_cidr             = var.vpc_cidr
  azs                  = var.azs
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  tags                 = local.common_tags
}

module "iam" {
  source = "./modules/iam"

  name = local.name
  tags = local.common_tags
}

module "eks" {
  source = "./modules/eks"

  cluster_name        = local.name
  kubernetes_version  = var.kubernetes_version
  cluster_role_arn    = module.iam.cluster_role_arn
  node_role_arn       = module.iam.node_role_arn
  cluster_subnet_ids  = concat(module.networking.public_subnet_ids, module.networking.private_subnet_ids)
  node_subnet_ids     = module.networking.private_subnet_ids
  node_instance_types = var.node_instance_types
  node_desired_size   = var.node_desired_size
  node_min_size       = var.node_min_size
  node_max_size       = var.node_max_size
  node_capacity_type  = var.node_capacity_type
  tags                = local.common_tags
}
