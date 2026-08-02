bucket         = "nyc-taxi-terraform-state"
key            = "dev/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "nyc-taxi-terraform-locks"
encrypt        = true
