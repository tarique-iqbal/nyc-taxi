bucket         = "nyc-taxi-terraform-state"
key            = "staging/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "nyc-taxi-terraform-locks"
encrypt        = true
