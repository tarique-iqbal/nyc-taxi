# Partial backend config -- backend blocks can't reference variables, so the
# bucket/key/table are supplied per environment at init time:
#
#   terraform init -backend-config=environments/dev/backend.hcl
#
# This keeps dev/staging/prod state files completely separate (different S3
# keys, same bucket), per docs/deployment.md's environments-as-separate-state
# guidance -- a plan/apply against one environment can't touch another's state.
terraform {
  backend "s3" {}
}
