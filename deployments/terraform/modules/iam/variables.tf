variable "name" {
  description = "Name prefix for IAM resources, e.g. nyc-taxi-dev."
  type        = string
}

variable "tags" {
  description = "Tags applied to all IAM resources."
  type        = map(string)
  default     = {}
}
