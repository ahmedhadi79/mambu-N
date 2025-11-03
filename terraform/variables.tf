# Global vars
variable "bespoke_account" {
  description = "bespoke account to deploy (sandbox, nfrt, alpha, beta, production)"
  type        = string
}

variable "resource_management_iam_role" {
  description = "Name of the role TF uses to manage resources in AWS accounts."
  type        = string
}

variable "external_id" {
  description = "External identifier to use when assuming the role."
  type        = string
}

variable "aws_account_id" {
  type        = string
  description = "AWS Account ID which may be operated on by this template"
}

variable "project_url" {
  description = "URL of the gitlab project that owns the resources"
  default     = "http://localhost"
  type        = string
}

variable "region" {
  type        = string
  default     = "eu-west-2"
  description = "AWS Region the S3 bucket should reside in"
}

variable "mambu_password_secret_name" {
  type        = string
  description = "Name of the secret in AWS Secrets Manager that contains the Mambu password"
  default     = "sls/etl/mambuAuthDetails"
}

variable "mambu_subdomain" {
  type        = string
  description = "Mambu subdomain"
}

variable "mambu_user_agent" {
  type        = string
  description = "User agent to use when calling Mambu API"
  default     = "tap-mambu andreas.adamides@bb2.tech"
}

variable "mambu_api_events" {
  default = {}
}

variable "mambu_stream_events" {
  default = {}
}
