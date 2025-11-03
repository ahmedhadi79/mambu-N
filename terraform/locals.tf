locals {
  project_name                            = "mambu-integration"
  prefix                                  = "datalake-${local.project_name}"
  lambda_layer_aws_wrangler_arn           = "arn:aws:lambda:${var.region}:336392948345:layer:AWSSDKPandas-Python312:8"
  lambda_layer_aws_wrangler_python310_arn = "arn:aws:lambda:${var.region}:336392948345:layer:AWSSDKPandas-Python310:3"
  raw_datalake_bucket_name                = "bb2-${var.bespoke_account}-datalake-raw"
  meta_datalake_bucket_name               = "bb2-${var.bespoke_account}-datalake-meta"
  reconciliation_datalake_bucket_name     = "bb2-${var.bespoke_account}-datalake-reconciliation"
  athena_results_bucket_name              = "bb2-${var.bespoke_account}-datalake-athena-results"
  glue_assets_bucket_name                 = "aws-glue-assets-${var.aws_account_id}-${var.region}"

  lambda_mambu_env_vars = {
    S3_RAW              = local.raw_datalake_bucket_name,
    S3_META             = local.meta_datalake_bucket_name,
    MAMBU_SUBDOMAIN     = var.mambu_subdomain,
    MAMBU_USERNAME      = "blme_s3_exports",
    MAMBU_USER_AGENT    = var.mambu_user_agent,
    MAMBU_PASSWORD_NAME = var.mambu_password_secret_name,
    MAMBU_START_DATE    = "2021-01-01T00:00:00Z"
  }
}
