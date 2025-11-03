###########################################################
# AWS Glue Job | Mambu API Client Backfill to S3 Raw
###########################################################
resource "aws_s3_object" "api_client_backfill_glue_to_s3_raw" {
  for_each = fileset("../src/glue/mambu_api_client_backfill_to_s3_raw/", "*.py")

  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/mambu_api_client_backfill_to_s3_raw/${each.value}"
  source = "../src/glue/mambu_api_client_backfill_to_s3_raw/${each.value}"
  etag   = filemd5("../src/glue/mambu_api_client_backfill_to_s3_raw/${each.value}")
}

resource "aws_s3_object" "api_client_backfill_lambda_to_s3_raw" {
  for_each = fileset("../src/lambdas/mambu_api_client_to_s3_raw/", "*.py")

  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/mambu_api_client_backfill_to_s3_raw/${each.value}"
  source = "../src/lambdas/mambu_api_client_to_s3_raw/${each.value}"
  etag   = filemd5("../src/lambdas/mambu_api_client_to_s3_raw/${each.value}")
}

resource "aws_glue_job" "api_client_backfill_to_s3_raw" {
  name         = "${local.prefix}-api-client-backfill-to-s3-raw"
  description  = "AWS Glue Job"
  role_arn     = aws_iam_role.iam_for_glue.arn
  max_retries  = 0
  max_capacity = 1
  timeout      = 4320 # 3 days

  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${local.project_name}/scripts/mambu_api_client_backfill_to_s3_raw/main.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"

    "--S3_RAW"              = local.raw_datalake_bucket_name
    "--MAMBU_SUBDOMAIN"     = local.lambda_mambu_env_vars.MAMBU_SUBDOMAIN
    "--MAMBU_PASSWORD_NAME" = local.lambda_mambu_env_vars.MAMBU_PASSWORD_NAME

    "--extra-py-files" = join(",", concat(
      [
        for file in fileset("../src/glue/mambu_api_client_backfill_to_s3_raw/", "*.py") :
        "s3://${local.glue_assets_bucket_name}/${local.project_name}/scripts/mambu_api_client_backfill_to_s3_raw/${file}"
      ],
      [
        for file in fileset("../src/lambdas/mambu_api_client_to_s3_raw/", "*.py") :
        "s3://${local.glue_assets_bucket_name}/${local.project_name}/scripts/mambu_api_client_backfill_to_s3_raw/${file}"
      ]
    ))
  }

  connections = ["${local.prefix}-vpc-private-connection"]
}

resource "aws_glue_connection" "vpc_private_connection" {
  name            = "${local.prefix}-vpc-private-connection"
  connection_type = "NETWORK"

  physical_connection_requirements {
    subnet_id         = data.aws_subnet.selected_private_subnet.id
    availability_zone = data.aws_subnet.selected_private_subnet.availability_zone
    security_group_id_list = [
      data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_id.value,       #For secrets manager
      data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_mambu_id.value, #For mambu API
      data.aws_ssm_parameter.sls_vpc_security_group_allow_s3_gw_id.value,     #For S3 scripts download, athena, etc
      aws_security_group.allow_glue_sg.id                                     #For Glue job to communicate with itself
    ]
  }
}

############################
# https://docs.aws.amazon.com/glue/latest/dg/setup-vpc-for-glue-access.html

resource "aws_security_group" "allow_glue_sg" {
  name        = "allow_glue"
  description = "Self-referencing rule to allow AWS Glue components to communicate"
  vpc_id      = data.aws_ssm_parameter.sls_vpc_id.value

  tags = {
    Name = "allow_glue"
  }
}

resource "aws_vpc_security_group_ingress_rule" "allow_glue_sg_inbound_rule" {
  security_group_id            = aws_security_group.allow_glue_sg.id
  referenced_security_group_id = aws_security_group.allow_glue_sg.id
  from_port                    = 0
  to_port                      = 65535
  ip_protocol                  = "tcp"
  description                  = "Self-referencing rule to allow AWS Glue components to communicate"
  depends_on                   = [aws_security_group.allow_glue_sg]
}

resource "aws_vpc_security_group_egress_rule" "allow_glue_sg_outbound_rule" {
  security_group_id            = aws_security_group.allow_glue_sg.id
  referenced_security_group_id = aws_security_group.allow_glue_sg.id
  from_port                    = 0
  to_port                      = 65535
  ip_protocol                  = "tcp"
  description                  = "Self-referencing rule to allow AWS Glue components to communicate"
  depends_on                   = [aws_security_group.allow_glue_sg]
}
