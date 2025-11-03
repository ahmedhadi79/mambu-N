###########################################################
# AWS Lambda function: Mambu gl accounts to S3 Raw
###########################################################
module "lambda_mambu_gl_accounts_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.8.1"

  function_name = "${local.prefix}-gl-accounts-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.10"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_python310_arn, module.tap_mambu.lambda_layer_arn]
  memory_size   = 10240

  source_path = [
    "../src/common/selective_copy.py",
    {
      path             = "${path.module}/../src/lambdas/mambu_gl_accounts_to_s3_raw",
      pip_requirements = true,
      patterns = [
        "!tests/.*",
        "!backfill/.*",
        "!.DS_Store",
        "!.*.md",
        "!.package/.*",
      ]
    },
  ]

  environment_variables = local.lambda_mambu_env_vars

  hash_extra   = "${local.prefix}-gl-accounts-to-s3-raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  vpc_subnet_ids = split(",", data.aws_ssm_parameter.sls_vpc_private_subnet_ids.value)
  vpc_security_group_ids = [
    data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_mambu_id.value,
    data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_id.value,
    data.aws_ssm_parameter.sls_vpc_security_group_allow_s3_gw_id.value
  ]
}

# Replaced by lambda_api_gl_account_to_s3_raw
# ###########################################################
# # AWS Event Bridge Rule
# ###########################################################
# resource "aws_cloudwatch_event_rule" "schedulemambuglaccountstos3" {
#   name                = module.lambda_mambu_gl_accounts_to_s3_raw.lambda_function_name
#   description         = "Schedule Lambda function execution from Mambu Gl Accounts to S3"
#   schedule_expression = "cron(*/50 * * * ? *)"
#   state               = "DISABLED"
# }

# resource "aws_cloudwatch_event_target" "lambdamambuglaccountstos3execution" {
#   arn  = module.lambda_mambu_gl_accounts_to_s3_raw.lambda_function_arn
#   rule = aws_cloudwatch_event_rule.schedulemambuglaccountstos3.name
# }

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
# resource "aws_lambda_permission" "allow_cloudwatch_mambu_gl_accounts_event_rule" {
#   statement_id  = "AllowExecutionFromCloudWatch"
#   action        = "lambda:InvokeFunction"
#   function_name = module.lambda_mambu_gl_accounts_to_s3_raw.lambda_function_arn
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.schedulemambuglaccountstos3.arn
# }
