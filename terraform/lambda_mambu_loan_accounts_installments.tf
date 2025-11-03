###########################################################
# AWS Lambda function: mambu loan accounts installments to S3 Raw
###########################################################
module "lambda_loan_accounts_installments_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.8.1"

  function_name = "${local.prefix}-loan-installments-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 10240

  source_path = [
    {
      path             = "${path.module}/../src/lambdas/mambu_loan_installments_to_s3_raw",
      pip_requirements = true,
    }
  ]

  environment_variables = merge(
    local.lambda_mambu_env_vars,
    {
      MAMBU_USER_AGENT = "tap-mambu andreas.adamides@bb2.tech",
    }
  )

  hash_extra   = "${local.prefix}-installments-to-s3-raw"
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

# ###########################################################
# # AWS Lambda Triggers - customers
# ###########################################################
resource "aws_cloudwatch_event_rule" "mambu_loan_accounts_installment_to_s3" {
  name                = module.lambda_loan_accounts_installments_to_s3_raw.lambda_function_name
  description         = "Schedule Lambda function execution from Mambu Installments to S3"
  schedule_expression = "cron(0 */8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_loan_accounts_installment_execution" {
  arn  = module.lambda_loan_accounts_installments_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.mambu_loan_accounts_installment_to_s3.name
}

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
resource "aws_lambda_permission" "allow_cloudwatch_event_rule_loan_account_installments" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_loan_accounts_installments_to_s3_raw.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.mambu_loan_accounts_installment_to_s3.arn
}
