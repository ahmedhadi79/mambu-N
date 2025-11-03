###########################################################
## AWS Lambda function: mambu api call gl-account to S3 Raw
###########################################################
module "lambda_mambu_api_client_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.8.1"

  function_name = "${local.prefix}-api-client-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 6144

  source_path = [
    {
      path             = "${path.module}/../src/lambdas/mambu_api_client_to_s3_raw",
      pip_requirements = true,
    }
  ]

  environment_variables = merge(
    local.lambda_mambu_env_vars,
  )

  hash_extra   = "${local.prefix}-api-client-lambda-to-s3-raw"
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
# # AWS Event Bridge Rules
# ###########################################################

resource "aws_cloudwatch_event_rule" "schedule_mambu_api_client_events" {
  for_each            = var.mambu_api_events
  name                = "datalake_raw_${each.key}_trigger"
  description         = "Schedule Lambda function execution for ${each.key}"
  schedule_expression = each.value.schedule
  state               = each.value.state
}

resource "aws_cloudwatch_event_target" "mambu_api_cloudwatch_events" {
  for_each = var.mambu_api_events

  arn  = module.lambda_mambu_api_client_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_mambu_api_client_events[each.key].name

  input = jsonencode({
    endpoint     = each.value.endpoint
    cdc_field    = each.value.cdc_field
    table_name   = each.key
    request_type = each.value.request_type
  })
}

resource "aws_lambda_permission" "allow_cloudwatch_event_rule_api_client" {
  for_each = var.mambu_api_events

  statement_id  = "AllowExecutionFromCloudWatch_${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_mambu_api_client_to_s3_raw.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_mambu_api_client_events[each.key].arn
}
