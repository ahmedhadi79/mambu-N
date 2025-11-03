module "lambda_mambu_custom_fields_clients_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.8.1"

  function_name = "${local.prefix}-custom-fields-clients-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 10240
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "${path.module}/../src/lambdas/mambu_custom_fields_clients_to_s3_raw",
  ]

  environment_variables = {
    S3_RAW           = local.raw_datalake_bucket_name,
    MAMBU_USER_AGENT = "ahmed.hadi@nomo.tech" # TODO: Review this
  }

  hash_extra   = "${local.prefix}-custom-fields-clients-to-s3-raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

# ############################################################
# # AWS Event Bridge Rule
# ###########################################################
resource "aws_cloudwatch_event_rule" "schedule_mambu_custom_fields_clients_to_s3" {
  name                = module.lambda_mambu_custom_fields_clients_to_s3_raw.lambda_function_name
  description         = "Schedule Lambda function execution from mambu custom fields clients"
  schedule_expression = "cron(0 01 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_mambu_custom_fields_clients_to_s3_execution" {
  arn  = module.lambda_mambu_custom_fields_clients_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_mambu_custom_fields_clients_to_s3.name
}

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
resource "aws_lambda_permission" "allow_cloudwatch_event_rule_mambu_custom_fields_clients_to_s3" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_mambu_custom_fields_clients_to_s3_raw.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_mambu_custom_fields_clients_to_s3.arn
}
