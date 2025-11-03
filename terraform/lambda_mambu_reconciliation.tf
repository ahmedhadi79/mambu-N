# ###########################################################
# # AWS Lambda function: Mambu Recon
# # - Keeps the function deployed; jobs will be stopped by disabling
# #   the EventBridge schedules below.
# # -https://bb-2.atlassian.net/browse/NM-44335
# ###########################################################
# module "lambda_mambu_datalake_reconciliation" {
#   source  = "terraform-aws-modules/lambda/aws"
#   version = "7.8.1"

#   function_name = "${local.prefix}-reconciliation"
#   handler       = "lambda_function.lambda_handler"
#   runtime       = "python3.10"
#   timeout       = 900
#   memory_size   = 2048
#   layers        = [local.lambda_layer_aws_wrangler_python310_arn, module.tap_mambu.lambda_layer_arn]

#   source_path = [
#     "../src/common/selective_copy.py",
#     {
#       path             = "${path.module}/../src/lambdas/mambu_reconciliation",
#       pip_requirements = true,
#       patterns = [
#         "!tests/.*",
#         "!backfill/.*",
#         "!.DS_Store",
#         "!.*.md",
#         "!.package/.*",
#       ]
#     },
#   ]
#   environment_variables = merge(
#     local.lambda_mambu_env_vars,
#     {
#       S3_RECONCILIATION = local.reconciliation_datalake_bucket_name,
#       IS_PRODUCTION     = true,
#     }
#   )

#   hash_extra   = "${local.prefix}-reconciliation"
#   create_role  = false
#   lambda_role  = aws_iam_role.iam_for_lambda.arn
#   tracing_mode = "Active"

#   vpc_subnet_ids = split(",", data.aws_ssm_parameter.sls_vpc_private_subnet_ids.value)
#   vpc_security_group_ids = [
#     data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_mambu_id.value,
#     data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_id.value,
#     data.aws_ssm_parameter.sls_vpc_security_group_allow_s3_gw_id.value
#   ]
# }


# ###########################################################
# # AWS Event Bridge Rule
# ###########################################################
# resource "aws_cloudwatch_event_rule" "schedule_reconciliation_mambu_data_lake" {
#   for_each            = var.mambu_stream_events
#   name                = "lambda_mambu_datalake_reconciliation_${each.key}"
#   description         = "Schedule Lambda function execution for ${each.key}"
#   schedule_expression = each.value.schedule
#   state               = each.value.state
# }

# resource "aws_cloudwatch_event_target" "lambdaexecution_mambu_reconciliation" {
#   for_each = var.mambu_stream_events
#   arn      = module.lambda_mambu_datalake_reconciliation.lambda_function_arn
#   rule     = aws_cloudwatch_event_rule.schedule_reconciliation_mambu_data_lake[each.key].name

#   input = jsonencode({
#     mambu_stream = each.key
#   })
# }

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
# resource "aws_lambda_permission" "mambu_reconciliation_allow_cloudwatch_event_rule" {
#   for_each      = var.mambu_stream_events
#   statement_id  = "AllowExecutionFromCloudWatch_${each.key}"
#   action        = "lambda:InvokeFunction"
#   function_name = module.lambda_mambu_datalake_reconciliation.lambda_function_arn
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.schedule_reconciliation_mambu_data_lake[each.key].arn
# }
