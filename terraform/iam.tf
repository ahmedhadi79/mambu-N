###########################################################
# IAM Role for AWS Lambda
###########################################################
resource "aws_iam_role" "iam_for_lambda" {
  name = "${local.prefix}-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com"
          ]
        }
      },
    ]
  })

  inline_policy {
    name   = "s3"
    policy = data.aws_iam_policy_document.s3.json
  }

  inline_policy {
    name   = "athena"
    policy = data.aws_iam_policy_document.athena.json
  }

  inline_policy {
    name   = "glue"
    policy = data.aws_iam_policy_document.glue.json
  }

  inline_policy {
    name   = "translate"
    policy = data.aws_iam_policy_document.translate.json
  }

  inline_policy {
    name   = "logs"
    policy = data.aws_iam_policy_document.logs.json
  }

  inline_policy {
    name   = "xray"
    policy = data.aws_iam_policy_document.xray.json
  }

  inline_policy {
    name   = "secret_manager"
    policy = data.aws_iam_policy_document.secret_manager.json
  }
}

resource "aws_iam_role_policy_attachment" "vpc" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaENIManagementAccess"
}

# Datalake raw permissions
resource "aws_lakeformation_permissions" "lambda_datalake_raw_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_raw_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_raw"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

# Datalake Curated permissions
resource "aws_lakeformation_permissions" "lambda_datalake_curated_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_curated"
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_curated_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_curated"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

# Datalake Reconciliation permissions
resource "aws_lakeformation_permissions" "lambda_datalake_reconciliation_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_reconciliation"
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_reconciliation_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_reconciliation"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

###########################################################
# IAM Role for AWS Glue
###########################################################
resource "aws_iam_role" "iam_for_glue" {
  name = "${local.prefix}-glue"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })


  inline_policy {
    name   = "s3"
    policy = data.aws_iam_policy_document.s3.json
  }

  inline_policy {
    name   = "athena"
    policy = data.aws_iam_policy_document.athena.json
  }

  inline_policy {
    name   = "glue"
    policy = data.aws_iam_policy_document.glue.json
  }

  inline_policy {
    name   = "translate"
    policy = data.aws_iam_policy_document.translate.json
  }

  inline_policy {
    name   = "logs"
    policy = data.aws_iam_policy_document.logs.json
  }

  inline_policy {
    name   = "xray"
    policy = data.aws_iam_policy_document.xray.json
  }

  inline_policy {
    name   = "secret_manager"
    policy = data.aws_iam_policy_document.secret_manager.json
  }
}

resource "aws_iam_role_policy_attachment" "glue_role_policy" {
  role       = aws_iam_role.iam_for_glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}


# AWS Lakeformation permissions
resource "aws_lakeformation_permissions" "glue_datalake_raw_database" {
  principal   = aws_iam_role.iam_for_glue.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }
}

resource "aws_lakeformation_permissions" "glue_datalake_raw_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
    "DROP",
  ]

  principal = aws_iam_role.iam_for_glue.arn

  table {
    database_name = "datalake_raw"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

###########################################################
# Inline policies
###########################################################
data "aws_iam_policy_document" "s3" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListObjects",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:PutObject",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.meta_datalake_bucket_name}",
      "arn:aws:s3:::${local.meta_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.reconciliation_datalake_bucket_name}",
      "arn:aws:s3:::${local.reconciliation_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "athena" {
  statement {
    actions   = ["athena:*"]
    effect    = "Allow"
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "glue" {
  statement {
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "lakeformation:*",
    ]
    effect    = "Allow"
    resources = ["*"]
  }
}


data "aws_iam_policy_document" "translate" {
  statement {
    effect    = "Allow"
    actions   = ["translate:TranslateText"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "logs" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "xray" {
  statement {
    effect = "Allow"
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords"
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "secret_manager" {
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      "arn:aws:secretsmanager:*:*:secret:${var.mambu_password_secret_name}*",
    ]
  }
}
