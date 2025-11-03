data "aws_ssm_parameter" "sls_vpc_id" {
  name = "/nomo/nomo_networking/sls_vpc/vpc/id"
}

data "aws_ssm_parameter" "sls_vpc_private_subnet_ids" {
  name = "/nomo/nomo_networking/sls_vpc/vpc/private_subnets/ids"
}

data "aws_subnet" "selected_private_subnet" {
  id = split(",", data.aws_ssm_parameter.sls_vpc_private_subnet_ids.value)[0]
}

data "aws_ssm_parameter" "sls_vpc_security_group_allow_tls_mambu_id" {
  name = "/nomo/nomo_networking/sls_vpc/security_group/allow_tls_mambu/id"
}

data "aws_ssm_parameter" "sls_vpc_security_group_allow_tls_id" {
  name = "/nomo/nomo_networking/sls_vpc/security_group/allow_tls/id"
}
data "aws_ssm_parameter" "sls_vpc_security_group_allow_s3_gw_id" {
  name = "/nomo/nomo_networking/sls_vpc/security_group/allow_s3_gw/id"
}
