
# Running

## Pre-requisities
- Have the VPN connected to Mambu. If you don't have the VPN request it in Slack channel #devsecops-public
- Create a python virtual evn with all requirements_tests.txt installed

## A. Making the change to the lambda code
Make the change to the lamba code. Changes required:
1. Create the Python reconciliation function in `mambu-datalake-reconciliation/lambda_function.py`. See one of the existing funnctions for an example.

2. Make sure `mambu-datalake-reconciliation/config.py` is filled with the `<new_stream>` dict and the SQL for the `<new_stream>` . See existing streams for examples.

## B. Deploying to Sandbox Lambda with Terraform

- First, refer to `data-lake-etl/terraform/README.md` in order to establish connection with AWS for deploying.
- After deployment to `sandbox` is complete, debug the AWS Lambda with invoking it:

```bash
aws lambda invoke \
--function-name datalake-sandbox-mambu-datalake-reconciliation \
--cli-binary-format raw-in-base64-out \
--cli-read-timeout 0 \
--profile bb2-sandbox-admin \
out.json
```
- Navigate to AWS Console, Cloudwatch group for this function, and inspect the results.
