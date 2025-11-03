# Running

## Pre-requisities
- Have the VPN connected to Mambu. If you don't have the VPN request it in Slack channel #devsecops-public
- Create a python virtual evn with all requirements_tests.txt installed

## A. Running `tap-mambu` locally.
- (Optional). This returns the mambu catalog. Think of this as the schema of available tables and columns/fields. This repository is already pre-filled with a `catalog.json` file.
```bash
tap-mambu --config tap_config.json --discover > catalog_temp.json
```

- This will return the actual data. It takes a few inputs:
  - `tap_config.json`: connection details. Make sure to get the correct password and supply the correct domain.
  - `catalog.json`: Which tables to download, make sure `selected: true` in the `metadata` section of the relevant table/stream, i.e.
  ```json
   "stream": "loan_accounts",
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "table-key-properties": [
                            "id"
                        ],
                        "forced-replication-method": "INCREMENTAL",
                        "valid-replication-keys": [
                            "last_modified_date"
                        ],
                        "inclusion": "available",
                        "selected": true
                    }
                },
  ```
  - `state.json`: Watermarks. From what date to download data from.
```bash
tap-mambu --config tap_config.json --catalog catalog.json --state state.json | target-jsonl > latest_state.json
```
After this step, you should be able to see locally downloaded `jsonl` files for the `selected` streams. If this does not work, please investigate.

## B. Making the change to the lambda code
If `A` works, it it is time to make the change to the lamba code. Changes required:
1. Change the main dict to include the new stream(if any) and its partition column(`date` by default):
```python
    mambudb_streams = {
        "gl_journal_entries": ["date"],
        "gl_accounts": ["date"],
        "deposit_transactions": ["date"],
        "deposit_accounts": ["date"],
        "custom_field_sets": ["date"],
        "clients": ["date"],
    }
```
2. Make sure `lambda/mambu-to-s3-raw/data_catalog.py` is filled in for `column_comments` and `schemas` for respective stream. You can get these information from step `A` output and the Mambu API documentation page at https://api.mambu.com/ .

## C. Deploying to Sandbox Lambda with Terraform

- First, refer to `data-lake-etl/terraform/README.md` in order to establish connection with AWS for being to deploy.
- After deployment to `sandbox` is complete, debug the AWS Lambda with invoking it:

```bash
aws lambda invoke \
--function-name datalake-sandbox-mambu-to-s3-raw \
--cli-binary-format raw-in-base64-out \
--cli-read-timeout 0 \
--profile bb2-sandbox-admin \
out.json
```
- Navigate to AWS Console, Cloudwatch group for this function, and inspect the results: https://eu-west-2.console.aws.amazon.com/cloudwatch/home?region=eu-west-2#logsV2:log-groups/log-group/$252Faws$252Flambda$252Fdatalake-sandbox-mambu-to-s3-raw
