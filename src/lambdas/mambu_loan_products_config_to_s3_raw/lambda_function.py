import json
import logging
import os
import time
from datetime import datetime

import awswrangler as wr
import boto3
import data_catalog
import pandas as pd
import requests
import yaml
from flatten_json import flatten
from requests.auth import HTTPBasicAuth

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def camel_to_snake(column_name):
    """
    Converts an input string from camel to snake case
    """
    return "".join(["_" + c.lower() if c.isupper() else c for c in column_name]).lstrip(
        "_"
    )


def write_to_data_lake(input_df, table_name):
    """
    Writes mambu data to the data lake
    :param input_df: the data in the form of a pandas dataframe
    :param mambudb_stream: The mambu stream for this iteration
    :return: The result of the specified action.
    """
    logger.info("Processing Mambu Stream for an Athena write:  %s", table_name)

    # fix for timestamp format coming from Mambu
    timestamp_cols = {
        k: v
        for (k, v) in data_catalog.schemas[table_name].items()
        if v == "timestamp" and k not in ("timestamp_extracted")
    }
    logger.info(timestamp_cols)
    for col in timestamp_cols:
        if col in input_df.columns:
            input_df[col] = pd.to_datetime(
                input_df[col], format="%Y-%m-%dT%H:%M:%S", utc=True
            )

    path = "s3://" + os.environ["S3_RAW"] + "/" + table_name + "/"
    logger.info("Uploading to S3 location:  %s", path)
    try:
        res = wr.s3.to_parquet(
            df=input_df,
            path=path,
            index=False,
            dataset=True,
            database="datalake_raw",
            table=table_name,
            mode="overwrite",
            schema_evolution=True,
            compression="snappy",
            dtype=data_catalog.schemas[table_name],
            # columns_comments=data_catalog.column_comments[table_name],
            glue_table_settings=wr.typing.GlueTableSettings(
                columns_comments=data_catalog.column_comments[table_name]
            ),
        )
        logger.info("Write to Athena complete!")

        return res
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


def get_secret(secret_name: str):
    """
    Retrieves a secret from AWS Secrets Manager
    :param secret_name: The key to retrieve
    :return: The value of the secret
    """
    logger.info("Retrieving :  %s", secret_name)
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        secret = secret_value["SecretString"]
        return json.loads(secret)["MAMBU_API_PASSWORD"]
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


def get_loan_products_config_from_mambu():
    """
    Store all loan products config in a pandas dataframe
    """
    headers = {
        "Accept": "application/vnd.mambu.v2+yaml",
    }
    params = {}
    mambu_secret = get_secret(os.environ["MAMBU_PASSWORD_NAME"])
    try:
        response = requests.get(
            "https://{0}.mambu.com/api/configuration/loanproducts.yaml".format(
                os.environ["MAMBU_SUBDOMAIN"]
            ),
            headers=headers,
            params=params,
            auth=HTTPBasicAuth(
                os.environ["MAMBU_USERNAME"],
                mambu_secret,
            ),
        )
        res = yaml.safe_load(response.text)
        loanproducts = []
        for loanproduct in res["loanProducts"]:
            loanproducts.append(flatten(loanproduct))
        for i, item in enumerate(loanproducts):
            for col in item:
                if loanproducts[i][col] in [{}, []]:
                    loanproducts[i][col] = ""
        loanproducts_df = pd.DataFrame(loanproducts)
        loanproducts_df["timestamp_extracted"] = datetime.utcnow()
        return loanproducts_df
    except Exception as e:
        logger.error("Pandas DF:  %s", res)
        logger.error("Exception occurred in parse:  %s", e)
        return e


def camel_to_snake_case(loan_products_df):
    old_cols = list(loan_products_df.columns)
    new_cols = []
    for old_col in old_cols:
        new_cols.append(camel_to_snake(old_col))
    new_names_map = {
        loan_products_df.columns[i]: new_cols[i] for i in range(len(new_cols))
    }

    loan_products_df.rename(new_names_map, axis=1, inplace=True)
    return loan_products_df


def lambda_handler(event, context):
    begin = time.time()

    logger.info("Getting Loan Products Config data.")
    all_loanproducts_df = get_loan_products_config_from_mambu()
    all_loanproducts_snake_case = camel_to_snake_case(all_loanproducts_df)
    logger.info("Loan Products Config data retrieved and parsed.")

    logger.info("Writing to data lake...")
    res = write_to_data_lake(all_loanproducts_snake_case, "loan_products_config")
    if res:
        logger.info("Data Lake write complete. Result:  %s", res)
    else:
        logger.error("Please investigate...")

    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin)/60):.2f}"
    )
    return True
