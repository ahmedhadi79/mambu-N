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
            # input_df[col] = pd.to_datetime(
            # input_df[col], format="%Y-%m-%dT%H:%M:%S", utc=True
            # )
            input_df[col] = pd.to_datetime(input_df[col], format="ISO8601", utc=True)

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
            mode="overwrite_partitions",
            schema_evolution=True,
            compression="snappy",
            partition_cols=["loan_account_id"],
            dtype=data_catalog.schemas[table_name],
            glue_table_settings=wr.typing.GlueTableSettings(
                columns_comments=data_catalog.column_comments[table_name]
            ),
        )
        logger.info("Write to Athena complete!")

        return res
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


def get_athena_df(sql_file: str):
    """
    Retrieve a dataset from athena based on input SQL file
    """
    logger.info(f"Executing {sql_file} ....")
    sql = sql_file.read()

    logger.info("Reading data from Athena...")
    try:
        athena_df = wr.athena.read_sql_query(
            sql=sql,
            database="datalake_raw",
            workgroup="datalake_workgroup",
            ctas_approach=False,
        )
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False
    return athena_df


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


def get_installments_df(installments_data):
    """
    Parse individual api response onto pandas df
    """
    # flatten
    installments = []
    for installment in installments_data:
        installments.append(flatten(installment))

    # fix issue that data wrangler cannot
    # write empty dicts or lists to parquet in S3
    for i, item in enumerate(installments):
        for col in item:
            if installments[i][col] in [{}, []]:
                installments[i][col] = ""
    df = pd.DataFrame(installments)
    return df


def get_installments_from_mambu(loan_account_ids_df):
    """
    Store all installments in a pandas dataframe
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.mambu.v2+json",
    }
    loan_accounts_ids = loan_account_ids_df["id"].tolist()
    output_list_of_dfs = []
    mambu_secret = get_secret(os.environ["MAMBU_PASSWORD_NAME"])
    for loan_account_id in loan_accounts_ids:
        try:
            res = requests.get(
                "https://{0}.mambu.com/api/loans/{1}/schedule?detailsLevel=FULL".format(
                    os.environ["MAMBU_SUBDOMAIN"], loan_account_id
                ),
                headers=headers,
                auth=HTTPBasicAuth(
                    os.environ["MAMBU_USERNAME"],
                    mambu_secret,
                ),
            )
            loan_account_installments_df = get_installments_df(
                res.json()["installments"]
            )
            loan_account_installments_df["loan_account_id"] = loan_account_id
            loan_account_installments_df["timestamp_extracted"] = datetime.utcnow()
            output_list_of_dfs.append(loan_account_installments_df)
        except Exception as e:
            logger.warn("Loan account id:  %s", loan_account_id)
            errors = res.json()["errors"]
            for error in errors:
                if error["errorReason"] == "INVALID_LOAN_ACCOUNT_ID":
                    logger.warn(
                        "Loan account is invalid, please cross check in Mambu UI."
                    )
                else:
                    logger.error("Exception occurred:  %s", e)
                    logger.error(errors)
                    logger.error(res.json())
            continue

    return pd.concat(output_list_of_dfs)


def camel_to_snake_case(all_installments_df):
    old_cols = list(all_installments_df.columns)
    new_cols = []
    for old_col in old_cols:
        new_cols.append(camel_to_snake(old_col))
    new_names_map = {
        all_installments_df.columns[i]: new_cols[i] for i in range(len(new_cols))
    }

    all_installments_df.rename(new_names_map, axis=1, inplace=True)
    return all_installments_df


def lambda_handler(event, context):
    """
    Accepts a Kinesis Data Stream Event
    :param event: The event dict that contains the parameters sent when the function
                is invoked.
    :param context: The context in which the function is called.
    :return: The result of the specified action.
    """
    begin = time.time()

    logger.info("Getting loan account ids..")
    loan_account_ids_df = get_athena_df(open("loan_account_ids.sql", "r"))
    logger.info("Loan account ids loaded.")

    logger.info("Getting installment data for loan accounts retrieved.")
    all_installments_df = get_installments_from_mambu(loan_account_ids_df)
    all_installments_snake_case = camel_to_snake_case(all_installments_df)
    logger.info("Installment data retrieved and parsed.")

    logger.info("Writing to data lake...")
    res = write_to_data_lake(all_installments_snake_case, "loan_accounts_installments")
    if res:
        logger.info("Data Lake write complete. Result:  %s", res)
    else:
        logger.error("Please investigate...")

    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin)/60):.2f}"
    )
    return True
