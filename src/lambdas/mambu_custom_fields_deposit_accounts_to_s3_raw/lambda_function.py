import logging
import os
import time
from datetime import date

import awswrangler as wr
import data_catalog
import pandas as pd
from awswrangler import exceptions

import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    """
    read_athena-read data using Athena service by providing sql query
    :param str: location for file with the query
    :param str: athena database
    :return: Pandas DF
    """

    today = (date.today()).strftime("%Y%m%d")
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()
        sql = sql.format(today)

    logger.info("Reading from Athena... ")
    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database=input_database,
            workgroup="datalake_workgroup",
            ctas_approach=False,
        )
    except (
        exceptions.NoFilesFound,
        exceptions.InvalidFile,
        exceptions.InvalidConnection,
        exceptions.ServiceApiError,
        exceptions.InvalidDataFrame,
    ) as e:
        logger.error("AWS Wrangler Exception occurred:  %s", e.__class__)
        exit(1)
    except Exception as e:
        logger.error("Failed reading from Athena")
        logger.error("Exception occurred:  %s", e)
        exit(1)
    return df


def get_mambu_custom_fields(df):
    if df.shape[0] != 0:
        df = df.fillna("0").replace("<NA>", "0")

        custom_fields_list = []

        customfields_id_list = [
            cf_id for cf_id in df.columns.to_list() if "_id" in cf_id
        ]

        customfields_value_list = [
            cf_value for cf_value in df.columns.to_list() if "_value" in cf_value
        ]

        for i in customfields_id_list:
            custom_fields_list.append(df[i].drop_duplicates().dropna().tolist())

        new_custom_fields_list = []
        # Next we want to iterate over the outer list
        for sub_list in custom_fields_list:
            # Now go over each item of the sublist
            for item in sub_list:
                # append it to our new list
                new_custom_fields_list.append(item)

        custom_fields_set = set(new_custom_fields_list)

        for new_col in custom_fields_set:
            df[new_col] = "None"

        for item in custom_fields_set:
            try:
                df.loc[df["custom_fields_0_id"] == item, item] = df[
                    "custom_fields_0_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_1_id"] == item, item] = df[
                    "custom_fields_1_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_2_id"] == item, item] = df[
                    "custom_fields_2_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_3_id"] == item, item] = df[
                    "custom_fields_3_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_4_id"] == item, item] = df[
                    "custom_fields_4_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_5_id"] == item, item] = df[
                    "custom_fields_5_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_6_id"] == item, item] = df[
                    "custom_fields_6_value"
                ].copy()
            except Exception:
                pass
            try:
                df.loc[df["custom_fields_7_id"] == item, item] = df[
                    "custom_fields_7_value"
                ].copy()
            except Exception:
                pass

        zero_list = ["0"]
        removed_list = customfields_id_list + customfields_value_list + zero_list

        df = df.loc[:, ~df.columns.isin(removed_list)].copy()

        # Define static vars
        df["date"] = date.today().strftime("%Y%m%d")

        return df


def write_to_data_lake(input_df, table_name):
    """
    Writes mambu custom fields data to the data lake
    :param input_df: the data in the form of a pandas dataframe
    :return: The result of the specified action.
    """
    logger.info(
        "Processing Mambu custom fields data for an Athena write:  %s",
        table_name,
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
            partition_cols=["date"],
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


def lambda_handler(event, context):
    """[summary]
    :param event: [description]
    :type event: [type]
    :param context: [description]
    :type context: [type]
    :return: [description]
    :rtype: [type]
    """
    begin = time.time()

    logger.info("Getting data from Athena...")
    for sql_path in config.config:
        sql = config.config[sql_path][sql_path]
        logger.info("Getting Data Frame Complete. Result:  %s", sql)
        df = read_athena(sql, "datalake_raw")
        logger.info(
            "Pivotting Data Mambu Custom Fields Complete. Result:  %s",
            sql.replace(".sql", ""),
        )
        cf_df = get_mambu_custom_fields(df)

        logger.info("Writing to data lake...")
        res = write_to_data_lake(cf_df, sql.replace(".sql", ""))
        if res:
            logger.info("Data Lake write complete. Result:  %s", res)
        else:
            logger.error("Please investigate...")

    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin)/60):.2f}"
    )
    return True
