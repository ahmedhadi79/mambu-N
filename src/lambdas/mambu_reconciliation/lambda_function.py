import glob
import json
import logging
import os
import subprocess
import sys
import shutil
import fnmatch
from datetime import date
from datetime import datetime
from datetime import timedelta

import awswrangler as wr
import boto3
import data_catalog
import numpy as np
import pandas as pd
from flatten_json import flatten

import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def selective_copy(src_dir, dest_dir, patterns=("*.py", "*.json","*.sql")):
    """
    Selectively copy files from src_dir to dest_dir based on matching patterns.
    """
    for root, dirs, files in os.walk(src_dir):
        for filename in files:
            for pattern in patterns:
                if fnmatch.fnmatch(filename, pattern):
                    full_src_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(full_src_path, src_dir)
                    full_dest_path = os.path.join(dest_dir, rel_path)
                    os.makedirs(os.path.dirname(full_dest_path), exist_ok=True)
                    shutil.copy2(full_src_path, full_dest_path)


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


def generate_tap_config(filepath: str):
    """
    Creates a JSON file which acts as input to tap mambu commands
    :param filepath: The filepath at which the JSON file is written to
    :return: The result of the specified action.
    """
    try:
        logger.info("Generating tap configuration file..")
        tap_config = {}

        tap_config["username"] = os.environ["MAMBU_USERNAME"]
        tap_config["password"] = get_secret(os.environ["MAMBU_PASSWORD_NAME"])
        tap_config["apikey"] = ""
        tap_config["subdomain"] = os.environ["MAMBU_SUBDOMAIN"]
        tap_config["start_date"] = os.environ["MAMBU_START_DATE"]
        tap_config["user_agent"] = os.environ["MAMBU_USER_AGENT"]

        with open(filepath, "w") as tap:
            json.dump(tap_config, tap, indent=4)

        logger.info("Tap config written in:  %s", filepath)

        return True
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


def mambu_fetch_latest_catalog(mambu_stream, get_from_remote, working_dir):
    """
    Run fetch command from Mambu for getting the latest catalog
    tap-mambu --config tap_config.json --discover > catalog.json
    :param mambu_stream: The stream to download
    :param get_from_remote: Whether to download from remote
    :param working_dir: The working directory
    :return: The result of the specified action.
    """
    try:
        if get_from_remote:
            catalog_command = "python /opt/package/tap-mambu --config {0}/tap_config.json --discover".format(
                working_dir
            ).split(
                " "
            )
            logger.info("Tap mambu catalog command:  %s", catalog_command)
            with open("catalog_temp.json", "w") as catalog_output:
                try:
                    subprocess.run(catalog_command, stdout=catalog_output)
                except Exception as e:
                    logger.error("Exception occurred:  %s", e)
                    return False

            logger.info("Downloaded latest catalog from Mambu")

        # opening downloaded file
        f = open("catalog_temp.json")
        catalog = json.load(f)

        # adding required field "selected" for downloading in next step
        for s in catalog["streams"]:
            if s["stream"] == mambu_stream:
                logger.info("Enabling stream:  %s", mambu_stream)
                s["metadata"][0]["metadata"]["selected"] = True

        # writing result to local directory as catalog.json
        with open("catalog.json", "w") as outfile:
            json.dump(catalog, outfile, indent=4)

        return True
    except Exception as e:
        logger.info("Exception occurred:  %s", e)
        return False


def mambu_fetch(
    mambu_statefile,
    mambu_stream,
    working_dir=None,
):
    """
    Run fetch command from Mambu
    tap-mambu --config tap_config.json --catalog catalog.json --state state.json | target-jsonl > latest_state.json
    :param mambu_statefile: The name of the mambu statefile.
    :return: The result of the specified action.
    """
    logger.info("Issuing command to download data from Mambu.")
    if working_dir is None:
        working_dir = os.getcwd()

    logger.info("Getting latest catalog from Mambu for configured streams..")
    mambu_fetch_latest_catalog(mambu_stream, True, working_dir)
    logger.info("Mambu catalog fetched!")

    # Issue command to Mambu API
    try:
        main_command = "python /opt/package/tap-mambu --config {0}/tap_config.json --catalog {0}/catalog.json --state {0}/state.json".format(
            working_dir
        ).split(
            " "
        )
        pipe_command = (
            "python /opt/package/target-jsonl --config {0}/config.json".format(
                working_dir
            ).split(" ")
        )
        logger.info("Tap mambu command:  %s", main_command)
        out_main_command = subprocess.run(main_command, check=True, capture_output=True)
        logger.info("Target jsonl command:  %s", pipe_command)
        with open("latest_" + mambu_statefile, "w") as state:
            subprocess.run(pipe_command, input=out_main_command.stdout, stdout=state)

        logger.info("Configured streams have been downloaded from Mambu.")
        return True
    except Exception as e:
        logger.info("Exception occurred:  %s", e)
        return e


def cleanup_dir(dir):
    """
    Since AWS Lambda might re-use the same context on each invocation
    we want to make sure the /tmp directory is clean to avoid our
    function re-using same files from old runs
    :param dir: The dir to clean contents in
    :return: The result of the specified action.
    """
    file_list = glob.glob(dir, recursive=True)
    try:
        for tmp_file_path in file_list:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
                logger.info("Removed the file %s" % tmp_file_path)
            else:
                logger.info("File %s does not exist." % tmp_file_path)
        return True
    except Exception as e:
        logger.info("Exception occurred:  %s", e)
        return e


def process_deposit_transactions(
    input_df: pd.DataFrame, athena_df: pd.DataFrame
) -> pd.DataFrame:
    # Calculate data points from source
    input_df["creation_date"] = pd.to_datetime(input_df["creation_date"])
    input_df["date"] = input_df["creation_date"].dt.date
    input_df["amount"] = input_df["amount"].astype(float)
    # Max id
    input_df["id"] = input_df["id"].astype(int)
    df2 = input_df.groupby(["date"], as_index=False)["id"].max()
    df2.rename(columns={"id": "source_max_id_for_date"}, inplace=True)
    # Min id
    df1 = input_df.groupby(["date"], as_index=False)["id"].min()
    df1.rename(columns={"id": "source_min_id_for_date"}, inplace=True)
    # Currency code
    df_currencies_types = input_df.value_counts(
        subset=["date", "currency_code", "type"]
    ).to_frame(name="source_currency_type_transactions_total_for_date")
    df_currencies_types.reset_index(inplace=True)
    # Amount total
    df_amounts = input_df.groupby(["date", "currency_code", "type"], as_index=False)[
        "amount"
    ].sum()
    df_amounts.rename(
        columns={"amount": "source_sum_amount_for_currency_and_date"}, inplace=True
    )
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)

    # Prepare mambu df
    mambu_source_df = df1.merge(df2, left_on="date", right_on="date")
    mambu_source_df = mambu_source_df.merge(
        df_currencies_types, left_on="date", right_on="date"
    )
    mambu_source_df = mambu_source_df.merge(
        df_amounts,
        left_on=["date", "currency_code", "type"],
        right_on=["date", "currency_code", "type"],
    )
    mambu_source_df = mambu_source_df.merge(
        df_rowcount, left_on=["date"], right_on=["date"]
    )
    mambu_source_df["date"] = pd.to_datetime(mambu_source_df["date"])

    # Merge and prepare final table
    full_df = mambu_source_df.merge(
        athena_df,
        left_on=["date", "currency_code", "type"],
        right_on=["date", "currency_code", "type"],
    )
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_clients(input_df: pd.DataFrame, athena_df: pd.DataFrame) -> pd.DataFrame:
    input_df["last_modified_date"] = pd.to_datetime(input_df["last_modified_date"])
    input_df["date"] = input_df["last_modified_date"].dt.date
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    # client count per creation date
    df_clientcount = input_df.groupby(["date"], as_index=False)["id"].nunique()
    df_clientcount.rename(columns={"id": "source_total_clients_for_date"}, inplace=True)

    # Prepare mambu df
    mambu_source_df = df_clientcount.merge(df_rowcount, left_on="date", right_on="date")

    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_loan_accounts(
    input_df: pd.DataFrame, athena_df: pd.DataFrame
) -> pd.DataFrame:
    input_df["last_modified_date"] = pd.to_datetime(input_df["last_modified_date"])
    input_df["date"] = input_df["last_modified_date"].dt.date
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    # loan accounts count per creation date
    df_loan_account_count = input_df.groupby(["date"], as_index=False)["id"].nunique()
    df_loan_account_count.rename(
        columns={"id": "source_total_loan_accounts_for_date"}, inplace=True
    )

    # Prepare mambu df
    mambu_source_df = df_loan_account_count.merge(
        df_rowcount, left_on="date", right_on="date"
    )

    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_loan_transactions(
    input_df: pd.DataFrame, athena_df: pd.DataFrame
) -> pd.DataFrame:
    input_df["creation_date"] = pd.to_datetime(input_df["creation_date"])
    input_df["date"] = input_df["creation_date"].dt.date
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    # loan transactions count per creation date
    df_loan_transactions_count = input_df.groupby(["date"], as_index=False)[
        "id"
    ].nunique()
    df_loan_transactions_count.rename(
        columns={"id": "source_total_loan_transactions_for_date"}, inplace=True
    )

    # Prepare mambu df
    mambu_source_df = df_loan_transactions_count.merge(
        df_rowcount, left_on="date", right_on="date"
    )

    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_gl_journal_entries(
    input_df: pd.DataFrame, athena_df: pd.DataFrame
) -> pd.DataFrame:
    input_df["creation_date"] = pd.to_datetime(input_df["creation_date"])
    input_df["date"] = input_df["creation_date"].dt.date
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    df_grouped = input_df.groupby(["date"], as_index=False)["entry_id"].nunique()
    df_grouped.rename(
        columns={"entry_id": "source_total_gl_journal_entries_for_date"}, inplace=True
    )

    # Prepare mambu df
    mambu_source_df = df_grouped.merge(df_rowcount, left_on="date", right_on="date")

    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_deposit_accounts(
    input_df: pd.DataFrame, athena_df: pd.DataFrame
) -> pd.DataFrame:
    input_df["creation_date"] = pd.to_datetime(input_df["creation_date"])
    input_df["date"] = input_df["creation_date"].dt.date
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    # deposit account count per creation date
    df_deposit_account_count = input_df.groupby(["date"], as_index=False)[
        "id"
    ].nunique()
    df_deposit_account_count.rename(
        columns={"id": "source_total_deposit_accounts_for_date"}, inplace=True
    )

    # Prepare mambu df
    mambu_source_df = df_deposit_account_count.merge(
        df_rowcount, left_on="date", right_on="date"
    )

    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_gl_accounts(
    input_df: pd.DataFrame, athena_df: pd.DataFrame
) -> pd.DataFrame:
    input_df["date"] = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    # gl account count per creation date
    df_gl_account_count = input_df.groupby(["date"], as_index=False)[
        "gl_code"
    ].nunique()
    df_gl_account_count.rename(
        columns={"gl_code": "source_total_gl_accounts_for_date"}, inplace=True
    )

    # Prepare mambu df
    mambu_source_df = df_gl_account_count.merge(
        df_rowcount, left_on="date", right_on="date"
    )

    mambu_source_df.date = pd.to_datetime(mambu_source_df.date)
    athena_df.date = pd.to_datetime(athena_df.date)
    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def process_users(input_df: pd.DataFrame, athena_df: pd.DataFrame) -> pd.DataFrame:
    input_df["last_modified_date"] = pd.to_datetime(input_df["last_modified_date"])
    input_df["date"] = input_df["last_modified_date"].dt.date
    # Rowcount per creation date
    df_rowcount = input_df.value_counts(subset=["date"]).to_frame(
        name="source_total_rowcount_for_date"
    )
    df_rowcount.reset_index(inplace=True)
    # user count per creation date
    df_usercount = input_df.groupby(["date"], as_index=False)["id"].nunique()
    df_usercount.rename(columns={"id": "source_total_users_for_date"}, inplace=True)

    # Prepare mambu df
    mambu_source_df = df_usercount.merge(df_rowcount, left_on="date", right_on="date")

    full_df = mambu_source_df.merge(athena_df, left_on=["date"], right_on=["date"])
    full_df.rename(columns={"date": "reconciliation_date"}, inplace=True)
    full_df["reconciliation_date"] = full_df["reconciliation_date"].astype(str)

    return full_df


def debug_write_to_s3(
    tempdf: pd.DataFrame, athena_table: str, s3_bucket: str, source: str
):
    """
    Fallback Boto3 writing to S3.
    :param tempdf: Pandas DF to write to S3
    :type tempdf: pd.DataFrame
    :param athena_table: Table to write to (it will be suffixed with _fallback)
    :type athena_table: str
    :param s3_bucket: The S3 Bucket to write to
    :type s3_bucket: str
    :param source: The source of df
    :type source: str
    """

    dt = datetime.utcnow()
    date = dt.strftime("%Y%m%d")
    time = dt.strftime("%H%M%S")

    s3_resource = boto3.resource("s3")

    fallback_path = f"debug_{athena_table}_{source}/{date}/{time}.csv"
    logger.info(f"Uploading to S3 location: {fallback_path} as CSV...")

    tmp_path = "/tmp/df.csv"
    tempdf.to_csv(tmp_path, index=False)
    s3_resource.meta.client.upload_file(tmp_path, s3_bucket, fallback_path)


def filter_mambu(mambu_df):
    """
    Get only yesterday's data
    """
    mambu_df_selected = mambu_df[["id", "creation_date"]]
    mambu_df_selected["creation_date"] = pd.to_datetime(
        mambu_df_selected["creation_date"]
    )
    mambu_df_selected["creation_date"] = mambu_df_selected["creation_date"].dt.date
    filter_date = date.today() - timedelta(days=1)
    mambu_df_selected = mambu_df_selected[
        mambu_df_selected["creation_date"] == filter_date
    ]
    mambu_df_selected["id"] = mambu_df_selected["id"].astype(np.int64)
    return mambu_df_selected


def write_to_s3_raw(input_df, mambudb_stream):
    """
    Writes mambu data to the data lake
    :param mambudb_streams: A dict of mambu streams info
    :param input_df: the data in the form of a pandas dataframe
    :param path: The s3 path to write to
    :param mambudb_stream: The mambu stream for this iteration
    :return: The result of the specified action.
    """

    mambudb_streams = {
        "gl_journal_entries": ["date"],
        "gl_accounts": ["date"],
        "deposit_transactions": ["date"],
        "deposit_accounts": ["date"],
        "custom_field_sets": ["date"],
        "clients": ["date"],
        "loan_accounts": ["date"],
        "loan_transactions": ["date"],
        "users": ["date"],
    }
    # custom_field_sets is a metadata table
    timestamp_cols = {
        k: v
        for (k, v) in data_catalog.schemas[mambudb_stream].items()
        if v == "timestamp" and k not in ("timestamp_extracted")
    }
    for col in timestamp_cols:
        if col in input_df.columns:
            input_df[col] = pd.to_datetime(
                input_df[col], format="%Y-%m-%dT%H:%M:%S.%fZ"
            )

    path = "s3://" + os.environ["S3_RAW"] + "/" + mambudb_stream + "/"
    logger.info("Uploading to S3 location:  %s", path)
    res = wr.s3.to_parquet(
        df=input_df,
        path=path,
        index=False,
        dataset=True,
        database="datalake_raw",
        table=mambudb_stream,
        mode="append",
        schema_evolution=True,
        compression="snappy",
        partition_cols=mambudb_streams[mambudb_stream],
        dtype=data_catalog.schemas[mambudb_stream],
        glue_table_settings=wr.typing.GlueTableSettings(
            columns_comments=data_catalog.column_comments[mambudb_stream]
        ),
    )

    return res


def backfill_late_settled_deposit_transactions(
    processed_df, mambu_stream, input_df, athena_ids_yesterday_df=False, local=False
):
    # check counts function if stream is dep transactions
    if mambu_stream == "deposit_transactions":
        # only read from athena if executing in lambda
        if not local:
            sql_file = open(
                config.mambudb_streams[mambu_stream]["backfill_sql_path"], "r"
            )
            athena_ids_yesterday_df = get_athena_df(sql_file)
        # get counts but don't raise error alarm, hence 3rd argument
        count_check = check_counts(
            processed_df, "deposit_transactions", "fix_deposit_transactions", None
        )
        # if counts don't match
        if count_check == -1:
            logger.info("Missing transactions detected.")
            # then get missing ids with notebook code
            mambu_df_filtered = filter_mambu(input_df)
            athena_ids_yesterday_df["id"] = athena_ids_yesterday_df["id"].astype(
                np.int64
            )
            merged = mambu_df_filtered.merge(
                athena_ids_yesterday_df,
                how="outer",
                left_on="id",
                right_on="id",
                indicator=True,
            )
            logger.info(f"Merged: {merged}")
            filtered_df = merged.query("_merge=='left_only'")
            missing_ids = filtered_df["id"].tolist()
            if missing_ids == []:
                logger.info("No missing transactions detected.")
                return False, None
            logger.info(f"Missing ids: {missing_ids}")
            # based on missing ids get mambu rows in a pandas df
            mambu_missing_ids_df = input_df.query("id in @missing_ids")
            # only write to s3 if executing lambda
            if not local:
                # append mode in datalake raw for pandas df
                mambu_missing_ids_df["date"] = date.today().strftime("%Y%m%d")
                mambu_missing_ids_df["timestamp_extracted"] = datetime.utcnow()
                mambu_missing_ids_df["creation_date"] = pd.to_datetime(
                    mambu_missing_ids_df["creation_date"], format="%Y-%m-%d %H:%M:%S"
                ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                res = write_to_s3_raw(mambu_missing_ids_df, "deposit_transactions")
                logger.info(f"Upload complete: {res}")
            return True, mambu_missing_ids_df
        elif count_check == 0:
            logger.info("No missing transactions detected.")
            return False, None
    else:
        return False, None


def get_athena_df(sql_file):
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
    logger.info("Query finished!")
    logger.info("Athena Shape:")
    logger.info(athena_df.shape)
    return athena_df


def create_reconciliation_status(athena_df, mambu_stream):
    """
    Creates a new table that has data points comparison
    between Mambu and Data Lake(Athena)
    """
    # Parse file that was produced with Singer (tap-mambu and target-jsonl)
    mambu_file = glob.glob(f"{mambu_stream}*.jsonl")
    if len(mambu_file) > 0:
        filename = mambu_file[0]
        data = []
        with open(filename) as f:
            for line in f:
                data.append(flatten(json.loads(line)))
        # create pandas dataframe
        input_df = pd.DataFrame(data)
    else:
        return False

    # process dataframe with the processing function defined in config
    logger.info(f"Calculating {mambu_stream} reconciliation table....")
    processed_df = getattr(
        sys.modules[__name__],
        config.mambudb_streams[mambu_stream]["processing_function"],
    )(input_df, athena_df)

    # This is for late settled transactions that are not picked up from live feed ETL: https://bb-2.atlassian.net/browse/NM-8763
    backfill_result = backfill_late_settled_deposit_transactions(
        processed_df, mambu_stream, input_df
    )

    # This block is only to execute if missing ids were backfilled for dep transactions
    if backfill_result[0]:
        # repeat reconciliation
        logger.info(f"Repeating reconciliation for {mambu_stream}!")
        sql_file = open(config.mambudb_streams[mambu_stream]["reconcile_sql_path"], "r")
        athena_totals_after_backfill_df = get_athena_df(sql_file)
        # override processed_df
        processed_df = getattr(
            sys.modules[__name__],
            config.mambudb_streams[mambu_stream]["processing_function"],
        )(input_df, athena_totals_after_backfill_df)

    # setup timestamp column
    now = datetime.utcnow()
    processed_df["timestamp_extracted"] = now

    # Write to s3/athena
    path = (
        "s3://" + os.environ["S3_RECONCILIATION"] + "/" + f"mambu_{mambu_stream}" + "/"
    )

    create_database_if_not_exists("datalake_reconciliation")

    try:
        logger.info("Uploading to S3 location:  %s", path)
        res = wr.s3.to_csv(
            df=processed_df,
            path=path,
            index=False,
            dataset=True,
            database="datalake_reconciliation",
            table=f"mambu_{mambu_stream}",
            mode="append",
            schema_evolution="true",
            dtype=config.mambudb_streams[mambu_stream]["schema"],
        )
        logger.info("Upload complete!")
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False

    # final count check
    check_counts(processed_df, mambu_stream, None, input_df)
    return res


def create_database_if_not_exists(database_name: str) -> dict:
    """
    Creates a database in Athena with the name provided if it doesn't already exist
    :param database_name: The name of the database
    :type database_name: str
    :return: The response
    :rtype: dict
    """

    if database_name not in wr.catalog.databases().values:
        res = wr.catalog.create_database(database_name)

        return res


def check_counts(
    full_df: pd.DataFrame, mambu_stream: str, caller_function=None, input_df=None
):
    """
    Check to see if the total rowcount for source matches the total rowcount for target
    :param full_df: DataFrame containing Source and Target rowcounts
    :type full_df: pd.DataFrame
    :param mambu_stream: Name of stream
    :type mambu_stream: str
    :param caller_function: Which function calls
    :type caller_function: str
    """

    logger.info("Checking counts... ")

    day_to_check = full_df["reconciliation_date"].drop_duplicates().tolist()[0]
    logger.info(f"Now checking counts for: {day_to_check}")

    for column_to_check in config.mambudb_streams[mambu_stream]["columns_to_check"]:
        target = full_df[full_df["reconciliation_date"] == day_to_check][
            f"target_{column_to_check}"
        ].mean()
        source = full_df[full_df["reconciliation_date"] == day_to_check][
            f"source_{column_to_check}"
        ].mean()

        if target < source:
            if os.environ.get("IS_PRODUCTION") and caller_function is None:
                logger.error(
                    f"{mambu_stream}: Row counts do not match... Target {column_to_check} = {target}; Source {column_to_check} = {source}"
                )
                debug_write_to_s3(input_df, mambu_stream, os.environ["S3_RAW"], "recon")
            else:
                logger.info(
                    f"{mambu_stream}: Row counts do not match... Target {column_to_check} = {target}; Source {column_to_check} = {source}"
                )
            return -1
        elif target > source:
            logger.info(
                f"{mambu_stream}: Row counts: Target {column_to_check} = {target}; Source {column_to_check} = {source}"
            )
            return -1
        else:
            logger.info(
                f"{mambu_stream}: Row counts match... Target {column_to_check}= {target}; Source {column_to_check} = {source}"
            )
            return 0


def process_stream(mambu_stream):
    logger.info(f"Running for {mambu_stream}...")
    # for gl_accounts we get the full table, therefore reconcile since beginning of capturing data at nomo
    if mambu_stream == "gl_accounts":
        state = {
            "bookmarks": {
                "gl_accounts": {
                    "ASSET": "2020-01-01T00:00:00.000000Z",
                    "LIABILITY": "2020-01-01T00:00:00.000000Z",
                    "EQUITY": "2020-01-01T00:00:00Z",
                    "INCOME": "2020-01-01T00:00:00.000000Z",
                    "EXPENSE": "2020-01-01T00:00:00.000000Z",
                }
            }
        }
    else:
        yesterday = (date.today() - timedelta(1)).strftime("%Y-%m-%dT00:00:00.000000Z")
        date_to_filter_from = yesterday
        state = {"bookmarks": {mambu_stream: date_to_filter_from}}

    # Download mambudb streams and convert to json
    mambu_statefile = "state.json"
    with open(mambu_statefile, "w") as statefile:
        json.dump(state, statefile)
    mambu_fetch_status = mambu_fetch(mambu_statefile, mambu_stream)

    # Run Athena queries
    sql_file = open(config.mambudb_streams[mambu_stream]["reconcile_sql_path"], "r")
    logger.info(f"Sql file  {sql_file}")
    athena_df = get_athena_df(sql_file)

    if mambu_fetch_status and not athena_df.empty:
        return create_reconciliation_status(athena_df, mambu_stream)
    else:
        return "No reconciliation for {0}".format(mambu_stream)


def main_routine():
    """
    Process each mambu stream for reconciliation
    """
    generate_tap_config("tap_config.json")
    return_res = []
    for mambu_stream in config.mambudb_streams.keys():
        res = process_stream(mambu_stream)
        return_res.append(res)

    logger.info(f"Finished running for {mambu_stream}...")
    return return_res


def lambda_handler(event, context):
    """
    Accepts a Kinesis Data Stream Event
    :param event: The event dict that contains the parameters sent when the function
                is invoked.
    :param context: The context in which the function is called.
    :return: The result of the specified action.
    """
    mambu_stream = event["mambu_stream"]

    # Cleanup of tmp dir due to Lambda caching
    shutil.rmtree("/tmp/", ignore_errors=True)

    # Change dir since Singer requires a dir that is writable;
    # only /tmp is; and there isn't a way to overwrite target-jsonl destination dir
    # shutil.copytree(os.getcwd(), "/tmp/", dirs_exist_ok=True)
    # os.chdir("/tmp/")
    # Selectively copy required files to /tmp/ instead of full copy (safer in Lambda)
    selective_copy(os.getcwd(), "/tmp/")
    os.chdir("/tmp/")

    # Get Singer filesfrom S3 to /tmp/
    s3 = boto3.client("s3")
    s3_bucket_name = os.environ["S3_META"]
    try:
        s3.download_file(s3_bucket_name, "mambu_meta/config/config.json", "config.json")
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False
    logger.info("Downloaded Mambu state S3.")

    # Generate config and run stream
    generate_tap_config("tap_config.json")
    result = process_stream(mambu_stream)
    logger.info("Finished reconciliation for stream: %s", mambu_stream)
    return {"status": "done", "stream": mambu_stream, "result": str(result)}
