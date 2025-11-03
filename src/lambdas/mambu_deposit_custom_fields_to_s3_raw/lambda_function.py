import glob
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date
from datetime import datetime

import awswrangler as wr
import boto3
import data_catalog
import pandas as pd
from flatten_json import flatten
from selective_copy import selective_copy

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sys.path.append(os.path.abspath("../"))


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager
    :param secret_name: The key to retrieve
    :return: The value of the secret
    """
    print("Retrieving: ", secret_name)
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        secret = secret_value["SecretString"]
        return json.loads(secret)["MAMBU_API_PASSWORD"]
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


def mambu_fetch_latest_catalog(mambudb_streams, get_from_remote, working_dir):
    """
    Run fetch command from Mambu for latest catalog
    tap-mambu --config tap_config.json --discover > catalog.json
    :param mambudb_streams: The streams dict to download
    :param get_from_remote: Whether to download from remote
    :param working_dir: The working directory
    :return: The result of the specified action.
    """
    if get_from_remote:
        catalog_command = "python /opt/package/tap-mambu --config {0}/tap_config.json --discover".format(
            working_dir
        ).split(
            " "
        )
        print("Tap mambu catalog command:  %s", catalog_command)
        with open("catalog_temp.json", "w") as catalog_output:
            subprocess.run(catalog_command, stdout=catalog_output)

        print("Downloaded latest catalog from Mambu")

    # opening downloaded file
    f = open("catalog_temp.json")
    catalog = json.load(f)

    # adding required field "selected" for downloading in next step
    for s in catalog["streams"]:
        if s["stream"] in list(mambudb_streams.keys()):
            print("Enabling stream:  %s", s["stream"])
            s["metadata"][0]["metadata"]["selected"] = True

    # writing result to local directory as catalog.json
    with open("catalog.json", "w") as outfile:
        json.dump(catalog, outfile, indent=4)

    return True


def mambu_fetch(mambu_statefile, mambudb_streams, working_dir=None):
    """
    Run fetch command from Mambu
    tap-mambu --config tap_config.json --catalog catalog.json --state state.json | target-jsonl > latest_state.json
    :param mambu_statefile: The name of the mambu statefile.
    :return: The result of the specified action.
    """
    print("Issuing command to download data from Mambu.")
    if working_dir is None:
        working_dir = os.getcwd()

    print("Getting latest catalog from Mambu for configured streams..")
    mambu_fetch_latest_catalog(mambudb_streams, True, working_dir)
    print("Mambu catalog fetched!")

    # Issue command to Mambu API
    main_command = "python /opt/package/tap-mambu --config {0}/tap_config.json --catalog {0}/catalog.json --state {0}/state.json".format(
        working_dir
    ).split(
        " "
    )
    pipe_command = (
        "python /opt/package/target-jsonl --config {0}/tap_config.json".format(
            working_dir
        ).split(" ")
    )
    print("Tap mambu command:  %s", main_command)
    out_main_command = subprocess.run(main_command, check=True, capture_output=True)
    print("Target jsonl command:  %s", pipe_command)
    with open(mambu_statefile, "w") as state:
        subprocess.run(pipe_command, input=out_main_command.stdout, stdout=state)
    print("Configured streams have been downloaded from Mambu.")
    return True


def parse(mambudb_streams):
    """
    Create a loop for each selected stream,
    parse
    :param mambudb_streams: A dict of mambu streams info
    :return: The result of the specified action.
    """
    mambu_streams_status = dict.fromkeys(mambudb_streams.keys(), None)
    input_df = pd.DataFrame()
    try:
        for mambudb_stream in mambudb_streams.keys():
            print("Processing Mambu Stream:  %s", mambudb_stream)

            # Check if file exists
            mambu_file = glob.glob("{0}*.jsonl".format(mambudb_stream))

            # Parse file that was produced with Singer (tap-mambu and target-jsonl)
            if len(mambu_file) > 0:
                filename = mambu_file[0]
                data = []
                with open(filename) as f:
                    for line in f:
                        data.append(flatten(json.loads(line)))

                # fix issue that data wrangler cannot write empty dicts or lists to parquet in S3
                for i, item in enumerate(data):
                    for col in item:
                        if data[i][col] in [{}, []]:
                            data[i][col] = ""

                # create pandas dataframe
                input_df = pd.DataFrame(data)
                # Define static vars
                input_df["date"] = date.today().strftime("%Y%m%d")
                input_df["timestamp_extracted"] = datetime.utcnow()

        input_df_filtered = input_df.loc[
            (
                (
                    input_df.transaction_details_transaction_channel_id
                    == "Wise_Local_Payments"
                )
                | (
                    input_df.transaction_details_transaction_channel_id.str.startswith(
                        "Card_", na=False
                    )
                )
                | (
                    input_df.transaction_details_transaction_channel_id
                    == "Paymentology-FastLite"
                )
            )
            & (input_df.custom_fields_0_id.str.len() > 0)
        ]

        input_df_selection = input_df_filtered[
            [
                "id",
                "encoded_key",
                "custom_fields_0_id",
                "custom_fields_0_field_set_id",
                "custom_fields_0_value",
                "custom_fields_1_field_set_id",
                "custom_fields_1_id",
                "custom_fields_1_value",
                "custom_fields_2_field_set_id",
                "custom_fields_2_id",
                "custom_fields_2_value",
                "custom_fields_3_field_set_id",
                "custom_fields_3_id",
                "custom_fields_3_value",
                "custom_fields_4_field_set_id",
                "custom_fields_4_id",
                "custom_fields_4_value",
                "date",
                "timestamp_extracted",
            ]
        ]
        print("Pandas DF shape:  %s", input_df_selection.shape)

        return input_df_selection, mambu_streams_status
    except Exception as e:
        logger.error("Pandas DF:  %s", input_df)
        logger.error("Exception occurred in parse:  %s", e)
        return e


def generate_tap_config(filepath):
    """
    Creates a JSON file which acts as input to tap mambu commands
    :param filepath: The filepath at which the JSON file is written to
    :return: The result of the specified action.
    """
    try:
        print("Generating tap configuration file..")
        tap_config = {}

        tap_config["username"] = os.environ.get("MAMBU_USERNAME")
        tap_config["password"] = get_secret(os.environ.get("MAMBU_PASSWORD_NAME"))
        tap_config["apikey"] = ""
        tap_config["subdomain"] = os.environ.get("MAMBU_SUBDOMAIN")
        tap_config["start_date"] = "2021-01-01T00:00:00Z"
        tap_config["user_agent"] = os.environ.get("MAMBU_USER_AGENT")

        with open(filepath, "w") as tap:
            json.dump(tap_config, tap, indent=4)

        print("Tap config written in:  %s", filepath)

        return True
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


def get_mambu(mambudb_streams):
    print("generating state...")
    state = {"bookmarks": {"clients": "2020-01-01T00:00:00Z"}}
    mambu_statefile = "state.json"

    with open(mambu_statefile, "w") as statefile:
        json.dump(state, statefile)

    print("generating tap config...")
    generate_tap_config("tap_config.json")

    print("fetching mambu data...")
    mambu_fetch(
        mambu_statefile,
        mambudb_streams,
    )
    df, mambu_streams_status = parse(mambudb_streams)
    print("Parsing finished successfully!")
    return df


def write_to_athena(athena_table: str, input_df: pd.DataFrame):
    path = "s3://" + os.environ["S3_RAW"] + "/" + athena_table + "/"
    print("Uploading to S3 location: %s", path)
    try:
        res = wr.s3.to_parquet(
            df=input_df,
            path=path,
            index=False,
            dataset=True,
            database="datalake_raw",
            table=athena_table,
            mode="overwrite",
            schema_evolution=True,
            compression="snappy",
            partition_cols=["date"],
            dtype=data_catalog.schemas["deposit_transactions"],
            glue_table_settings=wr.typing.GlueTableSettings(
                columns_comments=data_catalog.column_comments["deposit_transactions"]
            ),
        )
    except Exception as e:
        logger.error("Exception occurred in writing athena: %s", e)
        return e

    return res


def run(profile_name: str):
    boto3.setup_default_session(profile_name=profile_name)

    mambudb_streams = {"deposit_transactions": ["date"]}

    print("getting mambu data...")
    mambu_df = get_mambu(mambudb_streams)
    print(mambu_df.columns)

    res = write_to_athena(
        athena_table="deposit_transactions_wise_custom_fields_backfill",
        input_df=mambu_df,
    )
    print(res)


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
        logger.error("Exception occurred:  %s", e)
        return e


def lambda_handler(event, context):
    begin = time.time()
    mambudb_streams = {"deposit_transactions": ["date"]}

    print("getting mambu data...")
    try:
        # Cleanup of tmp dir due to Lambda caching
        cleanup_dir("/tmp/*.jsonl")
        cleanup_dir("/tmp/*.json")

        # Change dir since Singer requires a dir that is writable; only /tmp is; and there isn't a way to overwrite target-jsonl destination dir
        # shutil.copytree(os.getcwd(), "/tmp/", dirs_exist_ok=True)
        # os.chdir("/tmp/")
        # Selectively copy required files to /tmp/ instead of full copy (safer in Lambda)
        selective_copy(os.getcwd(), "/tmp/")
        os.chdir("/tmp/")

        mambu_df = get_mambu(mambudb_streams)
        print(mambu_df.columns)
        logger.info("Pandas DF:  %s", mambu_df)
        res = write_to_athena(
            athena_table="deposit_transactions_wise_custom_fields_backfill",
            input_df=mambu_df,
        )
        logger.info("Write to Athena complete!")
        logger.info("Result:  %s", res)
    except Exception as e:
        logger.error("Exception occurred in main:  %s", e)
        return False
    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin) / 60):.2f}"
    )
    return True
