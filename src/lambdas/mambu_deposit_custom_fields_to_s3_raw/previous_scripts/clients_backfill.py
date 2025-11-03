import argparse
import glob
import json
import logging
import os
import subprocess
import sys
from datetime import date
from datetime import datetime
from typing import Dict
from typing import List

import awswrangler as wr
import boto3
import pandas as pd
from flatten_json import flatten

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sys.path.append(os.path.abspath("../"))

import data_catalog


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager
    :param secret_name: The key to retrieve
    :return: The value of the secret
    """
    print("Retrieving :  %s", secret_name)
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


def mambu_fetch(
    mambu_statefile,
    mambudb_streams,
    working_dir=None,
):
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
                print("Pandas DF shape:  %s", input_df.shape)

                # Define static vars
                input_df["date"] = date.today().strftime("%Y%m%d")
                input_df["timestamp_extracted"] = datetime.utcnow()

        return input_df, mambu_streams_status
    except Exception as e:
        logger.error("Pandas DF:  %s", input_df)
        logger.error("Exception occurred:  %s", e)
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

    return df


def get_athena() -> pd.DataFrame:
    df = wr.athena.read_sql_query(
        sql="select * from datalake_raw.clients",
        database="datalake_raw",
        workgroup="datalake_workgroup",
    )

    return df


def left_anti_join(athena_df: pd.DataFrame, mambu_df: pd.DataFrame) -> pd.DataFrame:
    timestamp_cols = {
        k: v
        for (k, v) in data_catalog.schemas["clients"].items()
        if v == "timestamp" and k not in ("timestamp_extracted")
    }
    for col in timestamp_cols:
        if col in mambu_df.columns:
            mambu_df[col] = pd.to_datetime(
                mambu_df[col], format="%Y-%m-%dT%H:%M:%S.%fZ"
            )

    join_cols = ["id", "encoded_key", "state", "last_modified_date"]
    join_cols_athena_df = athena_df[join_cols]

    merged_df = pd.merge(
        mambu_df, join_cols_athena_df, on=join_cols, how="outer", indicator=True
    )

    mambu_only = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])

    return mambu_only


def write_to_athena(
    mambudb_stream: str, input_df: pd.DataFrame, mambudb_streams: Dict[str, List]
):
    path = f"s3://" + os.environ["S3_RAW"] + "/" + mambudb_stream + "/"
    print("Uploading to S3 location:  %s", path)
    res = wr.s3.to_parquet(
        df=input_df,
        path=path,
        index=False,
        dataset=True,
        database="datalake_raw",
        table=mambudb_stream,
        mode="append",
        schema_evolution="true",
        compression="snappy",
        partition_cols=mambudb_streams[mambudb_stream],
        dtype=data_catalog.schemas[mambudb_stream],
        columns_comments=data_catalog.column_comments[mambudb_stream],
    )

    return res


def run(profile_name: str):
    boto3.setup_default_session(profile_name=profile_name)

    mambudb_streams = {"clients": ["date"]}

    print("getting mambu data...")
    mambu_df = get_mambu(mambudb_streams)

    print("getting athena data...")
    athena_df = get_athena()

    mambu_only = left_anti_join(athena_df, mambu_df)
    print(mambu_only)
    print(os.environ["S3_RAW"])

    res = write_to_athena(
        mambudb_stream="clients", input_df=mambu_only, mambudb_streams=mambudb_streams
    )
    print(res)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--profile-name", action="store", dest="profile_name", help="aws profile name"
    )

    args = parser.parse_args()
    print("starting run... ")
    run(args.profile_name)
