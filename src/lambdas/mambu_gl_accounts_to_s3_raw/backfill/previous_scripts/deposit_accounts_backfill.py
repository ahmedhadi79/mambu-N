import argparse
import glob
import json
import logging
import os
import subprocess
import sys
from datetime import date
from datetime import datetime

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
        catalog_command = "venv-backfill-mambu/bin/tap-mambu --config {0}/tap_config.json --discover".format(
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
    main_command = "venv-backfill-mambu/bin/tap-mambu --config {0}/tap_config.json --catalog {0}/catalog.json --state {0}/state.json".format(
        working_dir
    ).split(
        " "
    )
    pipe_command = (
        "venv-backfill-mambu/bin/target-jsonl --config {0}/tap_config.json".format(
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
    state = {"bookmarks": {"deposit_accounts": "2020-01-01T00:00:00Z"}}
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


def write_to_athena(athena_table: str, input_df: pd.DataFrame, w_mode: str):
    path = f"s3://" + os.environ["S3_RAW"] + "/" + athena_table + "/"
    print("Uploading to S3 location:  %s", path)
    res = wr.s3.to_parquet(
        df=input_df,
        path=path,
        index=False,
        dataset=True,
        database="datalake_raw",
        table=athena_table,
        mode=w_mode,
        schema_evolution="true",
        compression="snappy",
        partition_cols=["date"],
        dtype=data_catalog.schemas["deposit_accounts"],
        columns_comments=data_catalog.column_comments["deposit_accounts"],
    )

    return res


def run(profile_name: str):
    boto3.setup_default_session(profile_name=profile_name)

    mambudb_streams = {"deposit_accounts": ["date"]}

    print("getting mambu data...")
    mambu_df = get_mambu(mambudb_streams)

    print("getting athena data...")
    athena_deposit_accounts_df = wr.athena.read_sql_query(
        sql="SELECT DISTINCT id FROM datalake_raw.deposit_accounts",
        database="datalake_raw",
    )
    print("athena accounts: ")
    print(athena_deposit_accounts_df.shape[0])

    all_mambu_deposit_accounts = mambu_df[["id"]].drop_duplicates()
    print("mambu accounts: ")
    print(all_mambu_deposit_accounts.shape[0])
    # merge to see which are missing
    fulldf = all_mambu_deposit_accounts.merge(
        athena_deposit_accounts_df,
        how="outer",
        indicator=True,
        left_on="id",
        right_on="id",
    )
    not_in_data_lake = fulldf[fulldf["_merge"] == "left_only"].drop_duplicates()
    print("Accounts not in data lake: ")
    print(not_in_data_lake.shape[0])
    not_in_data_lake.to_csv("not_in_data_lake.csv", index=False)
    not_in_data_lake_ids = not_in_data_lake[["id"]].drop_duplicates()

    not_in_mambu = fulldf[fulldf["_merge"] == "right_only"].drop_duplicates()
    print("Accounts not in Mambu: ")
    print(not_in_mambu.shape[0])

    in_both = fulldf[fulldf["_merge"] == "both"].drop_duplicates()
    print("Accounts both in Mambu and Data Lake: ")
    print(in_both.shape[0])

    to_backfill_df = mambu_df.merge(
        not_in_data_lake_ids, how="inner", indicator=True, left_on="id", right_on="id"
    )
    print("Accounts to backfill: ")
    print(to_backfill_df.shape[0])

    if to_backfill_df.shape[0] == 0:
        print("No accounts to backfill.. exiting.")
    else:
        to_backfill_df.drop("_merge", axis=1, inplace=True)
        # print("Backfilling...")

        # res = write_to_athena(
        #     athena_table="deposit_accounts",
        #     input_df=to_backfill_df,
        #     w_mode="append",
        # )
        # print(res)
        # print("Backfilling complete!")
        # print("Validating.. ")
        # athena_deposit_accounts_df_backfilled = wr.athena.read_sql_query(sql="SELECT DISTINCT id FROM datalake_raw.deposit_accounts_backfill", database="datalake_raw")
        # print(athena_deposit_accounts_df_backfilled.shape[0])


# python deposit_accounts_backfill.py --profile bb2-beta-admin
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--profile-name", action="store", dest="profile_name", help="aws profile name"
    )

    args = parser.parse_args()
    print("starting run... ")
    run(args.profile_name)
