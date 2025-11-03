import glob
import json
import logging
import os
import subprocess
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Dict

import awswrangler as wr
import boto3
import data_catalog
import pandas as pd
from flatten_json import flatten
from selective_copy import selective_copy

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_new_date(ts, format="%Y-%m-%dT%H:%M:%S.%fZ"):
    """
    Helper function which adds 1 second to an input datetime object
    :param ts: The datetime object
    :return: The datetime object modified
    """
    dt_object = datetime.strptime(ts, format)
    dt_new = dt_object + timedelta(seconds=1)
    return dt_new.strftime(format)


def fix_mambu_state(mambu_statefile, mambu_streams_status):
    """
    Fix Mambu state by adding 1 second to each last_updated at timestamp
    This is required due to a bug in Mambu which results in fetching duplicates
    :param mambu_statefile: The name of the mambu statefile.
    :param mambu_streams_status: Dict containing info for each stream
    :return: The result of the specified action.
    """

    try:
        file = open(mambu_statefile)
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return e

    state = json.load(file)

    for stream in state["bookmarks"].keys():
        # in case there are streams that are no longer enabled, but are in state
        # do nothing for this stream
        if stream not in mambu_streams_status.keys():
            continue
        else:
            # only update if there has been a new file downloaded
            if mambu_streams_status[stream]:
                # gl accounts has substreams with different formats
                if stream == "gl_accounts":
                    for substream in state["bookmarks"][stream]:
                        # special format for equity substream
                        if substream == "EQUITY":
                            # get all since beginning of capturing
                            ts = "2020-01-01T00:00:00Z"
                            state["bookmarks"][stream][substream] = get_new_date(
                                ts, format="%Y-%m-%dT%H:%M:%SZ"
                            )
                        else:
                            # get all since beginning of capturing
                            ts = "2020-01-01T00:00:00.000000Z"
                            state["bookmarks"][stream][substream] = get_new_date(ts)
                else:
                    ts = state["bookmarks"][stream]
                    state["bookmarks"][stream] = get_new_date(ts)

    with open(mambu_statefile, "w") as outfile:
        json.dump(state, outfile)

    return True


def mambu_fetch_latest_catalog(mambudb_streams, get_from_remote, working_dir):
    """
    Run fetch command from Mambu for latest catalog
    tap-mambu --config tap_config.json --discover > catalog.json
    :param mambudb_streams: The streams dict to download
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
                subprocess.run(catalog_command, stdout=catalog_output)

            logger.info("Downloaded latest catalog from Mambu")

        # opening downloaded file
        f = open("catalog_temp.json")
        catalog = json.load(f)

        # adding required field "selected" for downloading in next step
        for s in catalog["streams"]:
            if s["stream"] in list(mambudb_streams.keys()):
                logger.info("Enabling stream:  %s", s["stream"])
                s["metadata"][0]["metadata"]["selected"] = True

        # writing result to local directory as catalog.json
        with open("catalog.json", "w") as outfile:
            json.dump(catalog, outfile, indent=4)

        return True
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False


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
    logger.info("Issuing command to download data from Mambu.")
    if working_dir is None:
        working_dir = os.getcwd()

    logger.info("Getting latest catalog from Mambu for configured streams..")
    mambu_fetch_latest_catalog(mambudb_streams, True, working_dir)
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
        logger.error("Exception occurred:  %s", e)
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
        logger.error("Exception occurred:  %s", e)
        return e


def write_to_s3(mambudb_streams, input_df, mambudb_stream):
    """
    Writes mambu data to the data lake
    :param mambudb_streams: A dict of mambu streams info
    :param input_df: the data in the form of a pandas dataframe
    :param path: The s3 path to write to
    :param mambudb_stream: The mambu stream for this iteration
    :return: The result of the specified action.
    """
    logger.info("Processing Mambu Stream for an Athena write:  %s", mambudb_stream)

    # fix for timestamp format coming from Mambu
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
    # custom_field_sets is a metadata table
    if mambudb_stream == "gl_accounts":
        path = "s3://" + os.environ["S3_RAW"] + "/" + mambudb_stream + "/"
        logger.info("Uploading to S3 location:  %s", path)
        res = wr.s3.to_parquet(
            df=input_df,
            path=path,
            index=False,
            dataset=True,
            database="datalake_raw",
            table=mambudb_stream,
            mode="overwrite_partitions",
            schema_evolution=True,
            compression="snappy",
            partition_cols=mambudb_streams[mambudb_stream],
            dtype=data_catalog.schemas[mambudb_stream],
            glue_table_settings=wr.typing.GlueTableSettings(
                columns_comments=data_catalog.column_comments[mambudb_stream]
            ),
        )

    logger.info("Write to Athena complete!")

    return res


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
    return athena_df


def fix_for_users(input_df: pd.DataFrame):
    """
    In case new "access" columns show up in the future
    make sure they are casted to boolean or string
    """

    def fix(input_df, column):
        try:
            input_df[column] = input_df[column].astype(bool)
            # logger.info("Casted to boolean.")
        except Exception as e:
            logger.info(e)
            logger.info("Cannot cast to boolean, casting to string.")
            input_df[column] = input_df[column].astype(str)
            logger.info("Casted to string.")
        return input_df

    schema = data_catalog.schemas["users"]
    for column in input_df.columns:
        if column in schema:
            if schema[column] == "boolean":
                input_df = fix(input_df, column)
                continue
        elif column.startswith("access"):
            input_df = fix(input_df, column)

    return input_df


def parse_write_to_athena(mambudb_streams: Dict):
    """
    Create a loop for each selected stream,
    parse and write to athena/glue
    :param mambudb_streams: A dict of mambu streams info
    :return: The result of the specified action.
    """
    mambu_streams_status = dict.fromkeys(mambudb_streams.keys(), None)
    input_df = pd.DataFrame()

    for mambudb_stream in mambudb_streams.keys():
        logger.info("Processing Mambu Stream:  %s", mambudb_stream)

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
            logger.info("Pandas DF shape:  %s", input_df.shape)

            # Define static vars
            input_df["date"] = date.today().strftime("%Y%m%d")
            input_df["timestamp_extracted"] = datetime.utcnow()

            try:
                # Write to S3 Raw, also creates Glue catalog entry and updates Athena table
                res = write_to_s3(mambudb_streams, input_df, mambudb_stream)
                logger.info("Result:  %s", res)
                logger.info("Upload to Athena complete.")
                mambu_streams_status[mambudb_stream] = True
            except Exception as e:
                logger.error("Pandas DF:  %s", input_df)
                logger.error("Exception occurred:  %s", e)
                mambu_streams_status[mambudb_stream] = False
        else:
            logger.info(
                "Singer did not return any new data, continuing to the next configured stream"
            )
            mambu_streams_status[mambudb_stream] = False
            continue

    return mambu_streams_status


def check_file_exists(s3_client, bucket, key):
    """
    Checks if a key exists in an s3 bucket
    :param s3_client: The S3 client from boto3
    :param bucket: The S3 bucket
    :param key: The S3 key
    :return: The result of the specified action.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.error(e)
        return False
    return True


def get_secret(secret_name):
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


def generate_tap_config(filepath):
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


def lambda_handler(event, context):
    """
    Accepts a Kinesis Data Stream Event
    :param event: The event dict that contains the parameters sent when the function
                is invoked.
    :param context: The context in which the function is called.
    :return: The result of the specified action.
    """

    begin = time.time()

    # Name of mambudb stream and the partition cols for each stream in Athena
    mambudb_streams = {"gl_accounts": ["date"]}

    # Prep steps
    mambu_statefile = "state.json"
    s3 = boto3.client("s3")
    s3_bucket_name = os.environ["S3_META"]
    s3_mambu_statefile = "mambu_meta/latest/{0}".format(mambu_statefile)
    # check if this is the first time running mambu extract
    if not check_file_exists(s3, s3_bucket_name, s3_mambu_statefile):
        s3_mambu_statefile = "mambu_meta/initial/{0}".format(mambu_statefile)
    s3_mambu_statefile_archive = "mambu_meta/archive/{0}".format(mambu_statefile)

    # Cleanup of tmp dir due to Lambda caching
    cleanup_dir("/tmp/*.jsonl")
    cleanup_dir("/tmp/*.json")

    # Change dir since Singer requires a dir that is writable; only /tmp is; and there isn't a way to overwrite target-jsonl destination dir
    # shutil.copytree(os.getcwd(), "/tmp/", dirs_exist_ok=True)
    # os.chdir("/tmp/")
    # Selectively copy required files to /tmp/ instead of full copy (safer in Lambda)
    selective_copy(os.getcwd(), "/tmp/")
    os.chdir("/tmp/")

    # Get Singer files from S3 to /tmp/ (state and config)
    try:
        s3.download_file(s3_bucket_name, s3_mambu_statefile, mambu_statefile)
        s3.download_file(s3_bucket_name, "mambu_meta/config/config.json", "config.json")
        logger.info("Downloaded latest Mambu state S3.")
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False

    # Generate tap config (credentials)
    generate_tap_config("tap_config.json")

    # Download mambudb streams and convert to json
    mambu_fetch(mambu_statefile, mambudb_streams)

    # For each stream, parse json and write to Data Lake
    # get dict back with stream: updated_or_not flag, based on that, fix schema
    mambu_streams_status = parse_write_to_athena(mambudb_streams)

    # Upload latest state to be used for next run, i.e. the output of the pipe_command
    fix_mambu_state("latest_" + mambu_statefile, mambu_streams_status)
    s3_mambu_statefile = "mambu_meta/latest/{0}".format(mambu_statefile)
    try:
        s3.upload_file("latest_" + mambu_statefile, s3_bucket_name, s3_mambu_statefile)
        s3.upload_file(
            mambu_statefile,
            s3_bucket_name,
            s3_mambu_statefile_archive
            + "_"
            + datetime.now().strftime("%Y%m%d_%H%M%S")
            + ".json",
        )
        logger.info("Uploading latest state to S3 complete.")
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        return False

    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin)/60):.2f}"
    )
    return True
