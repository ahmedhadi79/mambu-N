import os
from datetime import datetime
from datetime import timezone

import awswrangler as wr
from api_client import APIClient
from data_catalog import schemas

from utils import fetch_data_switch
from utils import get_secret
from utils import get_start_time_from_athena
from utils import process_dataframe
from utils import setup_logger


logger = setup_logger("mambu_api_client_lambda")


def validate_event_inputs(event):
    """
    Validates and extracts inputs from the Lambda event payload.
    """
    required_keys = ["endpoint", "table_name", "request_type"]
    for key in required_keys:
        if key not in event:
            raise ValueError(f"Missing required event key: {key}")

    return {
        "endpoint": event["endpoint"],
        "request_type": event.get("request_type").lower(),
        "extra_params": event.get("extra_params", ""),
        "cdc_field": event.get("cdc_field", ""),
        "table_name": event["table_name"].lower(),
        "start_date": (
            datetime.strptime(event.get("start_date"), "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .isoformat()
            if event.get("start_date")
            else get_start_time_from_athena(
                event["table_name"], event.get("cdc_field", "")
            )
        ),
        "end_date": (
            datetime.strptime(event.get("end_date"), "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .isoformat()
            if event.get("end_date")
            else datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        ),
        "auto_schema": event.get("auto_schema", "False").lower() == "true",
        "rename_columns": event.get("rename_columns", []),
    }


def lambda_handler(event, context):
    """
    Main Lambda handler function.
    Processes data from Mambu API and writes it to S3 in Parquet format for Athena queries.
    """
    try:
        # Step 1: Initialize Environment Variables and Inputs
        mambu_subdomain = os.environ["MAMBU_SUBDOMAIN"]
        mambu_auth_path = os.environ["MAMBU_PASSWORD_NAME"]
        s3_raw = os.environ["S3_RAW"]

        event = validate_event_inputs(event)
        logger.info(f"Received event:\n{event}")

        base_url = f"https://{mambu_subdomain}.mambu.com/api/"
        path = f"s3://{s3_raw}/{event['table_name']}/"

        logger.info(
            f"Processing table: {event['table_name']}, Endpoint: {event['endpoint']}"
        )

        # Step 2: Initialize API Client
        logger.info("Initialing API Client.")
        mambu_client = APIClient(auth=get_secret(mambu_auth_path), base_url=base_url)
        logger.info("API Client initialized!")

        # Step 3: Fetch Data from API
        response_df = fetch_data_switch(
            mambu_client,
            event["endpoint"],
            event["request_type"],
            event["extra_params"],
            event["cdc_field"],
            event["start_date"],
            event["end_date"],
        )

        # Step 4: Process DataFrame
        if not response_df.empty:
            response_df, table_schema = process_dataframe(
                response_df,
                event["cdc_field"],
                event["rename_columns"],
                event["auto_schema"],
                event["table_name"],
                schemas,
            )

            # Step 5: Load processed response to Athena
            logger.info("Loading data to Athena.")
            wr.s3.to_parquet(
                df=response_df,
                path=path,
                index=False,
                dataset=True,
                database="datalake_raw",
                table=event["table_name"],
                mode="append",
                schema_evolution=True,
                compression="snappy",
                partition_cols=["date"],
                dtype=table_schema,
            )

        # Step 6: Return Response
        response = {
            "table_name": event["table_name"],
            "endpoint": event["endpoint"],
            "cdc_field": event["cdc_field"],
            "start_date": event["start_date"],
            "end_date": event["end_date"],
            "records_count": len(response_df),
        }
        logger.info(f"Lambda Response: {response}")
        return response

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise
