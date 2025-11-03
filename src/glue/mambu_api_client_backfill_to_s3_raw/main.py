# This function simply runs "api-client-lambda-to-s3-raw" lambda_handler for an extended time.
# Parallel workers are disabled not to overwhelm the Mambu API.
import os
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Dict
from typing import List
from typing import Tuple

from awsglue.utils import getResolvedOptions
from lambda_function import lambda_handler

from utils import setup_logger

##########################################################
# CAREFULLY modify the below event before RUNNING the script.
# Note: (It is always "append" mode!)
event = {
    "table_name": "target_athena_table_name",  # Required
    "endpoint": "mambu:api_endpoint",  # Required
    "request_type": "Post",  # Required
    "cdc_field": "lastModifiedDate",  # Required for POST requests
    "start_date": "0000-00-00 00:00:00",  # Required, format: %Y-%m-%d %H:%M:%S
    "end_date": "0000-00-00 00:00:00",  # Required, format: %Y-%m-%d %H:%M:%S
    "extra_params": "",  # Optional
    "auto_schema": "False",  # Optional, True only if table is not in data_catalog.py
    "rename_columns": [],  # Optional, list of dicts [{"old_name": "new_name"}]
    "chunk_hours": 24,  # Optional, Int, default 24 per lambda_handler call
}
##########################################################
# SAMPLE EVENTS (append start_date and end_date to the event below):
#   {
#     "table_name": "mambu_gl_journal_entries",
#     "endpoint": "gljournalentries:search",
#     "request_type": "post",
#     "cdc_field": "creationDate",
#   },
#   {
#     "table_name": "mambu_loan_accounts",
#     "endpoint": "loans:search",
#     "request_type": "post",
#     "cdc_field": "lastModifiedDate",
#   },
#   {
#     "table_name": "mambu_loan_transactions",
#     "endpoint": "loans/transactions:search",
#     "request_type": "post",
#     "cdc_field": "creationDate",
#   },
#   {
#     "table_name": "mambu_deposit_accounts",
#     "endpoint": "deposits:search",
#     "request_type": "post",
#     "cdc_field": "lastModifiedDate",
#   },
#   {
#     "table_name": "mambu_deposit_transactions",
#     "endpoint": "deposits/transactions:search",
#     "request_type": "post",
#     "cdc_field": "creationDate",
#   },
#   {
#     "table_name": "mambu_clients",
#     "endpoint": "clients:search",
#     "request_type": "post",
#     "cdc_field": "lastModifiedDate",
#   },
#   {
#     "table_name": "mambu_accounting_interestaccrual",
#     "endpoint": "accounting/interestaccrual:search",
#     "request_type": "post",
#     "cdc_field": "creationDate",
#   },
#   {
#     "table_name": "mambu_groups",
#     "endpoint": "groups:search",
#     "request_type": "post",
#     "cdc_field": "lastModifiedDate",
#   },
#   {
#     "table_name": "mambu_users",
#     "endpoint": "users",
#     "request_type": "get"
#   },
#   {
#     "table_name": "mambu_gl_accounts",
#     "endpoint": "glaccounts",
#     "request_type": "get"
#   },
#   {
#     "table_name": "mambu_loan_accounts_installments",
#     "endpoint": "installments",
#     "request_type": "get"
#   },
##########################################################

logger = setup_logger("mambu_api_client_glue")


def setup_environment() -> bool:
    """
    Set up environment variables from AWS Glue job parameters.

    Returns:
        bool: success/fail
    """
    try:
        args = getResolvedOptions(
            sys.argv, ["MAMBU_SUBDOMAIN", "MAMBU_PASSWORD_NAME", "S3_RAW"]
        )

        os.environ["MAMBU_SUBDOMAIN"] = args["MAMBU_SUBDOMAIN"]
        os.environ["MAMBU_PASSWORD_NAME"] = args["MAMBU_PASSWORD_NAME"]
        os.environ["S3_RAW"] = args["S3_RAW"]

        return True
    except Exception as e:
        logger.error(f"Failed to set up environment variables: {str(e)}")
        return False


def parse_date_range(event: Dict) -> Tuple[datetime, datetime]:
    """
    Parse start and end dates from the event dictionary.
    """
    try:
        start_date_str = event.get("start_date")
        end_date_str = event.get("end_date")

        if not start_date_str or not end_date_str:
            raise ValueError("Missing start_date or end_date in event")

        start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )

        logger.info(f"Full date range to process: {start_date_obj} to {end_date_obj}")
        return start_date_obj, end_date_obj
    except ValueError as e:
        logger.error(f"Date parsing error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error parsing dates: {str(e)}")
        raise


def process_data_in_chunks(
    event: Dict, start_date: datetime, end_date: datetime
) -> List[Dict]:
    """
    Process data in 24-hour chunks and track successes and failures.

    Args:
        event (Dict): The original event dictionary
        start_date (datetime): Start date/time for processing
        end_date (datetime): End date/time for processing
    """
    current_start = start_date
    chunk_size = int(event.get("chunk_hours", 24))
    chunk_num = 1
    all_chunks: List[Dict] = []

    # Calculate total duration for logging purposes
    total_duration = end_date - start_date
    total_hours = total_duration.total_seconds() / 3600
    logger.info(
        f"Total duration: {total_hours:.2f} hours to be processed in {chunk_size}-hour chunks"
    )

    while current_start < end_date:
        # (either 24 hours later or the final end date)
        current_end = min(current_start + timedelta(hours=chunk_size), end_date)

        chunk_info: Dict = {
            "chunk_num": chunk_num,
            "start_date": current_start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": current_end.strftime("%Y-%m-%d %H:%M:%S"),
        }

        logger.info(f"Processing chunk {chunk_num}: {current_start} to {current_end}")

        chunk_event = event.copy()
        chunk_event["start_date"] = chunk_info["start_date"]
        chunk_event["end_date"] = chunk_info["end_date"]

        try:
            result = lambda_handler(chunk_event, None)

            chunk_info["status"] = "success"
            chunk_info["records_count"] = result.get("records_count")
            logger.info(f"Successfully processed chunk {chunk_num}")
        except Exception as e:
            chunk_info["status"] = "failure"
            chunk_info["error"] = str(e)
            logger.error(f"Error processing chunk {chunk_num}: {str(e)}")
            logger.warning(
                f"Continuing with next chunk despite error in chunk {chunk_num}"
            )

        all_chunks.append(chunk_info)

        current_start = current_end
        chunk_num += 1

    return all_chunks


def pretty_processing_summary(all_chunks: List[Dict]) -> None:
    """
    Print a detailed summary of successful and failed processing chunks in one logger.info call.
    """
    if not all_chunks:
        logger.warning("No chunks to summarize.")
        return

    total_chunks = len(all_chunks)
    total_success_chunks = len(
        [chunk for chunk in all_chunks if chunk["status"] == "success"]
    )

    summary_lines = [""]
    summary_lines.append("=" * 60)
    summary_lines.append(f"PROCESSING SUMMARY: Completed {total_chunks} chunks")
    summary_lines.append("=" * 60)

    summary_lines.append(f"SUCCESSFUL CHUNKS: {total_success_chunks}/{total_chunks}")
    summary_lines.append("=" * 60)
    summary_lines.append("Detailed Chunk Status:")
    summary_lines.append("=" * 60)
    summary_lines.append(
        "Chunk Number | Start Date          | End Date            | Extras"
    )

    for chunk in all_chunks:
        chunk_status = chunk.get("status")
        if chunk_status == "success":
            summary_lines.append(
                f"  ✓ Chunk {chunk['chunk_num']}: {chunk['start_date']} to {chunk['end_date']}: {chunk['records_count']} records"
            )
        elif chunk_status == "failure":
            summary_lines.append(
                f"  ✗ Chunk {chunk['chunk_num']}: {chunk['start_date']} to {chunk['end_date']}: {chunk['error']}"
            )
        else:
            summary_lines.append(
                f"  ? Chunk {chunk['chunk_num']}: {chunk['start_date']} to {chunk['end_date']}: Unknown status"
            )

    summary_lines.append("=" * 60)

    logger.info("\n".join(summary_lines))


def main() -> int:
    """
    Main function to orchestrate the data process.
    Returns:
        int: Exit code indicating the result of the process
            0: Complete success (all chunks processed successfully)
            1: Complete failure (entire process failed)
            2: Partial success (some chunks failed)
    """
    try:
        logger.info("Starting Mambu API data extraction process")

        # STEP1: Set up environment
        if not setup_environment():
            logger.error("Failed to set up environment. Exiting.")
            return 1

        # STEP2: Parse date range
        try:
            start_date, end_date = parse_date_range(event)
        except Exception:
            logger.error("Failed to parse date range. Exiting.")
            return 1

        # STEP3: Process data in chunks
        all_chunks = process_data_in_chunks(event, start_date, end_date)

        # STEP4: Print summary report
        pretty_processing_summary(all_chunks)

        # STEP5: Determine exit code based on failures
        failed_chunks = [chunk for chunk in all_chunks if chunk["status"] == "failure"]
        if failed_chunks:
            logger.warning(
                f"[Success with Errors] Process completed with {len(failed_chunks)} failed chunks"
            )
            return 2  # Partial success
        else:
            total_records = sum(chunk.get("records_count", 0) for chunk in all_chunks)
            logger.info(
                f"[Success] Process completed successfully with {total_records} records processed"
            )
            return 0  # Complete success
    except Exception:
        logger.exception("[Error] Main function failed")
        return 1  # Complete failure


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
