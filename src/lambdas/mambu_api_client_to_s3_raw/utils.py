import ast
import json
import logging
import re
import sys
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional

import awswrangler as wr
import boto3
import pandas as pd
from api_client import APIClient


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename: Optional[str] = None,
) -> logging.Logger:
    """
    Sets up a logger with the specified configuration.

    Parameters:
    - name (Optional[str]): Name of the logger. If None, the root logger is used.
    - level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    - format (str): Log message format.
    - filename (Optional[str]): If specified, logs will be written to this file. Otherwise, logs are written to stdout.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    # To avoid duplicate handlers being added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


logger = setup_logger("mambu_api_client_utils")


def get_secret(secret_name: str) -> str:
    """
    Retrieves a specific secret value from AWS Secrets Manager.

    :param secret_name: The key of the secret to retrieve.
    :return: The value of the secret as a string.
    :raises KeyError: If 'MAMBU_API_KEY' is not found in the secret data.
    :raises ValueError: If the secret string is not valid JSON or missing.
    :raises RuntimeError: For other unexpected errors during secret retrieval.
    """
    logger.info("Retrieving secret: %s", secret_name)
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)

        secret_string = secret_value.get("SecretString")
        if not secret_string:
            raise ValueError(f"Secret string not found for secret: {secret_name}")

        secret_data = json.loads(secret_string)
        if "MAMBU_API_KEY" not in secret_data:
            raise KeyError(f"'MAMBU_API_KEY' key not found in secret: {secret_name}")

        return secret_data["MAMBU_API_KEY"]

    except boto3.exceptions.Boto3Error as boto_error:
        logger.error("Boto3 error occurred: %s", boto_error)
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_name}': {boto_error}"
        ) from boto_error
    except json.JSONDecodeError as json_error:
        logger.error("Failed to parse secret string: %s", json_error)
        raise ValueError(
            f"Invalid JSON in secret '{secret_name}': {json_error}"
        ) from json_error
    except KeyError as key_error:
        logger.error("Key error occurred: %s", key_error)
        raise
    except Exception as e:
        logger.error("Unexpected error occurred: %s", e)
        raise RuntimeError(
            f"An unexpected error occurred while retrieving secret '{secret_name}': {e}"
        ) from e


def get_actual_dtypes(df: pd.DataFrame) -> dict:
    """Takes a target dataframe, returns the schemas dict
    to be used while creating aws glue table,
    data types references from https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    """
    result_dict = {}
    for column_name in df.columns:
        column_values = (
            df[column_name].replace("None", None).replace("", None).dropna().astype(str)
        )
        try:
            if "date" != column_name.lower():
                column_values = pd.Series(
                    [
                        ast.literal_eval(entry.capitalize())
                        for entry in column_values.values
                    ]
                )
            else:
                raise Exception

        except Exception:
            try:
                column_values = pd.Series(
                    [pd.to_datetime(entry) for entry in column_values.values]
                )
            except Exception:
                pass

        try:
            if pd.api.types.is_integer_dtype(column_values):
                max_value = column_values.max()
                if max_value >= -(2**31) and max_value <= (2**31 - 1):
                    athena_dtype = "int"
                else:
                    athena_dtype = "bigint"
            elif pd.api.types.is_float_dtype(column_values):
                max_value = column_values.max()
                if str(max_value.dtype) == "float32":
                    athena_dtype = "float"
                else:
                    athena_dtype = "double"
            elif pd.api.types.is_bool_dtype(column_values):
                athena_dtype = "boolean"
            elif (
                pd.api.types.is_datetime64_any_dtype(column_values)
                and column_name != "date"
            ):
                if column_values.equals(column_values.dt.normalize()):
                    athena_dtype = "date"
                else:
                    athena_dtype = "timestamp"

            else:
                athena_dtype = "string"

        except Exception:
            athena_dtype = "string"

        result_dict[column_name] = athena_dtype

    return result_dict


def apply_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Apply specified data types to the columns of a DataFrame based on the input schema

    Args:
        df (pd.DataFrame): DataFrame with generic data types.
        schema (dict): athena df schema containing columns' dtypes

    Returns:
        pd.DataFrame: DataFrame with data types specified in the schema.
    """

    # Mapping schema types to pandas dtypes
    schema_type_mapping = {
        "int": "int32",
        "bigint": "int64",
        "string": "string[python]",
        "timestamp": "datetime64[ns]",
        "double": "float64",
        "boolean": "bool",
        "date": "datetime64[ns]",
    }

    for column, dtype in schema.items():
        if column in df.columns:
            pandas_dtype = schema_type_mapping.get(dtype, "string[python]")
            if dtype in ["timestamp", "date"] and column != "date":
                df[column] = apply_iso_format(df[column])
            elif dtype in ["int", "bigint", "double"]:
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
            else:
                df[column] = df[column].astype(pandas_dtype)

    return df


def apply_iso_format(timestamp_column: pd.Series) -> pd.Series:
    """
    Apply ISO format to a timestamp column, trying multiple formats for each record.

    Args:
        timestamp_column (pd.Series): Series with timestamp data to be processed.

    Returns:
        pd.Series: Series with ISO formatted timestamps.
    """
    # Define the list of date formats to try
    date_formats = [
        "ISO8601",  # ISO8601 format
        "%Y-%m-%d %H:%M:%S",  # Ex: 2024-07-30 18:27:00
        "%m/%d/%Y %I:%M:%S %p",  # Ex: 7/30/2024 6:27:00 PM
        "%d-%m-%Y %H:%M:%S",  # Ex: 30-07-2024 18:27:00
        "%Y-%d-%m %H:%M:%S",  # Ex: 2025-14-04 22:19:21
    ]

    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return pd.to_datetime(
                    date_str, format=date_format, utc=True, errors="raise"
                )
            except (ValueError, TypeError):
                continue
        raise ValueError(
            f"Error processing date {date_str} in {timestamp_column.name}: Unable to parse date with provided formats"
        )

    return timestamp_column.apply(parse_date)


def rename_df_columns(df: pd.DataFrame, columns_to_rename: dict):
    for rename_pair in columns_to_rename:
        df = df.rename(columns=rename_pair)
    return df


def camel_to_snake(column_name):
    """
    Converts an input string from camel to snake case
    """
    # Replace sequences like '_A' or '_B' with '_a' or '_b'
    column_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", column_name)
    column_name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", column_name)
    # Convert to lowercase and remove redundant underscores
    column_name = re.sub(r"_+", "_", column_name).strip("_").lower()
    return column_name


def camel_to_snake_case(df):
    old_cols = list(df.columns)
    new_cols = []
    for old_col in old_cols:
        new_cols.append(camel_to_snake(old_col))
    new_names_map = {df.columns[i]: new_cols[i] for i in range(len(new_cols))}

    df.rename(new_names_map, axis=1, inplace=True)
    return df


def fetch_all_pages(
    mambu_client: APIClient,
    endpoint: str,
    request_type: str,
    extra_params: str = "",
    limit: int = 1000,
    body: dict = None,
):
    """
    Fetch all pages of GL accounts for a specific type using a while loop.

    Args:
        mambu_client: The client instance used for making API calls.
        endpoint (str): The API endpoint to query.
        account_type (str): The type of GL account to fetch (e.g., ASSET, LIABILITY).
        limit (int): The maximum number of records to fetch per API call.

    Returns:
        pd.DataFrame: A DataFrame containing all fetched records for the specified account type.
    """
    offset = 0
    accumulated_data = []
    accumulated_count = 0

    while True:
        # Build the query for the current page
        query = f"detailsLevel=FULL&limit={limit}&offset={offset}&{extra_params}"

        if request_type == "post":
            current_page_data = mambu_client.post(
                endpoint=endpoint,
                body=body,
                query=query,
                clean=True,
                flatten=True,
            )
        else:
            current_page_data = mambu_client.get(
                endpoint=endpoint,
                query=query,
                clean=True,
                flatten=True,
            )

        # Append the current page to the accumulated results
        if current_page_data:
            accumulated_data.extend(current_page_data)
            received_count = len(current_page_data)
        else:
            received_count = 0

        accumulated_count += received_count
        logger.info(
            f"Received {received_count} record, accumulated {accumulated_count} record."
        )

        # Break the loop if the number of rows in the current page is less than the limit
        if received_count < limit:
            break

        # Increment the offset for the next page
        offset += limit

    return pd.DataFrame(accumulated_data)


def add_meta_columns(df: pd.DataFrame, cdc_field: str):
    if cdc_field:
        df[cdc_field] = apply_iso_format(df[cdc_field])
        df["date"] = df[cdc_field].dt.strftime("%Y%m%d")
    else:
        df["date"] = date.today().strftime("%Y%m%d")

    df["timestamp_extracted"] = datetime.now(timezone.utc)
    return df


def process_dataframe(
    df: pd.DataFrame,
    cdc_field: str,
    rename_columns: list,
    auto_schema: bool,
    table_name: str,
    schemas: dict,
):
    logger.info(f"Processing DataFrame for table: {table_name}")
    df = add_meta_columns(df, cdc_field)
    df = camel_to_snake_case(df)
    df = rename_df_columns(df, rename_columns)
    table_schema = get_actual_dtypes(df) if auto_schema else schemas[table_name]
    df = apply_schema(df, table_schema)
    df.dropna(axis=1, how="all", inplace=True)

    # Any object type column not mentioned in schema to be string as default
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("string[python]")
        table_schema[col] = "string"
        logger.info(f"[WARNING] {col} dtype not in data_catalog, assuming as a string.")

    return df, table_schema


def make_query(sql):
    logger.info(f"Executing query: {sql}")
    return wr.athena.read_sql_query(
        sql=sql,
        database="datalake_raw",
        workgroup="datalake_workgroup",
        ctas_approach=False,
        keep_files=False,
    )


def get_start_time_from_athena(table_name: str, cdc_field: str):
    """
    Fetches the latest creation_date from the Athena table.
    If the table does not exist, returns None.
    """
    if cdc_field:
        cdc_column = camel_to_snake(cdc_field)
        try:
            # Query Athena for the latest timestamp_extracted
            query = f"""
            SELECT MAX({cdc_column})
            FROM datalake_raw.{table_name}
            """
            logger.info("Executing Athena query to fetch start_time")
            result = make_query(query)

            if not result.empty:
                latest_creation_date = result["_col0"].iloc[0]
                if not isinstance(latest_creation_date, pd.Timestamp):
                    try:
                        latest_creation_date = pd.Timestamp(latest_creation_date)
                    except Exception as e:
                        logger.error(f"Error parsing {cdc_column}: {e}")
                        raise ValueError(
                            f"Invalid datetime format for {cdc_column}: {latest_creation_date}"
                        )

                # Convert to a compatible string with Mambu API
                latest_creation_date = latest_creation_date.replace(
                    tzinfo=timezone.utc
                ).isoformat()

                logger.info(f"Latest {cdc_column} from Athena: {latest_creation_date}")

                return latest_creation_date
            else:
                raise ValueError(
                    f"No available timestamp records at cdc_column {cdc_column}"
                )

        except Exception as e:
            logger.error("Failed to query Athena for start_time: %s", e)
            raise


def create_payload(cdc_field, start_date, end_date):
    """
    Constructs the payload for Mambu API requests with filtering and sorting.
    """
    payload = json.dumps(
        {
            "filterCriteria": [
                {
                    "field": cdc_field,
                    "operator": "BETWEEN",
                    "value": start_date,
                    "secondValue": end_date,
                }
            ],
            "sortingCriteria": {
                "field": cdc_field,
                "order": "ASC",
            },
        }
    )
    logger.info(f"Request payload: {payload}")
    return payload


def fetch_gl_accounts(client, end_date, extra_params=""):
    """
    Special case: Fetches data for GL accounts by account types.
    """
    gl_account_types = ["ASSET", "LIABILITY", "EQUITY", "INCOME", "EXPENSE"]
    combined_df = pd.DataFrame()
    end_date_obj = datetime.strptime(end_date.split("T")[0], "%Y-%m-%d")
    to_date = (end_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")

    for account_type in gl_account_types:
        params = f"type={account_type}&to={to_date}&" + extra_params
        logger.info(f"Fetching GL account type: {account_type}")
        account_df = fetch_all_pages(
            client,
            endpoint="glaccounts",
            request_type="get",
            extra_params=params,
        )
        combined_df = pd.concat([combined_df, account_df], ignore_index=True)

    # Add meta field mentioning to_date used while extraction
    combined_df["balance_to_date"] = to_date
    return combined_df


def fetch_loan_installments(client, extra_params=""):
    """
    Special case: Fetches data for loan accounts installments by account state types.
    """
    account_state_types = [
        "PARTIAL_APPLICATION",
        "PENDING_APPROVAL",
        "APPROVED",
        "ACTIVE",
        "ACTIVE_IN_ARREARS",
        "CLOSED",
        "CLOSED_WRITTEN_OFF",
        "CLOSED_REJECTED",
    ]
    combined_df = pd.DataFrame()

    for account_type in account_state_types:
        params = (
            f"dueFrom=2022-01-01&dueTo=2122-01-01&accountState={account_type}&"
            + extra_params
        )
        logger.info(
            f"Fetching installments for loan account state type: {account_type}"
        )
        account_df = fetch_all_pages(
            client,
            endpoint="installments",
            request_type="get",
            extra_params=params,
        )
        combined_df = pd.concat([combined_df, account_df], ignore_index=True)

    return combined_df


def fetch_data_switch(
    client, endpoint, request_type, extra_params, cdc_field, start_date, end_date
):
    """
    Fetches data from the Mambu API based on the endpoint and optional filters.
    """
    if request_type == "get":
        # Special case for glaccounts
        if endpoint == "glaccounts":
            return fetch_gl_accounts(client, end_date, extra_params=extra_params)
        # Special case for loan installments
        elif endpoint == "installments":
            return fetch_loan_installments(client, extra_params=extra_params)
        else:
            return fetch_all_pages(
                client, endpoint=endpoint, request_type="get", extra_params=extra_params
            )

    elif request_type == "post":
        if not cdc_field:
            raise ValueError("cdc_field is required for Post request.")
        payload = create_payload(cdc_field, start_date, end_date)

        # Special case for creditarrangements
        if endpoint == "creditarrangements:search":
            payload_dict = json.loads(payload)
            del payload_dict["sortingCriteria"]
            payload = json.dumps(payload_dict)

        return fetch_all_pages(
            client,
            endpoint=endpoint,
            request_type="post",
            body=payload,
            extra_params=extra_params,
        )
    else:
        raise ValueError("request_type is not supported, use get or post.")
