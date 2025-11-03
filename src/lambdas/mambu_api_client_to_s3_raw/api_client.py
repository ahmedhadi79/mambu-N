import base64
import json
import logging
import sys
import time
from datetime import date
from datetime import datetime
from datetime import timezone
from typing import Union

import boto3
import pandas as pd
import requests
from flatten_json import flatten


def initialize_log(name) -> logging.Logger:
    """
    logging function with set level logging output
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = initialize_log("common.APIClient")


class APIClient:
    """
    Works as a common Client designed to handle API requests.

    :param auth: API credentials or custom "Authorization" header.
    Supports:
        - Token authentication (api_token)
        - BasicAuth (api_key & secret_key)
        - OAuth2 (username, password, grant_type, client_id, client_secret, security_token)
        - Any custom Authorization header as string or dictionary.

    :param secrets_manager: Whether to use "auth" argument as AWS Secrets Manager
        resource name or to directly use "auth" value as Authorization header.
    :param base_url: The API base URL ending with "/"
    :param login_url: (used with OAuth2) Login URL ending with "/"
    :param boto3_session: Pass a custom boto3 session.
    """

    def __init__(
        self,
        auth: Union[str, dict],
        base_url: str = "",
        secrets_manager: bool = False,
        login_url: str = None,
        boto3_session: boto3.Session = None,
    ):
        self.base_url = base_url
        self.login_url = login_url
        self.auth = self.get_secret(self, auth, secrets_manager, boto3_session)

    @staticmethod
    def get_secret(self, secret_name, secrets_manager, boto3_session):
        """
        Retrieves a secret from AWS Secrets Manager
        :param secret_name: The key to retrieve
        """
        if secrets_manager:
            logger.info("Retrieving :  %s", secret_name)
            if boto3_session:
                secretsmanager = boto3_session.client("secretsmanager")
            else:
                secretsmanager = boto3.client("secretsmanager")
            secret_value = secretsmanager.get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        else:
            secret_value = secret_name
        try:
            if isinstance(secret_value, str):
                secret_value = json.loads(secret_value)

            if not self.login_url:
                # Case1: BasicAuth (api_access & api_secret keys)
                logger.info("Auth Header: Basic Auth")
                username, password = list(secret_value.values())[:2]
                encoded_auth = base64.b64encode(
                    f"{username}:{password}".encode("utf-8")
                ).decode("utf-8")
                return f"Basic {encoded_auth}"
            else:
                # Case2: OAuth2 (username, password, grant_type, client_id, client_secret)
                logger.info("Auth Header: OAuth, logging in...")

                # Sub case: for SalesForce
                if "security_token" in secret_value.keys():
                    secret_value["password"] += secret_value["security_token"]
                    secret_value.pop("security_token", None)

                # Call login function with 3 retries
                self.login(secret_value)

                # Sub case: for SalesForce Service Cloud
                if "instance_url" in self.login_payload.keys():
                    self.base_url = self.login_payload["instance_url"] + "/"
                # Sub case: for SalesForce Marketing Cloud
                if "rest_instance_url" in self.login_payload.keys():
                    self.base_url = self.login_payload["rest_instance_url"]
                return "Bearer " + self.login_payload["access_token"]

        except json.JSONDecodeError:
            # Case3: custom Authorization header.
            logger.info("Auth Header: Custom by user")
            return secret_value

    def login(self, secret_value):
        max_retries = 3
        for attempt in range(max_retries):
            response = requests.post(self.login_url, data=secret_value)
            if response.status_code == 200:
                self.login_payload = self.parse_response(response)
                logger.info("Login success!")
                break
            else:
                logger.info(
                    f"Login attempt{attempt + 1}: failed with status code {response.status_code}, retrying.."
                )
                if attempt < max_retries - 1:  # Only sleep if it's not the last attempt
                    time.sleep(1)
        else:
            # This block executes if all attempts failed
            e = f"Failed to authinticate after {max_retries} attempts"
            logger.error(e)
            raise Exception(e)
            # TO DO: implementing a backoff strategy

    @staticmethod
    def parse_response(response):
        if "json" not in response.headers.get("Content-Type", ""):
            logger.warning("Response is not application/json, returning raw response")
            return response

        try:
            return response.json()
        except ValueError:
            logger.error("Could not convert response to json, returning raw response")
            pass

        return response

    def make_request(
        self, method, endpoint, json_body=None, query=None, body=None, files=None
    ):
        methods = {
            "get": requests.get,
            "post": requests.post,
            "put": requests.put,
            "delete": requests.delete,
        }

        method = method.lower()
        request = methods.get(method, requests.get)

        request_params = {
            "headers": {
                # "Accept": "application/json",
                "Accept": "application/vnd.mambu.v2+json",
                "Content-Type": "application/json",
                "Authorization": self.auth,
                "apikey": self.auth,
            },
            "json": json_body,
            "params": query,
            "data": body,
            "files": files,
        }

        logger.info(
            f"Calling:{self.base_url}{endpoint}?{query}"
            if query is not None
            else f"Calling:{self.base_url}{endpoint}"
        )

        response = request(self.base_url + endpoint, **request_params)
        parsed_response = self.parse_response(response)
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            logger.exception(error)
            logger.error(response.json())

            # fallback to requests exception
            raise error

        return parsed_response

    def get(
        self,
        endpoint: str,
        query: str = None,
        filter_objects: list[str] = [],
        clean: bool = False,
        flatten: bool = False,
        df: bool = False,
    ):
        """
        Perform a GET request to the specified API endpoint.

        Parameters:
            - endpoint (str): The API endpoint to be accessed.
            - query (str, optional): Additional query parameters for the API request.
            - filter_objects (list, optional): List of target object keys to filter from the API response.
            - clean (bool, optional): Whether to clean the API response by removing new lines and double spaces.
            - flatten (bool, optional): Whether to flatten any nested structure in the API response.
            - df (bool, optional): Whether to convert reponse to a pd.DataFrame.

        Returns:
            The processed API response based on the specified options.
        """
        if query:
            query = query.replace(" ", "+")

        response = self.make_request("get", endpoint, query=query)
        return self.process_response(response, filter_objects, clean, flatten, df)

    def post(
        self,
        endpoint: str,
        json_body: dict = None,
        query: dict = None,
        body=None,
        files=None,
        filter_objects: list[str] = [],
        clean: bool = False,
        flatten: bool = False,
        df: bool = False,
    ):
        if query:
            query = query.replace(" ", "+")

        response = self.make_request(
            "post", endpoint, json_body=json_body, query=query, body=body, files=files
        )
        return self.process_response(response, filter_objects, clean, flatten, df)

    def put(
        self,
        endpoint: str,
        json_body: dict = None,
        query: dict = None,
        body=None,
        files=None,
    ):
        return self.make_request(
            "post", endpoint, json_body=json_body, query=query, body=body, files=files
        )

    def delete(
        self,
        endpoint: str,
        json_body: dict = None,
        query: dict = None,
        body=None,
        files=None,
    ):
        return self.make_request(
            "post", endpoint, json_body=json_body, query=query, body=body, files=files
        )

    def process_response(
        self,
        response,
        filter_objects: list[str] = [],
        clean: bool = False,
        flatten: bool = False,
        df: bool = False,
    ):
        logger.info("Processing API response..")

        if len(filter_objects) > 0:
            if len(filter_objects) == 1:
                response = response[filter_objects[0]]
            else:
                response = {
                    object: response[object]
                    for object in filter_objects
                    if object in response.keys()
                }
        if clean:
            response = self.clean(response)
        if flatten:
            response = self.data_flatten(response)
        if df:
            response = self.df_converter(response, flatten)
        return response

    @staticmethod
    def clean(response):
        """
         Recursively cleans response content by removing:
            - Newline characters.
            - Carriage return characters.
            - Tab characters.
            - Double spaces.

        Args:
        response: str/list/dict

        Returns: str/list/dict
        """

        def clean_text(value):
            if isinstance(value, str):
                cleaned_value = (
                    value.replace("\n", " ")
                    .replace("\r", " ")
                    .replace("\t", " ")
                    .replace("  ", " ")
                )
                return cleaned_value
            elif isinstance(value, list):
                return [clean_text(item) for item in value]
            elif isinstance(value, dict):
                return {key: clean_text(val) for key, val in value.items()}
            else:
                return value

        if response:
            cleaned_response = clean_text(response)
            return cleaned_response

    @staticmethod
    def data_flatten(response):
        def handle_json_strings(data):
            # Recursively checks and parses JSON strings into dictionaries.
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, str):
                        try:
                            # Attempt to parse as JSON
                            parsed_value = json.loads(value)
                            if isinstance(parsed_value, dict):
                                data[key] = parsed_value
                        except (json.JSONDecodeError, TypeError):
                            # Skip if not valid JSON
                            pass
            return data

        if response:
            if isinstance(response, list):
                return [flatten(handle_json_strings(entry)) for entry in response]
            else:
                return flatten(handle_json_strings(response))

    @staticmethod
    def df_converter(response, flatten):
        response = pd.DataFrame(response)
        response["date"] = date.today().strftime("%Y%m%d")
        response["timestamp_extracted"] = datetime.now(timezone.utc)
        if not flatten:
            for column in response.columns:
                if response[column].dtype == "object":
                    response[column] = response[column].astype(str)
        return response
