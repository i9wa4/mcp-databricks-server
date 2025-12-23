"""Databricks SQL Statement Execution API client."""

import asyncio
import os
from typing import Any

import httpx
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration constants
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")

# API endpoints
STATEMENTS_API = "/api/2.0/sql/statements"
STATEMENT_API = "/api/2.0/sql/statements/{statement_id}"


def get_auth_token() -> str:
    """Get authentication token for Databricks API."""
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    use_oauth = os.environ.get("DATABRICKS_AUTH_TYPE", "").lower() == "oauth"

    if use_oauth and host and client_id and client_secret:
        token_url = f"{host}/oidc/v1/token"
        data = {"grant_type": "client_credentials", "scope": "all-apis"}
        resp = requests.post(
            token_url, data=data, auth=(client_id, client_secret), timeout=10
        )
        resp.raise_for_status()
        access_token = resp.json().get("access_token")
        if access_token:
            return f"Bearer {access_token}"
        msg = "Failed to get access token from Databricks OAuth"
        raise RuntimeError(msg)
    # Fallback: PAT
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return f"Bearer {token}"
    msg = "No valid Databricks authentication found"
    raise RuntimeError(msg)


async def make_databricks_request(
    method: str,
    endpoint: str,
    json_data: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make a request to the Databricks API with proper error handling."""
    url = f"{DATABRICKS_HOST}{endpoint}"
    headers = {
        "Authorization": get_auth_token(),
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        try:
            if method.lower() == "get":
                response = await client.get(
                    url, headers=headers, params=params, timeout=30.0
                )
            elif method.lower() == "post":
                response = await client.post(
                    url, headers=headers, json=json_data, timeout=30.0
                )
            else:
                msg = f"Unsupported HTTP method: {method}"
                raise ValueError(msg)

            response.raise_for_status()
            return response.json()  # type: ignore[no-any-return]
        except httpx.HTTPStatusError as e:
            error_message = f"HTTP error: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_message += f" - {error_detail.get('message', '')}"
            except (ValueError, KeyError):
                # JSON parsing failed or 'message' key not found
                # Use the basic error message without details
                error_message += f" - {e.response.text[:200]}"
            raise RuntimeError(error_message) from e
        except Exception as e:
            msg = f"Error making request to Databricks API: {e!s}"
            raise RuntimeError(msg) from e


async def execute_statement(
    sql: str, warehouse_id: str | None = None
) -> dict[str, Any]:
    """Execute a SQL statement and wait for its completion."""
    if not warehouse_id:
        warehouse_id = DATABRICKS_SQL_WAREHOUSE_ID

    if not warehouse_id:
        msg = (
            "Warehouse ID is required. Set DATABRICKS_SQL_WAREHOUSE_ID environment"
            " variable or provide it as a parameter."
        )
        raise ValueError(msg)

    # Create the statement
    statement_data = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "0s",  # Don't wait for completion in the initial request
    }

    response = await make_databricks_request(
        "post", STATEMENTS_API, json_data=statement_data
    )
    statement_id = response.get("statement_id")

    if not statement_id:
        msg = "Failed to get statement ID from response"
        raise RuntimeError(msg)

    # Poll for statement completion
    max_retries = 60  # Maximum number of retries (10 minutes with 10-second intervals)
    retry_count = 0

    while retry_count < max_retries:
        statement_status = await make_databricks_request(
            "get", STATEMENT_API.format(statement_id=statement_id)
        )

        status = statement_status.get("status", {}).get("state")

        if status == "SUCCEEDED":
            return statement_status
        if status in ["FAILED", "CANCELED"]:
            error_message = (
                statement_status.get("status", {})
                .get("error", {})
                .get("message", "Unknown error")
            )
            msg = f"Statement execution failed: {error_message}"
            raise RuntimeError(msg)

        # Wait before polling again
        await asyncio.sleep(10)
        retry_count += 1

    msg = "Statement execution timed out"
    raise TimeoutError(msg)
