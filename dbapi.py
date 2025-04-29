import asyncio
import os
from typing import Any
from typing import Dict
from typing import Optional

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
        else:
            raise Exception("Failed to get access token from Databricks OAuth")
    # Fallback: PAT
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return f"Bearer {token}"
    raise Exception("No valid Databricks authentication found")


async def make_databricks_request(
    method: str,
    endpoint: str,
    json_data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
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
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            error_message = f"HTTP error: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_message += f" - {error_detail.get('message', '')}"
            except Exception:
                pass
            raise Exception(error_message)
        except Exception as e:
            raise Exception(f"Error making request to Databricks API: {str(e)}")


async def execute_statement(
    sql: str, warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Execute a SQL statement and wait for its completion."""
    if not warehouse_id:
        warehouse_id = DATABRICKS_SQL_WAREHOUSE_ID

    if not warehouse_id:
        raise ValueError(
            "Warehouse ID is required. Set DATABRICKS_SQL_WAREHOUSE_ID environment"
            " variable or provide it as a parameter."
        )

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
        raise Exception("Failed to get statement ID from response")

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
        elif status in ["FAILED", "CANCELED"]:
            error_message = (
                statement_status.get("status", {})
                .get("error", {})
                .get("message", "Unknown error")
            )
            raise Exception(f"Statement execution failed: {error_message}")

        # Wait before polling again
        await asyncio.sleep(10)
        retry_count += 1

    raise Exception("Statement execution timed out")
