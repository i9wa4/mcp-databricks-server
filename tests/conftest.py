"""Pytest configuration and fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load .env file if it exists
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path)


def _has_databricks_config() -> bool:
    """Check if Databricks configuration is available."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")

    has_token_auth = bool(host and token)
    has_oauth_auth = bool(host and client_id and client_secret)

    return has_token_auth or has_oauth_auth


# Skip condition for integration tests
skip_integration = pytest.mark.skipif(
    not _has_databricks_config(),
    reason="Databricks credentials not configured (no .env or environment variables)",
)
