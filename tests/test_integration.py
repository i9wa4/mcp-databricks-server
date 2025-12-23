"""Integration tests for Databricks SDK utilities.

These tests require a valid .env file with Databricks credentials.
They will be automatically skipped if credentials are not configured.
"""

from __future__ import annotations

import pytest

from tests.conftest import skip_integration


@skip_integration
class TestUnityCatalogIntegration:
    """Integration tests for Unity Catalog metadata exploration."""

    def test_list_uc_catalogs(self) -> None:
        """Test listing Unity Catalogs."""
        from mcp_databricks_server.sdk_utils import get_uc_all_catalogs_summary

        result = get_uc_all_catalogs_summary()

        assert "# Available Unity Catalogs" in result
        assert "Error" not in result or "Found" in result

    def test_describe_uc_catalog(self) -> None:
        """Test describing a specific catalog."""
        from mcp_databricks_server.sdk_utils import (
            get_sdk_client,
            get_uc_catalog_details,
        )

        # Get first available catalog
        client = get_sdk_client()
        catalogs = list(client.catalogs.list())
        if not catalogs:
            pytest.skip("No catalogs available for testing")

        catalog_name = catalogs[0].name
        result = get_uc_catalog_details(catalog_name)

        assert f"# Catalog Summary: **{catalog_name}**" in result

    def test_describe_uc_schema(self) -> None:
        """Test describing a specific schema."""
        from mcp_databricks_server.sdk_utils import (
            get_sdk_client,
            get_uc_schema_details,
        )

        # Get first available catalog and schema
        client = get_sdk_client()
        catalogs = list(client.catalogs.list())
        if not catalogs:
            pytest.skip("No catalogs available for testing")

        catalog_name = catalogs[0].name
        schemas = list(client.schemas.list(catalog_name=catalog_name))
        if not schemas:
            pytest.skip(f"No schemas in catalog {catalog_name}")

        schema_name = schemas[0].name
        result = get_uc_schema_details(catalog_name, schema_name)

        assert f"# Schema Details: **{catalog_name}.{schema_name}**" in result

    def test_get_table_details(self) -> None:
        """Test getting table details."""
        from mcp_databricks_server.sdk_utils import (
            get_sdk_client,
            get_uc_table_details,
        )

        # Get first available table
        client = get_sdk_client()
        catalogs = list(client.catalogs.list())
        if not catalogs:
            pytest.skip("No catalogs available for testing")

        for catalog in catalogs:
            schemas = list(client.schemas.list(catalog_name=catalog.name))
            for schema in schemas:
                tables = list(
                    client.tables.list(
                        catalog_name=catalog.name,
                        schema_name=schema.name,
                    )
                )
                if tables:
                    table = tables[0]
                    result = get_uc_table_details(table.full_name or "")

                    assert "# Table:" in result
                    return

        pytest.skip("No tables available for testing")


@skip_integration
class TestSqlExecutionIntegration:
    """Integration tests for SQL execution via SDK."""

    def test_execute_simple_query(self) -> None:
        """Test executing a simple SQL query."""
        import os

        from mcp_databricks_server.sdk_utils import execute_databricks_sql

        if not os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID"):
            pytest.skip("DATABRICKS_SQL_WAREHOUSE_ID not configured")

        result = execute_databricks_sql("SELECT 1 AS test_value")

        assert result.get("status") == "success"
        assert result.get("row_count", 0) > 0
