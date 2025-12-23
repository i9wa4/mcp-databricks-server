"""MCP server for executing SQL queries against Databricks."""

from __future__ import annotations

import asyncio

from mcp.server.fastmcp import FastMCP

from mcp_databricks_server.formatter import format_sdk_results
from mcp_databricks_server.sdk_utils import (
    execute_databricks_sql,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
)

__all__ = ["main", "mcp"]

mcp = FastMCP("databricks")


def _format_sql_result(result: dict[str, object]) -> str:
    """Format SQL execution result."""
    status = result.get("status")
    if status == "success":
        return format_sdk_results(result)
    elif status == "failed":
        error_message = result.get("error", "Unknown query execution error.")
        details = result.get("details", "")
        if details:
            return f"SQL Query Failed: {error_message}\nDetails: {details}"
        return f"SQL Query Failed: {error_message}"
    else:
        error_message = result.get("error", "Unknown error during SQL execution.")
        return f"Error: {error_message}"


@mcp.tool()
async def execute_sql_query_in_databricks(sql: str) -> str:
    """Execute a SQL query on Databricks and return the results.

    Args:
        sql: The SQL query to execute
    """
    try:
        result = await asyncio.to_thread(execute_databricks_sql, sql)
        return _format_sql_result(result)
    except Exception as e:
        return f"Error executing SQL query: {e!s}"


@mcp.tool()
async def list_schemas_in_databricks(catalog: str) -> str:
    """List all available schemas in a Databricks catalog.

    Args:
        catalog: The catalog name to list schemas from
    """
    sql = f"SHOW SCHEMAS IN {catalog}"
    try:
        result = await asyncio.to_thread(execute_databricks_sql, sql)
        return _format_sql_result(result)
    except Exception as e:
        return f"Error listing schemas: {e!s}"


@mcp.tool()
async def list_tables_in_databricks(schema: str) -> str:
    """List all tables in a specific schema.

    Args:
        schema: The schema name to list tables from
    """
    sql = f"SHOW TABLES IN {schema}"
    try:
        result = await asyncio.to_thread(execute_databricks_sql, sql)
        return _format_sql_result(result)
    except Exception as e:
        return f"Error listing tables: {e!s}"


@mcp.tool()
async def describe_table_in_databricks(table_name: str) -> str:
    """Describe a table's schema.

    Args:
        table_name: The fully qualified table name (e.g., schema.table_name)
    """
    sql = f"DESCRIBE TABLE {table_name}"
    try:
        result = await asyncio.to_thread(execute_databricks_sql, sql)
        return _format_sql_result(result)
    except Exception as e:
        return f"Error describing table: {e!s}"


# Unity Catalog metadata exploration tools using Databricks SDK


@mcp.tool()
async def list_uc_catalogs() -> str:
    """List all available Unity Catalogs with their names, descriptions, and types.

    Use this tool as a starting point to discover available data sources when you
    don't know specific catalog names. It provides a high-level overview of all
    accessible catalogs in the workspace.
    """
    try:
        return await asyncio.to_thread(get_uc_all_catalogs_summary)
    except Exception as e:
        return f"Error listing catalogs: {e!s}"


@mcp.tool()
async def describe_uc_catalog(catalog_name: str) -> str:
    """Get a summary of a specific Unity Catalog, listing all its schemas.

    Use this tool when you know the catalog name and need to discover the schemas
    within it. This is often a precursor to describing a specific schema or table.

    Args:
        catalog_name: The name of the Unity Catalog to describe (e.g., `prod`, `dev`)
    """
    try:
        return await asyncio.to_thread(get_uc_catalog_details, catalog_name)
    except Exception as e:
        return f"Error getting catalog details for '{catalog_name}': {e!s}"


@mcp.tool()
async def describe_uc_schema(
    catalog_name: str, schema_name: str, include_columns: bool | None = False
) -> str:
    """Get detailed information about a specific schema within a Unity Catalog.

    Use this tool to understand the contents of a schema, primarily its tables.
    Set `include_columns=True` to get column information, which is crucial for
    query construction but makes the output longer.

    Args:
        catalog_name: The name of the catalog containing the schema
        schema_name: The name of the schema to describe
        include_columns: If True, lists tables with their columns. Defaults to False.
    """
    try:
        return await asyncio.to_thread(
            get_uc_schema_details, catalog_name, schema_name, include_columns or False
        )
    except Exception as e:
        return f"Error getting schema details for '{catalog_name}.{schema_name}': {e!s}"


@mcp.tool()
async def describe_uc_table(
    full_table_name: str, include_lineage: bool | None = False
) -> str:
    """Get detailed description of a specific Unity Catalog table.

    Use this tool to understand the structure (columns, data types, partitioning)
    of a single table. This is essential before constructing SQL queries.

    Optionally includes comprehensive lineage information:
    - Upstream tables (tables this table reads from)
    - Downstream tables (tables that read from this table)
    - Notebooks that read from or write to this table with job details

    Args:
        full_table_name: The fully qualified table name (e.g., `catalog.schema.table`)
        include_lineage: Set to True to fetch lineage information. Defaults to False.
    """
    try:
        return await asyncio.to_thread(
            get_uc_table_details, full_table_name, include_lineage or False
        )
    except Exception as e:
        return f"Error getting table details for '{full_table_name}': {e!s}"


def main() -> None:
    """Run the MCP server."""
    mcp.run(transport="stdio")
