"""MCP server for executing SQL queries against Databricks."""

from mcp.server.fastmcp import FastMCP

from mcp_databricks_server.dbapi import execute_statement
from mcp_databricks_server.formatter import format_query_results

__all__ = ["main", "mcp"]

mcp = FastMCP("databricks")


@mcp.tool()
async def execute_sql_query_in_databricks(sql: str) -> str:
    """Execute a SQL query on Databricks and return the results.

    Args:
        sql: The SQL query to execute
    """
    try:
        result = await execute_statement(sql)
        return format_query_results(result)
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
        result = await execute_statement(sql)
        return format_query_results(result)
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
        result = await execute_statement(sql)
        return format_query_results(result)
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
        result = await execute_statement(sql)
        return format_query_results(result)
    except Exception as e:
        return f"Error describing table: {e!s}"


def main() -> None:
    """Run the MCP server."""
    mcp.run(transport="stdio")
