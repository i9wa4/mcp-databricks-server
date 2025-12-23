"""Format Databricks query results for display."""

from typing import Any


def format_query_results(result: dict[str, Any]) -> str:
    """Format query results into a readable string."""
    # Check if result is empty or doesn't have the expected structure
    if not result or "manifest" not in result or "result" not in result:
        return "No results or invalid result format."

    # Extract column names from the manifest
    column_names: list[str] = []
    if (
        "manifest" in result
        and "schema" in result["manifest"]
        and "columns" in result["manifest"]["schema"]
    ):
        columns = result["manifest"]["schema"]["columns"]
        column_names = [col["name"] for col in columns] if columns else []

    # If no column names were found, return early
    if not column_names:
        return "No columns found in the result."

    # Extract rows from the result
    rows: list[list[Any]] = []
    if "result" in result and "data_array" in result["result"]:
        rows = result["result"]["data_array"]

    # If no rows were found, return just the column headers
    if not rows:
        # Format as a table
        output = []

        # Add header
        output.append(" | ".join(column_names))
        output.append("-" * (sum(len(name) + 3 for name in column_names) - 1))
        output.append("No data rows found.")

        return "\n".join(output)

    # Format as a table
    output = []

    # Add header
    output.append(" | ".join(column_names))
    output.append("-" * (sum(len(name) + 3 for name in column_names) - 1))

    # Add rows
    for row in rows:
        row_values = []
        for value in row:
            if value is None:
                row_values.append("NULL")
            else:
                row_values.append(str(value))
        output.append(" | ".join(row_values))

    return "\n".join(output)


def format_sdk_results(result: dict[str, Any]) -> str:
    """Format Databricks SDK query results into a readable string.

    Args:
        result: Dictionary with 'columns', 'rows', 'row_count', and 'truncated' keys
    """
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    row_count = result.get("row_count", 0)
    truncated = result.get("truncated", False)

    if not columns:
        return "No columns found in the result."

    # Format as a table
    output = []

    # Add header
    output.append(" | ".join(columns))
    output.append("-" * (sum(len(name) + 3 for name in columns) - 1))

    # Add rows
    if not rows:
        output.append("No data rows found.")
    else:
        for row in rows:
            row_values = []
            for value in row:
                if value is None:
                    row_values.append("NULL")
                else:
                    row_values.append(str(value))
            output.append(" | ".join(row_values))

    # Add row count and truncation info
    output.append("")
    output.append(f"Total rows: {row_count}")
    if truncated:
        output.append("(Results truncated)")

    return "\n".join(output)
