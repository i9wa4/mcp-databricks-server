"""Format Databricks query results for display."""

from typing import Any


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
