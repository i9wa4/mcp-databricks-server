"""Tests for the formatter module."""

from typing import Any

from mcp_databricks_server.formatter import format_sdk_results


def test_format_sdk_results_with_data() -> None:
    """Test formatting SDK results with data."""
    result: dict[str, Any] = {
        "status": "success",
        "row_count": 2,
        "columns": ["id", "name"],
        "rows": [
            ["1", "Alice"],
            ["2", "Bob"],
        ],
    }

    output = format_sdk_results(result)

    assert "id | name" in output
    assert "1 | Alice" in output
    assert "2 | Bob" in output
    assert "Total rows: 2" in output


def test_format_sdk_results_empty_columns() -> None:
    """Test formatting SDK results with no columns."""
    result: dict[str, Any] = {
        "status": "success",
        "row_count": 0,
        "columns": [],
        "rows": [],
    }

    output = format_sdk_results(result)

    assert output == "No columns found in the result."


def test_format_sdk_results_no_rows() -> None:
    """Test formatting SDK results with no rows."""
    result: dict[str, Any] = {
        "status": "success",
        "row_count": 0,
        "columns": ["id", "name"],
        "rows": [],
    }

    output = format_sdk_results(result)

    assert "id | name" in output
    assert "No data rows found." in output
    assert "Total rows: 0" in output


def test_format_sdk_results_null_values() -> None:
    """Test formatting SDK results with NULL values."""
    result: dict[str, Any] = {
        "status": "success",
        "row_count": 1,
        "columns": ["id", "value"],
        "rows": [
            ["1", None],
        ],
    }

    output = format_sdk_results(result)

    assert "1 | NULL" in output
    assert "Total rows: 1" in output


def test_format_sdk_results_truncated() -> None:
    """Test formatting SDK results with truncation flag."""
    result: dict[str, Any] = {
        "status": "success",
        "row_count": 100,
        "columns": ["id"],
        "rows": [["1"]],
        "truncated": True,
    }

    output = format_sdk_results(result)

    assert "(Results truncated)" in output
