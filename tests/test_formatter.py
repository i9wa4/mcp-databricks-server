"""Tests for the formatter module."""

from mcp_databricks_server.formatter import format_query_results


def test_format_query_results_with_data() -> None:
    """Test formatting results with data."""
    result = {
        "manifest": {
            "schema": {
                "columns": [
                    {"name": "id"},
                    {"name": "name"},
                ]
            }
        },
        "result": {
            "data_array": [
                ["1", "Alice"],
                ["2", "Bob"],
            ]
        },
    }

    output = format_query_results(result)

    assert "id | name" in output
    assert "1 | Alice" in output
    assert "2 | Bob" in output


def test_format_query_results_empty() -> None:
    """Test formatting empty results."""
    result: dict = {}

    output = format_query_results(result)

    assert output == "No results or invalid result format."


def test_format_query_results_no_rows() -> None:
    """Test formatting results with no rows."""
    result = {
        "manifest": {
            "schema": {
                "columns": [
                    {"name": "id"},
                    {"name": "name"},
                ]
            }
        },
        "result": {"data_array": []},
    }

    output = format_query_results(result)

    assert "id | name" in output
    assert "No data rows found." in output


def test_format_query_results_null_values() -> None:
    """Test formatting results with NULL values."""
    result = {
        "manifest": {
            "schema": {
                "columns": [
                    {"name": "id"},
                    {"name": "value"},
                ]
            }
        },
        "result": {
            "data_array": [
                ["1", None],
            ]
        },
    }

    output = format_query_results(result)

    assert "1 | NULL" in output
