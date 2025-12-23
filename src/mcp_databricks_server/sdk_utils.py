"""Databricks SDK utilities for Unity Catalog metadata exploration."""

from __future__ import annotations

import configparser
import json
import os
import time
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    SchemaInfo,
    TableInfo,
)
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementResponse,
    StatementState,
)


def _get_warehouse_id() -> str | None:
    """Get Warehouse ID from environment variable or ~/.databrickscfg.

    Priority:
    1. DATABRICKS_SQL_WAREHOUSE_ID environment variable
    2. warehouse_id from ~/.databrickscfg (using DATABRICKS_CONFIG_PROFILE)
    """
    # Check environment variable first
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if warehouse_id:
        return warehouse_id

    # Read from ~/.databrickscfg
    databrickscfg_path = Path.home() / ".databrickscfg"
    if not databrickscfg_path.exists():
        return None

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    parser = configparser.ConfigParser(strict=False)
    try:
        parser.read(databrickscfg_path)
    except configparser.Error:
        return None

    if profile not in parser:
        return None

    return parser[profile].get("warehouse_id")


# Lazy initialization of Warehouse ID
_warehouse_id: str | None = None


def get_warehouse_id() -> str | None:
    """Get or initialize the Warehouse ID (lazy initialization)."""
    global _warehouse_id
    if _warehouse_id is None:
        _warehouse_id = _get_warehouse_id()
    return _warehouse_id


def _get_sdk_client() -> WorkspaceClient:
    """Create and return a Databricks SDK client.

    Authentication is resolved from ~/.databrickscfg using the profile
    specified by DATABRICKS_CONFIG_PROFILE environment variable (default: DEFAULT).

    Using explicit profile parameter ensures that conflicting environment
    variables (e.g., both TOKEN and CLIENT_ID/SECRET) are ignored.
    """
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


# Lazy initialization of SDK client
_sdk_client: WorkspaceClient | None = None


def get_sdk_client() -> WorkspaceClient:
    """Get or create the SDK client (lazy initialization)."""
    global _sdk_client
    if _sdk_client is None:
        _sdk_client = _get_sdk_client()
    return _sdk_client


# Cache for job information to avoid redundant API calls
_job_cache: dict[str, dict[str, Any]] = {}
_notebook_cache: dict[str, str | None] = {}


def _format_column_details_md(columns: list[ColumnInfo]) -> list[str]:
    """Format a list of ColumnInfo objects into Markdown strings."""
    markdown_lines: list[str] = []
    if not columns:
        markdown_lines.append("  - *No column information available.*")
        return markdown_lines

    for col in columns:
        if not isinstance(col, ColumnInfo):
            continue
        type_name = col.type_name
        if col.type_text:
            col_type = col.type_text
        elif type_name and hasattr(type_name, "value"):
            col_type = type_name.value
        else:
            col_type = "N/A"
        nullable_status = "nullable" if col.nullable else "not nullable"
        col_description = f": {col.comment}" if col.comment else ""
        markdown_lines.append(
            f"  - **{col.name}** (`{col_type}`, {nullable_status}){col_description}"
        )
    return markdown_lines


def _get_job_info_cached(job_id: str) -> dict[str, Any]:
    """Get job information with caching to avoid redundant API calls."""
    if job_id not in _job_cache:
        try:
            client = get_sdk_client()
            job_info = client.jobs.get(job_id=int(job_id))
            settings = job_info.settings
            job_name = settings.name if settings and settings.name else f"Job {job_id}"
            _job_cache[job_id] = {
                "name": job_name,
                "tasks": [],
            }

            if settings and settings.tasks:
                for task in settings.tasks:
                    if hasattr(task, "notebook_task") and task.notebook_task:
                        task_info = {
                            "task_key": task.task_key,
                            "notebook_path": task.notebook_task.notebook_path,
                        }
                        _job_cache[job_id]["tasks"].append(task_info)

        except Exception as e:
            _job_cache[job_id] = {
                "name": f"Job {job_id}",
                "tasks": [],
                "error": str(e),
            }

    return _job_cache[job_id]


def _get_notebook_id_cached(notebook_path: str) -> str | None:
    """Get notebook ID with caching to avoid redundant API calls."""
    if notebook_path not in _notebook_cache:
        try:
            client = get_sdk_client()
            notebook_details = client.workspace.get_status(notebook_path)
            _notebook_cache[notebook_path] = str(notebook_details.object_id)
        except Exception:
            _notebook_cache[notebook_path] = None

    return _notebook_cache[notebook_path]


def _resolve_notebook_info_optimized(notebook_id: str, job_id: str) -> dict[str, Any]:
    """Resolve notebook info using cached job data."""
    result: dict[str, Any] = {
        "notebook_id": notebook_id,
        "notebook_path": f"notebook_id:{notebook_id}",
        "notebook_name": f"notebook_id:{notebook_id}",
        "job_id": job_id,
        "job_name": f"Job {job_id}",
        "task_key": None,
    }

    job_info = _get_job_info_cached(job_id)
    result["job_name"] = job_info["name"]

    for task_info in job_info["tasks"]:
        notebook_path = task_info["notebook_path"]
        cached_notebook_id = _get_notebook_id_cached(notebook_path)

        if cached_notebook_id == notebook_id:
            result["notebook_path"] = notebook_path
            result["notebook_name"] = notebook_path.split("/")[-1]
            result["task_key"] = task_info["task_key"]
            break

    return result


def _format_notebook_info_optimized(notebook_info: dict[str, Any]) -> str:
    """Format notebook information using pre-resolved data."""
    lines: list[str] = []

    if notebook_info["notebook_path"].startswith("/"):
        lines.append(f"**`{notebook_info['notebook_name']}`**")
        lines.append(f"  - **Path**: `{notebook_info['notebook_path']}`")
    else:
        lines.append(f"**{notebook_info['notebook_name']}**")

    job_name = notebook_info["job_name"]
    job_id = notebook_info["job_id"]
    lines.append(f"  - **Job**: {job_name} (ID: {job_id})")
    if notebook_info["task_key"]:
        lines.append(f"  - **Task**: {notebook_info['task_key']}")

    return "\n".join(lines)


def _process_lineage_results(
    lineage_query_output: dict[str, Any], main_table_full_name: str
) -> dict[str, Any]:
    """Process lineage results with optimization and caching."""
    start_time = time.time()

    processed_data: dict[str, Any] = {
        "upstream_tables": [],
        "downstream_tables": [],
        "notebooks_reading": [],
        "notebooks_writing": [],
    }

    if not lineage_query_output or lineage_query_output.get("status") != "success":
        return processed_data

    # Convert rows + columns to list of dicts
    columns = lineage_query_output.get("columns", [])
    rows = lineage_query_output.get("rows", [])
    if not columns or not rows:
        return processed_data

    data_rows = [dict(zip(columns, row, strict=False)) for row in rows]

    upstream_set: set[str] = set()
    downstream_set: set[str] = set()
    notebooks_reading_dict: dict[str, str] = {}
    notebooks_writing_dict: dict[str, str] = {}

    unique_job_ids: set[str] = set()
    notebook_job_pairs: list[dict[str, Any]] = []

    for row in data_rows:
        source_table = row.get("source_table_full_name")
        target_table = row.get("target_table_full_name")
        entity_metadata = row.get("entity_metadata")

        notebook_id = None
        job_id = None

        if entity_metadata:
            try:
                if isinstance(entity_metadata, str):
                    metadata_dict = json.loads(entity_metadata)
                else:
                    metadata_dict = entity_metadata

                notebook_id = metadata_dict.get("notebook_id")
                job_info = metadata_dict.get("job_info")
                if job_info:
                    job_id = job_info.get("job_id")
            except (json.JSONDecodeError, AttributeError):
                pass

        if (
            source_table == main_table_full_name
            and target_table
            and target_table != main_table_full_name
        ):
            downstream_set.add(target_table)
        elif (
            target_table == main_table_full_name
            and source_table
            and source_table != main_table_full_name
        ):
            upstream_set.add(source_table)

        if notebook_id and job_id:
            unique_job_ids.add(job_id)
            notebook_job_pairs.append(
                {
                    "notebook_id": notebook_id,
                    "job_id": job_id,
                    "source_table": source_table,
                    "target_table": target_table,
                }
            )

    for job_id in unique_job_ids:
        _get_job_info_cached(job_id)

    for pair in notebook_job_pairs:
        nb_id = pair["notebook_id"]
        jb_id = pair["job_id"]
        notebook_info = _resolve_notebook_info_optimized(nb_id, jb_id)
        formatted_info = _format_notebook_info_optimized(notebook_info)

        if pair["source_table"] == main_table_full_name:
            notebooks_reading_dict[pair["notebook_id"]] = formatted_info
        elif pair["target_table"] == main_table_full_name:
            notebooks_writing_dict[pair["notebook_id"]] = formatted_info

    processed_data["upstream_tables"] = sorted(upstream_set)
    processed_data["downstream_tables"] = sorted(downstream_set)
    processed_data["notebooks_reading"] = sorted(notebooks_reading_dict.values())
    processed_data["notebooks_writing"] = sorted(notebooks_writing_dict.values())

    total_time = time.time() - start_time
    print(f"Lineage processing took {total_time:.2f} seconds")

    return processed_data


def clear_lineage_cache() -> None:
    """Clear the job and notebook caches to free memory."""
    global _job_cache, _notebook_cache
    _job_cache = {}
    _notebook_cache = {}


def _get_table_lineage(table_full_name: str) -> dict[str, Any]:
    """Retrieve table lineage information for a given table."""
    if not get_warehouse_id():
        return {
            "status": "error",
            "error": "sql_warehouse_id is not configured. Cannot fetch lineage.",
        }

    lineage_sql_query = f"""
    SELECT source_table_full_name, target_table_full_name, entity_type, entity_id,
           entity_run_id, entity_metadata, created_by, event_time
    FROM system.access.table_lineage
    WHERE source_table_full_name = '{table_full_name}'
       OR target_table_full_name = '{table_full_name}'
    ORDER BY event_time DESC LIMIT 100;
    """  # noqa: S608
    raw_lineage_output = execute_databricks_sql(lineage_sql_query)
    return _process_lineage_results(raw_lineage_output, table_full_name)


def _format_single_table_md(
    table_info: TableInfo, base_heading_level: int, display_columns: bool
) -> list[str]:
    """Format details for a single TableInfo object into Markdown strings."""
    table_markdown_parts: list[str] = []
    table_header_prefix = "#" * base_heading_level
    sub_header_prefix = "#" * (base_heading_level + 1)

    full_name = table_info.full_name
    table_markdown_parts.append(f"{table_header_prefix} Table: **{full_name}**")

    if table_info.comment:
        table_markdown_parts.extend(["", f"**Description**: {table_info.comment}"])
    elif base_heading_level == 1:
        table_markdown_parts.extend(["", "**Description**: No description provided."])

    partition_column_names: list[str] = []
    if table_info.columns:
        temp_partition_cols = [
            (col.name or "", col.partition_index)
            for col in table_info.columns
            if col.partition_index is not None
        ]
        if temp_partition_cols:
            temp_partition_cols.sort(key=lambda x: x[1])
            partition_column_names = [name for name, _ in temp_partition_cols]

    if partition_column_names:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Partition Columns"])
        col_list = [f"- `{col_name}`" for col_name in partition_column_names]
        table_markdown_parts.extend(col_list)
    elif base_heading_level == 1:
        no_partition_msg = (
            "- *This table is not partitioned or partition key info is unavailable.*"
        )
        table_markdown_parts.extend(
            ["", f"{sub_header_prefix} Partition Columns", no_partition_msg]
        )

    if display_columns:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Table Columns"])
        if table_info.columns:
            table_markdown_parts.extend(_format_column_details_md(table_info.columns))
        else:
            table_markdown_parts.append("  - *No column information available.*")

    return table_markdown_parts


# Blocked SQL keywords for safety (prevent destructive operations)
BLOCKED_SQL_KEYWORDS = frozenset(
    [
        "DROP",
        "DELETE",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "INSERT",
        "UPDATE",
        "MERGE",
        "GRANT",
        "REVOKE",
    ]
)


def _is_dangerous_sql(sql_query: str) -> str | None:
    """Check if SQL contains dangerous keywords. Returns the keyword if found."""
    normalized = sql_query.upper().split()
    for keyword in BLOCKED_SQL_KEYWORDS:
        if keyword in normalized:
            return keyword
    return None


def execute_databricks_sql(
    sql_query: str,
    max_wait_seconds: int = 600,
    poll_interval_seconds: int = 10,
) -> dict[str, Any]:
    """Execute a SQL query on Databricks using the SDK client.

    Args:
        sql_query: The SQL query to execute
        max_wait_seconds: Max wait time for query completion (default: 600s)
        poll_interval_seconds: Interval between status checks (default: 10s)
    """
    # Block dangerous SQL commands
    blocked_keyword = _is_dangerous_sql(sql_query)
    if blocked_keyword:
        error_msg = (
            f"[Databricks MCP Server] Blocked: '{blocked_keyword}' statements are "
            "not allowed for safety reasons."
        )
        print(error_msg)
        return {
            "status": "error",
            "error": error_msg,
        }

    warehouse_id = get_warehouse_id()
    if not warehouse_id:
        err = "sql_warehouse_id is not configured. Cannot execute SQL query."
        return {"status": "error", "error": err}

    try:
        client = get_sdk_client()
        response: StatementResponse = client.statement_execution.execute_statement(
            statement=sql_query,
            warehouse_id=warehouse_id,
            wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )

        # Poll for completion if still pending
        elapsed_seconds = 0
        while (
            response.status
            and response.status.state == StatementState.PENDING
            and elapsed_seconds < max_wait_seconds
        ):
            time.sleep(poll_interval_seconds)
            elapsed_seconds += poll_interval_seconds
            if response.statement_id:
                response = client.statement_execution.get_statement(
                    response.statement_id
                )

        # Check if timed out while still pending
        if response.status and response.status.state == StatementState.PENDING:
            return {
                "status": "failed",
                "error": f"Query timed out after {max_wait_seconds} seconds",
            }

        if response.status and response.status.state == StatementState.SUCCEEDED:
            if response.result and response.result.data_array:
                manifest = response.manifest
                column_names: list[str] = []
                if manifest and manifest.schema and manifest.schema.columns:
                    column_names = [col.name or "" for col in manifest.schema.columns]
                raw_rows = response.result.data_array
                return {
                    "status": "success",
                    "row_count": len(raw_rows),
                    "columns": column_names,
                    "rows": raw_rows,
                }
            else:
                return {
                    "status": "success",
                    "row_count": 0,
                    "columns": [],
                    "rows": [],
                    "message": "Query succeeded but returned no data.",
                }
        elif response.status:
            status_error = response.status.error
            error_message = (
                status_error.message if status_error else "No error details provided."
            )
            state = response.status.state
            state_val = state.value if state else "UNKNOWN"
            return {
                "status": "failed",
                "error": f"Query execution failed with state: {state_val}",
                "details": error_message,
            }
        else:
            return {"status": "failed", "error": "Query execution status unknown."}
    except Exception as e:
        err_msg = f"An error occurred during SQL execution: {e!s}"
        return {"status": "error", "error": err_msg}


def get_uc_table_details(full_table_name: str, include_lineage: bool = False) -> str:
    """Fetch table metadata and optionally lineage, formatted as Markdown."""
    try:
        client = get_sdk_client()
        table_info: TableInfo = client.tables.get(full_name=full_table_name)
    except Exception as e:
        error_details = str(e)
        return f"""# Error: Could Not Retrieve Table Details
**Table:** `{full_table_name}`
**Problem:** Failed to fetch the complete metadata for this table.
**Details:**
```
{error_details}
```"""

    markdown_parts = _format_single_table_md(
        table_info, base_heading_level=1, display_columns=True
    )

    if include_lineage:
        markdown_parts.extend(["", "## Lineage Information"])
        if not get_warehouse_id():
            skip_msg = "- *Lineage skipped: `sql_warehouse_id` not configured.*"
            markdown_parts.append(skip_msg)
        else:
            lineage_info = _get_table_lineage(full_table_name)

            has_upstream = (
                lineage_info
                and isinstance(lineage_info.get("upstream_tables"), list)
                and lineage_info["upstream_tables"]
            )
            has_downstream = (
                lineage_info
                and isinstance(lineage_info.get("downstream_tables"), list)
                and lineage_info["downstream_tables"]
            )
            has_notebooks_reading = (
                lineage_info
                and isinstance(lineage_info.get("notebooks_reading"), list)
                and lineage_info["notebooks_reading"]
            )
            has_notebooks_writing = (
                lineage_info
                and isinstance(lineage_info.get("notebooks_writing"), list)
                and lineage_info["notebooks_writing"]
            )

            if has_upstream:
                up_header = "### Upstream Tables (tables this table reads from):"
                markdown_parts.extend(["", up_header])
                up_tables = [f"- `{t}`" for t in lineage_info["upstream_tables"]]
                markdown_parts.extend(up_tables)

            if has_downstream:
                header = "### Downstream Tables (tables that read from this table):"
                markdown_parts.extend(["", header])
                markdown_parts.extend(
                    [f"- `{table}`" for table in lineage_info["downstream_tables"]]
                )

            if has_notebooks_reading:
                markdown_parts.extend(["", "### Notebooks Reading from this Table:"])
                for notebook in lineage_info["notebooks_reading"]:
                    markdown_parts.extend([f"- {notebook}", ""])

            if has_notebooks_writing:
                markdown_parts.extend(["", "### Notebooks Writing to this Table:"])
                for notebook in lineage_info["notebooks_writing"]:
                    markdown_parts.extend([f"- {notebook}", ""])

            all_flags = [
                has_upstream,
                has_downstream,
                has_notebooks_reading,
                has_notebooks_writing,
            ]
            has_any = any(all_flags)
            if not has_any:
                if lineage_info and lineage_info.get("status") == "error":
                    markdown_parts.extend(
                        [
                            "",
                            "*Note: Could not retrieve complete lineage information.*",
                            f"> *Lineage fetch error: {lineage_info.get('error')}*",
                        ]
                    )
                else:
                    markdown_parts.append(
                        "- *No table, notebook, or job dependencies found.*"
                    )
    else:
        skip_msg = "- *Lineage fetching skipped as per request.*"
        markdown_parts.extend(["", "## Lineage Information", skip_msg])

    return "\n".join(markdown_parts)


def get_uc_schema_details(
    catalog_name: str, schema_name: str, include_columns: bool = False
) -> str:
    """Fetch detailed information for a specific schema."""
    full_schema_name = f"{catalog_name}.{schema_name}"
    markdown_parts = [f"# Schema Details: **{full_schema_name}**"]

    try:
        client = get_sdk_client()
        schema_info: SchemaInfo = client.schemas.get(full_name=full_schema_name)

        description = (
            schema_info.comment if schema_info.comment else "No description provided."
        )
        markdown_parts.append(f"**Description**: {description}")
        markdown_parts.append("")

        markdown_parts.append(f"## Tables in Schema `{schema_name}`")

        tables_iterable = client.tables.list(
            catalog_name=catalog_name, schema_name=schema_name
        )
        tables_list = list(tables_iterable)

        if not tables_list:
            markdown_parts.append("- *No tables found in this schema.*")
        else:
            for i, table_info in enumerate(tables_list):
                if not isinstance(table_info, TableInfo):
                    continue

                formatted = _format_single_table_md(
                    table_info,
                    base_heading_level=3,
                    display_columns=include_columns,
                )
                markdown_parts.extend(formatted)
                if i < len(tables_list) - 1:
                    markdown_parts.append("\n=============\n")
                else:
                    markdown_parts.append("")

    except Exception as e:
        err_msg = f"Failed to retrieve details for schema '{full_schema_name}': {e!s}"
        return f"""# Error: Could Not Retrieve Schema Details
**Schema:** `{full_schema_name}`
**Problem:** An error occurred while attempting to fetch schema information.
**Details:**
```
{err_msg}
```"""

    return "\n".join(markdown_parts)


def get_uc_catalog_details(catalog_name: str) -> str:
    """Fetch and format a summary of all schemas within a given catalog."""
    markdown_parts = [f"# Catalog Summary: **{catalog_name}**", ""]
    schemas_found_count = 0

    try:
        client = get_sdk_client()
        schemas_iterable = client.schemas.list(catalog_name=catalog_name)
        schemas_list = list(schemas_iterable)

        if not schemas_list:
            markdown_parts.append(f"No schemas found in catalog `{catalog_name}`.")
            return "\n".join(markdown_parts)

        schemas_found_count = len(schemas_list)
        msg = f"Found {schemas_found_count} schemas in catalog `{catalog_name}`:"
        markdown_parts.append(msg)
        markdown_parts.append("")

        for schema_info in schemas_list:
            if not isinstance(schema_info, SchemaInfo):
                continue

            schema_name_display = (
                schema_info.full_name if schema_info.full_name else "Unnamed Schema"
            )
            markdown_parts.append(f"## {schema_name_display}")

            s_comment = schema_info.comment
            desc = f"**Description**: {s_comment}" if s_comment else ""
            markdown_parts.append(desc)
            markdown_parts.append("")

    except Exception as e:
        err_msg = f"Failed to retrieve schemas for catalog '{catalog_name}': {e!s}"
        return f"""# Error: Could Not Retrieve Catalog Summary
**Catalog:** `{catalog_name}`
**Problem:** An error occurred while attempting to fetch schema information.
**Details:**
```
{err_msg}
```"""

    total_msg = f"**Total Schemas Found in `{catalog_name}`**: {schemas_found_count}"
    markdown_parts.append(total_msg)
    return "\n".join(markdown_parts)


def get_uc_all_catalogs_summary() -> str:
    """Fetch a summary of all available Unity Catalogs."""
    markdown_parts = ["# Available Unity Catalogs", ""]
    catalogs_found_count = 0

    try:
        client = get_sdk_client()
        catalogs_iterable = client.catalogs.list()
        catalogs_list = list(catalogs_iterable)

        if not catalogs_list:
            markdown_parts.append("- *No catalogs found or accessible.*")
            return "\n".join(markdown_parts)

        catalogs_found_count = len(catalogs_list)
        markdown_parts.append(f"Found {catalogs_found_count} catalog(s):")
        markdown_parts.append("")

        for catalog_info in catalogs_list:
            if not isinstance(catalog_info, CatalogInfo):
                continue

            markdown_parts.append(f"- **`{catalog_info.name}`**")
            cat_comment = catalog_info.comment
            desc = cat_comment if cat_comment else "No description provided."
            markdown_parts.append(f"  - **Description**: {desc}")

            catalog_type_str = "N/A"
            cat_type = catalog_info.catalog_type
            if cat_type and hasattr(cat_type, "value"):
                catalog_type_str = cat_type.value
            elif catalog_info.catalog_type:
                catalog_type_str = str(catalog_info.catalog_type)
            markdown_parts.append(f"  - **Type**: `{catalog_type_str}`")

            markdown_parts.append("")

    except Exception as e:
        error_message = f"Failed to retrieve catalog list: {e!s}"
        return f"""# Error: Could Not Retrieve Catalog List
**Problem:** An error occurred while attempting to fetch the list of catalogs.
**Details:**
```
{error_message}
```"""

    return "\n".join(markdown_parts)
