# Databricks MCP Server

This is a Model Context Protocol (MCP) server for executing SQL queries
against Databricks using the Statement Execution API.
It can retrieve data by performing SQL requests using the Databricks API.
When used in an Agent mode, it can successfully iterate over a number of
requests to perform complex tasks.
It is even better when coupled with Unity Catalog Metadata.

## 1. Features

- Execute SQL queries on Databricks
- List available schemas in a catalog
- List tables in a schema
- Describe table schemas

## 2. Setup

### 2.1. System Requirements

- Python 3.11+

### 2.2. Installation

Clone the repository:

```bash
git clone https://github.com/i9wa4/mcp-databricks-server.git
cd mcp-databricks-server
```

#### 2.2.1. Option 1: Using uv (Recommended)

If you have [uv](https://docs.astral.sh/uv/getting-started/installation/)
installed:

```bash
uv sync
```

#### 2.2.2. Option 2: Using pip

```bash
pip install .
```

### 2.3. Environment Variables

Set up your environment variables:

Option 1: Using a .env file (recommended)

Create a .env file with your Databricks credentials:

```text
DATABRICKS_HOST=your-databricks-instance.cloud.databricks.com
DATABRICKS_TOKEN=your-databricks-access-token
DATABRICKS_SQL_WAREHOUSE_ID=your-sql-warehouse-id
# for OAuth authentication
DATABRICKS_CLIENT_ID=your-client-id
DATABRICKS_CLIENT_SECRET=your-client-secret
DATABRICKS_AUTH_TYPE=oauth
```

Option 2: Setting environment variables directly

```bash
export DATABRICKS_HOST="your-databricks-instance.cloud.databricks.com"
export DATABRICKS_TOKEN="your-databricks-access-token"
export DATABRICKS_SQL_WAREHOUSE_ID="your-sql-warehouse-id"
# for OAuth authentication
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export DATABRICKS_AUTH_TYPE="oauth"
```

You can find your SQL warehouse ID in the Databricks UI under SQL Warehouses.

## 3. Permissions Requirements

Before using this MCP server, ensure that:

1. **SQL Warehouse Permissions**:
   The user associated with the provided token must have appropriate
   permissions to access the specified SQL warehouse.
   You can configure warehouse permissions in the Databricks UI under
   SQL Warehouses > [Your Warehouse] > Permissions.

2. **Token Permissions**:
   The personal access token (or OAuth access token) used should have the
   minimum necessary permissions to perform the required operations.
   It is strongly recommended to:
    - Create a dedicated token specifically for this application
    - Grant read-only permissions where possible to limit security risks
    - Avoid using tokens with workspace-wide admin privileges

3. **Data Access Permissions**:
   The user associated with the token must have appropriate permissions
   to access the catalogs, schemas, and tables that will be queried.

To set SQL warehouse permissions via the Databricks REST API, you can use:

- `GET /api/2.0/sql/permissions/warehouses/{warehouse_id}`
  to check current permissions
- `PATCH /api/2.0/sql/permissions/warehouses/{warehouse_id}`
  to update permissions

For security best practices, consider regularly rotating your access tokens
and auditing query history to monitor usage.

## 4. Running the Server

### 4.1. Standalone Mode

To run the server in standalone mode:

Using uv:

```bash
uv run mcp-databricks-server
```

Using pip installation:

```bash
mcp-databricks-server
```

Or using Python module:

```bash
python -m mcp_databricks_server
```

This will start the MCP server using stdio transport, which can be used with
Agent Composer or other MCP clients.

### 4.2. Using with Cursor

To use this MCP server with [Cursor](https://cursor.sh/), you need to
configure it in your Cursor settings:

1. Create a `.cursor` directory in your home directory if it doesn't
   already exist
2. Create or edit the `mcp.json` file in that directory:

```bash
mkdir -p ~/.cursor
touch ~/.cursor/mcp.json
```

1. Add the following configuration to the `mcp.json` file, replacing the
   directory path with the actual path to where you've installed this server:

Using uv:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/your/mcp-databricks-server",
                "run",
                "mcp-databricks-server"
            ]
        }
    }
}
```

Using pip installation:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "mcp-databricks-server"
        }
    }
}
```

Using Python module:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "python",
            "args": ["-m", "mcp_databricks_server"]
        }
    }
}
```

1. Restart Cursor to apply the changes

Now you can use the Databricks MCP server directly within Cursor's
AI assistant.

## 5. Available Tools

The server provides the following tools:

1. `execute_sql_query_in_databricks`:
   Execute a SQL query and return the results

   ```text
   execute_sql_query_in_databricks(sql: str) -> str
   ```

2. `list_schemas_in_databricks`:
   List all available schemas in a specific catalog

   ```text
   list_schemas_in_databricks(catalog: str) -> str
   ```

3. `list_tables_in_databricks`: List all tables in a specific schema

   ```text
   list_tables_in_databricks(schema: str) -> str
   ```

4. `describe_table_in_databricks`: Describe a table's schema

   ```text
   describe_table_in_databricks(table_name: str) -> str
   ```

## 6. Example Usage

In Agent Composer or other MCP clients, you can use these tools like:

```text
execute_sql_query_in_databricks("SELECT * FROM my_schema.my_table LIMIT 10")
list_schemas_in_databricks("my_catalog")
list_tables_in_databricks("my_catalog.my_schema")
describe_table_in_databricks("my_catalog.my_schema.my_table")
```

## 7. Handling Long-Running Queries

The server is designed to handle long-running queries by polling the
Databricks API until the query completes or times out.
The default timeout is 10 minutes (60 retries with 10-second intervals),
which can be adjusted in the `src/mcp_databricks_server/dbapi.py` file
if needed.

## 8. Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## 9. Dependencies

- httpx: For making HTTP requests to the Databricks API
- python-dotenv: For loading environment variables from .env file
- mcp: The Model Context Protocol library
