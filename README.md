# Databricks MCP Server

MCP server for executing SQL queries on Databricks.

## 1. Features

- Execute SQL queries on Databricks
- List available schemas in a catalog
- List tables in a schema
- Describe table schemas
- Unity Catalog metadata exploration (catalogs, schemas, tables)
- Table lineage information (upstream/downstream tables, notebooks)

## 2. Setup

### 2.1. Install uv

See [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/).

### 2.2. Clone

```bash
git clone https://github.com/i9wa4/mcp-databricks-server
cd mcp-databricks-server
```

### 2.3. Create .env

In the `mcp-databricks-server` directory, create `.env`:

```bash
DATABRICKS_HOST=your-databricks-instance.cloud.databricks.com
DATABRICKS_SQL_WAREHOUSE_ID=your-sql-warehouse-id

# Option A: Personal Access Token
DATABRICKS_TOKEN=your-databricks-access-token

# Option B: OAuth (Service Principal)
DATABRICKS_AUTH_TYPE=oauth
DATABRICKS_CLIENT_ID=your-client-id
DATABRICKS_CLIENT_SECRET=your-client-secret
```

## 3. Running

Example MCP Server configuration:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/mcp-databricks-server",
                "run",
                "mcp-databricks-server"
            ]
        }
    }
}
```

## 4. Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
