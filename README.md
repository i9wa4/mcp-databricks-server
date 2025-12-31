# Databricks MCP Server

MCP server for executing SQL queries on Databricks.

Built with [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html).

Forked from <https://github.com/RafaelCartenet/mcp-databricks-server>.

## 1. Features

- Execute SQL queries on Databricks
- List available schemas in a catalog
- List tables in a schema
- Describe table schemas
- Unity Catalog metadata exploration (catalogs, schemas, tables)
- Table lineage information (upstream/downstream tables, notebooks)
- Block dangerous SQL commands for safety. The following statements are blocked:
    - DROP
    - DELETE
    - TRUNCATE
    - ALTER
    - CREATE
    - INSERT
    - UPDATE
    - MERGE
    - GRANT
    - REVOKE

## 2. Setup

### 2.1. Install uv

See uv installation guide: <https://docs.astral.sh/uv/getting-started/installation/>.

### 2.2. Clone

```bash
git clone https://github.com/i9wa4/mcp-databricks-server
```

### 2.3. Configure Databricks Authentication

Create `~/.databrickscfg` for authentication.

Example:

```ini
# Personal Access Token
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi_your_personal_access_token
warehouse_id = your_warehouse_id
```

or

```ini
# Service Principal (OAuth M2M)
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
client_id = your_client_id
client_secret = your_client_secret
warehouse_id = your_warehouse_id
```

## 3. Running

Example MCP Server configuration:

```json
{
  "mcpServers": {
    "databricks": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/mcp-databricks-server",
        "run",
        "mcp-databricks-server"
      ],
      "env": {
        "DATABRICKS_CONFIG_PROFILE": "DEFAULT"
      },
      "disabled": false
    }
  }
}
```

`DATABRICKS_CONFIG_PROFILE` environment variable is optional.
If not set, the `DEFAULT` profile from `~/.databrickscfg` is used.

## 4. Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
