# Wren MCP Tool Reference — HTTP JSON-RPC

Complete reference for calling each Wren MCP tool via `tools/call`.

Every example below assumes:
- **Base URL:** `http://localhost:9000/mcp`
- **Headers:** `Content-Type: application/json`, `Accept: application/json, text/event-stream`, `Mcp-Session-Id: <SESSION_ID>`

---

## Health & Status

### health_check

Check if Wren Engine is healthy and all configuration is in place. Returns a status report with actionable guidance for any missing configuration.

```json
{ "name": "health_check", "arguments": {} }
```

### is_deployed

Check if an MDL manifest is currently deployed.

```json
{ "name": "is_deployed", "arguments": {} }
```

### get_version

Get the MCP server version string.

```json
{ "name": "get_version", "arguments": {} }
```

---

## Deployment

### deploy

Deploy an MDL from a JSON file path (path inside the container).

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `mdl_file_path` | string | yes | Path to MDL JSON file inside the container (e.g. `/workspace/target/mdl.json`) |

```json
{ "name": "deploy", "arguments": { "mdl_file_path": "/workspace/target/mdl.json" } }
```

### deploy_manifest

Deploy an MDL manifest dict directly (no file needed).

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `mdl` | object | yes | The full MDL manifest as a JSON object |

```json
{
  "name": "deploy_manifest",
  "arguments": {
    "mdl": {
      "catalog": "wren",
      "schema": "public",
      "dataSource": "POSTGRES",
      "models": [
        {
          "name": "orders",
          "tableReference": { "schema": "public", "table": "orders" },
          "columns": [
            { "name": "order_id", "type": "INTEGER" },
            { "name": "status", "type": "VARCHAR" }
          ],
          "primaryKey": "order_id"
        }
      ],
      "relationships": [],
      "views": []
    }
  }
}
```

### mdl_validate_manifest

Validate an MDL manifest via ibis-server dry-plan without deploying.

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `mdl` | object | yes | The MDL manifest to validate |

```json
{ "name": "mdl_validate_manifest", "arguments": { "mdl": { "catalog": "wren", "schema": "public", "dataSource": "POSTGRES", "models": [] } } }
```

---

## Query Execution

### query

Execute a SQL query against the deployed MDL. Returns `{"columns":[...], "data":[...], "dtypes":{...}}`.

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `sql` | string | yes | The SQL query to execute |

```json
{ "name": "query", "arguments": { "sql": "SELECT * FROM orders LIMIT 10" } }
```

### dry_run

Validate a SQL query without executing it (cheap syntax + schema check).

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `sql` | string | yes | The SQL query to validate |

```json
{ "name": "dry_run", "arguments": { "sql": "SELECT * FROM orders LIMIT 10" } }
```

---

## Metadata — MDL Introspection

These tools read from the currently deployed MDL manifest.

### get_manifest

Get the full deployed MDL manifest as JSON.

```json
{ "name": "get_manifest", "arguments": {} }
```

### get_available_tables

List all table (model) names in the deployed MDL. Returns an array of strings.

```json
{ "name": "get_available_tables", "arguments": {} }
```

### get_table_info

Get info for a specific table including column names.

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table_name` | string | yes | Name of the model/table |

```json
{ "name": "get_table_info", "arguments": { "table_name": "orders" } }
```

### get_column_info

Get detailed info for a specific column (type, expression, relationship, etc.).

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table_name` | string | yes | Name of the model/table |
| `column_name` | string | yes | Name of the column |

```json
{ "name": "get_column_info", "arguments": { "table_name": "orders", "column_name": "order_id" } }
```

### get_table_columns_info

Batch get column info for multiple tables at once. Efficient for large MDL manifests.

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table_columns` | array | yes | Array of `{"table_name": "...", "column_names": [...]}` objects. Omit `column_names` to get all columns. |
| `full_column_info` | boolean | no | If `true`, return full column metadata. Default `false` (names only). |

```json
{
  "name": "get_table_columns_info",
  "arguments": {
    "table_columns": [
      { "table_name": "orders", "column_names": ["order_id", "status"] },
      { "table_name": "customers" }
    ],
    "full_column_info": true
  }
}
```

### get_relationships

Get all relationships defined in the deployed MDL.

```json
{ "name": "get_relationships", "arguments": {} }
```

### get_current_data_source_type

Get the current data source type (e.g. `postgres`, `bigquery`, `duckdb`).

```json
{ "name": "get_current_data_source_type", "arguments": {} }
```

### get_available_functions

Get all SQL functions available for the connected data source.

```json
{ "name": "get_available_functions", "arguments": {} }
```

### get_wren_guide

Get usage tips for Wren Engine, tailored to the connected data source type.

```json
{ "name": "get_wren_guide", "arguments": {} }
```

---

## Remote Database Introspection

These tools query the live database (not the MDL). Blocked when read-only mode is active.

### list_remote_tables

List all tables in the connected database with schema and column metadata.

```json
{ "name": "list_remote_tables", "arguments": {} }
```

### list_remote_constraints

List foreign key constraints in the connected database.

```json
{ "name": "list_remote_constraints", "arguments": {} }
```
