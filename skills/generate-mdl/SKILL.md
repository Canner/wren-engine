---
name: generate-mdl
description: Generate a Wren MDL manifest from a database using ibis-server metadata endpoints. Use when a user wants to create or set up a new Wren MDL, scaffold a manifest from an existing database, or onboard a new data source without installing any database drivers locally.
compatibility: Requires a running ibis-server (default port 8000). No local database drivers needed.
metadata:
  author: wren-engine
  version: "1.1"
---

# Generate Wren MDL

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `generate-mdl` key with this skill's version (`1.1`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **generate-mdl** skill is available (remote: X.Y, installed: 1.1).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force generate-mdl
> ```

Then continue with the workflow below regardless of update status.

---

Generates a Wren MDL manifest by using ibis-server to introspect the database schema — no local database drivers required. All schema discovery goes through ibis-server, which already has drivers for all supported data sources.

## Workflow

Follow these steps in order. Do not skip steps or ask unnecessary questions between them.

### Step 1 — Gather connection info

Ask the user for:
1. **Data source type** — one of: `POSTGRES`, `MYSQL`, `MSSQL`, `DUCKDB`, `BIGQUERY`, `SNOWFLAKE`, `CLICKHOUSE`, `TRINO`, `ATHENA`, `ORACLE`, `DATABRICKS`
2. **Connection credentials** — see [Connection info format](#connection-info-format) below
3. **Schema filter** (optional) — if the database has many schemas, ask which schema(s) to include

Do not ask for a SQLAlchemy connection string. Use the structured `connectionInfo` dict instead.

> **Important:** If the database runs on the host machine and ibis-server runs inside Docker, replace `localhost` / `127.0.0.1` with `host.docker.internal` in the host field.

### Step 2 — Fetch table schema

Call the ibis-server metadata endpoint directly:

```
POST http://localhost:8000/v3/connector/<data_source>/metadata/tables
Content-Type: application/json

{
  "connectionInfo": { <credentials dict> }
}
```

ibis-server returns a list of tables with their column names and types. Each table entry has a `properties.schema` field — use it to filter to the user's target schema if specified.

If this fails, report the error and ask the user to correct the credentials.

### Step 3 — Fetch relationships

```
POST http://localhost:8000/v3/connector/<data_source>/metadata/constraints
Content-Type: application/json

{
  "connectionInfo": { <credentials dict> }
}
```

Returns foreign key constraints. Use these to build `Relationship` entries in the MDL. If the response is empty (`[]`), infer relationships from column naming conventions (e.g. `order_id` → `orders.id`).

### Step 4 — Sample data (optional)

For columns where purpose is unclear from the name and type alone, query a few rows using the raw table name with schema prefix:

```
POST http://localhost:8000/v3/connector/<data_source>/query
Content-Type: application/json

{
  "sql": "SELECT * FROM <schema>.<table> LIMIT 3",
  "manifestStr": "",
  "connectionInfo": { <credentials dict> }
}
```

Note: use the raw `schema.table` reference at this stage, since the MDL is not yet deployed.

### Step 5 — Build MDL JSON

Construct the manifest following the [MDL structure](#mdl-structure) below.

Rules:
- `catalog`: use `"wren"` unless the user specifies otherwise
- `schema`: use the target schema name (e.g. `"public"` for PostgreSQL default, `"jaffle_shop"` if user specified)
- `dataSource`: set to the enum value from Step 1 (e.g. `"POSTGRES"`)
- `tableReference.catalog`: set to the database name (not `"wren"`)
- Each table → one `Model`. Set `tableReference.table` to the exact table name
- Each column → one `Column`. Use the exact DB column name
- Mark primary key columns with `"isPrimaryKey": true` and set `primaryKey` on the model
- For FK columns, add a `Relationship` entry linking the two models
- Omit calculated columns for now — they can be added later

### Step 6 — Validate

Validate the MDL by running a dry-plan against a simple query. Base64-encode the manifest first:

```python
import json, base64
manifest_b64 = base64.b64encode(json.dumps(mdl).encode()).decode()
```

Then call:

```
POST http://localhost:8000/v3/connector/<data_source>/dry-plan
Content-Type: application/json

{
  "manifestStr": "<base64-encoded manifest>",
  "sql": "SELECT * FROM <any_model_name> LIMIT 1"
}
```

If validation succeeds, the response is the planned SQL string. If it fails, fix the reported errors and validate again.

> **Note:** Use the `/v3/` endpoint, not `/v2/`. The v2 dry-plan requires a separate Wren Engine Java process (`WREN_ENGINE_ENDPOINT`) which is not part of the standard Docker setup.

### Step 7 — Save project (optional)

Ask the user if they want to save the MDL as a YAML project directory (useful for version control).

If yes, follow the **wren-project** skill (`skills/wren-project/SKILL.md`) to write the YAML files and build `target/mdl.json` + `target/connection.json`.

### Step 8 — Deploy

**If Wren MCP tools are available** (i.e., Claude Code has the `wren` MCP server registered):

```
deploy_manifest(mdl=<manifest dict>)
```

**If MCP tools are not available**, deploy by writing the MDL to the workspace file that the container watches:

1. Build `target/mdl.json` from the YAML project (see wren-project skill)
2. Ensure the container was started with `-e MDL_PATH=/workspace/target/mdl.json`
3. Restart the container to reload — or call the `deploy` MCP tool after connecting

Confirm success to the user. The MDL is now active and queries can run.

---

## MDL Structure

```json
{
  "catalog": "wren",
  "schema": "public",
  "dataSource": "POSTGRES",
  "models": [
    {
      "name": "orders",
      "tableReference": {
        "catalog": "",
        "schema": "public",
        "table": "orders"
      },
      "columns": [
        {
          "name": "order_id",
          "type": "INTEGER",
          "isCalculated": false,
          "notNull": true,
          "isPrimaryKey": true,
          "properties": {}
        },
        {
          "name": "customer_id",
          "type": "INTEGER",
          "isCalculated": false,
          "notNull": false,
          "properties": {}
        },
        {
          "name": "total",
          "type": "DECIMAL",
          "isCalculated": false,
          "notNull": false,
          "properties": {}
        }
      ],
      "primaryKey": "order_id",
      "cached": false,
      "properties": {}
    }
  ],
  "relationships": [
    {
      "name": "orders_customer",
      "models": ["orders", "customers"],
      "joinType": "MANY_TO_ONE",
      "condition": "orders.customer_id = customers.customer_id"
    }
  ],
  "views": []
}
```

### Column types

Map SQL/ibis types to MDL type strings:

| SQL / ibis type | MDL type |
|-----------------|----------|
| INT, INTEGER, INT4 | `INTEGER` |
| BIGINT, INT8 | `BIGINT` |
| SMALLINT, INT2 | `SMALLINT` |
| FLOAT, FLOAT4, REAL | `FLOAT` |
| DOUBLE, FLOAT8 | `DOUBLE` |
| DECIMAL, NUMERIC | `DECIMAL` |
| VARCHAR, TEXT, STRING | `VARCHAR` |
| CHAR | `CHAR` |
| BOOLEAN, BOOL | `BOOLEAN` |
| DATE | `DATE` |
| TIMESTAMP, DATETIME | `TIMESTAMP` |
| TIMESTAMPTZ | `TIMESTAMPTZ` |
| JSON, JSONB | `JSON` |
| ARRAY | `ARRAY` |
| BYTES, BYTEA | `BYTES` |

When in doubt, use `VARCHAR` as a safe fallback.

### Relationship join types

| Cardinality | `joinType` value |
|-------------|-----------------|
| Many-to-one (FK table → PK table) | `MANY_TO_ONE` |
| One-to-many | `ONE_TO_MANY` |
| One-to-one | `ONE_TO_ONE` |
| Many-to-many | `MANY_TO_MANY` |

---

## Connection info format

Pass to `setup_connection(datasource=..., connectionInfo={...})`:

```
POSTGRES    : {"host": "...", "port": "5432", "user": "...", "password": "...", "database": "..."}
MYSQL       : {"host": "...", "port": "3306", "user": "...", "password": "...", "database": "..."}
MSSQL       : {"host": "...", "port": "1433", "user": "...", "password": "...", "database": "..."}
DUCKDB      : {"path": "<file path>"}
BIGQUERY    : {"project": "...", "dataset": "...", "credentials_base64": "..."}
SNOWFLAKE   : {"account": "...", "user": "...", "password": "...", "database": "...", "schema": "..."}
CLICKHOUSE  : {"host": "...", "port": "8123", "user": "...", "password": "...", "database": "..."}
TRINO       : {"host": "...", "port": "8080", "user": "...", "catalog": "...", "schema": "..."}
ORACLE      : {"host": "...", "port": "1521", "user": "...", "password": "...", "database": "..."}
DATABRICKS  : {"host": "...", "httpPath": "...", "token": "..."}
```

The `datasource` value must match the `dataSource` field in the MDL exactly.
