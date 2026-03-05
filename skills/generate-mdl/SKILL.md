---
name: generate-mdl
description: Generate a Wren MDL manifest from a database using ibis-server metadata endpoints. Use when a user wants to create or set up a new Wren MDL, scaffold a manifest from an existing database, or onboard a new data source without installing any database drivers locally.
compatibility: Requires a running ibis-server and the Wren MCP server with tools: setup_connection, list_remote_tables, list_remote_constraints, mdl_validate_manifest, mdl_save_project, deploy_manifest
metadata:
  author: wren-engine
  version: "1.0"
---

# Generate Wren MDL

Generates a Wren MDL manifest by using ibis-server to introspect the database schema — no local database drivers required. All schema discovery goes through ibis-server, which already has drivers for all supported data sources.

## Workflow

Follow these steps in order. Do not skip steps or ask unnecessary questions between them.

### Step 1 — Gather connection info

Ask the user for:
1. **Data source type** — one of: `POSTGRES`, `MYSQL`, `MSSQL`, `DUCKDB`, `BIGQUERY`, `SNOWFLAKE`, `CLICKHOUSE`, `TRINO`, `ATHENA`, `ORACLE`, `DATABRICKS`
2. **Connection credentials** — see [Connection info format](#connection-info-format) below

Do not ask for a SQLAlchemy connection string. Use the structured `conn_info` dict instead.

### Step 2 — Register connection

```
setup_connection(datasource=<type>, conn_info=<dict>)
```

If this fails, report the error and ask the user to correct the credentials.

### Step 3 — Fetch table schema

```
list_remote_tables()
```

ibis-server returns a list of tables with their column names and types. Parse the response to understand the full database schema.

### Step 4 — Fetch relationships

```
list_remote_constraints()
```

ibis-server returns foreign key constraints. Use these to build `Relationship` entries in the MDL.

### Step 5 — Sample data (optional)

For columns where purpose is unclear from the name and type alone:

```
query("SELECT * FROM <table_name> LIMIT 3")
```

Note: use the raw table name (not model name) at this stage, since MDL is not yet deployed.

### Step 6 — Build MDL JSON

Construct the manifest following the [MDL structure](#mdl-structure) below.

Rules:
- `catalog`: use `"wren"` unless the user specifies otherwise
- `schema`: use the database's default schema (e.g. `"public"` for PostgreSQL, `"dbo"` for MSSQL)
- `dataSource`: set to the enum value from Step 1 (e.g. `"POSTGRES"`)
- Each table → one `Model`. Set `tableReference.table` to the exact table name
- Each column → one `Column`. Use the exact DB column name
- Mark primary key columns with `"isPrimaryKey": true` and set `primaryKey` on the model
- For FK columns, add a `Relationship` entry linking the two models
- Omit calculated columns for now — they can be added later

### Step 7 — Validate

```
mdl_validate_manifest(mdl=<manifest dict>)
```

This calls ibis-server's dry-plan endpoint. If validation fails, fix the reported errors and validate again.

### Step 8 — Save project (optional)

Ask the user if they want to save the MDL as a YAML project directory (useful for version control).

If yes, follow the **mdl-project** skill (`skills/mdl-project/SKILL.md`) to write the YAML files.

### Step 9 — Deploy

```
deploy_manifest(mdl=<manifest dict>)
```

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

Pass to `setup_connection(datasource=..., conn_info={...})`:

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
