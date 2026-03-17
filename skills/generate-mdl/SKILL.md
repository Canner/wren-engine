---
name: generate-mdl
description: Generate a Wren MDL manifest from a database using ibis-server metadata endpoints. Use when a user wants to create or set up a new Wren MDL, scaffold a manifest from an existing database, or onboard a new data source without installing any database drivers locally.
compatibility: Requires a running ibis-server (default port 8000). No local database drivers needed.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.4"
---

# Generate Wren MDL

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `generate-mdl` key with this skill's version (`1.4`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **generate-mdl** skill is available (remote: X.Y, installed: 1.4).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force generate-mdl
> ```

Then continue with the workflow below regardless of update status.

---

Generates a Wren MDL manifest by using ibis-server to introspect the database schema — no local database drivers required. All schema discovery goes through ibis-server, which already has drivers for all supported data sources.

## Workflow

Follow these steps in order. Do not skip steps or ask unnecessary questions between them.

### Step 1 — Verify connection and choose data source

> **Connection info can ONLY be configured through the Web UI at `http://localhost:9001`.** Do not attempt to set connection info programmatically via ibis-server API calls, curl, or any other method. The ibis-server does not expose a public API for writing connection info — only the Web UI can do this.

Confirm the MCP server has a working connection before proceeding:

```text
health_check()
```

If the health check fails, or if the user has not yet configured a connection, direct them to the Web UI at `http://localhost:9001` to enter their data source credentials. Wait for the user to confirm the connection is saved before continuing.

Ask the user for:
1. **Data source type** (e.g. `POSTGRES`, `BIGQUERY`, `SNOWFLAKE`, …) — needed to set `dataSource` in the MDL
2. **Schema filter** (optional) — if the database has many schemas, ask which schema(s) to include

After this step you will have:
- `data_source`: e.g. `"POSTGRES"`
- Optional `schema_filter`: used to narrow down results in subsequent steps

### Step 2 — Fetch table schema

```text
list_remote_tables()
```

Returns a list of tables with their column names and types. Each table entry has a `properties.schema` field — use it to filter to the user's target schema if specified.

If this fails:
1. Check that read-only mode is **disabled** in the Web UI (`http://localhost:9001`) — `list_remote_tables()` will fail when read-only mode is on, even if the connection is healthy.
2. Ask the user to verify connection info in the Web UI if read-only mode is already off.

### Step 3 — Fetch relationships

```text
list_remote_constraints()
```

Returns foreign key constraints. Use these to build `Relationship` entries in the MDL. If the response is empty (`[]`), infer relationships from column naming conventions (e.g. `order_id` → `orders.id`).

If this fails, verify that read-only mode is disabled in the Web UI (`http://localhost:9001`).

### Step 4 — Build MDL JSON

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

### Step 5 — Validate

Deploy the draft MDL and validate it with a dry run:

```text
deploy_manifest(mdl=<manifest dict>)
dry_run(sql="SELECT * FROM <any_model_name> LIMIT 1")
```

If `dry_run` succeeds, the MDL is valid. If it fails, fix the reported errors, call `deploy_manifest` again with the corrected MDL, and retry.

### Step 6 — Save project (optional)

Ask the user if they want to save the MDL as a YAML project directory (useful for version control).

If yes, follow the **wren-project** skill (`skills/wren-project/SKILL.md`) to write the YAML files and build `target/mdl.json`.

### Step 7 — Deploy final MDL

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

## Connection setup

Connection info is configured **exclusively** via the MCP server Web UI at `http://localhost:9001`. There is no API endpoint for setting connection info — do not attempt to configure it programmatically. See the **wren-mcp-setup** skill for Docker setup instructions.

> **Note:** If the Web UI is disabled (`WEB_UI_ENABLED=false`), connection info must be pre-configured in `~/.wren/connection_info.json` before starting the container. Use `/wren-connection-info` in Claude Code for the required fields per data source.
