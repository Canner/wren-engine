---
name: wren-query
description: >
  Run, dry-plan, or validate a SQL query through the Wren semantic CLI.
  Use when the user asks to query a data source using wren, run wren --sql,
  dry-plan SQL through MDL, or test a wren query against MySQL/Postgres/etc.
argument-hint: "[sql query]"
allowed-tools: Read, Bash(uv run wren *), Bash(wren *)
---

The user wants to run a Wren CLI command. $ARGUMENTS is the SQL query or instruction.

## What to do

1. **Check for `~/.wren/mdl.json` and `~/.wren/connection_info.json`** using Read or Glob.
   - If either is missing, tell the user what's needed and show the format below.
   - If both exist, proceed directly.

2. **Run the appropriate command** based on what the user asked:

| Intent | Command |
|--------|---------|
| Execute and return results | `uv run wren --sql '...'` |
| Translate to native SQL (no DB) | `uv run wren dry-plan --sql '...'` |
| Validate without fetching rows | `uv run wren dry-run --sql '...'` |
| Check SQL is valid | `uv run wren validate --sql '...'` |

If `wren` is installed globally (not via uv), use `wren` directly instead of `uv run wren`.

3. **Show the result** and explain what happened.

---

## Required files

Both files are auto-discovered from `~/.wren/`.

### mdl.json — semantic model
```json
{
  "catalog": "wren",
  "schema": "public",
  "models": [
    {
      "name": "orders",
      "tableReference": { "schema": "mydb", "table": "orders" },
      "columns": [
        { "name": "order_id",  "type": "integer" },
        { "name": "total",     "type": "double" },
        { "name": "status",    "type": "varchar" }
      ],
      "primaryKey": "order_id"
    }
  ]
}
```

### connection_info.json — connection info (include `datasource` field)
```json
{
  "datasource": "mysql",
  "host": "localhost",
  "port": 3306,
  "database": "mydb",
  "user": "root",
  "password": "secret"
}
```

Supported datasource values: `mysql`, `postgres`, `bigquery`, `snowflake`,
`clickhouse`, `trino`, `mssql`, `databricks`, `redshift`, `oracle`, `duckdb`.

---

## Override flags

When needed, flags can override the defaults:

```bash
wren --sql '...' --mdl other-mdl.json --connection-file prod-connection_info.json
wren --sql '...' --output csv          # table (default) | csv | json
wren --sql '...' --limit 100
```

---

## Common errors

| Error | Fix |
|-------|-----|
| `mdl.json not found` | Create `~/.wren/mdl.json` |
| `connection_info.json not found` | Create `~/.wren/connection_info.json` with a `datasource` field |
| `datasource key not found` | Add `"datasource": "mysql"` to connection_info.json |
| `unknown datasource 'X'` | Check spelling; see supported values above |
| Connection refused | Confirm the DB is running and host/port are correct |
