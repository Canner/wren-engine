---
name: wren-query
description: >
  Run, transpile, or validate a SQL query through the Wren semantic CLI.
  Use when the user asks to query a data source using wren, run wren --sql,
  transpile SQL through MDL, or test a wren query against MySQL/Postgres/etc.
argument-hint: "[sql query]"
allowed-tools: Read, Bash(uv run wren *), Bash(wren *)
---

The user wants to run a Wren CLI command. $ARGUMENTS is the SQL query or instruction.

## What to do

1. **Check for mdl.json and conn.json** in the current directory using Read or Glob.
   - If either is missing, tell the user what's needed and show the format below.
   - If both exist, proceed directly.

2. **Run the appropriate command** based on what the user asked:

| Intent | Command |
|--------|---------|
| Execute and return results | `uv run wren --sql '...'` |
| Translate to native SQL (no DB) | `uv run wren transpile --sql '...'` |
| Validate without fetching rows | `uv run wren dry-run --sql '...'` |
| Check SQL is valid | `uv run wren validate --sql '...'` |

If `wren` is installed globally (not via uv), use `wren` directly instead of `uv run wren`.

3. **Show the result** and explain what happened.

---

## Required files

Both files are auto-discovered from the current working directory.

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

### conn.json — connection info (include `datasource` field)
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
wren --sql '...' --mdl other-mdl.json --connection-file prod-conn.json
wren --sql '...' --output csv          # table (default) | csv | json
wren --sql '...' --limit 100
```

---

## Common errors

| Error | Fix |
|-------|-----|
| `mdl.json not found` | Create mdl.json in the current directory |
| `conn.json not found` | Create conn.json with a `datasource` field |
| `datasource key not found` | Add `"datasource": "mysql"` to conn.json |
| `unknown datasource 'X'` | Check spelling; see supported values above |
| Connection refused | Confirm the DB is running and host/port are correct |
