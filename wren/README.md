# wren

Wren Engine CLI and Python SDK — semantic SQL layer for 20+ data sources.

Translate natural SQL queries through an MDL (Modeling Definition Language) semantic layer and execute them against your database. Works with MySQL, PostgreSQL, DuckDB, BigQuery, Snowflake, and more.

## Installation

```bash
pip install wren[mysql]      # MySQL / Doris
pip install wren[postgres]   # PostgreSQL
pip install wren[bigquery]   # BigQuery
pip install wren[duckdb]     # DuckDB (local files)
pip install wren[all]        # All connectors
```

## Quick start

**1. Add `mdl.json`** — your semantic model:

```json
{
  "catalog": "wren",
  "schema": "public",
  "models": [
    {
      "name": "orders",
      "tableReference": { "schema": "mydb", "table": "orders" },
      "columns": [
        { "name": "order_id",    "type": "integer" },
        { "name": "customer_id", "type": "integer" },
        { "name": "total",       "type": "double" },
        { "name": "status",      "type": "varchar" },
        {
          "name": "order_label",
          "type": "varchar",
          "expression": "concat(cast(order_id as varchar), '_', status)"
        }
      ],
      "primaryKey": "order_id"
    }
  ]
}
```

**2. Add `conn.json`** — your connection (include a `datasource` field):

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

**3. Run queries** — `wren` auto-discovers both files from the current directory:

```bash
wren --sql 'SELECT order_id, order_label FROM "orders" LIMIT 10'
```

That's it. No `--mdl`, `--datasource`, or `--connection-file` flags needed.

---

## CLI reference

### Default command — query

Running `wren --sql '...'` executes a query and prints the result. This is the same as `wren query --sql '...'`.

```bash
wren --sql 'SELECT COUNT(*) FROM "orders"'
wren --sql 'SELECT * FROM "orders" LIMIT 5' --output csv
wren --sql 'SELECT * FROM "orders"' --limit 100 --output json
```

Output formats: `table` (default), `csv`, `json`.

### `wren query`

Execute SQL and return results.

```bash
wren query --sql 'SELECT order_id, total FROM "orders" ORDER BY total DESC LIMIT 5'
```

### `wren transpile`

Translate MDL SQL to the native dialect SQL for your data source. No database connection required.

```bash
wren transpile --sql 'SELECT order_id, order_label FROM "orders"'
```

### `wren dry-run`

Validate SQL against the live database without returning rows. Prints `OK` on success.

```bash
wren dry-run --sql 'SELECT * FROM "orders" LIMIT 1'
```

### `wren validate`

Same as `dry-run` but prints `Valid` / `Invalid: <reason>`.

```bash
wren validate --sql 'SELECT * FROM "NonExistent"'
# Invalid: table not found ...
```

### Overriding defaults

All flags are optional when `mdl.json` and `conn.json` exist in the current directory. You can override any of them:

```bash
wren --sql '...' \
  --mdl /path/to/other-mdl.json \
  --connection-file /path/to/prod-conn.json \
  --datasource postgres
```

Or pass connection info inline:

```bash
wren --sql 'SELECT COUNT(*) FROM "orders"' \
  --connection-info '{"datasource":"mysql","host":"localhost","port":3306,"database":"mydb","user":"root","password":"secret"}'
```

---

## conn.json format by datasource

<details>
<summary>MySQL</summary>

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
</details>

<details>
<summary>PostgreSQL</summary>

```json
{
  "datasource": "postgres",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "user": "postgres",
  "password": "secret"
}
```
</details>

<details>
<summary>BigQuery</summary>

```json
{
  "datasource": "bigquery",
  "project_id": "my-gcp-project",
  "dataset": "my_dataset",
  "credentials": "<base64-encoded-service-account-json>"
}
```
</details>

<details>
<summary>Snowflake</summary>

```json
{
  "datasource": "snowflake",
  "user": "myuser",
  "password": "secret",
  "account": "myorg-myaccount",
  "database": "MYDB",
  "sf_schema": "PUBLIC"
}
```
</details>

---

## Python SDK

Use `WrenEngine` directly in Python code:

```python
import base64, orjson
from wren import WrenEngine, DataSource

manifest = { ... }  # your MDL dict
manifest_str = base64.b64encode(orjson.dumps(manifest)).decode()

conn_info = {
    "host": "localhost",
    "port": 3306,
    "database": "mydb",
    "user": "root",
    "password": "secret",
}

with WrenEngine(manifest_str, DataSource.mysql, conn_info) as engine:
    # Execute and get a PyArrow Table
    result = engine.query('SELECT * FROM "orders" LIMIT 10')
    print(result.to_pandas())

    # Translate to native dialect SQL (no DB needed)
    sql = engine.transpile('SELECT * FROM "orders"')
    print(sql)

    # Validate without fetching rows
    engine.dry_run('SELECT * FROM "orders" LIMIT 1')
```

---

## Running tests

Install dev dependencies first:

```bash
just install-dev
```

| Command | What it runs | Docker needed |
|---------|-------------|---------------|
| `just test-unit` | Unit tests (transpile, dry-plan, context manager) | No |
| `just test-duckdb` | DuckDB connector tests — generates TPCH data via `dbgen` | No |
| `just test-postgres` | PostgreSQL connector tests — spins up a container | Yes |
| `just test-mysql` | MySQL connector tests — spins up a container | Yes |
| `just test` | All tests | Yes |

Run a specific connector via marker:

```bash
just test-connector mysql
```

To add tests for a new connector, subclass `WrenQueryTestSuite` in
`tests/connectors/test_<name>.py` and provide a class-scoped `engine` fixture.
All base tests are inherited automatically. See `tests/suite/query.py` for the full guide.
