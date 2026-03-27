# wren-engine

Wren Engine CLI and Python SDK — semantic SQL layer for 20+ data sources.

Translate natural SQL queries through an MDL (Modeling Definition Language) semantic layer and execute them against your database.

## Installation

```bash
pip install wren-engine[mysql]      # MySQL
pip install wren-engine[postgres]   # PostgreSQL
pip install wren-engine[duckdb]     # DuckDB (local files)
pip install wren-engine[all]        # All connectors
```

## Quick start

**1. Create `~/.wren/mdl.json`** — your semantic model:

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
        { "name": "status",      "type": "varchar" }
      ],
      "primaryKey": "order_id"
    }
  ]
}
```

**2. Create `~/.wren/connection_info.json`** — your connection:

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

**3. Run queries** — `wren` auto-discovers both files from `~/.wren`:

```bash
wren --sql 'SELECT order_id FROM "orders" LIMIT 10'
```

For the full CLI reference and per-datasource `connection_info.json` formats, see [`docs/cli.md`](docs/cli.md) and [`docs/connections.md`](docs/connections.md).

---

## Python SDK

```python
import base64, orjson
from wren import WrenEngine, DataSource

manifest = { ... }  # your MDL dict
manifest_str = base64.b64encode(orjson.dumps(manifest)).decode()

with WrenEngine(manifest_str, DataSource.mysql, {"host": "...", ...}) as engine:
    result = engine.query('SELECT * FROM "orders" LIMIT 10')
    print(result.to_pandas())
```

---

## Running tests

```bash
just install-dev
```

| Command | What it runs | Docker needed |
|---------|-------------|---------------|
| `just test-unit` | Unit tests | No |
| `just test-duckdb` | DuckDB connector tests | No |
| `just test-postgres` | PostgreSQL connector tests | Yes |
| `just test-mysql` | MySQL connector tests | Yes |
| `just test` | All tests | Yes |
