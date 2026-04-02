# wren-engine

[![PyPI version](https://img.shields.io/pypi/v/wren-engine.svg)](https://pypi.org/project/wren-engine/)
[![Python](https://img.shields.io/pypi/pyversions/wren-engine.svg)](https://pypi.org/project/wren-engine/)
[![License](https://img.shields.io/pypi/l/wren-engine.svg)](https://github.com/Canner/wren-engine/blob/main/LICENSE)

Wren Engine CLI and Python SDK — semantic SQL layer for 20+ data sources.

Translate natural SQL queries through an [MDL (Modeling Definition Language)](https://docs.getwren.ai/) semantic layer and execute them against your database. Powered by [Apache DataFusion](https://datafusion.apache.org/) and [Ibis](https://ibis-project.org/).

## Installation

```bash
pip install wren-engine              # Core (DuckDB included)
pip install wren-engine[postgres]    # PostgreSQL
pip install wren-engine[mysql]       # MySQL
pip install wren-engine[bigquery]    # BigQuery
pip install wren-engine[snowflake]   # Snowflake
pip install wren-engine[clickhouse]  # ClickHouse
pip install wren-engine[trino]       # Trino
pip install wren-engine[mssql]       # SQL Server
pip install wren-engine[databricks]  # Databricks
pip install wren-engine[redshift]    # Redshift
pip install wren-engine[spark]       # Spark
pip install wren-engine[athena]      # Athena
pip install wren-engine[oracle]      # Oracle
pip install 'wren-engine[memory]'    # Schema & query memory (LanceDB)
pip install 'wren-engine[all]'       # All connectors + memory
```

Requires Python 3.11+.

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

**4. Index schema for semantic search** (optional, requires `wren-engine[memory]`):

```bash
wren memory index                              # index MDL schema
wren memory fetch -q "customer order price"    # fetch relevant schema context
wren memory store --nl "top customers" --sql "SELECT ..."  # store NL→SQL pair
wren memory recall -q "best customers"         # retrieve similar past queries
```

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

## Development

```bash
just install-dev    # Install with dev dependencies
just lint           # Ruff format check + lint
just format         # Auto-fix
```

| Command | What it runs | Docker needed |
|---------|-------------|---------------|
| `just test-unit` | Unit tests | No |
| `just test-duckdb` | DuckDB connector tests | No |
| `just test-postgres` | PostgreSQL connector tests | Yes |
| `just test-mysql` | MySQL connector tests | Yes |
| `just test` | All tests | Yes |

## Publishing

```bash
./scripts/publish.sh            # Build + publish to PyPI
./scripts/publish.sh --test     # Build + publish to TestPyPI
./scripts/publish.sh --build    # Build only
```

## License

Apache-2.0
