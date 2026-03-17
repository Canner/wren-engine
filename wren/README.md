# wren

Wren Engine CLI and Python SDK — semantic SQL layer for 20+ data sources.

## Installation

```bash
pip install wren
```

## Usage

```python
from wren import WrenEngine, DataSource
```

See the [Wren Engine documentation](https://getwren.ai) for details.

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
| `just test` | All tests | Yes |

Run a specific connector via marker:

```bash
just test-connector postgres
```

To add tests for a new connector, subclass `WrenQueryTestSuite` in
`tests/connectors/test_<name>.py` and provide a class-scoped `engine` fixture.
All base tests are inherited automatically.
