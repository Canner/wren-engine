# ibis-server

FastAPI web server powering Wren Engine. Receives SQL queries, processes them through wren-core (Rust/DataFusion via PyO3), and executes against 22+ data sources using Ibis.

## Architecture

```text
REST client (MCP server / Wren Studio)
  → POST /v3/connector/{dataSource}/query (SQL + MDL base64 + connectionInfo)
  → MDL parsing & validation
  → wren-core-py (PyO3 FFI) → wren-core Rust → DataFusion
  → Ibis expression → connector SQL → data source
  → Result (+ optional query cache)
  [If v3 fails → fallback to Java engine v2]
```

### Key files

| Path | Purpose |
|---|---|
| `app/main.py` | FastAPI app init, middleware, lifespan, `/health`, `/config` |
| `app/routers/v3/connector.py` | Main endpoints: `query`, `dry-run`, `functions`, `metadata/tables`, `metadata/constraints` |
| `app/routers/v2/connector.py` | Legacy Java engine endpoints (fallback) |
| `app/mdl/core.py` | `WrenContext`: manifest parsing, symbol table, session management |
| `app/mdl/rewriter.py` | Query rewriting logic |
| `app/mdl/substitute.py` | Model substitution |
| `app/model/metadata/factory.py` | `MetadataFactory.create()` — instantiates connector by data source |
| `app/model/metadata/metadata.py` | Abstract `Metadata` base class |
| `app/model/error.py` | `WrenError`, `ErrorCode`, `ErrorResponse` |
| `app/query_cache/manager.py` | Query result caching |
| `app/custom_ibis/` | Ibis extensions (custom data type mappings) |
| `app/custom_sqlglot/dialects/` | SQLGlot dialect extensions (Wren, MySQL) |

## Dev Commands

```bash
just install         # Poetry install + build wren-core-py wheel + cython rebuild
just dev             # Dev server port 8000 (auto-reload)
just run             # Production server port 8000
just run-gunicorn    # Gunicorn with 2 workers

just test <MARKER>   # Run tests by marker (e.g., just test postgres)
just lint            # ruff format check + ruff check
just format          # ruff auto-fix + taplo

just compose-up      # docker-compose with Java engine
just compose-down
```

**macOS psycopg2 fix** (missing OpenSSL):
```bash
LDFLAGS="-L$(brew --prefix openssl)/lib" CPPFLAGS="-I$(brew --prefix openssl)/include" just install
```

## Environment Variables

**Required for tests:**
```bash
export QUERY_CACHE_STORAGE_TYPE=local
export WREN_ENGINE_ENDPOINT=http://localhost:8080
export WREN_WEB_ENDPOINT=http://localhost:3000
export PROFILING_STORE_PATH=file:///tmp/profiling
```

**Runtime:**

| Variable | Default | Description |
|---|---|---|
| `WREN_ENGINE_ENDPOINT` | — | Java engine fallback URL |
| `APP_TIMEOUT_SECONDS` | `240` | Request timeout |
| `WREN_NUM_WORKERS` | `2` | Gunicorn worker count |
| `QUERY_CACHE_STORAGE_TYPE` | — | Cache backend (`local`) |
| `OTLP_ENABLED` | — | Enable OpenTelemetry tracing |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | — | OTLP collector URL |

## Test Markers

Tests use testcontainers (Docker required). Each connector has its own marker:

```text
postgres  mysql  mssql  bigquery  snowflake  clickhouse  trino  oracle
athena  athena_spark  databricks  spark  local_file  s3_file  gcs_file
minio_file  functions  profile  cache  unit  enterprise  beta
```

**Example:**
```bash
just test clickhouse
just test unit         # Fast unit tests (no Docker)
```

Test files live in `tests/routers/v3/connector/{datasource}/`. Each connector has a `conftest.py` with testcontainer setup. TPCH test data is generated via DuckDB's TPCH extension.

## Adding a New Connector

1. Create `app/model/metadata/{connector}.py` implementing `Metadata` base class
2. Register in `app/model/metadata/factory.py`
3. Add Ibis backend dependency to `pyproject.toml`
4. Create test directory `tests/routers/v3/connector/{connector}/` with `conftest.py`
5. See `docs/how-to-add-data-source.md` for full guide

## Known Limitations

- **Correlated subqueries**: `ModelAnalyzeRule` cannot resolve outer column references inside correlated subqueries. Affects TPCH Q2, Q4, Q15, Q17, Q20, Q21, Q22. See `tests/routers/v3/connector/clickhouse/TPCH_ISSUES.md`.
- **wren-core fork**: Uses `https://github.com/Canner/datafusion.git` branch `canner/v49.0.1`
- **Ibis fork**: `https://github.com/Canner/ibis.git` branch `canner/10.8.1`
- Python version locked to `>=3.11,<3.12`
