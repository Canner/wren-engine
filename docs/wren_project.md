# Wren Project

This guide explains the Wren MDL project structure, file formats, and typical workflow for managing your data models as a collection of YAML files. The Wren Project format makes it easy to version control your models, track changes, and separate connection information from model definitions.

A Wren MDL project is a directory of YAML files that makes MDL manifests human-readable and version-control friendly — similar to how dbt organizes models. Instead of managing a single large JSON file, each model lives in its own YAML file, and the project is compiled to a deployable `mdl.json` when needed.

YAML files use **snake_case** field names for readability. The compiled `target/mdl.json` uses **camelCase**, which is the wire format expected by ibis-server.

## Project Structure

```text
my_project/
├── wren_project.yml       # Project metadata (catalog, schema, data_source)
├── connection.yml         # Data source connection parameters
├── models/
│   ├── orders.yml         # One file per model
│   ├── customers.yml
│   └── ...
├── relationships.yml      # All relationships
└── views.yml              # All views
```

After building, compiled files are written to:

```text
my_project/
└── target/
    ├── mdl.json           # Deployable MDL JSON (camelCase)
    └── connection.json    # Connection info JSON (camelCase)
```

:::caution Security note
`connection.yml` and `target/connection.json` may contain credentials. Add both `target/` and `connection.yml` to `.gitignore` before committing.
:::

---

## Project Files

### `wren_project.yml`

The root metadata file describing the project:

```yaml
name: my_project
version: "1.0"
catalog: wren
schema: public
data_source: POSTGRES
connection_mode: security   # "security" (default) | "inline"
```

| Field | Description |
|-------|-------------|
| `name` | Project name |
| `catalog` | MDL catalog (matches the `catalog` in your MDL manifest) |
| `schema` | MDL schema |
| `data_source` | Data source type (e.g. `POSTGRES`, `BIGQUERY`, `SNOWFLAKE`) |
| `connection_mode` | `security` — credentials are in files and never shown; `inline` — test/dev mode where credentials may be provided inline |

### `models/<model_name>.yml`

One file per model. Example for an `orders` model:

```yaml
name: orders
table_reference:
  catalog: ""
  schema: public
  table: orders
columns:
  - name: order_id
    type: INTEGER
    is_calculated: false
    not_null: true
    is_primary_key: true
    properties: {}
  - name: customer_id
    type: INTEGER
    is_calculated: false
    not_null: false
    properties: {}
  - name: total
    type: DECIMAL
    is_calculated: false
    not_null: false
    properties: {}
primary_key: order_id
cached: false
properties: {}
```

### `relationships.yml`

All relationships between models in a single file:

```yaml
relationships:
  - name: orders_customer
    models:
      - orders
      - customers
    join_type: MANY_TO_ONE
    condition: orders.customer_id = customers.customer_id
```

### `views.yml`

All views in a single file:

```yaml
views:
  - name: recent_orders
    statement: SELECT * FROM wren.public.orders WHERE order_date > '2024-01-01'
    properties: {}
```

### `connection.yml`

Connection parameters for the data source. Fields use **snake_case** and are converted to **camelCase** when compiled to `target/connection.json`.

Example for PostgreSQL:

```yaml
connector: POSTGRES
host: localhost
port: 5432
database: my_database
user: my_user
password: ""   # TODO: fill in before building
```

---

## Field Mapping

When converting between YAML (snake_case) and JSON (camelCase):

| YAML field | JSON field |
|------------|------------|
| `data_source` | `dataSource` |
| `table_reference` | `tableReference` |
| `is_calculated` | `isCalculated` |
| `not_null` | `notNull` |
| `is_primary_key` | `isPrimaryKey` |
| `primary_key` | `primaryKey` |
| `join_type` | `joinType` |

All other fields (`name`, `type`, `catalog`, `schema`, `table`, `condition`, `models`, `columns`, `cached`, `properties`) are identical in both formats.

---

## Building the Project

Building compiles the YAML project into two JSON files under `target/`:

```bash
# target/mdl.json — assembled MDL manifest
# target/connection.json — connection info
```

After building, you can use the compiled output in two ways:

- **Wren Engine API directly** — pass `mdl_file_path` and `connectionFilePath` as parameters in ibis-server REST API calls. See the [Wren Engine API reference](/oss/wren_engine_api/) for details.
- **Wren MCP Server** — mount the `target/` directory when starting the MCP server. The MCP server calls `deploy()` on startup to activate the MDL and handles subsequent API calls for you.

In both cases, `connectionFilePath` points to `target/connection.json` so ibis-server reads credentials from the file directly — they never appear in API payloads or LLM context.

---

## Typical Workflow

**1. Set up connection info**

Create `connection.yml` with host, port, database, and user. Leave sensitive fields (password, token) empty with a `# TODO` comment. Fill them in before building, then compile to `target/connection.json`.

**2. Generate MDL**

Use ibis-server metadata endpoints to introspect the database. Pass `connectionFilePath="<abs_path>/target/connection.json"` in all API calls.

**3. Save project**

Write `wren_project.yml`, `models/*.yml`, `relationships.yml`, and `views.yml` by converting the MDL JSON (camelCase) to snake_case YAML.

**4. Add to `.gitignore`**

```
target/
connection.yml
```

**5. Commit to version control**v

Commit the model files without secrets — `connection.yml` and `target/` are excluded.

**6. Build**

Read the YAML files, rename snake_case → camelCase, and write `target/mdl.json` and `target/connection.json`.

**7. Use the compiled output**

- **Option A — Wren Engine API directly**: Pass `mdl_file_path` and `connectionFilePath` as parameters when calling the ibis-server REST API. See the [Wren Engine API reference](/oss/wren_engine_api/) for details.

- **Option B — Wren MCP Server**: Start the MCP server with the project's `target/` directory mounted. The MCP server calls `deploy(mdl_file_path="./target/mdl.json")` on startup to activate the MDL, then handles API calls on your behalf. See the Wren MCP Server guide for setup details.

---

## Version Control Benefits

Storing MDL as a YAML project (rather than a single JSON blob) gives you:

- **Readable diffs** — model changes show up as clear line-level diffs in pull requests
- **One file per model** — merge conflicts are isolated to the affected model file
- **Separation of secrets** — `connection.yml` and `target/` are gitignored; everything else is safe to commit
- **Reproducible builds** — `target/mdl.json` is always regenerated from source, never committed
