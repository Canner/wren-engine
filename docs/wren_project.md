# Wren Project

This guide explains the Wren MDL project structure, file formats, and typical workflow for managing your data models as a collection of YAML files. The Wren Project format makes it easy to version control your models, track changes, and separate connection information from model definitions.

A Wren MDL project is a directory of YAML files that makes MDL manifests human-readable and version-control friendly — similar to how dbt organizes models. Instead of managing a single large JSON file, each model lives in its own YAML file, and the project is compiled to a deployable `mdl.json` when needed.

YAML files use **snake_case** field names for readability. The compiled `target/mdl.json` uses **camelCase**, which is the wire format expected by ibis-server.

## Project Structure

```text
my_project/
├── wren_project.yml       # Project metadata (catalog, schema, data_source)
├── models/
│   ├── orders.yml         # One file per model
│   ├── customers.yml
│   └── ...
├── relationships.yml      # All relationships
└── views.yml              # All views
```

After building, the compiled file is written to:

```text
my_project/
└── target/
    └── mdl.json           # Deployable MDL JSON (camelCase)
```

> **Connection info** is managed via the MCP server Web UI (`http://localhost:9001`) — it is not stored in the project directory.

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
```

| Field | Description |
|-------|-------------|
| `name` | Project name |
| `catalog` | MDL catalog (matches the `catalog` in your MDL manifest) |
| `schema` | MDL schema |
| `data_source` | Data source type (e.g. `POSTGRES`, `BIGQUERY`, `SNOWFLAKE`) |

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

Building compiles the YAML project into `target/mdl.json`:

```bash
# target/mdl.json — assembled MDL manifest (camelCase)
```

After building, deploy the MDL via the MCP server:

```text
deploy(mdl_file_path="/workspace/target/mdl.json")
```

Or place it in the workspace before starting the container so it is auto-loaded via `MDL_PATH`.

Connection info is configured separately via the Web UI (`http://localhost:9001`) — it is not part of the build output.

---

## Typical Workflow

**1. Configure connection info**

Open the Web UI at `http://localhost:9001`, select the data source type, and fill in connection credentials. Use `/wren-connection-info` in Claude Code for per-connector field reference.

**2. Generate MDL**

Run `/wren-generate-mdl` in Claude Code. The skill uses MCP tools (`list_remote_tables`, `list_remote_constraints`) to introspect the database and build the MDL JSON.

**3. Save project**

Write `wren_project.yml`, `models/*.yml`, `relationships.yml`, and `views.yml` by converting the MDL JSON (camelCase) to snake_case YAML.

**4. Add `target/` to `.gitignore`**

```text
target/
```

**5. Commit to version control**

Commit the model YAML files — `target/` is excluded.

**6. Build**

Read the YAML files, rename snake_case → camelCase, and write `target/mdl.json`.

**7. Deploy**

```text
deploy(mdl_file_path="/workspace/target/mdl.json")
```

---

## Version Control Benefits

Storing MDL as a YAML project (rather than a single JSON blob) gives you:

- **Readable diffs** — model changes show up as clear line-level diffs in pull requests
- **One file per model** — merge conflicts are isolated to the affected model file
- **Separation of secrets** — connection info lives in the Web UI, not in the project; `target/` is gitignored
- **Reproducible builds** — `target/mdl.json` is always regenerated from source, never committed
