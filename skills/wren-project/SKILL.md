---
name: wren-project
description: Save, load, and build Wren MDL manifests as YAML project directories for version control. Use when a user wants to persist an MDL as human-readable YAML files, load a YAML project back into MDL JSON, or compile a YAML project to a deployable mdl.json file. Also manages connection info stored in connection.yml and compiled to target/connection.json.
metadata:
  author: wren-engine
  version: "1.1"
---

# MDL Project

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-project` key with this skill's version (`1.1`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-project** skill is available (remote: X.Y, installed: 1.1).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-project
> ```

Then continue with the workflow below regardless of update status.

---

A Wren MDL project is a directory of YAML files — one file per model — that makes MDL manifests human-readable and version-control friendly (similar to dbt projects).

YAML files use **snake_case** field names for readability. The compiled `target/mdl.json` uses **camelCase** (the wire format expected by ibis-server). The conversion is documented in [Field mapping](#field-mapping).

## Project structure

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
```
my_project/
└── target/
    ├── mdl.json           # Deployable MDL JSON (camelCase)
    └── connection.json    # Connection info JSON (camelCase)
```

> **Security note**: `connection.yml` may contain credentials. Add `target/` and `connection.yml` to `.gitignore` or use environment variable substitution (see `connection.yml` below) before committing.

---

## Save MDL JSON → YAML project

Given an MDL JSON dict (camelCase), write it as a YAML project directory (snake_case):

### `wren_project.yml`

```yaml
name: my_project
version: "1.0"
catalog: wren
schema: public
data_source: POSTGRES
```

### `models/<model_name>.yml`

One file per model. Example for `orders`:

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

```yaml
views: []
```

### `connection.yml`

Connection parameters for the data source. Field names use **snake_case** in YAML and are converted to **camelCase** in `target/connection.json`.

**PostgreSQL / MySQL / MSSQL / ClickHouse / Trino / Oracle:**
```yaml
host: localhost
port: 5432
user: my_user
password: my_password
database: my_db
```

**BigQuery:**
```yaml
project_id: my-gcp-project
dataset_id: my_dataset
credentials_json_string: '{"type":"service_account","project_id":"..."}'
```

**Snowflake:**
```yaml
user: my_user
password: my_password
account: my_account
database: my_db
sf_schema: public
```

**DuckDB (local file):**
```yaml
url: /path/to/my.duckdb
```

**Environment variable substitution** — to avoid committing secrets, reference env vars with `${VAR_NAME}`:
```yaml
host: ${DB_HOST}
port: ${DB_PORT}
user: ${DB_USER}
password: ${DB_PASSWORD}
database: ${DB_NAME}
```
When building, resolve each `${VAR_NAME}` value from the environment before writing `target/connection.json`.

---

## Load YAML project → MDL JSON

To assemble a YAML project back into an MDL JSON dict:

1. Read `wren_project.yml` → extract `catalog`, `schema`, `data_source`
2. Read every file in `models/*.yml` → collect into `models` list
3. Read `relationships.yml` → extract `relationships` list
4. Read `views.yml` → extract `views` list
5. **Rename snake_case keys to camelCase** (see Field mapping section below)
6. Assemble:

```json
{
  "catalog": "<from wren_project.yml>",
  "schema": "<from wren_project.yml>",
  "dataSource": "<from wren_project.yml data_source>",
  "models": [...],
  "relationships": [...],
  "views": []
}
```

To load connection info:

1. Read `connection.yml`
2. Resolve any `${VAR_NAME}` placeholders from environment variables
3. **Rename snake_case keys to camelCase** (see Field mapping section below)
4. Result is a flat JSON object ready to pass as `connectionInfo` to ibis-server APIs

---

## Build YAML project → `target/`

Same as **Load** above, but write both compiled files:

- `<project_dir>/target/mdl.json` — assembled MDL JSON (camelCase)
- `<project_dir>/target/connection.json` — connection info JSON (camelCase, env vars resolved)

After building:
- Pass `mdl_file_path="<project_dir>/target/mdl.json"` to `deploy()` to activate the MDL
- Pass the contents of `target/connection.json` as the `connectionInfo` field in API requests

---

## Field mapping

When converting between YAML (snake_case) and JSON (camelCase):

**MDL fields:**

| YAML field (snake_case) | JSON field (camelCase) |
|-------------------------|------------------------|
| `data_source` | `dataSource` |
| `table_reference` | `tableReference` |
| `is_calculated` | `isCalculated` |
| `not_null` | `notNull` |
| `is_primary_key` | `isPrimaryKey` |
| `primary_key` | `primaryKey` |
| `join_type` | `joinType` |

All other MDL fields (`name`, `type`, `catalog`, `schema`, `table`, `condition`, `models`, `columns`, `cached`, `properties`) are the same in both formats.

**Connection fields:**

| YAML field (snake_case) | JSON field (camelCase) |
|-------------------------|------------------------|
| `project_id` | `projectId` |
| `dataset_id` | `datasetId` |
| `credentials_json_string` | `credentialsJsonString` |
| `sf_schema` | `sfSchema` |

All other connection fields (`host`, `port`, `user`, `password`, `database`, `account`, `url`) are the same in both formats.

---

## Typical workflow

```
1. Have MDL JSON dict (from generate-mdl skill or manual construction)
2. Save:  write wren_project.yml + connection.yml + models/*.yml + relationships.yml + views.yml
          (convert camelCase → snake_case)
3. Add target/ and optionally connection.yml to .gitignore
4. Commit project directory to version control
5. Later — Load: read all YAML files, resolve ${ENV_VAR} placeholders in connection.yml,
                  rename snake_case → camelCase, assemble MDL JSON dict + connection info dict
6. Build:  write assembled JSON to target/mdl.json and target/connection.json
7. Deploy: deploy(mdl_file_path="./target/mdl.json")
           use target/connection.json as connectionInfo in API requests
```
