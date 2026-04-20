# dbt Integration with Wren Engine

This guide walks you through importing a dbt project into Wren Engine — from connection setup to running queries against your dbt models.

## Prerequisites

- Python 3.11+
- dbt project with compiled artifacts (`manifest.json`, `catalog.json` in `target/`)
- Wren Engine CLI installed

```bash
pip install wren-engine
```

To also enable semantic memory (optional, needed for Step 5):

```bash
pip install 'wren-engine[memory]'
```

Generate dbt artifacts if you haven't yet:

```bash
cd your-dbt-project
dbt compile   # produces target/manifest.json + target/catalog.json
```

---

## Step 1 — Import your dbt connection profile

Wren reads your `~/.dbt/profiles.yml` and converts the active dbt target into a Wren connection profile.

```bash
wren profile import dbt \
  --project-dir ./your-dbt-project
```

**Options:**

| Flag | Description |
|------|-------------|
| `--project-dir` | dbt project root (contains `dbt_project.yml`). Defaults to `.` |
| `--profiles-path` | Custom path to `profiles.yml` (default: `~/.dbt/profiles.yml`) |
| `--profile` | dbt profile name override |
| `--target` | dbt target name override (e.g. `dev`, `prod`) |
| `--name` | Destination Wren profile name |
| `--no-activate` | Don't set this as the active profile |

**Example — import `prod` target:**

```bash
wren profile import dbt \
  --project-dir ./jaffle_shop \
  --target prod \
  --name jaffle-prod
```

**Supported dbt adapters:**

`postgres`, `bigquery`, `snowflake`, `databricks`, `trino`, `clickhouse`, `duckdb`, `mysql`, `redshift`, `spark`, `athena`, `mssql`, `doris`

**Verify the profile was imported:**

```bash
wren profile list
```

---

## Step 2 — Import your dbt project as a Wren context

This command reads your compiled dbt artifacts and generates a Wren YAML project (models, relationships, instructions).

```bash
wren context import dbt \
  --project-dir ./your-dbt-project
```

**Options:**

| Flag | Description |
|------|-------------|
| `--project-dir` | dbt project root (required) |
| `--path` / `-p` | Output directory for the Wren project (default: current directory) |
| `--profiles-path` | Custom path to `profiles.yml` |
| `--profile` | dbt profile name override |
| `--target` | dbt target name override |
| `--dry-run` | Preview what would be generated without writing files |
| `--force` | Overwrite existing Wren project files |

**Preview first (recommended):**

```bash
wren context import dbt \
  --project-dir ./jaffle_shop \
  --dry-run
```

Output example:
```
Dry run: would write 8 files to /your/project
  5 models, 0 sources, 0 ephemeral skipped, 0 without columns skipped
  - wren_project.yml
  - models/orders/metadata.yml
  - models/customers/metadata.yml
  ...
```

**Run the import:**

```bash
wren context import dbt \
  --project-dir ./jaffle_shop \
  --path ./wren-project
```

**What gets generated:**

| File | Description |
|------|-------------|
| `wren_project.yml` | Project metadata, data source binding, and dbt project reference |
| `models/*/metadata.yml` | One file per dbt model/source — columns, types, primary keys, descriptions, and dbt test metadata |
| `relationships.yml` | Joins inferred from dbt `relationships` tests |
| `instructions.md` | Verified constraints, accepted values, and relationship summary from dbt test results |
| `AGENTS.md` | AI agent workflow guidance |
| `queries.yml` | Seed NL-SQL pairs for memory indexing |

**What gets skipped:**

- Ephemeral models (`materialization = ephemeral`)
- Models without any column entries in `catalog.json`
- Columns documented in `schema.yml` but absent from `catalog.json` (i.e. not materialized in the actual database)

**What gets enriched from dbt tests:**

The importer reads `run_results.json` alongside `manifest.json` to add semantic metadata to each column:

- `not_null` test → `not_null: true` on the column
- `not_null` + `unique` together → `is_primary_key: true`
- `accepted_values` test → `accepted_values` property (e.g. `placed,shipped,completed`)
- `relationships` test → entry in `relationships.yml` with the join condition; the FK column records `dbt_tests: relationships` in its properties

---

## Step 3 — Build the Wren project

Compile your YAML project into the `target/mdl.json` file that Wren Engine uses.

```bash
cd ./wren-project
wren context build
```

Or from any directory:

```bash
wren context build --path ./wren-project
```

Expected output:
```
Built: 5 models, 0 views → ./wren-project/target/mdl.json
```

**Validate before building (optional):**

```bash
wren context validate --path ./wren-project
```

---

## Step 4 — Run a query

Query your dbt models using SQL:

```bash
wren --sql "SELECT * FROM orders LIMIT 5"
```

Wren translates your SQL through the semantic layer and executes it against your data source using the active profile.

**Specify a profile explicitly:**

```bash
wren --profile jaffle-prod --sql "SELECT customer_id, COUNT(*) FROM orders GROUP BY 1"
```

**Dry-plan (see the translated SQL without executing):**

```bash
wren dry-plan --sql "SELECT * FROM orders LIMIT 5"
```

---

## Step 5 — Index memory (optional)

If you installed `wren-engine[memory]`, seed the semantic memory with schema context and NL-SQL examples generated from your dbt project.

```bash
wren memory index --path ./wren-project
```

This indexes:
- Schema descriptions enriched with dbt model/column descriptions and accepted values
- dbt-derived NL-SQL seed pairs from `queries.yml` (generated by `wren context import dbt`)

**Check memory status:**

```bash
wren memory fetch --path ./wren-project
```

---

## Complete example — jaffle_shop_duckdb

```bash
# 1. Clone and compile the sample project
git clone https://github.com/dbt-labs/jaffle-shop.git
cd jaffle-shop
dbt deps && dbt build && dbt docs generate

# 2. Import the dbt connection as a Wren profile
wren profile import dbt --project-dir .

# 3. Import the dbt project as a Wren context
wren context import dbt --project-dir . --path ../wren-jaffle

# 4. Build the project
wren context build --path ../wren-jaffle

# 5. Query your models
wren --sql "SELECT * FROM orders LIMIT 5"
```

> **Note:** Run `dbt docs generate` (not just `dbt compile`) to produce `catalog.json`. Without it, column types and the column list cannot be populated.

---

## Troubleshooting

**`dbt project file not found`**
Make sure `--project-dir` points to the directory containing `dbt_project.yml`.

**`manifest.json not found`**
Run `dbt compile` (or `dbt run`) inside your dbt project first to generate artifacts in `target/`.

**`Environment variable 'X' is required but not set`**
Your `profiles.yml` uses `env_var()` references. Export the required variables before running the import:
```bash
export DBT_PASSWORD=your_password
wren profile import dbt --project-dir .
```

**`Unsupported dbt adapter 'X'`**
The adapter is not yet mapped. Check the supported list in Step 1 above. You can manually add a profile with:
```bash
wren profile add my-profile --datasource postgres
```

**Models skipped without columns**
Run `dbt docs generate` to populate `catalog.json`, which provides column type information and the authoritative column list.

```bash
dbt docs generate
wren context import dbt --project-dir . --force
```

**Column from `schema.yml` is missing from the generated model**
Only columns present in `catalog.json` are imported. If a column is documented in your dbt `schema.yml` but was never materialized (e.g. a docs-only stub), it will be skipped. Materialize the column in dbt first, then re-run `dbt docs generate` and re-import.
