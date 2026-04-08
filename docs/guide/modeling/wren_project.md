# Wren Project

A Wren project is a directory of YAML files that define a semantic layer (models, relationships, views, and instructions) over a database. It is the unit of authoring, version control, and deployment for MDL (Model Definition Language) definitions.

Instead of managing a single `mdl.json` by hand, you author each model in its own directory as human-readable YAML. The CLI compiles them into a deployable JSON manifest when needed.

YAML files use **snake_case** field names for readability. The compiled `target/mdl.json` uses **camelCase**, which is the wire format expected by the engine.

## Project Structure

```text
my_project/
├── wren_project.yml               # project metadata
├── models/
│   ├── orders/
│   │   └── metadata.yml           # table_reference mode (physical table)
│   ├── customers/
│   │   └── metadata.yml
│   └── revenue_summary/
│       ├── metadata.yml           # ref_sql mode (SQL-defined model)
│       └── ref_sql.sql            # SQL in separate file (optional)
├── views/
│   ├── monthly_revenue/
│   │   ├── metadata.yml
│   │   └── sql.yml                # statement in separate file (optional)
│   └── top_customers/
│       └── metadata.yml           # statement inline
├── relationships.yml              # all relationships
├── instructions.md                # user instructions for LLM (optional)
├── .wren/                         # runtime state (gitignored)
│   └── memory/                    # LanceDB index files
└── target/
    └── mdl.json                   # build output (gitignored)
```

Each model and view lives in its own subdirectory under `models/` and `views/` respectively.

---

## What Lives Where

A Wren project keeps schema artifacts together in the project directory. Global configuration lives separately in `~/.wren/`.

| Artifact | Location | Scope |
|----------|----------|-------|
| Models, views, relationships | `<project>/models/`, `<project>/views/`, `<project>/relationships.yml` | Project — version controlled |
| Instructions | `<project>/instructions.md` | Project — references this project's model/column names |
| Compiled MDL | `<project>/target/mdl.json` | Project — derived from YAML, gitignored |
| Memory (LanceDB) | `<project>/.wren/memory/` | Project — indexes this project's schema, gitignored |
| Profiles (connections) | `~/.wren/profiles.yml` | Global — environment-specific (dev/prod credentials) |
| Global config | `~/.wren/config.yml` | Global — CLI preferences |

**Why this separation?** Schema definitions are project-specific — they describe a particular data model. Connection credentials are environment-specific — the same project connects to different databases in dev vs. prod. Keeping them separate means projects are portable and safe to commit without leaking secrets.

---

## Project Discovery

When you run a wren command that needs the project (query, memory fetch, etc.), the CLI resolves the project root in this order:

1. `--path` flag (explicit)
2. `WREN_PROJECT_HOME` environment variable
3. Walk up from the current directory looking for `wren_project.yml`
4. `default_project` in `~/.wren/config.yml`

If no project is found, the CLI exits with an error and suggests running `wren context init` or setting `WREN_PROJECT_HOME`.

Once the project root is resolved, all paths (MDL, instructions, memory) are determined relative to it.

For running wren commands outside the project directory:

```bash
# option A: environment variable
export WREN_PROJECT_HOME=~/projects/sales
wren --sql "SELECT ..."

# option B: global config (~/.wren/config.yml)
default_project: ~/projects/sales
```

---

## Project Files

### `wren_project.yml`

```yaml
schema_version: 2
name: my_project
version: "1.0"
catalog: wren
schema: public
data_source: postgres
```

| Field | Description |
|-------|-------------|
| `schema_version` | Directory layout version. `2` = folder-per-entity (current). Owned by the CLI — do not bump manually. |
| `name` | Project name |
| `version` | User's own project version (free-form, no effect on parsing) |
| `catalog` | **Wren Engine namespace** — NOT your database catalog. Identifies this MDL project within the engine. Default: `wren`. |
| `schema` | **Wren Engine namespace** — NOT your database schema. Default: `public`. |
| `data_source` | Data source type (e.g. `postgres`, `bigquery`, `snowflake`) |

> **`catalog` / `schema` are NOT database settings.**
>
> These two fields define the Wren Engine's internal namespace for addressing models in SQL. They exist to support future multi-project querying. For single-project use, keep the defaults (`catalog: wren`, `schema: public`).
>
> Your database's actual catalog and schema are specified per-model in the `table_reference` section of each model's `metadata.yml`.

#### Two levels of catalog/schema

The same field names appear in two places with completely different meanings:

| Location | Refers to | Example | When to change |
|----------|-----------|---------|----------------|
| `wren_project.yml` → `catalog`, `schema` | Wren Engine namespace | `wren`, `public` | Only for multi-project setups |
| `models/*/metadata.yml` → `table_reference.catalog`, `table_reference.schema` | Database location | `""`, `main` | Must match your actual database |

### Model (`models/<name>/metadata.yml`)

A model must define its source in exactly one of two ways:

**table_reference** — maps to a physical table:

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
  - name: total
    type: DECIMAL
    is_calculated: false
    not_null: false
    properties: {}
primary_key: order_id
cached: false
properties: {}
```

**ref_sql** — defines the model via a SQL query. SQL can be inline in `metadata.yml` or in a separate `ref_sql.sql` file (the `.sql` file takes precedence if both exist):

```yaml
name: revenue_summary
columns:
  - name: month
    type: DATE
    is_calculated: false
    not_null: true
    properties: {}
  - name: total_revenue
    type: DECIMAL
    is_calculated: false
    not_null: false
    properties: {}
```

```sql
-- models/revenue_summary/ref_sql.sql
SELECT DATE_TRUNC('month', order_date) AS month,
       SUM(total) AS total_revenue
FROM orders
GROUP BY 1
```

Using both `table_reference` and `ref_sql` in the same model is a validation error.

### View (`views/<name>/metadata.yml`)

Views have a `statement` field. Like ref_sql models, the SQL can be inline in `metadata.yml` or in a separate `sql.yml` file (the `sql.yml` takes precedence if both exist):

```yaml
name: top_customers
statement: >
  SELECT customer_id, SUM(total) AS lifetime_value
  FROM wren.public.orders GROUP BY 1 ORDER BY 2 DESC LIMIT 100
properties:
  description: "Top customers by lifetime value"
```

### `relationships.yml`

```yaml
relationships:
  - name: orders_customers
    models:
      - orders
      - customers
    join_type: MANY_TO_ONE
    condition: orders.customer_id = customers.customer_id
```

### `instructions.md`

Free-form Markdown with rules and guidelines for LLM-based query generation. Organize by topic with `##` headings:

```markdown
## Business rules
- Revenue queries must use net_revenue, not gross_revenue
- All queries must filter status = 'completed'

## Formatting
- Currency is TWD, display with thousand separators
- Timestamps are UTC+8
```

Instructions are consumed by agents, not by the engine. They are intentionally excluded from `target/mdl.json` — the wren-core rewrite pipeline has no use for them. Agents access instructions through two paths:

- `wren context instructions` — returns full text, run once at session start to capture global constraints
- `wren memory fetch -q "..."` — returns relevant instruction chunks alongside schema context per query

---

## Lifecycle

```text
wren context init              → scaffold project in current directory
  (edit models/, relationships.yml, instructions.md)
wren context validate          → check YAML structure (no DB needed)
wren context build             → compile to target/mdl.json
wren profile add my-pg ...     → save connection to ~/.wren/profiles.yml
wren memory index              → index schema + instructions into .wren/memory/
wren --sql "SELECT 1"          → verify connection
wren --sql "SELECT ..."        → start querying
```

After editing models, rebuild and re-index:

```text
wren context build
wren memory index
```

---

## Migrating from MDL JSON

If you already have an `mdl.json` (from the MCP server, an earlier Wren setup, or an AI agent that generated one), use `--from-mdl` to convert it into a v2 YAML project in one step:

```bash
wren context init --from-mdl /path/to/mdl.json --path my_project
```

This reads the camelCase JSON, converts all fields to snake_case YAML, and writes out the full project structure:

```text
my_project/
├── wren_project.yml          # catalog, schema, data_source from the manifest
├── models/
│   ├── orders/
│   │   └── metadata.yml      # one directory per model
│   └── customers/
│       └── metadata.yml
├── views/
│   └── top_customers/
│       └── metadata.yml      # one directory per view
├── relationships.yml
└── instructions.md
```

After import, validate and build:

```bash
wren context validate --path my_project
wren context build --path my_project
```

If the target directory already contains project files, add `--force` to overwrite:

```bash
wren context init --from-mdl mdl.json --path my_project --force
```

> **When to use this:** You have an existing `mdl.json` that was authored by hand or generated by an older workflow (e.g. the MCP server's `mdl_save_project` tool), and you want to adopt the YAML project format for version control and CLI-driven workflows.

---

## Field Mapping

The `build` step converts all YAML keys from snake_case to camelCase:

| YAML | JSON |
|------|------|
| `table_reference` | `tableReference` |
| `ref_sql` | `refSql` |
| `is_calculated` | `isCalculated` |
| `not_null` | `notNull` |
| `is_primary_key` | `isPrimaryKey` |
| `primary_key` | `primaryKey` |
| `join_type` | `joinType` |
| `data_source` | `dataSource` |

Generic rule: split on `_`, capitalize each word after the first, join. All other fields (`name`, `type`, `catalog`, `schema`, `table`, `condition`, `models`, `columns`, `cached`, `properties`) are identical in both formats.

---

## .gitignore

```text
target/
.wren/
```

Source YAML and `instructions.md` are committed. Build output (`target/`) is always gitignored — it is derived from source YAML and can be regenerated with `wren context build`.

`.wren/memory/` contains both schema indexes (derived, rebuildable) and query history (NL-SQL pairs confirmed by users, not rebuildable). If your team wants to share confirmed query history as few-shot examples across members, you can commit `.wren/memory/` — but be aware that LanceDB files are binary and may produce merge conflicts when multiple people index or store concurrently.
