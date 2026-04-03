# Wren Project

A Wren project is a directory of YAML files that defines a semantic layer over a database. It is the unit of authoring, version control, and deployment — replacing the manual workflow of managing a single `mdl.json` file by hand.

Instead of one opaque JSON blob, each model lives in its own directory, relationships and views are human-readable YAML, and the compiled JSON is generated on demand. Project files are diffable, reviewable, and safe to commit. The compiled output is not.

YAML files use **snake_case** field names. The compiled `target/mdl.json` uses **camelCase** — the wire format expected by the Wren engine.

---

## Project Structure

```text
my_project/
├── wren_project.yml            # Project metadata
├── models/
│   ├── orders/
│   │   ├── metadata.yml        # Model definition
│   │   └── ref_sql.sql         # Optional: SQL expression for derived models
│   └── customers/
│       └── metadata.yml
├── views/
│   └── monthly_revenue/
│       ├── metadata.yml        # View name, description
│       └── sql.yml             # View SQL statement
├── relationships.yml           # All joins between models
├── instructions.md             # LLM query guidelines (optional)
├── .wren/                      # Runtime state — gitignore this
│   └── memory/                 # LanceDB semantic index
└── target/
    └── mdl.json                # Compiled output — gitignore this
```

Add to `.gitignore`:
```
target/
.wren/
```

---

## Why Projects Exist

The alternative is placing `mdl.json` in `~/.wren/` and keeping it current by hand. That workflow breaks down as soon as you have more than one project, because:

- Which project's schema is in global memory?
- When did the schema last change?
- Who changed it and why?

A project makes schema definitions version-controllable like code. `wren context build` regenerates `target/mdl.json` deterministically from source. `wren memory index` keeps the semantic index in sync with the project it belongs to.

---

## Project vs Global Scope

| Artifact | Lives in | Why |
|----------|----------|-----|
| Models, views, relationships | Project directory | Schema is project-specific |
| `target/mdl.json` | `<project>/target/` | Compiled from project source |
| `instructions.md` | Project directory | References model/column names from that project |
| Memory (LanceDB) | `<project>/.wren/memory/` | Indexes that project's schema |
| Connection profiles | `~/.wren/profiles.yml` | Credentials are environment-specific, not schema-specific |
| Global config | `~/.wren/config.yml` | Preferences like `default_project` path |

The key distinction: schema artifacts are project-specific and belong in the project. Connection credentials are environment-specific (same project connects to dev DB, prod DB via different profiles) and belong in `~/.wren/`.

---

## Project Discovery

When you run a `wren` command, the CLI resolves the project root in this order:

1. `--path` / `--project` flag (explicit)
2. `WREN_PROJECT_HOME` environment variable
3. Walk up from the current working directory looking for `wren_project.yml`
4. `default_project` in `~/.wren/config.yml`
5. None found → error: "Run `wren context init` or set `WREN_PROJECT_HOME`."

Once the project root is found, all paths are deterministic: `target/mdl.json`, `instructions.md`, `.wren/memory/`.

To work outside the project directory without `cd`:

```bash
# Option A: environment variable
export WREN_PROJECT_HOME=~/projects/sales
wren --sql "SELECT ..."

# Option B: global config
# ~/.wren/config.yml
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

Required fields: `name`, `data_source`. The `schema_version` field controls the directory layout (default: 1 for legacy flat files, 2 for folder-per-entity).

### `models/<name>/metadata.yml`

```yaml
name: orders
table_reference:
  catalog: ""
  schema: public
  table: orders
columns:
  - name: id
    type: INTEGER
    not_null: true
  - name: total
    type: DECIMAL
primary_key: id
cached: false
properties: {}
```

For derived models, put the SQL in `ref_sql.sql` alongside `metadata.yml` (takes precedence over `ref_sql` inline in YAML).

### `views/<name>/metadata.yml` + `sql.yml`

```yaml
# metadata.yml
name: monthly_revenue
description: "Revenue rolled up by month"
properties: {}
```

```yaml
# sql.yml
statement: >
  SELECT date_trunc('month', order_date) AS month,
         SUM(total) AS revenue
  FROM orders
  GROUP BY 1
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

Free-form Markdown. Consumed by LLM agents — not by the Wren engine. Common uses:

- Global constraints: "Always filter `status = 'active'`", "Use net_revenue not gross_revenue"
- Domain hints: "The `segment` column only contains: enterprise, smb, self-serve"
- Terminology: "Revenue means `net_revenue` in the finance context"

Instructions stay out of `target/mdl.json`. They are an agent-layer concern.

---

## Lifecycle

```
wren context init          → scaffold project in current directory
(edit models/, instructions.md)
wren context validate      → check structure before building
wren context build         → compile to target/mdl.json
wren memory index          → build semantic index in .wren/memory/
wren --sql "SELECT ..."    → start querying
```

After MDL changes, re-run `build` and `memory index`.

---

## Instructions and Agents

Instructions have two complementary retrieval paths:

```
wren context instructions     → all instructions, once per session
wren memory fetch -q "..."    → relevant instruction chunks + schema, per query
```

The explicit path (`wren context instructions`) catches global constraints that are too distant from any specific query embedding to surface via semantic search. The implicit path (`wren memory fetch`) catches domain hints near the relevant models.

Agents should run `wren context instructions` once at the start of a session and treat the output as rules that override defaults.
