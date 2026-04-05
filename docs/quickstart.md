# Quick Start: Wren CLI with jaffle_shop

Use natural-language questions against the **jaffle\_shop** dataset using **Wren Engine CLI** and **Claude Code** — no cloud database, no Docker, no MCP server.

> **Time:** ~15 minutes
>
> **What you'll get:** A local semantic layer + memory system that lets an AI agent write accurate SQL by understanding your data's meaning, not just its schema.

---

## Prerequisites

- **Claude Code** — installed and authenticated ([install guide](https://docs.anthropic.com/en/docs/claude-code/overview))
- **Python 3.11+**
- **Node.js / npm** — required if using `npx` to install skills (see Step 3)
- **Git**

---

## Step 0 — Create a Python virtual environment

Create and activate a virtual environment before installing any packages. This keeps dbt and wren-engine dependencies isolated from your system Python:

```bash
python3 -m venv ~/.venvs/wren
source ~/.venvs/wren/bin/activate
```

> **Tip:** Activate this environment (`source ~/.venvs/wren/bin/activate`) in every new terminal session before running `dbt` or `wren` commands.

---

## Step 1 — Seed the jaffle_shop dataset

Clone the dbt jaffle\_shop project and build the DuckDB database:

```bash
git clone https://github.com/dbt-labs/jaffle_shop_duckdb.git
cd jaffle_shop_duckdb
pip install dbt-core dbt-duckdb
dbt build
```

Verify the database file was created:

```bash
ls jaffle_shop.duckdb
```

Note the **absolute path** to this directory — you'll need it when setting up the profile:

```bash
pwd
# e.g. /Users/you/jaffle_shop_duckdb
```

---

## Step 2 — Install wren-engine Python package

Install `wren-engine` with the DuckDB connector, UI support, and memory system:

```bash
pip install "wren-engine[duckdb,ui,memory]"
```

> **Extras explained:**
> - `duckdb` — DuckDB connector (use `postgres`, `bigquery`, etc. for other data sources)
> - `ui` — browser-based profile configuration UI
> - `memory` — LanceDB-backed memory system for context retrieval and NL-SQL recall

Verify the installation:

```bash
wren version
```

---

## Step 3 — Install CLI skills

Skills are workflow guides that tell Claude Code how to use the Wren CLI effectively. Install both skills:

```bash
npx skills add Canner/wren-engine --skill '*' --agent claude-code
# or:
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
```

This installs two skills:

| Skill | Purpose |
|-------|---------|
| **wren-usage** | Day-to-day workflow — gather context, recall past queries, write SQL, store results |
| **wren-generate-mdl** | One-time setup — explore database schema and generate the MDL project |

---

## Step 4 — Set up a profile

A profile stores your database connection info (like dbt profiles). Create one for the jaffle\_shop DuckDB database:

### Option A: Browser UI (recommended)

```bash
wren profile add --ui
```

This opens a browser form. Fill in:

- **Profile name:** `jaffle-shop`
- **Data source:** `duckdb`
- **Database path:** `/Users/you/jaffle_shop_duckdb` — the **directory** containing `.duckdb` files, not the `.duckdb` file itself (your absolute path from Step 1)

### Option B: Interactive CLI

```bash
wren profile add --interactive
```

Follow the prompts to enter profile name, data source, and connection fields.

### Option C: From file

Create a YAML file `jaffle-profile.yml`:

```yaml
datasource: duckdb
url: /Users/you/jaffle_shop_duckdb
format: duckdb
```

Then import it:

```bash
wren profile add --from-file jaffle-profile.yml --name jaffle-shop
```

---

Verify the profile is active:

```bash
wren profile list
```

You should see `jaffle-shop` marked as active. Test the connection:

```bash
wren profile debug
```

---

## Step 5 — Initialize a Wren project

Create a new directory for your project and scaffold the project structure:

```bash
mkdir -p ~/jaffle-wren && cd ~/jaffle-wren
wren context init
```

This creates:

```
~/jaffle-wren/
├── wren_project.yml        # project metadata
├── models/                 # one folder per table
├── views/                  # reusable SQL views
├── relationships.yml       # table join definitions
└── instructions.md         # business rules for the AI
```

The generated `wren_project.yml` contains default values for `catalog` and `schema`:

> **Note:** `catalog` and `schema` in `wren_project.yml` define the **Wren Engine namespace** — they have nothing to do with your database's catalog or schema. Keep the defaults (`wren` / `public`). The actual database location of each table is specified per-model in the `table_reference` section.

---

## Step 6 — Generate MDL with Claude Code

Now let Claude Code explore the database and generate the MDL project files. Open Claude Code **in the project directory**:

```bash
cd ~/jaffle-wren
claude
```

Then ask:

```
Use the wren-generate-mdl skill to explore the jaffle_shop database
and generate the MDL for all tables. The data source is DuckDB.
```

Claude Code will:

1. **Discover tables** — `customers`, `orders`, `products`, `supplies`, etc.
2. **Introspect columns and types** — using SQLAlchemy or `information_schema`
3. **Normalize types** — via `wren utils parse-type`
4. **Write model YAML files** — one folder per table under `models/`
5. **Infer relationships** — from foreign keys and naming conventions
6. **Add descriptions** — Claude may ask you to describe key tables/columns
7. **Validate and build** — `wren context validate` → `wren context build`
8. **Index memory** — `wren memory index` (generates seed NL-SQL examples)

After completion, verify the project:

```bash
wren context show
wren memory status
```

---

## Step 7 — Start asking questions

You're ready to go. In Claude Code, just ask questions in natural language:

```
How many customers placed more than one order?
```

```
What are the top 5 products by total revenue?
```

```
Show me the monthly order count trend.
```

Behind the scenes, Claude Code uses the **wren-usage** skill to:

1. **Fetch context** (`wren memory fetch`) — find relevant tables and columns for your question
2. **Recall examples** (`wren memory recall`) — find similar past queries
3. **Write SQL** — using the semantic layer (model names, not raw table names)
4. **Execute** (`wren --sql "..."`) — run through the Wren engine
5. **Store** (`wren memory store`) — save successful NL-SQL pairs for future recall

The more you ask, the smarter the system gets — each stored query improves future recall accuracy.

---

## What's in the project

After setup, your project directory looks like this:

```
~/jaffle-wren/
├── wren_project.yml
├── models/
│   ├── customers/
│   │   └── metadata.yml        # table schema + descriptions
│   ├── orders/
│   │   └── metadata.yml
│   ├── products/
│   │   └── metadata.yml
│   └── supplies/
│       └── metadata.yml
├── views/
├── relationships.yml           # e.g. orders → customers (many_to_one)
├── instructions.md             # your business rules
├── .wren/
│   └── memory/                 # LanceDB index (auto-managed)
└── target/
    └── mdl.json                # compiled manifest
```

Key files to customize:

- **`instructions.md`** — Add business rules, naming conventions, or query guidelines. Use `##` headings to organize by topic. Example:

  ```markdown
  ## Naming Conventions
  - "revenue" always means order total, not supply cost
  - "active customers" means customers with at least one order in the last 90 days

  ## Query Rules
  - Always use order_date for time-based filtering, not created_at
  ```

- **`models/*/metadata.yml`** — Add or refine `description` fields on models and columns. Better descriptions = better memory search.

- **`relationships.yml`** — Add or fix join conditions. Wrong relationships cause silent query errors.

After editing any file, rebuild and re-index:

```bash
wren context validate
wren context build
wren memory index
```

---

## Useful commands reference

| Task | Command |
|------|---------|
| Run SQL | `wren --sql "SELECT ..." -o table` |
| Preview planned SQL | `wren dry-plan --sql "SELECT ..."` |
| Validate SQL | `wren validate --sql "SELECT ..."` |
| Show project context | `wren context show` |
| Show instructions | `wren context instructions` |
| Build manifest | `wren context build` |
| Fetch context for a question | `wren memory fetch --query "..."` |
| Recall similar queries | `wren memory recall --query "..."` |
| Store a NL-SQL pair | `wren memory store --nl "..." --sql "..."` |
| Check memory status | `wren memory status` |
| Re-index memory | `wren memory index` |
| Switch profile | `wren profile switch <name>` |
| List profiles | `wren profile list` |

---

## Next steps

- **Add views** for frequently asked questions — views with good descriptions become high-quality recall examples
- **Refine instructions** as you discover query patterns the AI gets wrong
