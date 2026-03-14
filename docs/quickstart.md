# Quickstart: Chat with jaffle_shop using Wren Engine + Claude Code

This guide gets you from zero to natural-language queries against the classic [jaffle_shop](https://github.com/dbt-labs/jaffle_shop_duckdb) dataset in about 15 minutes — no cloud database required.

**What you'll end up with:**

- A local DuckDB database seeded with jaffle_shop data (customers, orders, payments, products)
- A running Wren Engine container (ibis-server + MCP server)
- An MDL manifest generated from the jaffle_shop schema
- The Wren MCP server registered in Claude Code so you can query your data in natural language

---

## Prerequisites

| Tool | Notes |
|------|-------|
| [Claude Code](https://claude.ai/code) | Installed and authenticated |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Running |
| Python 3.9+ | For the dbt virtual environment in Step 1 |

---

## Step 1 — Seed the jaffle_shop dataset

Clone the jaffle_shop DuckDB project, set up a Python virtual environment, install dbt, and run the build to generate a local `.duckdb` file:

```bash
git clone https://github.com/dbt-labs/jaffle_shop_duckdb.git
cd jaffle_shop_duckdb
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-core dbt-duckdb
dbt build
```

After `dbt build` completes, a `jaffle_shop.duckdb` file is created in the project directory. Note the absolute path — you'll need it shortly:

```bash
pwd   # e.g. /Users/you/jaffle_shop_duckdb
ls jaffle_shop.duckdb
```

The database contains:

| Table | Description |
|-------|-------------|
| `customers` | Customer records with name and lifetime stats |
| `orders` | Orders with status, dates, and amounts |
| `order_items` | Line items per order |
| `products` | Product catalog with price and type |
| `supplies` | Supply costs per product |

---

## Step 2 — Install Wren skills

Wren Engine provides Claude Code **skills** — AI agent workflows for connecting databases, generating MDL, and managing the MCP server.

```bash
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
```

**Start a new Claude Code session** after installation — skills are loaded at session start.

---

## Step 3 — Create a workspace

Create a directory to hold your MDL files. This directory is mounted into the Docker container:

```bash
mkdir -p ~/wren-workspace
```

---

## Step 4 — Set up Wren Engine

Choose either the **automated path** (recommended) or **manual path** depending on your preference.

---

### Option A — Automated with `/wren-quickstart` (recommended)

In your Claude Code session, run:

```
/wren-quickstart
```

When prompted for connection details, provide:

| Field | Value |
|-------|-------|
| Data source type | `duckdb` |
| Database folder path | `/data` (the folder containing `jaffle_shop.duckdb`) |
| Workspace path | `~/wren-workspace` |
| jaffle_shop directory | your absolute path to `jaffle_shop_duckdb/` |

The skill handles everything: pulling the Docker image, starting the container (with the DuckDB file mounted at `/data`), introspecting the schema, generating the MDL, saving the YAML project, and registering the MCP server.

When it finishes, **start a new Claude Code session** and jump to [Step 5 — Start querying](#step-5--start-querying).

---

### Option B — Manual setup

Follow these steps if you prefer full control over each phase.

#### Phase 1 — Start the Wren Engine container

Pull the latest image and start the container, mounting both your workspace and the jaffle_shop directory:

```bash
docker pull ghcr.io/canner/wren-engine-ibis:latest

JAFFLE_SHOP_DIR=/Users/you/jaffle_shop_duckdb   # ← replace with your actual path

docker run -d \
  --name wren-mcp \
  -p 8000:8000 \
  -p 9000:9000 \
  -p 9001:9001 \
  -e ENABLE_MCP_SERVER=true \
  -e MCP_TRANSPORT=streamable-http \
  -e MCP_HOST=0.0.0.0 \
  -e MCP_PORT=9000 \
  -e WREN_URL=localhost:8000 \
  -e MDL_PATH=/workspace/target/mdl.json \
  -v ~/wren-workspace:/workspace \
  -v "$JAFFLE_SHOP_DIR":/data \
  ghcr.io/canner/wren-engine-ibis:latest
```

The DuckDB file is available inside the container at `/data/jaffle_shop.duckdb`.

Verify it's running:

```bash
docker ps --filter name=wren-mcp
curl http://localhost:8000/health
```

#### Phase 2 — Generate the MDL

In Claude Code, run each skill in sequence:

```
/generate-mdl
```

Before running `/generate-mdl`, configure the connection via the Web UI at `http://localhost:9001`:
- Data source type: `DUCKDB`
- Database folder path: `/data` (the folder containing `jaffle_shop.duckdb`)

Then run:

```text
/generate-mdl
```

Then save the MDL as a versioned YAML project:

```text
/wren-project
```

This writes human-readable YAML files to `~/wren-workspace/` and compiles `target/mdl.json`.

#### Phase 3 — Register the MCP server

```bash
claude mcp add --transport http wren http://localhost:9000/mcp
```

Verify it was added:

```bash
claude mcp list
```

**Start a new Claude Code session** — MCP servers are only loaded at session start.

---

## Step 5 — Start querying

In the new session, verify the connection:

```
Use health_check() to verify Wren Engine is reachable.
```

Then ask questions in natural language:

```
How many customers placed more than one order?
```

```
What are the top 5 products by total revenue?
```

```
Show me the order completion rate by month for the last year.
```

```
Which customers have the highest average order value?
```

```
What percentage of orders were returned?
```

Wren Engine translates these questions into SQL against the jaffle_shop MDL and returns results directly in your chat.

---

## What happens under the hood

```
Your question → Claude Code
  → MCP tool call → Wren MCP server (port 9000)
  → wren-ibis-server (port 8000)
  → MDL semantic layer (models + relationships)
  → DuckDB query execution
  → Results back to Claude Code
```

The MDL manifest acts as a semantic layer — it tells Wren how your tables relate to each other (e.g. `orders` belongs to `customers` via `customer_id`), so queries like "top customers by revenue" automatically join the right tables.

---

## Troubleshooting

**`dbt build` fails — adapter not found:**
Install the duckdb adapter: `uv tool install dbt-duckdb`

**Container can't find the DuckDB file:**
Check that the `-v` flag points to the directory containing `jaffle_shop.duckdb`, and that the path inside the container (`/data/jaffle_shop.duckdb`) matches what you gave for the connection.

**`/generate-mdl` fails immediately:**
The container must be running first. Run `docker ps --filter name=wren-mcp` to confirm, then retry.

**MCP tools not available:**
Start a new Claude Code session after running `claude mcp add`. MCP servers are loaded at session start only.

**`health_check()` returns an error:**
Check container logs: `docker logs wren-mcp`. Confirm ports are listening: `curl http://localhost:8000/health`. Check connection info in the Web UI: `http://localhost:9001`.

---

## Next steps

| Task | Command |
|------|---------|
| Add or edit MDL models | `/wren-project` |
| Write custom SQL | `/wren-sql` |
| Connect a different database | `/wren-connection-info` |
| Day-to-day usage guide | `/wren-usage` |

For a deeper dive into how skills work or how to connect a cloud database, see [Getting Started with Claude Code](./getting_started_with_claude_code.md).
