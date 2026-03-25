# Getting Started with Claude Code

This guide walks you through setting up Wren Engine end-to-end using **Claude Code** — from connecting your database to running your first natural-language query. Claude Code's AI agent skills automate the tedious parts: schema introspection, MDL generation, project scaffolding, and Docker setup.

**What you'll end up with:**

- A running Wren Engine container (wren-ibis-server + MCP server)
- An MDL manifest generated from your real database schema
- The Wren MCP server registered in Claude Code so you can query your data in natural language

---

## Prerequisites

- [Claude Code](https://claude.ai/code) installed and authenticated
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine) running
- A supported database (PostgreSQL, MySQL, BigQuery, Snowflake, ClickHouse, DuckDB, and more)

---

## Step 1 — Install Wren skills

Wren Engine provides Claude Code **skills** — reusable AI agent workflows for connecting databases, generating MDL, and managing the MCP server.

Install all Wren skills with one command:

```bash
npx skills add Canner/wren-engine --skill '*' --agent claude-code
```

Or use the install script directly:

```bash
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
```

This installs the following skills into `~/.claude/skills/`:

| Skill | Purpose |
|-------|---------|
| `wren-quickstart` | End-to-end guided setup |
| `wren-connection-info` | Connection field reference per data source |
| `wren-generate-mdl` | Generate MDL from a live database |
| `wren-project` | Save and build MDL as YAML files |
| `wren-mcp-setup` | Start the Docker container and register MCP |
| `wren-usage` | Day-to-day usage guide |
| `wren-sql` | Write and debug SQL queries |

After installation, **start a new Claude Code session** so the skills are loaded.

---

## Step 2 — Run the quickstart

In Claude Code, run:

```
/wren-quickstart
```

This skill guides you through the full setup in four phases. You can also follow the phases manually below.

---

## Manual setup

If you prefer to run each step yourself, follow these phases in order.

### Phase 1 — Create a workspace

Create a directory on your host machine. This directory will be mounted into the Docker container so it can read and write MDL files.

```bash
mkdir -p ${PWD}/wren-workspace
```

The completed workspace will look like:

```
${PWD}/wren-workspace/
├── wren_project.yml
├── models/
│   └── *.yml
├── relationships.yml
├── views.yml
└── target/
    └── mdl.json          # Compiled MDL — loaded by the container
```

> **Connection info** is configured via the MCP server Web UI (`http://localhost:9001`) — it is not stored in the workspace.

### Phase 2 — Start the Docker container

#### Check for a newer image

Before starting the container, pull the latest image to make sure you have the most recent version.

**First time (image not yet pulled):**

```bash
docker pull ghcr.io/canner/wren-engine-ibis:latest
```

**Already have the image locally?** Compare digests to detect updates:

```bash
# Save current local digest
LOCAL_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/canner/wren-engine-ibis:latest 2>/dev/null || echo "")

# Pull from registry (downloads only if remote digest differs)
docker pull ghcr.io/canner/wren-engine-ibis:latest

# Compare digests
NEW_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/canner/wren-engine-ibis:latest 2>/dev/null || echo "")

if [ "$LOCAL_DIGEST" != "$NEW_DIGEST" ]; then
  echo "New image pulled — container will use the updated version."
else
  echo "Already up to date."
fi
```

> If a `wren-mcp` container is already running and a new image was pulled, stop and remove it first:
> ```bash
> docker rm -f wren-mcp
> ```

#### Run the container

Start the Wren Engine container, mounting your workspace:

```bash
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
  ghcr.io/canner/wren-engine-ibis:latest
```

Three services start inside the container:

| Service | Port | Purpose |
|---------|------|---------|
| wren-ibis-server | 8000 | REST API for query execution and metadata |
| MCP server | 9000 | MCP endpoint for AI clients |
| Web UI | 9001 | Configuration UI (connection info, MDL editor, read-only mode) |

Verify it is running:

```bash
docker ps --filter name=wren-mcp
docker logs wren-mcp
```

> **Database on localhost?** If your database runs on your host machine, replace `localhost` / `127.0.0.1` with `host.docker.internal` in your connection settings — the container cannot reach the host's `localhost` directly.

### Phase 3 — Configure connection and register MCP server

Configure connection info in the Web UI at `http://localhost:9001` — select the data source type and enter credentials. Use `/wren-connection-info` in Claude Code for field reference per data source.

> **Connection info can only be configured through the Web UI.** Do not attempt to set it programmatically.

Then register the MCP server with Claude Code:

```bash
claude mcp add --transport http wren http://localhost:9000/mcp
```

Verify it was added:

```bash
claude mcp list
```

**Start a new Claude Code session** — MCP servers are only loaded at session start.

### Phase 4 — Generate the MDL

In the new session, run:

```
/wren-generate-mdl
```

The skill will:

1. Run `health_check()` to verify the connection is working
2. Ask for your data source type (PostgreSQL, BigQuery, Snowflake, etc.) and optional schema filter
3. Call `list_remote_tables()` and `list_remote_constraints()` via MCP tools to introspect your database schema
4. Build the MDL JSON (models, columns, relationships)
5. Validate the manifest with `deploy_manifest()` + `dry_run()`

> **Prerequisite:** The MCP server must be registered and a new session started (Phase 3). The `/wren-generate-mdl` skill uses MCP tools — do not call ibis-server API directly.

Then save the MDL as a versioned YAML project:

```text
/wren-project
```

This writes human-readable YAML files to your workspace and compiles `target/mdl.json`.

---

## Verify and start querying

In the new session, run a health check:

```
Use health_check() to verify Wren Engine is reachable.
```

Expected: `SELECT 1` returns successfully.

Then start querying your data in natural language:

```
How many orders were placed last month?
```

```
Show me the top 10 customers by revenue.
```

---

## Day-to-day usage

Once set up, use `/wren-usage` in Claude Code for ongoing tasks:

| Task | Skill |
|------|-------|
| Write or debug SQL | `/wren-sql` |
| Look up connection field reference | `/wren-connection-info` |
| Reconfigure connection via Web UI | `http://localhost:9001` |
| Add a model or column to the MDL | `/wren-project` |
| Regenerate MDL after schema changes | `/wren-generate-mdl` |
| Restart or reconfigure the MCP server | `/wren-mcp-setup` |

### MCP server quick reference

```bash
docker ps --filter name=wren-mcp   # check status
docker logs wren-mcp               # view logs
docker restart wren-mcp            # restart
```

### MCP tool reference

| Tool | Purpose |
|------|---------|
| `health_check()` | Verify Wren Engine is reachable |
| `query(sql=...)` | Execute SQL against the deployed MDL |
| `deploy(mdl_file_path=...)` | Load a compiled `mdl.json` |
| `list_remote_tables(...)` | Introspect database schema |

> **Note:** Connection info is configured exclusively through the Web UI at `http://localhost:9001` — there is no MCP tool for setting credentials.

---

## Troubleshooting

**MCP tools not available after registration:**
Start a new Claude Code session. MCP servers are only loaded at session start.

**Container not finding the MDL at startup:**
Confirm `~/wren-workspace/target/mdl.json` exists before starting the container. Check logs with `docker logs wren-mcp`.

**Database connection refused inside Docker:**
Change `localhost` / `127.0.0.1` to `host.docker.internal` in your connection credentials.

**MCP tools fail with "Session not found" after container restart:**
Start a new Claude Code session. Container restarts invalidate MCP sessions — the client must reconnect.

**`wren-generate-mdl` fails because wren-ibis-server is not running:**
Start the container first (Phase 2), then run `/wren-generate-mdl`. wren-ibis-server is available at `http://localhost:8000` once the container is up.

**Skill not found after installation:**
Start a new Claude Code session after installing skills — they are loaded at session start.

For more detailed troubleshooting, invoke `/wren-mcp-setup` in Claude Code.

---

## Updating skills

Each skill checks for updates automatically and notifies you when a newer version is available. To force-update all skills:

```bash
npx skills add Canner/wren-engine --skill '*' --agent claude-code
# or:
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force
```

To update a single skill:

```bash
npx skills add Canner/wren-engine --skill wren-generate-mdl --agent claude-code
# or:
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-generate-mdl
```

---

## Locking down with read-only mode

Once you have confirmed that queries are returning correct results and the MDL is working as expected, enable **read-only mode** in the Web UI:

1. Open `http://localhost:9001`
2. Toggle **Read-Only Mode** to on

When read-only mode is enabled:

- The AI agent can **query data** and **read metadata** through the deployed MDL as usual
- The AI agent **cannot** modify connection info, change the data source, or call `list_remote_tables()` / `list_remote_constraints()` to introspect the database directly
- This limits the agent to operating within the boundaries of the MDL you have defined, preventing it from accessing tables or schemas you have not explicitly modeled

We recommend enabling read-only mode for day-to-day use. Turn it off temporarily when you need to regenerate the MDL or change connection settings.
