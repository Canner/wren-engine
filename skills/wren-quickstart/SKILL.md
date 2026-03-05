---
name: wren-quickstart
description: Set up Wren Engine MCP server via Docker. Covers pulling the Docker image, configuring the compose file, mounting a workspace, fixing localhost → host.docker.internal for connection info, and registering the MCP server in Claude Code (or other MCP clients) using streamable-http transport. Trigger when a user wants to run Wren MCP in Docker, configure Claude Code MCP, or connect an AI client to a Dockerized Wren Engine.
compatibility: Requires Docker Desktop (or Docker Engine) with BuildKit enabled.
metadata:
  author: wren-engine
  version: "1.0"
---

# Set Up Wren MCP via Docker

Runs the Wren Engine ibis-server + MCP server together in a single Docker container and connects it to Claude Code (or another MCP client) over streamable-http.

---

## Step 1 — Ask for workspace path

Ask the user:

> What directory on your host machine should be mounted as the MCP workspace?
> This is where MDL files and YAML project directories will be read and written.
> (Default: `mcp-server/workspace` inside the wren-engine repo)

If the user has no preference, use `../workspace` relative to the `docker/` directory (i.e. `mcp-server/workspace`).

Save the answer as `<WORKSPACE_PATH>` for use in the next steps.

---

## Step 2 — Create the compose env file

Create `mcp-server/docker/.env` with:

```
MDL_WORKSPACE=<WORKSPACE_PATH>
```

Example:
```
MDL_WORKSPACE=/Users/me/my-mdl-files
```

If the user accepted the default, no `.env` file is needed — the compose default (`../workspace`) already points to `mcp-server/workspace/`.

---

## Step 3 — Start the container

```bash
cd mcp-server/docker
docker compose up -d
```

This starts the container using the image `ghcr.io/canner/wren-engine-ibis:mcp-2` with:

| Service | Port | Purpose |
|---------|------|---------|
| ibis-server | 8000 | REST API for query execution and metadata |
| mcp-server (streamable-http) | 9000 | MCP endpoint for AI clients |

The workspace directory is mounted at `/workspace` inside the container.

Verify the container is running:
```bash
docker compose ps
docker compose logs -f
```

---

## Step 4 — Fix connection info: localhost → host.docker.internal

**Critical:** The container cannot reach the host's `localhost` directly.

If the user's database connection info references `localhost` or `127.0.0.1` as the host, it must be changed to `host.docker.internal` so the container can reach a database running on the host machine.

**Examples:**

| Original | Inside Docker |
|----------|--------------|
| `"host": "localhost"` | `"host": "host.docker.internal"` |
| `"host": "127.0.0.1"` | `"host": "host.docker.internal"` |
| Cloud/remote host (e.g. `mydb.us-east-1.rds.amazonaws.com`) | No change needed |

When the user provides connection credentials later (via `setup_connection`), check the `host` field and warn if it is `localhost` or `127.0.0.1`.

---

## Step 5 — Configure Claude Code MCP

Claude Code uses **streamable-http** transport to connect to the containerized MCP server.

Add to `~/.claude.json` under `mcpServers` (or run `claude mcp add`):

```json
{
  "mcpServers": {
    "wren": {
      "type": "http",
      "url": "http://localhost:9000/mcp"
    }
  }
}
```

**Via Claude Code CLI (recommended):**

```bash
claude mcp add --transport http wren http://localhost:9000/mcp
```

After adding, restart Claude Code or reload the MCP server list. Confirm with:
```bash
claude mcp list
```

---

## Step 6 — Configure other MCP clients

### Cline / Cursor / VS Code MCP Extension

These clients also support HTTP transport. Add to their MCP settings:

```json
{
  "mcpServers": {
    "wren": {
      "type": "streamable-http",
      "url": "http://localhost:9000/mcp"
    }
  }
}
```

### Claude Desktop (stdio fallback)

Claude Desktop does not support HTTP transport natively. Use a local stdio proxy or run the MCP server locally instead. See the `wren-mcp-usage` skill for the local (non-Docker) stdio setup.

---

## Step 7 — Verify the connection

Ask the AI agent to run a health check:

```
Use the health_check() tool to verify Wren Engine is reachable.
```

Expected response: `SELECT 1` returns a successful result.

If health check fails:
- Check that the container is running: `docker compose ps`
- Check logs: `docker compose logs ibis-server`
- Confirm port 9000 is not blocked by a firewall or another process

---

## Step 8 — Load an MDL and start querying

Place your MDL JSON file in the workspace directory, then in the AI client:

```
deploy(mdl_file_path="/workspace/my_mdl.json")
```

Or generate a new MDL from scratch using the `generate-mdl` skill.

---

## Reference: compose.yaml

The full compose file at `mcp-server/docker/compose.yaml`:

```yaml
services:
  ibis-server:
    image: ghcr.io/canner/wren-engine-ibis:latest
    ports:
      - "8000:8000"
      - "9000:9000"
    environment:
      ENABLE_MCP_SERVER: "true"
      MCP_TRANSPORT: "streamable-http"
      MCP_HOST: "0.0.0.0"
      MCP_PORT: "9000"
      WREN_URL: "localhost:8000"
    volumes:
      - ${MDL_WORKSPACE:-../workspace}:/workspace
```

---

## Reference: connection info by data source

When calling `setup_connection`, use these formats. **Replace `localhost` with `host.docker.internal`** if the database runs on your host machine.

```
POSTGRES    : {"host": "host.docker.internal", "port": "5432", "user": "...", "password": "...", "database": "..."}
MYSQL       : {"host": "host.docker.internal", "port": "3306", "user": "...", "password": "...", "database": "..."}
MSSQL       : {"host": "host.docker.internal", "port": "1433", "user": "...", "password": "...", "database": "..."}
CLICKHOUSE  : {"host": "host.docker.internal", "port": "8123", "user": "...", "password": "...", "database": "..."}
TRINO       : {"host": "host.docker.internal", "port": "8080", "user": "...", "catalog": "...", "schema": "..."}
DUCKDB      : {"path": "/workspace/<file>.duckdb"}   ← file must be inside workspace
BIGQUERY    : {"project": "...", "dataset": "...", "credentials_base64": "..."}
SNOWFLAKE   : {"account": "...", "user": "...", "password": "...", "database": "...", "schema": "..."}
```

For DuckDB: the database file must be placed in the mounted workspace directory so the container can access it at `/workspace/<file>.duckdb`.
