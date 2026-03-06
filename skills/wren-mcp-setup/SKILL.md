---
name: wren-mcp-setup
description: Set up Wren Engine MCP server via Docker and register it with an AI agent. Covers pulling the Docker image, running the container with docker run, mounting a workspace, fixing localhost → host.docker.internal for connection info, registering the MCP server in Claude Code (or other MCP clients) using streamable-http transport, and starting a new session to interact with Wren MCP. Trigger when a user wants to run Wren MCP in Docker, configure Claude Code MCP, or connect an AI client to a Dockerized Wren Engine.
compatibility: Requires Docker Desktop (or Docker Engine).
metadata:
  author: wren-engine
  version: "1.1"
---

# Set Up Wren MCP via Docker

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-mcp-setup` key with this skill's version (`1.1`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-mcp-setup** skill is available (remote: X.Y, installed: 1.1).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-mcp-setup
> ```

Then continue with the workflow below regardless of update status.

---

Runs the Wren Engine ibis-server + MCP server together in a single Docker container and connects it to Claude Code (or another MCP client) over streamable-http.

---

## Step 1 — Ask for workspace path

Ask the user:

> What directory on your host machine should be mounted as the MCP workspace?
> This is where MDL files and YAML project directories will be read and written.
> (Example: `~/wren-workspace`)

If the user has no preference, suggest creating a dedicated directory such as `~/wren-workspace`:

```bash
mkdir -p ~/wren-workspace
```

Save the answer as `<WORKSPACE_PATH>` (use the absolute path, e.g. `/Users/me/wren-workspace`) for use in the next steps.

---

## Step 2 — Prepare workspace and start the container

The workspace directory is mounted at `/workspace` inside the container. The container auto-loads the MDL and connection info at startup if you provide `MDL_PATH` and `CONNECTION_INFO_FILE` pointing to files inside the workspace.

**Recommended workspace layout:**

```
<WORKSPACE_PATH>/
└── target/
    ├── mdl.json           # Compiled MDL (from wren-project build)
    └── connection.json    # Connection info JSON
```

Run the following command, substituting `<WORKSPACE_PATH>` with the path from Step 1:

```bash
docker run -d \
  --name wren-mcp \
  -p 8000:8000 \
  -p 9000:9000 \
  -e ENABLE_MCP_SERVER=true \
  -e MCP_TRANSPORT=streamable-http \
  -e MCP_HOST=0.0.0.0 \
  -e MCP_PORT=9000 \
  -e WREN_URL=localhost:8000 \
  -e MDL_PATH=/workspace/target/mdl.json \
  -e CONNECTION_INFO_FILE=/workspace/target/connection.json \
  -v <WORKSPACE_PATH>:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

Example with a concrete path:

```bash
docker run -d \
  --name wren-mcp \
  -p 8000:8000 \
  -p 9000:9000 \
  -e ENABLE_MCP_SERVER=true \
  -e MCP_TRANSPORT=streamable-http \
  -e MCP_HOST=0.0.0.0 \
  -e MCP_PORT=9000 \
  -e WREN_URL=localhost:8000 \
  -e MDL_PATH=/workspace/target/mdl.json \
  -e CONNECTION_INFO_FILE=/workspace/target/connection.json \
  -v /Users/me/my-mdl-files:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

> If `MDL_PATH` or `CONNECTION_INFO_FILE` are not set (or the files don't exist yet), the container starts without a loaded MDL. You can deploy later using the `deploy` MCP tool.

This starts the container using the image `ghcr.io/canner/wren-engine-ibis:latest` with:

| Service | Port | Purpose |
|---------|------|---------|
| ibis-server | 8000 | REST API for query execution and metadata |
| mcp-server (streamable-http) | 9000 | MCP endpoint for AI clients |

The workspace directory is mounted at `/workspace` inside the container.

Verify the container is running:
```bash
docker ps --filter name=wren-mcp
docker logs -f wren-mcp
```

---

## Step 3 — Fix connection info: localhost → host.docker.internal

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

## Step 4 — Configure Claude Code MCP

Claude Code uses **streamable-http** transport to connect to the containerized MCP server.

Add to `~/.claude/settings.json` under `mcpServers`:

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

After adding, **restart Claude Code** for the new MCP server to be loaded into the session. Confirm with:
```bash
claude mcp list
```

> **Note:** The config file is `~/.claude/settings.json`, not `~/.claude.json`. Adding the MCP server while a session is already running has no effect until the session is restarted.

---

## Step 5 — Configure other MCP clients

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

Claude Desktop does not support HTTP transport natively. Use a local stdio proxy or run the MCP server locally instead.

---

## Step 6 — Verify the connection

Ask the AI agent to run a health check:

```
Use the health_check() tool to verify Wren Engine is reachable.
```

Expected response: `SELECT 1` returns a successful result.

If health check fails, see **Troubleshooting** below.

---

## Troubleshooting — MCP server not healthy

### 1. Check container status and logs

```bash
docker ps --filter name=wren-mcp
docker logs wren-mcp
```

Look for startup errors or crash loops. If the container exited, `logs` will show the last output before it stopped.

### 2. Port already in use

The container exposes ports **8000** (ibis-server) and **9000** (MCP). If either port is already bound by another process on the host, the `docker run` command will fail with a bind error.

**Check what is using the port:**

```bash
# macOS / Linux
lsof -i :9000
lsof -i :8000

# or with ss (Linux)
ss -tlnp | grep -E '8000|9000'
```

If another process is occupying the port you have two options:

**Option A — stop the conflicting process:**
```bash
# macOS: kill by port
kill $(lsof -ti :9000)
```

**Option B — remap to different host ports:**

Stop and remove the existing container first, then re-run with different host-side ports:
```bash
docker rm -f wren-mcp
docker run -d \
  --name wren-mcp \
  -p 18000:8000 \
  -p 19000:9000 \
  -e ENABLE_MCP_SERVER=true \
  -e MCP_TRANSPORT=streamable-http \
  -e MCP_HOST=0.0.0.0 \
  -e MCP_PORT=9000 \
  -e WREN_URL=localhost:8000 \
  -v <WORKSPACE_PATH>:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

Then update the MCP client URL to match:
```bash
claude mcp add --transport http wren http://localhost:19000/mcp
```

### 3. Container started but MCP endpoint returns an error

- Confirm the container was started with `-e ENABLE_MCP_SERVER=true`, `-e MCP_TRANSPORT=streamable-http`, and `-e MCP_HOST=0.0.0.0`
- Try curling the endpoint directly:
  ```bash
  curl -v http://localhost:9000/mcp
  ```
  A `405 Method Not Allowed` response means the endpoint is reachable but expects a POST — that is normal and indicates the MCP server is up.

### 4. Database connection refused inside the container

If `health_check()` passes but queries fail with a connection error, the database host is likely still set to `localhost`. See **Step 3** above — change it to `host.docker.internal`.

---

## Step 7 — Load an MDL and start querying

**Option A — Auto-load at startup (recommended):**

Place your compiled `mdl.json` in `<WORKSPACE_PATH>/target/mdl.json` and `connection.json` in `<WORKSPACE_PATH>/target/connection.json` before starting the container. The container reads `MDL_PATH` and `CONNECTION_INFO_FILE` at startup and logs:

```
Loaded MDL /workspace/target/mdl.json (9 models, 47 columns)
Loaded connection info /workspace/target/connection.json
```

**Option B — Deploy via MCP tool (after container is running):**

In the AI client (after MCP is connected):

```
deploy(mdl_file_path="/workspace/target/mdl.json")
```

Or deploy from an in-memory dict:

```
deploy_manifest(mdl=<manifest dict>)
```

**To generate an MDL from scratch**, use the `generate-mdl` skill. It will introspect your database schema via ibis-server and build the manifest for you.

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

---

## You're ready!

Start a **new Claude Code session** (or restart your MCP client). The Wren tools (`health_check`, `query`, `deploy`, `setup_connection`, etc.) are now available.

Try it:

```
Use health_check() to verify the connection.
```

Once confirmed, you can start querying your data through Wren Engine — use `/generate-mdl` to scaffold an MDL from your database, or `/wren-sql` for help writing queries.
