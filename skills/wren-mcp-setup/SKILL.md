---
name: wren-mcp-setup
description: Set up Wren Engine MCP server via Docker and register it with an AI agent. Covers pulling the Docker image, running the container with docker run, mounting a workspace, configuring connection info via the Web UI (with Docker host hint), registering the MCP server in Claude Code (or other MCP clients) using streamable-http transport, and starting a new session to interact with Wren MCP. Trigger when a user wants to run Wren MCP in Docker, configure Claude Code MCP, or connect an AI client to a Dockerized Wren Engine.
compatibility: Requires Docker Desktop (or Docker Engine).
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.3"
---

# Set Up Wren MCP via Docker

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-mcp-setup` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-mcp-setup** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-mcp-setup --agent claude-code
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

### Check for a newer image

Before starting the container, verify whether a newer `wren-engine-ibis` image is available on the registry.

**If the image has never been pulled** (first-time setup), just pull it:

```bash
docker pull ghcr.io/canner/wren-engine-ibis:latest
```

**If the image already exists locally**, compare the local digest with the remote one to detect updates:

```bash
# Save current local digest (empty string if image not present)
LOCAL_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/canner/wren-engine-ibis:latest 2>/dev/null || echo "")

# Pull from registry (downloads only if remote digest differs)
docker pull ghcr.io/canner/wren-engine-ibis:latest

# Compare digests
NEW_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/canner/wren-engine-ibis:latest 2>/dev/null || echo "")

if [ "$LOCAL_DIGEST" != "$NEW_DIGEST" ]; then
  echo "✓ New image pulled — container will use the updated version."
else
  echo "✓ Already up to date. No update was needed."
fi
```

> **Note:** If a `wren-mcp` container is already running and a new image was pulled, stop and remove the old container before proceeding:
> ```bash
> docker rm -f wren-mcp
> ```

The workspace directory is mounted at `/workspace` inside the container. Place your compiled MDL at `<WORKSPACE_PATH>/target/mdl.json` so the container can load it at startup via `MDL_PATH`.

**Recommended workspace layout:**

```
<WORKSPACE_PATH>/
└── target/
    └── mdl.json           # Compiled MDL (from wren-project build)
```

> **Note:** Connection info is managed via the MCP server Web UI (see Step 3) and persisted to `~/.wren/connection_info.json`. You can also pre-configure this file before starting the container.

Run the following command, substituting `<WORKSPACE_PATH>` with the path from Step 1:

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
  -v <WORKSPACE_PATH>:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

Example with a concrete path:

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
  -v /Users/me/wren-workspace:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

> If `MDL_PATH` is not set (or the file doesn't exist yet), the container starts without a loaded MDL. You can deploy later using the `deploy` MCP tool or the Web UI.

This starts the container using the image `ghcr.io/canner/wren-engine-ibis:latest` with:

| Service | Port | Purpose |
|---------|------|---------|
| ibis-server | 8000 | REST API for query execution and metadata |
| mcp-server (streamable-http) | 9000 | MCP endpoint for AI clients |
| Web UI | 9001 | Configuration UI (connection info, MDL editor, read-only mode) |

The workspace directory is mounted at `/workspace` inside the container.

Verify the container is running:
```bash
docker ps --filter name=wren-mcp
docker logs -f wren-mcp
```

---

## Step 3 — Configure connection info via Web UI

Open the Web UI at **http://localhost:9001**.

The Web UI provides:

- **Read-only Mode** — toggle to prevent the AI agent from calling `deploy`, `deploy_manifest`, `list_remote_tables`, or `list_remote_constraints`. Useful for read-only query access.
- **MDL Status** — shows the currently deployed MDL (models, columns, data source).
- **Connection Info** — select the data source type and fill in credentials. Click **Save Connection** to test and persist.
- **MDL Editor** — view and edit the live MDL JSON inline. Click **Save & Deploy** to update.

### Docker host hint

**Critical:** The container cannot reach the host's `localhost` directly. If your database runs on the host machine, use `host.docker.internal` instead of `localhost` or `127.0.0.1` as the hostname.

The Web UI shows a hint for this when it detects you may be connecting to a local database:

| Original | Inside Docker |
|----------|--------------|
| `localhost` | `host.docker.internal` |
| `127.0.0.1` | `host.docker.internal` |
| Cloud/remote host (e.g. `mydb.us-east-1.rds.amazonaws.com`) | No change needed |

Fill in the connection credentials in the Web UI form, apply the Docker host hint if needed, and click **Save Connection**.

---

## Step 4 — Configure Claude Code MCP

Claude Code uses **streamable-http** transport to connect to the containerized MCP server.

**Via Claude Code CLI (recommended):**

```bash
claude mcp add --transport http wren http://localhost:9000/mcp
```

Or add manually to `~/.claude/settings.json` under `mcpServers`:

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

The container exposes ports **8000** (ibis-server), **9000** (MCP), and **9001** (Web UI). If any port is already bound by another process on the host, the `docker run` command will fail with a bind error.

**Check what is using the port:**

```bash
# macOS / Linux
lsof -i :9000
lsof -i :8000
lsof -i :9001
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
  -p 19001:9001 \
  -e ENABLE_MCP_SERVER=true \
  -e MCP_TRANSPORT=streamable-http \
  -e MCP_HOST=0.0.0.0 \
  -e MCP_PORT=9000 \
  -e WREN_URL=localhost:8000 \
  -e WEB_UI_PORT=9001 \
  -e MDL_PATH=/workspace/target/mdl.json \
  -v <WORKSPACE_PATH>:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

Then update the MCP client URL and Web UI address to match:
```bash
claude mcp add --transport http wren http://localhost:19000/mcp
# Web UI: http://localhost:19001
```

### 3. Container started but MCP endpoint returns an error

- Confirm the container was started with `-e ENABLE_MCP_SERVER=true`, `-e MCP_TRANSPORT=streamable-http`, and `-e MCP_HOST=0.0.0.0`
- Try curling the endpoint directly:
  ```bash
  curl -v http://localhost:9000/mcp
  ```
  A `405 Method Not Allowed` response means the endpoint is reachable but expects a POST — that is normal and indicates the MCP server is up.

### 4. Database connection refused inside the container

If `health_check()` passes but queries fail with a connection error, the database host is likely still set to `localhost`. Open the Web UI at `http://localhost:9001`, edit the connection info, and change the host to `host.docker.internal`.

---

## Step 7 — Load an MDL and start querying

**Option A — Auto-load at startup (recommended):**

Place your compiled `mdl.json` in `<WORKSPACE_PATH>/target/mdl.json` before starting the container. The container reads `MDL_PATH` at startup and logs:

```
Loaded MDL /workspace/target/mdl.json (9 models, 47 columns)
```

**Option B — Deploy via Web UI:**

Open `http://localhost:9001`, paste or edit your MDL JSON in the MDL Editor, and click **Save & Deploy**.

**Option C — Deploy via MCP tool (after container is running):**

In the AI client (after MCP is connected):

```
deploy(mdl_file_path="/workspace/target/mdl.json")
```

Or deploy from an in-memory dict:

```
deploy_manifest(mdl=<manifest dict>)
```

**To generate an MDL from scratch**, use the `wren-generate-mdl` skill. It will introspect your database schema via ibis-server and build the manifest for you.

---

## You're ready!

Start a **new Claude Code session** (or restart your MCP client). The Wren tools (`health_check`, `query`, `deploy`, `setup_connection`, etc.) are now available.

Try it:

```
Use health_check() to verify the connection.
```

Once confirmed, you can start querying your data through Wren Engine — use `/wren-generate-mdl` to scaffold an MDL from your database, or `/wren-sql` for help writing queries.
