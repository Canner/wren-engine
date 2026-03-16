---
name: wren-quickstart
description: End-to-end quickstart for Wren Engine — create a workspace, generate an MDL from a live database, save it as a versioned project, start the Wren MCP Docker container, and verify the setup with a health check. Trigger when a user wants to set up Wren Engine from scratch, onboard a new data source, or get started with Wren MCP. Requires dependent skills already installed (use /wren-usage to install them first).
compatibility: Requires Docker Desktop (or Docker Engine). No local database drivers needed.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.2"
---

# Wren Quickstart

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-quickstart` key with this skill's version (`1.2`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-quickstart** skill is available (remote: X.Y, installed: 1.2).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-quickstart
> ```

Then continue with the workflow below regardless of update status.

---

This skill walks a user through setting up Wren Engine end-to-end — from creating a workspace to running their first query via MCP. Each phase delegates to a focused skill. Follow the steps in order.

> **Prerequisites:** The dependent skills (`generate-mdl`, `wren-project`, `wren-mcp-setup`, `wren-connection-info`) must be installed. If they are missing, use `/wren-usage` first — it handles skill installation and then routes back here for setup.

---

## Phase 1 — Create a workspace

### 1a — Set up Python virtual environment

Before creating the workspace, ensure a Python virtual environment is available. This is required if the user will run **ibis-server locally** (instead of relying solely on Docker). Skip this step if the user will use only the Dockerized ibis-server.

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows
pip install --upgrade pip
```

> **Tip:** Place the venv inside or adjacent to the workspace directory so it is easy to find. Avoid committing it to version control — add `.venv/` to `.gitignore`.

### 1b — Create a workspace directory

Create a dedicated workspace directory on the host machine. This directory will be mounted into the Docker container, so the container can read and write MDL files.

Ask the user where they want the workspace, or suggest a default:

```bash
mkdir -p ${PWD}/wren-workspace
```

Save the chosen path as `<WORKSPACE_PATH>` (absolute path, e.g. `/Users/me/wren-workspace`). All subsequent steps reference this path.

Recommended workspace layout after the quickstart completes:

```
<WORKSPACE_PATH>/
├── wren_project.yml
├── models/
│   └── *.yml
├── relationships.yml
├── views.yml
└── target/
    └── mdl.json          # Compiled MDL — loaded by Docker container
```

---

## Phase 2 — Generate MDL and save project

### 2a — Generate MDL

Invoke the **generate-mdl** skill to introspect the user's database and build the MDL manifest:

```
@generate-mdl
```

The generate-mdl skill will:
1. Ask for data source type and connection credentials
2. Call ibis-server to fetch table schema and foreign key constraints
3. Build the MDL JSON (models, columns, relationships)
4. Validate the manifest with a dry-plan

> **Important:** At this stage ibis-server may not be running yet. If the user has not started a container, proceed to Phase 3 first (start the container), then come back to generate the MDL using the running ibis-server on port 8000.
>
> Alternatively, if the user already has a running ibis-server, run Phase 2 before Phase 3.

### 2b — Save as YAML project

After the MDL is generated, invoke the **wren-project** skill to save it as a versioned YAML project inside the workspace:

```
@wren-project
```

Direct the skill to write the project files into `<WORKSPACE_PATH>`:

- `<WORKSPACE_PATH>/wren_project.yml`
- `<WORKSPACE_PATH>/models/*.yml`
- `<WORKSPACE_PATH>/relationships.yml`
- `<WORKSPACE_PATH>/views.yml`

Then build the compiled target:

- `<WORKSPACE_PATH>/target/mdl.json`

The Docker container will auto-load this file at startup.

---

## Phase 3 — Start and register the MCP server

Invoke the **wren-mcp-setup** skill to start the Docker container and register the MCP server with the AI client:

```
@wren-mcp-setup
```

Pass `<WORKSPACE_PATH>` as the workspace mount path when the skill asks.

The wren-mcp-setup skill will:
1. Start the container with `-v <WORKSPACE_PATH>:/workspace`
2. Set `MDL_PATH=/workspace/target/mdl.json`
3. Register the MCP server with the AI client (`claude mcp add`)
4. Verify the container is running

> If `<WORKSPACE_PATH>/target/mdl.json` already exists before the container starts, it is loaded automatically at boot. No separate `deploy` call is needed.

### 3b — Configure connection info via Web UI

Once the container is running, open the MCP server Web UI to configure connection info:

```text
http://localhost:9001
```

Enter the data source credentials (host, port, database, user, password, etc.) in the UI form and save. The MCP server stores and applies the connection info without exposing credentials to this conversation.

> **Tip:** If your database is running locally, use `host.docker.internal` instead of `localhost` as the host address.

---

## Phase 4 — Verify and confirm

Once the MCP server is registered, the user must **start a new session** for the Wren MCP tools to be loaded. Instruct the user to do this now.

In the new session, ask the AI agent to run a health check:

```
Use health_check() to verify Wren Engine is reachable.
```

Expected response: `SELECT 1` returns successfully.

If the health check passes:

- Tell the user setup is complete.
- In this session, they can start querying immediately:

```
Query: How many orders are in the orders table?
```

If the health check fails, follow the troubleshooting steps in the **wren-mcp-setup** skill.

---

## Quick reference — skill invocations

| Phase | Skill | Purpose |
|-------|-------|---------|
| 2a | `@generate-mdl` | Introspect database and build MDL JSON |
| 2b | `@wren-project` | Save MDL as YAML project + compile to `target/` |
| 3 | `@wren-mcp-setup` | Start Docker container and register MCP server |

---

## Troubleshooting

**Container not finding MDL at startup:**
- Confirm `<WORKSPACE_PATH>/target/mdl.json` exists before starting the container.
- Check container logs: `docker logs wren-mcp`

**generate-mdl fails because ibis-server is not yet running:**
- Start the container first (Phase 3), then return to Phase 2.
- ibis-server is available at `http://localhost:8000` once the container is up.

**MCP tools not available after registration:**
- The MCP server is only loaded at session start. Start a new Claude Code session after registering.

**Database connection refused inside Docker:**
- Change `localhost` / `127.0.0.1` to `host.docker.internal` in connection credentials.
- See the **wren-mcp-setup** skill for the full localhost fix.
