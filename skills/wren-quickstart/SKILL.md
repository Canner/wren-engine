---
name: wren-quickstart
description: End-to-end quickstart for Wren Engine — create a workspace, generate an MDL from a live database, save it as a versioned project, start the Wren MCP Docker container, and verify the setup with a health check. Trigger when a user wants to set up Wren Engine from scratch, onboard a new data source, or get started with Wren MCP. Requires dependent skills already installed (use /wren-usage to install them first).
compatibility: Requires Docker Desktop (or Docker Engine). No local database drivers needed.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.3"
---

# Wren Quickstart

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-quickstart` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-quickstart** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-quickstart --agent claude-code
> ```

Then continue with the workflow below regardless of update status.

---

This skill walks a user through setting up Wren Engine end-to-end — from creating a workspace to running their first query via MCP. Each phase delegates to a focused skill. Follow the steps in order.

> **Prerequisites:** The dependent skills (`wren-generate-mdl`, `wren-project`, `wren-mcp-setup`, `wren-connection-info`) must be installed. If they are missing, use `/wren-usage` first — it handles skill installation and then routes back here for setup.

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

## Phase 2 — Start Docker container and register MCP server

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

### 2b — Configure connection info via Web UI

Once the container is running, open the MCP server Web UI to configure connection info:

```text
http://localhost:9001
```

Enter the data source credentials (host, port, database, user, password, etc.) in the UI form and save. The MCP server stores and applies the connection info without exposing credentials to this conversation.

> **Tip:** If your database is running locally, use `host.docker.internal` instead of `localhost` as the host address.

### 2c — Start a new session

The user must **start a new Claude Code session** for the Wren MCP tools to be loaded. Instruct the user to do this now and come back to continue with Phase 3.

> **Important:** Do not proceed to Phase 3 until the new session is started. The `wren-generate-mdl` skill requires MCP tools (`health_check()`, `list_remote_tables()`, etc.) which are only available after the MCP server is registered and a new session is started.

---

## Phase 3 — Generate MDL and save project

> **Prerequisite:** The MCP server must be registered and a new session started (Phase 2c). The `wren-generate-mdl` skill uses MCP tools — do not call ibis-server API directly.

### 3a — Generate MDL

Invoke the **wren-generate-mdl** skill to introspect the user's database and build the MDL manifest:

```text
@wren-generate-mdl
```

The wren-generate-mdl skill will:
1. Run `health_check()` to verify the connection is working
2. Ask for data source type and optional schema filter
3. Call `list_remote_tables()` and `list_remote_constraints()` via MCP to fetch schema
4. Build the MDL JSON (models, columns, relationships)
5. Validate the manifest with `deploy_manifest()` + `dry_run()`

### 3b — Save as YAML project

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

---

## Phase 4 — Verify and start querying

Run a health check to confirm everything is working:

```
Use health_check() to verify Wren Engine is reachable.
```

Expected response: `SELECT 1` returns successfully.

If the health check passes:

- Tell the user setup is complete.
- They can start querying immediately:

```
Query: How many orders are in the orders table?
```

If the health check fails, follow the troubleshooting steps in the **wren-mcp-setup** skill.

---

## Quick reference — skill invocations

| Phase | Skill | Purpose |
|-------|-------|---------|
| 2 | `@wren-mcp-setup` | Start Docker container and register MCP server |
| 3a | `@wren-generate-mdl` | Introspect database and build MDL JSON |
| 3b | `@wren-project` | Save MDL as YAML project + compile to `target/` |

---

## Troubleshooting

**MCP tools not available:**
- The MCP server is only loaded at session start. Start a new Claude Code session after registering.
- Do not attempt to call ibis-server API directly — always use MCP tools.

**wren-generate-mdl fails:**
- Ensure the container is running: `docker ps --filter name=wren-mcp`
- Ensure connection info is configured in the Web UI (`http://localhost:9001`)
- Ensure a new session was started after `claude mcp add`

**Database connection refused inside Docker:**
- Change `localhost` / `127.0.0.1` to `host.docker.internal` in connection credentials.
- See the **wren-mcp-setup** skill for the full localhost fix.
