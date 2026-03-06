---
name: wren-quickstart
description: End-to-end quickstart for Wren Engine — from zero to querying. Guides the user through installing skills, creating a workspace, generating an MDL from a live database, saving it as a versioned project, starting the Wren MCP Docker container, and verifying the setup with a health check. Trigger when a user wants to set up Wren Engine from scratch, onboard a new data source, or get started with Wren MCP.
compatibility: Requires Docker Desktop (or Docker Engine). No local database drivers needed.
metadata:
  author: wren-engine
  version: "1.0"
---

# Wren Quickstart

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-quickstart` key with this skill's version (`1.0`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-quickstart** skill is available (remote: X.Y, installed: 1.0).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-quickstart
> ```

Then continue with the workflow below regardless of update status.

---

This skill walks a user through setting up Wren Engine end-to-end — from installing the required skills to running their first query via MCP. Each phase delegates to a focused skill. Follow the steps in order.

---

## Phase 1 — Install skills

Before the workflow can proceed, the user needs the dependent skills installed locally.

Tell the user to run the install script once:

```bash
# From a local clone:
bash skills/install.sh

# Or remotely (no clone required):
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
```

This installs all Wren skills (`generate-mdl`, `wren-project`, `wren-sql`, `wren-mcp-setup`, `wren-quickstart`) into `~/.claude/skills/`.

After installation, the user should **restart their AI client session** so the new skills are loaded.

> If the user only wants specific skills, they can pass names as arguments:
> ```bash
> bash skills/install.sh generate-mdl wren-project wren-mcp-setup
> ```

---

## Phase 2 — Create a workspace

Create a dedicated workspace directory on the host machine. This directory will be mounted into the Docker container, so the container can read and write MDL files.

Ask the user where they want the workspace, or suggest a default:

```bash
mkdir -p ~/wren-workspace
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
├── connection.yml
└── target/
    ├── mdl.json          # Compiled MDL — loaded by Docker container
    └── connection.json   # Connection info — loaded by Docker container
```

---

## Phase 3 — Generate MDL and save project

### 3a — Generate MDL

Invoke the **generate-mdl** skill to introspect the user's database and build the MDL manifest:

```
@generate-mdl
```

The generate-mdl skill will:
1. Ask for data source type and connection credentials
2. Call ibis-server to fetch table schema and foreign key constraints
3. Build the MDL JSON (models, columns, relationships)
4. Validate the manifest with a dry-plan

> **Important:** At this stage ibis-server may not be running yet. If the user has not started a container, proceed to Phase 4 first (start the container), then come back to generate the MDL using the running ibis-server on port 8000.
>
> Alternatively, if the user already has a running ibis-server, run Phase 3 before Phase 4.

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
- `<WORKSPACE_PATH>/connection.yml`

Then build the compiled targets:

- `<WORKSPACE_PATH>/target/mdl.json`
- `<WORKSPACE_PATH>/target/connection.json`

The Docker container will auto-load these files at startup.

---

## Phase 4 — Start and register the MCP server

Invoke the **wren-mcp-setup** skill to start the Docker container and register the MCP server with the AI client:

```
@wren-mcp-setup
```

Pass `<WORKSPACE_PATH>` as the workspace mount path when the skill asks.

The wren-mcp-setup skill will:
1. Start the container with `-v <WORKSPACE_PATH>:/workspace`
2. Set `MDL_PATH=/workspace/target/mdl.json` and `CONNECTION_INFO_FILE=/workspace/target/connection.json`
3. Register the MCP server with the AI client (`claude mcp add`)
4. Verify the container is running

> If the MDL files already exist in `<WORKSPACE_PATH>/target/` before the container starts, they are loaded automatically at boot. No separate `deploy` call is needed.

---

## Phase 5 — Verify and confirm

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
| 3a | `@generate-mdl` | Introspect database and build MDL JSON |
| 3b | `@wren-project` | Save MDL as YAML project + compile to `target/` |
| 4 | `@wren-mcp-setup` | Start Docker container and register MCP server |

---

## Troubleshooting

**Container not finding MDL at startup:**
- Confirm `<WORKSPACE_PATH>/target/mdl.json` exists before starting the container.
- Check container logs: `docker logs wren-mcp`

**generate-mdl fails because ibis-server is not yet running:**
- Start the container first (Phase 4), then return to Phase 3.
- ibis-server is available at `http://localhost:8000` once the container is up.

**MCP tools not available after registration:**
- The MCP server is only loaded at session start. Start a new Claude Code session after registering.

**Database connection refused inside Docker:**
- Change `localhost` / `127.0.0.1` to `host.docker.internal` in connection credentials.
- See the **wren-mcp-setup** skill for the full localhost fix.
