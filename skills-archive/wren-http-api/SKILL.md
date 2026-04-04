---
name: wren-http-api
description: "Interact with Wren Engine MCP server via plain HTTP JSON-RPC requests — no MCP client SDK required. Covers session initialization, tool discovery, and calling all 20+ Wren tools (query, deploy, metadata, health check) using standard HTTP POST with JSON-RPC 2.0 payloads. Use when the client cannot or prefers not to use the MCP protocol directly (e.g. OpenClaw, custom HTTP clients, shell scripts, or any environment without an MCP SDK)."
compatibility: "Requires a running Wren MCP server with streamable-http transport (default Docker setup). curl or any HTTP client."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Interact with Wren MCP via HTTP JSON-RPC

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-http-api` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-http-api** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-http-api --agent claude-code
> ```

Then continue with the workflow below regardless of update status.

---

## Overview

The Wren MCP server exposes a **streamable-http** endpoint that speaks **JSON-RPC 2.0** over plain HTTP POST. Any HTTP client (curl, httpx, fetch, requests) can call Wren tools without an MCP SDK.

**Base URL:** `http://localhost:9000/mcp` (default Docker setup from `wren-mcp-setup`)

All requests use:
- Method: `POST`
- Content-Type: `application/json`
- Accept: `application/json, text/event-stream`

---

## Step 1 — Initialize a session

Before calling any tool, initialize a JSON-RPC session. The server returns a `Mcp-Session-Id` header that must be included in all subsequent requests.

```bash
curl -s -D - http://localhost:9000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-03-26",
      "capabilities": {},
      "clientInfo": { "name": "my-client", "version": "1.0" }
    }
  }'
```

**Save the `Mcp-Session-Id` header** from the response. Then complete the handshake:

```bash
curl -s http://localhost:9000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Mcp-Session-Id: <SESSION_ID>" \
  -d '{"jsonrpc": "2.0", "method": "notifications/initialized"}'
```

> The `initialized` notification has no `id` field — it is a JSON-RPC notification, not a request.

Or run the helper script: `bash scripts/session.sh http://localhost:9000/mcp`

---

## Step 2 — Discover available tools

```bash
curl -s http://localhost:9000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Mcp-Session-Id: <SESSION_ID>" \
  -d '{"jsonrpc": "2.0", "id": 2, "method": "tools/list"}'
```

Returns `result.tools` — an array of tool definitions with name, description, and input schema.

---

## Step 3 — Call tools

All tool calls use the `tools/call` method with this structure:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "<tool_name>",
    "arguments": { ... }
  }
}
```

Responses arrive as **Server-Sent Events** (SSE). Parse the `data:` line:

```
event: message
data: {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"..."}]}}
```

Extract the tool output from `result.content[0].text`. Shell shortcut:

```bash
curl -s ... | grep '^data: ' | sed 's/^data: //' | jq '.result.content[0].text'
```

See [references/response-format.md](references/response-format.md) for full response parsing and error handling details.

---

## Available Tools — Quick Reference

All arguments are passed inside `params.arguments`. See [references/tools.md](references/tools.md) for full details, argument tables, and example payloads for every tool.

| Category | Tool | Arguments | Description |
|----------|------|-----------|-------------|
| **Health** | `health_check` | — | Check engine health and configuration |
| | `is_deployed` | — | Check if MDL is deployed |
| | `get_version` | — | Get MCP server version |
| **Deploy** | `deploy` | `mdl_file_path` | Deploy MDL from a JSON file path |
| | `deploy_manifest` | `mdl` | Deploy MDL dict directly |
| | `mdl_validate_manifest` | `mdl` | Validate MDL without deploying |
| **Query** | `query` | `sql` | Execute SQL query |
| | `dry_run` | `sql` | Validate SQL without executing |
| **Metadata** | `get_manifest` | — | Get full deployed MDL |
| | `get_available_tables` | — | List model/table names |
| | `get_table_info` | `table_name` | Get table info + column names |
| | `get_column_info` | `table_name`, `column_name` | Get column detail |
| | `get_table_columns_info` | `table_columns`, `full_column_info?` | Batch column lookup |
| | `get_relationships` | — | Get all MDL relationships |
| | `get_current_data_source_type` | — | Get data source type |
| | `get_available_functions` | — | List SQL functions for data source |
| | `get_wren_guide` | — | Get usage tips |
| **Remote DB** | `list_remote_tables` | — | List tables in connected DB |
| | `list_remote_constraints` | — | List foreign key constraints |

### Example: query

```bash
curl -s http://localhost:9000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Mcp-Session-Id: <SESSION_ID>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 10,
    "method": "tools/call",
    "params": {
      "name": "query",
      "arguments": { "sql": "SELECT * FROM orders LIMIT 5" }
    }
  }'
```

### Example: deploy_manifest

```bash
curl -s http://localhost:9000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Mcp-Session-Id: <SESSION_ID>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 11,
    "method": "tools/call",
    "params": {
      "name": "deploy_manifest",
      "arguments": {
        "mdl": {
          "catalog": "wren",
          "schema": "public",
          "dataSource": "POSTGRES",
          "models": [],
          "relationships": [],
          "views": []
        }
      }
    }
  }'
```

---

## Prerequisites

This skill assumes the Wren MCP server is already running with **streamable-http** transport. If not set up yet, use the `wren-mcp-setup` skill or run:

```bash
docker run -d --name wren-mcp \
  -p 8000:8000 -p 9000:9000 -p 9001:9001 \
  -e ENABLE_MCP_SERVER=true \
  -e MCP_TRANSPORT=streamable-http \
  -e MCP_HOST=0.0.0.0 -e MCP_PORT=9000 \
  -e WREN_URL=localhost:8000 \
  -e MDL_PATH=/workspace/target/mdl.json \
  -v <WORKSPACE_PATH>:/workspace \
  ghcr.io/canner/wren-engine-ibis:latest
```

Configure connection info via the Web UI at `http://localhost:9001`.
