# mcp-server

MCP (Model Context Protocol) server exposing Wren Engine to AI agents (Claude, Cline, Cursor). Translates agent requests into Wren Engine API calls and manages MDL manifest deployments.

## Architecture

```text
AI Agent (Claude/Cline/Cursor)
  → MCP protocol (stdio or streamable-http)
  → app/wren.py (FastMCP server, 25+ tools)
  → httpx → ibis-server (localhost:8000)
```

### Key files

- `app/wren.py` — Entry point. FastMCP server, all MCP tools, `MdlCache` for O(1) metadata lookups
- `app/dto.py` — Pydantic models: `Manifest`, `Model`, `Column`, `Relationship`, `View`, `DataSourceType`
- `app/mdl_tools.py` — Optional MDL generation tools (SQLAlchemy-based DB introspection, JSON Schema validation, YAML project save/load)
- `app/utils.py` — Base64 encoding for MDL transmission

## Dev Commands

```bash
uv venv && source .venv/bin/activate
uv run app/wren.py          # Start server (stdio transport by default)

# Install optional MDL tools (SQLAlchemy, jsonschema, pyyaml)
just install-mdl
just install-mdl-driver     # DB drivers: postgres, mysql, duckdb

# Lint
uv run ruff check app/
uv run ruff format app/
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `WREN_URL` | Yes | `localhost:8000` | ibis-server URL |
| `CONNECTION_INFO_FILE` | Yes | — | Path to connection info JSON |
| `MDL_PATH` | Yes | — | Path to MDL JSON file |
| `MCP_TRANSPORT` | No | `stdio` | `stdio` or `streamable-http` |
| `MCP_HOST` | No | `0.0.0.0` | Host for HTTP transport |
| `MCP_PORT` | No | `9000` | Port for HTTP transport |
| `WREN_ENGINE_ENDPOINT` | No | — | Used by MDL tools for dry-plan validation |

Copy `.env.example` to `.env` for local dev.

## MCP Tools

**Deployment & status:** `deploy`, `is_deployed`

**Query:** `query`, `dry_run`

**Metadata:** `get_manifest`, `get_available_tables`, `get_table_info`, `get_table_columns_info`, `get_column_info`, `get_relationships`, `get_available_functions`, `get_current_data_source_type`, `list_remote_tables`, `list_remote_constraints`

**MDL generation (optional):** `mdl_connect_database`, `mdl_list_tables`, `mdl_get_column_info`, `mdl_get_column_stats`, `mdl_get_sample_data`, `mdl_validate_manifest`, `mdl_save_project`, `mdl_load_project`, `build_mdl_project`

**Utilities:** `health_check`, `get_wren_guide`

## MCP Client Config

```json
{
  "mcpServers": {
    "wren": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/mcp-server", "run", "app/wren.py"],
      "env": {
        "WREN_URL": "localhost:8000",
        "CONNECTION_INFO_FILE": "/path/to/connection.json",
        "MDL_PATH": "/path/to/mdl.json"
      }
    }
  }
}
```

## Notes

- No test suite currently. Validate via `mdl_validate_manifest` tool and dry-run endpoints.
- MDL tools (`mdl_tools.py`) require optional dependencies — install with `just install-mdl`.
- MDL is transmitted to ibis-server as base64-encoded JSON.
- Package manager is `uv` (not Poetry). Lock file is `uv.lock`.
