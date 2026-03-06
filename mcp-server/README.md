# Wren MCP Server

The **Wren MCP Server** is a **Model Context Protocol (MCP) server** that provides tools for interacting with **Wren Engine** to facilitate AI agent integration.

## Requirements

Before setting up the Wren MCP Server, ensure you have the following dependency installed:

- **[uv](https://docs.astral.sh/uv/getting-started/installation/#installing-uv)** - A fast and efficient Python package manager.

## Environment Variables

The server requires the following environment variables to be set:

| Variable | Description |
|----------|------------|
| `WREN_URL` | The URL of the **Wren Ibis server**. |
| `CONNECTION_INFO_FILE` | The path to the **required connection info file**. |
| `MDL_PATH` | The path to the **MDL file**. |

### Connection Info

The following JSON is a connection info of a Postgres. You can find the requried fields for each data source in the [source code](https://github.com/Canner/wren-engine/blob/4ac283ee0754b12a8c3b0a6f13b32c935fcb7b0d/ibis-server/app/model/__init__.py#L75).
```json
{
    "host": "localhost",
    "port": "5432",
    "user": "test",
    "password": "test",
    "database": "test"
}
```

### The `dataSource` field is requried.

In the MDL, the `dataSource` field is required to indicate which data source should be connected. 

### `.env` File Support

Wren MCP Server supports an `.env` file for easier environment configuration. You can define all the required environment variables in this file.

---

## Installation & Usage

### 1. Set the Python Envrionment

Use the `uv` command to create a virtual envrionment and activate it:
```
> uv venv
Using CPython 3.11.11
Creating virtual environment at: .venv
Activate with: source .venv/bin/activate
> source .venv/bin/activate   
> uv run app/wren.py
Loaded MDL etc/mdl.json
Loaded connection info etc/pg_conneciton.json
```
You would see that the MDL and connection info are loaded. Then, you can use `Ctrl + C` terminate the process.

### 2. Start Wren Engine and Ibis Server

- If you **already have a running Wren Engine**, ensure that `WREN_URL` is correctly set to point to your server.
- If you **don't have a running engine**, you can start one using Docker:

  ```sh
  cd docker
  docker compose up
  ```

### MDL Workspace

The `workspace/` directory is the conventional location for MDL files when using Docker.
It is mounted into the container at `/workspace`.

**Input — load an existing MDL:**
Place your MDL JSON file in `mcp-server/workspace/` on your host, then reference it in the MCP tool:
```
deploy(mdl_file_path="/workspace/my_mdl.json")
```

**Output — save a generated MDL:**
The agent writes YAML project files and compiled MDL JSON back to your host via the `wren-project` skill:
```
# agent writes: mcp-server/workspace/my_project/wren_project.yml, models/*.yml, ...
# agent compiles to: mcp-server/workspace/my_project/target/mdl.json
```

**Custom host path:**
Set `MDL_WORKSPACE` in `docker/.env` to point to any directory on your host:
```sh
# docker/.env
MDL_WORKSPACE=/Users/me/my-mdl-files
```

### 3. Set Environment Variables

There are two ways to set the required environment variables:
- Set up `.env` file in the root directory of the MCP server.
Make sure all required environment variables are properly configured, either in your system or within a `.env` file.
- Set up system environment variables in MCP configuration. See the next step.

### 4. Configure the MCP Server

Create a configuration file with the following structure:

```json
{
    "mcpServers": {
        "wren": {
            "command": "uv",
            "args": [
                "--directory",
                "/ABSOLUTE/PATH/TO/PARENT/FOLDER/wren-engine/mcp-server",
                "run",
                "app/wren.py"
            ],
            "env": {
                "WREN_URL": "localhost:8000",
                "CONNECTION_INFO_FILE": "/path-to-connection-info/connection.json",
                "MDL_PATH": "/path-to-mdl/mdl.json"
            },
            "autoApprove": [],
            "disabled": false
        }
    }
}
```

#### Notes:
- You **may need to provide the full path** to the `uv` executable in the `"command"` field. You can find it using:
  - **MacOS/Linux**: `which uv`
  - **Windows**: `where uv`
- Ensure that the **absolute path** to the MCP server directory is used in the configuration.
- For more details, refer to the [MCP Server Guide](https://modelcontextprotocol.io/quickstart/server#test-with-commands).

### 5. Choose an AI Agent That Supports MCP Server

The following AI agents are compatible with Wren MCP Server and deploy the MCP config:

- **[Claude Desktop](https://modelcontextprotocol.io/quickstart/user)**  
- **[Cline](https://docs.cline.bot/mcp-servers/mcp-quickstart)**  
- **[VsCode MCP Extension](https://code.visualstudio.com/docs/copilot/customization/mcp-servers)**

### 6. Check the Wren Engine is Connected

You can ask the AI agent to perform a health check for Wren Engine.

### 7. Start the Conversation

Now, you can start asking questions through your AI agent and interact with Wren Engine.
Tip: prime your agent with a short instruction so it knows how to use the Wren MCP tools.

Recommended prompt:
```
Use the get_wren_guide() tool to learn how to use Wren Engine and discover available tools and examples.
```

Optional follow-ups:
- "Open the Wren guide."
- "What Wren MCP tools are available?"
- "Show me the available tables in Wren Engine."
- "Query Wren Engine to get ... (your question here)."

---

## Generating an MDL (No Database Drivers Required)

The **`generate-mdl` skill** (`skills/generate-mdl/SKILL.md`) guides any AI agent through
creating a Wren MDL manifest from scratch — using ibis-server's metadata endpoints for
schema discovery. No local database drivers need to be installed on the MCP server.

### How it works

The agent calls ibis-server tools in sequence:

```
User  → "Generate an MDL for my PostgreSQL ecommerce database"

Agent → setup_connection(datasource="POSTGRES",
          conn_info={"host":"localhost","port":"5432","user":"...","database":"shop"})

Agent → list_remote_tables()
      ← [{"name":"orders","columns":[...]}, {"name":"customers","columns":[...]}, ...]

Agent → list_remote_constraints()
      ← [{"constraintType":"FOREIGN_KEY","constraintTable":"orders", ...}]

Agent → (builds MDL JSON from schema info)

Agent → mdl_validate_manifest({"catalog":"wren","schema":"public",
           "dataSource":"POSTGRES","models":[...]})
      ← "MDL validation passed."

Agent → deploy_manifest({"catalog":"wren","schema":"public","dataSource":"POSTGRES","models":[...]})
      ← "MDL deployed successfully (5 models, 23 columns)"
```

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `WREN_ENGINE_ENDPOINT` | No | ibis-server URL for dry-plan check in `mdl_validate_manifest` (default: `http://localhost:8000`) |

---

## Additional Resources

- **Wren Engine Documentation**: [Wren AI](https://getwren.ai/)  
- **MCP Protocol Guide**: [Model Context Protocol](https://modelcontextprotocol.io/)  
