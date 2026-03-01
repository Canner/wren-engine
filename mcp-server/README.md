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

## MDL Agent (Optional)

The **MDL Agent** is an optional extension that adds three MCP tools for **agentic MDL generation**.  
Instead of writing MDL JSON by hand, you describe your database and the agent explores the schema,
builds the manifest, and hands it off to the existing `deploy` tool — all within the same MCP session.

### Prerequisites

- An LLM provider API key (Anthropic, OpenAI, etc.)
- The `wren-agent` package (lives inside this monorepo at `../wren-agent`)

### Installation

Install `wren-agent` as an editable path dependency into the MCP server's virtual environment:

```sh
# inside mcp-server/
just install-mdl-agent
# or, without just:
uv pip install --editable "../wren-agent"
```

> The server uses a **graceful fallback**: if `wren-agent` is not installed the three MDL tools
> are simply not registered, and all other tools continue to work unchanged.

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `PYDANTIC_AI_MODEL` | **Yes** | `anthropic:claude-3-5-sonnet-latest` | LLM to use, e.g. `openai:gpt-4o` |
| `WREN_ENGINE_ENDPOINT` | No | — | ibis-server URL for MDL dry-plan validation |
| `MDL_AGENT_SKILLS_DIR` | No | — | Directory of `*.md` / `*.txt` skill files loaded at startup |
| `MDL_AGENT_MEMORY_PATH` | No | — | JSON file path for persistent cross-session memory |
| `MDL_AGENT_MAX_SKILLS` | No | `5` | Max skills injected per turn (semantic ranking) |

Add these to your `.env` file or to the `"env"` block in your MCP config:

```json
{
    "mcpServers": {
        "wren": {
            "command": "uv",
            "args": [
                "--directory", "/ABSOLUTE/PATH/TO/wren-engine/mcp-server",
                "run", "app/wren.py"
            ],
            "env": {
                "WREN_URL": "localhost:8000",
                "CONNECTION_INFO_FILE": "/path/connection.json",
                "MDL_PATH": "/path/mdl.json",
                "PYDANTIC_AI_MODEL": "anthropic:claude-3-5-sonnet-latest",
                "ANTHROPIC_API_KEY": "<your-key>",
                "WREN_ENGINE_ENDPOINT": "http://localhost:8000",
                "MDL_AGENT_MEMORY_PATH": "~/.wren/memory.json"
            }
        }
    }
}
```

### Available Tools

| Tool | Description |
|---|---|
| `mdl_chat(message, session_id?)` | Send a message to the MDL generation agent. Returns a follow-up question or a completed MDL JSON. |
| `mdl_reset_session(session_id?)` | Clear conversation history and DB connection for a session. |
| `mdl_list_sessions()` | List all active sessions and their message counts. |

### Example Workflow

The following example shows how an AI agent (Claude, Copilot, etc.) would use the MDL tools
in a conversation:

```
User  → "Generate an MDL for my PostgreSQL ecommerce database."

Agent → mdl_chat("Generate an MDL for my PostgreSQL ecommerce database.")
      ← [MDL Agent asks]: Please provide your PostgreSQL connection string.

Agent → mdl_chat("postgresql://user:pass@localhost:5432/shop")
      ← [MDL Agent asks]: I found tables: orders, customers, products, order_items.
        Should I include all of them?

Agent → mdl_chat("Yes, include all.")
      ← [MDL Ready — use the 'deploy' tool with this manifest]
        {
          "catalog": "wren",
          "schema": "public",
          "dataSource": "POSTGRES",
          "models": [ ... ]
        }

Agent → deploy(<paste MDL JSON above>)
      ← "MDL deployed successfully"
```

Multiple independent sessions can run in parallel using different `session_id` values:

```
mdl_chat("...", session_id="project_a")
mdl_chat("...", session_id="project_b")
```

### Skills

You can inject domain knowledge (naming conventions, relationship patterns, etc.) by providing a
skills directory. Each `.md` or `.txt` file in the directory becomes a skill. On every turn,
the agent automatically selects the most relevant skills using TF-IDF cosine similarity — only
the top `MDL_AGENT_MAX_SKILLS` are injected, so the context stays focused.

```
MDL_AGENT_SKILLS_DIR=./skills/
```

```
skills/
  postgres_conventions.md   # "Use bigint for all primary keys…"
  ecommerce_patterns.md     # "Order tables should reference customers via customer_id…"
  bigquery_tips.md          # "Use STRUCT for nested columns…"
```

---

## Additional Resources

- **Wren Engine Documentation**: [Wren AI](https://getwren.ai/)  
- **MCP Protocol Guide**: [Model Context Protocol](https://modelcontextprotocol.io/)  
