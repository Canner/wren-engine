# wren-agent

AI agent for agentic MDL generation for [Wren Engine](https://github.com/Canner/WrenAI).

## Overview

`wren-agent` is a Python library that uses [PydanticAI](https://ai.pydantic.dev/) to drive a multi-turn conversation with the user, explores a connected database via SQLAlchemy, and outputs a valid [Wren MDL](https://github.com/Canner/WrenAI/tree/main/wren-mdl) manifest.

## Usage

```python
import asyncio
from wren_agent import MDLAgent

async def main():
    agent = MDLAgent()  # uses PYDANTIC_AI_MODEL env var, defaults to claude-3-5-sonnet-latest
    history = []

    # Turn 1: agent asks for connection string
    resp = await agent.run("I want to generate MDL for my database")
    print(resp.result)   # AgentQuestion(question="Please provide your connection string…")
    history = resp.message_history

    # Turn 2: provide connection string
    resp = await agent.run("postgresql://user:pass@localhost/mydb", message_history=history)
    # resp.result → MDLManifest(...)

asyncio.run(main())
```

## Installation

```bash
# Basic (no DB driver)
uv add wren-agent

# With PostgreSQL support
uv add "wren-agent[postgres]"

# With MySQL + DuckDB
uv add "wren-agent[mysql,duckdb]"
```

## Configuration

| Environment variable | Description |
|---|---|
| `PYDANTIC_AI_MODEL` | LLM model string (e.g., `claude-3-5-sonnet-latest`, `openai:gpt-4o`) |
| `ANTHROPIC_API_KEY` | Required when using Claude |
| `OPENAI_API_KEY` | Required when using OpenAI |
| `WREN_ENGINE_ENDPOINT` | Optional ibis-server URL for dry-plan validation (e.g., `http://localhost:8000`) |

Copy `.env.example` to `.env` and fill in your credentials.
