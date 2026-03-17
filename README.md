<p align="center">
  <a href="https://getwren.ai">
    <picture>
      <source media="(prefers-color-scheme: light)" srcset="./misc/wrenai_logo.png">
      <img src="./misc/wrenai_logo.png" alt="Wren AI logo">
    </picture>
    <h1 align="center">Wren Engine</h1>
  </a>
</p>

<p align="center">
  The open context engine for AI agents
</p>

<p align="center">
  <a aria-label="Follow us" href="https://x.com/getwrenai">
    <img alt="" src="https://img.shields.io/badge/-@getwrenai-blue?style=for-the-badge&logo=x&logoColor=white&labelColor=gray&logoWidth=20">
  </a>
  <a aria-label="License" href="https://github.com/Canner/wren-engine/blob/main/LICENSE">
    <img alt="" src="https://img.shields.io/github/license/canner/wren-engine?color=blue&style=for-the-badge">
  </a>
  <a aria-label="Join the community on GitHub" href="https://discord.gg/5DvshJqG8Z">
    <img alt="" src="https://img.shields.io/badge/-JOIN%20THE%20COMMUNITY-blue?style=for-the-badge&logo=discord&logoColor=white&labelColor=grey&logoWidth=20">
  </a>
  <a aria-label="Canner" href="https://cannerdata.com/">
    <img src="https://img.shields.io/badge/%F0%9F%A7%A1-Made%20by%20Canner-blue?style=for-the-badge">
  </a>
</p>

> Wren Engine is the open foundation behind Wren AI: a semantic, governed, agent-ready context layer for business data.

<p align="center">
  <img width="1267" height="705" alt="with_wren_engine" src="https://github.com/user-attachments/assets/3a6531fe-4731-4f21-ae9a-786b219f3c0e" />
</p>

## Why Wren Engine

AI agents can already call tools, browse docs, and write code. What they still struggle with is business context.

Enterprise data is not just rows in a warehouse. It is definitions, metrics, relationships, permissions, lineage, and intent. An agent that can connect to PostgreSQL or Snowflake still does not know what "net revenue", "active customer", or "pipeline coverage" actually mean in your company.

That is why the category is moving beyond raw data access. In [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/), a16z makes the case that data agents need business definitions, institutional knowledge, and system-level context to be useful in production. That is exactly the problem Wren Engine is built to solve in the open.

<p align="center">
  <img width="920" height="638" alt="without_wren_engine" src="https://github.com/user-attachments/assets/3295dde5-ce41-4e56-a8ad-daff6a0c3459" />
</p>

Wren Engine exists to solve that gap.

It gives AI agents a semantic layer they can reason over, so they can:

- understand models instead of raw tables
- use trusted metrics instead of inventing SQL
- follow relationships instead of guessing joins
- respect governance instead of bypassing it
- turn natural language into accurate, explainable data access

This is the open source context engine for teams building the next generation of agent experiences.

## The Vision

We believe the future of AI is not tool calling alone. It is context-rich systems where agents can reason, retrieve, plan, and act on top of a shared understanding of business reality.

Or more simply: agents need more than connectivity. They need context.

Wren Engine is our open source contribution to that future.

It is the semantic and execution foundation beneath Wren AI, and it is designed to be useful well beyond a single product:

- embedded in MCP servers and agent workflows
- connected to modern warehouses, databases, and file systems
- expressive enough to model business meaning through MDL
- robust enough to support governed enterprise use cases
- open enough for the community to extend, integrate, and build on

If Wren AI is the full vision, Wren Engine is the open core that makes that vision interoperable.

## What Wren Engine Does

Wren Engine turns business data into agent-usable context.

At a high level:

1. You describe your business domain with Wren's semantic model and MDL.
2. Wren Engine analyzes intent, models, relationships, and access rules.
3. It plans and generates correct queries across your underlying data sources.
4. MCP clients and AI agents interact with that context through a clean interface.

This is the practical open source path from text-to-SQL toward context-aware data agents.

That means your agent is no longer asking, "Which raw table should I query?"

It is asking, "Which business concept, metric, or governed slice of context do I need to answer this task correctly?"

## Built For Agent Builders

Wren Engine is especially useful for the open source community building agent-native workflows in tools like:

- OpenClaw
- Cloud Code
- VS Code
- Claude Desktop
- Cline
- Cursor

If your environment can speak MCP, call HTTP APIs, or embed a semantic service, Wren Engine can become the context layer behind your agent.

Use it to power experiences like:

- natural-language analytics with trusted business definitions
- AI copilots that can answer questions across governed enterprise data
- agents that generate dashboards, reports, and workflow decisions
- code assistants that need real business context, not just schema dumps
- internal AI tools that should be grounded in semantic models instead of ad hoc SQL

## Why Open Source

We think agent infrastructure should be composable.

The world does not need one more closed box that only works in one UI, one cloud, or one workflow. It needs shared infrastructure that developers can inspect, extend, self-host, and integrate anywhere.

Wren Engine is open source so the community can:

- run it locally or in their own stack
- connect it to their preferred MCP client or IDE
- contribute connectors, optimizations, and semantic capabilities
- build opinionated agent products on a transparent foundation
- help define what a real context layer for AI should look like

We want this layer to be inspectable, composable, and community-owned, not hidden behind a single proprietary experience.

## Architecture At A Glance

```text
User / Agent
  -> MCP Client or App (OpenClaw, Cloud Code, VS Code, Claude Desktop, Cline, Cursor, etc.)
  -> Wren MCP Server or HTTP API
  -> Wren Engine semantic layer
  -> Query planning and optimization
  -> Your warehouse, database, or file-backed data source
```

Core ideas:

- `MDL` captures business meaning, not just physical schema
- `wren-core` performs semantic analysis and query planning in Rust
- `ibis-server` provides the execution and connector-facing API layer
- `mcp-server` makes Wren easy to use from MCP-compatible agents

## Repository Map

This repository contains the core engine modules:

| Module | What it does |
| --- | --- |
| [`wren-core`](./wren-core) | Rust semantic engine powered by Apache DataFusion for MDL analysis, planning, and optimization |
| [`wren-core-base`](./wren-core-base) | Shared manifest and modeling types |
| [`wren-core-py`](./wren-core-py) | PyO3 bindings that expose the engine to Python |
| [`ibis-server`](./ibis-server/) | FastAPI server for query execution, validation, metadata, and connectors |
| [`mcp-server`](./mcp-server/) | MCP server for AI agents and MCP-compatible clients |

Supporting modules include `wren-core-legacy`, `example`, `mock-web-server`, and benchmarking utilities.

## Supported Data Sources

Wren Engine is built to work across modern data stacks, including warehouses, databases, and file-based sources.

Current open source support includes connectors such as:

- Amazon S3
- Apache Spark
- Athena
- BigQuery
- Databricks
- DuckDB
- Google Cloud Storage
- Local files
- MinIO
- MySQL
- Oracle
- PostgreSQL
- Redshift
- SQL Server
- Snowflake
- Trino

See the connector API docs in the project documentation for the latest connection schemas and capabilities.

## Get Started

### Use Wren through MCP

If you want to use Wren Engine from an AI agent or MCP-capable IDE, start here:

- [MCP Server README](./mcp-server/README.md)

The MCP server includes:

- a local Web UI for connection and MDL setup
- read-only mode for safer agent usage
- manifest deployment and validation tools
- metadata tools for remote schema discovery

### Learn the concepts

- [Quick start with Wren Engine](https://docs.getwren.ai/oss/engine/get_started/quickstart)
- [What is semantics?](https://docs.getwren.ai/oss/engine/concept/what_is_semantics)
- [What is Modeling Definition Language (MDL)?](https://docs.getwren.ai/oss/engine/concept/what_is_mdl)
- [Benefits of Wren Engine with LLMs](https://docs.getwren.ai/oss/engine/concept/benefits_llm)
- [Powering Semantic SQL for AI Agents with Apache DataFusion](https://getwren.ai/post/powering-semantic-sql-for-ai-agents-with-apache-datafusion)

### Developer entry points

- [`wren-core/README.md`](./wren-core/README.md)
- [`wren-core-py/README.md`](./wren-core-py/README.md)
- [`ibis-server/README.md`](./ibis-server/README.md)
- [`mcp-server/README.md`](./mcp-server/README.md)

## Local Development

Common workflows:

```bash
# Rust semantic engine
cd wren-core
cargo check --all-targets

# Python + connector server
cd ibis-server
just install
just dev

# MCP server
cd mcp-server
# see module README for uv-based setup
```

## Project Status

Wren Engine is actively evolving in the open. The current focus is to make the semantic layer, execution path, and MCP integration stronger for real-world agent workflows.

If you are building with agents today, this is a great time to get involved.

## Community

- Join our [Discord community](https://discord.gg/5DvshJqG8Z)
- Open a [GitHub issue](https://github.com/Canner/wren-engine/issues)
- Explore [Wren AI](https://github.com/Canner/WrenAI) to see the broader product vision
- Read our mission piece: [Fueling the Next Wave of AI Agents](https://getwren.ai/post/fueling-the-next-wave-of-ai-agents-building-the-foundation-for-future-mcp-clients-and-enterprise-data-access)
- Read the a16z perspective: [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/)

Wren Engine is for builders who believe AI needs better context, not just better prompts.
