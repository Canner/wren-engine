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

> Wren Engine is the open foundation behind [Wren AI](https://github.com/Canner/WrenAI): a semantic, governed, agent-ready context layer for business data.

https://github.com/user-attachments/assets/037f2317-d8e5-41f2-9563-1e0bce4ef50c

## Why Wren Engine

AI agents can already call tools, browse docs, and write code. What they still struggle with is business context.

Enterprise data is not just rows in a warehouse. It is definitions, metrics, relationships, permissions, lineage, and intent. An agent that can connect to PostgreSQL or Snowflake still does not know what "net revenue", "active customer", or "pipeline coverage" actually mean in your company.

This is not just our thesis. In [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/), a16z argues that data agents break down when they only have connectivity and SQL generation, but lack business definitions, source-of-truth context, and the operational knowledge that explains how a company actually runs.

<p align="center">
  <img width="888" height="678" alt="before Wren Engine Challenge" src="https://github.com/user-attachments/assets/62e2cbe6-ee28-4417-ba69-805de35d3187" />
</p>

Wren Engine exists to solve that gap.

It gives AI agents a context engine they can reason over, so they can:

- understand models instead of raw tables
- use trusted metrics instead of inventing SQL
- follow relationships instead of guessing joins
- respect governance instead of bypassing it
- turn natural language into accurate, explainable data access

This is the open source context engine for teams building the next generation of agent experiences.

## The Vision

We believe the future of AI is not tool calling alone. It is context-rich systems where agents can reason, retrieve, plan, and act on top of a shared understanding of business reality.

Wren Engine is our open source contribution to that future.

<p align="center">
  <img width="1267" height="705" alt="with_wren_engine" src="https://github.com/user-attachments/assets/3a6531fe-4731-4f21-ae9a-786b219f3c0e" />
</p>

It is the semantic and execution foundation beneath Wren AI, and it is designed to be useful well beyond a single product:

- embedded in MCP servers and agent workflows
- connected to modern warehouses, databases, and file systems
- expressive enough to model business meaning through MDL
- robust enough to support governed enterprise use cases
- open enough for the community to extend, integrate, and build on

If Wren AI is the full vision, Wren Engine is the open core that makes that vision interoperable.

## What Wren Engine Does

Wren Engine turns business data into agent-usable context.

<p align="center">
  <img width="2199" height="1537" alt="engine-architecture" src="https://github.com/user-attachments/assets/302351e7-9ac3-4916-99f6-972d33aee076" />
</p>

At a high level:

1. You describe your business domain with Wren's semantic model and MDL.
2. Wren Engine captures the context agents need: models, metrics, relationships, and access rules.
3. It analyzes intent and plans correct queries across your underlying data sources.
4. MCP clients and AI agents interact with that context through a clean interface.
5. Teams keep refining the model as business logic and systems evolve.

This is the practical open source path from text-to-SQL toward context-aware data agents.

That means your agent is no longer asking, "Which raw table should I query?"

It is asking, "Which business concept, metric, or governed slice of context do I need to answer this task correctly?"

## Built For Agent Builders

Wren Engine is especially useful for the open source community building agent-native workflows in tools like:

- OpenClaw
- Claude Code
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

This is especially important in developer-facing agent environments, where the assistant may understand your codebase but still lacks the business context required to answer data questions correctly.

## Supported Data Sources

Wren Engine is built to work across modern data stacks, including warehouses, databases, and file-based sources.

Current open source support includes connectors such as:

- Amazon S3
- Apache Spark
- Apache Doris
- Athena
- BigQuery
- ClickHouse
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

### Use Wren Engine through MCP

If you want to use Wren Engine from an Claude Code or MCP-capable IDE, start here:

- [Quick start: Chat with jaffle_shop using Wren Engine + Claude Code](https://docs.getwren.ai/oss/engine/get_started/quickstart)
- [Quick start with Claude Desktop](https://docs.getwren.ai/oss/engine/get_started/quickstart_claude)
- [Understanding Wren AI project structure](https://docs.getwren.ai/oss/engine/get_started/structure)

The MCP server includes:

- a local Web UI for connection and MDL setup
- read-only mode for safer agent usage
- manifest deployment and validation tools
- metadata tools for remote schema discovery

### Learn the concepts

- [What is context?](https://docs.getwren.ai/oss/engine/concept/what_is_context)
- [What is Modeling Definition Language (MDL)?](https://docs.getwren.ai/oss/engine/concept/what_is_mdl)
- [Benefits of Wren Engine with LLMs](https://docs.getwren.ai/oss/engine/concept/benefits_llm)
- [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/)
- [Powering Semantic SQL for AI Agents with Apache DataFusion](https://getwren.ai/post/powering-semantic-sql-for-ai-agents-with-apache-datafusion)



## Wren Engine vs. Other Data Tools

People often compare Wren Engine to catalog services like DataHub, raw database MCP servers, BI semantic tools, or text-to-SQL agents.

The simple difference is:

- those tools usually help an agent find data or generate SQL
- Wren Engine helps an agent understand business meaning and produce the right query through a context engine

| Tool type | What it gives the agent | What Wren Engine adds |
| --- | --- | --- |
| Data catalog services | Tables, columns, lineage, owners, descriptions | Business models, metrics, relationships, and governed query planning |
| Raw database or schema access | Direct access to schemas and SQL execution | A business layer above raw tables so the agent does not have to guess intent |
| BI or semantic tools | Curated metrics and entities for analytics workflows | An open context layer designed for MCP and agent workflows |
| Text-to-SQL agents | Fast SQL generation from natural language | Better accuracy by grounding generation in explicit business definitions |

Many teams will want both:

- a catalog to inventory and document the data estate
- Wren Engine to turn that data into agent-ready context

Why that matters:

- more accurate answers because joins and metrics are defined instead of guessed
- more consistent answers because every agent uses the same business definitions
- safer data access because governance can be carried into query planning
- less prompt engineering because the context lives in the engine, not in the prompt

Without Wren, an agent may know where the data is but still not know how to answer the question correctly.

## Repository Map

This repository contains the core engine modules:

| Module | What it does |
| --- | --- |
| [`wren-core`](./wren-core) | Rust context engine powered by Apache DataFusion for MDL analysis, planning, and optimization |
| [`wren-core-base`](./wren-core-base) | Shared manifest and modeling types |
| [`wren-core-py`](./wren-core-py) | PyO3 bindings that expose the engine to Python |
| [`ibis-server`](./ibis-server/) | FastAPI server for query execution, validation, metadata, and connectors |
| [`mcp-server`](./mcp-server/) | MCP server for AI agents and MCP-compatible clients |

Supporting modules include `wren-core-legacy`, `example`, `mock-web-server`, and benchmarking utilities.

### Developer entry points

- [`wren-core/README.md`](./wren-core/README.md)
- [`wren-core-py/README.md`](./wren-core-py/README.md)
- [`ibis-server/README.md`](./ibis-server/README.md)
- [`mcp-server/README.md`](./mcp-server/README.md)

## Local Development

Common workflows:

```bash
# Rust context engine
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

Wren Engine is actively evolving in the open. The current focus is to make the context engine, execution path, and MCP integration stronger for real-world agent workflows.

If you are building with agents today, this is a great time to get involved.

## Community

- Join our [Discord community](https://discord.gg/5DvshJqG8Z)
- Open a [GitHub issue](https://github.com/Canner/wren-engine/issues)
- Explore [Wren AI](https://github.com/Canner/WrenAI) to see the broader product vision
- Read the market thesis from a16z: [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/)

Wren Engine is for builders who believe AI needs better context, not just better prompts.
