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

This is not just our thesis. In [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/), a16z argues that data agents break down when they only have connectivity and SQL generation, but lack business definitions, source-of-truth context, and the operational knowledge that explains how a company actually runs.

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

The a16z post captures this shift well: the market is moving beyond text-to-SQL and toward a living context layer that combines semantic meaning, system structure, governance, and human refinement.

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
2. Wren Engine captures the context agents need: models, metrics, relationships, and access rules.
3. It analyzes intent and plans correct queries across your underlying data sources.
4. MCP clients and AI agents interact with that context through a clean interface.
5. Teams keep refining the model as business logic and systems evolve.

This is the practical open source path from text-to-SQL toward context-aware data agents.

That means your agent is no longer asking, "Which raw table should I query?"

It is asking, "Which business concept, metric, or governed slice of context do I need to answer this task correctly?"

## Where Wren Engine Fits Compared To Other Approaches

Teams sometimes ask how Wren Engine differs from connecting an agent to a catalog service such as DataHub through MCP, or from other tools that already expose metadata, BI models, or text-to-SQL access.

The short answer is: many tools help agents discover data or generate queries, while Wren helps agents reason over business meaning and execute governed data access through a semantic layer.

| Approach people often compare | Examples | What it is great at | What is still typically left unresolved for the agent | How Wren Engine differs |
| --- | --- | --- | --- | --- |
| Data catalog service via MCP | DataHub, similar catalog and metadata platforms | Metadata discovery, lineage, ownership, documentation, asset search | Choosing trusted metrics, resolving joins correctly, and turning metadata into governed runtime query behavior | Wren adds semantic models, relationships, metrics, and runtime query planning |
| Database connector or schema browser | Direct warehouse/database MCP servers, JDBC-style access, raw schema inspection tools | Fast access to tables, columns, and SQL execution endpoints | Business meaning is mostly implicit, so agents still have to infer intent from physical schema | Wren gives agents a business layer above raw tables and columns |
| BI or semantic tooling | Semantic layers, metrics layers, BI modeling tools | Defining curated metrics and business-friendly entities for analytics consumers | Many are designed first for dashboards or analyst workflows, not as an open runtime context layer for MCP-native agents | Wren is designed to expose semantic context directly to agent workflows through MCP and APIs |
| Text-to-SQL or SQL agent tooling | NL-to-SQL assistants, generic SQL copilots, LLM agents with database tools | Turning questions into SQL quickly when schema is simple or well-known | Accuracy drops when business definitions, joins, governance, or ambiguity matter | Wren reduces guessing by grounding generation in explicit semantic definitions |
| Knowledge base or documentation retrieval | Wiki search, docs search, RAG over data docs | Retrieving written explanations, runbooks, definitions, and usage notes | Retrieved docs may be stale, inconsistent, or not executable at query time | Wren operationalizes business context so it can be used consistently during planning and execution |

Another way to frame it:

| Dimension | Many adjacent tools | Wren Engine |
| --- | --- | --- |
| Primary role | Discovery, retrieval, documentation, or raw access | Semantic modeling, query planning, and governed execution context |
| What the agent mainly sees | Tables, columns, lineage, tags, owners, descriptions, docs, or SQL endpoints | Models, metrics, relationships, business definitions, and access rules |
| Main question it answers | "What data exists, and how can I inspect or query it?" | "What business concept or trusted metric should I use to answer this question correctly?" |
| SQL generation | Often left to the agent or a generic LLM layer | Built around translating intent through a semantic layer into correct queries |
| Join logic | Agent often has to infer joins from schema, lineage, or examples | Relationships are modeled explicitly so joins are not guessed ad hoc |
| Metric consistency | Definitions may exist in docs or dashboards, but enforcement is indirect | Metrics are defined in the semantic model and reused consistently |
| Governance at query time | Often visible as metadata or policy hints | Designed to carry governed business context into runtime query planning |
| Runtime role in an agent workflow | Helps the agent find, read, or access candidate data assets | Serves as the context and execution layer the agent uses to answer questions |

In practice, many teams may want multiple layers working together:

- a catalog service to inventory and govern the data estate
- documentation and lineage tools to help humans understand the environment
- Wren Engine to turn that estate into a semantic, agent-ready context layer

If you only give an agent metadata, docs, or raw database access, it may still need to guess which joins, filters, and metric definitions are actually correct. Wren exists to reduce that gap between discovery and trustworthy execution.

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

This is especially important in developer-facing agent environments, where the assistant may understand your codebase but still lacks the business context required to answer data questions correctly.

## Why Open Source

We think agent infrastructure should be composable.

The world does not need one more closed box that only works in one UI, one cloud, or one workflow. It needs shared infrastructure that developers can inspect, extend, self-host, and integrate anywhere.

Wren Engine is open source so the community can:

- run it locally or in their own stack
- connect it to their preferred MCP client or IDE
- contribute connectors, optimizations, and semantic capabilities
- build opinionated agent products on a transparent foundation
- help define what a real context layer for AI should look like

We want that context layer to be inspectable, composable, and community-owned, not trapped inside a single proprietary interface.

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

That last point matters: context only helps agents when it is available at runtime. Wren is built to expose that layer over MCP and APIs.

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
- [What is context?](https://docs.getwren.ai/oss/engine/concept/what_is_context)
- [What is Modeling Definition Language (MDL)?](https://docs.getwren.ai/oss/engine/concept/what_is_mdl)
- [Benefits of Wren Engine with LLMs](https://docs.getwren.ai/oss/engine/concept/benefits_llm)
- [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/)
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
- Read the market thesis from a16z: [Your Data Agents Need Context](https://a16z.com/your-data-agents-need-context/)

Wren Engine is for builders who believe AI needs better context, not just better prompts.
