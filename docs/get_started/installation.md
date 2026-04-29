# Installation

## Requirements

- **Python 3.11+**
- **pip** (or any Python package manager)

Optional, depending on your workflow:

- **Git** — for cloning skill repositories
- **Node.js / npm** — for installing skills via `npx`
- An AI coding agent ([Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview), Cursor, Windsurf, Cline, etc.) — for skill-driven workflows

## Install the CLI

```bash
pip install "wren-engine[main]"
```

This installs:

- `wren` CLI — query, plan, validate, build, profile, and memory commands
- `memory` — LanceDB-backed schema indexing and NL-SQL recall
- `interactive` — terminal-based interactive prompts
- `ui` — browser-based profile configuration form

Verify the installation:

```bash
wren version
```

## Data source extras

DuckDB is included by default. For other databases, add the corresponding extra:

```bash
# Single data source
pip install "wren-engine[postgres,main]"

# Multiple data sources
pip install "wren-engine[postgres,bigquery,main]"
```

| Data source | Extra | Notes |
|-------------|-------|-------|
| DuckDB | _(included)_ | No extra needed |
| PostgreSQL | `postgres` | |
| MySQL | `mysql` | |
| BigQuery | `bigquery` | Requires Google Cloud credentials |
| Snowflake | `snowflake` | |
| ClickHouse | `clickhouse` | |
| Trino | `trino` | |
| SQL Server | `mssql` | |
| Databricks | `databricks` | |
| Redshift | `redshift` | |
| Oracle | `oracle` | |
| Athena | `athena` | Requires AWS credentials |
| Apache Spark | `spark` | |

## Install skills

Skills are structured workflow guides that teach AI coding agents how to use the Wren CLI. They are optional but strongly recommended.

```bash
# Via npx
npx skills add Canner/wren-engine --skill '*'

# Or via install script
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
```

The installer auto-detects your AI agent. To target a specific one:

```bash
npx skills add Canner/wren-engine --skill '*' --agent claude-code
```

The installer drops these skills into your agent's skill directory:

| Skill | Purpose |
|-------|---------|
| **wren-onboarding** | End-to-end install + first-connection flow. Triggers on `/wren-onboarding`, "install wren", "set up wren engine" |
| **wren-generate-mdl** | One-time setup — database introspection, type normalization, MDL generation |
| **wren-usage** | Day-to-day workflow — schema context, query recall, SQL execution, result storage |
| **wren-dlt-connector** | Connect SaaS data (HubSpot, Stripe, Salesforce, GitHub, Slack) via dlt pipelines into DuckDB, then auto-generate a Wren project |

After installation, **start a new agent session** — skills are loaded at session start.

See [Skills Reference](../reference/skills.md) for details on what each skill does.

## Virtual environment (recommended)

Keep wren-engine and its dependencies isolated from your system Python:

```bash
python3 -m venv ~/.venvs/wren
source ~/.venvs/wren/bin/activate
pip install "wren-engine[postgres,main]"
```

Activate the environment in every new terminal session before running `wren` commands:

```bash
source ~/.venvs/wren/bin/activate
```

## Upgrading

```bash
pip install --upgrade "wren-engine[main]"
```

To update skills:

```bash
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force
```

## What's next

- [Quickstart](./quickstart.md) — try the CLI with a sample dataset
- [Connect Your Database](./connect.md) — set up your own data source
