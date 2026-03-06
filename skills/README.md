# Wren Engine Skills

This directory contains reusable AI agent skills for working with Wren Engine. Skills are instruction files that teach AI agents (Claude, Cline, Cursor, etc.) how to perform specific workflows with Wren's tools and APIs.

## Installation

### Option 1 — install script (recommended)

**From a local clone:**
```bash
bash skills/install.sh                        # all skills
bash skills/install.sh generate-mdl wren-sql  # specific skills
bash skills/install.sh --force generate-mdl   # overwrite existing
```

**Remote (one-liner, after pushing to GitHub):**
```bash
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
# specific skills:
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- generate-mdl
```

### Option 2 — npx openskills (cross-agent)

Installs into Claude Code, Cursor, Windsurf, and 30+ other agent tools:
```bash
npx openskills add Canner/wren-engine
```

### Option 3 — manual copy

```bash
cp -r skills/generate-mdl ~/.claude/skills/
# or all at once:
cp -r skills/generate-mdl skills/mdl-project skills/wren-sql skills/wren-quickstart ~/.claude/skills/
```

Once installed, invoke a skill by name in your conversation:

```text
/generate-mdl
/mdl-project
/wren-sql
/wren-quickstart
```

## Available Skills

| Skill | Description |
|-------|-------------|
| [generate-mdl](generate-mdl/SKILL.md) | Generate a Wren MDL manifest from a live database using ibis-server introspection |
| [mdl-project](mdl-project/SKILL.md) | Save, load, and build MDL manifests as version-controlled YAML project directories |
| [wren-sql](wren-sql/SKILL.md) | Write and correct SQL queries for Wren Engine — types, date/time, BigQuery dialect, error diagnosis |
| [wren-quickstart](wren-quickstart/SKILL.md) | Set up Wren Engine MCP via Docker and connect to Claude Code or other MCP clients |

See [SKILLS.md](SKILLS.md) for full details on each skill.

## Requirements

- A running [ibis-server](../ibis-server/) instance
- The [Wren MCP server](../mcp-server/) connected to your AI client
- An AI client that supports skills (Claude Code, Cline, Cursor, etc.)
