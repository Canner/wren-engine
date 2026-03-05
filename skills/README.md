# Wren Engine Skills

This directory contains reusable AI agent skills for working with Wren Engine. Skills are instruction files that teach AI agents (Claude, Cline, Cursor, etc.) how to perform specific workflows with Wren's tools and APIs.

## Installation

Copy any skill directory into your local Claude skills folder:

```bash
# macOS / Linux
cp -r skills/generate-mdl ~/.claude/skills/

# Or install all skills at once
cp -r skills/* ~/.claude/skills/
```

Once installed, invoke a skill by name in your conversation:

```
/generate-mdl
/mdl-project
/wren-sql
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
