# Wren Engine Skills

This directory contains reusable AI agent skills for working with Wren Engine. Skills are instruction files that teach AI agents (Claude, Cline, Cursor, etc.) how to perform specific workflows with Wren's tools and APIs.

## Installation

### Option 1 — Claude Code Plugin

Add the marketplace and install:
```
/plugin marketplace add Canner/wren-engine --path skills-archive
/plugin install wren@wren
```

Or test locally during development:
```bash
claude --plugin-dir ./skills-archive
```

Skills are namespaced as `/wren:<skill>` (e.g., `/wren:generate-mdl`, `/wren:sql`).

### Option 2 — npx skills

Install all skills for Claude Code:
```bash
npx skills add Canner/wren-engine --skill '*' --agent claude-code
```

`npx skills` also supports Cursor, Windsurf, and 30+ other agent tools — replace `--agent claude-code` with your agent of choice.

### Option 3 — ClawHub

Two skills are published on [ClawHub](https://clawhub.ai) for agents that support the ClawHub registry:

```bash
clawhub install wren-usage       # main entry point — installs all dependent skills
clawhub install wren-http-api    # standalone — HTTP JSON-RPC for non-MCP clients
```

### Option 4 — install script (from a local clone)

```bash
bash skills-archive/install.sh                        # all skills
bash skills-archive/install.sh wren-generate-mdl wren-sql  # specific skills
bash skills-archive/install.sh --force wren-generate-mdl   # overwrite existing
```

### Option 5 — manual copy

```bash
cp -r skills-archive/wren-usage ~/.claude/skills/
# or all at once:
cp -r skills-archive/wren-usage skills-archive/wren-quickstart skills-archive/wren-generate-mdl skills-archive/wren-project skills-archive/wren-sql skills-archive/wren-mcp-setup skills-archive/wren-connection-info skills-archive/wren-http-api ~/.claude/skills/
```

Once installed, invoke a skill by name in your conversation:

```text
/wren-usage
/wren-quickstart
/wren-generate-mdl
/wren-project
/wren-sql
/wren-mcp-setup
```

> **Tip:** Use `--skill '*'` to install all skills at once, or specify individual skills (e.g., `--skill wren-generate-mdl --skill wren-sql`).

## Available Skills

| Skill | Description |
|-------|-------------|
| [wren-usage](wren-usage/SKILL.md) | **Primary skill** — daily usage guide: query data, manage MDL, connect databases, operate MCP server `ClawHub` |
| [wren-quickstart](wren-quickstart/SKILL.md) | End-to-end first-time setup — install skills, generate MDL, save project, start MCP server, verify setup |
| [wren-generate-mdl](wren-generate-mdl/SKILL.md) | Generate a Wren MDL manifest from a live database using ibis-server introspection |
| [wren-project](wren-project/SKILL.md) | Save, load, and build MDL manifests as version-controlled YAML project directories |
| [wren-sql](wren-sql/SKILL.md) | Write and correct SQL queries for Wren Engine — types, date/time, BigQuery dialect, error diagnosis |
| [wren-mcp-setup](wren-mcp-setup/SKILL.md) | Set up Wren Engine MCP via Docker, register with Claude Code or other MCP clients, and start querying |
| [wren-connection-info](wren-connection-info/SKILL.md) | Set up data source credentials — produces `connectionFilePath` or inline dict |
| [wren-http-api](wren-http-api/SKILL.md) | Interact with Wren MCP via plain HTTP JSON-RPC — no MCP SDK required (for OpenClaw, custom clients, scripts) `ClawHub` |

See [SKILLS.md](SKILLS.md) for full details on each skill.

## Updating Skills

Each skill automatically checks for updates when invoked. If a newer version is available, the AI agent will notify you with the update command before continuing.

To update manually at any time:

```bash
# Re-add to reinstall the latest version
npx skills add Canner/wren-engine --skill '*' --agent claude-code

# Or reinstall a specific skill
npx skills add Canner/wren-engine --skill wren-generate-mdl --agent claude-code
```

## Releasing a New Skill Version

When updating a skill, two files must be kept in sync:

1. Update `version` in the skill's `SKILL.md` frontmatter:
   ```yaml
   metadata:
     author: wren-engine
     version: "1.2"   # bump this
   ```

2. Update the matching entry in [`versions.json`](versions.json):
   ```json
   {
     "wren-generate-mdl": "1.2"
   }
   ```

Both files must have the same version number. The `SKILL.md` version is what users have installed locally; `versions.json` is what the update check compares against.

## Requirements

- A running [ibis-server](../ibis-server/) instance
- The [Wren MCP server](../mcp-server/) connected to your AI client
- An AI client that supports skills (Claude Code, Cline, Cursor, etc.)
