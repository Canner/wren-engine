# Wren Engine Skills

This directory contains reusable AI agent skills for working with Wren Engine. Skills are instruction files that teach AI agents (Claude, Cline, Cursor, etc.) how to perform specific workflows with Wren's tools and APIs.

## Installation

### Option 1 — npx skills (recommended)

Install all skills for Claude Code:
```bash
npx skills add Canner/wren-engine --skill '*' --agent claude-code
```

`npx skills` also supports Cursor, Windsurf, and 30+ other agent tools — replace `--agent claude-code` with your agent of choice.

### Option 2 — install script (from a local clone)

```bash
bash skills/install.sh                        # all skills
bash skills/install.sh generate-mdl wren-sql  # specific skills
bash skills/install.sh --force generate-mdl   # overwrite existing
```

### Option 3 — manual copy

```bash
cp -r skills/wren-usage ~/.claude/skills/
# or all at once:
cp -r skills/wren-usage skills/generate-mdl skills/wren-project skills/wren-sql skills/wren-mcp-setup skills/wren-connection-info ~/.claude/skills/
```

Once installed, invoke a skill by name in your conversation:

```text
/wren-usage
/wren-quickstart
/generate-mdl
/wren-project
/wren-sql
/wren-mcp-setup
```

> **Tip:** Use `--skill '*'` to install all skills at once, or specify individual skills (e.g., `--skill generate-mdl --skill wren-sql`).

## Available Skills

| Skill | Description |
|-------|-------------|
| [wren-usage](wren-usage/SKILL.md) | **Primary skill** — daily usage guide: query data, manage MDL, connect databases, operate MCP server |
| [wren-quickstart](wren-quickstart/SKILL.md) | End-to-end first-time setup — install skills, generate MDL, save project, start MCP server, verify setup |
| [generate-mdl](generate-mdl/SKILL.md) | Generate a Wren MDL manifest from a live database using ibis-server introspection |
| [wren-project](wren-project/SKILL.md) | Save, load, and build MDL manifests as version-controlled YAML project directories |
| [wren-sql](wren-sql/SKILL.md) | Write and correct SQL queries for Wren Engine — types, date/time, BigQuery dialect, error diagnosis |
| [wren-mcp-setup](wren-mcp-setup/SKILL.md) | Set up Wren Engine MCP via Docker, register with Claude Code or other MCP clients, and start querying |
| [wren-connection-info](wren-connection-info/SKILL.md) | Set up data source credentials — produces `connectionFilePath` or inline dict |

See [SKILLS.md](SKILLS.md) for full details on each skill.

## Updating Skills

Each skill automatically checks for updates when invoked. If a newer version is available, the AI agent will notify you with the update command before continuing.

To update manually at any time:

```bash
# Re-add to reinstall the latest version
npx skills add Canner/wren-engine --skill '*' --agent claude-code

# Or reinstall a specific skill
npx skills add Canner/wren-engine --skill generate-mdl --agent claude-code
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
     "generate-mdl": "1.2"
   }
   ```

Both files must have the same version number. The `SKILL.md` version is what users have installed locally; `versions.json` is what the update check compares against.

## Requirements

- A running [ibis-server](../ibis-server/) instance
- The [Wren MCP server](../mcp-server/) connected to your AI client
- An AI client that supports skills (Claude Code, Cline, Cursor, etc.)
