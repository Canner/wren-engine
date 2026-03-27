# Wren Engine Skills

Claude Code skills for the Wren Engine CLI.

## Available skills

| Skill | Trigger | Description |
|-------|---------|-------------|
| [`wren-query`](wren-query/SKILL.md) | `/wren-query [sql]` | Run, dry-run, or validate a SQL query through the Wren semantic CLI |

## Usage

Skills are invoked via slash commands in Claude Code:

```
/wren-query SELECT order_id FROM "orders" LIMIT 5
/wren-query --dry-plan SELECT * FROM "orders"
/wren-query --validate SELECT * FROM "NonExistent"
```

See each skill's `SKILL.md` for full details.
