# CLI reference

## Default command — query

Running `wren --sql '...'` executes a query and prints the result. This is the same as `wren query --sql '...'`.

```bash
wren --sql 'SELECT COUNT(*) FROM "orders"'
wren --sql 'SELECT * FROM "orders" LIMIT 5' --output csv
wren --sql 'SELECT * FROM "orders"' --limit 100 --output json
```

Output formats: `table` (default), `csv`, `json`.

## `wren query`

Execute SQL and return results.

```bash
wren query --sql 'SELECT order_id, total FROM "orders" ORDER BY total DESC LIMIT 5'
```

## `wren dry-plan`

Translate MDL SQL to the native dialect SQL for your data source. No database connection required.

```bash
wren dry-plan --sql 'SELECT order_id FROM "orders"'
```

## `wren dry-run`

Validate SQL against the live database without returning rows. Prints `OK` on success.

```bash
wren dry-run --sql 'SELECT * FROM "orders" LIMIT 1'
```

## `wren validate`

Same as `dry-run` but prints `Valid` / `Invalid: <reason>`.

```bash
wren validate --sql 'SELECT * FROM "NonExistent"'
# Invalid: table not found ...
```

## Overriding defaults

All flags are optional when `~/.wren/mdl.json` and `~/.wren/connection_info.json` exist:

```bash
wren --sql '...' \
  --mdl /path/to/other-mdl.json \
  --connection-file /path/to/prod-connection_info.json \
  --datasource postgres
```

Or pass connection info inline:

```bash
wren --sql 'SELECT COUNT(*) FROM "orders"' \
  --connection-info '{"datasource":"mysql","host":"localhost","port":3306,"database":"mydb","user":"root","password":"secret"}'
```
