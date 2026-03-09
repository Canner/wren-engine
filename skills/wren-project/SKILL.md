---
name: wren-project
description: Save, load, and build Wren MDL manifests as YAML project directories for version control. Use when a user wants to persist an MDL as human-readable YAML files, load a YAML project back into MDL JSON, or compile a YAML project to a deployable mdl.json file. Also manages connection info stored in connection.yml and compiled to target/connection.json.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.4"
---

# MDL Project

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-project` key with this skill's version (`1.4`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-project** skill is available (remote: X.Y, installed: 1.4).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-project
> ```



Then continue with the workflow below regardless of update status.

---

A Wren MDL project is a directory of YAML files — one file per model — that makes MDL manifests human-readable and version-control friendly (similar to dbt projects).

YAML files use **snake_case** field names for readability. The compiled `target/mdl.json` uses **camelCase** (the wire format expected by ibis-server). The conversion is documented in [Field mapping](#field-mapping).

## Project structure

```text
my_project/
├── wren_project.yml       # Project metadata (catalog, schema, data_source)
├── connection.yml         # Data source connection parameters
├── models/
│   ├── orders.yml         # One file per model
│   ├── customers.yml
│   └── ...
├── relationships.yml      # All relationships
└── views.yml              # All views
```

After building, compiled files are written to:
```
my_project/
└── target/
    ├── mdl.json           # Deployable MDL JSON (camelCase)
    └── connection.json    # Connection info JSON (camelCase)
```

> **Security note**: `connection.yml` may contain credentials. Add `target/` and `connection.yml` to `.gitignore` before committing.
>
> **Secrets policy (default)**: When generating `connection.yml`, leave sensitive fields empty with a `# TODO` comment — never ask the user for passwords or credentials in this conversation. ibis-server reads `target/connection.json` directly via its `connectionFilePath` parameter, so secrets stay in the file and out of the LLM context.
>
> **Testing / inline mode (opt-in)**: If the user explicitly says they are in a test/development environment and willing to share their credentials, you may help construct the full `connection.yml` or `connectionInfo` dict with actual values inline. Always confirm this intent before proceeding — do not assume.

---

## Save MDL JSON → YAML project

Given an MDL JSON dict (camelCase), write it as a YAML project directory (snake_case):

### `wren_project.yml`

```yaml
name: my_project
version: "1.0"
catalog: wren
schema: public
data_source: POSTGRES
connection_mode: security   # "security" | "inline" (default: "security")
```

`connection_mode` records how the user has chosen to manage connection credentials:

| Value | Meaning |
|-------|---------|
| `security` | `connection.yml` / `target/connection.json` contain sensitive credentials. **Never read these files without explicit user confirmation.** |
| `inline` | Credentials are provided inline (test/dev environment). Connection files may be read freely. |

**Security mode rules** — enforced whenever `connection_mode: security` (or the field is absent):

1. **Never** read `connection.yml` or `target/connection.json` without first asking the user for permission.
2. **Never** display, log, or echo the contents of those files.
3. If debugging requires connection info, ask the user to share only the non-sensitive fields (e.g. `host`, `port`, `database`, `user`) and leave out passwords, tokens, and keys.
4. After building the project, do **not** read `target/connection.json` to verify — confirm success by other means (e.g. checking that the file exists).

### `models/<model_name>.yml`

One file per model. Example for `orders`:

```yaml
name: orders
table_reference:
  catalog: ""
  schema: public
  table: orders
columns:
  - name: order_id
    type: INTEGER
    is_calculated: false
    not_null: true
    is_primary_key: true
    properties: {}
  - name: customer_id
    type: INTEGER
    is_calculated: false
    not_null: false
    properties: {}
  - name: total
    type: DECIMAL
    is_calculated: false
    not_null: false
    properties: {}
primary_key: order_id
cached: false
properties: {}
```

### `relationships.yml`

```yaml
relationships:
  - name: orders_customer
    models:
      - orders
      - customers
    join_type: MANY_TO_ONE
    condition: orders.customer_id = customers.customer_id
```

### `views.yml`

```yaml
views: []
```

### `connection.yml`

Connection parameters for the data source. Field names use **snake_case** in YAML and are converted to **camelCase** in `target/connection.json`.

Follow the **wren-connection-info** skill (`skills/wren-connection-info/SKILL.md`) for the per-connector field reference, secrets policy, and how to generate `connection.yml` with sensitive fields left as `# TODO` comments.

---

## Load YAML project → MDL JSON

To assemble a YAML project back into an MDL JSON dict:

1. Read `wren_project.yml` → extract `catalog`, `schema`, `data_source`, `connection_mode`
2. Read every file in `models/*.yml` → collect into `models` list
3. Read `relationships.yml` → extract `relationships` list
4. Read `views.yml` → extract `views` list
5. **Rename snake_case keys to camelCase** (see Field mapping section below)
6. Assemble:

```json
{
  "catalog": "<from wren_project.yml>",
  "schema": "<from wren_project.yml>",
  "dataSource": "<from wren_project.yml data_source>",
  "models": [...],
  "relationships": [...],
  "views": []
}
```

To load connection info:

1. Check `connection_mode` from `wren_project.yml`. If it is `security` (or absent), **ask the user for permission before reading `connection.yml`**.
2. Read `connection.yml`
3. Resolve any `${VAR_NAME}` placeholders from environment variables
4. **Rename snake_case keys to camelCase** (see Field mapping section below)
5. Result is a flat JSON object ready to pass as `connectionInfo` to ibis-server APIs

---

## Build YAML project → `target/`

Same as **Load** above, but write both compiled files:

- `<project_dir>/target/mdl.json` — assembled MDL JSON (camelCase)
- `<project_dir>/target/connection.json` — connection info JSON (camelCase)

**Important**: If `connection_mode` is `security` (or absent), do NOT read `connection.yml` without user confirmation, and do NOT read or display `target/connection.json` after building — it contains credentials.

After building:
- Pass `mdl_file_path="<project_dir>/target/mdl.json"` to `deploy()` to activate the MDL
- Pass `connectionFilePath="<absolute_path>/target/connection.json"` in API requests instead of inline `connectionInfo`
  — ibis-server reads the file directly, so secret values never enter the LLM context

---

## Field mapping

When converting between YAML (snake_case) and JSON (camelCase):

**MDL fields:**

| YAML field (snake_case) | JSON field (camelCase) |
|-------------------------|------------------------|
| `data_source` | `dataSource` |
| `table_reference` | `tableReference` |
| `is_calculated` | `isCalculated` |
| `not_null` | `notNull` |
| `is_primary_key` | `isPrimaryKey` |
| `primary_key` | `primaryKey` |
| `join_type` | `joinType` |

All other MDL fields (`name`, `type`, `catalog`, `schema`, `table`, `condition`, `models`, `columns`, `cached`, `properties`) are the same in both formats.

**Connection fields:**

See the **wren-connection-info** skill (`skills/wren-connection-info/SKILL.md`) for the full field mapping and secrets policy.

---

## Typical workflow

```
1. Set up data source and connection info
   Follow the wren-connection-info skill to choose data source type, gather credentials,
   and produce connection.yml (sensitive fields as # TODO) + target/connection.json.
   Use connectionFilePath="<abs_path>/target/connection.json" in all subsequent API calls.

2. Generate MDL
   Follow the generate-mdl skill to introspect the database and build the MDL JSON dict,
   using the connectionFilePath from step 1 for all ibis-server calls.

3. Save project
   Write wren_project.yml + models/*.yml + relationships.yml + views.yml
   (convert camelCase → snake_case from the MDL JSON).
   Set connection_mode in wren_project.yml based on the user's chosen mode (default: security).
   connection.yml was already written in step 1.

4. Add target/ and connection.yml to .gitignore
5. Commit project directory to version control (without secrets)

6. Later — Build: read wren_project.yml first to check connection_mode.
                   If security mode, ask user before reading connection.yml.
                   Read remaining YAML files, rename snake_case → camelCase,
                   write target/mdl.json and target/connection.json.
                   Do NOT read or display target/connection.json (security mode).

7. Deploy: deploy(mdl_file_path="./target/mdl.json")
           use connectionFilePath="<absolute_path>/target/connection.json" in API requests
           (ibis-server reads the file directly — secrets stay out of this conversation)
```
