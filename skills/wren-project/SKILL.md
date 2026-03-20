---
name: wren-project
description: Save, load, and build Wren MDL manifests as YAML project directories for version control. Use when a user wants to persist an MDL as human-readable YAML files, load a YAML project back into MDL JSON, or compile a YAML project to a deployable mdl.json file.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.5"
---

# MDL Project

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-project` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-project** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-project --agent claude-code
> ```



Then continue with the workflow below regardless of update status.

---

A Wren MDL project is a directory of YAML files — one file per model — that makes MDL manifests human-readable and version-control friendly (similar to dbt projects).

YAML files use **snake_case** field names for readability. The compiled `target/mdl.json` uses **camelCase** (the wire format expected by ibis-server). The conversion is documented in [Field mapping](#field-mapping).

## Project structure

```text
my_project/
├── wren_project.yml       # Project metadata (catalog, schema, data_source)
├── models/
│   ├── orders.yml         # One file per model
│   ├── customers.yml
│   └── ...
├── relationships.yml      # All relationships
└── views.yml              # All views
```

After building, the compiled file is written to:
```text
my_project/
└── target/
    └── mdl.json           # Deployable MDL JSON (camelCase)
```

> **Note**: Connection info is managed separately via the MCP server Web UI, not stored in the project directory.

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
```

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

---

## Load YAML project → MDL JSON

To assemble a YAML project back into an MDL JSON dict:

1. Read `wren_project.yml` → extract `catalog`, `schema`, `data_source`
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

---

## Build YAML project → `target/`

Same as **Load** above, but write the compiled file:

- `<project_dir>/target/mdl.json` — assembled MDL JSON (camelCase)

After building:
- Pass `mdl_file_path="<project_dir>/target/mdl.json"` to `deploy()` to activate the MDL
- Connection info is managed via the MCP server Web UI — no file needed

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

---

## Typical workflow

```
1. Generate MDL
   Follow the generate-mdl skill to introspect the database and build the MDL JSON dict.

2. Save project
   Write wren_project.yml + models/*.yml + relationships.yml + views.yml
   (convert camelCase → snake_case from the MDL JSON).

3. Add target/ to .gitignore
4. Commit project directory to version control

5. Later — Build: read wren_project.yml, then models/*.yml, relationships.yml, views.yml.
                   Rename snake_case → camelCase, write target/mdl.json.

6. Connection info: configure via the MCP server Web UI
                   (typically http://localhost:9001; use the Docker host hint when running in a container)

7. Deploy: deploy(mdl_file_path="./target/mdl.json")
```
