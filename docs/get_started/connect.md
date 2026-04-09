# Connect Your Database

This guide walks you through connecting Wren Engine to your own database — from creating a profile to running your first query. If you haven't installed the CLI yet, see [Installation](./installation.md) first.

---

## Step 1 — Install the data source extra

Each database requires its own connector. Install the extra for your data source:

```bash
pip install "wren-engine[postgres,main]"
```

Replace `postgres` with your data source (see [supported data sources](./installation.md#data-source-extras) for the full list). If you already installed with the correct extra, skip this step.

---

## Step 2 — Create a profile

A profile stores your database connection. The browser UI is the easiest way to create one:

```bash
wren profile add my-db --ui
```

This opens a form where you select the data source type and fill in the required connection fields.

Each data source has different fields. To see the exact fields for any data source:

```bash
wren docs connection-info --datasource postgres
```

Replace `postgres` with your data source name. The command prints all required and optional fields with descriptions.

**Example — PostgreSQL:**

```bash
wren profile add my-db --ui
# Select "postgres", then fill in: host, port, database, user, password
```

### Other options

If you prefer not to use the browser UI:

```bash
# Interactive prompts
wren profile add my-db --interactive

# Import from a JSON/YAML file
wren profile add my-db --from-file connection.json
```

See [Profiles](../guide/profiles.md) for all options.

---

## Step 3 — Verify the connection

```bash
wren profile debug    # show resolved config (secrets masked)
wren --sql "SELECT 1" # test actual connectivity
```

If the connection fails, check:

- **Credentials** — run `wren profile debug` to see what was saved (passwords are masked)
- **Network access** — can your machine reach the database host and port?
- **SSL** — some databases require SSL. Add the relevant SSL fields to your profile
- **Firewall / IP allowlist** — cloud databases (BigQuery, Snowflake, Redshift) may require your IP to be allowlisted

---

## Step 4 — Initialize a project

Create a directory for your MDL project:

```bash
mkdir ~/my-project && cd ~/my-project
wren context init
```

This scaffolds:

```
my-project/
├── wren_project.yml       # project metadata
├── models/                # one folder per model
├── views/                 # reusable SQL views
├── relationships.yml      # join definitions
└── instructions.md        # business rules for the AI
```

Remove the example placeholders:

```bash
rm -rf models/example_model views/example_view
```

---

## Step 5 — Generate MDL from your database

### With an AI agent (recommended)

Open your AI agent in the project directory and ask:

```
Use the wren-generate-mdl skill to explore my database and generate
the MDL for all tables. The data source is postgres.
```

The agent will introspect your schema, normalize types, write model files, infer relationships, and build the manifest.

### Manually

If you prefer manual control:

**a. Discover tables:**

```bash
wren --sql "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'" -o json
```

**b. Create a model file** for each table:

```yaml
# models/orders/metadata.yml
name: orders
table_reference:
  catalog: ""
  schema: public
  table: orders
primary_key: order_id
columns:
  - name: order_id
    type: INTEGER
    not_null: true
    is_primary_key: true
  - name: customer_id
    type: INTEGER
  - name: total
    type: "DECIMAL(10,2)"
  - name: status
    type: VARCHAR
    properties:
      description: "Order status: pending, shipped, delivered, cancelled"
  - name: order_date
    type: DATE
```

Use `wren utils parse-type` to normalize database-specific types:

```bash
wren utils parse-type --type "character varying(255)" --dialect postgres
# → VARCHAR(255)
```

**c. Define relationships** in `relationships.yml`:

```yaml
relationships:
  - name: orders_customers
    models:
      - orders
      - customers
    join_type: MANY_TO_ONE
    condition: orders.customer_id = customers.customer_id
```

**d. Validate and build:**

```bash
wren context validate
wren context build
```

---

## Step 6 — Index memory and start querying

```bash
wren memory index
wren memory status    # should show schema_items > 0
```

Now query your data:

```bash
wren --sql "SELECT * FROM orders LIMIT 5"
```

Or ask questions in natural language through your AI agent:

```
How many orders were placed last month?
```

---

## Adding descriptions

Good descriptions significantly improve AI query accuracy. Add them to models and columns via `properties.description`:

```yaml
name: orders
properties:
  description: "Customer orders with payment status and shipping details"
columns:
  - name: total
    type: "DECIMAL(10,2)"
    properties:
      description: "Net order total in USD after discounts, before tax"
```

After adding descriptions, rebuild and re-index:

```bash
wren context build
wren memory index
```

---

## Adding instructions

`instructions.md` contains business rules that guide the AI agent:

```markdown
## Naming conventions
- "revenue" always means net_total, not gross_total
- "active customers" = customers with at least one order in the last 90 days

## Query rules
- Always filter by status = 'completed' unless explicitly asked for all statuses
- Use order_date for time-based filtering, not created_at
```

Instructions are loaded by the agent at session start and included in memory search results.

---

## Common issues

**"table not found" after building:**
Model names in SQL must match the `name` field in your YAML, not the physical table name. Run `wren context show` to see available models.

**Wrong column types:**
Always normalize types through `wren utils parse-type --dialect <your_datasource>`. Raw database types like `int8` or `character varying` may not be recognized.

**Relationships not working:**
Check that both models have a `primary_key` defined. The `condition` must use `model_name.column_name` syntax on both sides.

**Memory returns irrelevant results:**
Re-index after every MDL change: `wren context build && wren memory index`. Add `properties.description` to improve search quality.

**Switching between databases:**
Create separate profiles for each database and switch with `wren profile switch <name>`. The same project can work with different profiles if the schema is compatible.
