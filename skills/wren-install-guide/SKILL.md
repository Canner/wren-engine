---
name: wren-install-guide
description: "Install Wren Engine end-to-end. Walks the user through environment checks, project scaffolding, connection configuration via .env, and first query. Use when: user wants to install Wren Engine, set up a new data source connection, or bootstrap a new project from scratch. Triggers: '/wren-install-guide', 'install wren', 'set up wren engine', 'wren onboarding', 'connect new database to wren'."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Install Wren Engine — CLI Agent Workflow

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-install-guide` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-install-guide** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-install-guide
> ```
> The CLI auto-detects your installed agent. To target a specific one, add `--agent <name>` (e.g., `claude-code`, `cursor`, `windsurf`, `cline`).

Then continue with the workflow below regardless of update status.

---

## Mode of operation — READ THIS FIRST

**One Step per round-trip with the user.**  Each numbered step below
is its own turn: explain briefly what the step does, ask **only** for
the information that step needs, run the command(s), confirm success,
move on.

Concretely:

- ❌ **Never collect information for future steps upfront.**  If you
  find yourself asking for the project name *and* the MySQL host *and*
  the password in the same message, stop — you are flattening the
  step boundaries and losing the benefit of this skill.
- ❌ **Do not dump the whole flow as a checklist** before starting.
  Walk it, don't narrate it.
- ✅ Name only the current step when you start it, and one-sentence
  hint at the next when it finishes.
- ✅ Wait for each command to finish and report its output in plain
  language before asking the next question.

Preflight is environment checks only — no project or credential
questions there.  After Preflight the flow is: a single demo-or-own
branch, one combined "project name + DS" question, a batch that
installs + scaffolds + writes a `.env` template, user edits `.env`,
then profile validation, then MDL generation.

---

## Preflight (environment only — no user questions about the project)

**Side effects**: None.  Read-only checks.

Report the findings of the checks below.  Do **not** ask the user
about project name, datasource, credentials, or anything else during
Preflight — those belong to their own steps.

1. `python3 --version` — requires Python 3.11 or newer.  If older,
   ask the user to upgrade and stop.

2. **Check if the user is in a Python virtual environment**:

        python3 -c "import sys; print(sys.prefix != sys.base_prefix)"

   - `True` → already in a venv, continue.
   - `False` → ask the user:
     > "You're on system Python.  On macOS (Homebrew) and Ubuntu
     > 22.04+, `pip install` may fail with PEP 668 errors.  Shall I
     > create a venv for this project?
     > - **Yes (recommended)**: `python3 -m venv .venv && source .venv/bin/activate`
     > - **No, I use uv / poetry / pyenv / conda**: skip (but activate yours first)
     > - **No, proceed anyway**: may fail — you'll need `--break-system-packages`"

     If the user picks **Yes**, run the venv commands and re-check
     the same `sys.prefix` probe to confirm activation before
     moving on.

3. `wren --version` — if already installed, ask the user whether to
   continue with the existing install or reinstall fresh.

4. `pwd` — record it for context only.  **Do not ask yet** where the
   project should live; that is part of the project-setup step.

When Preflight is done, report the four findings in a short list and
move on to the demo-or-own branch below.

---

## Early branch — demo or own database?

Ask this one question on its own before any installation step runs:

> "Do you want to try the bundled `jaffle_shop` demo first (no
> database needed, ~30 seconds to first query), or set up a
> connection to your own database?"

- If **demo** → point the user at
  [docs/get_started/quickstart.md](https://github.com/Canner/wren-engine/blob/main/docs/get_started/quickstart.md)
  and stop this skill.  The quickstart walks the `jaffle_shop`
  end-to-end demo in detail.
- If **own DB** → continue to the next step.

---

## Step 1 — Collect project name + database type

**Side effects**: None yet — this step is pure questioning.

**Ask both in one message.**  The next step (project setup) needs
both answers before it can run, so they go together:

> "I need two things before I scaffold the project:
>
> 1. **Project name** — I'll create `~/<project-name>` and `cd` into
>    it.  It must be outside the wren-engine source repository.
> 2. **Database type** — which source do you want to connect to?
>    Supported options:
>
>    `duckdb` (local file) · `postgres` · `mysql` · `bigquery` ·
>    `snowflake` · `clickhouse` · `trino` · `mssql` · `databricks` ·
>    `redshift` · `oracle` · `athena` · `spark`"

Wait for both answers before proceeding.  Do **not** ask for
credentials in this step — that happens through `.env`, not chat.

---

## Step 2 — Project setup (batch)

**Side effects**:
- Creates `~/<project-name>/` and `cd`s into it.
- Installs Python packages via pip (`wren-engine[<ds>,main]`).
- Writes `wren_project.yml`, `models/`, `views/`, `relationships.yml`,
  `instructions.md`, `AGENTS.md`, `queries.yml`.
- Writes `~/<project-name>/.env` (empty template based on DS).

Run the whole batch without asking the user anything in between.
Report each command's output briefly, then end with a single "please
fill the `.env`" ask.

1. **Make project directory and enter it**

        mkdir -p ~/<project-name> && cd ~/<project-name>

   If the path already exists and contains a `wren_project.yml`,
   stop and ask the user whether to pick a different name or
   continue in-place.  Never overwrite an existing project silently.

2. **Install wren-engine with the right extras**

        pip install "wren-engine[<ds>,main]"

   For `duckdb` the extra is just `[main]` — DuckDB ships with the
   core wheel.

   **macOS mysql special case**: if `<ds>` is `mysql` and `pip`
   fails with a build error about `mysql-client`, ask the user to
   run:

        brew install mysql-client pkg-config
        export PKG_CONFIG_PATH="$(brew --prefix mysql-client)/lib/pkgconfig"

   Use `brew list mysql-client` to verify the keg is installed —
   `brew --prefix mysql-client` prints a path even when the
   package is missing, which is misleading.  Wait for the user to
   confirm, then retry the `pip install`.

3. **Scaffold the project (skip the placeholder example)**

        wren context init --empty

   Then edit `wren_project.yml` with your editor tool: change the
   `data_source:` line to match the chosen DS (it defaults to
   `postgres`):

        data_source: <ds>

4. **Write the `.env` template** using your file-write tool.  The
   exact keys depend on the DS.  Use UPPER_SNAKE_CASE prefix
   matching the DS so multi-profile setups stay unambiguous.

   Use these templates — fill the `<path>` and DS name; leave the
   values **empty** so the user fills them in:

   ```ini
   # postgres
   POSTGRES_HOST=
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=
   POSTGRES_USER=
   POSTGRES_PASSWORD=
   ```

   ```ini
   # mysql
   MYSQL_HOST=
   MYSQL_PORT=3306
   MYSQL_DATABASE=
   MYSQL_USER=
   MYSQL_PASSWORD=
   ```

   ```ini
   # bigquery
   BIGQUERY_PROJECT_ID=
   # base64-encoded service-account JSON; run:
   #   base64 -i /path/to/credentials.json | pbcopy   (macOS)
   #   base64 /path/to/credentials.json                (Linux)
   BIGQUERY_CREDENTIALS=
   ```

   ```ini
   # snowflake
   SNOWFLAKE_ACCOUNT=
   SNOWFLAKE_USER=
   SNOWFLAKE_PASSWORD=
   SNOWFLAKE_WAREHOUSE=
   SNOWFLAKE_DATABASE=
   SNOWFLAKE_SCHEMA=
   ```

   ```ini
   # clickhouse
   CLICKHOUSE_HOST=
   CLICKHOUSE_PORT=8123
   CLICKHOUSE_DATABASE=
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=
   ```

   ```ini
   # trino
   TRINO_HOST=
   TRINO_PORT=8080
   TRINO_CATALOG=
   TRINO_SCHEMA=
   TRINO_USER=
   TRINO_PASSWORD=
   ```

   ```ini
   # mssql
   MSSQL_HOST=
   MSSQL_PORT=1433
   MSSQL_DATABASE=
   MSSQL_USER=
   MSSQL_PASSWORD=
   ```

   ```ini
   # redshift
   REDSHIFT_HOST=
   REDSHIFT_PORT=5439
   REDSHIFT_DATABASE=
   REDSHIFT_USER=
   REDSHIFT_PASSWORD=
   ```

   ```ini
   # oracle
   ORACLE_HOST=
   ORACLE_PORT=1521
   ORACLE_DATABASE=
   ORACLE_USER=
   ORACLE_PASSWORD=
   ```

   ```ini
   # databricks
   DATABRICKS_SERVER_HOSTNAME=
   DATABRICKS_HTTP_PATH=
   DATABRICKS_ACCESS_TOKEN=
   ```

   ```ini
   # athena
   ATHENA_S3_STAGING_DIR=
   ATHENA_REGION_NAME=
   ATHENA_AWS_ACCESS_KEY_ID=
   ATHENA_AWS_SECRET_ACCESS_KEY=
   ```

   ```ini
   # spark
   SPARK_HOST=
   SPARK_PORT=
   ```

   ```ini
   # duckdb (just a local file path, no credentials)
   DUCKDB_URL=
   DUCKDB_FORMAT=duckdb
   ```

   If the DS isn't in the list above, run
   `wren docs connection-info <ds>` (positional argument, **not**
   `--datasource`) to see the real field names, then write the
   template yourself.

5. **Tell the user all in one message**:

   > "Done with setup:
   > - Project at `~/<project-name>/`
   > - Installed `wren-engine[<ds>,main]`
   > - Scaffolded an empty wren project (no placeholder models)
   > - Wrote a `.env` template at `~/<project-name>/.env`
   >
   > Please open `.env` in your editor, fill in every value, save,
   > and say **'done'**."

Also remind them to add `.env` to `.gitignore` if the directory is
a git repo (`echo '.env' >> .gitignore`).  On Unix, `chmod 600 .env`
restricts read access to the current user.

---

## Step 3 — Create the connection profile

**Side effects**: Writes to `~/.wren/profiles.yml`.  Reads `.env` at
validation time.

Only run this step after the user has replied "done".

1. Write `/tmp/conn.yml` where **every field is a `${VAR}`
   reference** (not a value).  Example for postgres:

        datasource: postgres
        host: ${POSTGRES_HOST}
        port: ${POSTGRES_PORT}
        database: ${POSTGRES_DATABASE}
        user: ${POSTGRES_USER}
        password: ${POSTGRES_PASSWORD}

   Match the placeholder names to the `.env` keys you wrote in
   Step 2.  Port values are strings in the YAML schema even when
   numeric — quoting happens automatically because `${…_PORT}` is
   a string placeholder.

2. Run:

        wren profile add <project-name> --from-file /tmp/conn.yml

   `wren profile add` accepts flat keys or a `properties:` /
   `connection:` / `config:` envelope.  Stick to the flat form
   shown above.

3. Validation runs automatically.

   - ✓ **Success** → move on to Step 4.
   - ⚠ **Missing secret** (`MissingSecretError: ... is not set in
     the environment or any discovered .env file.`) → the user
     left a `.env` value blank.  Show the variable name, ask them
     to fill it in, then re-run:

          wren profile add <project-name> --from-file /tmp/conn.yml

     `wren profile add` overwrites existing profiles silently, so no
     `--force` flag is needed.
   - ⚠ **Driver error** (e.g. MySQL `1044 Access denied`, Postgres
     `password authentication failed`, `Host unreachable`) → show
     the driver error verbatim.  Diagnose when possible (firewall,
     IP allowlist for cloud DBs, wrong DB name).  Ask the user to
     fix `.env`, then re-run the same `wren profile add` command.
   - ⚠ **Invalid profile** (Pydantic `ValidationError`) → the
     template is missing a required field.  Regenerate the `.env`
     template with the missing key and re-run.

   The profile stays on disk regardless of validation result;
   `wren profile debug <project-name>` shows the `${VAR}`
   placeholders (never the resolved secrets).

---

## Step 4 — Generate MDL using the `wren-generate-mdl` skill

> ⚠️ **IMPORTANT**: You **must** invoke the `wren-generate-mdl` skill
> to build the MDL **before** attempting to query or explore the
> database.  Queries against tables that aren't defined in the MDL
> will fail.

> **The skill is a playbook, not an autopilot.**  It tells **you**
> (the agent) what steps to follow — it does not do the work for
> you.  You'll run the SQL introspection yourself (SQLAlchemy, the
> native driver, or raw `wren --sql` queries), interpret the types,
> and write the YAML files.  Treat the skill as a structured
> checklist.

**Side effects**: Writes YAML files in `models/` and
`relationships.yml`; compiles `target/mdl.json`.

Invoke the `wren-generate-mdl` skill.  The skill walks you through:

1. Connecting to the user's database (via the profile from Step 3)
2. Listing tables and views
3. Introspecting columns and types
4. Generating model YAML files
5. Inferring relationships from foreign keys and naming conventions

Then validate and build:

        wren context validate
        wren context build

Report to the user:
- Number of models generated
- Any warnings from `validate`

### Decision: should we enable memory?

Count the generated models:

        wren context show | grep -c '^model:'

- **If count ≥ 200**: recommend memory to the user.
  > "Your schema has <N> models.  Enabling semantic memory helps me
  > find the right tables quickly when you ask questions.  This adds
  > about 800 MB to your install (PyTorch + sentence-transformers)."

  If they say yes:

          pip install "wren-engine[memory]"
          wren memory index

- **If count < 200**: skip memory.  Tell the user they can enable it
  later if the schema grows or query accuracy suffers.

- **If the user asks for memory at any point**, install it regardless
  of model count.

---

## Step 5 — Ready to explore

**Only after Step 4 is complete** may you answer data questions.

Tell the user they can now ask questions in natural language, and
suggest 2–3 examples based on the tables you discovered.  For an
orders/customers schema:

- "How many orders were placed last month?"
- "Top 5 customers by total order amount"
- "Monthly trend of order count"

End the `wren-install-guide` skill here.  For day-to-day querying
the agent should switch to the `wren-usage` skill.

---

## Error handling playbook

### `wren: command not found`

Package installed but not on PATH.  Check `pip show wren-engine`
for the install location; verify the `bin/` directory is on PATH
(or the venv is active).

### `pip install` fails with "externally-managed-environment"

PEP 668-protected Python.  Go back to Preflight step 2 and create
a venv first.

### Connection validation keeps failing

Walk the user through diagnostics:
1. Can they reach the host?  (`ping <host>` or `nc -zv <host> <port>`)
2. Are credentials correct?  (`wren profile debug <name>`)
3. Firewall / VPN in the way?
4. Cloud DBs (BigQuery, Snowflake, Redshift): is their IP
   allow-listed?

### `wren profile add` swallows driver errors

As of wren-engine 0.3.1+, driver errors surface verbatim.  If you
see `'dict' object has no attribute 'kwargs'`, the install is stale
— tell the user to upgrade: `pip install --upgrade "wren-engine[main]"`.

### macOS mysql build error

Show the `brew install mysql-client pkg-config` commands from
Step 2.  Wait for the user to confirm, then retry the `pip install`.

### `wren context validate` warnings

`wren context validate` groups warnings by category once there are
more than 10 — pass `--verbose` to see each line.  Show the summary
to the user and only drill into specifics that matter.

Common categories:

- **Missing description** — cosmetic; agents can still answer
  questions without it.  Skip unless the user asks for cleanup.
- **Missing `primary_key`** — **not always a bug**.  Junction /
  log-style tables legitimately have no PK.
- **Invalid relationship condition** — real bug.  Regenerate via
  `wren-generate-mdl` or have the user fix manually.

### Unknown errors

Tell the user: "I hit an error I don't know how to fix: `<error>`.
You may want to check <https://docs.getwren.ai/oss/engine> or open
an issue at <https://github.com/Canner/wren-engine/issues>."

---

## Rules of engagement

### Do

- ✅ **Work one step at a time.**  Each step owns its own question;
  the Mode of operation section above is non-negotiable.
- ✅ Always ask before running commands that have side effects.
- ✅ Always report command output to the user in plain language.
- ✅ Always preview destructive commands before running them (show
  the exact command).
- ✅ Always validate connections (default `--validate` = on).
- ✅ Always use `wren-generate-mdl` to build MDL before exploring
  data.

### Don't

- ❌ **Never batch questions across steps.**  Asking for the project
  path + MySQL host + user + password in one message flattens the
  step boundaries and defeats the point of this skill.
- ❌ **Never ask for connection fields in chat — not even the
  "non-secret" ones.**  Host, port, user, password all go through
  `.env`.  Splitting "ask host in chat, ask password via `.env`" is
  a half-measure that trains users to feel sharing partial
  credentials is okay.
- ❌ Never assume credentials — always have the user supply them via
  `.env`.
- ❌ Never install memory by default — only when model count ≥ 200
  or the user asks.
- ❌ Never skip `wren context validate`.
- ❌ Never run commands outside the project directory once you're
  in Step 2 or later.
- ❌ Never query the database before the MDL is built via
  `wren-generate-mdl`.
- ❌ Never create a profile with `--no-validate` unless the user
  explicitly opts out.
