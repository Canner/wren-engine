# dbt Integration Plan

Branch: `codex/dbt-integration`

Goal: deliver the beta dbt import flow in small, testable checkpoints and
pause after step 3 so the generated project can be validated with
`jaffle_shop_duckdb`.

## Step 0: Planning

- Record the implementation plan in the repository.
- Keep all work on `codex/dbt-integration`.
- Make one commit per completed step.

## Step 1: dbt Artifact Loading

Scope:

- Add a `wren.dbt` module for reading dbt artifacts and profile configuration.
- Support loading:
  - `manifest.json`
  - `catalog.json`
  - optional `run_results.json`
  - compiled SQL under `target/compiled/`
  - dbt `profiles.yml`
- Normalize adapter names and selected target resolution.
- Surface clear validation errors for missing or malformed artifacts.

Deliverables:

- Reusable artifact loader APIs.
- Unit tests for happy paths and partial/missing artifact cases.

Commit:

- Commit once the loader APIs and tests are passing.

## Step 2: `wren profile import dbt`

Scope:

- Add a CLI command to import the active dbt target into `~/.wren/profiles.yml`.
- Map dbt adapter types to Wren datasource names.
- Resolve `env_var()` references from the current environment.
- Write or update the selected Wren profile and optionally activate it.

Deliverables:

- `wren profile import dbt`
- Tests for adapter mapping, env var resolution, and profile persistence.

Commit:

- Commit once the CLI command and tests are passing.

## Step 3: `wren context import dbt`

Scope:

- Add a CLI command to generate a Wren project from dbt artifacts.
- Support `--project-dir`, `--dry-run`, and `--force`.
- Generate:
  - `wren_project.yml`
  - `models/*/metadata.yml`
  - `relationships.yml`
  - `instructions.md`
  - `AGENTS.md`
  - `queries.yml`
- Bind the imported project back to the dbt target/project in project config.

Deliverables:

- Base import flow that produces a buildable Wren project.
- Tests for file generation, dry-run previews, and project overwrite rules.

Testing checkpoint:

- Pause here for manual validation with `jaffle_shop_duckdb`.
- Expected manual flow:
  - `wren profile import dbt`
  - `wren context import dbt --project-dir ./jaffle_shop_duckdb`
  - `wren context build`
  - `wren --sql "SELECT * FROM fct_orders LIMIT 5"`

Commit:

- Commit once the generated project builds successfully and tests pass.

## Step 4: Semantic Enrichment from dbt Tests and Metadata

Scope:

- Skip ephemeral nodes.
- Detect raw, staging, intermediate, and mart layers.
- Apply `catalog.json` type overrides.
- Map dbt tests into Wren metadata:
  - `not_null`
  - `unique` + `not_null` => primary key
  - `relationships`
  - `accepted_values`
- Write test-result warnings and verification notes into `instructions.md`.

Deliverables:

- Richer imported metadata and relationship generation.
- Tests for test-node mapping and instructions output.

Commit:

- Commit once the semantic import mapping is covered by fixtures and tests.

## Step 5: Memory Integration for dbt Projects

Scope:

- Enrich memory indexing text for dbt-imported projects.
- Generate dbt-derived NL-SQL examples into `queries.yml`.
- Fit the implementation into the new upstream memory workflow where
  `wren memory index` auto-loads `queries.yml`.

Deliverables:

- dbt-aware schema text enrichment.
- dbt-generated query seeds that flow through `queries.yml`.
- Tests for indexing and query seed generation.

Commit:

- Commit once the dbt memory flow works with `wren memory index`.

## Step 6: Docs and Polish

Scope:

- Update CLI docs for the new dbt commands.
- Add usage notes for the `jaffle_shop_duckdb` validation path.
- Clean up command help text and error messaging.

Deliverables:

- User-facing documentation for the beta workflow.

Commit:

- Commit once the docs and final polish are in place.
