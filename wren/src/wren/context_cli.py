"""Typer sub-app for ``wren context`` commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

context_app = typer.Typer(
    name="context",
    help="Manage MDL context — models, views, relationships, and instructions.",
)


ProjectPathOpt = Annotated[
    Optional[str],
    typer.Option(
        "--path",
        "-p",
        help="Project directory. Auto-detected via WREN_PROJECT_HOME, cwd walk, or ~/.wren/config.yml.",
    ),
]


@context_app.command()
def init(
    path: ProjectPathOpt = None,
    from_mdl: Annotated[
        Optional[str],
        typer.Option("--from-mdl", help="Import from MDL JSON file (camelCase)."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite existing project files."),
    ] = False,
) -> None:
    """Initialize a new Wren project.

    Without --from-mdl: scaffolds an empty project structure.
    With --from-mdl: imports an existing MDL JSON and produces a complete
    v2 YAML project, ready for `wren context validate/build`.
    """
    project_path = Path(path).expanduser() if path else Path.cwd()

    if from_mdl:
        # ── Import from MDL JSON ──────────────────────────────
        from wren.context import (  # noqa: PLC0415
            convert_mdl_to_project,
            write_project_files,
        )

        mdl_path = Path(from_mdl).expanduser()
        if not mdl_path.exists():
            typer.echo(f"Error: {mdl_path} not found.", err=True)
            raise typer.Exit(1)

        mdl_json = json.loads(mdl_path.read_text())
        files = convert_mdl_to_project(mdl_json)
        try:
            write_project_files(files, project_path, force=force)
        except SystemExit as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(1)

        model_count = len(mdl_json.get("models", []))
        view_count = len(mdl_json.get("views", []))
        rel_count = len(mdl_json.get("relationships", []))

        typer.echo(f"Imported MDL to YAML project at {project_path}/")
        typer.echo(
            f"  {model_count} models, {view_count} views, {rel_count} relationships"
        )
        typer.echo("\nNext steps:")
        typer.echo(f"  wren context validate --path {project_path}")
        typer.echo(f"  wren context build --path {project_path}")
        return

    # ── Scaffold empty project (existing behavior) ────────────
    project_file = project_path / "wren_project.yml"
    agents_file = project_path / "AGENTS.md"
    queries_file = project_path / "queries.yml"
    conflicts = [f for f in (project_file, agents_file, queries_file) if f.exists()]
    if conflicts and not force:
        names = ", ".join(f"'{c.name}'" for c in conflicts)
        typer.echo(
            f"Error: {names} already exists. Use --force to overwrite.",
            err=True,
        )
        raise typer.Exit(1)

    # Create directory structure
    (project_path / "models").mkdir(parents=True, exist_ok=True)
    (project_path / "views").mkdir(parents=True, exist_ok=True)

    # wren_project.yml
    project_yml = (
        "schema_version: 2\n"
        "name: my_project\n"
        'version: "1.0"\n'
        "\n"
        "# Wren Engine namespace (NOT your database's catalog/schema).\n"
        "# These identify this MDL project within the engine.\n"
        "# Your database's actual catalog/schema goes in each model's table_reference.\n"
        "catalog: wren\n"
        "schema: public\n"
        "\n"
        "data_source: postgres  # change to your datasource type\n"
    )
    project_file.write_text(project_yml)

    # Scaffold example model (table_reference mode)
    example_model_dir = project_path / "models" / "example"
    example_model_dir.mkdir(parents=True, exist_ok=True)
    (example_model_dir / "metadata.yml").write_text(
        "# Example model — replace with your actual table\n"
        "name: example\n"
        "# table_reference points to the ACTUAL database table location\n"
        "table_reference:\n"
        '  catalog: ""        # your database catalog (empty if N/A)\n'
        "  schema: public      # your database schema\n"
        "  table: example      # your database table name\n"
        "columns:\n"
        "  - name: id\n"
        "    type: INTEGER\n"
        "    is_calculated: false\n"
        "    not_null: true\n"
        "    is_primary_key: true\n"
        "    properties: {}\n"
        "  - name: name\n"
        "    type: VARCHAR\n"
        "    is_calculated: false\n"
        "    not_null: false\n"
        "    properties: {}\n"
        "primary_key: id\n"
        "cached: false\n"
        "properties: {}\n"
    )

    # Empty relationships.yml
    rels = (
        "relationships: []\n"
        "# Example:\n"
        "# relationships:\n"
        "#   - name: orders_customers\n"
        "#     models:\n"
        "#       - orders\n"
        "#       - customers\n"
        "#     join_type: MANY_TO_ONE\n"
        "#     condition: orders.customer_id = customers.customer_id\n"
    )
    (project_path / "relationships.yml").write_text(rels)

    # Scaffold example view
    example_view_dir = project_path / "views" / "example_view"
    example_view_dir.mkdir(parents=True, exist_ok=True)
    (example_view_dir / "metadata.yml").write_text(
        "# Example view — replace with your actual view\n"
        "name: example_view\n"
        "properties:\n"
        '  description: "An example view"\n'
    )
    (example_view_dir / "sql.yml").write_text(
        "statement: >\n  SELECT * FROM example LIMIT 100\n"
    )

    # Instructions placeholder
    (project_path / "instructions.md").write_text(
        "# User Instructions\n\n"
        "Add custom rules or guidelines for LLM-based query generation here.\n"
    )

    # ── AGENTS.md ──
    from wren.context import _AGENTS_MD_TEMPLATE  # noqa: PLC0415

    (project_path / "AGENTS.md").write_text(_AGENTS_MD_TEMPLATE)

    # Curated NL-SQL pairs (auto-loaded by `wren memory index`)
    (project_path / "queries.yml").write_text(
        "# Curated NL-SQL pairs for this project.\n"
        "# These are auto-loaded into memory on `wren memory index`.\n"
        "# Use `wren memory dump` to export pairs from memory to this file.\n"
        "# Format: same as `wren memory dump` output.\n"
        "version: 1\n"
        "pairs: []\n"
    )

    typer.echo(f"Wren project initialized: {project_path}")
    typer.echo("  wren_project.yml            — project metadata (edit data_source)")
    typer.echo("  models/example/             — example model (metadata.yml)")
    typer.echo("  views/example_view/         — example view (metadata.yml + sql.yml)")
    typer.echo("  relationships.yml           — define joins between models")
    typer.echo("  instructions.md             — LLM instructions")
    typer.echo("  AGENTS.md                   — AI agent workflow guidance")
    typer.echo("  queries.yml                 — curated NL-SQL pairs for memory")
    typer.echo("\nNext: edit your models, then run `wren context build`.")


@context_app.command()
def validate(
    path: ProjectPathOpt = None,
    strict: Annotated[
        bool,
        typer.Option("--strict", help="Treat warnings as errors."),
    ] = False,
    level: Annotated[
        str,
        typer.Option(
            "--level",
            help="Semantic check depth: error (dry-plan only), warning (+ descriptions), strict (+ columns).",
        ),
    ] = "warning",
) -> None:
    """Validate MDL project: YAML structure + view SQL dry-plan + description checks."""
    import base64 as _b64  # noqa: PLC0415

    from wren.context import (  # noqa: PLC0415
        build_json,
        discover_project_path,
        load_models,
        load_project_config,
        load_relationships,
        load_views,
        validate_manifest,
        validate_project,
    )

    try:
        project_path = discover_project_path(path)
    except SystemExit as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    # ── Structural validation ────────────────────────────────────────────
    struct_errors = validate_project(project_path)
    struct_warnings = [e for e in struct_errors if e.level == "warning"]
    struct_hard = [e for e in struct_errors if e.level == "error"]

    if struct_errors:
        for e in struct_errors:
            typer.echo(str(e), err=True)

    # ── Semantic validation (dry-plan + description checks) ──────────────
    sem_errors: list[str] = []
    sem_warnings: list[str] = []
    try:
        config = load_project_config(project_path)
        ds_str = config.get("data_source", "")
        manifest_json = build_json(project_path)
        manifest_str = _b64.b64encode(
            json.dumps(manifest_json, ensure_ascii=False).encode()
        ).decode()
        sem_result = validate_manifest(manifest_str, ds_str, level=level)
        sem_errors = sem_result["errors"]
        sem_warnings = sem_result["warnings"]
    except Exception as e:
        sem_errors = [f"Semantic validation failed: {e}"]

    if sem_errors:
        typer.echo("\nSemantic errors:")
        for msg in sem_errors:
            typer.echo(f"  \u2717 {msg}", err=True)

    if sem_warnings:
        typer.echo("\nWarnings:")
        for msg in sem_warnings:
            typer.echo(f"  \u26a0 {msg}")

    # ── Exit logic ────────────────────────────────────────────────────────
    has_hard_error = bool(struct_hard or sem_errors)
    has_warning = bool(struct_warnings or sem_warnings)

    if has_hard_error or (strict and has_warning):
        raise typer.Exit(1)

    if not struct_errors and not sem_errors and not sem_warnings:
        models = load_models(project_path)
        views = load_views(project_path)
        rels = load_relationships(project_path)
        typer.echo(
            f"Valid — {len(models)} models, {len(views)} views, {len(rels)} relationships."
        )
    elif has_warning:
        n_warn = len(struct_warnings) + len(sem_warnings)
        typer.echo(f"\n{n_warn} warning(s), 0 errors.")


@context_app.command()
def build(
    path: ProjectPathOpt = None,
    output: Annotated[
        Optional[str],
        typer.Option(
            "--output", "-o", help="Output path. Defaults to <project>/target/mdl.json."
        ),
    ] = None,
    validate_first: Annotated[
        bool,
        typer.Option(
            "--validate/--no-validate", help="Run validation before building."
        ),
    ] = True,
) -> None:
    """Build YAML project into target/mdl.json for the engine.

    Reads wren_project.yml, models/*/metadata.yml (+ref_sql.sql),
    views/*/metadata.yml (+sql.yml), relationships.yml, and instructions.md.
    Converts snake_case YAML keys to camelCase JSON and writes target/mdl.json.
    """
    from wren.context import (  # noqa: PLC0415
        build_json,
        discover_project_path,
        save_target,
        validate_project,
    )

    try:
        project_path = discover_project_path(path)
    except SystemExit as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    if validate_first:
        errors = validate_project(project_path)
        hard_errors = [e for e in errors if e.level == "error"]
        if hard_errors:
            for e in hard_errors:
                typer.echo(str(e), err=True)
            typer.echo("\nBuild aborted due to validation errors.", err=True)
            raise typer.Exit(1)

    manifest_json = build_json(project_path)

    if output:
        out_path = Path(output).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(manifest_json, indent=2, ensure_ascii=False))
    else:
        out_path = save_target(manifest_json, project_path)

    n_models = len(manifest_json.get("models", []))
    n_views = len(manifest_json.get("views", []))
    typer.echo(f"Built: {n_models} models, {n_views} views → {out_path}")


@context_app.command()
def show(
    path: ProjectPathOpt = None,
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output format: json|yaml|summary"),
    ] = "summary",
) -> None:
    """Show the current project context (models, views, relationships)."""
    import yaml as _yaml  # noqa: PLC0415

    from wren.context import (  # noqa: PLC0415
        build_json,
        build_manifest,
        discover_project_path,
        load_instructions,
        load_project_config,
    )

    try:
        project_path = discover_project_path(path)
    except SystemExit as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    if output == "json":
        # JSON output uses camelCase
        manifest_json = build_json(project_path)
        typer.echo(json.dumps(manifest_json, indent=2, ensure_ascii=False))
    elif output == "yaml":
        # YAML output uses snake_case (native)
        manifest = build_manifest(project_path)
        typer.echo(_yaml.dump(manifest, default_flow_style=False, sort_keys=False))
    else:
        # Summary view
        config = load_project_config(project_path)
        manifest = build_manifest(project_path)
        models = manifest.get("models", [])
        views = manifest.get("views", [])
        rels = manifest.get("relationships", [])
        instr_content = load_instructions(project_path)

        typer.echo(
            f"Project: {config.get('name', '?')} (v{config.get('version', '?')})"
        )
        typer.echo(f"Data source: {config.get('data_source', '?')}")
        typer.echo(f"Path: {project_path}\n")

        if models:
            typer.echo(f"Models ({len(models)}):")
            for m in models:
                n_cols = len(m.get("columns", []))
                pk = m.get("primary_key", "—")
                source = "ref_sql" if m.get("ref_sql") else "table"
                typer.echo(f"  {m['name']}  ({source}, {n_cols} columns, pk={pk})")

        if views:
            typer.echo(f"\nViews ({len(views)}):")
            for v in views:
                typer.echo(f"  {v['name']}")

        if rels:
            typer.echo(f"\nRelationships ({len(rels)}):")
            for r in rels:
                models_str = " ↔ ".join(r.get("models", []))
                jt = r.get("join_type", "?")
                typer.echo(f"  {r.get('name', '?')}  ({models_str}, {jt})")

        if instr_content:
            lines = instr_content.strip().split("\n")
            typer.echo(f"\nInstructions: {len(lines)} lines")

        if not models and not views:
            typer.echo("Empty project. Run `wren context init` to get started.")


@context_app.command()
def instructions(
    path: ProjectPathOpt = None,
) -> None:
    """Print user instructions for LLM consumption."""
    from wren.context import discover_project_path, load_instructions  # noqa: PLC0415

    try:
        project_path = discover_project_path(path)
    except SystemExit as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    content = load_instructions(project_path)
    if content:
        typer.echo(content)
