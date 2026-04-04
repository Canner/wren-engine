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
) -> None:
    """Initialize a new Wren project with scaffold files.

    Creates the directory structure with wren_project.yml,
    an example model, and placeholder files.
    """
    project_path = Path(path).expanduser() if path else Path.cwd()

    project_file = project_path / "wren_project.yml"
    if project_file.exists():
        typer.echo(
            f"Error: '{project_file}' already exists. This is already a Wren project.",
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
        "catalog: wren\n"
        "schema: public\n"
        "data_source: postgres\n"
    )
    project_file.write_text(project_yml)

    # Scaffold example model (table_reference mode)
    example_model_dir = project_path / "models" / "example"
    example_model_dir.mkdir(parents=True, exist_ok=True)
    (example_model_dir / "metadata.yml").write_text(
        "# Example model — replace with your actual table\n"
        "name: example\n"
        "table_reference:\n"
        '  catalog: ""\n'
        "  schema: public\n"
        "  table: example\n"
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
        'description: "An example view"\n'
        "properties: {}\n"
    )
    (example_view_dir / "sql.yml").write_text(
        "statement: >\n  SELECT * FROM example LIMIT 100\n"
    )

    # Instructions placeholder
    (project_path / "instructions.md").write_text(
        "# User Instructions\n\n"
        "Add custom rules or guidelines for LLM-based query generation here.\n"
    )

    typer.echo(f"Wren project initialized: {project_path}")
    typer.echo("  wren_project.yml            — project metadata (edit data_source)")
    typer.echo("  models/example/             — example model (metadata.yml)")
    typer.echo("  views/example_view/         — example view (metadata.yml + sql.yml)")
    typer.echo("  relationships.yml           — define joins between models")
    typer.echo("  instructions.md             — LLM instructions")
    typer.echo("\nNext: edit your models, then run `wren context build`.")


@context_app.command()
def validate(
    path: ProjectPathOpt = None,
    strict: Annotated[
        bool,
        typer.Option("--strict", help="Treat warnings as errors."),
    ] = False,
) -> None:
    """Validate MDL project structure (no database required).

    Checks wren_project.yml, model/view definitions, column types,
    relationship integrity, and naming uniqueness.
    """
    from wren.context import (  # noqa: PLC0415
        discover_project_path,
        load_models,
        load_relationships,
        load_views,
        validate_project,
    )

    try:
        project_path = discover_project_path(path)
    except SystemExit as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    errors = validate_project(project_path)

    if not errors:
        models = load_models(project_path)
        views = load_views(project_path)
        rels = load_relationships(project_path)
        typer.echo(
            f"Valid — {len(models)} models, {len(views)} views, {len(rels)} relationships."
        )
        return

    warnings = [e for e in errors if e.level == "warning"]
    hard_errors = [e for e in errors if e.level == "error"]

    for e in errors:
        typer.echo(str(e), err=True)

    if hard_errors or (strict and warnings):
        raise typer.Exit(1)

    typer.echo(f"\n{len(warnings)} warning(s), 0 errors.")


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


# ── Validate ─────────────────────────────────────────────────────────────

_LEVEL_HELP = (
    "Validation level: error (CI/CD only), warning (default), strict (all checks)"
)
_VALID_CLI_LEVELS = {"error", "warning", "strict"}


@context_app.command()
def validate(
    level: Annotated[str, typer.Option("--level", help=_LEVEL_HELP)] = "warning",
    mdl: Annotated[
        Optional[str],
        typer.Option(
            "--mdl",
            "-m",
            help="Path to MDL JSON file or base64 string. Defaults to ~/.wren/mdl.json.",
        ),
    ] = None,
    datasource: Annotated[
        Optional[str],
        typer.Option(
            "--datasource",
            "-d",
            help="Data source dialect (e.g. duckdb, postgres). Falls back to connection_info.json.",
        ),
    ] = None,
    connection_file: Annotated[
        Optional[str],
        typer.Option(
            "--connection-file",
            help="Path to JSON connection file. Defaults to ~/.wren/connection_info.json.",
        ),
    ] = None,
) -> None:
    """Validate MDL manifest: structure, view SQL dry-plan, and description completeness."""
    from wren.cli import (  # noqa: PLC0415
        _load_conn,
        _load_manifest,
        _require_mdl,
        _resolve_datasource,
    )
    from wren.context import validate as _validate  # noqa: PLC0415
    from wren.model.data_source import DataSource  # noqa: PLC0415

    if level not in _VALID_CLI_LEVELS:
        typer.echo(
            f"Error: --level must be one of: {', '.join(sorted(_VALID_CLI_LEVELS))}",
            err=True,
        )
        raise typer.Exit(1)

    manifest_str = _load_manifest(_require_mdl(mdl))
    conn_dict = (
        _load_conn(None, connection_file, required=False) if datasource is None else {}
    )
    ds_str = _resolve_datasource(conn_dict, explicit=datasource)

    try:
        ds = DataSource(ds_str.lower())
    except ValueError:
        typer.echo(f"Error: unknown datasource '{ds_str}'", err=True)
        raise typer.Exit(1)

    result = _validate(manifest_str, ds, level=level)
    errors = result["errors"]
    warnings = result["warnings"]

    if errors:
        typer.echo("\nErrors:")
        for msg in errors:
            typer.echo(f"  \u2717 {msg}")

    if warnings:
        typer.echo("\nWarnings:")
        for msg in warnings:
            typer.echo(f"  \u26a0 {msg}")

    if errors:
        n_err = len(errors)
        n_warn = len(warnings)
        parts = [f"{n_err} error{'s' if n_err != 1 else ''}"]
        if warnings:
            parts.append(f"{n_warn} warning{'s' if n_warn != 1 else ''}")
        typer.echo(f"\n\u2717 MDL validation failed ({', '.join(parts)})")
        raise typer.Exit(1)
    elif warnings:
        n_warn = len(warnings)
        typer.echo(
            f"\n\u2713 MDL structure is valid ({n_warn} warning{'s' if n_warn != 1 else ''})"
        )
    else:
        typer.echo("\n\u2713 MDL structure is valid")
