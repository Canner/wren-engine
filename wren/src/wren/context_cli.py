"""CLI commands for `wren context` — MDL manifest inspection and validation."""

from __future__ import annotations

from typing import Annotated, Optional

import typer

context_app = typer.Typer(
    name="context", help="MDL context commands", no_args_is_help=True
)

_LEVEL_HELP = (
    "Validation level: error (CI/CD only), warning (default), strict (all checks)"
)
_VALID_LEVELS = {"error", "warning", "strict"}


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

    if level not in _VALID_LEVELS:
        typer.echo(
            f"Error: --level must be one of: {', '.join(sorted(_VALID_LEVELS))}",
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
