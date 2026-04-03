"""Typer sub-app for ``wren profile`` commands."""

from __future__ import annotations

import json
from typing import Annotated, Optional

import typer
import yaml

profile_app = typer.Typer(
    name="profile",
    help="Manage connection profiles (~/.wren/profiles.yml).",
)


@profile_app.command("list")
def list_cmd() -> None:
    """List all profiles, highlighting the active one."""
    from wren.profile import get_active_name, list_profiles  # noqa: PLC0415

    try:
        profiles = list_profiles()
        active = get_active_name()
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    if not profiles:
        typer.echo("No profiles configured. Run `wren profile add` to create one.")
        return
    for name, conf in profiles.items():
        marker = " *" if name == active else ""
        ds = conf.get("datasource", "?")
        typer.echo(f"  {name}{marker}  ({ds})")


@profile_app.command()
def add(
    name: Annotated[str, typer.Argument(help="Profile name")],
    datasource: Annotated[
        Optional[str],
        typer.Option("--datasource", "-d", help="Data source type"),
    ] = None,
    from_file: Annotated[
        Optional[str],
        typer.Option(
            "--from-file", "-f", help="Import from a JSON/YAML connection file"
        ),
    ] = None,
    activate: Annotated[
        bool, typer.Option("--activate", help="Set as active profile")
    ] = False,
    interactive: Annotated[
        bool, typer.Option("--interactive", "-i", help="Interactive prompts")
    ] = False,
) -> None:
    """Add a new connection profile.

    Three modes: --from-file (import), --interactive (guided prompts), or
    inline --datasource + additional --key=value pairs (future).
    """
    from wren.profile import add_profile  # noqa: PLC0415

    if from_file:
        from pathlib import Path  # noqa: PLC0415

        path = Path(from_file).expanduser()
        if not path.exists():
            typer.echo(f"Error: file not found: {from_file}", err=True)
            raise typer.Exit(1)
        try:
            text = path.read_text()
            if path.suffix in (".yml", ".yaml"):
                raw = yaml.safe_load(text)
            else:
                raw = json.loads(text)
        except Exception as exc:
            typer.echo(f"Error: could not parse {from_file}: {exc}", err=True)
            raise typer.Exit(1)
        if not isinstance(raw, dict):
            typer.echo("Error: file must contain a JSON/YAML object.", err=True)
            raise typer.Exit(1)
        # Flatten the MCP/web envelope {"datasource": ..., "properties": {...}}
        # into a flat profile dict so _build_engine receives consistent connection_info.
        if "properties" in raw and isinstance(raw["properties"], dict):
            props = raw["properties"]
            props["datasource"] = raw.get("datasource", props.get("datasource"))
            profile_data = props
        else:
            profile_data = raw
        if not profile_data.get("datasource"):
            typer.echo(
                "Error: imported file must contain a 'datasource' key.", err=True
            )
            raise typer.Exit(1)
    elif interactive:
        profile_data = _interactive_add(datasource)
    else:
        if not datasource:
            typer.echo(
                "Error: --datasource is required (or use --interactive / --from-file).",
                err=True,
            )
            raise typer.Exit(1)
        profile_data = {"datasource": datasource}
        typer.echo(
            f"Created minimal profile '{name}' with datasource={datasource}. "
            "Edit ~/.wren/profiles.yml to add connection fields."
        )

    add_profile(name, profile_data, activate=activate)
    typer.echo(f"Profile '{name}' added.")


def _interactive_add(default_ds: str | None) -> dict:
    """Guided interactive profile creation."""
    import click  # noqa: PLC0415

    from wren.model.data_source import DataSource  # noqa: PLC0415

    ds_choices = [e.value for e in DataSource]
    ds = typer.prompt(
        "Data source",
        default=default_ds,
        type=click.Choice(ds_choices, case_sensitive=False),
    )
    profile: dict = {"datasource": ds}

    # Fields to prompt for each datasource: list of (name, default) pairs.
    # format is injected automatically for duckdb/local_file — not prompted.
    _COMMON_FIELDS: dict[str, list[tuple[str, str]]] = {
        "postgres": [
            ("host", ""),
            ("port", "5432"),
            ("database", ""),
            ("user", ""),
            ("password", ""),
        ],
        "mysql": [
            ("host", ""),
            ("port", "3306"),
            ("database", ""),
            ("user", ""),
            ("password", ""),
        ],
        "bigquery": [("project_id", ""), ("dataset_id", ""), ("credentials", "")],
        "duckdb": [("url", "")],
        "local_file": [("url", ""), ("format", "csv")],
    }
    _AUTO_FIELDS: dict[str, dict[str, str]] = {
        "duckdb": {"format": "duckdb"},
    }
    default_fields: list[tuple[str, str]] = [
        ("host", ""),
        ("port", ""),
        ("database", ""),
        ("user", ""),
        ("password", ""),
    ]
    _SENSITIVE_FIELDS = {"password", "credentials", "secret", "token", "private_key"}
    fields = _COMMON_FIELDS.get(ds, default_fields)
    for field, default in fields:
        hide = field.lower() in _SENSITIVE_FIELDS
        value = typer.prompt(
            f"  {field}",
            default=default,
            show_default=bool(default) and not hide,
            hide_input=hide,
        )
        if value:
            profile[field] = value
    profile.update(_AUTO_FIELDS.get(ds, {}))
    return profile


@profile_app.command()
def rm(
    name: Annotated[str, typer.Argument(help="Profile name to remove")],
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Skip confirmation")
    ] = False,
) -> None:
    """Remove a profile."""
    from wren.profile import remove_profile  # noqa: PLC0415

    if not force:
        confirm = typer.confirm(f"Remove profile '{name}'?")
        if not confirm:
            raise typer.Abort()
    try:
        found = remove_profile(name)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    if found:
        typer.echo(f"Profile '{name}' removed.")
    else:
        typer.echo(f"Error: profile '{name}' not found.", err=True)
        raise typer.Exit(1)


@profile_app.command()
def switch(
    name: Annotated[str, typer.Argument(help="Profile name to activate")],
) -> None:
    """Switch the active profile."""
    from wren.profile import switch_profile  # noqa: PLC0415

    try:
        found = switch_profile(name)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    if found:
        typer.echo(f"Active profile: {name}")
    else:
        typer.echo(f"Error: profile '{name}' not found.", err=True)
        raise typer.Exit(1)


@profile_app.command()
def debug(
    name: Annotated[
        Optional[str], typer.Argument(help="Profile name (default: active)")
    ] = None,
) -> None:
    """Show resolved profile config (sensitive fields masked)."""
    from wren.profile import debug_profile  # noqa: PLC0415

    try:
        info = debug_profile(name)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    if "error" in info:
        typer.echo(f"Error: {info['error']}", err=True)
        raise typer.Exit(1)
    typer.echo(json.dumps(info, indent=2, ensure_ascii=False))
