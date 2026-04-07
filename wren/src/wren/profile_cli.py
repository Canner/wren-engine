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
    ui: Annotated[
        bool,
        typer.Option("--ui", help="Open browser-based form to fill connection fields"),
    ] = False,
    ui_port: Annotated[
        int, typer.Option("--port", help="Port for the UI server (0 = auto-select)")
    ] = 0,
    no_open: Annotated[
        bool, typer.Option("--no-open", help="Don't auto-open browser (just print URL)")
    ] = False,
) -> None:
    """Add a new connection profile.

    Four modes: --ui (browser form), --from-file (import), --interactive
    (guided prompts), or inline --datasource (minimal profile).
    """
    from wren.profile import add_profile  # noqa: PLC0415

    selected_modes = sum(bool(flag) for flag in (ui, from_file, interactive))
    if selected_modes > 1:
        typer.echo(
            "Error: choose only one of --ui, --from-file, or --interactive.",
            err=True,
        )
        raise typer.Exit(1)

    if ui:
        try:
            from wren.profile_web import start as web_start  # noqa: PLC0415
        except ImportError as e:
            if e.name not in {"starlette", "uvicorn", "jinja2"}:
                raise
            typer.echo(
                "Error: --ui requires extra dependencies.\n"
                "Install with: pip install 'wren-engine[ui]'",
                err=True,
            )
            raise typer.Exit(1)
        if not no_open:
            typer.echo("Opening browser... (press Ctrl+C to cancel)")
        result = web_start(
            name, activate=activate, port=ui_port, open_browser=not no_open
        )
        if result:
            typer.echo(
                f"Profile '{result['name']}' saved (datasource: {result['datasource']})"
            )
            if activate:
                typer.echo(f"  Profile '{result['name']}' is now active.")
        else:
            typer.echo("Cancelled.", err=True)
            raise typer.Exit(1)
        return

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
    """Guided interactive profile creation using shared field registry."""
    import click  # noqa: PLC0415

    from wren.model.field_registry import (  # noqa: PLC0415
        get_datasource_options,
        get_fields,
        get_variants,
    )

    ds_choices = get_datasource_options()
    ds = typer.prompt(
        "Data source",
        default=default_ds,
        type=click.Choice(ds_choices, case_sensitive=False),
    )
    profile: dict = {"datasource": ds}

    # Handle datasources with subtypes (bigquery, redshift, databricks)
    variants = get_variants(ds)
    if variants:
        variant = typer.prompt(
            f"  Type ({', '.join(variants)})",
            type=click.Choice(variants, case_sensitive=False),
        )
        profile[f"{ds}_type"] = variant
    else:
        variant = None

    fields = get_fields(ds, variant=variant)

    for f in fields:
        # Hidden fields (e.g. duckdb format, discriminator fields) are injected automatically
        if f.input_type == "hidden":
            if f.default is not None:
                profile[f.name] = f.default
            continue
        # File fields: accept a path, read & base64-encode
        if f.input_type == "file_base64":
            path_str = typer.prompt(
                f"  {f.label} (file path)", default="", show_default=False
            )
            if path_str:
                import base64  # noqa: PLC0415
                from pathlib import Path  # noqa: PLC0415

                file_path = Path(path_str).expanduser()
                try:
                    content = file_path.read_bytes()
                    profile[f.name] = base64.b64encode(content).decode()
                except (FileNotFoundError, PermissionError) as e:
                    if f.required:
                        typer.echo(
                            f"  Error: required file not readable: {e}", err=True
                        )
                        raise typer.Exit(1)
                    typer.echo(f"  Warning: could not read file: {e}", err=True)
            elif f.required:
                typer.echo(f"  Error: {f.label} is required.", err=True)
                raise typer.Exit(1)
        # Sensitive fields: hide input
        elif f.sensitive or f.input_type == "password":
            value = typer.prompt(
                f"  {f.label}",
                default=f.default or "",
                show_default=False,
                hide_input=True,
            )
            if value:
                profile[f.name] = value
            elif f.required:
                typer.echo(f"  Error: {f.label} is required.", err=True)
                raise typer.Exit(1)
        # Normal text fields
        else:
            prompt_default = f.default or ""
            prompt_label = f"  {f.label}"
            if f.placeholder and not f.default:
                prompt_label += f" ({f.placeholder})"
            value = typer.prompt(
                prompt_label,
                default=prompt_default,
                show_default=bool(f.default),
            )
            if value:
                profile[f.name] = value
            elif f.required:
                typer.echo(f"  Error: {f.label} is required.", err=True)
                raise typer.Exit(1)
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
