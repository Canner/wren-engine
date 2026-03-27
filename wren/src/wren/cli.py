"""Wren CLI — SQL transform and execution via the Wren semantic layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

app = typer.Typer(name="wren", help="Wren Engine CLI", no_args_is_help=False)

_WREN_HOME = Path.home() / ".wren"
_DEFAULT_MDL = _WREN_HOME / "mdl.json"
_DEFAULT_CONN = _WREN_HOME / "connection_info.json"


# ── File discovery helpers ─────────────────────────────────────────────────


def _require_mdl(mdl: str | None) -> str:
    """Return mdl arg if given, else auto-discover mdl.json from ~/.wren."""
    if mdl is not None:
        return mdl
    if _DEFAULT_MDL.exists():
        return str(_DEFAULT_MDL)
    typer.echo(
        f"Error: --mdl not specified and '{_DEFAULT_MDL}' not found.",
        err=True,
    )
    raise typer.Exit(1)


def _load_manifest(mdl: str) -> str:
    """Load MDL from a file path or treat as base64 string directly."""
    path = Path(mdl).expanduser()
    if path.exists():
        import base64  # noqa: PLC0415

        content = path.read_bytes()
        if path.suffix.lower() == ".json":
            # Raw JSON file — base64-encode it for WrenEngine
            return base64.b64encode(content).decode()
        # Non-.json file — assume it already contains a base64-encoded MDL string
        return content.decode().strip()
    # Not a file path — treat as a raw base64 string passed directly
    return mdl


def _load_conn(
    connection_info: str | None,
    connection_file: str | None,
    *,
    required: bool = True,
) -> dict:
    """Load connection dict from inline JSON or file, with ~/.wren auto-discovery.

    If neither --connection-info nor --connection-file is given, looks for
    connection_info.json in ~/.wren.  Raises typer.Exit(1) if required=True and nothing
    is found.
    """
    if connection_info:
        try:
            conn = json.loads(connection_info)
        except json.JSONDecodeError as e:
            typer.echo(f"Error: invalid JSON in --connection-info: {e}", err=True)
            raise typer.Exit(1)
        if not isinstance(conn, dict):
            typer.echo(
                "Error: --connection-info must decode to a JSON object.", err=True
            )
            raise typer.Exit(1)
        return conn

    path_str = connection_file or (
        str(_DEFAULT_CONN) if _DEFAULT_CONN.exists() else None
    )
    if path_str:
        path = Path(path_str).expanduser()
        if not path.exists():
            typer.echo(f"Error: connection file not found: {path_str}", err=True)
            raise typer.Exit(1)
        try:
            conn = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            typer.echo(f"Error: invalid JSON in {path_str}: {e}", err=True)
            raise typer.Exit(1)
        if not isinstance(conn, dict):
            typer.echo(f"Error: {path_str} must contain a JSON object.", err=True)
            raise typer.Exit(1)
        return conn

    if required:
        typer.echo(
            f"Error: --connection-file not specified and '{_DEFAULT_CONN}' not found.",
            err=True,
        )
        raise typer.Exit(1)
    return {}


def _resolve_datasource(explicit: str | None, conn_dict: dict) -> str:
    """Return datasource: use explicit --datasource arg first, then pop from conn dict.

    Note: mutates conn_dict by removing the 'datasource' key so it is not
    forwarded as an unknown field to WrenEngine / the connector.
    """
    if explicit:
        conn_dict.pop("datasource", None)
        return explicit
    ds = conn_dict.pop("datasource", None)
    if ds:
        return ds
    typer.echo(
        "Error: --datasource not specified and 'datasource' key not found in connection info.",
        err=True,
    )
    raise typer.Exit(1)


def _build_engine(
    datasource: str | None,
    mdl: str | None,
    connection_info: str | None,
    connection_file: str | None,
    *,
    conn_required: bool = True,
):
    from wren.engine import WrenEngine  # noqa: PLC0415
    from wren.model.data_source import DataSource  # noqa: PLC0415

    manifest_str = _load_manifest(_require_mdl(mdl))
    conn_dict = _load_conn(connection_info, connection_file, required=conn_required)
    ds_str = _resolve_datasource(datasource, conn_dict)

    try:
        ds = DataSource(ds_str.lower())
    except ValueError:
        typer.echo(f"Error: unknown datasource '{ds_str}'", err=True)
        raise typer.Exit(1)

    return WrenEngine(
        manifest_str=manifest_str, data_source=ds, connection_info=conn_dict
    )


# ── Shared option types ────────────────────────────────────────────────────

DatasourceOpt = Annotated[
    Optional[str],
    typer.Option(
        "--datasource",
        "-d",
        help="Data source (e.g. mysql, postgres). Defaults to 'datasource' field in connection_info.json.",
    ),
]
MdlOpt = Annotated[
    Optional[str],
    typer.Option(
        "--mdl",
        "-m",
        help=f"Path to MDL JSON file or base64 string. Defaults to {_DEFAULT_MDL}.",
    ),
]
ConnInfoOpt = Annotated[
    Optional[str],
    typer.Option("--connection-info", help="Inline JSON connection string"),
]
ConnFileOpt = Annotated[
    Optional[str],
    typer.Option(
        "--connection-file",
        help=f"Path to JSON connection file. Defaults to {_DEFAULT_CONN}.",
    ),
]
LimitOpt = Annotated[
    Optional[int], typer.Option("--limit", "-l", help="Max rows to return")
]
OutputOpt = Annotated[
    str, typer.Option("--output", "-o", help="Output format: json|csv|table")
]


# ── Default command (no subcommand = query) ────────────────────────────────


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    sql: Annotated[
        Optional[str],
        typer.Option(
            "--sql", "-s", help="SQL query to execute (runs query by default)"
        ),
    ] = None,
    datasource: DatasourceOpt = None,
    mdl: MdlOpt = None,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
    limit: LimitOpt = None,
    output: OutputOpt = "table",
) -> None:
    """Wren Engine CLI.

    Run with --sql to execute a query using mdl.json and connection_info.json from
    ~/.wren.  Use a subcommand (query / dry-run / dry-plan / validate)
    for explicit control.

    connection_info.json format:

    \b
      {
        "datasource": "mysql",
        "host": "localhost",
        "port": 3306,
        "database": "mydb",
        "user": "root",
        "password": "secret"
      }
    """
    if ctx.invoked_subcommand is not None:
        return
    if sql is None:
        typer.echo(ctx.get_help())
        return
    with _build_engine(datasource, mdl, connection_info, connection_file) as engine:
        try:
            result = engine.query(sql, limit=limit)
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)
    _print_result(result, output)


# ── Subcommands ────────────────────────────────────────────────────────────


@app.command()
def query(
    sql: Annotated[str, typer.Option("--sql", "-s", help="SQL query to execute")],
    datasource: DatasourceOpt = None,
    mdl: MdlOpt = None,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
    limit: LimitOpt = None,
    output: OutputOpt = "table",
):
    """Execute a SQL query through the Wren semantic layer."""
    with _build_engine(datasource, mdl, connection_info, connection_file) as engine:
        try:
            result = engine.query(sql, limit=limit)
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)
    _print_result(result, output)


@app.command(name="dry-run")
def dry_run(
    sql: Annotated[str, typer.Option("--sql", "-s", help="SQL query to validate")],
    datasource: DatasourceOpt = None,
    mdl: MdlOpt = None,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
):
    """Dry-run a SQL query (parse + validate, no results returned)."""
    with _build_engine(datasource, mdl, connection_info, connection_file) as engine:
        try:
            engine.dry_run(sql)
            typer.echo("OK")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)


@app.command(name="dry-plan")
def dry_plan(
    sql: Annotated[str, typer.Option("--sql", "-s", help="SQL query to plan")],
    datasource: DatasourceOpt = None,
    mdl: MdlOpt = None,
    connection_file: ConnFileOpt = None,
):
    """Plan SQL through MDL and print the expanded SQL (no DB required)."""
    from wren.engine import WrenEngine  # noqa: PLC0415
    from wren.model.data_source import DataSource  # noqa: PLC0415

    manifest_str = _load_manifest(_require_mdl(mdl))
    # Read datasource from connection_info.json only when --datasource is not given
    conn_dict = (
        _load_conn(None, connection_file, required=False)
        if connection_file is not None or datasource is None
        else {}
    )
    ds_str = _resolve_datasource(datasource, conn_dict)

    try:
        ds = DataSource(ds_str.lower())
    except ValueError:
        typer.echo(f"Error: unknown datasource '{ds_str}'", err=True)
        raise typer.Exit(1)

    with WrenEngine(
        manifest_str=manifest_str, data_source=ds, connection_info={}
    ) as engine:
        try:
            result = engine.dry_plan(sql)
            typer.echo(result)
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)


@app.command()
def validate(
    sql: Annotated[str, typer.Option("--sql", "-s", help="SQL query to validate")],
    datasource: DatasourceOpt = None,
    mdl: MdlOpt = None,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
):
    """Validate SQL can be planned and dry-run against the data source."""
    with _build_engine(datasource, mdl, connection_info, connection_file) as engine:
        try:
            engine.dry_run(sql)
            typer.echo("Valid")
        except Exception as e:
            typer.echo(f"Invalid: {e}", err=True)
            raise typer.Exit(1)


# ── Output formatting ──────────────────────────────────────────────────────


def _print_result(table, output: str) -> None:
    output = output.lower()
    if output not in {"json", "csv", "table"}:
        typer.echo(
            f"Error: unsupported output format '{output}'. Use json, csv, or table.",
            err=True,
        )
        raise typer.Exit(1)
    if output == "json":
        try:
            df = table.to_pandas()
            typer.echo(df.to_json(orient="records", lines=True))
        except Exception:
            typer.echo(json.dumps(table.to_pydict()))
    elif output == "csv":
        try:
            df = table.to_pandas()
            typer.echo(df.to_csv(index=False))
        except Exception:
            typer.echo(str(table))
    else:
        try:
            df = table.to_pandas()
            typer.echo(df.to_string(index=False))
        except Exception:
            typer.echo(str(table))


if __name__ == "__main__":
    app()
