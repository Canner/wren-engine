"""Wren CLI — SQL transform and execution via the Wren semantic layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

app = typer.Typer(name="wren", help="Wren Engine CLI", no_args_is_help=True)


def _load_connection_info(
    connection_info: str | None,
    connection_file_path: str | None,
) -> dict:
    if connection_info:
        try:
            return json.loads(connection_info)
        except json.JSONDecodeError as e:
            typer.echo(f"Error: invalid JSON in --connection-info: {e}", err=True)
            raise typer.Exit(1)
    if connection_file_path:
        path = Path(connection_file_path)
        if not path.exists():
            typer.echo(
                f"Error: connection file not found: {connection_file_path}", err=True
            )
            raise typer.Exit(1)
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError as e:
            typer.echo(f"Error: invalid JSON in {connection_file_path}: {e}", err=True)
            raise typer.Exit(1)
    typer.echo(
        "Error: either --connection-info or --connection-file must be provided",
        err=True,
    )
    raise typer.Exit(1)


def _load_manifest(mdl: str) -> str:
    """Load MDL from a file path or treat as base64 string directly."""
    path = Path(mdl)
    if path.exists():
        import base64  # noqa: PLC0415

        content = path.read_bytes()
        # If it's a JSON file, base64-encode it
        if mdl.endswith(".json"):
            return base64.b64encode(content).decode()
        # Otherwise assume it's already base64
        return content.decode().strip()
    # Treat as inline base64 string
    return mdl


def _make_engine(
    sql: str,
    datasource: str,
    mdl: str,
    connection_info: str | None,
    connection_file: str | None,
):
    from wren.engine import WrenEngine  # noqa: PLC0415
    from wren.model.data_source import DataSource  # noqa: PLC0415

    manifest_str = _load_manifest(mdl)
    conn_dict = _load_connection_info(connection_info, connection_file)

    try:
        ds = DataSource(datasource.lower())
    except ValueError:
        typer.echo(f"Error: unknown datasource '{datasource}'", err=True)
        raise typer.Exit(1)

    return WrenEngine(
        manifest_str=manifest_str, data_source=ds, connection_info=conn_dict
    )


# ── Common options ─────────────────────────────────────────────────────────

SqlArg = Annotated[str, typer.Option("--sql", "-s", help="SQL query to execute")]
DatasourceOpt = Annotated[
    str,
    typer.Option("--datasource", "-d", help="Data source name (e.g. postgres, duckdb)"),
]
MdlOpt = Annotated[
    str, typer.Option("--mdl", "-m", help="Path to MDL JSON file or base64 MDL string")
]
ConnInfoOpt = Annotated[
    Optional[str], typer.Option("--connection-info", help="JSON connection info string")
]
ConnFileOpt = Annotated[
    Optional[str],
    typer.Option("--connection-file", help="Path to JSON connection info file"),
]
LimitOpt = Annotated[
    Optional[int], typer.Option("--limit", "-l", help="Max rows to return")
]


@app.command()
def query(
    sql: SqlArg,
    datasource: DatasourceOpt,
    mdl: MdlOpt,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
    limit: LimitOpt = None,
    output: Annotated[
        str, typer.Option("--output", "-o", help="Output format: json|csv|table")
    ] = "table",
):
    """Execute a SQL query through the Wren semantic layer."""
    engine = _make_engine(sql, datasource, mdl, connection_info, connection_file)
    try:
        result = engine.query(sql, limit=limit)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    finally:
        engine.close()

    _print_result(result, output)


@app.command(name="dry-run")
def dry_run(
    sql: SqlArg,
    datasource: DatasourceOpt,
    mdl: MdlOpt,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
):
    """Dry-run a SQL query (parse + validate, no results returned)."""
    engine = _make_engine(sql, datasource, mdl, connection_info, connection_file)
    try:
        engine.dry_run(sql)
        typer.echo("OK")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    finally:
        engine.close()


@app.command()
def transpile(
    sql: SqlArg,
    datasource: DatasourceOpt,
    mdl: MdlOpt,
):
    """Transform SQL through MDL and emit the target dialect SQL (no DB required)."""
    from wren.engine import WrenEngine  # noqa: PLC0415
    from wren.model.data_source import DataSource  # noqa: PLC0415

    manifest_str = _load_manifest(mdl)
    try:
        ds = DataSource(datasource.lower())
    except ValueError:
        typer.echo(f"Error: unknown datasource '{datasource}'", err=True)
        raise typer.Exit(1)

    # For transpile we don't need real connection_info — pass a dummy dict
    engine = WrenEngine(manifest_str=manifest_str, data_source=ds, connection_info={})
    try:
        result = engine.transpile(sql)
        typer.echo(result)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    finally:
        engine.close()


@app.command()
def validate(
    sql: SqlArg,
    datasource: DatasourceOpt,
    mdl: MdlOpt,
    connection_info: ConnInfoOpt = None,
    connection_file: ConnFileOpt = None,
):
    """Validate SQL can be planned and dry-run against the data source."""
    engine = _make_engine(sql, datasource, mdl, connection_info, connection_file)
    try:
        engine.dry_run(sql)
        typer.echo("Valid")
    except Exception as e:
        typer.echo(f"Invalid: {e}", err=True)
        raise typer.Exit(1)
    finally:
        engine.close()


# ── Output formatting ──────────────────────────────────────────────────────


def _print_result(table, output: str) -> None:
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
        # Default: table format via pandas
        try:
            df = table.to_pandas()
            typer.echo(df.to_string(index=False))
        except Exception:
            typer.echo(str(table))


if __name__ == "__main__":
    app()
