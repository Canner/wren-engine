"""Typer sub-app for ``wren memory`` commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

memory_app = typer.Typer(
    name="memory",
    help="Schema and query memory backed by LanceDB.",
)

_WREN_HOME = Path.home() / ".wren"

# ── Shared option types ───────────────────────────────────────────────────

PathOpt = Annotated[
    Optional[str],
    typer.Option(
        "--path",
        "-p",
        help="LanceDB storage directory. Defaults to <project>/.wren/memory/.",
    ),
]
MdlOpt = Annotated[
    Optional[str],
    typer.Option(
        "--mdl",
        "-m",
        help="Path to MDL JSON file. Defaults to <project>/target/mdl.json.",
    ),
]
OutputOpt = Annotated[
    str, typer.Option("--output", "-o", help="Output format: json|table")
]


# ── Helpers ───────────────────────────────────────────────────────────────


def _default_memory_path() -> Path:
    """Return the project-local memory path, or ~/.wren/memory/ as fallback."""
    try:
        from wren.context import discover_project_path  # noqa: PLC0415

        return discover_project_path() / ".wren" / "memory"
    except (SystemExit, Exception):
        return _WREN_HOME / "memory"


def _load_manifest(mdl: str | None) -> dict:
    """Load and return the MDL manifest as a dict."""
    if mdl:
        mdl_path = Path(mdl).expanduser()
    else:
        try:
            from wren.context import discover_project_path  # noqa: PLC0415

            mdl_path = discover_project_path() / "target" / "mdl.json"
        except SystemExit:
            typer.echo(
                "Error: no wren project found and --mdl not specified.", err=True
            )
            raise typer.Exit(1)
    if not mdl_path.exists():
        typer.echo(
            f"Error: MDL file not found: {mdl_path}",
            err=True,
        )
        raise typer.Exit(1)
    try:
        return json.loads(mdl_path.read_text())
    except json.JSONDecodeError as e:
        typer.echo(f"Error: invalid JSON in {mdl_path}: {e}", err=True)
        raise typer.Exit(1)


def _get_store(path: str | None):
    """Lazy-import and construct a MemoryStore."""
    resolved = path or str(_default_memory_path())
    try:
        from wren.memory.store import MemoryStore  # noqa: PLC0415

        return MemoryStore(path=resolved)
    except ModuleNotFoundError as e:
        if (e.name or "").split(".")[0] not in {
            "lancedb",
            "sentence_transformers",
            "pyarrow",
        }:
            raise
        typer.echo(
            "Error: wren[memory] extras not installed. "
            "Run: pip install 'wren-engine[memory]'",
            err=True,
        )
        raise typer.Exit(1)


def _print_results(results: list[dict], output: str) -> None:
    """Format and print search results."""
    if not results:
        typer.echo("No results found.")
        return

    output = output.lower()
    if output not in {"json", "table"}:
        typer.echo(
            f"Error: unsupported output format '{output}'. Use json or table.",
            err=True,
        )
        raise typer.Exit(1)
    if output == "json":
        serializable = []
        for r in results:
            row = dict(r)
            # Convert datetime objects to ISO strings for JSON serialization
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
            serializable.append(row)
        typer.echo(json.dumps(serializable, indent=2, ensure_ascii=False))
    else:
        try:
            import pandas as pd  # noqa: PLC0415

            df = pd.DataFrame(results)
            # Drop noisy columns for table display
            for col in ("vector", "_rowid"):
                if col in df.columns:
                    df = df.drop(columns=[col])
            typer.echo(df.to_string(index=False))
        except Exception:
            for r in results:
                typer.echo(str(r))


# ── Commands ──────────────────────────────────────────────────────────────


@memory_app.command()
def index(
    mdl: MdlOpt = None,
    path: PathOpt = None,
    include_instructions: Annotated[
        bool,
        typer.Option(
            "--instructions/--no-instructions",
            help="Also index user instructions from project directory.",
        ),
    ] = True,
    no_seed: Annotated[
        bool,
        typer.Option("--no-seed", help="Skip generating seed NL-SQL examples."),
    ] = False,
) -> None:
    """Index MDL schema into LanceDB (and optionally seed example queries)."""
    manifest = _load_manifest(mdl)

    if include_instructions and mdl is None:
        try:
            from wren.context import (  # noqa: I001, PLC0415
                discover_project_path,
                load_instructions,
            )

            project_path = discover_project_path()
            instr = load_instructions(project_path)
            if instr:
                manifest["_instructions"] = instr
        except (
            SystemExit,
            FileNotFoundError,
            PermissionError,
            IsADirectoryError,
            UnicodeDecodeError,
            ImportError,
            ModuleNotFoundError,
        ):
            pass  # instructions are optional; never fail index because of them

    store = _get_store(path)
    result = store.index_schema(manifest, seed_queries=not no_seed)
    typer.echo(
        f"Indexed {result['schema_items']} schema items"
        + (f", {result['seed_queries']} seed queries" if result["seed_queries"] else "")
        + "."
    )


@memory_app.command()
def describe(
    mdl: MdlOpt = None,
) -> None:
    """Print the full schema as structured plain text (no embedding needed)."""
    from wren.memory.schema_indexer import describe_schema  # noqa: PLC0415

    manifest = _load_manifest(mdl)
    text = describe_schema(manifest)
    typer.echo(text)


@memory_app.command()
def fetch(
    query: Annotated[str, typer.Option("--query", "-q", help="Search query")],
    mdl: MdlOpt = None,
    limit: Annotated[int, typer.Option("--limit", "-l")] = 5,
    item_type: Annotated[
        Optional[str],
        typer.Option(
            "--type",
            "-t",
            help="Filter: model|column|relationship|view (search strategy only)",
        ),
    ] = None,
    model_name: Annotated[
        Optional[str],
        typer.Option("--model", help="Filter by model name (search strategy only)"),
    ] = None,
    threshold: Annotated[
        Optional[int],
        typer.Option(
            "--threshold", help="Character threshold for full vs search strategy"
        ),
    ] = None,
    path: PathOpt = None,
    output: OutputOpt = "table",
) -> None:
    """Get schema context for an LLM.

    Small schemas are returned as full plain text.  Large schemas use
    embedding search with optional --type and --model filters.
    """
    manifest = _load_manifest(mdl)
    store = _get_store(path)
    kwargs: dict = {"limit": limit, "item_type": item_type, "model_name": model_name}
    if threshold is not None:
        kwargs["threshold"] = threshold
    result = store.get_context(manifest, query, **kwargs)
    strategy = result["strategy"]
    if output.lower() == "json":
        payload = dict(result)
        if "results" in payload:
            payload["results"] = [
                {
                    k: (v.isoformat() if hasattr(v, "isoformat") else v)
                    for k, v in row.items()
                }
                for row in payload["results"]
            ]
        typer.echo(json.dumps(payload, indent=2, ensure_ascii=False))
        return
    typer.echo(f"Strategy: {strategy}")
    if strategy == "full":
        typer.echo(result["schema"])
    else:
        _print_results(result["results"], output)


@memory_app.command()
def store(
    nl: Annotated[str, typer.Option("--nl", help="Natural language query")],
    sql: Annotated[str, typer.Option("--sql", help="Corresponding SQL query")],
    datasource: Annotated[Optional[str], typer.Option("--datasource", "-d")] = None,
    tags: Annotated[Optional[str], typer.Option("--tags")] = None,
    path: PathOpt = None,
) -> None:
    """Store a NL→SQL pair for future few-shot retrieval."""
    mem_store = _get_store(path)
    mem_store.store_query(nl, sql, datasource=datasource, tags=tags)
    typer.echo("Query stored.")


@memory_app.command()
def recall(
    query: Annotated[str, typer.Option("--query", "-q", help="Search query")],
    limit: Annotated[int, typer.Option("--limit", "-l")] = 3,
    datasource: Annotated[Optional[str], typer.Option("--datasource", "-d")] = None,
    path: PathOpt = None,
    output: OutputOpt = "table",
) -> None:
    """Search past NL→SQL pairs by semantic similarity."""
    mem_store = _get_store(path)
    results = mem_store.recall_queries(query, limit=limit, datasource=datasource)
    _print_results(results, output)


@memory_app.command()
def status(
    path: PathOpt = None,
) -> None:
    """Show memory index statistics."""
    mem_store = _get_store(path)
    info = mem_store.status()
    typer.echo(f"Path: {info['path']}")
    tables = info.get("tables", {})
    if not tables:
        typer.echo("No tables indexed yet.")
        return
    for name, count in tables.items():
        typer.echo(f"  {name}: {count} rows")


@memory_app.command()
def reset(
    path: PathOpt = None,
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Skip confirmation")
    ] = False,
) -> None:
    """Drop all memory tables and start fresh."""
    if not force:
        confirm = typer.confirm("This will delete all indexed memory. Continue?")
        if not confirm:
            raise typer.Abort()
    mem_store = _get_store(path)
    mem_store.reset()
    typer.echo("Memory reset.")
