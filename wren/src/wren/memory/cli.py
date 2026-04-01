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
_DEFAULT_MDL = _WREN_HOME / "mdl.json"
_DEFAULT_MEMORY = _WREN_HOME / "memory"

# ── Shared option types ───────────────────────────────────────────────────

PathOpt = Annotated[
    Optional[str],
    typer.Option(
        "--path",
        "-p",
        help=f"LanceDB storage directory. Defaults to {_DEFAULT_MEMORY}.",
    ),
]
MdlOpt = Annotated[
    Optional[str],
    typer.Option(
        "--mdl",
        "-m",
        help=f"Path to MDL JSON file. Defaults to {_DEFAULT_MDL}.",
    ),
]
OutputOpt = Annotated[
    str, typer.Option("--output", "-o", help="Output format: json|table")
]


# ── Helpers ───────────────────────────────────────────────────────────────


def _load_manifest(mdl: str | None) -> dict:
    """Load and return the MDL manifest as a dict."""
    mdl_path = Path(mdl).expanduser() if mdl else _DEFAULT_MDL
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
    try:
        from wren.memory.store import MemoryStore  # noqa: PLC0415

        return MemoryStore(path=path)
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
) -> None:
    """Index MDL schema into LanceDB for semantic search."""
    manifest = _load_manifest(mdl)
    store = _get_store(path)
    count = store.index_schema(manifest)
    typer.echo(f"Indexed {count} schema items.")


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
