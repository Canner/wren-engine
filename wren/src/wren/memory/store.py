"""LanceDB-backed memory store for schema items and query history."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from wren.memory.embeddings import _DEFAULT_DIM, _DEFAULT_MODEL, get_embedding_function
from wren.memory.schema_indexer import (
    SCHEMA_DESCRIBE_THRESHOLD,
    describe_schema,
    extract_schema_items,
    manifest_hash,
)

_WREN_MEMORY_DIR = Path.home() / ".wren" / "memory"

_SCHEMA_TABLE = "schema_items"
_QUERY_TABLE = "query_history"


def _esc(value: str) -> str:
    """Escape single quotes for LanceDB where-clause literals."""
    return value.replace("'", "''")


def _schema_items_arrow_schema(dim: int = _DEFAULT_DIM) -> pa.Schema:
    return pa.schema(
        [
            pa.field("text", pa.utf8()),
            pa.field("vector", pa.list_(pa.float32(), dim)),
            pa.field("item_type", pa.utf8()),
            pa.field("model_name", pa.utf8()),
            pa.field("item_name", pa.utf8()),
            pa.field("data_type", pa.utf8()),
            pa.field("expression", pa.utf8()),
            pa.field("is_calculated", pa.bool_()),
            pa.field("mdl_hash", pa.utf8()),
            pa.field("indexed_at", pa.timestamp("us", tz="UTC")),
        ]
    )


def _query_history_arrow_schema(dim: int = _DEFAULT_DIM) -> pa.Schema:
    return pa.schema(
        [
            pa.field("text", pa.utf8()),
            pa.field("vector", pa.list_(pa.float32(), dim)),
            pa.field("nl_query", pa.utf8()),
            pa.field("sql_query", pa.utf8()),
            pa.field("datasource", pa.utf8()),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
            pa.field("tags", pa.utf8()),
        ]
    )


def _table_names(db) -> list[str]:
    """Get table names, compatible with lancedb >=0.30 (ListTablesResponse)."""
    result = db.list_tables()
    if isinstance(result, list):
        return result
    return result.tables


class MemoryStore:
    """Manage LanceDB tables for schema and query memory.

    Parameters
    ----------
    path:
        Directory for LanceDB storage.  Defaults to ``~/.wren/memory/``.
    model_name:
        Sentence-transformers model name.  ``None`` → default multilingual model.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        model_name: str | None = None,
    ):
        import lancedb  # noqa: PLC0415

        resolved = Path(path).expanduser() if path else _WREN_MEMORY_DIR
        resolved.mkdir(parents=True, exist_ok=True)
        self._path = resolved
        self._db = lancedb.connect(str(resolved))
        self._embed_fn = get_embedding_function(model_name or _DEFAULT_MODEL)
        # Derive actual vector dimension from the model so custom models work.
        probe = self._embed_fn.compute_source_embeddings(["probe"])
        self._dim = len(probe[0])

    def _schema_table_schema(self) -> pa.Schema:
        return _schema_items_arrow_schema(dim=self._dim)

    def _query_table_schema(self) -> pa.Schema:
        return _query_history_arrow_schema(dim=self._dim)

    # ── Schema indexing ───────────────────────────────────────────────────

    def index_schema(
        self,
        manifest: dict,
        *,
        replace: bool = True,
        seed_queries: bool = True,
    ) -> dict:
        """Extract schema items from *manifest*, embed, and store.

        If *seed_queries* is True, also generates canonical NL-SQL pairs
        and inserts them into query_history (tagged 'source:seed').
        Old seed entries are replaced; user-confirmed entries are preserved.

        Returns {"schema_items": int, "seed_queries": int}.
        """
        items = extract_schema_items(manifest)
        table_exists = _SCHEMA_TABLE in _table_names(self._db)

        if not items:
            if replace and table_exists:
                self._db.drop_table(_SCHEMA_TABLE)
            schema_count = 0
        else:
            texts = [item["text"] for item in items]
            vectors = self._embed_fn.compute_source_embeddings(texts)

            for item, vec in zip(items, vectors):
                item["vector"] = vec

            if replace:
                if table_exists:
                    self._db.drop_table(_SCHEMA_TABLE)
                self._db.create_table(
                    _SCHEMA_TABLE,
                    items,
                    schema=self._schema_table_schema(),
                )
            else:
                if table_exists:
                    tbl = self._db.open_table(_SCHEMA_TABLE)
                    tbl.add(items)
                else:
                    self._db.create_table(
                        _SCHEMA_TABLE,
                        items,
                        schema=self._schema_table_schema(),
                    )
            schema_count = len(items)

        seed_count = 0
        if seed_queries:
            seed_count = self._upsert_seed_queries(manifest)

        return {"schema_items": schema_count, "seed_queries": seed_count}

    def _upsert_seed_queries(self, manifest: dict) -> int:
        """Replace seed query entries, preserving user-confirmed ones."""
        from wren.memory.seed_queries import (  # noqa: PLC0415
            SEED_TAG,
            generate_seed_queries,
        )

        # Remove old seeds (tagged 'source:seed') but keep user entries
        if _QUERY_TABLE in _table_names(self._db):
            table = self._db.open_table(_QUERY_TABLE)
            table.delete(f"tags = '{SEED_TAG}'")

        pairs = generate_seed_queries(manifest)
        if not pairs:
            return 0

        # Insert new seeds via the existing store_query() method
        for pair in pairs:
            self.store_query(
                nl_query=pair["nl"],
                sql_query=pair["sql"],
                tags=SEED_TAG,
            )

        return len(pairs)

    def schema_is_current(self, manifest: dict) -> bool:
        """Check whether the indexed schema matches *manifest*.

        Returns ``True`` only when every row in the schema table carries
        the current manifest hash (i.e. no stale rows from a previous
        manifest remain).
        """
        if _SCHEMA_TABLE not in _table_names(self._db):
            return False
        table = self._db.open_table(_SCHEMA_TABLE)
        if table.count_rows() == 0:
            return False
        current_hash = manifest_hash(manifest)
        df = table.to_pandas()
        return bool((df["mdl_hash"] == current_hash).all())

    # ── Plain-text / hybrid ────────────────────────────────────────────────

    @staticmethod
    def describe_schema(manifest: dict) -> str:
        """Return the full schema as structured plain text (no embedding)."""
        return describe_schema(manifest)

    def get_context(
        self,
        manifest: dict,
        query: str,
        *,
        limit: int = 5,
        item_type: str | None = None,
        model_name: str | None = None,
        threshold: int = SCHEMA_DESCRIBE_THRESHOLD,
    ) -> dict:
        """Return schema context using the best strategy for the schema size.

        For small schemas (plain-text description below *threshold* chars),
        returns the full text (``strategy="full"``).  For large schemas,
        uses embedding search with optional filters (``strategy="search"``).

        Returns a dict with keys ``strategy``, ``schema`` (full) or
        ``results`` (search).
        """
        text = describe_schema(manifest)
        if len(text) <= threshold:
            return {"strategy": "full", "schema": text}

        mdl_hash_val = manifest_hash(manifest)
        results = self._search_schema(
            query,
            limit=limit,
            item_type=item_type,
            model_name=model_name,
            mdl_hash=mdl_hash_val,
        )
        return {"strategy": "search", "results": results}

    def _search_schema(
        self,
        query: str,
        *,
        limit: int = 5,
        item_type: str | None = None,
        model_name: str | None = None,
        mdl_hash: str | None = None,
    ) -> list[dict]:
        """Embedding search over indexed schema items (internal)."""
        if _SCHEMA_TABLE not in _table_names(self._db):
            return []

        table = self._db.open_table(_SCHEMA_TABLE)
        q = table.search(
            self._embed_fn.compute_query_embeddings(query)[0],
        )

        where_parts: list[str] = []
        if mdl_hash:
            where_parts.append(f"mdl_hash = '{_esc(mdl_hash)}'")
        if item_type:
            where_parts.append(f"item_type = '{_esc(item_type)}'")
        if model_name:
            where_parts.append(f"model_name = '{_esc(model_name)}'")
        if where_parts:
            q = q.where(" AND ".join(where_parts))

        results = q.limit(limit).to_list()
        for r in results:
            r.pop("vector", None)
        return results

    # ── Query history ─────────────────────────────────────────────────────

    def store_query(
        self,
        nl_query: str,
        sql_query: str,
        *,
        datasource: str | None = None,
        tags: str | None = None,
    ) -> None:
        """Store a NL→SQL pair with embedding of the NL query."""
        now = datetime.now(timezone.utc)
        vectors = self._embed_fn.compute_source_embeddings([nl_query])

        record = {
            "text": nl_query,
            "vector": vectors[0],
            "nl_query": nl_query,
            "sql_query": sql_query,
            "datasource": datasource or "",
            "created_at": now,
            "tags": tags or "",
        }

        if _QUERY_TABLE in _table_names(self._db):
            table = self._db.open_table(_QUERY_TABLE)
            table.add([record])
        else:
            self._db.create_table(
                _QUERY_TABLE,
                [record],
                schema=self._query_table_schema(),
            )

    def recall_queries(
        self,
        query: str,
        *,
        limit: int = 3,
        datasource: str | None = None,
    ) -> list[dict]:
        """Search past NL→SQL pairs by semantic similarity."""
        if _QUERY_TABLE not in _table_names(self._db):
            return []

        table = self._db.open_table(_QUERY_TABLE)
        q = table.search(
            self._embed_fn.compute_query_embeddings(query)[0],
        )

        if datasource:
            q = q.where(f"datasource = '{_esc(datasource)}'")

        results = q.limit(limit).to_list()
        for r in results:
            r.pop("vector", None)
        return results

    # ── Housekeeping ──────────────────────────────────────────────────────

    def status(self) -> dict:
        """Return index statistics."""
        info: dict = {"path": str(self._path), "tables": {}}
        for name in _table_names(self._db):
            table = self._db.open_table(name)
            info["tables"][name] = table.count_rows()
        return info

    def reset(self) -> None:
        """Drop Wren memory tables."""
        for name in (_SCHEMA_TABLE, _QUERY_TABLE):
            if name in _table_names(self._db):
                self._db.drop_table(name)
