"""Embedding function abstraction for Wren Memory.

Uses LanceDB's embedding registry with sentence-transformers (local, no API key).
"""

from __future__ import annotations

_DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
_DEFAULT_DIM = 384


def get_embedding_function(model_name: str = _DEFAULT_MODEL):
    """Return a LanceDB sentence-transformers embedding function.

    The returned object implements ``compute_source_embeddings(texts)``
    and ``compute_query_embeddings(query)`` used by :class:`MemoryStore`.
    """
    import lancedb.embeddings  # noqa: PLC0415

    registry = lancedb.embeddings.get_registry()
    return registry.get("sentence-transformers").create(name=model_name)


def default_dimension() -> int:
    """Return the vector dimension for the default model."""
    return _DEFAULT_DIM
