"""Memory store — persistent key-value memory readable and writable by the agent.

The agent can call memory tools to:
- ``remember(key, value)`` — store a fact for future sessions
- ``recall(key)`` — retrieve a stored fact
- ``search_memory(query)`` — find entries whose key or value contains a substring
- ``list_memory_keys()`` — list all stored keys
- ``forget(key)`` — delete an entry

By default the store is in-memory only (lost when the process exits).
Pass ``persist_path`` to persist to a JSON file between sessions.

Example::

    mem = MemoryStore(persist_path="~/.wren_agent/memory.json")
    agent = MDLAgent(memory=mem)
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class MemoryEntry:
    key: str
    value: str
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    tags: list[str] = field(default_factory=list)


class MemoryStore:
    """Simple key-value memory that the agent can read and write.

    Args:
        persist_path: If given, the store is loaded from this JSON file on init
                      and saved back after every write.  ``~`` is expanded.
    """

    def __init__(self, persist_path: str | Path | None = None) -> None:
        self._entries: dict[str, MemoryEntry] = {}
        self._persist_path: Path | None = (
            Path(persist_path).expanduser() if persist_path else None
        )
        if self._persist_path and self._persist_path.exists():
            self._load()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def remember(
        self,
        key: str,
        value: str,
        *,
        tags: list[str] | None = None,
        overwrite: bool = True,
    ) -> None:
        """Store a memory entry.

        Args:
            key: Identifier (e.g., ``"tpch_db_schema"``, ``"user_prefers_snake_case"``).
            value: The content to remember.
            tags: Optional list of tags for filtering.
            overwrite: If False and key already exists, raises ValueError.
        """
        if not overwrite and key in self._entries:
            raise ValueError(f"Memory key '{key}' already exists. Use overwrite=True to update.")
        self._entries[key] = MemoryEntry(
            key=key,
            value=value,
            created_at=datetime.now(timezone.utc).isoformat(),
            tags=tags or [],
        )
        self._save()

    def forget(self, key: str) -> bool:
        """Delete a memory entry. Returns True if it existed."""
        existed = key in self._entries
        if existed:
            del self._entries[key]
            self._save()
        return existed

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def recall(self, key: str) -> str | None:
        """Retrieve the value for a key, or None if not found."""
        entry = self._entries.get(key)
        return entry.value if entry else None

    def search(self, query: str) -> list[MemoryEntry]:
        """Return entries whose key or value contains ``query`` (case-insensitive)."""
        q = query.lower()
        return [
            e for e in self._entries.values()
            if q in e.key.lower() or q in e.value.lower()
        ]

    def list_keys(self) -> list[str]:
        return list(self._entries.keys())

    def get_entry(self, key: str) -> MemoryEntry | None:
        return self._entries.get(key)

    def all_entries(self) -> list[MemoryEntry]:
        return list(self._entries.values())

    def __len__(self) -> int:
        return len(self._entries)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        if self._persist_path is None:
            return
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = [asdict(e) for e in self._entries.values()]
        self._persist_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _load(self) -> None:
        assert self._persist_path is not None
        try:
            data = json.loads(self._persist_path.read_text(encoding="utf-8"))
            for item in data:
                entry = MemoryEntry(**item)
                self._entries[entry.key] = entry
        except (json.JSONDecodeError, TypeError, KeyError):
            pass  # Corrupted file — start fresh
