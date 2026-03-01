"""Agent dependency container, passed to all tools via RunContext."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import sqlalchemy as sa

if TYPE_CHECKING:
    from wren_agent.memory import MemoryStore


@dataclass
class AgentDeps:
    """Holds stateful resources shared across all tool calls in one agent run."""

    # Set by connect_to_database tool; None until connection is established
    engine: sa.Engine | None = None

    # Optional ibis-server URL for dry-plan validation
    # Reads from env WREN_ENGINE_ENDPOINT if not supplied explicitly
    ibis_server_url: str | None = field(default=None)

    # Optional persistent memory store; None means memory tools are disabled
    memory: MemoryStore | None = None
