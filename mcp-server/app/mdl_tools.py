"""MDL generation tools — powered by wren-agent.

Registers three tools onto the shared FastMCP instance:

- ``mdl_chat``          — multi-turn conversation to build an MDL manifest
- ``mdl_reset_session`` — clear a session (history + DB connection)
- ``mdl_list_sessions`` — list active sessions

Requires ``wren-agent`` to be installed.  If it is missing the module
loads silently and no tools are registered, so the rest of the server
continues to work unchanged.

Environment variables (all optional):

- ``PYDANTIC_AI_MODEL``     — LLM to use, e.g. ``anthropic:claude-3-5-sonnet-latest``
- ``WREN_ENGINE_ENDPOINT``  — ibis-server URL for MDL dry-plan validation
- ``MDL_AGENT_SKILLS_DIR``  — directory of *.md / *.txt skill files to load on startup
- ``MDL_AGENT_MEMORY_PATH`` — JSON file path for persistent cross-session memory
- ``MDL_AGENT_MAX_SKILLS``  — max skills injected per turn (default 5)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

try:
    from wren_agent import MDLAgent, AgentQuestion, SkillStore, MemoryStore

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------


@dataclass
class _Session:
    agent: "MDLAgent"
    history: list = field(default_factory=list)


_sessions: dict[str, _Session] = {}

# Shared across all sessions (loaded once at startup)
_skill_store: "SkillStore | None" = None
_memory: "MemoryStore | None" = None


def _init_shared_state() -> None:
    """Load skills and memory from environment variables (called once)."""
    global _skill_store, _memory

    skills_dir = os.getenv("MDL_AGENT_SKILLS_DIR")
    if skills_dir and os.path.isdir(skills_dir):
        _skill_store = SkillStore()
        _skill_store.load_dir(skills_dir)
        skill_count = len(_skill_store)
        print(f"[MDL Agent] Loaded {skill_count} skills from {skills_dir}")  # noqa: T201

    memory_path = os.getenv("MDL_AGENT_MEMORY_PATH")
    if memory_path:
        _memory = MemoryStore(persist_path=memory_path)
        print(f"[MDL Agent] Memory store: {memory_path}")  # noqa: T201


def _get_or_create_session(session_id: str) -> "_Session":
    if session_id not in _sessions:
        max_skills = int(os.getenv("MDL_AGENT_MAX_SKILLS", "5"))
        _sessions[session_id] = _Session(
            agent=MDLAgent(
                ibis_server_url=os.getenv("WREN_ENGINE_ENDPOINT"),
                skill_store=_skill_store,
                memory=_memory,
                max_skills=max_skills,
            )
        )
    return _sessions[session_id]


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register_mdl_tools(mcp: FastMCP) -> None:
    """Register MDL agent tools onto *mcp*. No-op if wren-agent is missing."""
    if not _AVAILABLE:
        print("[MDL Agent] wren-agent not installed — MDL tools not registered.")  # noqa: T201
        return

    _init_shared_state()

    @mcp.tool(
        annotations=ToolAnnotations(
            title="MDL Chat",
            readOnlyHint=False,
        ),
    )
    async def mdl_chat(message: str, session_id: str = "default") -> str:
        """Generate or refine a Wren MDL manifest through a guided conversation.

        The agent explores the database schema and produces a validated MDL
        manifest ready for deployment with the ``deploy`` tool.

        **Typical workflow**:

        1. Say what database you want to model (e.g. "I want to model my PostgreSQL
           ecommerce database").
        2. The agent may ask for a connection string or clarifying questions.
        3. When MDL is ready, a JSON block is returned — copy it into ``deploy``.

        You can run multiple independent sessions in parallel using different
        ``session_id`` values.

        Args:
            message: Your message or answer to the agent's question.
            session_id: Conversation ID. Reuse the same ID across turns to
                        continue an existing session.

        Returns:
            Either a follow-up question prefixed with ``[MDL Agent asks]``
            or a completed MDL JSON prefixed with ``[MDL Ready]``.
        """
        session = _get_or_create_session(session_id)
        resp = await session.agent.run(message, message_history=session.history)
        session.history = resp.message_history

        if isinstance(resp.result, AgentQuestion):
            return f"[MDL Agent asks]: {resp.result.question}"

        mdl_json = resp.result.model_dump_json(by_alias=True, indent=2)
        return f"[MDL Ready — use the 'deploy' tool with this manifest]\n\n{mdl_json}"

    @mcp.tool(
        annotations=ToolAnnotations(
            title="Reset MDL Session",
            readOnlyHint=False,
        ),
    )
    async def mdl_reset_session(session_id: str = "default") -> str:
        """Reset an MDL generation session.

        Clears the conversation history and the database connection held inside
        the session. The next ``mdl_chat`` call on this ID starts fresh.

        Args:
            session_id: Session to reset (default: ``"default"``).
        """
        if session_id in _sessions:
            del _sessions[session_id]
            return f"Session '{session_id}' has been reset."
        return f"Session '{session_id}' not found — nothing to reset."

    @mcp.tool(
        annotations=ToolAnnotations(
            title="List MDL Sessions",
            readOnlyHint=True,
        ),
    )
    async def mdl_list_sessions() -> str:
        """List all active MDL generation sessions and their message counts.

        Returns a summary so you can track which sessions are in progress or
        decide which one to continue.
        """
        if not _sessions:
            return "No active MDL sessions."
        lines = [
            f"- '{sid}': {len(s.history)} messages in history"
            for sid, s in _sessions.items()
        ]
        return "\n".join(lines)
