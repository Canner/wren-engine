"""MDL generation agent assembled with PydanticAI."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Union

from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import ModelMessage

from wren_agent.deps import AgentDeps
from wren_agent.memory import MemoryStore
from wren_agent.models import AgentQuestion, MDLManifest
from wren_agent.prompts import SYSTEM_PROMPT
from wren_agent.skills import SkillStore
from wren_agent.tools.db import (
    connect_to_database,
    get_column_info,
    get_column_stats,
    get_sample_data,
    list_tables,
)
from wren_agent.tools.memory import (
    tool_forget,
    tool_list_memory_keys,
    tool_recall,
    tool_remember,
    tool_search_memory,
)
from wren_agent.tools.validate import validate_mdl

# ---------------------------------------------------------------------------
# Internal PydanticAI agent
# ---------------------------------------------------------------------------

# model=None: supplied at run time via agent.run(model=...) to avoid requiring
# LLM credentials at import time.
_agent: Agent[AgentDeps, Union[AgentQuestion, MDLManifest]] = Agent(
    model=None,
    output_type=Union[AgentQuestion, MDLManifest],  # type: ignore[arg-type]
    deps_type=AgentDeps,
    system_prompt=SYSTEM_PROMPT,
)


# Register tools
@_agent.tool
async def tool_connect_to_database(
    ctx: RunContext[AgentDeps], connection_string: str
) -> str:
    """Establish a database connection.

    Call this with the user's connection string before exploring the schema.
    Example: postgresql://user:password@host:5432/dbname
    """
    return await connect_to_database(ctx, connection_string)


@_agent.tool
async def tool_list_tables(ctx: RunContext[AgentDeps]) -> list[str] | str:
    """List all user tables in the connected database."""
    return await list_tables(ctx)


@_agent.tool
async def tool_get_column_info(
    ctx: RunContext[AgentDeps], table: str
) -> list[dict] | str:
    """Get column metadata for a table: name, type, nullable, primary key, foreign keys."""
    return await get_column_info(ctx, table)


@_agent.tool
async def tool_get_column_stats(
    ctx: RunContext[AgentDeps], table: str, column: str
) -> dict | str:
    """Get basic statistics for a column: distinct count, null count, min, max."""
    return await get_column_stats(ctx, table, column)


@_agent.tool
async def tool_get_sample_data(
    ctx: RunContext[AgentDeps], table: str, limit: int = 5
) -> list[dict] | str:
    """Fetch sample rows from a table to understand data semantics."""
    return await get_sample_data(ctx, table, limit)


@_agent.tool
async def tool_validate_mdl(ctx: RunContext[AgentDeps], mdl: dict) -> str:
    """Validate an MDL manifest dict against the official JSON Schema and optionally
    run a dry-plan against ibis-server.

    Pass the MDL as a plain dict (using 'schema' key, not 'schema_name').
    """
    return await validate_mdl(ctx, mdl)


# ------------------------------------------------------------------
# Memory tools
# ------------------------------------------------------------------

@_agent.tool
async def _tool_remember(
    ctx: RunContext[AgentDeps], key: str, value: str, tags: list[str] | None = None
) -> str:
    """Store a piece of information in persistent memory for future sessions.

    Use this for facts that will be useful later, such as:
    - Database schema summaries
    - User naming preferences
    - Previously validated MDL fragments

    Args:
        key: Short identifier, e.g. "tpch_schema" or "user_prefers_snake_case".
        value: The text to remember.
        tags: Optional tags for grouping, e.g. ["schema", "tpch"].
    """
    return await tool_remember(ctx, key, value, tags)


@_agent.tool
async def _tool_recall(ctx: RunContext[AgentDeps], key: str) -> str:
    """Retrieve a previously stored memory entry by key.

    Returns the stored value, or a 'not found' message.
    """
    return await tool_recall(ctx, key)


@_agent.tool
async def _tool_search_memory(ctx: RunContext[AgentDeps], query: str) -> str:
    """Search memory entries whose key or value contains the query string.

    Call this at the start of a conversation to check if relevant prior
    knowledge about this database or user preferences already exists.
    """
    return await tool_search_memory(ctx, query)


@_agent.tool
async def _tool_list_memory_keys(ctx: RunContext[AgentDeps]) -> str:
    """List all keys currently stored in memory."""
    return await tool_list_memory_keys(ctx)


@_agent.tool
async def _tool_forget(ctx: RunContext[AgentDeps], key: str) -> str:
    """Delete a memory entry by key when a stored fact is outdated or incorrect."""
    return await tool_forget(ctx, key)


# ---------------------------------------------------------------------------
# Public MDLAgent wrapper
# ---------------------------------------------------------------------------


@dataclass
class MDLAgentResponse:
    """Response returned from MDLAgent.run()."""

    result: AgentQuestion | MDLManifest
    """The agent output: either a follow-up question or the completed MDL manifest."""

    message_history: list[ModelMessage] = field(default_factory=list)
    """Full conversation history — pass back to the next run() call to continue."""


class MDLAgent:
    """High-level wrapper around the PydanticAI MDL generation agent.

    Supports optional **skills** (domain knowledge injected per run) and
    **memory** (persistent key-value store readable/writable by the agent).

    Basic usage (multi-turn)::

        agent = MDLAgent()
        history = []

        resp = await agent.run("I want to create MDL for my database")
        # resp.result → AgentQuestion(question="Please provide your connection string…")
        history = resp.message_history

        resp = await agent.run("postgresql://user:pass@localhost/mydb", message_history=history)
        # resp.result → MDLManifest(...)

    With skills and memory::

        from wren_agent.skills import SkillStore
        from wren_agent.memory import MemoryStore

        skills = SkillStore()
        skills.load_dir("my_skills/")         # loads *.md / *.txt
        skills.add("ecommerce",
                   "Order tables usually reference customers via customer_id FK.")

        memory = MemoryStore(persist_path="~/.wren_agent/memory.json")

        agent = MDLAgent(skill_store=skills, memory=memory)
    """

    def __init__(
        self,
        model: str | None = None,
        ibis_server_url: str | None = None,
        skill_store: SkillStore | None = None,
        memory: MemoryStore | None = None,
        max_skills: int = 5,
    ) -> None:
        """
        Args:
            model: A pydantic-ai model string, e.g. ``"anthropic:claude-3-5-sonnet-latest"``,
                   ``"openai:gpt-4o"``. Defaults to ``PYDANTIC_AI_MODEL`` env var.
            ibis_server_url: Optional Wren ibis-server URL for dry-plan validation.
                             Defaults to ``WREN_ENGINE_ENDPOINT`` env var.
            skill_store: Optional :class:`~wren_agent.skills.SkillStore` with
                         domain-specific knowledge selected and injected per run.
                         On each :meth:`run`, the store's ``select(query, top_k)``
                         is called to pick the most relevant skills for that message.
            memory: Optional :class:`~wren_agent.memory.MemoryStore` for persistent
                    cross-session memory. The agent can call memory tools to read
                    and write entries during the conversation.
            max_skills: Maximum number of skills to inject per run (default 5).
                        Skills are ranked by TF-IDF cosine similarity to the current
                        message before injection.
        """
        self._model = (
            model
            or os.environ.get("PYDANTIC_AI_MODEL")
            or "anthropic:claude-3-5-sonnet-latest"
        )
        self._ibis_server_url = ibis_server_url or os.environ.get(
            "WREN_ENGINE_ENDPOINT"
        )
        self._skill_store = skill_store
        self._memory = memory
        self._max_skills = max_skills
        # Persist SA engine across turns so the DB connection survives multi-turn
        self._engine = None

    async def run(
        self,
        message: str,
        *,
        message_history: list[ModelMessage] | None = None,
    ) -> MDLAgentResponse:
        """Send a message to the agent and return the response.

        Args:
            message: The user's message.
            message_history: Previous messages from prior run() calls. Pass the
                             ``message_history`` from the last MDLAgentResponse to
                             continue an ongoing conversation.

        Returns:
            MDLAgentResponse with ``result`` (AgentQuestion or MDLManifest)
            and ``message_history`` for the next turn.
        """
        deps = AgentDeps(
            engine=self._engine,
            ibis_server_url=self._ibis_server_url,
            memory=self._memory,
        )

        # Skills are injected as per-run instructions so they don't permanently
        # bloat the base system prompt for every agent instance.
        # select() applies tag filter + TF-IDF semantic ranking against the user
        # message so only the most relevant skills are injected.
        instructions: str | None = (
            self._skill_store.select(
                query=message,
                top_k=self._max_skills,
            ).as_instructions()
            if self._skill_store
            else None
        )

        result = await _agent.run(
            message,
            model=self._model,
            deps=deps,
            message_history=message_history or [],
            instructions=instructions,
        )

        # Persist the engine for subsequent turns
        if deps.engine is not None:
            self._engine = deps.engine

        return MDLAgentResponse(
            result=result.output,
            message_history=list(result.all_messages()),
        )
