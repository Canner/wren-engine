"""Memory tools exposed to the agent."""

from __future__ import annotations

from pydantic_ai import RunContext

from wren_agent.deps import AgentDeps


async def tool_remember(
    ctx: RunContext[AgentDeps], key: str, value: str, tags: list[str] | None = None
) -> str:
    """Store a piece of information in persistent memory for future sessions.

    Use this for facts that are likely to be useful later, such as:
    - Database schema summaries ("key: tpch_schema, value: has orders, lineitem, …")
    - User preferences ("key: naming_pref, value: keep original column names")
    - Previously generated MDL fragments

    Args:
        key: Short identifier, e.g. "tpch_schema" or "user_prefers_snake_case".
        value: The text to remember.
        tags: Optional tags for filtering, e.g. ["schema", "tpch"].
    """
    if ctx.deps.memory is None:
        return "Memory is not enabled for this session."
    ctx.deps.memory.remember(key, value, tags=tags)
    return f"Remembered '{key}'."


async def tool_recall(ctx: RunContext[AgentDeps], key: str) -> str:
    """Retrieve a previously stored memory entry by key.

    Returns the stored value, or a 'not found' message.
    """
    if ctx.deps.memory is None:
        return "Memory is not enabled for this session."
    value = ctx.deps.memory.recall(key)
    if value is None:
        return f"No memory found for key '{key}'."
    return value


async def tool_search_memory(ctx: RunContext[AgentDeps], query: str) -> str:
    """Search memory entries whose key or value contains the query string.

    Useful for finding relevant past observations before exploring the DB.
    Returns a formatted summary of matching entries.
    """
    if ctx.deps.memory is None:
        return "Memory is not enabled for this session."
    results = ctx.deps.memory.search(query)
    if not results:
        return f"No memory entries matching '{query}'."
    lines = [f"Found {len(results)} matching memory entry(ies):"]
    for e in results:
        tag_str = f" [{', '.join(e.tags)}]" if e.tags else ""
        lines.append(f"  [{e.key}]{tag_str}: {e.value[:200]}")
    return "\n".join(lines)


async def tool_list_memory_keys(ctx: RunContext[AgentDeps]) -> str:
    """List all keys currently stored in memory.

    Call this at the start of a session to check if relevant prior knowledge exists.
    """
    if ctx.deps.memory is None:
        return "Memory is not enabled for this session."
    keys = ctx.deps.memory.list_keys()
    if not keys:
        return "Memory is empty."
    return "Memory keys: " + ", ".join(keys)


async def tool_forget(ctx: RunContext[AgentDeps], key: str) -> str:
    """Delete a memory entry by key.

    Use when a stored fact is outdated or incorrect.
    """
    if ctx.deps.memory is None:
        return "Memory is not enabled for this session."
    existed = ctx.deps.memory.forget(key)
    return f"Forgot '{key}'." if existed else f"Key '{key}' was not in memory."
