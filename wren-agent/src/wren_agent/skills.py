"""Skill store — load domain-specific knowledge snippets and inject them into the agent.

Skills are static Markdown/text documents that describe conventions, patterns,
or domain knowledge. They are injected into the agent as ``instructions`` on
each run, with two selection mechanisms:

- **Tag filter** — ``by_tags(["postgres", "bigquery"])``
  Returns a new SkillStore with only skills whose tags overlap with the given set.

- **Semantic search** — ``search("nested JSON columns", top_k=3)``
  Ranks all skills by TF-IDF cosine similarity to the query and returns the top-k.

- **Combined** — ``select(query="...", tags=["postgres"], top_k=3)``
  Applies tag filter first, then semantic ranking.

``MDLAgent`` calls ``select(query=<user_message>)`` automatically on every run,
so only relevant skills are injected rather than the entire store.

Example::

    store = SkillStore()
    store.load_dir("skills/")
    store.add("ecommerce",
              "Order tables usually have customer_id FK to customers.",
              tags=["ecommerce", "postgres"])
    store.add("bigquery_nested",
              "Use STRUCT for nested columns in BigQuery.",
              tags=["bigquery"])

    agent = MDLAgent(skill_store=store, max_skills=3)
    # On each run, agent auto-calls store.select(query=message, top_k=3)
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Skill:
    """A single named knowledge snippet."""

    name: str
    content: str
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# TF-IDF helpers (pure Python, no extra dependencies)
# ---------------------------------------------------------------------------


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _tfidf_cosine(query: str, skills: list[Skill]) -> list[float]:
    """Return a cosine similarity score for each skill against the query."""
    if not skills:
        return []

    docs = [_tokenize(s.name + " " + s.content) for s in skills]
    query_tokens = _tokenize(query)

    if not query_tokens:
        return [0.0] * len(skills)

    # Document frequency across the skill corpus
    N = len(docs)
    df: Counter[str] = Counter()
    for doc in docs:
        for term in set(doc):
            df[term] += 1

    all_query_terms = set(query_tokens)
    idf = {
        t: math.log((N + 1) / (df.get(t, 0) + 1))
        for t in all_query_terms
    }

    # Query TF-IDF vector
    query_tf = Counter(query_tokens)
    query_vec = {t: query_tf[t] * idf[t] for t in query_tokens}
    query_norm = math.sqrt(sum(v * v for v in query_vec.values()))

    scores: list[float] = []
    for doc in docs:
        doc_tf = Counter(doc)
        shared = all_query_terms & set(doc)
        if not shared or query_norm == 0:
            scores.append(0.0)
            continue
        dot = sum(query_vec[t] * doc_tf[t] * idf[t] for t in shared)
        doc_vec_vals = [
            doc_tf[t] * idf.get(t, math.log((N + 1) / 1))
            for t in set(doc)
        ]
        doc_norm = math.sqrt(sum(v * v for v in doc_vec_vals))
        scores.append(dot / (query_norm * doc_norm) if doc_norm > 0 else 0.0)

    return scores


# ---------------------------------------------------------------------------
# SkillStore
# ---------------------------------------------------------------------------


class SkillStore:
    """Collection of skills with tag filtering and semantic search."""

    def __init__(self, _skills: dict[str, Skill] | None = None) -> None:
        self._skills: dict[str, Skill] = _skills or {}

    # ------------------------------------------------------------------
    # Loading / adding
    # ------------------------------------------------------------------

    def add(
        self,
        name: str,
        content: str,
        tags: list[str] | None = None,
    ) -> None:
        """Add a skill from an in-memory string.

        Args:
            name: Unique identifier for this skill.
            content: Markdown or plain-text knowledge content.
            tags: Optional list of tags for ``by_tags()`` filtering,
                  e.g. ``["postgres", "schema"]``.
        """
        self._skills[name] = Skill(
            name=name, content=content.strip(), tags=tags or []
        )

    def load_file(
        self,
        path: str | Path,
        tags: list[str] | None = None,
    ) -> None:
        """Load a single .md or .txt file as a skill.

        The skill name is the file stem (e.g., ``bigquery_mdl.md`` → ``bigquery_mdl``).

        Args:
            path: Path to the file.
            tags: Tags to attach to this skill.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Skill file not found: {p}")
        self.add(p.stem, p.read_text(encoding="utf-8"), tags=tags)

    def load_dir(
        self,
        path: str | Path,
        tags: list[str] | None = None,
    ) -> None:
        """Load all .md and .txt files in a directory as skills.

        Args:
            path: Directory to scan.
            tags: Tags applied to every loaded skill.
        """
        p = Path(path)
        if not p.is_dir():
            raise NotADirectoryError(f"Not a directory: {p}")
        for f in sorted(p.glob("*.md")) + sorted(p.glob("*.txt")):
            self.load_file(f, tags=tags)

    # ------------------------------------------------------------------
    # Basic querying
    # ------------------------------------------------------------------

    def get(self, name: str) -> Skill | None:
        return self._skills.get(name)

    def list_names(self) -> list[str]:
        return list(self._skills.keys())

    def __len__(self) -> int:
        return len(self._skills)

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    def by_tags(
        self,
        tags: list[str],
        match: str = "any",
    ) -> SkillStore:
        """Return a new SkillStore containing only skills whose tags overlap.

        Args:
            tags: Tags to filter by.
            match: ``"any"`` — keep skills with at least one matching tag
                   (union).  ``"all"`` — keep only skills that have every
                   given tag (intersection).

        Returns:
            A new :class:`SkillStore` (may be empty if no skills match).
        """
        tag_set = {t.lower() for t in tags}
        if match == "all":
            filtered = {
                name: s
                for name, s in self._skills.items()
                if tag_set.issubset({t.lower() for t in s.tags})
            }
        else:  # "any"
            filtered = {
                name: s
                for name, s in self._skills.items()
                if tag_set & {t.lower() for t in s.tags}
            }
        return SkillStore(filtered)

    def search(self, query: str, top_k: int = 5) -> SkillStore:
        """Rank skills by TF-IDF cosine similarity to *query* and return top-k.

        Uses pure-Python TF-IDF (no dependencies beyond the stdlib).

        Args:
            query: Text to compare against (typically the user's message).
            top_k: Maximum number of skills to return. Skills with a score
                   of 0 are excluded even if fewer than *top_k* remain.

        Returns:
            A new :class:`SkillStore` ordered by relevance (best first).
        """
        if not self._skills:
            return SkillStore()

        skills = list(self._skills.values())
        scores = _tfidf_cosine(query, skills)

        ranked = sorted(zip(scores, skills), key=lambda x: x[0], reverse=True)
        selected = {
            s.name: s
            for score, s in ranked[:top_k]
            if score > 0
        }
        return SkillStore(selected)

    def select(
        self,
        query: str | None = None,
        tags: list[str] | None = None,
        top_k: int | None = None,
    ) -> SkillStore:
        """Combined selection: tag filter → semantic ranking.

        This is what ``MDLAgent`` calls automatically on every ``run()``.

        Steps:

        1. If *tags* given, apply ``by_tags(tags)`` to narrow the pool.
        2. If *query* given, apply ``search(query, top_k)`` on the remaining pool.
        3. If neither, return all skills (optionally limited by *top_k*).

        Args:
            query: Current user message — used for semantic ranking.
            tags: Tags to pre-filter by before semantic ranking.
            top_k: Maximum number of skills to return.

        Returns:
            A new :class:`SkillStore` with the selected subset.
        """
        result: SkillStore = self

        if tags:
            result = result.by_tags(tags)

        if query:
            k = top_k if top_k is not None else len(result)
            result = result.search(query, top_k=k)
        elif top_k is not None:
            # No query — take first top_k in insertion order
            items = dict(list(result._skills.items())[:top_k])
            result = SkillStore(items)

        return result

    # ------------------------------------------------------------------
    # Injection
    # ------------------------------------------------------------------

    def as_instructions(self) -> str | None:
        """Concatenate selected skills into a single instructions string.

        Returns ``None`` if the store is empty (no instructions injected).
        """
        if not self._skills:
            return None
        parts = ["## Relevant Skills\n"]
        for skill in self._skills.values():
            tag_line = (
                f"*Tags: {', '.join(skill.tags)}*\n\n" if skill.tags else ""
            )
            parts.append(f"### {skill.name}\n\n{tag_line}{skill.content}\n")
        return "\n".join(parts)
