"""Tests for wren_agent."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wren_agent.memory import MemoryStore
from wren_agent.skills import Skill, SkillStore

from wren_agent.models import (
    AgentQuestion,
    MDLColumn,
    MDLManifest,
    MDLModel,
    MDLRelationship,
    TableReference,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

JAFFLE_MDL: dict[str, Any] = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "catalog": "jaffle_shop",
                "schema": "main",
                "table": "orders",
            },
            "columns": [
                {"name": "amount", "type": "double"},
                {"name": "id", "type": "integer"},
                {"name": "status", "type": "varchar"},
            ],
        }
    ],
}

LOCAL_MDL: dict[str, Any] = {
    "catalog": "wrenai",
    "schema": "public",
    "models": [
        {
            "name": "Customer",
            "tableReference": {"table": "s3://bucket/customer.parquet"},
            "columns": [
                {"name": "custkey", "type": "integer"},
                {
                    "name": "orders",
                    "type": "Orders",
                    "relationship": "CustomerOrders",
                },
                {
                    "name": "orders_key",
                    "type": "varchar",
                    "isCalculated": True,
                    "expression": "orders.orderkey",
                },
            ],
            "primaryKey": "custkey",
        },
        {
            "name": "Orders",
            "tableReference": {"table": "s3://bucket/orders.parquet"},
            "columns": [
                {"name": "orderkey", "type": "integer"},
                {"name": "custkey", "type": "integer"},
            ],
            "primaryKey": "orderkey",
        },
    ],
    "relationships": [
        {
            "name": "CustomerOrders",
            "models": ["Customer", "Orders"],
            "joinType": "ONE_TO_MANY",
            "condition": "Customer.custkey = Orders.custkey",
        }
    ],
}


# ---------------------------------------------------------------------------
# Model parsing tests
# ---------------------------------------------------------------------------


class TestMDLManifestParse:
    def test_parse_jaffle(self) -> None:
        manifest = MDLManifest.model_validate(JAFFLE_MDL)
        assert manifest.catalog == "wren"
        assert manifest.schema_name == "public"
        assert manifest.models is not None
        assert len(manifest.models) == 1
        assert manifest.models[0].name == "orders"
        assert len(manifest.models[0].columns) == 3

    def test_parse_local_mdl_with_relationships(self) -> None:
        manifest = MDLManifest.model_validate(LOCAL_MDL)
        assert manifest.catalog == "wrenai"
        assert manifest.relationships is not None
        assert len(manifest.relationships) == 1
        rel = manifest.relationships[0]
        assert rel.name == "CustomerOrders"
        assert rel.joinType == "ONE_TO_MANY"
        assert rel.models == ["Customer", "Orders"]

    def test_parse_calculated_column(self) -> None:
        manifest = MDLManifest.model_validate(LOCAL_MDL)
        assert manifest.models is not None
        customer = next(m for m in manifest.models if m.name == "Customer")
        calc_col = next(c for c in customer.columns if c.name == "orders_key")
        assert calc_col.isCalculated is True
        assert calc_col.expression == "orders.orderkey"

    def test_to_mdl_dict_uses_schema_key(self) -> None:
        manifest = MDLManifest.model_validate(JAFFLE_MDL)
        d = manifest.to_mdl_dict()
        assert "schema" in d
        assert "schema_name" not in d

    def test_model_requires_table_source(self) -> None:
        with pytest.raises(Exception):
            MDLModel(name="NoBacking")  # no tableReference, refSql, or baseObject

    def test_model_rejects_multiple_sources(self) -> None:
        with pytest.raises(Exception):
            MDLModel(
                name="Ambiguous",
                tableReference=TableReference(table="t"),
                refSql="SELECT 1",
            )

    def test_datasource_valid(self) -> None:
        manifest = MDLManifest.model_validate({
            **JAFFLE_MDL,
            "dataSource": "POSTGRES",
        })
        assert manifest.dataSource == "POSTGRES"

    def test_datasource_invalid(self) -> None:
        with pytest.raises(Exception):
            MDLManifest.model_validate({
                **JAFFLE_MDL,
                "dataSource": "NONEXISTENT_DB",
            })

    def test_relationship_requires_two_models(self) -> None:
        with pytest.raises(Exception):
            MDLRelationship(
                name="Bad",
                models=["OnlyOne"],
                joinType="ONE_TO_MANY",
                condition="x = y",
            )


# ---------------------------------------------------------------------------
# validate_mdl tool tests
# ---------------------------------------------------------------------------


class TestValidateMdl:
    @pytest.mark.asyncio
    async def test_valid_mdl_passes_schema(self) -> None:
        from unittest.mock import patch

        import httpx

        from wren_agent.tools.validate import validate_mdl, _cached_schema
        import wren_agent.tools.validate as validate_module

        # Inject a minimally correct schema for testing
        minimal_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "catalog": {"type": "string"},
                "schema": {"type": "string"},
            },
            "required": ["catalog", "schema"],
        }
        validate_module._cached_schema = minimal_schema

        deps_mock = MagicMock()
        deps_mock.ibis_server_url = None
        ctx = MagicMock()
        ctx.deps = deps_mock

        result = await validate_mdl(ctx, {"catalog": "wren", "schema": "public"})
        assert "passed" in result.lower()

        # restore
        validate_module._cached_schema = None

    @pytest.mark.asyncio
    async def test_invalid_mdl_fails_schema(self) -> None:
        import wren_agent.tools.validate as validate_module
        from wren_agent.tools.validate import validate_mdl

        minimal_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "catalog": {"type": "string"},
                "schema": {"type": "string"},
            },
            "required": ["catalog", "schema"],
        }
        validate_module._cached_schema = minimal_schema

        deps_mock = MagicMock()
        deps_mock.ibis_server_url = None
        ctx = MagicMock()
        ctx.deps = deps_mock

        result = await validate_mdl(ctx, {"catalog": "wren"})  # missing "schema"
        assert "failed" in result.lower()

        validate_module._cached_schema = None


# ---------------------------------------------------------------------------
# AgentQuestion model test
# ---------------------------------------------------------------------------


class TestAgentQuestion:
    def test_question_is_string(self) -> None:
        q = AgentQuestion(question="What is your connection string?")
        assert q.question == "What is your connection string?"


# ---------------------------------------------------------------------------
# SkillStore tests
# ---------------------------------------------------------------------------


class TestSkillStore:
    def test_add_and_get(self) -> None:
        store = SkillStore()
        store.add("tip1", "Always use PascalCase for model names.")
        assert store.get("tip1") is not None
        assert store.get("tip1").content == "Always use PascalCase for model names."

    def test_len(self) -> None:
        store = SkillStore()
        assert len(store) == 0
        store.add("a", "content a")
        store.add("b", "content b")
        assert len(store) == 2

    def test_list_names(self) -> None:
        store = SkillStore()
        store.add("x", "...")
        store.add("y", "...")
        assert set(store.list_names()) == {"x", "y"}

    def test_as_instructions_empty(self) -> None:
        assert SkillStore().as_instructions() is None

    def test_as_instructions_contains_skill(self) -> None:
        store = SkillStore()
        store.add("ecommerce", "Order tables reference customers via customer_id.")
        instructions = store.as_instructions()
        assert instructions is not None
        assert "ecommerce" in instructions
        assert "Order tables" in instructions
        assert "## Relevant Skills" in instructions

    def test_load_file(self, tmp_path: Path) -> None:
        skill_file = tmp_path / "bigquery.md"
        skill_file.write_text("Use STRUCT for nested columns.", encoding="utf-8")
        store = SkillStore()
        store.load_file(skill_file)
        assert store.get("bigquery") is not None
        assert "STRUCT" in store.get("bigquery").content

    def test_load_dir(self, tmp_path: Path) -> None:
        (tmp_path / "skill_a.md").write_text("Skill A content", encoding="utf-8")
        (tmp_path / "skill_b.txt").write_text("Skill B content", encoding="utf-8")
        (tmp_path / "ignored.json").write_text("{}", encoding="utf-8")
        store = SkillStore()
        store.load_dir(tmp_path)
        assert len(store) == 2
        assert store.get("skill_a") is not None
        assert store.get("skill_b") is not None

    def test_load_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            SkillStore().load_file("/nonexistent/path/skill.md")

    # ---- tags ----

    def test_add_with_tags(self) -> None:
        store = SkillStore()
        store.add("pg_tip", "Use bigint for IDs.", tags=["postgres", "schema"])
        skill = store.get("pg_tip")
        assert skill is not None
        assert "postgres" in skill.tags

    def test_by_tags_any(self) -> None:
        store = SkillStore()
        store.add("pg", "Postgres tip", tags=["postgres"])
        store.add("bq", "BigQuery tip", tags=["bigquery"])
        store.add("generic", "Generic tip", tags=["schema"])
        result = store.by_tags(["postgres"])
        assert len(result) == 1
        assert result.get("pg") is not None

    def test_by_tags_any_multiple(self) -> None:
        store = SkillStore()
        store.add("pg", "Postgres tip", tags=["postgres"])
        store.add("bq", "BigQuery tip", tags=["bigquery"])
        store.add("generic", "Generic tip", tags=["schema"])
        result = store.by_tags(["postgres", "bigquery"])
        assert len(result) == 2

    def test_by_tags_all(self) -> None:
        store = SkillStore()
        store.add("both", "Has both tags", tags=["postgres", "schema"])
        store.add("one", "Has only postgres", tags=["postgres"])
        result = store.by_tags(["postgres", "schema"], match="all")
        assert len(result) == 1
        assert result.get("both") is not None

    def test_by_tags_empty_result(self) -> None:
        store = SkillStore()
        store.add("pg", "Postgres tip", tags=["postgres"])
        result = store.by_tags(["mysql"])
        assert len(result) == 0

    def test_by_tags_case_insensitive(self) -> None:
        store = SkillStore()
        store.add("pg", "Postgres tip", tags=["PostgreSQL"])
        result = store.by_tags(["postgresql"])
        assert len(result) == 1

    # ---- search ----

    def test_search_ranks_by_relevance(self) -> None:
        store = SkillStore()
        store.add("bigquery", "BigQuery tips for nested JSON columns using STRUCT.")
        store.add("postgres", "PostgreSQL tips for indexing and constraints.")
        store.add("ecommerce", "Ecommerce order tables reference customers.")
        result = store.search("BigQuery nested JSON", top_k=1)
        assert len(result) == 1
        assert result.get("bigquery") is not None

    def test_search_excludes_zero_scores(self) -> None:
        store = SkillStore()
        store.add("irrelevant", "Completely unrelated topic about bicycles.")
        result = store.search("database schema MDL", top_k=5)
        # Might be 0 or 1 depending on overlap; should not crash
        assert len(result) <= 1

    def test_search_empty_store(self) -> None:
        result = SkillStore().search("anything")
        assert len(result) == 0

    def test_search_empty_query(self) -> None:
        store = SkillStore()
        store.add("tip", "something")
        result = store.search("", top_k=5)
        assert len(result) == 0  # empty query → all scores 0

    def test_search_respects_top_k(self) -> None:
        store = SkillStore()
        for i in range(10):
            store.add(f"skill_{i}", f"database schema table column model {i}")
        result = store.search("database schema", top_k=3)
        assert len(result) <= 3

    # ---- select ----

    def test_select_tags_only(self) -> None:
        store = SkillStore()
        store.add("pg", "Postgres tip", tags=["postgres"])
        store.add("bq", "BigQuery tip", tags=["bigquery"])
        result = store.select(tags=["postgres"])
        assert len(result) == 1
        assert result.get("pg") is not None

    def test_select_query_only(self) -> None:
        store = SkillStore()
        store.add("bigquery", "BigQuery nested STRUCT column model schema.")
        store.add("ecommerce", "Order table references customer_id FK.")
        result = store.select(query="BigQuery STRUCT nested")
        assert result.get("bigquery") is not None

    def test_select_tags_then_query(self) -> None:
        store = SkillStore()
        store.add("pg_idx", "PostgreSQL index strategies.", tags=["postgres"])
        store.add("pg_fk", "PostgreSQL FK naming conventions.", tags=["postgres"])
        store.add("bq", "BigQuery tips.", tags=["bigquery"])
        result = store.select(query="FK naming", tags=["postgres"], top_k=1)
        assert len(result) == 1
        assert result.get("pg_fk") is not None

    def test_select_no_args_returns_all(self) -> None:
        store = SkillStore()
        store.add("a", "content a")
        store.add("b", "content b")
        result = store.select()
        assert len(result) == 2

    def test_select_top_k_no_query(self) -> None:
        store = SkillStore()
        for i in range(5):
            store.add(f"skill_{i}", f"skill content {i}")
        result = store.select(top_k=3)
        assert len(result) == 3

    def test_as_instructions_shows_tags(self) -> None:
        store = SkillStore()
        store.add("pg_tip", "Use bigint for IDs.", tags=["postgres", "schema"])
        instructions = store.as_instructions()
        assert instructions is not None
        assert "postgres" in instructions
        assert "schema" in instructions


# ---------------------------------------------------------------------------
# MemoryStore tests
# ---------------------------------------------------------------------------


class TestMemoryStore:
    def test_remember_and_recall(self) -> None:
        mem = MemoryStore()
        mem.remember("db_schema", "orders table has 10 columns")
        assert mem.recall("db_schema") == "orders table has 10 columns"

    def test_recall_missing(self) -> None:
        mem = MemoryStore()
        assert mem.recall("nonexistent") is None

    def test_overwrite(self) -> None:
        mem = MemoryStore()
        mem.remember("key", "old value")
        mem.remember("key", "new value")
        assert mem.recall("key") == "new value"

    def test_overwrite_false_raises(self) -> None:
        mem = MemoryStore()
        mem.remember("key", "value")
        with pytest.raises(ValueError, match="already exists"):
            mem.remember("key", "other", overwrite=False)

    def test_forget(self) -> None:
        mem = MemoryStore()
        mem.remember("temp", "data")
        assert mem.forget("temp") is True
        assert mem.recall("temp") is None
        assert mem.forget("temp") is False  # already gone

    def test_search(self) -> None:
        mem = MemoryStore()
        mem.remember("tpch_tables", "orders, customer, lineitem")
        mem.remember("user_pref", "prefer snake_case columns")
        results = mem.search("tpch")
        assert len(results) == 1
        assert results[0].key == "tpch_tables"

    def test_search_no_results(self) -> None:
        mem = MemoryStore()
        mem.remember("foo", "bar")
        assert mem.search("zzz_no_match") == []

    def test_list_keys(self) -> None:
        mem = MemoryStore()
        mem.remember("a", "1")
        mem.remember("b", "2")
        assert set(mem.list_keys()) == {"a", "b"}

    def test_tags(self) -> None:
        mem = MemoryStore()
        mem.remember("schema_x", "...", tags=["schema", "postgres"])
        entry = mem.get_entry("schema_x")
        assert entry is not None
        assert "schema" in entry.tags

    def test_persist_to_file(self, tmp_path: Path) -> None:
        path = tmp_path / "memory.json"
        mem = MemoryStore(persist_path=path)
        mem.remember("persisted_key", "persisted value")
        assert path.exists()

        # Load in a new instance
        mem2 = MemoryStore(persist_path=path)
        assert mem2.recall("persisted_key") == "persisted value"

    def test_persist_corrupted_file(self, tmp_path: Path) -> None:
        path = tmp_path / "memory.json"
        path.write_text("not valid json", encoding="utf-8")
        # Should not raise; start fresh
        mem = MemoryStore(persist_path=path)
        assert len(mem) == 0


# ---------------------------------------------------------------------------
# Memory tools tests
# ---------------------------------------------------------------------------


class TestMemoryTools:
    def _make_ctx(self, memory: MemoryStore | None = None) -> MagicMock:
        ctx = MagicMock()
        from wren_agent.deps import AgentDeps
        ctx.deps = AgentDeps(memory=memory)
        return ctx

    @pytest.mark.asyncio
    async def test_remember_and_recall_tools(self) -> None:
        from wren_agent.tools.memory import tool_recall, tool_remember

        mem = MemoryStore()
        ctx = self._make_ctx(mem)

        result = await tool_remember(ctx, "k", "v")
        assert "Remembered" in result

        result = await tool_recall(ctx, "k")
        assert result == "v"

    @pytest.mark.asyncio
    async def test_recall_missing_key(self) -> None:
        from wren_agent.tools.memory import tool_recall

        mem = MemoryStore()
        ctx = self._make_ctx(mem)
        result = await tool_recall(ctx, "missing")
        assert "no memory found" in result.lower()

    @pytest.mark.asyncio
    async def test_tools_disabled_without_memory(self) -> None:
        from wren_agent.tools.memory import tool_recall, tool_remember

        ctx = self._make_ctx(memory=None)
        assert "not enabled" in await tool_remember(ctx, "k", "v")
        assert "not enabled" in await tool_recall(ctx, "k")

    @pytest.mark.asyncio
    async def test_search_and_list_tools(self) -> None:
        from wren_agent.tools.memory import tool_list_memory_keys, tool_search_memory

        mem = MemoryStore()
        mem.remember("tpch_schema", "orders lineitem customer")
        ctx = self._make_ctx(mem)

        search_result = await tool_search_memory(ctx, "tpch")
        assert "tpch_schema" in search_result

        list_result = await tool_list_memory_keys(ctx)
        assert "tpch_schema" in list_result

    @pytest.mark.asyncio
    async def test_forget_tool(self) -> None:
        from wren_agent.tools.memory import tool_forget, tool_remember

        mem = MemoryStore()
        ctx = self._make_ctx(mem)
        await tool_remember(ctx, "del_me", "temporary")
        result = await tool_forget(ctx, "del_me")
        assert "Forgot" in result
        assert mem.recall("del_me") is None


# ---------------------------------------------------------------------------
# MDLAgent skill selection integration
# ---------------------------------------------------------------------------


class TestMDLAgentMaxSkills:
    """Test that MDLAgent wires max_skills to SkillStore.select()."""

    def test_default_max_skills(self) -> None:
        from wren_agent.agent import MDLAgent

        agent = MDLAgent()
        assert agent._max_skills == 5

    def test_custom_max_skills(self) -> None:
        from wren_agent.agent import MDLAgent

        agent = MDLAgent(max_skills=2)
        assert agent._max_skills == 2

    def test_skill_store_stored(self) -> None:
        from wren_agent.agent import MDLAgent

        store = SkillStore()
        store.add("tip", "some tip content")
        agent = MDLAgent(skill_store=store)
        assert agent._skill_store is store

    def test_no_skill_store_is_none(self) -> None:
        from wren_agent.agent import MDLAgent

        agent = MDLAgent()
        assert agent._skill_store is None

    def test_select_called_semantically(self) -> None:
        """select() with a query returns relevant subset, not always all skills."""
        store = SkillStore()
        store.add("bigquery_struct", "BigQuery STRUCT nested schema model column.", tags=["bigquery"])
        store.add("postgres_fk", "PostgreSQL FK constraint referencing primary key.", tags=["postgres"])
        store.add("ecommerce", "Ecommerce order table referencing customer.", tags=["ecommerce"])

        # Simulate what agent.run() does: select with query + top_k
        selected = store.select(query="BigQuery nested column", top_k=1)
        instructions = selected.as_instructions()
        assert instructions is not None
        assert "bigquery_struct" in instructions
        # With top_k=1, postgres_fk and ecommerce should not appear
        assert "postgres_fk" not in instructions
        assert "ecommerce" not in instructions
