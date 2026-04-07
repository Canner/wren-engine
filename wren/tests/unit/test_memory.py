"""Unit tests for the wren.memory module."""

from __future__ import annotations

import pytest

from wren.memory.schema_indexer import (
    describe_schema,
    extract_schema_items,
    manifest_hash,
)

# ── Fixtures ──────────────────────────────────────────────────────────────

_MANIFEST = {
    "catalog": "test",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": "test.public.orders",
            "primaryKey": "o_orderkey",
            "columns": [
                {"name": "o_orderkey", "type": "varchar", "isCalculated": False},
                {"name": "o_custkey", "type": "varchar", "isCalculated": False},
                {
                    "name": "o_totalprice",
                    "type": "double",
                    "isCalculated": True,
                    "expression": "sum(l_extendedprice)",
                },
                {
                    "name": "customer",
                    "type": "varchar",
                    "isCalculated": False,
                    "relationship": "orders_customer",
                },
            ],
        },
        {
            "name": "customer",
            "tableReference": "test.public.customer",
            "primaryKey": "c_custkey",
            "columns": [
                {"name": "c_custkey", "type": "varchar", "isCalculated": False},
                {"name": "c_name", "type": "varchar", "isCalculated": False},
            ],
            "properties": {"description": "Customer master data"},
        },
    ],
    "relationships": [
        {
            "name": "orders_customer",
            "models": ["orders", "customer"],
            "joinType": "many_to_one",
            "condition": "orders.o_custkey = customer.c_custkey",
        }
    ],
    "views": [
        {
            "name": "top_customers",
            "statement": "SELECT c_name, sum(o_totalprice) FROM orders JOIN customer LIMIT 10",
        }
    ],
}


# ── manifest_hash tests ───────────────────────────────────────────────────


@pytest.mark.unit
class TestManifestHash:
    def test_deterministic(self):
        h1 = manifest_hash(_MANIFEST)
        h2 = manifest_hash(_MANIFEST)
        assert h1 == h2
        assert len(h1) == 16

    def test_changes_on_modification(self):
        modified = {**_MANIFEST, "catalog": "other"}
        assert manifest_hash(_MANIFEST) != manifest_hash(modified)


# ── extract_schema_items tests ────────────────────────────────────────────


@pytest.mark.unit
class TestExtractSchemaItems:
    def test_total_count(self):
        items = extract_schema_items(_MANIFEST)
        # 2 models + 6 columns + 1 relationship + 1 view = 10
        assert len(items) == 10

    def test_item_types(self):
        items = extract_schema_items(_MANIFEST)
        types = {item["item_type"] for item in items}
        assert types == {"model", "column", "relationship", "view"}

    def test_model_record(self):
        items = extract_schema_items(_MANIFEST)
        models = [i for i in items if i["item_type"] == "model"]
        assert len(models) == 2
        orders = next(m for m in models if m["item_name"] == "orders")
        assert "o_orderkey" in orders["text"]
        assert orders["model_name"] == "orders"

    def test_model_with_description(self):
        items = extract_schema_items(_MANIFEST)
        models = [i for i in items if i["item_type"] == "model"]
        customer = next(m for m in models if m["item_name"] == "customer")
        assert "Customer master data" in customer["text"]

    def test_column_calculated(self):
        items = extract_schema_items(_MANIFEST)
        cols = [i for i in items if i["item_type"] == "column"]
        calc = next(c for c in cols if c["item_name"] == "o_totalprice")
        assert calc["is_calculated"] is True
        assert "sum(l_extendedprice)" in calc["text"]
        assert calc["expression"] == "sum(l_extendedprice)"

    def test_column_relationship(self):
        items = extract_schema_items(_MANIFEST)
        cols = [i for i in items if i["item_type"] == "column"]
        rel_col = next(c for c in cols if c["item_name"] == "customer")
        assert "orders_customer" in rel_col["text"]

    def test_relationship_record(self):
        items = extract_schema_items(_MANIFEST)
        rels = [i for i in items if i["item_type"] == "relationship"]
        assert len(rels) == 1
        r = rels[0]
        assert r["item_name"] == "orders_customer"
        assert "many_to_one" in r["text"]

    def test_view_record(self):
        items = extract_schema_items(_MANIFEST)
        views = [i for i in items if i["item_type"] == "view"]
        assert len(views) == 1
        assert "top_customers" in views[0]["text"]

    def test_all_items_have_required_keys(self):
        items = extract_schema_items(_MANIFEST)
        required = {
            "text",
            "item_type",
            "model_name",
            "item_name",
            "mdl_hash",
            "indexed_at",
        }
        for item in items:
            assert required.issubset(item.keys()), f"Missing keys in {item}"

    def test_empty_manifest(self):
        items = extract_schema_items({})
        assert items == []

    def test_manifest_without_optional_sections(self):
        minimal = {"models": [{"name": "t1", "columns": []}]}
        items = extract_schema_items(minimal)
        assert len(items) == 1
        assert items[0]["item_type"] == "model"


# ── describe_schema tests ─────────────────────────────────────────────────


@pytest.mark.unit
class TestDescribeSchema:
    def test_contains_model_names(self):
        text = describe_schema(_MANIFEST)
        assert "### Model: orders" in text
        assert "### Model: customer" in text

    def test_contains_columns(self):
        text = describe_schema(_MANIFEST)
        assert "o_orderkey (varchar)" in text
        assert "o_totalprice (double)" in text

    def test_contains_calculated_expression(self):
        text = describe_schema(_MANIFEST)
        assert "[calculated: sum(l_extendedprice)]" in text

    def test_contains_relationship_column(self):
        text = describe_schema(_MANIFEST)
        assert "[relationship: orders_customer]" in text

    def test_contains_relationship_section(self):
        text = describe_schema(_MANIFEST)
        assert "### Relationship: orders_customer" in text
        assert "many_to_one" in text

    def test_contains_view(self):
        text = describe_schema(_MANIFEST)
        assert "### View: top_customers" in text

    def test_contains_description(self):
        text = describe_schema(_MANIFEST)
        assert "Customer master data" in text

    def test_contains_primary_key(self):
        text = describe_schema(_MANIFEST)
        assert "Primary key: o_orderkey" in text

    def test_excludes_table_reference(self):
        text = describe_schema(_MANIFEST)
        assert "tableReference" not in text
        assert "test.public.orders" not in text
        assert "test.public.customer" not in text

    def test_empty_manifest(self):
        text = describe_schema({})
        assert text == ""

    def test_return_type_is_string(self):
        text = describe_schema(_MANIFEST)
        assert isinstance(text, str)
        assert len(text) > 0


# ── MemoryStore integration tests ─────────────────────────────────────────
# These require lancedb + sentence-transformers (wren[memory] extra).


@pytest.fixture
def memory_store(tmp_path):
    """Create a MemoryStore backed by a temp directory."""
    pytest.importorskip("lancedb", reason="wren[memory] extras not installed")
    pytest.importorskip(
        "sentence_transformers", reason="wren[memory] extras not installed"
    )

    from wren.memory.store import MemoryStore  # noqa: PLC0415

    return MemoryStore(path=tmp_path)


@pytest.mark.unit
class TestMemoryStore:
    def test_index_and_context(self, memory_store):
        result = memory_store.index_schema(_MANIFEST)
        assert result["schema_items"] == 10

        # Small schema → full strategy
        ctx = memory_store.get_context(_MANIFEST, "customer order price")
        assert ctx["strategy"] == "full"
        assert "### Model: orders" in ctx["schema"]

    def test_context_search_strategy(self, memory_store):
        memory_store.index_schema(_MANIFEST)
        result = memory_store.get_context(_MANIFEST, "customer orders", threshold=10)
        assert result["strategy"] == "search"
        assert "results" in result
        assert len(result["results"]) > 0
        assert "text" in result["results"][0]

    def test_context_search_with_type_filter(self, memory_store):
        memory_store.index_schema(_MANIFEST)
        result = memory_store.get_context(
            _MANIFEST, "order", item_type="model", threshold=10
        )
        assert result["strategy"] == "search"
        assert all(r["item_type"] == "model" for r in result["results"])

    def test_context_search_with_model_filter(self, memory_store):
        memory_store.index_schema(_MANIFEST)
        result = memory_store.get_context(
            _MANIFEST, "price", model_name="orders", threshold=10
        )
        assert result["strategy"] == "search"
        assert all(r["model_name"] == "orders" for r in result["results"])

    def test_context_empty_store(self, memory_store):
        result = memory_store.get_context(_MANIFEST, "anything", threshold=10)
        assert result["strategy"] == "search"
        assert result["results"] == []

    def test_schema_is_current(self, memory_store):
        memory_store.index_schema(_MANIFEST)
        assert memory_store.schema_is_current(_MANIFEST) is True

        modified = {**_MANIFEST, "catalog": "changed"}
        assert memory_store.schema_is_current(modified) is False

    def test_store_and_recall_query(self, memory_store):
        memory_store.store_query(
            nl_query="show top customers by revenue",
            sql_query="SELECT c_name, sum(o_totalprice) FROM orders GROUP BY 1 ORDER BY 2 DESC",
            datasource="postgres",
        )
        results = memory_store.recall_queries("best customers", limit=1)
        assert len(results) == 1
        assert "top customers" in results[0]["nl_query"]
        assert "SELECT" in results[0]["sql_query"]

    def test_recall_empty_store(self, memory_store):
        results = memory_store.recall_queries("anything")
        assert results == []

    def test_status(self, memory_store):
        info = memory_store.status()
        assert "path" in info
        assert "tables" in info

        memory_store.index_schema(_MANIFEST)
        info = memory_store.status()
        assert info["tables"]["schema_items"] == 10

    def test_reset(self, memory_store):
        memory_store.index_schema(_MANIFEST)
        memory_store.reset()
        info = memory_store.status()
        assert info["tables"] == {}

    def test_describe_schema_static(self, memory_store):
        text = memory_store.describe_schema(_MANIFEST)
        assert "### Model: orders" in text

    def test_index_replace(self, memory_store):
        memory_store.index_schema(_MANIFEST)
        result = memory_store.index_schema(_MANIFEST, replace=True)
        assert result["schema_items"] == 10
        info = memory_store.status()
        assert info["tables"]["schema_items"] == 10


# ── WrenMemory public API tests ───────────────────────────────────────────


@pytest.fixture
def wren_memory(tmp_path):
    """Create a WrenMemory instance backed by a temp directory."""
    pytest.importorskip("lancedb", reason="wren[memory] extras not installed")
    pytest.importorskip(
        "sentence_transformers", reason="wren[memory] extras not installed"
    )

    from wren.memory import WrenMemory  # noqa: PLC0415

    return WrenMemory(path=tmp_path)


@pytest.mark.unit
class TestWrenMemory:
    def test_full_lifecycle(self, wren_memory):
        result = wren_memory.index_manifest(_MANIFEST)
        assert result["schema_items"] == 10

        ctx = wren_memory.get_context(_MANIFEST, "customer")
        assert ctx["strategy"] == "full"
        assert "### Model: customer" in ctx["schema"]

        wren_memory.store_query(
            nl_query="find expensive orders",
            sql_query="SELECT * FROM orders WHERE o_totalprice > 1000",
        )
        recalled = wren_memory.recall_queries("costly orders")
        assert len(recalled) >= 1
        assert any(r["nl_query"] == "find expensive orders" for r in recalled)

        assert wren_memory.schema_is_current(_MANIFEST)

        status = wren_memory.status()
        assert status["tables"]["schema_items"] == 10
        # query_history has 1 user query + seed queries
        assert status["tables"]["query_history"] >= 1

        wren_memory.reset()
        assert wren_memory.status()["tables"] == {}


# ── MemoryStore seed lifecycle tests ─────────────────────────────────────


@pytest.mark.unit
class TestMemoryStoreSeedLifecycle:
    def test_index_schema_seeds_query_history(self, memory_store):
        from wren.memory.seed_queries import SEED_TAG  # noqa: PLC0415

        result = memory_store.index_schema(_MANIFEST, seed_queries=True)
        assert result["seed_queries"] > 0

        import pandas as pd  # noqa: PLC0415

        table = memory_store._db.open_table("query_history")
        df = table.to_pandas()
        seeds = df[df["tags"] == SEED_TAG]
        assert len(seeds) == result["seed_queries"]

    def test_index_schema_no_seed_flag(self, memory_store):
        result = memory_store.index_schema(_MANIFEST, seed_queries=False)
        assert result["seed_queries"] == 0
        from wren.memory.store import _table_names  # noqa: PLC0415

        assert "query_history" not in _table_names(memory_store._db)

    def test_reindex_replaces_seeds_preserves_user_queries(self, memory_store):
        from wren.memory.seed_queries import SEED_TAG  # noqa: PLC0415

        # Index once to create seeds
        first = memory_store.index_schema(_MANIFEST, seed_queries=True)
        seed_count = first["seed_queries"]
        assert seed_count > 0

        # Store a user-confirmed query (no seed tag)
        memory_store.store_query(
            nl_query="show me the most expensive orders",
            sql_query="SELECT * FROM orders ORDER BY o_totalprice DESC LIMIT 10",
        )

        import pandas as pd  # noqa: PLC0415

        table = memory_store._db.open_table("query_history")
        total_before = table.count_rows()
        assert total_before == seed_count + 1

        # Re-index — seeds should be replaced, user entry preserved
        second = memory_store.index_schema(_MANIFEST, seed_queries=True)
        assert second["seed_queries"] == seed_count

        table = memory_store._db.open_table("query_history")
        df = table.to_pandas()
        seeds = df[df["tags"] == SEED_TAG]
        user_rows = df[df["tags"] != SEED_TAG]
        assert len(seeds) == seed_count
        assert len(user_rows) == 1
        assert "expensive orders" in user_rows.iloc[0]["nl_query"]

    def test_recall_returns_seed_entries(self, memory_store):
        from wren.memory.seed_queries import SEED_TAG  # noqa: PLC0415

        memory_store.index_schema(_MANIFEST, seed_queries=True)
        results = memory_store.recall_queries("list all orders", limit=5)
        assert len(results) > 0
        # At least one result should be a seed entry
        tags = [r.get("tags", "") for r in results]
        assert any(t == SEED_TAG for t in tags)

    def test_index_schema_returns_dict(self, memory_store):
        result = memory_store.index_schema(_MANIFEST)
        assert isinstance(result, dict)
        assert "schema_items" in result
        assert "seed_queries" in result
        assert result["schema_items"] == 10
