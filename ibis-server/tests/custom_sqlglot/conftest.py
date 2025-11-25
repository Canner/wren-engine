"""
Pytest configuration for SQLGlot dialect tests.

These tests run in isolation without requiring the full WrenAI application.
This conftest.py is used instead of the app-level conftest.py which requires
full application setup (WREN_ENGINE_ENDPOINT, etc.).
"""

import pytest
import sqlglot
from app.custom_sqlglot.dialects.oracle import Oracle


@pytest.fixture
def oracle_dialect():
    """Return Oracle 19c dialect class for testing."""
    return Oracle


@pytest.fixture
def oracle_generator():
    """Return Oracle 19c generator instance for testing."""
    return Oracle.Generator()


@pytest.fixture
def oracle_type_mapping():
    """Return Oracle 19c TYPE_MAPPING dictionary."""
    return Oracle.Generator.TYPE_MAPPING


@pytest.fixture
def base_oracle_type_mapping():
    """Return base Oracle TYPE_MAPPING for comparison."""
    from sqlglot.dialects.oracle import Oracle as OriginalOracle
    return OriginalOracle.Generator.TYPE_MAPPING


@pytest.fixture
def transpile_trino_to_oracle():
    """
    Helper fixture to transpile Trino SQL to Oracle 19c.

    Returns:
        Callable that takes SQL string and returns transpiled result.

    Example:
        def test_something(transpile_trino_to_oracle):
            result = transpile_trino_to_oracle("SELECT * FROM users")
            assert "SELECT" in result
    """
    def _transpile(sql: str) -> str:
        """Transpile Trino SQL to Oracle 19c SQL."""
        return sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    return _transpile
