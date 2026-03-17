"""Unit tests for ClickHouse type mapping in _transform_column_type.

These tests validate that ClickHouse-specific data types are correctly
mapped to RustWrenEngineColumnType, including wrapper types like
LowCardinality() and Nullable().
"""

import pytest

from app.model.metadata.clickhouse import ClickHouseMetadata
from app.model.metadata.dto import RustWrenEngineColumnType


@pytest.mark.clickhouse
class TestTransformColumnType:
    """Tests for ClickHouseMetadata._transform_column_type."""

    @pytest.fixture
    def metadata(self):
        """Create a ClickHouseMetadata instance for testing.

        We use __new__ to skip __init__ since we only need the method.
        """
        instance = object.__new__(ClickHouseMetadata)
        return instance

    # --- Basic type mapping ---

    def test_string(self, metadata):
        assert (
            metadata._transform_column_type("String")
            == RustWrenEngineColumnType.VARCHAR
        )

    def test_int32(self, metadata):
        assert metadata._transform_column_type("Int32") == RustWrenEngineColumnType.INT4

    def test_int64(self, metadata):
        assert metadata._transform_column_type("Int64") == RustWrenEngineColumnType.INT8

    def test_uint8(self, metadata):
        assert metadata._transform_column_type("UInt8") == RustWrenEngineColumnType.INT2

    def test_uint16(self, metadata):
        assert (
            metadata._transform_column_type("UInt16") == RustWrenEngineColumnType.INT2
        )

    def test_uint32(self, metadata):
        assert (
            metadata._transform_column_type("UInt32") == RustWrenEngineColumnType.INT4
        )

    def test_float32(self, metadata):
        assert (
            metadata._transform_column_type("Float32")
            == RustWrenEngineColumnType.FLOAT4
        )

    def test_float64(self, metadata):
        assert (
            metadata._transform_column_type("Float64")
            == RustWrenEngineColumnType.FLOAT8
        )

    def test_uuid(self, metadata):
        assert metadata._transform_column_type("UUID") == RustWrenEngineColumnType.UUID

    def test_date(self, metadata):
        assert metadata._transform_column_type("Date") == RustWrenEngineColumnType.DATE

    def test_datetime(self, metadata):
        assert (
            metadata._transform_column_type("DateTime")
            == RustWrenEngineColumnType.TIMESTAMP
        )

    # --- Bool type (the missing alias) ---

    def test_bool(self, metadata):
        """Bool is a common ClickHouse type that was previously unmapped."""
        assert metadata._transform_column_type("Bool") == RustWrenEngineColumnType.BOOL

    def test_boolean(self, metadata):
        assert (
            metadata._transform_column_type("Boolean") == RustWrenEngineColumnType.BOOL
        )

    # --- Date32 and DateTime64 ---

    def test_date32(self, metadata):
        assert (
            metadata._transform_column_type("Date32") == RustWrenEngineColumnType.DATE
        )

    def test_datetime64_no_tz(self, metadata):
        assert (
            metadata._transform_column_type("DateTime64(3)")
            == RustWrenEngineColumnType.TIMESTAMP
        )

    def test_datetime64_with_tz(self, metadata):
        assert (
            metadata._transform_column_type("DateTime64(3, 'UTC')")
            == RustWrenEngineColumnType.TIMESTAMP
        )

    # --- Large integer types ---

    def test_int128(self, metadata):
        assert (
            metadata._transform_column_type("Int128")
            == RustWrenEngineColumnType.NUMERIC
        )

    def test_int256(self, metadata):
        assert (
            metadata._transform_column_type("Int256")
            == RustWrenEngineColumnType.NUMERIC
        )

    def test_uint128(self, metadata):
        assert (
            metadata._transform_column_type("UInt128")
            == RustWrenEngineColumnType.NUMERIC
        )

    def test_uint256(self, metadata):
        assert (
            metadata._transform_column_type("UInt256")
            == RustWrenEngineColumnType.NUMERIC
        )

    # --- Decimal ---

    def test_decimal(self, metadata):
        assert (
            metadata._transform_column_type("Decimal(15,2)")
            == RustWrenEngineColumnType.DECIMAL
        )

    def test_decimal128(self, metadata):
        assert (
            metadata._transform_column_type("Decimal128(9)")
            == RustWrenEngineColumnType.DECIMAL
        )

    # --- LowCardinality wrapper (the main fix) ---

    def test_lowcardinality_string(self, metadata):
        """LowCardinality(String) should unwrap to VARCHAR."""
        assert (
            metadata._transform_column_type("LowCardinality(String)")
            == RustWrenEngineColumnType.VARCHAR
        )

    def test_lowcardinality_fixedstring(self, metadata):
        assert (
            metadata._transform_column_type("LowCardinality(FixedString(32))")
            == RustWrenEngineColumnType.CHAR
        )

    # --- Nullable wrapper ---

    def test_nullable_uuid(self, metadata):
        assert (
            metadata._transform_column_type("Nullable(UUID)")
            == RustWrenEngineColumnType.UUID
        )

    def test_nullable_float32(self, metadata):
        assert (
            metadata._transform_column_type("Nullable(Float32)")
            == RustWrenEngineColumnType.FLOAT4
        )

    def test_nullable_uint16(self, metadata):
        assert (
            metadata._transform_column_type("Nullable(UInt16)")
            == RustWrenEngineColumnType.INT2
        )

    def test_nullable_string(self, metadata):
        assert (
            metadata._transform_column_type("Nullable(String)")
            == RustWrenEngineColumnType.VARCHAR
        )

    # --- Nested wrappers (LowCardinality + Nullable) ---

    def test_lowcardinality_nullable_string(self, metadata):
        """LowCardinality(Nullable(String)) should recursively unwrap."""
        assert (
            metadata._transform_column_type("LowCardinality(Nullable(String))")
            == RustWrenEngineColumnType.VARCHAR
        )

    # --- FixedString with size ---

    def test_fixedstring_with_size(self, metadata):
        assert (
            metadata._transform_column_type("FixedString(32)")
            == RustWrenEngineColumnType.CHAR
        )

    # --- Complex types ---

    def test_array(self, metadata):
        assert (
            metadata._transform_column_type("Array(String)")
            == RustWrenEngineColumnType.VARCHAR
        )

    def test_map(self, metadata):
        assert (
            metadata._transform_column_type("Map(String, String)")
            == RustWrenEngineColumnType.VARCHAR
        )

    def test_tuple(self, metadata):
        assert (
            metadata._transform_column_type("Tuple(String, Int32)")
            == RustWrenEngineColumnType.VARCHAR
        )

    # --- Enum with values ---

    def test_enum8_with_values(self, metadata):
        assert (
            metadata._transform_column_type("Enum8('a'=1, 'b'=2)")
            == RustWrenEngineColumnType.STRING
        )

    def test_enum16_with_values(self, metadata):
        assert (
            metadata._transform_column_type("Enum16('x'=1, 'y'=2)")
            == RustWrenEngineColumnType.STRING
        )

    # --- Special ---

    def test_nothing(self, metadata):
        assert (
            metadata._transform_column_type("Nothing") == RustWrenEngineColumnType.NULL
        )

    def test_unknown_type_returns_unknown(self, metadata):
        result = metadata._transform_column_type("SomeWeirdType")
        assert result == RustWrenEngineColumnType.UNKNOWN

    # --- Parameterized DateTime (with timezone) ---

    def test_datetime_with_timezone(self, metadata):
        """DateTime('UTC') should map to TIMESTAMP."""
        assert (
            metadata._transform_column_type("DateTime('UTC')")
            == RustWrenEngineColumnType.TIMESTAMP
        )

    def test_datetime_with_named_timezone(self, metadata):
        """DateTime('Europe/Berlin') should map to TIMESTAMP."""
        assert (
            metadata._transform_column_type("DateTime('Europe/Berlin')")
            == RustWrenEngineColumnType.TIMESTAMP
        )

    # --- Parameterized JSON ---

    def test_json_with_options(self, metadata):
        """JSON(max_dynamic_paths=1024) should map to JSON."""
        assert (
            metadata._transform_column_type("JSON(max_dynamic_paths=1024)")
            == RustWrenEngineColumnType.JSON
        )

    def test_json_plain(self, metadata):
        """Plain JSON (no params) should map to JSON."""
        assert metadata._transform_column_type("JSON") == RustWrenEngineColumnType.JSON
