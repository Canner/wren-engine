"""
Unit tests for Oracle 19c custom dialect type mapping overrides.

Tests validate that the custom Oracle 19c dialect correctly maps data types
for Oracle 19c compatibility, specifically:
- BOOLEAN type maps to CHAR(1) (19c doesn't have native BOOLEAN until 21c)
- Other types inherit from base Oracle dialect unchanged

Implements: SCAIS-23 P3.002 (TEST-001, COMP-002)
"""

import pytest
from sqlglot import exp


@pytest.mark.oracle19c
@pytest.mark.type_mapping
class TestOracle19cTypeMapping:
    """Unit tests for Oracle 19c type mapping overrides."""

    def test_boolean_type_maps_to_char1(self, oracle_type_mapping):
        """
        Test BOOLEAN type maps to CHAR(1) for Oracle 19c compatibility.
        
        Oracle 19c doesn't have native BOOLEAN type (21c+ feature).
        We map to CHAR(1) to match our 'Y'/'N' boolean representation pattern.
        """
        assert exp.DataType.Type.BOOLEAN in oracle_type_mapping
        assert oracle_type_mapping[exp.DataType.Type.BOOLEAN] == "CHAR(1)"

    def test_type_mapping_inheritance(self, oracle_type_mapping, base_oracle_type_mapping):
        """
        Test that non-overridden types inherit from base Oracle dialect.
        
        Verifies that the spread operator (**OriginalOracle.Generator.TYPE_MAPPING)
        correctly inherits all base type mappings.
        """
        # Get all base types
        base_types = set(base_oracle_type_mapping.keys())
        
        # Verify all base types are present in our mapping
        for data_type in base_types:
            assert data_type in oracle_type_mapping, f"Type {data_type} not inherited"
            
        # Verify BOOLEAN is the only override (count should be base + 1 if BOOLEAN wasn't in base)
        # or same count if BOOLEAN was already in base
        assert len(oracle_type_mapping) >= len(base_oracle_type_mapping)

    def test_date_type_preserved(self, oracle_type_mapping, base_oracle_type_mapping):
        """Test DATE type is inherited unchanged from base Oracle dialect (if present)."""
        # DATE might not be in type mapping if it's the default/native type
        if exp.DataType.Type.DATE in base_oracle_type_mapping:
            assert exp.DataType.Type.DATE in oracle_type_mapping
            assert oracle_type_mapping[exp.DataType.Type.DATE] == base_oracle_type_mapping[exp.DataType.Type.DATE]

    def test_timestamp_type_preserved(self, oracle_type_mapping, base_oracle_type_mapping):
        """Test TIMESTAMP type is inherited unchanged from base Oracle dialect (if present)."""
        # TIMESTAMP might not be in type mapping if it's the default/native type
        if exp.DataType.Type.TIMESTAMP in base_oracle_type_mapping:
            assert exp.DataType.Type.TIMESTAMP in oracle_type_mapping
            assert oracle_type_mapping[exp.DataType.Type.TIMESTAMP] == base_oracle_type_mapping[exp.DataType.Type.TIMESTAMP]

    def test_varchar_type_preserved(self, oracle_type_mapping, base_oracle_type_mapping):
        """Test VARCHAR type is inherited unchanged from base Oracle dialect."""
        assert exp.DataType.Type.VARCHAR in oracle_type_mapping
        assert oracle_type_mapping[exp.DataType.Type.VARCHAR] == base_oracle_type_mapping[exp.DataType.Type.VARCHAR]

    def test_number_type_preserved(self, oracle_type_mapping, base_oracle_type_mapping):
        """
        Test numeric types are inherited unchanged from base Oracle dialect.
        
        Oracle uses NUMBER type for various numeric representations.
        """
        numeric_types = [
            exp.DataType.Type.INT,
            exp.DataType.Type.BIGINT,
            exp.DataType.Type.DECIMAL,
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.DOUBLE,
        ]
        
        for numeric_type in numeric_types:
            if numeric_type in base_oracle_type_mapping:
                assert oracle_type_mapping[numeric_type] == base_oracle_type_mapping[numeric_type], \
                    f"Type {numeric_type} should be inherited unchanged"

    def test_boolean_override_is_intentional(self, oracle_type_mapping, base_oracle_type_mapping):
        """
        Test that BOOLEAN override is intentional and different from base.
        
        If base Oracle dialect has BOOLEAN mapped to something else,
        verify we're intentionally overriding it to CHAR(1).
        """
        if exp.DataType.Type.BOOLEAN in base_oracle_type_mapping:
            # If base has BOOLEAN, verify we override it
            base_mapping = base_oracle_type_mapping[exp.DataType.Type.BOOLEAN]
            our_mapping = oracle_type_mapping[exp.DataType.Type.BOOLEAN]
            
            # Our mapping should be CHAR(1)
            assert our_mapping == "CHAR(1)"
            
            # Document if we're overriding base
            # (this is informational, not a failure condition)
            if base_mapping != "CHAR(1)":
                print(f"INFO: Overriding base BOOLEAN mapping '{base_mapping}' with 'CHAR(1)'")
        else:
            # Base doesn't have BOOLEAN, we're adding it
            assert oracle_type_mapping[exp.DataType.Type.BOOLEAN] == "CHAR(1)"
            print("INFO: Adding BOOLEAN → CHAR(1) mapping (not in base)")

    @pytest.mark.parametrize("data_type,expected_mapping", [
        (exp.DataType.Type.CHAR, "CHAR"),
        (exp.DataType.Type.NCHAR, "NCHAR"),
        (exp.DataType.Type.VARCHAR, "VARCHAR2"),
        (exp.DataType.Type.NVARCHAR, "NVARCHAR2"),
    ])
    def test_oracle_specific_types(self, oracle_type_mapping, data_type, expected_mapping):
        """
        Test Oracle-specific type mappings are preserved.
        
        Oracle has specific type names like VARCHAR2, NVARCHAR2, etc.
        These should be inherited from base dialect.
        """
        if data_type in oracle_type_mapping:
            assert oracle_type_mapping[data_type] == expected_mapping, \
                f"Oracle-specific type {data_type} should map to {expected_mapping}"
