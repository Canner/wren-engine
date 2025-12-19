from loguru import logger
from sqlglot import exp
from sqlglot.dialects.oracle import Oracle as OriginalOracle


class Oracle(OriginalOracle):
    """
    Custom Oracle dialect for Oracle 19c compatibility.

    Overrides SQLGlot's default Oracle dialect to fix specific Oracle 19c issues:
    - TIMESTAMPTZ → TIMESTAMP type mapping (avoids timezone format issues)
    - CAST timestamp literals with explicit TO_TIMESTAMP format (fixes ORA-01843)
    - BOOLEAN → CHAR(1) type mapping (user's boolean representation pattern)

    Note: INTERVAL syntax is fully supported in Oracle 19c and does not need transformation.

    Based on SQLGlot version >=23.4,<26.5
    """

    class Generator(OriginalOracle.Generator):
        """Custom generator for Oracle 19c SQL syntax."""

        TYPE_MAPPING = {
            **OriginalOracle.Generator.TYPE_MAPPING,
            # Oracle 19c doesn't have native BOOLEAN type (21c+ feature)
            # Map to CHAR(1) to match our 'Y'/'N' boolean representation pattern
            exp.DataType.Type.BOOLEAN: "CHAR(1)",
            # Map TIMESTAMPTZ to TIMESTAMP (without timezone) for Oracle 19c
            # Avoids format conversion issues with TIMESTAMP WITH TIME ZONE
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
        }

        TRANSFORMS = {
            **OriginalOracle.Generator.TRANSFORMS,
            # Handle CAST to TIMESTAMP with explicit format for string literals (ORA-01843 fix)
            exp.Cast: lambda self, e: self._handle_cast_oracle19c(e),
        }

        def __init__(self, *args, **kwargs):
            """Initialize Oracle 19c generator with logging."""
            super().__init__(*args, **kwargs)
            logger.debug("Using custom Oracle 19c dialect for SQL generation")

        def _handle_cast_oracle19c(self, expression: exp.Cast) -> str:
            """
            Handle CAST expressions for Oracle 19c timestamp compatibility.

            Oracle 19c cannot implicitly convert string literals like '2025-11-24 00:00:00'
            when casting to TIMESTAMP. This transform converts:

            CAST('2025-11-24 00:00:00' AS TIMESTAMP)
            → TO_TIMESTAMP('2025-11-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS')

            Only applies when:
            - Source is a string literal
            - Target type is TIMESTAMP or DATE
            - Literal matches YYYY-MM-DD pattern

            Args:
                expression: Cast expression node

            Returns:
                Oracle 19c-compatible SQL string
            """
            source = expression.this
            target_type = expression.to

            # Check if we're casting to TIMESTAMP or DATE
            if target_type and target_type.this in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.DATE):
                # Check if source is a string literal
                if isinstance(source, exp.Literal) and source.is_string:
                    literal_value = source.this

                    # Check if it matches YYYY-MM-DD pattern (with or without time)
                    # Pattern: YYYY-MM-DD or YYYY-MM-DD HH:MI:SS
                    if literal_value and len(literal_value) >= 10 and literal_value[4] == '-' and literal_value[7] == '-':
                        # Determine format based on length
                        if len(literal_value) == 10:
                            # Just date: YYYY-MM-DD
                            format_mask = 'YYYY-MM-DD'
                        elif len(literal_value) == 19:
                            # Date with time: YYYY-MM-DD HH:MI:SS
                            format_mask = 'YYYY-MM-DD HH24:MI:SS'
                        else:
                            # Other length, use default CAST
                            return self.cast_sql(expression)

                        # Use TO_TIMESTAMP or TO_DATE with explicit format
                        func_name = "TO_TIMESTAMP" if target_type.this == exp.DataType.Type.TIMESTAMP else "TO_DATE"
                        return f"{func_name}('{literal_value}', '{format_mask}')"

            # For all other cases, use default CAST behavior
            return self.cast_sql(expression)
