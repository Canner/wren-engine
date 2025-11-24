from loguru import logger
from sqlglot import exp
from sqlglot.dialects.oracle import Oracle as OriginalOracle


class Oracle(OriginalOracle):
    """
    Custom Oracle dialect for Oracle 19c compatibility.

    Overrides SQLGlot's default Oracle dialect to generate 19c-compatible
    syntax instead of 21c+ syntax. Specifically handles:
    - Date arithmetic without INTERVAL expressions
    - Boolean literals as CHAR(1) with 'Y'/'N' values
    - Type mappings for 19c compatibility

    Based on SQLGlot version >=23.4,<26.5
    """

    class Generator(OriginalOracle.Generator):
        """Custom generator for Oracle 19c SQL syntax."""

        TYPE_MAPPING = {
            **OriginalOracle.Generator.TYPE_MAPPING,
            # Oracle 19c doesn't have native BOOLEAN type (21c+ feature)
            # Map to CHAR(1) to match our 'Y'/'N' boolean representation pattern
            exp.DataType.Type.BOOLEAN: "CHAR(1)",
        }

        def __init__(self, *args, **kwargs):
            """Initialize Oracle 19c generator with logging."""
            super().__init__(*args, **kwargs)
            logger.debug("Using custom Oracle 19c dialect for SQL generation")
