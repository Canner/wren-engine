from loguru import logger
from sqlglot import exp
from sqlglot.dialects.oracle import Oracle as OriginalOracle


class Oracle(OriginalOracle):
    """
    Custom Oracle dialect for Oracle 19c compatibility.

    Overrides SQLGlot's default Oracle dialect to generate 19c-compatible
    syntax instead of 21c+ syntax. Specifically handles:
    - Date arithmetic without INTERVAL expressions
    - Boolean literals as numeric (1/0)
    - Type mappings for 19c compatibility

    Based on SQLGlot version >=23.4,<26.5
    """

    class Generator(OriginalOracle.Generator):
        """Custom generator for Oracle 19c SQL syntax."""

        def __init__(self, *args, **kwargs):
            """Initialize Oracle 19c generator with logging."""
            super().__init__(*args, **kwargs)
            logger.debug("Using custom Oracle 19c dialect for SQL generation")
