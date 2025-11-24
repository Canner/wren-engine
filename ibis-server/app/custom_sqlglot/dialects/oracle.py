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

        def _dateadd_oracle19c(self, expression: exp.DateAdd) -> str:
            """
            Generate Oracle 19c-compatible date addition.

            Converts INTERVAL expressions to numeric addition or ADD_MONTHS:
            - date + INTERVAL 'n' DAY → date + n
            - date + INTERVAL 'n' MONTH → ADD_MONTHS(date, n)
            - date + INTERVAL 'n' YEAR → ADD_MONTHS(date, n * 12)

            Oracle 19c doesn't support INTERVAL arithmetic syntax (21c+ feature).

            Args:
                expression: DateAdd expression node

            Returns:
                Oracle 19c-compatible SQL string
            """
            date_expr = self.sql(expression, "this")
            interval = expression.expression

            if isinstance(interval, exp.Interval):
                unit = interval.unit.this.upper() if interval.unit else "DAY"
                value = self.sql(interval, "this")

                if unit == "DAY":
                    # date + n days → date + n
                    return f"{date_expr} + {value}"
                
                elif unit == "MONTH":
                    # date + n months → ADD_MONTHS(date, n)
                    # ADD_MONTHS handles month-end edge cases correctly
                    # (e.g., Jan 31 + 1 month = Feb 28/29, not Mar 3)
                    return f"ADD_MONTHS({date_expr}, {value})"
                
                elif unit == "YEAR":
                    # date + n years → ADD_MONTHS(date, n * 12)
                    # Convert years to months and use ADD_MONTHS for consistency
                    # This handles year-end edge cases like leap years correctly
                    return f"ADD_MONTHS({date_expr}, {value} * 12)"
                
                else:
                    # Other units handled in subsequent tasks
                    logger.warning(f"Unsupported INTERVAL unit for Oracle 19c: {unit}")
                    return f"{date_expr} + {value}"  # Fallback

            # Not an INTERVAL expression, use default behavior
            return self.dateadd_sql(expression)

        def _datesub_oracle19c(self, expression: exp.DateSub) -> str:
            """
            Generate Oracle 19c-compatible date subtraction.

            Converts INTERVAL expressions to numeric subtraction or ADD_MONTHS:
            - date - INTERVAL 'n' DAY → date - n
            - date - INTERVAL 'n' MONTH → ADD_MONTHS(date, -n)
            - date - INTERVAL 'n' YEAR → ADD_MONTHS(date, -(n * 12))

            Oracle 19c doesn't support INTERVAL arithmetic syntax (21c+ feature).

            Args:
                expression: DateSub expression node

            Returns:
                Oracle 19c-compatible SQL string
            """
            date_expr = self.sql(expression, "this")
            interval = expression.expression

            if isinstance(interval, exp.Interval):
                unit = interval.unit.this.upper() if interval.unit else "DAY"
                value = self.sql(interval, "this")

                if unit == "DAY":
                    # date - n days → date - n
                    return f"{date_expr} - {value}"
                
                elif unit == "MONTH":
                    # date - n months → ADD_MONTHS(date, -n)
                    # ADD_MONTHS with negative value handles month-end edge cases
                    return f"ADD_MONTHS({date_expr}, -{value})"
                
                elif unit == "YEAR":
                    # date - n years → ADD_MONTHS(date, -(n * 12))
                    # Convert years to months and use ADD_MONTHS for consistency
                    # Parentheses ensure correct order: negate (value * 12), not (-value) * 12
                    return f"ADD_MONTHS({date_expr}, -({value} * 12))"
                
                else:
                    # Other units handled in subsequent tasks
                    logger.warning(f"Unsupported INTERVAL unit for Oracle 19c: {unit}")
                    return f"{date_expr} - {value}"  # Fallback

            # Not an INTERVAL expression, use default behavior
            return self.datesub_sql(expression)
