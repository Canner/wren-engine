"""Lightweight SQL classification for store-tip heuristics."""

import sqlglot
from sqlglot import exp


def is_exploratory(sql: str) -> bool:
    """Return True if *sql* looks like an exploratory peek query.

    Exploratory = bare SELECT + no WHERE/GROUP BY/HAVING/aggregate + has LIMIT.
    """
    try:
        parsed = sqlglot.parse(sql)
    except sqlglot.errors.ParseError:
        return False  # can't parse → don't suppress tip

    if len(parsed) != 1 or not isinstance(parsed[0], exp.Select):
        return False  # multi-statement or non-SELECT → not exploratory

    stmt = parsed[0]

    # A CTE-backed SELECT has a With node as parent or sibling — detect via With
    if stmt.find(exp.With) is not None:
        return False  # CTE → not exploratory

    has_where = stmt.find(exp.Where) is not None
    has_group = stmt.find(exp.Group) is not None
    has_having = stmt.find(exp.Having) is not None
    has_limit = stmt.find(exp.Limit) is not None
    has_agg = any(isinstance(node, exp.AggFunc) for node in stmt.walk())

    if has_where or has_group or has_having or has_agg:
        return False  # analytical query → not exploratory

    return has_limit  # bare SELECT + LIMIT = peek
