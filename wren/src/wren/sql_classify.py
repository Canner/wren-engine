"""Lightweight SQL classification for store-tip heuristics."""

import sqlglot
from sqlglot import exp


def is_exploratory(sql: str) -> bool:
    """Return True if *sql* looks like an exploratory peek query.

    Exploratory = bare SELECT + no WHERE/GROUP BY/HAVING/aggregate + has LIMIT.
    Top-level clauses are inspected via stmt.args to avoid descending into
    subqueries (e.g. an inner LIMIT does not make the outer query exploratory).
    """
    try:
        parsed = sqlglot.parse(sql)
    except sqlglot.errors.ParseError:
        return False  # can't parse → don't suppress tip

    if len(parsed) != 1 or not isinstance(parsed[0], exp.Select):
        return False  # multi-statement or non-SELECT → not exploratory

    stmt = parsed[0]

    # A CTE-backed SELECT stores the With clause under the "with_" key
    if stmt.args.get("with_") is not None:
        return False

    # Check only the top-level clause args — does not descend into subqueries
    has_where = stmt.args.get("where") is not None
    has_group = stmt.args.get("group") is not None
    has_having = stmt.args.get("having") is not None
    has_limit = stmt.args.get("limit") is not None

    # find() descends into children but that's fine for aggregates: any
    # aggregate anywhere in the tree indicates non-trivial processing
    has_agg = stmt.find(exp.AggFunc) is not None

    if has_where or has_group or has_having or has_agg:
        return False  # analytical query → not exploratory

    return has_limit  # bare SELECT + LIMIT = peek
