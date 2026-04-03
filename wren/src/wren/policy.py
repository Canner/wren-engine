"""SQL policy validation for strict query mode.

Validates that a parsed SQL AST only references tables defined in the MDL
manifest and does not use any denied functions.
"""

from __future__ import annotations

from sqlglot import exp

from wren.config import WrenConfig
from wren.model.error import ErrorCode, ErrorPhase, WrenError


def validate_sql_policy(
    ast: exp.Expression,
    model_names: set[str],
    config: WrenConfig,
) -> None:
    """Raise ``WrenError`` if the SQL violates strict-mode policies.

    Parameters
    ----------
    ast:
        Parsed sqlglot AST of the user query.
    model_names:
        Set of model names defined in the MDL manifest.
    config:
        Wren configuration with strict_mode and denied_functions settings.
    """
    if config.strict_mode:
        _check_tables(ast, model_names)
    if config.denied_functions:
        _check_functions(ast, config.denied_functions)


def _visible_cte_names(node: exp.Expression) -> set[str]:
    """Return CTE names visible at *node*'s scope by walking up the AST."""
    names: set[str] = set()
    cursor = node.parent
    while cursor is not None:
        # A WITH clause is visible to its parent SELECT and siblings.
        with_clause = cursor.args.get("with_") if hasattr(cursor, "args") else None
        if isinstance(with_clause, exp.With):
            for cte in with_clause.expressions:
                alias = cte.args.get("alias")
                if alias:
                    cte_name = (
                        alias.this.name
                        if isinstance(alias.this, exp.Identifier)
                        else str(alias.this)
                    )
                    names.add(cte_name.lower())
        cursor = cursor.parent
    return names


def _check_tables(
    ast: exp.Expression,
    model_names: set[str],
) -> None:
    model_names_lower = {n.lower() for n in model_names}

    for table in ast.find_all(exp.Table):
        name = table.name
        if not name:
            # Table nodes with no name are table-valued functions
            # (e.g. read_csv(), generate_series()). Block them in strict mode.
            sql_text = table.sql()
            if sql_text:
                raise WrenError(
                    ErrorCode.MODEL_NOT_FOUND,
                    f"Table-valued function '{sql_text}' is not allowed. "
                    "In strict mode, all table references must correspond to MDL models.",
                    phase=ErrorPhase.SQL_POLICY_CHECK,
                )
            continue
        name_lower = name.lower()
        if name_lower in model_names_lower:
            continue
        if name_lower in _visible_cte_names(table):
            continue
        raise WrenError(
            ErrorCode.MODEL_NOT_FOUND,
            f"Table '{name}' is not defined in the MDL manifest. "
            "In strict mode, all table references must correspond to MDL models.",
            phase=ErrorPhase.SQL_POLICY_CHECK,
        )

    # Func subclasses used as FROM sources (e.g. UNNEST) produce no exp.Table
    # node at all. Scan for Func nodes inside From clauses.
    for from_clause in ast.find_all(exp.From):
        source = from_clause.this
        if isinstance(source, exp.Alias):
            source = source.this
        if isinstance(source, exp.Func):
            raise WrenError(
                ErrorCode.MODEL_NOT_FOUND,
                f"Table-valued function '{source.sql()}' is not allowed. "
                "In strict mode, all table references must correspond to MDL models.",
                phase=ErrorPhase.SQL_POLICY_CHECK,
            )


def _check_functions(
    ast: exp.Expression,
    denied: frozenset[str],
) -> None:
    for func in ast.find_all(exp.Func):
        if isinstance(func, exp.Anonymous):
            name = func.name
        else:
            name = type(func).key
        if name.lower() in denied:
            raise WrenError(
                ErrorCode.BLOCKED_FUNCTION,
                f"Function '{name}' is not allowed. "
                "This function is on the denied list.",
                phase=ErrorPhase.SQL_POLICY_CHECK,
            )
