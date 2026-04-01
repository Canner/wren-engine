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
    user_cte_names: set[str],
    config: WrenConfig,
) -> None:
    """Raise ``WrenError`` if the SQL violates strict-mode policies.

    Parameters
    ----------
    ast:
        Parsed sqlglot AST of the user query.
    model_names:
        Set of model names defined in the MDL manifest.
    user_cte_names:
        Set of CTE names defined in the user's SQL (excluded from table checks).
    config:
        Wren configuration with strict_mode and denied_functions settings.
    """
    if config.strict_mode:
        _check_tables(ast, model_names, user_cte_names)
    if config.denied_functions:
        _check_functions(ast, config.denied_functions)


def _check_tables(
    ast: exp.Expression,
    model_names: set[str],
    user_cte_names: set[str],
) -> None:
    model_names_lower = {n.lower() for n in model_names}
    user_cte_names_lower = {n.lower() for n in user_cte_names}
    for table in ast.find_all(exp.Table):
        name = table.name
        if not name:
            continue
        if name.lower() in user_cte_names_lower:
            continue
        if name.lower() not in model_names_lower:
            raise WrenError(
                ErrorCode.MODEL_NOT_FOUND,
                f"Table '{name}' is not defined in the MDL manifest. "
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
