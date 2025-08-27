"""Initialization for the Wren module."""

__version__ = "0.17.2"


import base64
import json

from app import model
from app.model import ConnectionInfo
from app.model.data_source import DataSource
from app.model.error import ErrorCode, WrenError

__all__ = ["Context", "Task", "create_session_context", "model"]


def __dir__() -> list[str]:
    """Adds tab completion for wren to the top-level module."""
    out = set(__all__)
    return sorted(out)


def create_session_context(
    data_source: str | None = None,
    connection_info: ConnectionInfo | None = None,
    mdl_path: str | None = None,
    **kwargs: dict,
):
    """Create a Wren session context.

    Parameters
    ----------
    data_source : str, optional
        The data source to connect to.
    connection_info : dict, optional
        Connection information for the data source.
    mdl_path : str, optional
        Path to the model file.
    **kwargs : dict
        Additional keyword arguments.

    Returns
    -------
    Session
        A Wren session object.
    """
    from .session import Context  # noqa: PLC0415

    if not mdl_path:
        raise WrenError(ErrorCode.GENERIC_USER_ERROR, "mdl_path must be provided")

    if not data_source:
        raise WrenError(ErrorCode.GENERIC_USER_ERROR, "data_source must be provided")

    data_source = DataSource(data_source)

    with open(mdl_path) as f:
        if not f.readable():
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR, f"Cannot read MDL file at {mdl_path}"
            )
        try:
            manifest = json.load(f)
            manifest_base64 = (
                base64.b64encode(json.dumps(manifest).encode("utf-8")).decode("utf-8")
                if manifest
                else None
            )
        except json.JSONDecodeError as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                f"Invalid JSON in MDL file at {mdl_path}: {e}",
            ) from e

    return Context(
        data_source=data_source,
        connection_info=connection_info,
        manifest_base64=manifest_base64,
        **kwargs,
    )
