"""Temporary local web server for browser-based profile creation.

Usage:
    from wren.profile_web import create_app, start

    # For testing:
    app, result = create_app("my-profile")

    # For CLI:
    result = start("my-profile", activate=True)
"""

from __future__ import annotations

import asyncio
import json
import socket
from pathlib import Path
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from wren.model.field_registry import get_datasource_options, get_fields, get_variants

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def create_app(
    profile_name: str,
    activate: bool = False,
) -> tuple[Starlette, dict[str, Any], list]:
    """Create the profile web app.

    Returns:
        (app, result, server_ref) where result dict is populated with
        {"name", "datasource"} after a successful POST /save, and
        server_ref is a mutable list the start() function appends the
        uvicorn.Server instance into so the save handler can trigger shutdown.
    """
    result: dict[str, Any] = {}
    # Mutable reference so the save handler can reach the server instance
    server_ref: list[uvicorn.Server] = []

    async def index(request: Request):
        return templates.TemplateResponse(
            request,
            "profile_form.html",
            {
                "profile_name": profile_name,
                "datasource_options": get_datasource_options(),
            },
        )

    async def fields_endpoint(request: Request):
        ds = request.query_params.get("datasource", "").lower()
        variant = request.query_params.get("_variant") or None
        if not ds:
            return HTMLResponse("")
        variants = get_variants(ds)
        try:
            field_list = get_fields(ds, variant=variant)
        except ValueError:
            field_list = []
        return templates.TemplateResponse(
            request,
            "_profile_fields.html",
            {
                "datasource": ds,
                "fields": field_list,
                "variants": variants,
                "current_variant": variant or (variants[0] if variants else None),
            },
        )

    async def save(request: Request):
        from wren.profile import add_profile  # noqa: PLC0415

        form = await request.form()
        ds = form.get("datasource", "").strip().lower()
        name = form.get("_profile_name", profile_name)
        variant_key = form.get("_variant")

        if not ds:
            return HTMLResponse(
                '<small style="color:var(--pico-color-red-500)">✗ Please select a data source.</small>'
            )

        profile: dict[str, Any] = {"datasource": ds}
        if variant_key:
            profile[f"{ds}_type"] = variant_key

        _INTERNAL = {"datasource", "_profile_name", "_variant", "_json"}
        if "_json" in form and form["_json"].strip():
            try:
                profile.update(json.loads(form["_json"]))
            except json.JSONDecodeError:
                return HTMLResponse(
                    '<small style="color:var(--pico-color-red-500)">✗ Invalid JSON.</small>'
                )
        else:
            for k, v in form.items():
                if k not in _INTERNAL and v.strip():
                    profile[k] = v.strip()

        try:
            add_profile(name, profile, activate=activate)
        except (ValueError, OSError) as exc:
            return HTMLResponse(
                f'<small style="color:var(--pico-color-red-500)">✗ Failed to save profile: {exc}</small>'
            )
        result.update({"name": name, "datasource": ds})

        # Schedule graceful shutdown after response is delivered
        srv = server_ref[0] if server_ref else None
        if srv is not None:
            loop = asyncio.get_running_loop()
            loop.call_later(0.5, lambda: setattr(srv, "should_exit", True))

        return HTMLResponse(
            f'<small style="color:var(--pico-color-green-500)">'
            f"✓ Profile <strong>{name}</strong> saved. You can close this tab.</small>"
        )

    app = Starlette(
        routes=[
            Route("/", index),
            Route("/fields", fields_endpoint),
            Route("/save", save, methods=["POST"]),
        ]
    )
    return app, result, server_ref  # type: ignore[return-value]


class _ProfileServer(uvicorn.Server):
    """Uvicorn server that calls a hook once the server is ready to accept connections."""

    def __init__(self, config: uvicorn.Config, *, on_ready=None) -> None:
        super().__init__(config)
        self._on_ready = on_ready

    async def startup(self, sockets=None):
        await super().startup(sockets=sockets)
        if self.started and self._on_ready:
            self._on_ready()


def _free_port() -> int:
    """Find and return a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(
    profile_name: str,
    *,
    activate: bool = False,
    port: int = 0,
    open_browser: bool = True,
) -> dict[str, Any]:
    """Start the temporary profile web server and block until saved or Ctrl+C.

    Args:
        profile_name: Name for the new profile.
        activate: Set the profile as active after saving.
        port: Port to bind (0 = auto-select free port).
        open_browser: Auto-open the browser when the server is ready.

    Returns:
        {"name": ..., "datasource": ...} on success, or {} on Ctrl+C.
    """
    actual_port = port if port != 0 else _free_port()
    app, result, server_ref = create_app(profile_name, activate=activate)

    def on_ready():
        url = f"http://localhost:{actual_port}"
        if open_browser:
            import webbrowser  # noqa: PLC0415

            webbrowser.open(url)
        else:
            print(f"Profile form available at {url}", flush=True)  # noqa: T201

    config = uvicorn.Config(
        app, host="127.0.0.1", port=actual_port, log_level="warning"
    )
    server = _ProfileServer(config, on_ready=on_ready)
    server_ref.append(server)

    try:
        server.run()
    except KeyboardInterrupt:
        pass

    return result
