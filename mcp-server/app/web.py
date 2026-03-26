import asyncio
import json
import os
import threading
from pathlib import Path
from typing import Callable

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.routing import Route
from starlette.templating import Jinja2Templates

try:
    from utils import is_docker
except ImportError:
    from app.utils import is_docker

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Fields definition for known datasources (name, label, input type, placeholder)
DATASOURCE_FIELDS: dict[str, list[dict]] = {
    "POSTGRES": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "5432"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "postgres"},
        {"name": "user", "label": "User", "type": "text", "placeholder": "postgres"},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "MYSQL": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "3306"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "mydb"},
        {"name": "user", "label": "User", "type": "text", "placeholder": "root"},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "DUCKDB": [
        {"name": "url", "label": "Directory Path", "type": "text", "placeholder": "/data", "hint": "Path to a directory containing .duckdb files, not the .duckdb file itself."},
        {"name": "format", "label": "Format", "type": "hidden", "value": "duckdb"},
    ],
    "BIGQUERY": [
        {"name": "project_id", "label": "Project ID", "type": "text", "placeholder": "my-gcp-project"},
        {"name": "dataset_id", "label": "Dataset", "type": "text", "placeholder": "my_dataset"},
        {"name": "credentials", "label": "Service Account JSON", "type": "file_base64", "accept": ".json", "hint": "Upload your GCP service account credentials.json file. It will be base64-encoded automatically."},
    ],
    "SNOWFLAKE": [
        {"name": "user", "label": "User", "type": "text", "placeholder": ""},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
        {"name": "account", "label": "Account", "type": "text", "placeholder": "xy12345.us-east-1"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": ""},
        {"name": "schema", "label": "Schema", "type": "text", "placeholder": "PUBLIC"},
        {"name": "warehouse", "label": "Warehouse", "type": "text", "placeholder": ""},
    ],
    "CLICKHOUSE": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "8123"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "default"},
        {"name": "user", "label": "User", "type": "text", "placeholder": "default"},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "TRINO": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "8080"},
        {"name": "catalog", "label": "Catalog", "type": "text", "placeholder": ""},
        {"name": "schema", "label": "Schema", "type": "text", "placeholder": ""},
        {"name": "user", "label": "User", "type": "text", "placeholder": ""},
    ],
    "MSSQL": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "1433"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "master"},
        {"name": "user", "label": "User", "type": "text", "placeholder": "sa"},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "ORACLE": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "1521"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "orcl"},
        {"name": "user", "label": "User", "type": "text", "placeholder": "admin"},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "REDSHIFT": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": ""},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "5439"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "dev"},
        {"name": "user", "label": "User", "type": "text", "placeholder": ""},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "ATHENA": [
        {"name": "s3_staging_dir", "label": "S3 Staging Dir", "type": "text", "placeholder": "s3://my-bucket/staging/"},
        {"name": "region_name", "label": "Region", "type": "text", "placeholder": "us-east-1"},
        {"name": "aws_access_key_id", "label": "Access Key ID", "type": "text", "placeholder": ""},
        {"name": "aws_secret_access_key", "label": "Secret Access Key", "type": "password", "placeholder": ""},
    ],
    "DORIS": [
        {"name": "host", "label": "Host", "type": "text", "placeholder": "localhost"},
        {"name": "port", "label": "Port", "type": "text", "placeholder": "9030"},
        {"name": "database", "label": "Database", "type": "text", "placeholder": "default"},
        {"name": "user", "label": "User", "type": "text", "placeholder": "root"},
        {"name": "password", "label": "Password", "type": "password", "placeholder": ""},
    ],
    "LOCAL_FILE": [
        {"name": "url", "label": "Root Path", "type": "text", "placeholder": "/data"},
        {"name": "format", "label": "Format", "type": "text", "placeholder": "csv"},
    ],
    "S3_FILE": [
        {"name": "url", "label": "Root Path", "type": "text", "placeholder": "/data"},
        {"name": "format", "label": "Format", "type": "text", "placeholder": "csv"},
        {"name": "bucket", "label": "Bucket", "type": "text", "placeholder": "my-bucket"},
        {"name": "region", "label": "Region", "type": "text", "placeholder": "us-east-1"},
        {"name": "access_key", "label": "Access Key ID", "type": "text", "placeholder": ""},
        {"name": "secret_key", "label": "Secret Access Key", "type": "password", "placeholder": ""},
    ],
    "MINIO_FILE": [
        {"name": "url", "label": "Root Path", "type": "text", "placeholder": "/data"},
        {"name": "format", "label": "Format", "type": "text", "placeholder": "csv"},
        {"name": "endpoint", "label": "Endpoint", "type": "text", "placeholder": "localhost:9000"},
        {"name": "bucket", "label": "Bucket", "type": "text", "placeholder": "my-bucket"},
        {"name": "access_key", "label": "Access Key ID", "type": "text", "placeholder": ""},
        {"name": "secret_key", "label": "Secret Access Key", "type": "password", "placeholder": ""},
        {"name": "ssl_enabled", "label": "SSL Enabled", "type": "text", "placeholder": "false"},
    ],
    "GCS_FILE": [
        {"name": "url", "label": "Root Path", "type": "text", "placeholder": "/data"},
        {"name": "format", "label": "Format", "type": "text", "placeholder": "csv"},
        {"name": "bucket", "label": "Bucket", "type": "text", "placeholder": "my-bucket"},
        {"name": "key_id", "label": "Key ID", "type": "text", "placeholder": ""},
        {"name": "secret_key", "label": "Secret Key", "type": "password", "placeholder": ""},
        {"name": "credentials", "label": "Credentials (Base64)", "type": "password", "placeholder": "eyJ..."},
    ],
    "DATABRICKS": [
        {"name": "serverHostname", "label": "Server Hostname", "type": "text", "placeholder": "dbc-xxxxxxxx-xxxx.cloud.databricks.com"},
        {"name": "httpPath", "label": "HTTP Path", "type": "text", "placeholder": "/sql/1.0/warehouses/xxxxxxxx"},
        {"name": "accessToken", "label": "Access Token", "type": "password", "placeholder": ""},
    ],
}

# Callbacks injected by wren.py via init()
_get_state: Callable[[], dict] | None = None
_set_connection: Callable[[str, dict], None] | None = None
_deploy_from_dict: Callable[[dict], tuple[bool, str]] | None = None
_set_read_only_mode: Callable[[bool], None] | None = None


def init(
    get_state: Callable[[], dict],
    set_connection: Callable[[str, dict], None],
    deploy_from_dict: Callable[[dict], tuple[bool, str]],
    set_read_only_mode: Callable[[bool], None],
) -> None:
    global _get_state, _set_connection, _deploy_from_dict, _set_read_only_mode
    _get_state = get_state
    _set_connection = set_connection
    _deploy_from_dict = deploy_from_dict
    _set_read_only_mode = set_read_only_mode


def _base_ctx(state: dict) -> dict:
    return {
        "datasource_options": list(DATASOURCE_FIELDS.keys()),
        "datasource_fields": DATASOURCE_FIELDS,
        "data_source": state.get("data_source"),
        "connection_info": state.get("connection_info") or {},
        "is_deployed": state.get("is_deployed", False),
        "model_count": state.get("model_count", 0),
        "column_count": state.get("column_count", 0),
        "mdl_path": state.get("mdl_path"),
        "connection_info_path": state.get("connection_info_path"),
        "mdl_json": json.dumps(state.get("mdl_dict") or {}, indent=2),
        "is_docker": is_docker(),
        "read_only_mode": state.get("read_only_mode", False),
    }


async def index(request: Request):
    ctx = _base_ctx(_get_state())
    ctx["request"] = request
    # Pre-select current datasource for field rendering
    ctx["datasource"] = (ctx["data_source"] or "").upper()
    return templates.TemplateResponse("index.html", ctx)


async def connection_fields(request: Request):
    ds = request.query_params.get("datasource", "").upper()
    state = _get_state()
    return templates.TemplateResponse(
        "_fields.html",
        {
            "request": request,
            "datasource": ds,
            "datasource_fields": DATASOURCE_FIELDS,
            "connection_info": state.get("connection_info") or {},
            "is_docker": is_docker(),
        },
    )


async def post_connection(request: Request):
    form = await request.form()
    ds = form.get("datasource", "").strip().upper()

    if "_json" in form and form["_json"].strip():
        try:
            conn_info = json.loads(form["_json"])
        except json.JSONDecodeError:
            return HTMLResponse(_msg("✗ Invalid JSON — connection not saved.", ok=False))
    else:
        conn_info = {
            k: v
            for k, v in form.items()
            if k not in ("datasource", "_json") and v.strip()
        }

    if not ds:
        return HTMLResponse(_msg("✗ Please select a data source.", ok=False))

    state = _get_state()

    # Merge with existing connection info so that omitted sensitive fields
    # (e.g. credentials not re-uploaded) retain their saved values.
    existing = state.get("connection_info") or {}
    if existing:
        merged = {**existing, **conn_info}
        conn_info = merged
    mdl_ds = (state.get("data_source") or "").upper()
    if state.get("is_deployed") and mdl_ds and mdl_ds != ds:
        return HTMLResponse(
            _msg(
                f"✗ Data source mismatch: connection is <code>{ds}</code> but MDL declares <code>{mdl_ds}</code>. Update <code>dataSource</code> in the MDL Editor first.",
                ok=False,
            )
        )

    _set_connection(ds, conn_info)

    conn_path = state.get("connection_info_path")
    if conn_path:
        try:
            os.makedirs(os.path.dirname(conn_path), exist_ok=True)
            with open(conn_path, "w") as f:
                json.dump({"type": ds.lower(), "properties": conn_info}, f, indent=2)
        except OSError as e:
            return HTMLResponse(
                _msg(f"✗ Connection info updated in memory but could not be written to disk: {e}", ok=False)
            )

    # Test the connection against ibis-server
    wren_url = state.get("wren_url", "localhost:8000")
    test_url = f"http://{wren_url}/v2/connector/{ds.lower()}/metadata/tables"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(test_url, json={"connectionInfo": conn_info})
        if resp.status_code == 200:
            test_msg = _msg(f"✓ {ds} connection successful", ok=True)
        else:
            try:
                detail = resp.json().get("message", resp.text[:120])
            except Exception:
                detail = resp.text[:120]
            test_msg = _msg(f"✗ Connection test failed (HTTP {resp.status_code}): {detail}", ok=False)
    except httpx.ConnectError:
        test_msg = _msg(f"✗ Cannot reach ibis-server at <code>{wren_url}</code> — is it running?", ok=False)
    except Exception as e:
        test_msg = _msg(f"✗ Connection test error: {e}", ok=False)

    return HTMLResponse(test_msg)



async def get_mdl(request: Request):
    from starlette.responses import Response

    state = _get_state()
    mdl_json = json.dumps(state.get("mdl_dict") or {}, indent=2)
    return Response(content=mdl_json, media_type="application/json")


async def post_mdl(request: Request):
    try:
        body = await request.body()
        mdl_dict = json.loads(body)
    except (json.JSONDecodeError, ValueError) as e:
        return HTMLResponse(_msg(f"✗ Invalid JSON: {e}", ok=False), status_code=400)

    success, message = _deploy_from_dict(mdl_dict)
    if not success:
        return HTMLResponse(_msg(f"✗ {message}", ok=False), status_code=500)

    state = _get_state()
    ds_badge = f"&nbsp;·&nbsp;<code>{state['data_source'].upper()}</code>" if state.get("data_source") else ""
    mdl_status_html = (
        f'<span class="badge badge-ok">✓ Deployed</span>'
        f"&nbsp;{state['model_count']} models&nbsp;·&nbsp;{state['column_count']} columns{ds_badge}"
    )

    return HTMLResponse(
        f"{_msg(f'✓ {message}', ok=True)}"
        f'<div id="mdl-status-content" hx-swap-oob="innerHTML">{mdl_status_html}</div>'
    )


async def post_read_only_mode(request: Request):
    form = await request.form()
    enabled = form.get("read_only_mode") == "on"
    _set_read_only_mode(enabled)
    badge = (
        '<span class="badge badge-readonly">Read-only ON</span>'
        if enabled
        else '<span class="badge badge-off">Read-only OFF</span>'
    )
    return HTMLResponse(
        f'<div id="read-only-badge" hx-swap-oob="innerHTML">{badge}</div>'
        + _msg("✓ Read-only mode updated.", ok=True)
    )


def _msg(text: str, ok: bool) -> str:
    color = "var(--pico-color-green-500)" if ok else "var(--pico-color-red-500)"
    return f'<small style="color:{color}">{text}</small>'


app = Starlette(
    routes=[
        Route("/", index),
        Route("/connection/fields", connection_fields),
        Route("/connection", post_connection, methods=["POST"]),
        Route("/mdl", get_mdl),
        Route("/mdl", post_mdl, methods=["POST"]),
        Route("/read-only-mode", post_read_only_mode, methods=["POST"]),
    ]
)


def start(host: str = "0.0.0.0", port: int = 9001) -> None:
    def run() -> None:
        asyncio.run(
            uvicorn.Server(
                uvicorn.Config(app, host=host, port=port, log_level="warning")
            ).serve()
        )

    t = threading.Thread(target=run, daemon=True, name="wren-web-ui")
    t.start()
    print(f"Web UI available at http://localhost:{port}")  # noqa: T201
