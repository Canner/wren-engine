"""Profile management — load, save, list, switch, add, remove profiles."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import yaml

_WREN_HOME = Path(os.environ.get("WREN_HOME", Path.home() / ".wren"))
_PROFILES_FILE = _WREN_HOME / "profiles.yml"


def _load_raw() -> dict:
    """Load profiles.yml, returning empty structure if missing.

    Raises ValueError on malformed content so callers get a deterministic error
    instead of an AttributeError deep inside library code.
    """
    if not _PROFILES_FILE.exists():
        return {"active": None, "profiles": {}}
    try:
        data = yaml.safe_load(_PROFILES_FILE.read_text())
    except yaml.YAMLError as exc:
        raise ValueError(
            f"profiles.yml is not valid YAML: {exc}\n"
            f"Fix or remove {_PROFILES_FILE} and try again."
        ) from exc
    if data is None:
        return {"active": None, "profiles": {}}
    if not isinstance(data, dict):
        raise ValueError(
            f"profiles.yml must contain a YAML mapping; got {type(data).__name__}.\n"
            f"Fix or remove {_PROFILES_FILE} and try again."
        )
    profiles = data.get("profiles", {})
    if not isinstance(profiles, dict):
        raise ValueError(
            f"profiles.yml: 'profiles' must be a mapping; got {type(profiles).__name__}.\n"
            f"Fix or remove {_PROFILES_FILE} and try again."
        )
    active = data.get("active")
    if active is not None and not isinstance(active, str):
        raise ValueError(
            f"profiles.yml: 'active' must be a string or null; got {type(active).__name__}.\n"
            f"Fix or remove {_PROFILES_FILE} and try again."
        )
    return data


def _save_raw(data: dict) -> None:
    """Write profiles.yml atomically with 0600 permissions."""
    _WREN_HOME.mkdir(parents=True, exist_ok=True)
    payload = yaml.dump(data, default_flow_style=False, sort_keys=False)
    # Write to a temp file in the same directory then atomically replace
    fd, tmp_path = tempfile.mkstemp(dir=_WREN_HOME, suffix=".yml.tmp")
    try:
        os.chmod(tmp_path, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp_path, _PROFILES_FILE)
    except Exception:
        os.unlink(tmp_path)
        raise
    os.chmod(_PROFILES_FILE, 0o600)


def list_profiles() -> dict[str, dict]:
    """Return {name: profile_dict} for all profiles."""
    return _load_raw().get("profiles", {})


def get_active_name() -> str | None:
    """Return the name of the currently active profile, or None."""
    return _load_raw().get("active")


def get_active_profile() -> tuple[str | None, dict]:
    """Return (name, profile_dict) for the active profile. ({} if none set)."""
    data = _load_raw()
    name = data.get("active")
    if name is None:
        return None, {}
    profiles = data.get("profiles", {})
    return name, dict(profiles.get(name, {}))


def add_profile(name: str, profile: dict, *, activate: bool = False) -> None:
    """Add or overwrite a named profile."""
    data = _load_raw()
    data.setdefault("profiles", {})[name] = profile
    if activate or data.get("active") is None:
        data["active"] = name
    _save_raw(data)


def remove_profile(name: str) -> bool:
    """Remove a profile. Returns True if found. Clears active if it was this profile."""
    data = _load_raw()
    profiles = data.get("profiles", {})
    if name not in profiles:
        return False
    del profiles[name]
    if data.get("active") == name:
        data["active"] = next(iter(profiles), None)
    _save_raw(data)
    return True


def switch_profile(name: str) -> bool:
    """Set the active profile. Returns False if name not found."""
    data = _load_raw()
    if name not in data.get("profiles", {}):
        return False
    data["active"] = name
    _save_raw(data)
    return True


def resolve_connection(
    explicit_datasource: str | None,
    explicit_conn_info: str | None,
    explicit_conn_file: str | None,
) -> tuple[str | None, dict]:
    """Resolve datasource + connection_info from explicit flags or active profile.

    Priority: explicit flags > active profile.
    Legacy ~/.wren/connection_info.json fallback is handled separately by
    cli._load_conn() and is not performed here.
    Returns (datasource_str_or_None, connection_dict).
    """
    if explicit_datasource or explicit_conn_info or explicit_conn_file:
        return explicit_datasource, {}

    name, profile = get_active_profile()
    if profile:
        ds = profile.pop("datasource", None)
        return ds, profile

    return None, {}


def debug_profile(name: str | None = None) -> dict[str, Any]:
    """Return diagnostic info for a profile (or the active one).

    Masks sensitive fields (password, credentials, secret, token).
    """
    if name is None:
        name = get_active_name()
    if name is None:
        return {"error": "no active profile"}
    data = _load_raw()
    profile = data.get("profiles", {}).get(name)
    if profile is None:
        return {"error": f"profile '{name}' not found"}

    _SENSITIVE = {
        "password",
        "credentials",
        "secret",
        "token",
        "private_key",
        "access_key",
        "key_id",
        "client_id",
        "bucket",
        "endpoint",
        "staging_dir",
        "hostname",
        "http_path",
        "role_arn",
    }
    masked = {}
    for k, v in profile.items():
        if k.lower() in _SENSITIVE or any(s in k.lower() for s in _SENSITIVE):
            masked[k] = "***"
        else:
            masked[k] = v
    return {"name": name, "active": data.get("active") == name, "config": masked}
