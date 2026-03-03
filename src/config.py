"""
Configuration loading and validation.

Expected JSON structure (see config.example.json for a template):

{
    "csfloat_api_key": "your-csfloat-api-key",
    "steam_id64": 76561198000000000,
    "steam_login": "your_steam_login",
    "steam_password": "your_steam_password",
    "shared_secret": "base64-shared-secret",
    "identity_secret": "base64-identity-secret",

    // Optional
    "steam_api_key": "optional-steam-web-api-key",
    "client_proxy": "socks5://user:pass@host:port",
    "steam_use_proxy": false,
    "user_agent": "Mozilla/5.0 ...",
    "notif_interval_seconds": 15,
    "heartbeat_interval_seconds": 60,
    "post_run_delay_seconds": 5
}
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

_REQUIRED = [
    "csfloat_api_key",
    "steam_id64",
    "steam_login",
    "steam_password",
    "shared_secret",
    "identity_secret",
]


class ConfigError(ValueError):
    pass


def load(path: str | Path = "config.json") -> dict:
    """Load and validate configuration from *path*."""
    p = Path(path)
    if not p.is_file():
        raise ConfigError(
            f"Config file not found: {p}\n"
            "Copy config.example.json → config.json and fill in your credentials."
        )

    with p.open() as fh:
        cfg: dict = json.load(fh)

    missing = [k for k in _REQUIRED if not cfg.get(k)]
    if missing:
        raise ConfigError(f"Missing required config keys: {missing}")

    # Normalise steam_id64 to int
    cfg["steam_id64"] = int(cfg["steam_id64"])

    # Normalise boolean flags
    for key in ("steam_use_proxy",):
        if key in cfg:
            cfg[key] = _to_bool(cfg[key])

    log.debug("Configuration loaded from %s", p)
    return cfg


def _to_bool(v: object) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("yes", "true", "t", "1", "y")
