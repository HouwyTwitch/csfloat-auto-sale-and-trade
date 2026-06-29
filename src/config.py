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
import os
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

# Mapping of environment variable name -> config key.
# This lets the bot be configured entirely via environment variables
# (e.g. a Docker .env file) without writing a config.json at all.
_ENV_MAP = {
    "CSFLOAT_API_KEY": "csfloat_api_key",
    "STEAM_ID64": "steam_id64",
    "STEAM_LOGIN": "steam_login",
    "STEAM_PASSWORD": "steam_password",
    "STEAM_SHARED_SECRET": "shared_secret",
    "STEAM_IDENTITY_SECRET": "identity_secret",
    "STEAM_API_KEY": "steam_api_key",
    "CLIENT_PROXY": "client_proxy",
    "STEAM_USE_PROXY": "steam_use_proxy",
    "USER_AGENT": "user_agent",
    "NOTIF_INTERVAL_SECONDS": "notif_interval_seconds",
    "HEARTBEAT_INTERVAL_SECONDS": "heartbeat_interval_seconds",
    "POST_RUN_DELAY_SECONDS": "post_run_delay_seconds",
}


class ConfigError(ValueError):
    pass


def load(path: str | Path = "config.json") -> dict:
    """
    Load and validate configuration.

    Values are resolved from two sources, with environment variables taking
    precedence over the JSON file:

      1. ``config.json`` (if it exists) — optional.
      2. Environment variables (see ``_ENV_MAP``) — ideal for Docker / `.env`.

    This means the bot runs fine with *no* config.json as long as the required
    values are supplied via the environment.
    """
    cfg: dict = {}

    p = Path(path)
    if p.is_file():
        with p.open() as fh:
            cfg = json.load(fh)
        log.debug("Configuration loaded from %s", p)

    # Overlay environment variables (env wins over the JSON file).
    env_overrides = _from_env()
    if env_overrides:
        cfg.update(env_overrides)
        log.debug("Applied %d configuration override(s) from environment", len(env_overrides))

    # Drop template/comment keys so validation isn't confused by them.
    cfg = {k: v for k, v in cfg.items() if not k.startswith("_")}

    missing = [k for k in _REQUIRED if not cfg.get(k)]
    if missing:
        raise ConfigError(
            "Missing required configuration: " + ", ".join(missing) + ".\n"
            "Set them via environment variables (see .env.example) or in config.json "
            "(copy config.example.json → config.json)."
        )

    # Normalise steam_id64 to int
    cfg["steam_id64"] = int(cfg["steam_id64"])

    # Normalise numeric tuning knobs (env vars arrive as strings)
    for key in ("notif_interval_seconds", "heartbeat_interval_seconds", "post_run_delay_seconds"):
        if key in cfg and cfg[key] is not None:
            cfg[key] = float(cfg[key])

    # Normalise boolean flags
    for key in ("steam_use_proxy",):
        if key in cfg:
            cfg[key] = _to_bool(cfg[key])

    return cfg


def _from_env() -> dict:
    """Collect configuration values present in the environment."""
    out: dict = {}
    for env_name, cfg_key in _ENV_MAP.items():
        val = os.environ.get(env_name)
        if val is not None and val != "":
            out[cfg_key] = val
    return out


def _to_bool(v: object) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("yes", "true", "t", "1", "y")
