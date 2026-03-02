#!/usr/bin/env python3
"""
CSFloat Auto-Sale Processor
============================
Automatically processes CSFloat sales:
  1. Detects new sell trades via fast notification polling.
  2. Bulk-accepts them on CSFloat.
  3. Sends Steam trade offers to buyers.
  4. Confirms offers via Steam Guard (mobile authenticator).

Usage:
    python main.py [--config config.json] [--cookies cookies.json]
                   [--log-level DEBUG] [--log-file app.log] [--trace-http]

Requirements:
    pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import signal
import sys
from pathlib import Path

import aiohttp
from aiohttp_socks.connector import ProxyConnector

import config as cfg_module
from csfloat_client import CSFloatClient
from processor import NotificationPoller, TradeProcessor
from steam_trader import SteamTrader

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(
    level: str = "INFO",
    log_file: str | None = None,
    trace_http: bool = False,
) -> None:
    fmt = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Optional rotating file handler
    if log_file:
        fh = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        fh.setFormatter(formatter)
        root.addHandler(fh)
        logging.getLogger(__name__).info("Logging to file: %s (10 MB × 5 rotation)", log_file)

    # Quiet down noisy third-party loggers (unless DEBUG requested)
    if level.upper() != "DEBUG":
        for name in ("aiohttp", "asyncio", "steampy", "urllib3"):
            logging.getLogger(name).setLevel(logging.WARNING)

    if trace_http:
        logging.getLogger("csfloat_client").setLevel(logging.DEBUG)
        logging.getLogger(__name__).info("HTTP trace logging enabled for CSFloat requests")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def amain(
    config_path: Path,
    cookie_path: Path,
    trace_http: bool = False,
) -> None:
    cfg = cfg_module.load(config_path)

    # Build aiohttp session (with optional proxy for CSFloat requests)
    proxy: str | None = cfg.get("client_proxy") or None
    if proxy:
        connector = ProxyConnector.from_url(proxy, ttl_dns_cache=300)
        logging.getLogger(__name__).info("Using proxy for CSFloat HTTP: %s", proxy)
    else:
        connector = aiohttp.TCPConnector(
            resolver=aiohttp.resolver.AsyncResolver(),
            limit_per_host=10,
        )

    async with aiohttp.ClientSession(connector=connector) as http_session:
        csfloat = CSFloatClient(
            api_key=cfg["csfloat_api_key"],
            session=http_session,
            trace_requests=trace_http,
        )

        async with SteamTrader.from_config(cfg, cookie_path) as steam:
            processor = TradeProcessor(
                csfloat=csfloat,
                steam=steam,
                my_steam_id=cfg["steam_id64"],
            )

            poller = NotificationPoller(
                processor=processor,
                csfloat=csfloat,
                notif_interval=float(cfg.get("notif_interval_seconds", 15)),
                heartbeat_interval=float(cfg.get("heartbeat_interval_seconds", 60)),
                post_run_delay=float(cfg.get("post_run_delay_seconds", 5)),
            )

            await poller.run()


def _resolve_path(raw: str) -> Path:
    """
    Resolve a path argument.  Absolute paths are used as-is;
    relative paths are resolved against the script's directory
    (not CWD) so the tool works when invoked from elsewhere.
    """
    p = Path(raw)
    if p.is_absolute():
        return p
    return Path(__file__).resolve().parent / p


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CSFloat Auto-Sale Processor — automatically handles CS2 sales on CSFloat."
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config JSON file (default: config.json next to this script)",
    )
    parser.add_argument(
        "--cookies",
        default="cookies.json",
        help="Path to Steam session cookies file (default: cookies.json next to this script)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Write logs to this file in addition to stdout (rotated at 10 MB, 5 backups kept)",
    )
    parser.add_argument(
        "--trace-http",
        action="store_true",
        default=False,
        help="Log full CSFloat HTTP request/response bodies (useful for debugging API issues)",
    )
    args = parser.parse_args()

    _setup_logging(
        level=args.log_level,
        log_file=args.log_file,
        trace_http=args.trace_http,
    )
    log = logging.getLogger(__name__)

    # Handle SIGTERM (e.g. systemd / Docker stop) the same as Ctrl+C
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    try:
        asyncio.run(
            amain(
                _resolve_path(args.config),
                _resolve_path(args.cookies),
                trace_http=args.trace_http,
            )
        )
    except cfg_module.ConfigError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
