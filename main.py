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

Requirements:
    pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import asyncio
import logging
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

def _setup_logging(level: str = "INFO") -> None:
    fmt = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt)
    # Quiet down noisy third-party loggers
    for name in ("aiohttp", "asyncio", "aiosteampy"):
        logging.getLogger(name).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def amain(config_path: Path, cookie_path: Path) -> None:
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

            try:
                await poller.run()
            except KeyboardInterrupt:
                logging.getLogger(__name__).info("Interrupted by user — shutting down.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CSFloat Auto-Sale Processor — automatically handles CS2 sales on CSFloat."
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config JSON file (default: config.json)",
    )
    parser.add_argument(
        "--cookies",
        default="cookies.json",
        help="Path to Steam session cookies file (default: cookies.json)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    args = parser.parse_args()

    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    try:
        asyncio.run(amain(Path(args.config), Path(args.cookies)))
    except cfg_module.ConfigError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
