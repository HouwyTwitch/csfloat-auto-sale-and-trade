"""
Steam-side trade logic: send offers and confirm them via Steam Guard.

Wraps aiosteampy's SteamClient to provide:
  - Session restore / login with retry
  - Inventory fetch
  - Trade offer creation
  - Mobile confirmation via identity_secret
  - Cookie persistence
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
from aiosteampy import SteamClient, AppContext
from aiosteampy.constants import TradeOfferStatus
from aiosteampy.helpers import restore_from_cookies
from aiosteampy.mixins.guard import SteamGuardMixin
from aiosteampy.mixins.web_api import SteamWebApiMixin
from aiosteampy.utils import get_jsonable_cookies

if TYPE_CHECKING:
    from aiosteampy.models import TradeOffer

log = logging.getLogger(__name__)

# CS2 game / inventory context
_CS2_CTX = AppContext.CS2


# ---------------------------------------------------------------------------
# Extended Steam client (adds Guard + WebAPI mixins)
# ---------------------------------------------------------------------------

class _SteamClient(SteamClient, SteamWebApiMixin, SteamGuardMixin):
    pass


# ---------------------------------------------------------------------------
# SteamTrader
# ---------------------------------------------------------------------------

class SteamTrader:
    """
    High-level async helper for Steam trade operations.

    Usage::

        trader = SteamTrader.from_config(cfg)
        async with trader:
            offer_id = await trader.send_offer(buyer_id, [asset_id], trade_url)
            await trader.confirm_offer(offer_id)
    """

    def __init__(self, client: _SteamClient, cookie_path: Path) -> None:
        self._client = client
        self._cookie_path = cookie_path

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, cfg: dict, cookie_path: Path) -> "SteamTrader":
        """Build a SteamTrader from the parsed config dict."""
        proxy: str | None = cfg.get("client_proxy") or None
        kwargs: dict = dict(
            steam_id=int(cfg["steam_id64"]),
            username=cfg["steam_login"],
            password=cfg["steam_password"],
            shared_secret=cfg["shared_secret"],
            identity_secret=cfg["identity_secret"],
            api_key=cfg.get("steam_api_key") or None,
            user_agent=cfg.get("user_agent") or (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        )
        if proxy and cfg.get("steam_use_proxy"):
            kwargs["proxy"] = proxy

        client = _SteamClient(**kwargs)
        return cls(client, cookie_path)

    # ------------------------------------------------------------------
    # Context manager (handles login + cookie save on exit)
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "SteamTrader":
        await self._login()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._save_cookies()
        if not self._client.session.closed:
            await self._client.session.close()

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _login(self) -> None:
        """Restore session from cookies if available, otherwise do a fresh login."""
        if self._cookie_path.is_file():
            try:
                cookies = json.loads(self._cookie_path.read_text())
                await _with_retry(
                    lambda: restore_from_cookies(cookies, self._client),
                    label="restore_cookies",
                )
                log.info("Steam session restored from cookies (account: %s)", self._client.username)
                return
            except Exception as exc:
                log.warning("Could not restore cookies (%s), doing fresh login", exc)

        await _with_retry(self._client.login, label="steam_login")
        log.info("Logged in to Steam as %s", self._client.username)

    async def _save_cookies(self) -> None:
        """Persist session cookies for next run."""
        try:
            cookies = get_jsonable_cookies(self._client.session)
            self._cookie_path.write_text(json.dumps(cookies, indent=2))
            log.debug("Steam session cookies saved to %s", self._cookie_path)
        except Exception as exc:
            log.warning("Failed to save Steam cookies: %s", exc)

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    async def fetch_inventory(self) -> list:
        """Return all CS2 inventory items."""
        items, _, _ = await _with_retry(
            lambda: self._client.get_inventory(_CS2_CTX, count=5000),
            label="get_inventory",
        )
        log.info("Fetched %d inventory items", len(items))
        return list(items)

    # ------------------------------------------------------------------
    # Trade offers
    # ------------------------------------------------------------------

    async def send_offer(
        self,
        buyer_steam_id: int,
        asset_ids: list[int],
        trade_url: str | None,
        trade_token: str | None,
        message: str,
    ) -> int | None:
        """
        Create a Steam trade offer giving *asset_ids* to the buyer.

        Returns the Steam trade offer ID on success, or None on failure.
        """
        if not asset_ids:
            log.warning("send_offer called with empty asset_ids — skipping")
            return None

        inventory = await self.fetch_inventory()
        items_to_give = _find_items(inventory, asset_ids)
        if not items_to_give:
            log.error(
                "None of the requested asset_ids %s were found in inventory", asset_ids
            )
            return None
        if len(items_to_give) != len(asset_ids):
            missing = set(asset_ids) - {i.asset_id for i in items_to_give}
            log.warning("Some asset_ids not in inventory: %s — sending with available items", missing)

        log.info(
            "Sending trade offer to %s with %d item(s)…",
            buyer_steam_id,
            len(items_to_give),
        )

        kwargs: dict = dict(
            to_give=items_to_give,
            to_receive=[],
            message=message,
            confirm=False,      # We confirm separately for better control
        )

        if trade_url:
            target = trade_url
        else:
            target = buyer_steam_id
            if trade_token:
                kwargs["token"] = trade_token

        offer_id = await _with_retry(
            lambda: self._client.make_trade_offer(target, **kwargs),
            label=f"make_trade_offer(buyer={buyer_steam_id})",
        )
        log.info("Trade offer %s sent successfully", offer_id)
        return offer_id

    async def confirm_offer(self, offer_id: int) -> bool:
        """
        Confirm a pending Steam trade offer via mobile/Steam Guard confirmation.

        Returns True if confirmed, False otherwise.
        """
        log.info("Confirming trade offer %s…", offer_id)
        try:
            await _with_retry(
                lambda: self._client.confirm_trade_offer(offer_id),
                label=f"confirm_trade_offer({offer_id})",
            )
            log.info("Trade offer %s confirmed", offer_id)
            return True
        except Exception as exc:
            log.error("Failed to confirm trade offer %s: %s", offer_id, exc)
            return False

    async def confirm_all_pending(self) -> None:
        """
        Confirm ALL pending trade confirmations (broad sweep).

        Use only as a safety net after sending offers.
        """
        try:
            confirmations = await _with_retry(
                self._client.get_confirmations,
                label="get_confirmations",
            )
            if not confirmations:
                log.debug("No pending confirmations")
                return

            for conf in confirmations:
                try:
                    key, ts = await self._client._gen_confirmation_key(tag="conf")
                    result = await self._client.confirm_confirmation(conf, key, ts)
                    if result:
                        log.info("Confirmed trade confirmation (offer_id=%s)", conf.offer_id)
                    else:
                        log.warning("Confirmation returned False for offer_id=%s", conf.offer_id)
                except Exception as exc:
                    log.error("Error confirming %s: %s", conf.offer_id, exc)
        except Exception as exc:
            log.error("Error fetching confirmations: %s", exc)

    async def get_active_sent_offers(self) -> list["TradeOffer"]:
        """Return all active sent trade offers."""
        sent, _, _ = await _with_retry(
            lambda: self._client.get_trade_offers(active_only=False, received=False),
            label="get_trade_offers",
        )
        return list(sent)

    @property
    def steam_id(self) -> int:
        return int(self._client.steam_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _with_retry(coro_fn, *, label: str = "", max_attempts: int = 6, base_delay: float = 3.0):
    """
    Run an async coroutine factory with exponential-backoff retry.

    *coro_fn* is a zero-argument callable that returns a coroutine each time
    it is called (i.e., a lambda wrapping an ``await``-able expression).
    """
    delay = base_delay
    for attempt in range(1, max_attempts + 2):
        try:
            return await coro_fn()
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as exc:
            if attempt > max_attempts:
                log.error("[%s] Giving up after %d attempts: %s", label, attempt, exc)
                raise
            log.warning(
                "[%s] Transient error (attempt %d/%d): %s — retrying in %.1fs",
                label, attempt, max_attempts + 1, exc, delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)


def _find_items(inventory: list, asset_ids: list[int]) -> list:
    """Return inventory items whose asset_id is in *asset_ids*."""
    target = set(asset_ids)
    found: list = []
    for item in inventory:
        if item.asset_id in target:
            found.append(item)
            target.discard(item.asset_id)
            if not target:
                break
    return found


def _normalize_steam_id(raw: int) -> int:
    """Strip the SteamID64 base offset if present to get account ID."""
    base = 76561197960265728
    return raw - base if raw > base else raw
