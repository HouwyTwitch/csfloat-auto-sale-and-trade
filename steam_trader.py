"""
Steam-side trade logic: send offers and confirm them via Steam Guard.

Uses aiosteampy — fully async, native aiohttp, no thread-pool wrapping needed.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiosteampy import SteamClient, TradeOfferStatus
from aiosteampy.constants import AppContext
from aiosteampy.exceptions import LoginError
from aiosteampy.models import EconItem

log = logging.getLogger(__name__)

_CS2 = AppContext.CS2
_STEAMID64_BASE = 76561197960265728


# ---------------------------------------------------------------------------
# Lightweight adapter types so processor.py needs no changes
# ---------------------------------------------------------------------------

@dataclass
class _OfferItem:
    asset_id: int


@dataclass
class SentOffer:
    """Wraps an aiosteampy TradeOffer into the attribute shape processor.py expects."""
    trade_offer_id: int
    partner_id: int
    status: TradeOfferStatus
    items_to_give: list[_OfferItem] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SteamTrader
# ---------------------------------------------------------------------------

class SteamTrader:
    """
    High-level helper for Steam trade operations (login, inventory, offers,
    confirmations).  All I/O is native async via aiosteampy / aiohttp.

    Usage::

        async with SteamTrader.from_config(cfg, Path("cookies.json")) as trader:
            offer_id = await trader.send_offer(...)
    """

    def __init__(
        self,
        client: SteamClient,
        steam_id: int,
        cookie_path: Path,
    ) -> None:
        self._client = client
        self._steam_id = steam_id
        self._cookie_path = cookie_path

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, cfg: dict, cookie_path: Path) -> "SteamTrader":
        """Build a SteamTrader from the parsed config dict."""
        steam_id = int(cfg["steam_id64"])
        proxy: str | None = None
        if cfg.get("client_proxy") and cfg.get("steam_use_proxy"):
            proxy = cfg["client_proxy"]

        client = SteamClient(
            steam_id=steam_id,
            username=cfg["steam_login"],
            password=cfg["steam_password"],
            shared_secret=cfg["shared_secret"],
            identity_secret=cfg["identity_secret"],
            api_key=cfg.get("steam_api_key") or None,
            proxy=proxy,
        )
        return cls(client=client, steam_id=steam_id, cookie_path=cookie_path)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "SteamTrader":
        await self._login()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.session.close()

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _login(self) -> None:
        await _retry(self._client.login, label="steam_login")
        log.info("Logged in to Steam")

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    async def fetch_inventory(self) -> dict[int, EconItem]:
        """
        Return the full CS2 inventory as a dict keyed by asset_id (int).

        Handles pagination automatically via last_assetid.
        """
        all_items: list[EconItem] = []
        last_assetid: int | None = None

        while True:
            # Capture current last_assetid in default arg to avoid closure surprise
            items, _total, last = await _retry(
                lambda la=last_assetid: self._client.get_inventory(
                    _CS2, count=2000, last_assetid=la
                ),
                label="get_inventory",
            )
            all_items.extend(items)
            if last is None:
                break
            last_assetid = last

        log.info("Fetched %d inventory items", len(all_items))
        return {item.asset_id: item for item in all_items}

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
    ) -> str | None:
        """
        Send a Steam trade offer giving *asset_ids* to the buyer.

        Returns the trade offer ID string on success, or None on failure.
        Confirmation is handled automatically by aiosteampy (confirm=True).
        """
        if not asset_ids:
            log.warning("send_offer called with empty asset_ids — skipping")
            return None

        inventory = await self.fetch_inventory()

        items_to_give: list[EconItem] = []
        for aid in asset_ids:
            item = inventory.get(aid)
            if item:
                items_to_give.append(item)
            else:
                log.error("asset_id %s not found in inventory", aid)

        if len(items_to_give) != len(asset_ids):
            missing = set(asset_ids) - {i.asset_id for i in items_to_give}
            log.error(
                "Cannot send partial trade — missing asset_ids: %s. "
                "Aborting to avoid CSFloat verification failure.",
                missing,
            )
            return None

        log.info(
            "Sending trade offer to %s with %d item(s)…",
            buyer_steam_id,
            len(items_to_give),
        )

        # Extract trade token from URL if not provided directly
        token = trade_token
        if not token and trade_url:
            params = parse_qs(urlparse(trade_url).query)
            token = (params.get("token") or [None])[0]

        try:
            offer_id: int = await _retry(
                lambda: self._client.make_trade_offer(
                    buyer_steam_id,
                    to_give=items_to_give,
                    token=token,
                    message=message,
                    confirm=True,
                ),
                label=f"make_trade_offer(buyer={buyer_steam_id})",
            )
        except Exception as exc:
            log.error("Failed to send trade offer to %s: %s", buyer_steam_id, exc)
            return None

        offer_id_str = str(offer_id)
        log.info("Trade offer %s sent (auto-confirmed if required)", offer_id_str)
        return offer_id_str

    async def confirm_offer(self, offer_id: str | int) -> bool:
        """
        Explicitly confirm a trade offer via Steam Guard.

        aiosteampy's make_trade_offer already confirms when confirm=True, so
        this is mostly a safety-net.  If no pending confirmation exists for the
        offer (already confirmed), we treat that silently as success.
        """
        log.info("Confirming trade offer %s…", offer_id)
        try:
            await _retry(
                lambda: self._client.confirm_trade_offer(int(offer_id)),
                label=f"confirm_trade_offer({offer_id})",
            )
            log.info("Trade offer %s confirmed", offer_id)
            return True
        except Exception as exc:
            # If the offer was already confirmed there is no pending confirmation
            # to find — treat that as a success rather than an error.
            msg = str(exc).lower()
            if any(kw in msg for kw in ("not found", "no confirmation", "404", "empty")):
                log.debug("Offer %s already confirmed (no pending confirmation): %s", offer_id, exc)
                return True
            log.error("Failed to confirm trade offer %s: %s", offer_id, exc)
            return False

    async def confirm_all_pending(self) -> None:
        """
        Confirm all pending trade offer confirmations (broad sweep).

        Called as a fallback after per-offer confirmation fails.
        """
        try:
            await _retry(
                self._client.allow_all_confirmations,
                label="allow_all_confirmations",
            )
            log.info("confirm_all_pending: all pending confirmations allowed")
        except Exception as exc:
            log.error("confirm_all_pending error: %s", exc)

    async def get_active_sent_offers(self) -> list[SentOffer]:
        """
        Return active sent trade offers as SentOffer adapter objects.
        """
        try:
            sent, _, _ = await _retry(
                lambda: self._client.get_trade_offers(
                    sent=True,
                    received=False,
                    active_only=True,
                ),
                label="get_trade_offers",
            )
        except Exception as exc:
            log.error("Failed to fetch sent trade offers: %s", exc)
            return []

        result: list[SentOffer] = []
        for offer in sent:
            try:
                items = [_OfferItem(asset_id=i.asset_id) for i in offer.items_to_give]
                result.append(SentOffer(
                    trade_offer_id=offer.trade_offer_id,
                    partner_id=offer.partner_id,
                    status=offer.status,
                    items_to_give=items,
                ))
            except Exception as exc:
                log.warning("Skipping malformed offer entry: %s", exc)

        return result

    @property
    def steam_id(self) -> int:
        return self._steam_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _retry(
    fn,
    *,
    label: str = "",
    max_attempts: int = 6,
    base_delay: float = 3.0,
):
    """
    Retry an async callable on transient network errors with exponential backoff.

    Non-transient exceptions (LoginError, etc.) propagate immediately.
    """
    delay = base_delay
    for attempt in range(1, max_attempts + 2):
        try:
            return await fn()
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
            TimeoutError,
        ) as exc:
            if attempt > max_attempts:
                log.error("[%s] Giving up after %d attempts: %s", label, attempt, exc)
                raise
            log.warning(
                "[%s] Transient error (attempt %d/%d): %s — retrying in %.1fs",
                label, attempt, max_attempts + 1, exc, delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
        except Exception:
            # Non-transient: propagate immediately
            raise
