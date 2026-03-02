"""
Steam-side trade logic: send offers and confirm them via Steam Guard.

Uses steampy (synchronous, requests-based) wrapped in asyncio.to_thread()
so it fits naturally into the async processing loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import requests.exceptions
from steampy.client import SteamClient, Asset
from steampy.exceptions import ApiException, InvalidCredentials, LoginRequired
from steampy.models import GameOptions, TradeOfferState

log = logging.getLogger(__name__)

# CS2 inventory context
_CS2 = GameOptions.CS

# SteamID64 base offset
_STEAMID64_BASE = 76561197960265728


# ---------------------------------------------------------------------------
# Lightweight adapter types so processor.py needs no changes
# ---------------------------------------------------------------------------

@dataclass
class _OfferItem:
    asset_id: int


@dataclass
class SentOffer:
    """Wraps a steampy trade-offer dict into attribute form for processor.py."""
    trade_offer_id: int
    partner_id: int          # 32-bit account ID (as returned by Steam API)
    status: TradeOfferState
    items_to_give: list[_OfferItem] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SteamTrader
# ---------------------------------------------------------------------------

class SteamTrader:
    """
    High-level helper for Steam trade operations (login, inventory, offers,
    confirmations).  All steampy calls are synchronous and are run in a
    thread pool via asyncio.to_thread() so they are safe to await.

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
        # steampy's login() accepts a JSON string for steam_guard
        steam_guard_json = json.dumps({
            "steamid": str(cfg["steam_id64"]),
            "shared_secret": cfg["shared_secret"],
            "identity_secret": cfg["identity_secret"],
        })

        api_key: str = cfg.get("steam_api_key") or ""
        proxies: dict | None = None
        if cfg.get("client_proxy") and cfg.get("steam_use_proxy"):
            p = cfg["client_proxy"]
            proxies = {"http": p, "https": p}

        client = SteamClient(
            api_key,
            username=cfg["steam_login"],
            password=cfg["steam_password"],
            steam_guard=steam_guard_json,
            proxies=proxies,
        )
        return cls(
            client=client,
            steam_id=int(cfg["steam_id64"]),
            cookie_path=cookie_path,
        )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "SteamTrader":
        await self._login()
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self._save_cookies_sync)

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _save_cookies_sync(self) -> None:
        try:
            cookies = self._client.session.cookies.get_dict()
            self._cookie_path.write_text(json.dumps(cookies, indent=2))
            log.debug("Steam cookies saved to %s", self._cookie_path)
        except Exception as exc:
            log.warning("Failed to save Steam cookies: %s", exc)

    async def _login(self) -> None:
        """Restore session from cookies, or do a fresh credential login."""
        if self._cookie_path.is_file():
            try:
                cookies = json.loads(self._cookie_path.read_text())
                await asyncio.to_thread(self._client.set_login_cookies, cookies)
                alive = await asyncio.to_thread(self._client.is_session_alive)
                if alive:
                    log.info("Steam session restored from cookies")
                    return
                log.warning("Saved cookies expired — doing fresh login")
            except Exception as exc:
                log.warning("Could not restore cookies (%s) — doing fresh login", exc)

        await _with_retry(
            self._client.login,
            label="steam_login",
        )
        log.info("Logged in to Steam")

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    async def fetch_inventory(self) -> dict:
        """
        Return the CS2 inventory as a merged dict keyed by asset_id string.

        steampy returns a flat dict: {"asset_id_str": {item info}, ...}
        """
        inv = await _with_retry(
            lambda: self._client.get_my_inventory(_CS2, merge=True, count=5000),
            label="get_inventory",
        )
        log.info("Fetched %d inventory items", len(inv))
        return inv

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
        Confirmation is handled automatically by steampy when the offer is sent.
        """
        if not asset_ids:
            log.warning("send_offer called with empty asset_ids — skipping")
            return None

        inventory = await self.fetch_inventory()

        # Build Asset objects, verifying every item is in the inventory
        assets: list[Asset] = []
        for aid in asset_ids:
            if str(aid) in inventory:
                assets.append(Asset(str(aid), _CS2))
            else:
                log.error("asset_id %s not found in inventory", aid)

        if len(assets) != len(asset_ids):
            missing = set(asset_ids) - {int(a.asset_id) for a in assets}
            log.error(
                "Cannot send partial trade — missing asset_ids: %s. "
                "Aborting to avoid CSFloat verification failure.",
                missing,
            )
            return None

        log.info(
            "Sending trade offer to %s with %d item(s)…",
            buyer_steam_id,
            len(assets),
        )

        # Build a trade URL if we have the components but not a full URL
        if not trade_url:
            if trade_token:
                account_id = (
                    buyer_steam_id - _STEAMID64_BASE
                    if buyer_steam_id > _STEAMID64_BASE
                    else buyer_steam_id
                )
                trade_url = (
                    f"https://steamcommunity.com/tradeoffer/new/"
                    f"?partner={account_id}&token={trade_token}"
                )

        try:
            if trade_url:
                # make_offer_with_url auto-confirms when needs_mobile_confirmation
                result = await _with_retry(
                    lambda: self._client.make_offer_with_url(
                        items_from_me=assets,
                        items_from_them=[],
                        trade_offer_url=trade_url,
                        message=message,
                        confirm_trade=True,
                    ),
                    label=f"make_offer_with_url(buyer={buyer_steam_id})",
                )
            else:
                # make_offer also auto-confirms when needs_mobile_confirmation
                result = await _with_retry(
                    lambda: self._client.make_offer(
                        items_from_me=assets,
                        items_from_them=[],
                        partner_steam_id=str(buyer_steam_id),
                        message=message,
                    ),
                    label=f"make_offer(buyer={buyer_steam_id})",
                )
        except Exception as exc:
            log.error("Failed to send trade offer to %s: %s", buyer_steam_id, exc)
            return None

        offer_id: str | None = (result or {}).get("tradeofferid")
        if offer_id:
            log.info("Trade offer %s sent (and auto-confirmed if required)", offer_id)
        else:
            log.error("Offer sent but no tradeofferid in response: %s", result)
        return offer_id

    async def confirm_offer(self, offer_id: str | int) -> bool:
        """
        Explicitly confirm a trade offer via Steam Guard.

        steampy's make_offer* already calls _confirm_transaction internally
        when the offer needs confirmation, so this is mostly a safety-net for
        cases where that auto-confirm failed.
        """
        log.info("Confirming trade offer %s…", offer_id)
        try:
            await _with_retry(
                lambda: self._client._confirm_transaction(str(offer_id)),
                label=f"confirm_transaction({offer_id})",
            )
            log.info("Trade offer %s confirmed", offer_id)
            return True
        except Exception as exc:
            log.error("Failed to confirm trade offer %s: %s", offer_id, exc)
            return False

    async def confirm_all_pending(self) -> None:
        """
        Confirm all Active/ConfirmationNeed sent trade offers (broad sweep).

        Called as a fallback after per-offer confirmation fails.
        """
        try:
            # Use merge=False to get the raw offer list without description merging
            raw = await _with_retry(
                lambda: self._client.get_trade_offers(
                    merge=False,
                    get_sent_offers=True,
                    get_received_offers=False,
                ),
                label="get_trade_offers_for_confirm_all",
            )
            sent = (raw or {}).get("response", {}).get("trade_offers_sent", [])
            for offer in sent:
                state = offer.get("trade_offer_state")
                if state in (TradeOfferState.Active, TradeOfferState.ConfirmationNeed):
                    oid = offer.get("tradeofferid")
                    if oid:
                        try:
                            await asyncio.to_thread(
                                self._client._confirm_transaction, str(oid)
                            )
                            log.info("confirm_all_pending: confirmed offer %s", oid)
                        except Exception as exc:
                            log.warning(
                                "confirm_all_pending: failed for offer %s: %s", oid, exc
                            )
        except Exception as exc:
            log.error("confirm_all_pending: error: %s", exc)

    async def get_active_sent_offers(self) -> list[SentOffer]:
        """
        Return active sent trade offers as SentOffer adapter objects.

        Note: steampy's get_trade_offers only returns Active offers by default.
        """
        try:
            raw = await _with_retry(
                lambda: self._client.get_trade_offers(
                    merge=False,
                    get_sent_offers=True,
                    get_received_offers=False,
                ),
                label="get_trade_offers",
            )
        except Exception as exc:
            log.error("Failed to fetch sent trade offers: %s", exc)
            return []

        sent_raw: list[dict] = (raw or {}).get("response", {}).get("trade_offers_sent", [])
        result: list[SentOffer] = []
        for offer in sent_raw:
            try:
                items = [
                    _OfferItem(asset_id=int(i["assetid"]))
                    for i in offer.get("items_to_give", [])
                ]
                result.append(SentOffer(
                    trade_offer_id=int(offer["tradeofferid"]),
                    # accountid_other is already 32-bit (not SteamID64)
                    partner_id=int(offer["accountid_other"]),
                    status=TradeOfferState(int(offer["trade_offer_state"])),
                    items_to_give=items,
                ))
            except (KeyError, ValueError, TypeError) as exc:
                log.warning("Skipping malformed offer entry: %s", exc)

        return result

    @property
    def steam_id(self) -> int:
        return self._steam_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _with_retry(
    fn,
    *,
    label: str = "",
    max_attempts: int = 6,
    base_delay: float = 3.0,
):
    """
    Run a synchronous callable in a thread pool with exponential-backoff retry.

    Only retries on transient network errors (ConnectionError, Timeout, OSError).
    Non-transient exceptions (InvalidCredentials, ApiException, etc.) propagate
    immediately without retry.
    """
    import requests.exceptions as req_exc

    delay = base_delay
    for attempt in range(1, max_attempts + 2):
        try:
            return await asyncio.to_thread(fn)
        except (
            req_exc.ConnectionError,
            req_exc.Timeout,
            req_exc.ChunkedEncodingError,
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
            # Non-transient: propagate immediately (wrong credentials, API error, etc.)
            raise
