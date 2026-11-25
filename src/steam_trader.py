"""
Steam-side trade logic: send offers and confirm them via Steam Guard.

Uses aiosteampy — fully async, native aiohttp, no thread-pool wrapping needed.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiosteampy import SteamClient, TradeOfferStatus
from aiosteampy.constants import STEAM_URL, AppContext
from aiosteampy.exceptions import SessionExpired
from aiosteampy.models import BaseTradeOfferItem, EconItem, ItemDescription

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Monkey-patch aiosteampy to handle CS2 inventory items whose "Inspect"
# action link does not contain the "%D" placeholder.  Without this guard the
# library raises IndexError: list index out of range.
# ---------------------------------------------------------------------------
def _patched_set_d_id(self: ItemDescription) -> None:
    from aiosteampy.constants import App
    if self.app is App.CS2:
        i_action = next(filter(lambda a: "Inspect" in a.name, self.actions), None)
        if i_action is not None:
            parts = i_action.link.split("%D")
            if len(parts) > 1:
                object.__setattr__(self, "d_id", int(parts[1]))

ItemDescription._set_d_id = _patched_set_d_id  # type: ignore[method-assign]

# ---------------------------------------------------------------------------
# Monkey-patch aiosteampy's tradable-after date parsing.  The library only
# accepts "Jul 18, 2026 (23:59:59) GMT", but Steam also returns a short,
# year-less form such as "18 Jul @ 12:00am" for trade-protected items, which
# crashes the entire inventory fetch with ValueError.  Try all known formats
# and, if none match, log a warning and leave tradable_after as None instead
# of raising.
# ---------------------------------------------------------------------------

_TRADABLE_AFTER_FORMATS = (
    "%b %d, %Y (%H:%M:%S) %Z",  # "Jul 18, 2026 (23:59:59) GMT" — library default
    "%d %b, %Y @ %I:%M%p",      # "18 Jul, 2026 @ 12:00am"
    "%b %d, %Y @ %I:%M%p",      # "Jul 18, 2026 @ 12:00am"
    "%d %b @ %I:%M%p",          # "18 Jul @ 12:00am" — Steam omits the year
    "%b %d @ %I:%M%p",          # "Jul 18 @ 12:00am"
)

_TRADE_RESTRICTION_PREFIXES = (
    "Tradable/Marketable After ",
    "This item is trade-protected and cannot be consumed, modified, or transferred until ",
)


def _parse_tradable_after(date_string: str) -> datetime | None:
    date_string = date_string.strip()
    for fmt in _TRADABLE_AFTER_FORMATS:
        try:
            parsed = datetime.strptime(date_string, fmt)
        except ValueError:
            continue
        if parsed.year == 1900:
            # Year-less format: unlock dates are always ahead of us, so pick
            # the current year unless that lands well in the past (Dec→Jan
            # rollover), then use next year.
            now = datetime.utcnow()
            parsed = parsed.replace(year=now.year)
            if parsed < now - timedelta(days=7):
                parsed = parsed.replace(year=now.year + 1)
        return parsed
    return None


def _patched_set_tradable_after(self: EconItem) -> None:
    description = self.description
    if description is None or not description.market_tradable_restriction:
        return
    for prefix in _TRADE_RESTRICTION_PREFIXES:
        descr = next(
            filter(lambda d: prefix in d.value, description.owner_descriptions or ()),
            None,
        )
        if descr is None:
            continue
        date_string = descr.value.split(prefix)[1]
        parsed = _parse_tradable_after(date_string)
        if parsed is None:
            log.warning(
                "Unrecognised tradable-after date %r on asset %s — treating as unknown",
                date_string, self.asset_id,
            )
        else:
            self.tradable_after = parsed
        return


EconItem._set_tradable_after = _patched_set_tradable_after  # type: ignore[method-assign]
BaseTradeOfferItem._set_tradable_after = _patched_set_tradable_after  # type: ignore[method-assign]

_CS2 = AppContext.CS2
_STEAMID64_BASE = 76561197960265728
_KEEPALIVE_INTERVAL = 20 * 60  # check every 20 minutes


class SteamTradeStatus:
    """``status`` values of ``IEconService/GetTradeHistory`` trades (ETradeStatus)."""
    INIT = 0
    PRE_COMMITTED = 1
    COMMITTED = 2
    COMPLETE = 3
    FAILED = 4
    PARTIAL_SUPPORT_ROLLBACK = 5
    FULL_SUPPORT_ROLLBACK = 6
    SUPPORT_ROLLBACK_SELECTIVE = 7
    ROLLBACK_FAILED = 8
    ROLLBACK_ABANDONED = 9
    IN_ESCROW = 10
    ESCROW_ROLLBACK = 11
    TRADE_PROTECTION_ROLLBACK = 12


# Trade-history statuses CSFloat's extension (5.19.2) and mobile app report.
# ``Committed`` (trade done, still inside Steam's trade-protection window) and
# ``Failed`` were added in the Nov 2026 updates.
_REPORTED_TRADE_STATUSES = frozenset({
    SteamTradeStatus.COMMITTED,
    SteamTradeStatus.COMPLETE,
    SteamTradeStatus.FAILED,
    SteamTradeStatus.TRADE_PROTECTION_ROLLBACK,
})

_CS2_APPID = 730
_TRADE_HISTORY_MAX = 250  # MAX_TRADE_HISTORY_FETCH in the CSFloat extension

# Steam Web API calls made by the background trade-state sync use a smaller
# retry budget than the offer-sending path so one flaky sync cannot stall for
# minutes; the next sync simply tries again.
_SYNC_RETRY_ATTEMPTS = 2

_BLOCKED_STEAMID_RE = re.compile(r'class=".+?ignored.+?".*?id=".+?".*?data-steamid="(7656\d+)"', re.S)

_SECRET_QUERY_RE = re.compile(r"((?:access_token|key)=)[^&\s'\"]+", re.IGNORECASE)


def redact_secrets(value: object) -> str:
    """Mask ``access_token=`` / ``key=`` query values in an error message or URL."""
    return _SECRET_QUERY_RE.sub(r"\1***", str(value))


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
    time_created: datetime | None = None


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
        self._keepalive_task: asyncio.Task | None = None
        # The processor, the trade-state sync and the keep-alive share this
        # client; serialise re-logins so they don't clobber each other's cookies.
        self._login_lock = asyncio.Lock()

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
        try:
            await self._login()
        except BaseException:
            # __aexit__ is not called when __aenter__ raises — close the
            # session here so a failed login doesn't leak "Unclosed client
            # session" errors.
            await self._client.session.close()
            raise
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
        await self._client.session.close()

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _relogin(self) -> None:
        async with self._login_lock:
            await self._client.login()

    async def _login(self) -> None:
        await _retry(self._relogin, label="steam_login")
        log.info("Logged in to Steam")

    async def _keepalive_loop(self) -> None:
        """Background task: re-login whenever the Steam session expires."""
        while True:
            await asyncio.sleep(_KEEPALIVE_INTERVAL)
            try:
                alive = await self._client.is_session_alive()
                if alive:
                    log.debug("Keep-alive: Steam session OK")
                else:
                    log.warning("Keep-alive: session expired — re-logging in")
                    await _retry(self._relogin, label="keepalive_login")
                    log.info("Keep-alive: session restored")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("Keep-alive: unexpected error: %s", redact_secrets(exc))

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
                reauth_fn=self._relogin,
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
                reauth_fn=self._relogin,
            )
        except Exception as exc:
            log.error("Failed to send trade offer to %s: %s", buyer_steam_id, redact_secrets(exc))
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
                reauth_fn=self._relogin,
            )
            log.info("Trade offer %s confirmed", offer_id)
            return True
        except Exception as exc:
            # If the offer was already confirmed (make_trade_offer used confirm=True)
            # there is no pending confirmation to find — treat that as success.
            msg = str(exc).lower()
            if any(kw in msg for kw in (
                "not found",
                "no confirmation",
                "unable to find",
                "404",
                "empty",
            )):
                log.debug(
                    "Offer %s already confirmed (no pending confirmation found): %s",
                    offer_id, redact_secrets(exc),
                )
                return True
            log.error("Failed to confirm trade offer %s: %s", offer_id, redact_secrets(exc))
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
                reauth_fn=self._relogin,
            )
            log.info("confirm_all_pending: all pending confirmations allowed")
        except Exception as exc:
            log.error("confirm_all_pending error: %s", redact_secrets(exc))

    async def get_active_sent_offers(self) -> list[SentOffer]:
        """
        Return active sent trade offers as SentOffer adapter objects.

        Raises if Steam can't be queried: callers must not mistake "unknown"
        for "no offers", or they would send a duplicate offer.
        """
        await self._ensure_access_token()
        sent, _, _ = await _retry(
            lambda: self._client.get_trade_offers(
                sent=True,
                received=False,
                active_only=True,
            ),
            label="get_trade_offers",
            reauth_fn=self._relogin,
        )

        result: list[SentOffer] = []
        for offer in sent:
            try:
                items = [_OfferItem(asset_id=i.asset_id) for i in offer.items_to_give]
                result.append(SentOffer(
                    trade_offer_id=offer.trade_offer_id,
                    partner_id=offer.partner_id,
                    status=offer.status,
                    items_to_give=items,
                    time_created=getattr(offer, "time_created", None),
                ))
            except Exception as exc:
                log.warning("Skipping malformed offer entry: %s", redact_secrets(exc))

        return result

    # ------------------------------------------------------------------
    # Trade-state sync (raw Web API data, shaped like the CSFloat extension)
    # ------------------------------------------------------------------

    async def _ensure_access_token(self) -> None:
        """
        Make sure the ``steamLoginSecure`` access token is still valid.

        Steam removed the ``data-loyalty_webapi_token`` attribute that
        CSFloat's clients used to scrape from steamcommunity.com.  The
        extension now asks the store (``pointssummary/ajaxgetasyncconfig``) and
        the mobile app reads the ``steamLoginSecure`` cookie; aiosteampy has
        always used that cookie, so the bot needs no change here.
        """
        if not self._client.is_access_token_expired:
            return
        log.info("Steam access token expired — refreshing")
        try:
            await self._client.refresh_access_token()
        except Exception as exc:
            log.warning("Access token refresh failed: %s", redact_secrets(exc))
        if self._client.is_access_token_expired:
            log.warning("Access token still expired — re-logging in")
            await _retry(self._relogin, label="token_relogin")

    async def _call_web_api(self, url, params: dict, label: str) -> dict:
        await self._ensure_access_token()
        return await _retry(
            lambda: self._client.call_web_api(url, params=params),
            label=label,
            max_attempts=_SYNC_RETRY_ATTEMPTS,
            reauth_fn=self._relogin,
        )

    async def get_trade_history_entries(self, max_trades: int = _TRADE_HISTORY_MAX) -> list[dict]:
        """
        Fetch recent Steam trades (``IEconService/GetTradeHistory``) and map
        them exactly like the CSFloat extension's ``getTradeHistoryFromAPI``:

        * only Committed / Complete / Failed / TradeProtectionRollback trades,
        * skip trades still in escrow,
        * keep only CS2 assets, drop trades with none.
        """
        rj = await self._call_web_api(
            STEAM_URL.API.IEconService.GetTradeHistory,
            {
                "max_trades": max_trades,
                "include_failed": 1,
                "get_descriptions": 0,
            },
            label="get_trade_history",
        )
        now = time.time()
        entries: list[dict] = []
        for t in (rj.get("response") or {}).get("trades") or []:
            if t.get("status") not in _REPORTED_TRADE_STATUSES:
                continue
            escrow_end = t.get("time_escrow_end")
            if escrow_end and int(escrow_end) >= now:
                continue
            received = _cs2_assets(t.get("assets_received"))
            given = _cs2_assets(t.get("assets_given"))
            if not received and not given:
                continue
            other = t.get("steamid_other")
            entry = {
                "other_party_url": f"https://steamcommunity.com/profiles/{other}",
                "other_party_id": other,
                "received_assets": received,
                "given_assets": given,
                "trade_id": t.get("tradeid"),
                "time_settlement": t.get("time_settlement"),
                "status": t.get("status"),
                "rollback_trade": t.get("rollback_trade"),
                "time_init": t.get("time_init"),
            }
            # JSON.stringify drops undefined fields — mirror that.
            entries.append({k: v for k, v in entry.items() if v is not None})
        return entries

    async def get_offer_states(self) -> dict[str, list[dict]]:
        """
        Fetch sent and received offers (``IEconService/GetTradeOffers``) mapped
        like the CSFloat extension's ``offerStateMapper``.

        Returns ``{"sent": [...], "received": [...]}``.
        """
        rj = await self._call_web_api(
            STEAM_URL.API.IEconService.GetTradeOffers,
            {
                "get_sent_offers": 1,
                "get_received_offers": 1,
                "get_descriptions": 0,
            },
            label="get_offer_states",
        )
        response = rj.get("response") or {}
        return {
            "sent": [_map_offer_state(o) for o in response.get("trade_offers_sent") or []],
            "received": [_map_offer_state(o) for o in response.get("trade_offers_received") or []],
        }

    async def get_sent_offer_state(self, offer_id: str | int) -> int | None:
        """Current Steam state of one of our sent offers, or None if unknown."""
        try:
            offers = await self.get_offer_states()
        except Exception as exc:
            log.warning("Could not check state of offer %s: %s", offer_id, redact_secrets(exc))
            return None
        return next(
            (o.get("state") for o in offers["sent"] if str(o.get("offer_id")) == str(offer_id)),
            None,
        )

    async def access_token_steam_id(self) -> str:
        """
        SteamID64 the Web API token belongs to (refreshing it if expired), or
        "" if no valid token can be obtained — the extension's
        ``getAccessToken().steam_id``.
        """
        try:
            await self._ensure_access_token()
            if self._client.is_access_token_expired:
                return ""
            return str((self._client.access_token_decoded or {}).get("sub") or "")
        except Exception as exc:
            log.warning("Could not obtain a Steam Web API token: %s", redact_secrets(exc))
            return ""

    async def cancel_trade_offer(self, offer_id: str | int) -> bool:
        """Cancel one of our sent trade offers (``/tradeoffer/{id}/cancel``)."""
        try:
            await _retry(
                lambda: self._client.cancel_trade_offer(int(offer_id)),
                label=f"cancel_trade_offer({offer_id})",
                max_attempts=_SYNC_RETRY_ATTEMPTS,
                reauth_fn=self._relogin,
            )
            return True
        except Exception as exc:
            log.error("Failed to cancel trade offer %s: %s", offer_id, redact_secrets(exc))
            return False

    async def fetch_blocked_steam_ids(self) -> list[str]:
        """
        SteamID64s on our Steam block list — scraped from
        ``/profiles/{id}/friends/blocked?ajax=1`` like the extension's
        ``FetchBlockedUsers``.
        """
        url = STEAM_URL.COMMUNITY / f"profiles/{self._steam_id}/friends/blocked"

        async def fetch() -> str:
            r = await self._client.session.get(url, params={"ajax": "1", "l": "english"})
            r.raise_for_status()
            return await r.text()

        text = await _retry(fetch, label="fetch_blocked_users", max_attempts=_SYNC_RETRY_ATTEMPTS)
        if "blocked" not in text.lower():
            raise RuntimeError("failed to parse blocked users page (not logged in?)")
        return _BLOCKED_STEAMID_RE.findall(text)

    @property
    def steam_id(self) -> int:
        return self._steam_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cs2_assets(assets: list[dict] | None) -> list[dict]:
    return [
        {"asset_id": a.get("assetid"), "new_asset_id": a.get("new_assetid")}
        for a in assets or ()
        if int(a.get("appid", 0)) == _CS2_APPID
    ]


def _map_offer_state(o: dict) -> dict:
    account_id = o.get("accountid_other")
    return {
        "offer_id": o.get("tradeofferid"),
        "state": o.get("trade_offer_state"),
        "given_asset_ids": [i.get("assetid") for i in o.get("items_to_give") or ()],
        "received_asset_ids": [i.get("assetid") for i in o.get("items_to_receive") or ()],
        "time_created": o.get("time_created"),
        "time_updated": o.get("time_updated"),
        # Extension spelling (the mobile app uses "other_steamid64").
        "other_steam_id64": str(_STEAMID64_BASE + int(account_id)) if account_id is not None else None,
    }


async def _retry(
    fn,
    *,
    label: str = "",
    max_attempts: int = 6,
    base_delay: float = 3.0,
    reauth_fn=None,
):
    """
    Retry an async callable on transient network errors with exponential backoff.

    If *reauth_fn* is provided and the session is rejected (HTTP 401 or
    aiosteampy's SessionExpired), it is awaited once to re-establish the Steam
    session and the call is retried without extra delay.  A second rejection
    after re-authenticating, or a rejection without *reauth_fn*, propagates —
    repeated logins in a row would only risk Steam's login rate limit.

    Non-transient exceptions (LoginError, etc.) propagate immediately.
    """
    delay = base_delay
    reauthed = False
    for attempt in range(1, max_attempts + 2):
        try:
            return await fn()
        except (aiohttp.ClientResponseError, SessionExpired) as exc:
            if isinstance(exc, SessionExpired) or exc.status == 401:
                reason = "session expired" if isinstance(exc, SessionExpired) else "401 Unauthorized"
                if reauth_fn is None or reauthed or attempt > max_attempts:
                    log.error(
                        "[%s] %s — giving up (attempt %d/%d): %s",
                        label, reason, attempt, max_attempts + 1, redact_secrets(exc),
                    )
                    raise
                log.warning(
                    "[%s] %s (attempt %d/%d) — re-authenticating before retry",
                    label, reason, attempt, max_attempts + 1,
                )
                try:
                    await reauth_fn()
                except Exception as reauth_exc:
                    log.error("[%s] Re-auth failed: %s — giving up", label, redact_secrets(reauth_exc))
                    raise exc from reauth_exc
                reauthed = True
                delay = base_delay
                continue
            # Other HTTP error codes: treat as transient
            if attempt > max_attempts:
                log.error("[%s] Giving up after %d attempts: %s", label, attempt, redact_secrets(exc))
                raise
            log.warning(
                "[%s] Transient error (attempt %d/%d): %s — retrying in %.1fs",
                label, attempt, max_attempts + 1, redact_secrets(exc), delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
            TimeoutError,
        ) as exc:
            if attempt > max_attempts:
                log.error("[%s] Giving up after %d attempts: %s", label, attempt, redact_secrets(exc))
                raise
            log.warning(
                "[%s] Transient error (attempt %d/%d): %s — retrying in %.1fs",
                label, attempt, max_attempts + 1, redact_secrets(exc), delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
        except Exception:
            # Non-transient: propagate immediately
            raise
