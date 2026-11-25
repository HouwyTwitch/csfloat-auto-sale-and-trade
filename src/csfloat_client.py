"""
CSFloat REST API client.

Handles all communication with the CSFloat API:
 - Fetching user info / actionable trade count  (lightweight poll)
 - Fetching pending / queued sell trades (slim listing, like the extension)
 - Fetching a single full trade (trade URL / token for sending the offer)
 - Bulk-accepting trades
 - Reporting Steam trade / offer state back to CSFloat (``steam-status``,
   ``cancel-ping``, ``rollback``, blocked users, extension status) — the same
   requests the CSFloat browser extension (5.19.2) sends from its background
   trade-tracking alarm.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------
_BASE = "https://csfloat.com/api/v1"

URL_ME                   = f"{_BASE}/me"
URL_NOTIFICATIONS        = f"{_BASE}/me/notifications"
URL_TRADES               = f"{_BASE}/me/trades"
URL_TRADES_SLIM          = f"{_BASE}/me/trades/slim"
URL_TRADE                = f"{_BASE}/trades/{{trade_id}}"
URL_ACCEPT_BULK          = f"{_BASE}/trades/bulk/accept"
URL_ACCEPT_ONE           = f"{_BASE}/trades/{{trade_id}}/accept"
URL_STEAM_STATUS_OFFER   = f"{_BASE}/trades/steam-status/offer"
URL_STEAM_STATUS_NEW_OFFER = f"{_BASE}/trades/steam-status/new-offer"
URL_STEAM_STATUS_HISTORY = f"{_BASE}/trades/steam-status/trade-history"
URL_TRADE_CANCEL_PING    = f"{_BASE}/trades/{{trade_id}}/cancel-ping"
URL_TRADE_ROLLBACK       = f"{_BASE}/trades/{{trade_id}}/rollback"
URL_STEAM_STATUS_BLOCKED = f"{_BASE}/trades/steam-status/blocked-users"
URL_EXTENSION_STATUS     = f"{_BASE}/me/extension/status"

# Steam TradeOfferState enum values
TRADE_STATE_ACTIVE = 2
TRADE_STATE_ACCEPTED = 3

# ``type`` field of steam-status reports: data came from the Steam Web API
# (as opposed to 2 = scraped from HTML).
STATUS_SOURCE_API = 1

# ``origin`` of rollback reports (RollbackOrigin.EXTENSION in the extension).
# The bot reads Steam through the Web API like the extension does.
ROLLBACK_ORIGIN_EXTENSION = 1

# Statuses on the slim endpoint that mean "not available for this auth method"
# — fall back to the full /me/trades listing instead.
_SLIM_UNAVAILABLE = frozenset({401, 403, 404, 405})


class CSFloatAPIError(Exception):
    """Raised on non-2xx responses from CSFloat."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"CSFloat API error {status}: {body}")
        self.status = status
        self.body = body


class CSFloatClient:
    """
    Thin async wrapper around the CSFloat REST API.

    Args:
        api_key:        CSFloat API key (from your profile → Developer tab).
        session:        An *already-created* aiohttp.ClientSession to reuse.
        retry_max:      Maximum number of retry attempts for transient errors.
        retry_delay:    Base delay (seconds) between retries (doubles each time).
        trace_requests: When True (or when DEBUG logging is active), log full
                        request URLs/bodies and response bodies.  Useful for
                        debugging API issues — logs appear at DEBUG level.
        timeout:        Per-request time limit (seconds).  A response that
                        stalls mid-body is abandoned after ``timeout / 2``
                        seconds without data instead of aiohttp's 5-minute
                        default.
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        retry_max: int = 6,
        retry_delay: float = 2.0,
        trace_requests: bool = False,
        timeout: float = 60.0,
    ) -> None:
        self._key = api_key
        self._session = session
        self._retry_max = retry_max
        self._retry_delay = retry_delay
        self._trace = trace_requests
        self._timeout = aiohttp.ClientTimeout(total=timeout, sock_read=timeout / 2)
        self._slim_available = True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self._key}

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json: Any = None,
        params: dict | None = None,
        label: str = "",
    ) -> Any:
        """Execute an HTTP request with exponential-backoff retry."""
        _trace = self._trace or log.isEnabledFor(logging.DEBUG)
        delay = self._retry_delay
        error_attempts = 0
        rate_limit_attempts = 0
        max_rate_limit = 10  # separate budget so 429s don't eat error retries
        while True:
            if _trace:
                log.debug(
                    "CSFloat → %s %s  params=%s  body=%s",
                    method, url, params,
                    _redact(json) if json is not None else None,
                )
            try:
                async with self._session.request(
                    method,
                    url,
                    headers=self._headers(),
                    json=json,
                    params=params,
                    timeout=self._timeout,
                ) as resp:
                    text = await resp.text()
                    if _trace:
                        log.debug(
                            "CSFloat ← %s %s  [%d]  body=%s",
                            method, url, resp.status,
                            text[:2000] if len(text) > 2000 else text,
                        )
                    if resp.status == 429:
                        rate_limit_attempts += 1
                        if rate_limit_attempts > max_rate_limit:
                            raise CSFloatAPIError(resp.status, text)
                        retry_after = float(resp.headers.get("Retry-After", delay))
                        log.warning(
                            "Rate-limited by CSFloat (%d/%d); sleeping %.1fs",
                            rate_limit_attempts, max_rate_limit, retry_after,
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    if not resp.ok:
                        raise CSFloatAPIError(resp.status, text)
                    if resp.content_type == "application/json":
                        return await resp.json(content_type=None)
                    return text

            except CSFloatAPIError:
                raise  # propagate immediately — caller decides
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                error_attempts += 1
                if error_attempts > self._retry_max:
                    log.error("Giving up on %s after %d attempts: %s", label or url, error_attempts, exc)
                    raise
                log.warning(
                    "Transient error on %s (attempt %d/%d): %s — retrying in %.1fs",
                    label or url,
                    error_attempts,
                    self._retry_max,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def get_me(self) -> dict:
        """Return current user profile (includes ``actionable_trades`` count)."""
        return await self._request("GET", URL_ME, label="get_me")

    async def get_notifications(self) -> list[dict]:
        """
        Fetch the latest notifications list.

        CSFloat does not expose a WebSocket / SSE stream publicly, so we poll
        this lightweight endpoint every few seconds as a push-notification proxy.
        It typically reflects new trades faster than polling ``/me/trades``.
        """
        try:
            data = await self._request(
                "GET",
                URL_NOTIFICATIONS,
                params={"limit": "50"},
                label="get_notifications",
            )
            return data if isinstance(data, list) else data.get("data", [])
        except CSFloatAPIError as exc:
            # Notifications endpoint may not be available on all accounts
            if exc.status == 404:
                log.debug("Notifications endpoint not available, falling back to /me polling")
                return []
            raise

    async def get_sell_trades(
        self,
        my_steam_id: int,
        states: str = "queued,pending",
        limit: int = 1000,
    ) -> list[dict]:
        """
        Return pending/queued sell trades for *my_steam_id*.

        Uses ``/me/trades/slim`` — the lightweight listing the CSFloat
        extension polls.  The full ``/me/trades`` listing now regularly stalls
        and gets cut off mid-response.  Slim trades carry no ``trade_url`` /
        ``trade_token``; fetch those with :meth:`get_trade` when needed.
        Falls back to the full listing if the slim endpoint is unavailable.

        Filters out trades with ``wait_for_cancel_ping=true`` (buyer-initiated
        cancellations) which must not be acted upon.
        """
        trades = await self._list_trades(states, limit, role="seller", label="get_sell_trades")
        return [t for t in trades if _is_actionable_sale(t, my_steam_id)]

    async def get_pending_trades(self, limit: int = 5000) -> list[dict]:
        """
        Return *all* pending trades (as buyer or seller), unfiltered.

        This is the listing the CSFloat mobile app feeds into its background
        trade-state sync (``/me/trades/slim?state=pending&limit=5000``).
        """
        return await self._list_trades("pending", limit, role=None, label="get_pending_trades")

    async def _list_trades(
        self,
        states: str,
        limit: int,
        *,
        role: str | None,
        label: str,
    ) -> list[dict]:
        """List trades via the slim endpoint, falling back to the full listing."""
        data = None
        if self._slim_available:
            try:
                data = await self._request(
                    "GET",
                    URL_TRADES_SLIM,
                    params={"state": states, "limit": str(limit), "page": "0"},
                    label=label,
                )
            except CSFloatAPIError as exc:
                if exc.status not in _SLIM_UNAVAILABLE:
                    raise
                log.warning(
                    "Slim trades endpoint unavailable (HTTP %d) — using full /me/trades listing",
                    exc.status,
                )
                self._slim_available = False

        if data is None:
            params = {
                "state": states,
                "limit": str(limit),
                "exclude_wait_for_settlement": "false",
                "page": "0",
            }
            if role:
                params["role"] = role
            data = await self._request("GET", URL_TRADES, params=params, label=label)

        return data.get("trades", []) if isinstance(data, dict) else []

    async def get_trade(self, trade_id: str | int) -> dict:
        """Return the full trade record (incl. ``trade_url`` / ``trade_token``)."""
        return await self._request(
            "GET",
            URL_TRADE.format(trade_id=trade_id),
            label=f"get_trade({trade_id})",
        )

    async def bulk_accept(self, trade_ids: list[str]) -> list[dict] | None:
        """
        Accept multiple trades in a single API call.

        Returns the accepted trades as echoed back by CSFloat (full records,
        ``{"data": [...]}``) — possibly empty if the response carries none —
        or None if the server reports failure.
        """
        if not trade_ids:
            return []
        log.info("Bulk-accepting %d trade(s): %s", len(trade_ids), trade_ids)
        try:
            data = await self._request(
                "POST",
                URL_ACCEPT_BULK,
                json={"trade_ids": trade_ids},
                label="bulk_accept",
            )
        except CSFloatAPIError as exc:
            log.error("Bulk accept failed: %s", exc)
            return None
        accepted = data.get("data") if isinstance(data, dict) else None
        return [t for t in accepted if isinstance(t, dict)] if isinstance(accepted, list) else []

    async def accept_one(self, trade_id: str, trade_token: str | None = None) -> bool:
        """Accept a single trade (fallback for individual acceptance)."""
        log.info("Accepting trade %s", trade_id)
        url = URL_ACCEPT_ONE.format(trade_id=trade_id)
        payload: dict = {}
        if trade_token:
            payload["trade_token"] = trade_token
        try:
            await self._request("POST", url, json=payload, label=f"accept_one({trade_id})")
            return True
        except CSFloatAPIError as exc:
            log.error("Accept trade %s failed: %s", trade_id, exc)
            return False

    async def annotate_offer(
        self,
        offer_id: str,
        given_asset_ids: list[int | str],
        buyer_steam_id64: int | None = None,
        received_asset_ids: list[int | str] | None = None,
    ) -> bool:
        """
        Link a Steam trade offer to CSFloat's internal trade records.

        Equivalent to the extension's ``AnnotateOffer``
        (POST /api/v1/trades/steam-status/new-offer), which it sends for offers
        created outside of CSFloat's site (i.e. sent by this bot).
        """
        payload: dict[str, Any] = {
            "offer_id": str(offer_id),
            "given_asset_ids": [str(a) for a in given_asset_ids],
            "received_asset_ids": [str(a) for a in received_asset_ids or ()],
        }
        if buyer_steam_id64:
            payload["other_steam_id64"] = str(buyer_steam_id64)
        try:
            await self._request(
                "POST",
                URL_STEAM_STATUS_NEW_OFFER,
                json=payload,
                label=f"annotate_offer({offer_id})",
            )
            return True
        except CSFloatAPIError as exc:
            log.warning("annotate_offer(%s) failed: %s", offer_id, exc)
            return False

    async def ping_offer_status(
        self,
        offer_id: str,
        given_asset_ids: list[int],
        buyer_steam_id64: int,
        time_created: int | None = None,
    ) -> bool:
        """
        Report the current status of a sent Steam trade offer to CSFloat.

        Equivalent to what the CSFloat extension does via
        POST /api/v1/trades/steam-status/offer after confirming an offer.
        Reports state=2 (Active) — the offer is confirmed and awaiting buyer acceptance.
        """
        now = int(time.time())
        offer = {
            "offer_id": str(offer_id),
            "state": TRADE_STATE_ACTIVE,
            "given_asset_ids": [str(a) for a in given_asset_ids],
            "received_asset_ids": [],
            "time_created": time_created or now,
            "time_updated": now,
            "other_steam_id64": str(buyer_steam_id64),
        }
        return await self.report_offer_states([offer], label=f"ping_offer_status({offer_id})")

    async def report_offer_states(self, offers: list[dict], *, label: str = "report_offer_states") -> bool:
        """
        POST /trades/steam-status/offer with Steam offer states.

        Each offer uses the extension's ``OfferStatus`` shape: ``offer_id``,
        ``state``, ``given_asset_ids``, ``received_asset_ids``,
        ``time_created``, ``time_updated``, ``other_steam_id64``.
        """
        if not offers:
            return True
        payload = {"sent_offers": offers, "type": STATUS_SOURCE_API}
        try:
            await self._request("POST", URL_STEAM_STATUS_OFFER, json=payload, label=label)
            return True
        except CSFloatAPIError as exc:
            log.warning("%s failed: %s", label, exc)
            return False

    async def report_trade_history(self, history: list[dict]) -> bool:
        """
        POST /trades/steam-status/trade-history with Steam trade-history entries.

        This is how the extension / mobile app tell CSFloat that a sale's
        Steam trade actually went through (or failed / was rolled back), which
        is what CSFloat verifies the sale against.
        """
        if not history:
            return True
        payload = {"history": history, "type": STATUS_SOURCE_API}
        try:
            await self._request(
                "POST",
                URL_STEAM_STATUS_HISTORY,
                json=payload,
                label="report_trade_history",
            )
            return True
        except CSFloatAPIError as exc:
            log.warning("report_trade_history failed: %s", exc)
            return False

    async def cancel_ping(self, trade_id: str | int, steam_id: str | None = None) -> bool:
        """
        POST /trades/{id}/cancel-ping — confirm that no live Steam offer exists
        for a trade the counter-party asked to cancel (``wait_for_cancel_ping``).

        *steam_id* is the account the Steam data was read from (the extension
        sends the SteamID of its Web API token).
        """
        try:
            await self._request(
                "POST",
                URL_TRADE_CANCEL_PING.format(trade_id=trade_id),
                json={"steam_id": str(steam_id)} if steam_id else {},
                label=f"cancel_ping({trade_id})",
            )
            return True
        except CSFloatAPIError as exc:
            log.warning("cancel_ping(%s) failed: %s", trade_id, exc)
            return False

    async def report_rollback(self, trade_id: str | int, rollback_trade_id: str | None) -> bool:
        """
        POST /trades/{id}/rollback — report that the Steam trade for a CSFloat
        trade was reversed under Steam's trade protection.
        """
        payload: dict[str, Any] = {"origin": ROLLBACK_ORIGIN_EXTENSION}
        if rollback_trade_id:
            payload["rollback_trade_id"] = str(rollback_trade_id)
        try:
            await self._request(
                "POST",
                URL_TRADE_ROLLBACK.format(trade_id=trade_id),
                json=payload,
                label=f"report_rollback({trade_id})",
            )
            return True
        except CSFloatAPIError as exc:
            log.warning("report_rollback(%s) failed: %s", trade_id, exc)
            return False

    async def report_blocked_users(self, blocked_steam_ids: list[str]) -> bool:
        """
        POST /trades/steam-status/blocked-users — counter-parties of pending
        trades that are on our Steam block list (they can't receive offers).
        """
        if not blocked_steam_ids:
            return True
        try:
            await self._request(
                "POST",
                URL_STEAM_STATUS_BLOCKED,
                json={"blocked_steam_ids": [str(i) for i in blocked_steam_ids]},
                label="report_blocked_users",
            )
            return True
        except CSFloatAPIError as exc:
            log.warning("report_blocked_users failed: %s", exc)
            return False

    async def ping_extension_status(
        self,
        *,
        version: str,
        access_token_steam_id: str = "",
        history_error: str = "",
        trade_offer_error: str = "",
    ) -> bool:
        """
        POST /me/extension/status — the trade-tracking health report the
        extension sends after every run: Steam access, which account the Web
        API token belongs to, and the last history / offer fetch errors.
        """
        payload = {
            "steam_community_permission": True,
            "steam_powered_permission": True,
            "version": version,
            "access_token_steam_id": access_token_steam_id or "",
            "history_error": history_error or "",
            "trade_offer_error": trade_offer_error or "",
        }
        try:
            await self._request("POST", URL_EXTENSION_STATUS, json=payload, label="ping_extension_status")
            return True
        except CSFloatAPIError as exc:
            log.warning("ping_extension_status failed: %s", exc)
            return False


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("yes", "true", "t", "1", "y")


def _is_actionable_sale(trade: dict, my_steam_id: int) -> bool:
    """True for our own sale that is not waiting on a buyer cancel ping."""
    return (
        int(trade.get("seller_id", 0)) == my_steam_id
        and not _truthy(trade.get("wait_for_cancel_ping"))
    )


def offer_already_sent(trade: dict) -> bool:
    """
    True if CSFloat already tracks a live Steam offer for *trade*
    (``steam_offer.state`` is Active or Accepted), so no new offer is needed.
    """
    offer = trade.get("steam_offer") or {}
    return bool(offer.get("id")) and offer.get("state") in (
        TRADE_STATE_ACTIVE,
        TRADE_STATE_ACCEPTED,
    )


def _redact(obj: Any, _sensitive: frozenset = frozenset({
    "api_key", "authorization", "password", "token", "secret",
})) -> Any:
    """Return a copy of *obj* with sensitive keys replaced by '***'."""
    if isinstance(obj, dict):
        return {
            k: "***" if k.lower() in _sensitive else _redact(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj
