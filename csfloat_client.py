"""
CSFloat REST API client.

Handles all communication with the CSFloat API:
 - Fetching user info / actionable trade count  (lightweight poll)
 - Fetching pending / queued sell trades
 - Bulk-accepting trades
"""

from __future__ import annotations

import asyncio
import logging
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
URL_ACCEPT_BULK          = f"{_BASE}/trades/bulk/accept"
URL_ACCEPT_ONE           = f"{_BASE}/trades/{{trade_id}}/accept"
URL_STEAM_STATUS_OFFER   = f"{_BASE}/trades/steam-status/offer"
URL_STEAM_STATUS_NEW_OFFER = f"{_BASE}/trades/steam-status/new-offer"

# Steam TradeOfferState enum values
TRADE_STATE_ACTIVE = 2


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
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        retry_max: int = 6,
        retry_delay: float = 2.0,
        trace_requests: bool = False,
    ) -> None:
        self._key = api_key
        self._session = session
        self._retry_max = retry_max
        self._retry_delay = retry_delay
        self._trace = trace_requests

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
        limit: int = 500,
    ) -> list[dict]:
        """
        Return pending/queued sell trades for *my_steam_id*.

        Filters out trades with ``wait_for_cancel_ping=true`` (buyer-initiated
        cancellations) which must not be acted upon.
        """
        data = await self._request(
            "GET",
            URL_TRADES,
            params={"state": states, "limit": str(limit)},
            label="get_sell_trades",
        )

        trades: list[dict] = data.get("trades", []) if isinstance(data, dict) else []
        return [
            t for t in trades
            if int(t.get("seller_id", 0)) == my_steam_id
            and not _truthy(t.get("wait_for_cancel_ping"))
        ]

    async def bulk_accept(self, trade_ids: list[str]) -> bool:
        """
        Accept multiple trades in a single API call.

        Returns True on success, False if the server reports failure.
        """
        if not trade_ids:
            return True
        log.info("Bulk-accepting %d trade(s): %s", len(trade_ids), trade_ids)
        try:
            await self._request(
                "POST",
                URL_ACCEPT_BULK,
                json={"trade_ids": trade_ids},
                label="bulk_accept",
            )
            return True
        except CSFloatAPIError as exc:
            log.error("Bulk accept failed: %s", exc)
            return False

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
        given_asset_ids: list[int],
        buyer_steam_id64: int,
    ) -> bool:
        """
        Link a Steam trade offer to CSFloat's internal trade records.

        Equivalent to what the CSFloat browser extension does via
        POST /api/v1/trades/steam-status/new-offer when it detects an offer
        sent outside the extension (i.e. sent by this bot).
        """
        payload = {
            "offer_id": offer_id,
            "given_asset_ids": [str(a) for a in given_asset_ids],
            "received_asset_ids": [],
            "other_steam_id64": str(buyer_steam_id64),
        }
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

        Equivalent to what the CSFloat browser extension does via
        POST /api/v1/trades/steam-status/offer after confirming an offer.
        Reports state=2 (Active) — the offer is confirmed and awaiting buyer acceptance.
        """
        import time as _time
        now = int(_time.time())
        payload = {
            "sent_offers": [
                {
                    "offer_id": offer_id,
                    "state": TRADE_STATE_ACTIVE,
                    "given_asset_ids": [str(a) for a in given_asset_ids],
                    "received_asset_ids": [],
                    "time_created": time_created or now,
                    "time_updated": now,
                    "other_steam_id64": str(buyer_steam_id64),
                }
            ],
            "type": 1,  # 1 = API source (vs 2 = HTML scrape)
        }
        try:
            await self._request(
                "POST",
                URL_STEAM_STATUS_OFFER,
                json=payload,
                label=f"ping_offer_status({offer_id})",
            )
            return True
        except CSFloatAPIError as exc:
            log.warning("ping_offer_status(%s) failed: %s", offer_id, exc)
            return False


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("yes", "true", "t", "1", "y")


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
