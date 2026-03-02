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

URL_ME            = f"{_BASE}/me"
URL_NOTIFICATIONS = f"{_BASE}/me/notifications"
URL_TRADES        = f"{_BASE}/me/trades"
URL_ACCEPT_BULK   = f"{_BASE}/trades/bulk/accept"
URL_ACCEPT_ONE    = f"{_BASE}/trades/{{trade_id}}/accept"


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
        api_key:     CSFloat API key (from your profile → Developer tab).
        session:     An *already-created* aiohttp.ClientSession to reuse.
        retry_max:   Maximum number of retry attempts for transient errors.
        retry_delay: Base delay (seconds) between retries (doubles each time).
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        retry_max: int = 6,
        retry_delay: float = 2.0,
    ) -> None:
        self._key = api_key
        self._session = session
        self._retry_max = retry_max
        self._retry_delay = retry_delay

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
        delay = self._retry_delay
        for attempt in range(1, self._retry_max + 2):
            try:
                async with self._session.request(
                    method,
                    url,
                    headers=self._headers(),
                    json=json,
                    params=params,
                ) as resp:
                    text = await resp.text()
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", delay))
                        log.warning("Rate-limited by CSFloat; sleeping %.1fs", retry_after)
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
                if attempt > self._retry_max:
                    log.error("Giving up on %s after %d attempts: %s", label or url, attempt, exc)
                    raise
                log.warning(
                    "Transient error on %s (attempt %d/%d): %s — retrying in %.1fs",
                    label or url,
                    attempt,
                    self._retry_max + 1,
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


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("yes", "true", "t", "1", "y")
