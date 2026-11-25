"""
Background Steam → CSFloat trade tracking.

Port of the CSFloat browser extension's trade-tracking alarm
(``src/lib/alarms/csfloat_trade_pings.ts``, extension 5.19.2), which runs every
3 minutes and keeps CSFloat informed about what actually happened on Steam.
Each run, in the extension's order:

  1. Blocked buyers  — at most every 10 min, report counter-parties of pending
     trades that are on our Steam block list (``steam-status/blocked-users``).
  2. Stale offers    — cancel our offers CSFloat has seen waiting for mobile
     confirmation for over an hour, so they can't be confirmed long after the
     sale failed.
  3. Trade history   — ``IEconService/GetTradeHistory`` entries touching a
     pending CSFloat trade go to ``steam-status/trade-history``.  Includes
     Steam's newer ``Committed`` and ``Failed`` statuses.
  4. Offer states    — sent offers whose state differs from CSFloat's record go
     to ``steam-status/offer``; live offers CSFloat hasn't linked yet are
     annotated via ``steam-status/new-offer``.
  5. Rollbacks       — trades reversed under Steam trade protection are
     reported to ``/trades/{id}/rollback``.
  6. Cancel pings    — for trades the counter-party asked to cancel
     (``wait_for_cancel_ping``), confirm via ``/trades/{id}/cancel-ping`` once
     no live Steam offer exists.
  7. Status ping     — ``/me/extension/status`` with the token's SteamID and
     the history / offer errors, even when there are no pending trades.

Not ported: the TLSNotary proofs (``/trades/notary``) the extension builds for
accepted, failed and rolled-back trades — they need its WASM prover.  For
rollbacks the extension itself falls back to the legacy report above; for
accepted / failed trades the trade-history report is what CSFloat gets.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

from .csfloat_client import (
    TRADE_STATE_ACCEPTED,
    TRADE_STATE_ACTIVE,
    CSFloatClient,
)
from .steam_trader import SteamTrader, SteamTradeStatus, redact_secrets

log = logging.getLogger(__name__)

TRADE_PENDING = "pending"
TRADE_OFFER_NEEDS_CONFIRMATION = 9

DEFAULT_INTERVAL = 3 * 60          # extension alarm period
BLOCKED_USERS_INTERVAL = 10 * 60   # extension reports blocked buyers at most this often
STALE_CONFIRMATION_SECONDS = 60 * 60

# Extension release whose trade-tracking protocol this module implements;
# sent as ``version`` in the status ping.
EXTENSION_PROTOCOL_VERSION = "5.19.2"


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

class TradeStateReporter:
    """
    Periodically syncs Steam trade / offer state to CSFloat.

    Args:
        csfloat:       Authenticated CSFloatClient.
        steam:         Logged-in SteamTrader.
        my_steam_id:   Our SteamID64.
        interval:      Seconds between runs; ``<= 0`` disables the sync.
        report_status: Send the ``/me/extension/status`` ping after each run.
    """

    def __init__(
        self,
        csfloat: CSFloatClient,
        steam: SteamTrader,
        my_steam_id: int,
        interval: float = DEFAULT_INTERVAL,
        report_status: bool = True,
    ) -> None:
        self._cf = csfloat
        self._st = steam
        self._me = str(my_steam_id)
        self._interval = interval
        self._report_status = report_status
        self._last_blocked_check = 0.0

    async def run(self) -> None:
        if self._interval <= 0:
            log.info("Trade-state sync disabled (trade_state_interval_seconds <= 0)")
            return
        log.info("Trade-state sync started — interval=%.0fs", self._interval)
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Trade-state sync failed: %s", redact_secrets(exc))
            await asyncio.sleep(self._interval)

    async def run_once(self) -> None:
        try:
            trades = await self._cf.get_pending_trades()
        except Exception as exc:
            # Extension: can't reach CSFloat → skip the run, including the status ping.
            log.warning("Trade-state sync: cannot fetch pending CSFloat trades: %s", exc)
            return

        token_steam_id = await self._st.access_token_steam_id()

        errors: dict[str, str] = {}
        if trades:
            errors = await self._ping_updates(trades, token_steam_id)
        else:
            log.debug("Trade-state sync: no pending trades")

        if self._report_status:
            await self._cf.ping_extension_status(
                version=EXTENSION_PROTOCOL_VERSION,
                access_token_steam_id=token_steam_id,
                history_error=errors.get("history", ""),
                trade_offer_error=errors.get("trade_offer", ""),
            )

    async def _ping_updates(self, trades: list[dict], token_steam_id: str) -> dict[str, str]:
        errors: dict[str, str] = {}

        # ---- 1. Blocked buyers ----
        try:
            await self._report_blocked_buyers(trades)
        except Exception as exc:
            log.warning("Trade-state sync: blocked-buyer check failed: %s", redact_secrets(exc))

        # ---- 2. Cancel offers stuck waiting for confirmation ----
        try:
            await self._cancel_unconfirmed_offers(trades)
        except Exception as exc:
            log.warning("Trade-state sync: stale-offer cancel failed: %s", redact_secrets(exc))

        # ---- 3. Trade history ----
        history: list[dict] = []
        try:
            history = await self._st.get_trade_history_entries()
        except Exception as exc:
            errors["history"] = redact_secrets(exc)
            log.warning("Trade-state sync: could not fetch Steam trade history: %s", errors["history"])
        if history:
            to_report = select_history_to_report(history, trades, self._me)
            if to_report and await self._cf.report_trade_history(to_report):
                log.info(
                    "Trade-state sync: reported %d Steam trade(s) to CSFloat: %s",
                    len(to_report),
                    [f"{h.get('trade_id')}:{_trade_status_name(h.get('status'))}" for h in to_report],
                )

        # ---- 4. Offer states (one Steam request shared with the cancel pings) ----
        offers: dict[str, list[dict]] | None = None
        try:
            offers = await self._st.get_offer_states()
        except Exception as exc:
            errors["trade_offer"] = redact_secrets(exc)
            log.warning("Trade-state sync: could not fetch Steam trade offers: %s", errors["trade_offer"])

        if offers is not None:
            changed = select_offers_to_report(offers["sent"], trades, self._me)
            if changed:
                if await self._cf.report_offer_states([_drop_none(o) for o in changed]):
                    log.info(
                        "Trade-state sync: reported %d offer state change(s): %s",
                        len(changed),
                        [f"{o['offer_id']}:{o['state']}" for o in changed],
                    )
                else:
                    errors["trade_offer"] = "failed to report trade offer states"
            for offer in select_offers_to_annotate(offers["sent"], trades):
                if await self._cf.annotate_offer(
                    offer_id=offer["offer_id"],
                    given_asset_ids=offer.get("given_asset_ids") or [],
                    received_asset_ids=offer.get("received_asset_ids") or [],
                    buyer_steam_id64=offer.get("other_steam_id64"),
                ):
                    log.info("Trade-state sync: linked untracked offer %s to CSFloat", offer["offer_id"])

        # ---- 5. Trade-protection rollbacks (legacy report; no notary) ----
        for trade, steam_trade, rollback in find_rollback_trades(trades, history):
            log.warning(
                "Trade-state sync: Steam trade %s for CSFloat trade %s was rolled back — reporting",
                steam_trade.get("trade_id"), trade.get("id"),
            )
            await self._cf.report_rollback(trade["id"], rollback.get("trade_id") if rollback else None)

        # ---- 6. Cancel pings (need the offer list) ----
        if offers is not None:
            for trade in find_cancel_ping_trades(trades, history, offers):
                if await self._cf.cancel_ping(trade["id"], token_steam_id or None):
                    log.info(
                        "Trade-state sync: sent cancel ping for CSFloat trade %s (no live Steam offer)",
                        trade["id"],
                    )

        return errors

    async def _report_blocked_buyers(self, trades: list[dict]) -> None:
        now = time.monotonic()
        if self._last_blocked_check and now - self._last_blocked_check < BLOCKED_USERS_INTERVAL:
            return
        self._last_blocked_check = now

        if not any(self._me in (_s(t.get("seller_id")), _s(t.get("buyer_id"))) for t in trades):
            return
        blocked = await self._st.fetch_blocked_steam_ids()
        to_report = select_blocked_counterparties(blocked, trades)
        if to_report and await self._cf.report_blocked_users(to_report):
            log.warning(
                "Trade-state sync: %d trade counter-part(y/ies) are on your Steam block list — reported: %s",
                len(to_report), to_report,
            )

    async def _cancel_unconfirmed_offers(self, trades: list[dict]) -> None:
        candidates = select_stale_unconfirmed_offer_ids(trades)
        if not candidates:
            return
        offers = await self._st.get_offer_states()
        still_unconfirmed = {
            _s(o.get("offer_id"))
            for o in offers["sent"]
            if o.get("state") == TRADE_OFFER_NEEDS_CONFIRMATION
        }
        for offer_id in candidates:
            if offer_id not in still_unconfirmed:
                continue
            log.warning(
                "Trade-state sync: offer %s has waited for mobile confirmation for over an hour — cancelling",
                offer_id,
            )
            if not await self._st.cancel_trade_offer(offer_id):
                return  # extension: stop at the first failure


# ---------------------------------------------------------------------------
# Selection logic (pure functions, ported from the CSFloat extension)
# ---------------------------------------------------------------------------

def select_history_to_report(history: list[dict], trades: list[dict], me: str) -> list[dict]:
    """
    Trade-history entries touching a pending CSFloat trade — except those
    where every matching trade is already marked Accepted with us as seller
    (``pingTradeHistory``).
    """
    by_asset = {_asset_id(t): t for t in trades if _asset_id(t)}
    out = []
    for entry in history:
        matched = [by_asset[a] for a in _entry_asset_ids(entry) if a in by_asset]
        if not matched:
            continue
        if all(
            _offer(t).get("state") == TRADE_STATE_ACCEPTED and _s(t.get("seller_id")) == me
            for t in matched
        ):
            continue
        out.append(entry)
    return out


def select_offers_to_report(sent: list[dict], trades: list[dict], me: str) -> list[dict]:
    """
    Sent offers linked to a pending CSFloat trade whose state CSFloat does not
    have on record yet (``pingSentTradeOffers``).
    """
    by_offer: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        oid = _s(_offer(t).get("id"))
        if oid:
            by_offer[oid].append(t)
    out = []
    for offer in sent:
        linked = by_offer.get(_s(offer.get("offer_id")))
        if not linked:
            continue
        if all(
            _offer(t).get("state") == offer.get("state") and _s(t.get("seller_id")) == me
            for t in linked
        ):
            continue
        out.append(offer)
    return out


def select_offers_to_annotate(sent: list[dict], trades: list[dict]) -> list[dict]:
    """
    Active sent offers that give away the item of a pending CSFloat trade
    which has no Steam offer linked yet (post-hoc ``AnnotateOffer``).
    """
    unlinked_assets = {_asset_id(t) for t in trades if not _offer(t).get("id") and _asset_id(t)}
    return [
        o for o in sent
        if o.get("state") == TRADE_STATE_ACTIVE
        and unlinked_assets.intersection(_s(a) for a in o.get("given_asset_ids") or ())
    ]


def select_stale_unconfirmed_offer_ids(trades: list[dict], now: float | None = None) -> list[str]:
    """
    Offer ids CSFloat has recorded as waiting for mobile confirmation for over
    an hour (``cancelUnconfirmedTradeOffers``).
    """
    now = time.time() if now is None else now
    out: list[str] = []
    for t in trades:
        offer = _offer(t)
        oid = _s(offer.get("id"))
        if not oid or offer.get("state") != TRADE_OFFER_NEEDS_CONFIRMATION:
            continue
        sent_at = _parse_ts(offer.get("sent_at"))
        if sent_at is None or sent_at >= now - STALE_CONFIRMATION_SECONDS:
            continue
        if oid not in out:
            out.append(oid)
    return out


def select_blocked_counterparties(blocked: list[str], trades: list[dict]) -> list[str]:
    """Blocked SteamIDs that are the buyer or seller of a pending trade (``reportBlockedBuyers``)."""
    parties = {_s(t.get("seller_id")) for t in trades} | {_s(t.get("buyer_id")) for t in trades}
    return [b for b in blocked if _s(b) in parties]


def find_rollback_trades(
    trades: list[dict],
    history: list[dict],
) -> list[tuple[dict, dict, dict | None]]:
    """
    ``(csfloat_trade, rolled_back_steam_trade, rollback_steam_trade | None)``
    for TradeProtectionRollback entries matching a pending CSFloat trade with
    the same counter-party (``findRollbackTrades``).
    """
    out = []
    for entry in history:
        if entry.get("status") != SteamTradeStatus.TRADE_PROTECTION_ROLLBACK:
            continue
        assets = set(_entry_asset_ids(entry))
        other = _s(entry.get("other_party_id"))
        trade = next(
            (
                t for t in trades
                if t.get("state") == TRADE_PENDING
                and _asset_id(t) in assets
                and other in (_s(t.get("seller_id")), _s(t.get("buyer_id")))
            ),
            None,
        )
        if trade is None:
            continue
        trade_id = _s(entry.get("trade_id"))
        rollback = next(
            (h for h in history if trade_id and _s(h.get("rollback_trade")) == trade_id),
            None,
        )
        out.append((trade, entry, rollback))
    return out


def find_cancel_ping_trades(
    trades: list[dict],
    history: list[dict],
    offers: dict[str, list[dict]],
) -> list[dict]:
    """
    Pending trades waiting for a cancel ping whose Steam offer is no longer
    live (``pingCancelTrades``).

    Slightly stricter than the extension, since nobody is watching the bot:
      * never ping while any Active sent offer still gives the trade's item
        (covers offers CSFloat hasn't linked yet), and
      * if the trade has a linked offer id that Steam didn't return, wait
        instead of assuming it is gone.
    """
    if not any(t.get("state") == TRADE_PENDING and _truthy(t.get("wait_for_cancel_ping")) for t in trades):
        return []

    all_offers = [*(offers.get("sent") or []), *(offers.get("received") or [])]
    by_id = {_s(o.get("offer_id")): o for o in all_offers if o.get("offer_id")}
    live_given_assets = {
        _s(a)
        for o in offers.get("sent") or []
        if o.get("state") == TRADE_STATE_ACTIVE
        for a in o.get("given_asset_ids") or ()
    }

    out = []
    for t in trades:
        if t.get("state") != TRADE_PENDING or not _truthy(t.get("wait_for_cancel_ping")):
            continue
        asset = _asset_id(t)
        if asset and asset in live_given_assets:
            continue

        offer_id = _s(_offer(t).get("id"))
        offer = by_id.get(offer_id) if offer_id else None
        if offer_id and offer is None:
            log.debug(
                "Cancel ping for trade %s deferred: linked offer %s not in Steam's offer list",
                t.get("id"), offer_id,
            )
            continue

        state = offer.get("state") if offer else None
        if state == TRADE_STATE_ACTIVE:
            continue
        if state == TRADE_STATE_ACCEPTED and not _was_rolled_back(t, history):
            continue
        out.append(t)
    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _was_rolled_back(trade: dict, history: list[dict]) -> bool:
    asset = _asset_id(trade)
    parties = (_s(trade.get("seller_id")), _s(trade.get("buyer_id")))
    return any(
        h.get("status") == SteamTradeStatus.TRADE_PROTECTION_ROLLBACK
        and any(_s(a.get("asset_id")) == asset for a in h.get("received_assets") or ())
        and _s(h.get("other_party_id")) in parties
        for h in history
    )


def _entry_asset_ids(entry: dict) -> list[str]:
    return [
        _s(a.get("asset_id"))
        for a in [*(entry.get("received_assets") or ()), *(entry.get("given_assets") or ())]
    ]


def _asset_id(trade: dict) -> str:
    return _s(((trade.get("contract") or {}).get("item") or {}).get("asset_id"))


def _offer(trade: dict) -> dict:
    return trade.get("steam_offer") or {}


def _s(value: Any) -> str:
    return "" if value is None else str(value)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("yes", "true", "t", "1", "y")


def _parse_ts(value: Any) -> float | None:
    """Epoch seconds from an ISO-8601 string such as ``2026-11-24T05:07:21.757Z``."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


_STATUS_NAMES = {
    v: k.lower() for k, v in vars(SteamTradeStatus).items() if not k.startswith("_")
}


def _trade_status_name(status: Any) -> str:
    return _STATUS_NAMES.get(status, str(status))
