"""
Background Steam → CSFloat trade-state sync.

Mirrors ``updateTradeState()`` from the CSFloat mobile app (Nov 2026 live
update), which runs every 4.5 minutes and keeps CSFloat informed about what
actually happened on Steam:

  1. Trade history  — ``IEconService/GetTradeHistory`` entries touching a
     pending CSFloat trade are posted to ``/trades/steam-status/trade-history``.
     Includes Steam's newer ``Committed`` and ``Failed`` statuses; missing
     those is what broke trade verification after Steam's change.
  2. Offer states   — every sent offer whose state differs from what CSFloat
     has on record is posted to ``/trades/steam-status/offer`` (Accepted,
     Declined, Expired, …, not just the initial "Active"), and live offers
     CSFloat has not linked yet are annotated via ``steam-status/new-offer``.
  3. Rollbacks      — trades reversed under Steam trade protection are
     reported to ``/trades/{id}/rollback``.
  4. Cancel pings   — for trades the counter-party asked to cancel
     (``wait_for_cancel_ping``), confirm via ``/trades/{id}/cancel-ping`` once
     no live Steam offer exists.

Not mirrored: the app's TLSNotary proofs (``/trades/notary``) for accepted /
failed trades and its ``/me/mobile/status`` ping — both need the app's native
TLSNotary prover / identity.  The legacy rollback report above is the app's
own fallback when notary proving is unavailable.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from .csfloat_client import (
    TRADE_STATE_ACCEPTED,
    TRADE_STATE_ACTIVE,
    CSFloatClient,
)
from .steam_trader import SteamTrader, SteamTradeStatus, redact_secrets

log = logging.getLogger(__name__)

TRADE_PENDING = "pending"
DEFAULT_INTERVAL = 4.5 * 60  # same cadence as the CSFloat app


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

class TradeStateReporter:
    """
    Periodically syncs Steam trade / offer state to CSFloat.

    Args:
        csfloat:     Authenticated CSFloatClient.
        steam:       Logged-in SteamTrader.
        my_steam_id: Our SteamID64.
        interval:    Seconds between syncs; ``<= 0`` disables the sync.
    """

    def __init__(
        self,
        csfloat: CSFloatClient,
        steam: SteamTrader,
        my_steam_id: int,
        interval: float = DEFAULT_INTERVAL,
    ) -> None:
        self._cf = csfloat
        self._st = steam
        self._me = str(my_steam_id)
        self._interval = interval

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
        trades = await self._cf.get_pending_trades()
        if not trades:
            log.debug("Trade-state sync: no pending trades")
            return

        # ---- 1. Trade history ----
        history: list[dict] = []
        try:
            history = await self._st.get_trade_history_entries()
        except Exception as exc:
            log.warning("Trade-state sync: could not fetch Steam trade history: %s", redact_secrets(exc))
        if history:
            to_report = select_history_to_report(history, trades, self._me)
            if to_report and await self._cf.report_trade_history(to_report):
                log.info(
                    "Trade-state sync: reported %d Steam trade(s) to CSFloat: %s",
                    len(to_report),
                    [f"{h.get('trade_id')}:{_trade_status_name(h.get('status'))}" for h in to_report],
                )

        # ---- 2. Offer states ----
        offers: dict[str, list[dict]] | None = None
        try:
            offers = await self._st.get_offer_states()
        except Exception as exc:
            log.warning("Trade-state sync: could not fetch Steam trade offers: %s", redact_secrets(exc))

        if offers is not None:
            changed = select_offers_to_report(offers["sent"], trades, self._me)
            if changed and await self._cf.report_offer_states(
                [_drop_none(o) for o in changed],
                label="report_offer_states",
            ):
                log.info(
                    "Trade-state sync: reported %d offer state change(s): %s",
                    len(changed),
                    [f"{o['offer_id']}:{o['state']}" for o in changed],
                )
            for offer in select_offers_to_annotate(offers["sent"], trades):
                if await self._cf.annotate_offer(
                    offer_id=offer["offer_id"],
                    given_asset_ids=offer.get("given_asset_ids") or [],
                    received_asset_ids=offer.get("received_asset_ids") or [],
                ):
                    log.info("Trade-state sync: linked untracked offer %s to CSFloat", offer["offer_id"])

        # ---- 3. Trade-protection rollbacks ----
        for trade, steam_trade, rollback in find_rollback_trades(trades, history):
            rollback_id = rollback.get("trade_id") if rollback else None
            log.warning(
                "Trade-state sync: Steam trade %s for CSFloat trade %s was rolled back — reporting",
                steam_trade.get("trade_id"), trade.get("id"),
            )
            await self._cf.report_rollback(trade["id"], rollback_id)

        # ---- 4. Cancel pings (need the offer list) ----
        if offers is not None:
            for trade in find_cancel_ping_trades(trades, history, offers):
                if await self._cf.cancel_ping(trade["id"]):
                    log.info(
                        "Trade-state sync: sent cancel ping for CSFloat trade %s (no live Steam offer)",
                        trade["id"],
                    )


# ---------------------------------------------------------------------------
# Selection logic (pure functions, ported from the CSFloat app)
# ---------------------------------------------------------------------------

def select_history_to_report(history: list[dict], trades: list[dict], me: str) -> list[dict]:
    """
    Trade-history entries touching a pending CSFloat trade — except those
    where every matching trade is already marked Accepted with us as seller
    (app: ``pingTradeHistory``).
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
    have on record yet (app: ``pingSentTradeOffers``).
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
    which has no Steam offer linked yet (app: post-hoc ``new-offer``).
    """
    unlinked_assets = {_asset_id(t) for t in trades if not _offer(t).get("id") and _asset_id(t)}
    return [
        o for o in sent
        if o.get("state") == TRADE_STATE_ACTIVE
        and unlinked_assets.intersection(_s(a) for a in o.get("given_asset_ids") or ())
    ]


def find_rollback_trades(
    trades: list[dict],
    history: list[dict],
) -> list[tuple[dict, dict, dict | None]]:
    """
    ``(csfloat_trade, rolled_back_steam_trade, rollback_steam_trade | None)``
    for TradeProtectionRollback entries matching a pending CSFloat trade with
    the same counter-party (app: ``findRollbackTrades``).
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
    live (app: ``pingCancelTrades``).

    Slightly stricter than the app, since nobody is watching the bot:
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


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


_STATUS_NAMES = {
    v: k.lower() for k, v in vars(SteamTradeStatus).items() if not k.startswith("_")
}


def _trade_status_name(status: Any) -> str:
    return _STATUS_NAMES.get(status, str(status))
