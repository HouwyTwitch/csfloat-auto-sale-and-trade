"""
Trade processing orchestration.

This module contains:
  - TradeProcessor: the main business-logic class that ties CSFloat and Steam
    together, performing the full sale lifecycle.
  - NotificationPoller: a fast async polling loop that uses CSFloat's
    /me/notifications endpoint as a pseudo-push mechanism (falls back to
    /me for the actionable_trades count).

Full lifecycle per trade:
  1. Detect new sell trade via notification polling.
  2. Bulk-accept queued trades on CSFloat.
  3. Send Steam trade offer to buyer.
  4. Confirm the trade offer via Steam Guard.
  5. CSFloat detects the on-chain trade automatically (no extra API call).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from steampy.models import TradeOfferState

from csfloat_client import CSFloatClient
from steam_trader import SteamTrader

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class OfferPlan:
    """
    Represents one or more CSFloat trades that should be bundled into a
    single Steam trade offer (same buyer + same item name grouping).
    """
    csfloat_trade_id: int       # ID of the "representative" trade (for logging)
    buyer_id: int
    seller_id: int
    asset_ids: list[int]
    trade_url: str | None
    trade_token: str | None
    csfloat_trade_ids: list[int] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------

class TradeProcessor:
    """
    Orchestrates the full CSFloat → Steam trade lifecycle.

    Args:
        csfloat:    Authenticated CSFloatClient.
        steam:      Authenticated SteamTrader (must already be logged in).
        my_steam_id: The seller's SteamID64.
    """

    def __init__(
        self,
        csfloat: CSFloatClient,
        steam: SteamTrader,
        my_steam_id: int,
    ) -> None:
        self._cf = csfloat
        self._st = steam
        self._my_id = my_steam_id

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run_once(self) -> None:
        """
        Perform a single processing cycle:
          1. Fetch all pending sell trades.
          2. Bulk-accept any unaccepted ones.
          3. Send / confirm Steam offers for accepted trades.
        """
        log.info("=== Processing cycle started ===")

        # ---- Step 1: fetch all pending/queued sell trades ----
        try:
            trades = await self._cf.get_sell_trades(self._my_id)
        except Exception as exc:
            log.error("Failed to fetch trades: %s", exc)
            return

        if not trades:
            log.info("No actionable sell trades found.")
            return

        log.info("Found %d actionable sell trade(s).", len(trades))

        # ---- Step 2: accept unaccepted trades on CSFloat ----
        unaccepted = [t for t in trades if not t.get("accepted_at")]
        if unaccepted:
            await self._accept_trades(unaccepted)
            # Give CSFloat a moment to persist the acceptance
            await asyncio.sleep(4)
            # Re-fetch so we see the updated accepted_at timestamps
            try:
                trades = await self._cf.get_sell_trades(self._my_id)
            except Exception as exc:
                log.error("Re-fetch after accept failed: %s", exc)
                return

        # ---- Step 3: send Steam offers for accepted trades ----
        accepted = [
            t for t in trades
            if t.get("accepted_at") and not t.get("verify_sale_at")
        ]
        if not accepted:
            log.info("No accepted-but-unverified trades to process.")
            return

        plans = _build_offer_plans(accepted)
        log.info("Built %d offer plan(s).", len(plans))

        # Load currently active sent offers so we can skip already-sent ones
        try:
            existing_offers = await self._st.get_active_sent_offers()
        except Exception as exc:
            log.warning("Could not fetch existing Steam offers: %s", exc)
            existing_offers = []

        for plan in plans:
            await self._execute_plan(plan, existing_offers)

        log.info("=== Processing cycle complete ===")

    # ------------------------------------------------------------------
    # Accept helpers
    # ------------------------------------------------------------------

    async def _accept_trades(self, trades: list[dict]) -> None:
        """Bulk-accept a list of CSFloat trades with retry logic."""
        ids = [str(t["id"]) for t in trades]
        log.info("Accepting %d trade(s): %s", len(ids), ids)

        for attempt in range(1, 6):
            ok = await self._cf.bulk_accept(ids)
            if ok:
                log.info("Bulk accept succeeded.")
                return

            log.warning("Bulk accept attempt %d failed, retrying…", attempt)
            await asyncio.sleep(4 * attempt)

            # Re-check which are still unaccepted
            try:
                fresh = await self._cf.get_sell_trades(self._my_id)
            except Exception:
                continue
            ids = [str(t["id"]) for t in fresh if not t.get("accepted_at")]
            if not ids:
                log.info("All trades now accepted (concurrent update).")
                return

        log.error("Could not accept all trades after multiple attempts.")

    # ------------------------------------------------------------------
    # Offer execution helpers
    # ------------------------------------------------------------------

    async def _execute_plan(
        self,
        plan: OfferPlan,
        existing_offers: list,
    ) -> None:
        """Send and confirm one Steam trade offer according to an OfferPlan."""

        # --- Check if we already sent an offer for these items ---
        already_sent_id = _find_existing_offer(plan, existing_offers)
        if already_sent_id is not None:
            offer = next((o for o in existing_offers if o.trade_offer_id == already_sent_id), None)
            if offer and offer.status == TradeOfferState.ConfirmationNeed:
                log.info(
                    "Offer %s already sent but needs confirmation — confirming now.",
                    already_sent_id,
                )
                await self._st.confirm_offer(already_sent_id)
            else:
                log.info(
                    "Offer for buyer %s already active (offer_id=%s) — skipping.",
                    plan.buyer_id,
                    already_sent_id,
                )
            return

        # --- Build the Steam offer message ---
        if len(plan.asset_ids) > 1:
            msg = (
                f"CSFloat Market Trade Offer #{plan.csfloat_trade_id} "
                f"and {len(plan.asset_ids) - 1} other item(s). "
                "Thanks for using CSFloat!"
            )
        else:
            msg = f"CSFloat Market Trade Offer #{plan.csfloat_trade_id}. Thanks for using CSFloat!"

        # --- Send the trade offer ---
        offer_id = await self._st.send_offer(
            buyer_steam_id=plan.buyer_id,
            asset_ids=plan.asset_ids,
            trade_url=plan.trade_url,
            trade_token=plan.trade_token,
            message=msg,
        )

        if not offer_id:
            log.error(
                "Failed to send Steam offer for CSFloat trade(s) %s",
                plan.csfloat_trade_ids,
            )
            return

        # --- Confirm the trade offer ---
        confirmed = await self._st.confirm_offer(offer_id)
        if not confirmed:
            log.warning(
                "Offer %s individual confirm failed — trying broad confirmation sweep.",
                offer_id,
            )
            await self._st.confirm_all_pending()


# ---------------------------------------------------------------------------
# Notification-driven polling loop
# ---------------------------------------------------------------------------

class NotificationPoller:
    """
    Fast polling loop that drives TradeProcessor.

    Strategy (fastest practical approach without an official push API):

    1. **Notification poll** (every ``notif_interval`` seconds, default 15 s):
       Hits ``/api/v1/me/notifications`` — a lightweight endpoint that often
       reflects new trades before they appear in ``/me/trades``.  When a new
       trade-related notification is detected the processor runs immediately.

    2. **Heartbeat poll** (every ``heartbeat_interval`` seconds, default 60 s):
       Hits ``/api/v1/me`` and checks ``actionable_trades > 0`` as a safety net
       to catch anything missed by the notification poll.

    3. **Immediate re-check**: After each processing cycle completes, the poller
       waits ``post_run_delay`` seconds then does one more cycle — in case new
       trades arrived while the previous batch was being handled.
    """

    # Notification types that indicate a new trade is waiting
    TRADE_NOTIF_TYPES = {
        "new_sale",
        "trade_accepted",
        "trade_queued",
        "new_trade",
        "offer_received",
        "sale_accepted",
    }

    def __init__(
        self,
        processor: TradeProcessor,
        csfloat: CSFloatClient,
        *,
        notif_interval: float = 15.0,
        heartbeat_interval: float = 60.0,
        post_run_delay: float = 5.0,
    ) -> None:
        self._proc = processor
        self._cf = csfloat
        self._notif_interval = notif_interval
        self._heartbeat_interval = heartbeat_interval
        self._post_run_delay = post_run_delay
        # OrderedDict preserves insertion order so we can evict oldest entries
        self._seen_notif_ids: OrderedDict[Any, None] = OrderedDict()
        self._last_heartbeat: float = 0.0

    async def run(self) -> None:
        """
        Run forever, driving the trade processor as fast as notifications allow.
        Press Ctrl+C to stop.
        """
        log.info(
            "Notification poller started — notif_interval=%.0fs, heartbeat=%.0fs",
            self._notif_interval,
            self._heartbeat_interval,
        )

        # Initial run on startup to handle any trades that arrived while offline
        await self._safe_run()

        while True:
            await asyncio.sleep(self._notif_interval)
            now = time.monotonic()

            triggered = False

            # --- Notification check ---
            try:
                notifs = await self._cf.get_notifications()
                new_trade_notifs = [
                    n for n in notifs
                    if n.get("id") not in self._seen_notif_ids
                    and self._is_trade_notif(n)
                ]
                # Mark all as seen (OrderedDict preserves insertion order)
                for n in notifs:
                    nid = n.get("id")
                    if nid not in self._seen_notif_ids:
                        self._seen_notif_ids[nid] = None
                # Evict oldest entries to bound memory (FIFO)
                while len(self._seen_notif_ids) > 2000:
                    self._seen_notif_ids.popitem(last=False)

                if new_trade_notifs:
                    log.info(
                        "Notification trigger: %d new trade notification(s) detected.",
                        len(new_trade_notifs),
                    )
                    triggered = True
            except Exception as exc:
                log.debug("Notification poll failed (will rely on heartbeat): %s", exc)

            # --- Heartbeat check (fallback) ---
            if not triggered and (now - self._last_heartbeat) >= self._heartbeat_interval:
                self._last_heartbeat = now
                try:
                    me = await self._cf.get_me()
                    actionable = me.get("actionable_trades", 0)
                    if actionable > 0:
                        log.info(
                            "Heartbeat trigger: %d actionable trade(s) reported by /me.",
                            actionable,
                        )
                        triggered = True
                    else:
                        log.debug("Heartbeat: no actionable trades.")
                except Exception as exc:
                    log.warning("Heartbeat /me poll failed: %s", exc)

            if triggered:
                await self._safe_run()
                # Immediate re-check in case more trades arrived during processing
                await asyncio.sleep(self._post_run_delay)
                await self._safe_run()

    async def _safe_run(self) -> None:
        """Run one processor cycle, catching all exceptions."""
        try:
            await self._proc.run_once()
        except Exception as exc:
            log.exception("Unexpected error in processor cycle: %s", exc)

    def _is_trade_notif(self, notif: dict) -> bool:
        """Return True if this notification indicates a new pending trade."""
        ntype = str(notif.get("type", "")).lower()
        # Match any type that contains a known keyword
        for keyword in self.TRADE_NOTIF_TYPES:
            if keyword in ntype:
                return True
        # Also match if the notification body mentions a trade
        body = str(notif.get("body", "") or notif.get("text", "") or notif.get("message", "")).lower()
        return "trade" in body or "sale" in body or "sold" in body


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_offer_plans(accepted_trades: list[dict]) -> list[OfferPlan]:
    """
    Group accepted trades by buyer_id so that all items sold to the same
    buyer go into a single Steam trade offer.
    """
    groups: dict[int, OfferPlan] = {}
    for trade in accepted_trades:
        buyer_id = int(trade["buyer_id"])
        seller_id = int(trade["seller_id"])
        item = trade.get("contract", {}).get("item", {})
        raw_asset_id = item.get("asset_id")
        trade_id = int(trade["id"])

        if not raw_asset_id:
            log.warning("Trade %s has no asset_id in contract — skipping", trade_id)
            continue
        asset_id = int(raw_asset_id)

        if buyer_id not in groups:
            groups[buyer_id] = OfferPlan(
                csfloat_trade_id=trade_id,
                buyer_id=buyer_id,
                seller_id=seller_id,
                asset_ids=[asset_id],
                trade_url=trade.get("trade_url"),
                trade_token=trade.get("trade_token"),
                csfloat_trade_ids=[trade_id],
            )
        else:
            groups[buyer_id].asset_ids.append(asset_id)
            groups[buyer_id].csfloat_trade_ids.append(trade_id)

    return list(groups.values())


def _find_existing_offer(plan: OfferPlan, sent_offers: list) -> int | None:
    """
    Return the trade_offer_id if there is already an active / confirmation-need
    offer to this buyer that covers all of our intended asset_ids.
    """
    _BASE = 76561197960265728
    buyer_account_id = plan.buyer_id - _BASE if plan.buyer_id > _BASE else plan.buyer_id

    for offer in sent_offers:
        if offer.status not in (
            TradeOfferState.Active,
            TradeOfferState.ConfirmationNeed,
            TradeOfferState.Accepted,
        ):
            continue

        partner_account_id = (
            offer.partner_id - _BASE
            if offer.partner_id > _BASE
            else offer.partner_id
        )
        if partner_account_id != buyer_account_id:
            continue

        sent_asset_ids = {item.asset_id for item in (offer.items_to_give or [])}
        if sent_asset_ids.issuperset(set(plan.asset_ids)):
            return offer.trade_offer_id

    return None
