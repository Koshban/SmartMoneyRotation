"""
backtest/phase2/tracker.py
Virtual portfolio tracker with minimum hold period,
trailing stop, max hold duration, and score-ranked buys.

Features:
  - Dynamic position sizing: _buy() uses current_nav (passed per-call)
    instead of frozen initial_capital. Slot size grows with equity.
  - force_exits() tags exit_type on closed trade records for attribution.
  - _can_sell() uses calendar-day semantics for min hold period.
  - market_value property on Position for clean NAV calculation.
  - nav property on PortfolioTracker for engine integration.
  - Trailing stop fires at open price (gap-down protection).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import logging

log = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float
    peak_price: float = 0.0
    latest_score: float = 0.0
    current_price: float = 0.0

    def __post_init__(self):
        if self.peak_price == 0.0:
            self.peak_price = self.entry_price
        if self.current_price == 0.0:
            self.current_price = self.entry_price

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price


class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        min_hold_days: int = 5,
        min_profit_early_exit_pct: float = 0.05,
        trailing_stop_pct: float = 0.18,
        max_hold_days: int = 120,
        upgrade_min_score_gap: float = 999,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.min_hold_days = min_hold_days
        self.min_profit_early_exit_pct = min_profit_early_exit_pct

        self.trailing_stop_pct = trailing_stop_pct
        self.max_hold_days = max_hold_days
        self.upgrade_min_score_gap = upgrade_min_score_gap

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

        # Diagnostic counters
        self._total_buys: int = 0
        self._total_sells: int = 0
        self._blocked_sells: int = 0

    # ──────────────────────────────────────────────────────────
    #  NAV calculation (used by engine for dynamic sizing)
    # ──────────────────────────────────────────────────────────
    @property
    def nav(self) -> float:
        """Current net asset value = cash + sum of position market values."""
        return self.cash + sum(
            pos.market_value for pos in self.positions.values()
        )

    # ──────────────────────────────────────────────────────────
    #  Min-hold check
    #
    #  NOTE: Uses calendar days, not trading days.
    #  min_hold_days=5 means 5 calendar days (≈3 trading days).
    #  If you want 5 trading days, set min_hold_days=7.
    # ──────────────────────────────────────────────────────────
    def _can_sell(self, pos: Position, date, current_price: float) -> bool:
        held_days = (date - pos.entry_date).days
        if held_days >= self.min_hold_days:
            return True
        unrealised_pct = (current_price / pos.entry_price) - 1.0
        if unrealised_pct >= self.min_profit_early_exit_pct:
            log.debug(
                "  early exit allowed %s: +%.1f%% after %dd",
                pos.ticker, unrealised_pct * 100, held_days,
            )
            return True
        return False

    # ──────────────────────────────────────────────────────────
    #  Force exits: trailing stop + max hold
    #
    #  Returns list of tickers force-sold.
    #  Tags each closed trade with 'exit_type' for attribution.
    # ──────────────────────────────────────────────────────────
    def force_exits(self, date, prices, **kw) -> List[str]:
        force_sold = []
        for ticker in list(self.positions):
            pos = self.positions[ticker]
            price = prices.get(ticker)

            if price is None:
                held = (date - pos.entry_date).days
                if held > 5:
                    log.warning(
                        "  ⚠ NO PRICE for %-10s  held %dd  "
                        "entry=$%.2f  peak=$%.2f  — STOP CANNOT FIRE",
                        ticker, held, pos.entry_price, pos.peak_price,
                    )
                continue

            held_days = (date - pos.entry_date).days
            drawdown_from_peak = (price - pos.peak_price) / pos.peak_price
            drawdown_from_entry = (price - pos.entry_price) / pos.entry_price

            # Diagnostic: catch positions with large losses
            if drawdown_from_entry < -0.25:
                log.warning(
                    "  ⚠ BIG LOSS %-10s  from_entry=%.1f%%  from_peak=%.1f%%  "
                    "price=$%.2f  peak=$%.2f  entry=$%.2f  held=%dd",
                    ticker,
                    drawdown_from_entry * 100,
                    drawdown_from_peak * 100,
                    price, pos.peak_price, pos.entry_price, held_days,
                )

            reason = None
            exit_type = None

            if held_days >= self.max_hold_days:
                reason = f"max_hold ({held_days}d >= {self.max_hold_days}d)"
                exit_type = "force_exit_max_hold"
            elif drawdown_from_peak <= -self.trailing_stop_pct:
                reason = (
                    f"trailing_stop ({drawdown_from_peak:+.1%} from "
                    f"peak ${pos.peak_price:.2f})"
                )
                exit_type = "force_exit_trailing"

            if reason:
                log.info("  ✖ FORCE-EXIT %-10s  %s", ticker, reason)
                self._sell(date, ticker, price, exit_type=exit_type)
                force_sold.append(ticker)

        return force_sold

    # ──────────────────────────────────────────────────────────
    #  Upgrade — swap weak held position for strong candidate
    # ──────────────────────────────────────────────────────────
    def try_upgrades(
        self,
        date,
        candidate_scores: Dict[str, float],
        prices: Dict[str, float],
        max_upgrades: int = 2,
    ) -> List[Tuple[str, str]]:
        """
        Compare the weakest held positions against the strongest
        un-held candidates.  If the score gap exceeds the threshold,
        sell the held position and buy the candidate.

        Returns list of (sold_ticker, bought_ticker) pairs.
        """
        if not self.positions or not candidate_scores:
            return []

        # Rank held positions by latest score (ascending = weakest first)
        held_scored = [
            (t, pos.latest_score)
            for t, pos in self.positions.items()
            if self._can_sell(pos, date, prices.get(t, pos.entry_price))
        ]
        held_scored.sort(key=lambda x: x[1])

        # Rank candidates by score (descending = strongest first)
        candidates = [
            (t, s)
            for t, s in candidate_scores.items()
            if t not in self.positions and t in prices
        ]
        candidates.sort(key=lambda x: x[1], reverse=True)

        swaps = []
        used_held = set()
        used_cand = set()

        for cand_ticker, cand_score in candidates:
            if len(swaps) >= max_upgrades:
                break
            if cand_ticker in used_cand:
                continue

            for held_ticker, held_score in held_scored:
                if held_ticker in used_held:
                    continue
                gap = cand_score - held_score
                if gap >= self.upgrade_min_score_gap:
                    log.info(
                        "  ⇄ UPGRADE  sell %-10s (score %.3f) → "
                        "buy %-10s (score %.3f, gap +%.3f)",
                        held_ticker, held_score,
                        cand_ticker, cand_score, gap,
                    )
                    self._sell(
                        date, held_ticker, prices[held_ticker],
                        exit_type="upgrade_out",
                    )
                    self._buy(
                        date, cand_ticker, prices[cand_ticker],
                        current_nav=self.nav,
                    )
                    used_held.add(held_ticker)
                    used_cand.add(cand_ticker)
                    swaps.append((held_ticker, cand_ticker))
                    break

        return swaps

    # ──────────────────────────────────────────────────────────
    #  Process signals — accepts current_nav for dynamic sizing
    # ──────────────────────────────────────────────────────────
    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
        scores: Optional[Dict[str, float]] = None,
        current_nav: Optional[float] = None,
    ) -> None:
        scores = scores or {}

        # ── sells first (respect min hold) ────────────────────
        blocked_sells = []
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker not in prices:
                    continue
                pos = self.positions[ticker]
                if self._can_sell(pos, date, prices[ticker]):
                    self._sell(date, ticker, prices[ticker], exit_type="signal_exit")
                else:
                    held = (date - pos.entry_date).days
                    blocked_sells.append((ticker, held))

        if blocked_sells:
            self._blocked_sells += len(blocked_sells)
            log.debug(
                "  min-hold blocked %d sells: %s",
                len(blocked_sells),
                [(t, f"{d}d") for t, d in blocked_sells[:5]],
            )

        # ── then buys — SORTED BY SCORE (highest first) ──────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]
        buy_tickers.sort(key=lambda t: scores.get(t, 0.0), reverse=True)

        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            self._buy(date, ticker, prices[ticker], current_nav=current_nav)

    # ──────────────────────────────────────────────────────────
    #  Update held positions' scores (call each day)
    # ──────────────────────────────────────────────────────────
    def update_scores(self, scores: Dict[str, float]) -> None:
        """Refresh latest_score on each held position for upgrade logic."""
        for ticker, pos in self.positions.items():
            if ticker in scores:
                pos.latest_score = scores[ticker]

    # ──────────────────────────────────────────────────────────
    #  Buy — DYNAMIC SIZING based on current NAV
    # ──────────────────────────────────────────────────────────
    def _buy(
        self,
        date,
        ticker: str,
        raw_price: float,
        current_nav: Optional[float] = None,
    ) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)

        # Use current NAV (if provided) instead of frozen initial_capital.
        # Slot size = NAV / max_positions (equal-weight target).
        # Capped at 95% of available cash to prevent over-commitment.
        sizing_nav = current_nav if current_nav else self.nav
        slot_target = sizing_nav / self.max_positions

        target_value = min(slot_target, self.cash * 0.95)

        if target_value < 1_000:
            log.debug(
                "  SKIP BUY %-10s: target_value=$%.0f too small "
                "(nav=$%s, cash=$%s, slots=%d)",
                ticker, target_value,
                f"{sizing_nav:,.0f}", f"{self.cash:,.0f}",
                self.max_positions,
            )
            return

        shares = int(target_value / exec_price)
        if shares <= 0:
            return

        cost = shares * exec_price
        commission = cost * self.commission_rate
        total = cost + commission
        if total > self.cash:
            # Try smaller allocation (use all remaining cash minus commission buffer)
            max_cost = self.cash / (1 + self.commission_rate)
            shares = int(max_cost / exec_price)
            if shares <= 0:
                return
            cost = shares * exec_price
            commission = cost * self.commission_rate
            total = cost + commission
            if total > self.cash:
                return

        self.cash -= total
        self.positions[ticker] = Position(
            ticker=ticker,
            entry_date=date,
            entry_price=exec_price,
            shares=shares,
            cost_basis=total,
            peak_price=exec_price,
            current_price=exec_price,
        )
        self._total_buys += 1

        log.info(
            "  ▲ BUY  %-10s  %d shares @ %.2f  cost $%s  "
            "(slot=$%s, nav=$%s)",
            ticker, shares, exec_price,
            f"{total:,.0f}",
            f"{slot_target:,.0f}",
            f"{sizing_nav:,.0f}",
        )

    # ──────────────────────────────────────────────────────────
    #  Sell — tags exit_type for attribution
    # ──────────────────────────────────────────────────────────
    def _sell(
        self,
        date,
        ticker: str,
        raw_price: float,
        exit_type: Optional[str] = None,
    ) -> None:
        pos = self.positions.get(ticker)
        if pos is None:
            return

        exec_price = raw_price * (1 - self.slippage_rate)
        proceeds = pos.shares * exec_price
        commission = proceeds * self.commission_rate
        net = proceeds - commission

        pnl = net - pos.cost_basis
        pnl_pct = pnl / pos.cost_basis
        held = (date - pos.entry_date).days

        self.cash += net
        del self.positions[ticker]
        self._total_sells += 1

        trade_record = {
            "ticker": ticker,
            "entry_date": pos.entry_date,
            "exit_date": date,
            "entry_price": pos.entry_price,
            "exit_price": exec_price,
            "shares": pos.shares,
            "cost_basis": pos.cost_basis,
            "net_proceeds": net,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "holding_days": held,
            "peak_price": pos.peak_price,
            "max_favorable": (pos.peak_price / pos.entry_price) - 1.0,
            "latest_score": pos.latest_score,
        }
        if exit_type:
            trade_record["exit_type"] = exit_type

        self.closed_trades.append(trade_record)

        log.info(
            "  ▼ SELL %-10s  %d shares @ %.2f  PnL $%s (%+.1f%%)  "
            "held %dd  [%s]",
            ticker, pos.shares, exec_price,
            f"{pnl:,.0f}", pnl_pct * 100, held,
            exit_type or "unknown",
        )

    # ──────────────────────────────────────────────────────────
    #  Mark-to-market — updates peak + current_price
    # ──────────────────────────────────────────────────────────
    def mark_to_market(self, date, close_prices: Dict[str, float]) -> float:
        pos_val = 0.0
        for t, p in self.positions.items():
            price = close_prices.get(t, p.current_price)
            p.current_price = price
            pos_val += price * p.shares
            if price > p.peak_price:
                p.peak_price = price
        return self.cash + pos_val