"""
backtest/phase2/tracker.py
Virtual portfolio tracker with minimum hold period.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List
import logging

log = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float


class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        min_hold_days: int = 5,
        min_profit_early_exit_pct: float = 0.05,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.min_hold_days = min_hold_days
        self.min_profit_early_exit_pct = min_profit_early_exit_pct

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

    def _can_sell(self, pos: Position, date, current_price: float) -> bool:
        """Check if minimum hold period is satisfied or profit target met."""
        held_days = (date - pos.entry_date).days
        if held_days >= self.min_hold_days:
            return True
        # Early exit allowed if profit target met
        unrealised_pct = (current_price / pos.entry_price) - 1.0
        if unrealised_pct >= self.min_profit_early_exit_pct:
            log.debug(
                "  early exit allowed %s: +%.1f%% after %dd",
                pos.ticker, unrealised_pct * 100, held_days,
            )
            return True
        return False

    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
    ) -> None:
        # ── sells first (respect min hold) ────────────────────
        blocked_sells = []
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker not in prices:
                    continue
                pos = self.positions[ticker]
                if self._can_sell(pos, date, prices[ticker]):
                    self._sell(date, ticker, prices[ticker])
                else:
                    held = (date - pos.entry_date).days
                    blocked_sells.append((ticker, held))

        if blocked_sells:
            log.debug(
                "  min-hold blocked %d sells: %s",
                len(blocked_sells),
                [(t, f"{d}d") for t, d in blocked_sells[:5]],
            )

        # ── then buys ────────────────────────────────────────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]
        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            self._buy(date, ticker, prices[ticker])

    def _buy(self, date, ticker: str, raw_price: float) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)
        target_value = min(
            self.initial_capital / self.max_positions,
            self.cash * 0.95,
        )
        if target_value < 1_000:
            return

        shares = int(target_value / exec_price)
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
        )
        log.info(
            "  ▲ BUY  %-10s  %d shares @ %.2f  cost $%s",
            ticker, shares, exec_price, f"{total:,.0f}",
        )

    def _sell(self, date, ticker: str, raw_price: float) -> None:
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

        self.closed_trades.append({
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
        })
        log.info(
            "  ▼ SELL %-10s  %d shares @ %.2f  PnL $%s (%+.1f%%)  held %dd",
            ticker, pos.shares, exec_price, f"{pnl:,.0f}", pnl_pct * 100, held,
        )

    def mark_to_market(self, date, close_prices: Dict[str, float]) -> float:
        pos_val = sum(
            close_prices.get(t, p.entry_price) * p.shares
            for t, p in self.positions.items()
        )
        return self.cash + pos_val