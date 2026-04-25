"""
backtest/phase2/tracker.py
Virtual portfolio tracker.

Manages cash, positions, trade execution (with slippage + commission),
and records every completed round-trip for analysis.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
import logging

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float          # total cost including commission


# ------------------------------------------------------------------
class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,   # 10 bps per trade
        slippage_rate: float = 0.0010,     # 10 bps per trade
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

    # ------------------------------------------------------------------
    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
    ) -> None:
        """
        Execute one day's actions.  Sells run first (free up cash),
        then buys fill up to max_positions.

        Args:
            date:    current date
            actions: {ticker: "BUY" | "SELL" | "HOLD" | ...}
            prices:  {ticker: open_price} for execution
        """
        # ── sells first ──────────────────────────────────────────
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker in prices:
                    self._sell(date, ticker, prices[ticker])

        # ── then buys ────────────────────────────────────────────
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

    # ------------------------------------------------------------------
    def _buy(self, date, ticker: str, raw_price: float) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)

        target_value = min(
            self.initial_capital / self.max_positions,
            self.cash * 0.95,                          # keep 5 % buffer
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
        log.debug(
            "BUY  %s  %d @ %.3f  cost $%,.0f  comm $%.0f",
            ticker, shares, exec_price, cost, commission,
        )

    # ------------------------------------------------------------------
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

        self.closed_trades.append(
            {
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
            }
        )
        log.debug(
            "SELL %s  %d @ %.3f  PnL $%,.0f (%+.1f%%)  held %dd",
            ticker, pos.shares, exec_price, pnl, pnl_pct * 100, held,
        )

    # ------------------------------------------------------------------
    def mark_to_market(
        self, date, close_prices: Dict[str, float]
    ) -> float:
        """Total portfolio value at today's close."""
        pos_val = sum(
            close_prices.get(t, p.entry_price) * p.shares
            for t, p in self.positions.items()
        )
        return self.cash + pos_val