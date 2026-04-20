"""
portfolio/rebalance.py
----------------------
Rebalancing logic and trade generation.

Compares current portfolio weights to target weights and
generates a list of trades needed to align them, subject to
drift thresholds, minimum trade sizes, and transaction costs.

Pipeline
────────
  current_weights + target_weights
       ↓
  compute_drift()          — per-position drift
       ↓
  needs_rebalance()        — does max drift exceed threshold?
       ↓
  generate_trades()        — list of Trade objects
       ↓
  estimate_costs()         — commission + slippage
"""

from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class RebalanceConfig:
    """Rebalancing parameters."""

    # Drift threshold: rebalance when any position drifts
    # more than this from its target weight
    drift_threshold: float = 0.05

    # Minimum trade as fraction of portfolio value
    min_trade_pct: float = 0.01

    # Transaction costs
    commission_pct: float = 0.001    # 10 bps
    slippage_pct: float = 0.0005     # 5 bps


# ═══════════════════════════════════════════════════════════════
#  TRADE OBJECT
# ═══════════════════════════════════════════════════════════════

@dataclass
class Trade:
    """A single trade to execute."""

    date: pd.Timestamp
    ticker: str
    action: str          # "BUY" or "SELL"
    shares: float
    price: float
    value: float         # shares × price (unsigned)
    commission: float
    slippage: float

    @property
    def total_cost(self) -> float:
        return self.commission + self.slippage

    @property
    def net_value(self) -> float:
        if self.action == "BUY":
            return -(self.value + self.total_cost)
        return self.value - self.total_cost


# ═══════════════════════════════════════════════════════════════
#  DRIFT COMPUTATION
# ═══════════════════════════════════════════════════════════════

def compute_drift(
    current_weights: dict[str, float],
    target_weights: dict[str, float],
) -> dict[str, float]:
    """
    Compute per-position drift from target.

    Positive drift = overweight, negative = underweight.
    """
    all_tickers = set(current_weights) | set(target_weights)
    drift = {}
    for t in all_tickers:
        cur = current_weights.get(t, 0.0)
        tgt = target_weights.get(t, 0.0)
        drift[t] = cur - tgt
    return drift


def needs_rebalance(
    drift: dict[str, float],
    config: RebalanceConfig | None = None,
) -> bool:
    """Check if any position's drift exceeds the threshold."""
    if config is None:
        config = RebalanceConfig()
    if not drift:
        return False
    max_drift = max(abs(d) for d in drift.values())
    return max_drift > config.drift_threshold


# ═══════════════════════════════════════════════════════════════
#  TRADE GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_trades(
    current_positions: dict[str, float],
    target_weights: dict[str, float],
    prices: dict[str, float],
    portfolio_value: float,
    date: pd.Timestamp,
    config: RebalanceConfig | None = None,
) -> list[Trade]:
    """
    Generate trades to move from current positions to targets.

    Parameters
    ----------
    current_positions : dict
        {ticker: n_shares} currently held.
    target_weights : dict
        {ticker: weight} target allocation (0–1).
    prices : dict
        {ticker: price} current prices.
    portfolio_value : float
        Total portfolio value (cash + positions).
    date : pd.Timestamp
        Trade date.
    config : RebalanceConfig or None
        Rebalancing parameters.

    Returns
    -------
    list[Trade]
        Sells first, then buys, each with cost estimates.
    """
    if config is None:
        config = RebalanceConfig()

    trades: list[Trade] = []
    min_trade_val = portfolio_value * config.min_trade_pct

    all_tickers = set(current_positions) | set(target_weights)

    sells: list[Trade] = []
    buys: list[Trade] = []

    for ticker in all_tickers:
        price = prices.get(ticker)
        if price is None or price <= 0:
            continue

        current_shares = current_positions.get(ticker, 0.0)
        current_value = current_shares * price

        target_value = portfolio_value * target_weights.get(
            ticker, 0.0
        )
        trade_value = target_value - current_value

        if abs(trade_value) < min_trade_val:
            continue

        trade_shares = abs(trade_value) / price
        abs_value = abs(trade_value)
        commission = abs_value * config.commission_pct
        slippage = abs_value * config.slippage_pct

        trade = Trade(
            date=date,
            ticker=ticker,
            action="BUY" if trade_value > 0 else "SELL",
            shares=trade_shares,
            price=price,
            value=abs_value,
            commission=commission,
            slippage=slippage,
        )

        if trade.action == "SELL":
            sells.append(trade)
        else:
            buys.append(trade)

    # Sells first to free up cash
    return sells + buys


def estimate_costs(
    trades: list[Trade],
) -> dict[str, float]:
    """Summarise transaction costs for a list of trades."""
    total_commission = sum(t.commission for t in trades)
    total_slippage = sum(t.slippage for t in trades)
    total_value = sum(t.value for t in trades)
    return {
        "n_trades": len(trades),
        "total_value": total_value,
        "total_commission": total_commission,
        "total_slippage": total_slippage,
        "total_cost": total_commission + total_slippage,
    }