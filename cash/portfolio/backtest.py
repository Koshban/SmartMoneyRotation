"""
portfolio/backtest.py
---------------------
Historical simulation engine.

Takes the output of ``compute_all_signals()`` — the full
MultiIndex (date, ticker) panel with BUY / HOLD / SELL /
NEUTRAL signals and signal strengths — and simulates
portfolio performance day by day.

Architecture
────────────
  signals_df (date × ticker panel)
       ↓
  for each date:
       mark_to_market()          update portfolio value
       process_exits()           sell SELL signals
       compute_target_weights()  sizing for active set
       process_entries()         buy new BUY signals
       check_rebalance()         drift-based rebalance
       record_state()            equity, positions, trades
       ↓
  BacktestResult
       ↓
  compute_performance_metrics()  Sharpe, CAGR, drawdown, etc.

Execution Model
───────────────
  Signals on day T are executed at day T's close price.
  The ``execution_delay`` parameter shifts execution to T+N.
  Commission and slippage are applied to each trade.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np
import pandas as pd

from cash.portfolio.sizing import (
    SizingConfig,
    compute_target_weights,
)
from cash.portfolio.rebalance import (
    RebalanceConfig,
    Trade,
    compute_drift,
    needs_rebalance,
    generate_trades,
)
from cash.portfolio.risk import (
    compute_portfolio_risk,
    drawdown_stats,
)
from cash.output.signals import BUY, HOLD, SELL, NEUTRAL


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestConfig:
    """Full backtest configuration."""

    initial_capital: float = 100_000.0

    sizing: SizingConfig = field(
        default_factory=SizingConfig,
    )
    rebalance: RebalanceConfig = field(
        default_factory=RebalanceConfig,
    )

    # Execution delay: 0 = same-day close, 1 = next-day close
    execution_delay: int = 0

    # Rebalance HOLD positions periodically
    rebalance_holds: bool = True


# ═══════════════════════════════════════════════════════════════
#  RESULT OBJECT
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestResult:
    """Complete backtest output."""

    equity_curve: pd.Series         # date → portfolio value
    daily_returns: pd.Series        # date → daily return
    positions: pd.DataFrame         # date × ticker → shares
    weights: pd.DataFrame           # date × ticker → weight
    trades: list[Trade]             # all trades executed
    metrics: dict                   # performance metrics
    config: BacktestConfig = field(
        default_factory=BacktestConfig,
    )


# ═══════════════════════════════════════════════════════════════
#  BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════

def run_backtest(
    signals_df: pd.DataFrame,
    config: BacktestConfig | None = None,
) -> BacktestResult:
    """
    Run a historical simulation over the signal panel.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``compute_all_signals()``.  Must contain
        columns: signal, signal_strength, close.
    config : BacktestConfig or None
        Simulation parameters.

    Returns
    -------
    BacktestResult
        Equity curve, positions, trades, and metrics.
    """
    if config is None:
        config = BacktestConfig()

    if signals_df.empty or "close" not in signals_df.columns:
        return _empty_result(config)

    # ── Extract price matrix ──────────────────────────────
    prices = signals_df["close"].unstack(level="ticker")
    dates = prices.index.sort_values()

    # ── State ─────────────────────────────────────────────
    cash: float = config.initial_capital
    holdings: dict[str, float] = {}   # ticker → shares

    equity_records: dict[pd.Timestamp, float] = {}
    position_records: list[dict] = []
    weight_records: list[dict] = []
    all_trades: list[Trade] = []

    # ── Signal matrix for delayed execution ───────────────
    signal_wide = signals_df["signal"].unstack(level="ticker")
    strength_wide = signals_df["signal_strength"].unstack(
        level="ticker",
    )

    # Shift for execution delay
    delay = config.execution_delay
    if delay > 0:
        signal_wide = signal_wide.shift(delay)
        strength_wide = strength_wide.shift(delay)

    # ── Day-by-day simulation ─────────────────────────────
    for date in dates:

        if date not in signal_wide.index:
            continue

        today_prices = prices.loc[date].dropna().to_dict()
        today_signals = signal_wide.loc[date].dropna().to_dict()
        today_strengths = (
            strength_wide.loc[date].dropna().to_dict()
        )

        if not today_prices:
            continue

        # ── 1. Mark to market ─────────────────────────────
        portfolio_value = cash
        for ticker, shares in holdings.items():
            price = today_prices.get(ticker)
            if price is not None:
                portfolio_value += shares * price

        # ── 2. Process SELL signals ───────────────────────
        sells = [
            t for t, s in today_signals.items()
            if s == SELL and t in holdings
        ]

        for ticker in sells:
            price = today_prices.get(ticker)
            if price is None or price <= 0:
                continue

            shares = holdings.pop(ticker)
            trade_value = shares * price
            commission = (
                trade_value * config.rebalance.commission_pct
            )
            slippage = (
                trade_value * config.rebalance.slippage_pct
            )
            cash += trade_value - commission - slippage

            all_trades.append(Trade(
                date=date,
                ticker=ticker,
                action="SELL",
                shares=shares,
                price=price,
                value=trade_value,
                commission=commission,
                slippage=slippage,
            ))

        # ── 3. Determine active set and target weights ────
        active = [
            t for t, s in today_signals.items()
            if s in (BUY, HOLD)
        ]

        if active:
            # Compute volatilities for vol-based sizing
            vols = _compute_volatilities(
                prices, date, active, config.sizing.vol_lookback,
            )

            target_weights = compute_target_weights(
                tickers=active,
                config=config.sizing,
                strengths={
                    t: today_strengths.get(t, 0.5)
                    for t in active
                },
                volatilities=vols,
            )
        else:
            target_weights = {}

        # ── 4. Recalculate portfolio value after sells ────
        portfolio_value = cash
        for ticker, shares in holdings.items():
            price = today_prices.get(ticker)
            if price is not None:
                portfolio_value += shares * price

        # ── 5. Check rebalance need ──────────────────────
        current_weights = {}
        if portfolio_value > 0:
            for ticker, shares in holdings.items():
                price = today_prices.get(ticker, 0)
                current_weights[ticker] = (
                    shares * price / portfolio_value
                )

        drift = compute_drift(current_weights, target_weights)

        # New BUY signals always trigger trades
        new_buys = [
            t for t, s in today_signals.items()
            if s == BUY and t not in holdings
        ]

        do_rebalance = (
            bool(new_buys)
            or (
                config.rebalance_holds
                and needs_rebalance(drift, config.rebalance)
            )
        )

        # ── 6. Generate and execute trades ────────────────
        if do_rebalance and target_weights:
            trades = generate_trades(
                current_positions=dict(holdings),
                target_weights=target_weights,
                prices=today_prices,
                portfolio_value=portfolio_value,
                date=date,
                config=config.rebalance,
            )

            for trade in trades:
                if trade.action == "SELL":
                    sold_shares = min(
                        trade.shares,
                        holdings.get(trade.ticker, 0),
                    )
                    if sold_shares > 0:
                        cost = (
                            trade.commission + trade.slippage
                        )
                        cash += sold_shares * trade.price - cost
                        holdings[trade.ticker] = (
                            holdings.get(trade.ticker, 0)
                            - sold_shares
                        )
                        if holdings[trade.ticker] <= 0.001:
                            holdings.pop(trade.ticker, None)

                        all_trades.append(Trade(
                            date=trade.date,
                            ticker=trade.ticker,
                            action="SELL",
                            shares=sold_shares,
                            price=trade.price,
                            value=sold_shares * trade.price,
                            commission=trade.commission,
                            slippage=trade.slippage,
                        ))

                elif trade.action == "BUY":
                    cost = (
                        trade.value
                        + trade.commission
                        + trade.slippage
                    )
                    if cost <= cash:
                        cash -= cost
                        holdings[trade.ticker] = (
                            holdings.get(trade.ticker, 0)
                            + trade.shares
                        )
                        all_trades.append(trade)

        # ── 7. End-of-day portfolio value ─────────────────
        # First pass: compute total portfolio value
        eod_value = cash
        pos_record = {"date": date, "_cash": cash}

        for ticker, shares in holdings.items():
            price = today_prices.get(ticker, 0)
            eod_value += shares * price
            pos_record[ticker] = shares

        # Second pass: compute weights using correct total
        wt_record = {"date": date}
        if eod_value > 0:
            for ticker, shares in holdings.items():
                price = today_prices.get(ticker, 0)
                wt_record[ticker] = (
                    (shares * price) / eod_value
                )

        equity_records[date] = eod_value
        position_records.append(pos_record)
        weight_records.append(wt_record)

    # ── Build result ──────────────────────────────────────
    equity = pd.Series(equity_records, name="equity")
    equity.index.name = "date"

    daily_ret = equity.pct_change().fillna(0)
    daily_ret.name = "return"

    positions_df = pd.DataFrame(position_records).set_index(
        "date",
    ).fillna(0)
    weights_df = pd.DataFrame(weight_records).set_index(
        "date",
    ).fillna(0)

    # Compute metrics
    metrics = compute_performance_metrics(
        equity, daily_ret, all_trades, config,
    )

    return BacktestResult(
        equity_curve=equity,
        daily_returns=daily_ret,
        positions=positions_df,
        weights=weights_df,
        trades=all_trades,
        metrics=metrics,
        config=config,
    )


# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE METRICS
# ═══════════════════════════════════════════════════════════════

def compute_performance_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    trades: list[Trade],
    config: BacktestConfig,
) -> dict:
    """
    Compute comprehensive performance metrics.

    Returns dict with: total_return, cagr, annual_volatility,
    sharpe_ratio, sortino_ratio, calmar_ratio, max_drawdown,
    max_dd_duration, current_drawdown, total_trades, win_rate,
    profit_factor, avg_win, avg_loss, total_commission,
    initial_capital, final_capital, peak_capital.
    """
    if equity.empty:
        return {}

    initial = config.initial_capital
    final = equity.iloc[-1]
    peak = equity.max()

    # Returns
    total_return = (final / initial) - 1
    n_days = len(equity)
    n_years = max(n_days / 252, 0.01)
    cagr = (final / initial) ** (1 / n_years) - 1

    # Volatility
    ann_vol = float(daily_returns.std() * np.sqrt(252))

    # Sharpe (assume rf = 0)
    mean_daily = daily_returns.mean()
    sharpe = (
        (mean_daily / daily_returns.std() * np.sqrt(252))
        if daily_returns.std() > 0 else 0.0
    )

    # Sortino (downside vol)
    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 0 else 0.001
    sortino = (
        mean_daily / down_std * np.sqrt(252)
        if down_std > 0 else 0.0
    )

    # Drawdown
    dd = drawdown_stats(equity)
    max_dd = dd.get("max_drawdown", 0)
    calmar = abs(cagr / max_dd) if max_dd < 0 else 0.0

    # Trade statistics
    trade_pnls = _compute_trade_pnls(trades)
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    n_trades = len(trade_pnls)
    win_rate = len(wins) / max(n_trades, 1)

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    profit_factor = gross_profit / gross_loss

    avg_win = np.mean(wins) if wins else 0.0
    avg_loss = np.mean(losses) if losses else 0.0

    total_commission = sum(
        t.commission + t.slippage for t in trades
    )

    return {
        "total_return": total_return,
        "cagr": cagr,
        "annual_volatility": ann_vol,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "max_drawdown": dd.get("max_drawdown", 0),
        "max_dd_duration": dd.get("max_dd_duration", 0),
        "current_drawdown": dd.get("current_drawdown", 0),
        "total_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "total_commission": total_commission,
        "initial_capital": initial,
        "final_capital": final,
        "peak_capital": peak,
        "n_days": n_days,
        "n_years": n_years,
    }


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _compute_volatilities(
    prices: pd.DataFrame,
    current_date: pd.Timestamp,
    tickers: list[str],
    lookback: int,
) -> dict[str, float]:
    """Compute annualised volatility for each ticker."""
    vols = {}
    for ticker in tickers:
        if ticker not in prices.columns:
            continue
        loc = prices.index.get_loc(current_date)
        start = max(0, loc - lookback)
        window = prices.iloc[start: loc + 1][ticker].dropna()
        if len(window) > 2:
            ret = window.pct_change().dropna()
            vols[ticker] = float(ret.std() * np.sqrt(252))
        else:
            vols[ticker] = 0.20  # default 20% vol
    return vols


def _compute_trade_pnls(trades: list[Trade]) -> list[float]:
    """
    Compute P&L for each round-trip trade pair.

    Matches BUY → SELL pairs per ticker in FIFO order and
    computes the percentage return for each closed trade.
    """
    open_trades: dict[str, list[Trade]] = {}
    pnls: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                entry_cost = (
                    entry.price
                    + entry.price * 0.0015  # approx costs
                )
                exit_net = (
                    trade.price
                    - trade.price * 0.0015
                )
                pnl = (exit_net / entry_cost) - 1
                pnls.append(pnl)

    return pnls


def _empty_result(config: BacktestConfig) -> BacktestResult:
    """Return an empty BacktestResult."""
    return BacktestResult(
        equity_curve=pd.Series(dtype=float),
        daily_returns=pd.Series(dtype=float),
        positions=pd.DataFrame(),
        weights=pd.DataFrame(),
        trades=[],
        metrics={},
        config=config,
    )