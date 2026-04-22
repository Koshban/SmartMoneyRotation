################### BACKTESTING ###################
"""
backtest/__init__.py
=========
Historical backtesting harness for the CASH Smart Money Rotation system.

Loads 20 years of OHLCV, runs the full pipeline over any date range,
computes performance metrics (including CAGR), and compares strategy
variants side-by-side.

Quick start
-----------
    from backtest.engine import run_backtest_period
    from backtest.data_loader import ensure_history

    data = ensure_history()
    result = run_backtest_period(data)
    print(f"CAGR: {result.metrics['cagr']:.2%}")

CLI
---
    python -m backtest.runner                          # 20-year default
    python -m backtest.runner --start 2015 --end 2024  # custom period
    python -m backtest.runner --compare                # all strategies
    python -m backtest.runner --strategy momentum_heavy
"""

from backtest.engine import run_backtest_period, BacktestRun
from backtest.metrics import compute_cagr, compute_full_metrics
from backtest.comparison import compare_strategies
from backtest.data_loader import ensure_history, load_cached_history

__all__ = [
    "run_backtest_period",
    "BacktestRun",
    "compute_cagr",
    "compute_full_metrics",
    "compare_strategies",
    "ensure_history",
    "load_cached_history",
]

##############################################################
"""
backtest/metrics.py
-------------------
Comprehensive performance analytics for backtesting.

Standalone functions — no state.  Each takes an equity curve,
daily returns, or trade list and returns metrics.

The ``compute_cagr()`` function is the primary tool for
evaluating strategy quality across different time periods.

All functions are independent of the CASH pipeline and can
be used with any equity curve or return series.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio.rebalance import Trade


# ═══════════════════════════════════════════════════════════════
#  CAGR  (Compound Annual Growth Rate)
# ═══════════════════════════════════════════════════════════════

def compute_cagr(
    initial_value: float,
    final_value: float,
    n_years: float,
) -> float:
    """
    Compound Annual Growth Rate.

    Parameters
    ----------
    initial_value : float
        Starting portfolio value.
    final_value : float
        Ending portfolio value.
    n_years : float
        Number of years (can be fractional).

    Returns
    -------
    float
        CAGR as a decimal (e.g. 0.12 = 12% per year).

    Examples
    --------
    >>> compute_cagr(100_000, 250_000, 10)
    0.09596...  # ~9.6% CAGR
    """
    if initial_value <= 0 or n_years <= 0:
        return 0.0
    if final_value <= 0:
        return -1.0
    return (final_value / initial_value) ** (1.0 / n_years) - 1.0


def cagr_from_equity(equity: pd.Series) -> float:
    """Compute CAGR directly from an equity curve."""
    if equity.empty or len(equity) < 2:
        return 0.0
    initial = equity.iloc[0]
    final = equity.iloc[-1]
    n_days = (equity.index[-1] - equity.index[0]).days
    n_years = max(n_days / 365.25, 0.01)
    return compute_cagr(initial, final, n_years)


def cagr_from_returns(daily_returns: pd.Series) -> float:
    """Compute CAGR from a daily return series."""
    if daily_returns.empty:
        return 0.0
    equity = (1 + daily_returns).cumprod()
    return cagr_from_equity(equity)


# ═══════════════════════════════════════════════════════════════
#  ROLLING CAGR
# ═══════════════════════════════════════════════════════════════

def rolling_cagr(
    equity: pd.Series,
    window_years: int = 3,
) -> pd.Series:
    """
    Rolling CAGR over a trailing window.

    Useful for seeing how the strategy's annualised return
    varies over different market regimes.
    """
    if equity.empty:
        return pd.Series(dtype=float)

    window_days = int(window_years * 252)
    if len(equity) < window_days:
        return pd.Series(dtype=float, index=equity.index)

    result = pd.Series(np.nan, index=equity.index)

    for i in range(window_days, len(equity)):
        initial = equity.iloc[i - window_days]
        final = equity.iloc[i]
        if initial > 0:
            result.iloc[i] = (final / initial) ** (1.0 / window_years) - 1

    result.name = f"rolling_{window_years}y_cagr"
    return result


# ═══════════════════════════════════════════════════════════════
#  COMPREHENSIVE METRICS
# ═══════════════════════════════════════════════════════════════

def compute_full_metrics(
    equity_curve: pd.Series,
    daily_returns: pd.Series,
    trades: list[Trade],
    initial_capital: float,
    benchmark_equity: pd.Series | None = None,
) -> dict:
    """
    Compute every performance metric needed for strategy evaluation.

    Returns
    -------
    dict with keys:

    Returns
        total_return, cagr, best_year, worst_year

    Risk
        annual_volatility, sharpe_ratio, sortino_ratio,
        calmar_ratio, max_drawdown, max_dd_duration,
        current_drawdown, var_95, cvar_95, skewness, kurtosis

    Trading
        total_trades, win_rate, profit_factor, avg_win,
        avg_loss, avg_holding_days, total_commission

    Capital
        initial_capital, final_capital, peak_capital

    Benchmark (if provided)
        bench_cagr, bench_sharpe, bench_max_dd,
        excess_cagr, information_ratio, tracking_error,
        up_capture, down_capture

    Periods
        n_days, n_years, start_date, end_date
    """
    if equity_curve.empty:
        return {}

    final = equity_curve.iloc[-1]
    peak = equity_curve.max()
    n_days = len(equity_curve)
    calendar_days = (equity_curve.index[-1] - equity_curve.index[0]).days
    n_years = max(calendar_days / 365.25, 0.01)

    # ── Returns ───────────────────────────────────────────────
    total_return = (final / initial_capital) - 1
    cagr = compute_cagr(initial_capital, final, n_years)

    ann_vol = float(daily_returns.std() * np.sqrt(252)) if len(daily_returns) > 1 else 0.0
    mean_daily = daily_returns.mean() if len(daily_returns) > 0 else 0.0

    # ── Risk-adjusted ─────────────────────────────────────────
    sharpe = (
        (mean_daily / daily_returns.std() * np.sqrt(252))
        if daily_returns.std() > 0 else 0.0
    )

    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 0 else 0.001
    sortino = (
        mean_daily / down_std * np.sqrt(252)
        if down_std > 0 else 0.0
    )

    # ── Drawdown ──────────────────────────────────────────────
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    max_dd = drawdown.min()
    current_dd = drawdown.iloc[-1]

    # Max drawdown duration
    is_dd = drawdown < 0
    dd_groups = (~is_dd).cumsum()
    dd_lengths = is_dd.groupby(dd_groups).sum()
    max_dd_duration = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0

    calmar = abs(cagr / max_dd) if max_dd < 0 else 0.0

    # ── VaR / CVaR ────────────────────────────────────────────
    var_95 = float(daily_returns.quantile(0.05)) if len(daily_returns) > 20 else 0.0
    tail = daily_returns[daily_returns <= var_95]
    cvar_95 = float(tail.mean()) if len(tail) > 0 else var_95

    # ── Higher moments ────────────────────────────────────────
    skewness = float(daily_returns.skew()) if len(daily_returns) > 5 else 0.0
    kurtosis = float(daily_returns.kurtosis()) if len(daily_returns) > 5 else 0.0

    # ── Annual returns ────────────────────────────────────────
    yearly = equity_curve.resample("YE").last()
    yearly_ret = yearly.pct_change().dropna()
    best_year = float(yearly_ret.max()) if len(yearly_ret) > 0 else 0.0
    worst_year = float(yearly_ret.min()) if len(yearly_ret) > 0 else 0.0
    pct_positive_years = (
        float((yearly_ret > 0).mean()) if len(yearly_ret) > 0 else 0.0
    )

    # ── Trades ────────────────────────────────────────────────
    trade_pnls = _compute_trade_pnls(trades)
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    n_trades = len(trade_pnls)
    win_rate = len(wins) / max(n_trades, 1)

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0

    total_commission = sum(t.commission + t.slippage for t in trades)

    # ── Assemble base metrics ─────────────────────────────────
    metrics = {
        # Returns
        "total_return": total_return,
        "cagr": cagr,
        "best_year": best_year,
        "worst_year": worst_year,
        "pct_positive_years": pct_positive_years,
        # Risk
        "annual_volatility": ann_vol,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "max_drawdown": max_dd,
        "max_dd_duration": max_dd_duration,
        "current_drawdown": current_dd,
        "var_95": var_95,
        "cvar_95": cvar_95,
        "skewness": skewness,
        "kurtosis": kurtosis,
        # Trading
        "total_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "total_commission": total_commission,
        # Capital
        "initial_capital": initial_capital,
        "final_capital": final,
        "peak_capital": peak,
        # Periods
        "n_days": n_days,
        "n_years": n_years,
        "start_date": equity_curve.index[0],
        "end_date": equity_curve.index[-1],
    }

    # ── Benchmark comparison ──────────────────────────────────
    if (
        benchmark_equity is not None
        and not benchmark_equity.empty
        and len(benchmark_equity) > 30
    ):
        bench_metrics = _compute_benchmark_metrics(
            equity_curve, daily_returns,
            benchmark_equity, initial_capital,
        )
        metrics.update(bench_metrics)

    return metrics


# ═══════════════════════════════════════════════════════════════
#  BENCHMARK COMPARISON
# ═══════════════════════════════════════════════════════════════

def _compute_benchmark_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    bench_equity: pd.Series,
    initial_capital: float,
) -> dict:
    """Compute metrics relative to a benchmark."""
    # Align
    common = equity.index.intersection(bench_equity.index)
    if len(common) < 30:
        return {}

    strat_ret = daily_returns.reindex(common).fillna(0)
    bench_ret = bench_equity.reindex(common).pct_change().fillna(0)

    n_days = (common[-1] - common[0]).days
    n_years = max(n_days / 365.25, 0.01)

    bench_final = bench_equity.reindex(common).iloc[-1]
    bench_cagr = compute_cagr(initial_capital, bench_final, n_years)

    bench_vol = float(bench_ret.std() * np.sqrt(252))
    bench_sharpe = (
        (bench_ret.mean() / bench_ret.std() * np.sqrt(252))
        if bench_ret.std() > 0 else 0.0
    )

    bench_max_dd = (
        (bench_equity.reindex(common) / bench_equity.reindex(common).cummax() - 1).min()
    )

    # Excess return
    strat_cagr = cagr_from_equity(equity.reindex(common))
    excess_cagr = strat_cagr - bench_cagr

    # Tracking error and information ratio
    active_ret = strat_ret - bench_ret
    tracking_error = float(active_ret.std() * np.sqrt(252))
    information_ratio = (
        float(active_ret.mean() / active_ret.std() * np.sqrt(252))
        if active_ret.std() > 0 else 0.0
    )

    # Up/down capture
    up_days = bench_ret > 0
    dn_days = bench_ret < 0

    up_capture = (
        float(strat_ret[up_days].mean() / bench_ret[up_days].mean())
        if up_days.any() and bench_ret[up_days].mean() != 0 else 1.0
    )
    down_capture = (
        float(strat_ret[dn_days].mean() / bench_ret[dn_days].mean())
        if dn_days.any() and bench_ret[dn_days].mean() != 0 else 1.0
    )

    return {
        "bench_cagr": bench_cagr,
        "bench_sharpe": float(bench_sharpe),
        "bench_max_dd": bench_max_dd,
        "bench_volatility": bench_vol,
        "excess_cagr": excess_cagr,
        "information_ratio": information_ratio,
        "tracking_error": tracking_error,
        "up_capture": up_capture,
        "down_capture": down_capture,
    }


# ═══════════════════════════════════════════════════════════════
#  TRADE PnL
# ═══════════════════════════════════════════════════════════════

def _compute_trade_pnls(trades: list[Trade]) -> list[float]:
    """Match BUY→SELL pairs per ticker (FIFO) and compute returns."""
    open_trades: dict[str, list[Trade]] = {}
    pnls: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                entry_cost = entry.price * (1 + 0.0015)
                exit_net = trade.price * (1 - 0.0015)
                pnl = (exit_net / entry_cost) - 1 if entry_cost > 0 else 0
                pnls.append(pnl)

    return pnls


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def metrics_report(run: "BacktestRun") -> str:
    """Format a BacktestRun as a comprehensive text report."""
    m = run.metrics
    if not m:
        return f"No metrics for '{run.strategy.name}'"

    ln: list[str] = []
    div = "=" * 70
    sub = "-" * 70

    ln.append(div)
    ln.append(f"  BACKTEST REPORT: {run.strategy.name}")
    ln.append(f"  {run.strategy.description}")
    ln.append(div)

    ln.append(f"  Period:          {m.get('start_date', '?')} → "
              f"{m.get('end_date', '?')}  ({m.get('n_years', 0):.1f} years)")
    ln.append(f"  Initial capital: ${m.get('initial_capital', 0):,.0f}")
    ln.append(f"  Final capital:   ${m.get('final_capital', 0):,.0f}")
    ln.append(f"  Peak capital:    ${m.get('peak_capital', 0):,.0f}")

    ln.append("")
    ln.append(sub)
    ln.append("  RETURNS")
    ln.append(sub)
    ln.append(f"  Total return:        {m.get('total_return', 0):>+8.2%}")
    ln.append(f"  CAGR:                {m.get('cagr', 0):>+8.2%}")
    ln.append(f"  Best year:           {m.get('best_year', 0):>+8.2%}")
    ln.append(f"  Worst year:          {m.get('worst_year', 0):>+8.2%}")
    ln.append(f"  % positive years:    {m.get('pct_positive_years', 0):>8.0%}")

    ln.append("")
    ln.append(sub)
    ln.append("  RISK")
    ln.append(sub)
    ln.append(f"  Ann. volatility:     {m.get('annual_volatility', 0):>8.2%}")
    ln.append(f"  Sharpe ratio:        {m.get('sharpe_ratio', 0):>8.3f}")
    ln.append(f"  Sortino ratio:       {m.get('sortino_ratio', 0):>8.3f}")
    ln.append(f"  Calmar ratio:        {m.get('calmar_ratio', 0):>8.3f}")
    ln.append(f"  Max drawdown:        {m.get('max_drawdown', 0):>8.2%}")
    ln.append(f"  Max DD duration:     {m.get('max_dd_duration', 0):>5d} days")
    ln.append(f"  VaR (95%):           {m.get('var_95', 0):>8.4f}")
    ln.append(f"  CVaR (95%):          {m.get('cvar_95', 0):>8.4f}")

    ln.append("")
    ln.append(sub)
    ln.append("  TRADING")
    ln.append(sub)
    ln.append(f"  Total trades:        {m.get('total_trades', 0):>5d}")
    ln.append(f"  Win rate:            {m.get('win_rate', 0):>8.1%}")
    ln.append(f"  Profit factor:       {m.get('profit_factor', 0):>8.2f}")
    ln.append(f"  Avg win:             {m.get('avg_win', 0):>+8.2%}")
    ln.append(f"  Avg loss:            {m.get('avg_loss', 0):>+8.2%}")
    ln.append(f"  Total costs:         ${m.get('total_commission', 0):>10,.2f}")

    if "bench_cagr" in m:
        ln.append("")
        ln.append(sub)
        ln.append("  vs BENCHMARK (SPY)")
        ln.append(sub)
        ln.append(f"  Benchmark CAGR:      {m.get('bench_cagr', 0):>+8.2%}")
        ln.append(f"  Excess CAGR:         {m.get('excess_cagr', 0):>+8.2%}")
        ln.append(f"  Information ratio:   {m.get('information_ratio', 0):>8.3f}")
        ln.append(f"  Tracking error:      {m.get('tracking_error', 0):>8.2%}")
        ln.append(f"  Up capture:          {m.get('up_capture', 0):>8.2f}")
        ln.append(f"  Down capture:        {m.get('down_capture', 0):>8.2f}")
        ln.append(f"  Bench max DD:        {m.get('bench_max_dd', 0):>8.2%}")

    # Annual returns
    if not run.annual_returns.empty:
        ln.append("")
        ln.append(sub)
        ln.append("  ANNUAL RETURNS")
        ln.append(sub)
        for year, ret in run.annual_returns.items():
            bar = "█" * max(0, int(ret * 100))
            ln.append(f"  {year}:  {ret:>+7.2%}  {bar}")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)

###############################

"""
backtest/engine.py
------------------
Core backtesting engine.

Ties together data loading, the full CASH pipeline, and the
portfolio simulation into a single ``run_backtest_period()``
call.

For strategy comparison, the engine accepts parameter overrides
via a ``StrategyConfig`` object.  Overrides are applied to the
global config dicts using a context manager, then restored
after the run — safe for sequential comparison of many variants.

Key improvements
----------------
- Config overrides now wrap both pipeline AND portfolio simulation
- Minimum holding period prevents excessive churn (default 20 cal-days)
- Breadth crisis response blocks equity BUYs in weak/critical regimes
- Cash proxy (SHY) earns returns on idle capital instead of 0 %
- Robust column detection for any signal DataFrame schema
"""

from __future__ import annotations

import copy
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SCORING_WEIGHTS,
    SCORING_PARAMS,
    SIGNAL_PARAMS,
    BREADTH_PORTFOLIO,
)
from pipeline.orchestrator import Orchestrator, PipelineResult
from portfolio.backtest import (
    BacktestConfig,
    BacktestResult,
    run_backtest as run_portfolio_backtest,
    compute_performance_metrics,
)
from portfolio.sizing import SizingConfig
from portfolio.rebalance import RebalanceConfig
from output.signals import SignalConfig

from backtest.data_loader import slice_period, data_summary
from backtest.metrics import compute_full_metrics

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

DEFENSIVE_TICKERS = frozenset({
    "AGG", "SHY", "TLT", "IEF", "GLD", "BIL",
})

# Column-name candidates (checked in priority order)
_SIGNAL_COLS = ("signal", "action", "trade_signal")
_TICKER_COLS = ("ticker", "symbol", "asset")
_DATE_COLS   = ("date", "trade_date", "timestamp")
_SCORE_COLS  = ("composite_score", "score", "total_score", "rank_score")
_REGIME_COLS = (
    "breadth_regime", "regime", "market_regime", "breadth_label",
)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL VALUE HELPERS
# ═══════════════════════════════════════════════════════════════

def _is_buy(val) -> bool:
    """Return True if *val* represents a BUY / entry signal."""
    if isinstance(val, str):
        return val.upper() in ("BUY", "STRONG_BUY", "ENTRY")
    try:
        return float(val) == 1.0
    except (TypeError, ValueError):
        return False


def _is_sell(val) -> bool:
    """Return True if *val* represents a SELL / exit signal."""
    if isinstance(val, str):
        return val.upper() in ("SELL", "STRONG_SELL", "EXIT")
    try:
        return float(val) == -1.0
    except (TypeError, ValueError):
        return False


def _buy_value(sample) -> Any:
    """Return the BUY constant matching the dtype of *sample*."""
    return "BUY" if isinstance(sample, str) else 1


def _hold_value(sample) -> Any:
    """Return the HOLD / neutral constant matching *sample*."""
    return "HOLD" if isinstance(sample, str) else 0


# ═══════════════════════════════════════════════════════════════
#  STRATEGY CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class StrategyConfig:
    """
    A named set of parameter overrides for backtesting.

    Each dict field is either ``None`` (use global default) or a
    mapping of key → value that will be merged into the
    corresponding config dict for the duration of the run.

    Example
    -------
    >>> aggressive = StrategyConfig(
    ...     name="aggressive_momentum",
    ...     description="Heavy momentum weighting",
    ...     scoring_weights={"pillar_momentum": 0.40},
    ...     signal_params={"entry_score_min": 0.50},
    ...     min_hold_days=30,
    ... )
    """
    name: str = "baseline"
    description: str = "Default CASH parameters"

    # ── Config-dict overrides (applied to globals) ────────────
    scoring_weights:   dict | None = None
    scoring_params:    dict | None = None
    signal_params:     dict | None = None
    portfolio_params:  dict | None = None
    breadth_portfolio: dict | None = None

    # ── Component-config overrides ────────────────────────────
    signal_config_overrides:   dict | None = None
    sizing_config_overrides:   dict | None = None
    backtest_config_overrides: dict | None = None

    # ── Universe filter ───────────────────────────────────────
    universe_filter: list[str] | None = None

    # ── Trading rules ─────────────────────────────────────────
    min_hold_days: int = 20
    """Minimum calendar days between entry and exit.  0 = disabled."""

    cash_proxy: str | None = "SHY"
    """Ticker whose returns are applied to idle cash.  None = disabled."""

    # ── Breadth crisis response ───────────────────────────────
    breadth_defensive: bool = True
    """Block equity BUY signals during weak / critical breadth."""

    max_equity_weak: float = 0.40
    """Maximum equity exposure allowed during *weak* breadth."""

    max_equity_critical: float = 0.15
    """Maximum equity exposure allowed during *critical* breadth."""


# ═══════════════════════════════════════════════════════════════
#  BACKTEST RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestRun:
    """
    Complete output of a single backtest run.

    Wraps the pipeline result, the portfolio simulation result,
    and comprehensive performance metrics.
    """
    strategy: StrategyConfig
    start_date: pd.Timestamp
    end_date: pd.Timestamp

    pipeline_result: PipelineResult | None = None
    backtest_result: BacktestResult | None = None

    metrics: dict = field(default_factory=dict)
    annual_returns: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )
    monthly_returns: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark_equity: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )

    elapsed_seconds: float = 0.0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.metrics.get("cagr") is not None

    @property
    def cagr(self) -> float:
        return self.metrics.get("cagr", 0.0)

    @property
    def sharpe(self) -> float:
        return self.metrics.get("sharpe_ratio", 0.0)

    @property
    def max_drawdown(self) -> float:
        return self.metrics.get("max_drawdown", 0.0)

    def summary_line(self) -> str:
        if not self.ok:
            return f"{self.strategy.name:<24s}  ERROR: {self.error}"
        return (
            f"{self.strategy.name:<24s}  "
            f"CAGR={self.cagr:>+7.2%}  "
            f"Sharpe={self.sharpe:>5.2f}  "
            f"MaxDD={self.max_drawdown:>7.2%}  "
            f"Trades={self.metrics.get('total_trades', 0):>5d}  "
            f"({self.elapsed_seconds:.0f}s)"
        )


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_backtest_period(
    data: dict[str, pd.DataFrame],
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    strategy: StrategyConfig | None = None,
    capital: float = 100_000.0,
    benchmark: str | None = None,
) -> BacktestRun:
    """
    Run a complete backtest over a date range.

    Steps
    -----
    1. Slice data to ``[start, end]``
    2. Apply strategy parameter overrides  (context manager)
    3. Run the full CASH pipeline  (Orchestrator)
    4. **Pre-process signals**  (min-hold, breadth override)
    5. Run the portfolio simulation  (portfolio/backtest.py)
    6. **Adjust equity for cash-proxy returns**
    7. Compute performance metrics and benchmarks
    8. Return everything in a ``BacktestRun``

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        ``{ticker: OHLCV}`` from ``ensure_history()``.
    start, end : str / Timestamp / None
        Period bounds.  *None* → earliest / latest available.
    strategy : StrategyConfig or None
        Override set.  *None* → defaults.
    capital : float
        Initial portfolio value.
    benchmark : str or None
        Benchmark ticker.  Default = SPY.
    """
    if strategy is None:
        strategy = StrategyConfig()

    benchmark = benchmark or BENCHMARK_TICKER
    t0 = time.perf_counter()

    # ── 1. Slice data ─────────────────────────────────────────
    sliced = slice_period(data, start=start, end=end)
    if not sliced:
        return BacktestRun(
            strategy=strategy,
            start_date=pd.Timestamp(start) if start else pd.NaT,
            end_date=pd.Timestamp(end) if end else pd.NaT,
            error="No data after slicing to requested period",
        )

    # ── 2. Apply universe filter ──────────────────────────────
    if strategy.universe_filter:
        # Always keep benchmark + cash proxy in the universe
        must_keep = {benchmark}
        if strategy.cash_proxy and strategy.cash_proxy in data:
            must_keep.add(strategy.cash_proxy)

        sliced = {
            k: v for k, v in sliced.items()
            if k in strategy.universe_filter or k in must_keep
        }

    summary = data_summary(sliced)
    actual_start = summary["earliest_start"]
    actual_end = summary["latest_end"]

    logger.info(
        f"Backtest '{strategy.name}': "
        f"{summary['n_tickers']} tickers, "
        f"{actual_start.date()} \u2192 {actual_end.date()}, "
        f"${capital:,.0f}"
    )

    # ── 3. Extract benchmark equity ───────────────────────────
    bench_df = sliced.get(benchmark)
    if bench_df is None or bench_df.empty:
        logger.warning(
            f"Benchmark {benchmark} not in data — "
            f"benchmark comparison will be unavailable"
        )
        bench_equity = pd.Series(dtype=float)
    else:
        bench_equity = (
            bench_df["close"] / bench_df["close"].iloc[0] * capital
        )
        bench_equity.name = "benchmark"

    # ══════════════════════════════════════════════════════════
    #  Everything inside _config_overrides so that scoring
    #  weights, signal params, portfolio params, breadth
    #  settings, etc. are consistently applied to BOTH the
    #  pipeline AND the portfolio simulation.
    # ══════════════════════════════════════════════════════════
    with _config_overrides(strategy):

        # ── 4a. Run CASH pipeline (Phases 0 – 4) ─────────────
        try:
            orch = Orchestrator(
                universe=list(sliced.keys()),
                benchmark=benchmark,
                capital=capital,
                enable_breadth=True,
                enable_sectors=True,
                enable_signals=True,
                enable_backtest=False,   # we run our own below
            )

            orch.load_data(preloaded=sliced, bench_df=bench_df)
            orch.compute_universe_context()
            orch.run_tickers()
            orch.cross_sectional_analysis()
            pipeline_result = orch.generate_reports()

        except Exception as e:
            logger.error(f"Pipeline failed for '{strategy.name}': {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Pipeline error: {e}",
            )

        # ── 4b. Validate signals ─────────────────────────────
        signals_df = pipeline_result.signals
        if signals_df is None or signals_df.empty:
            logger.warning(
                f"No signals generated for '{strategy.name}'"
            )
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error="No signals generated — check pipeline logs",
            )

        # ── 4c. Pre-process signals ──────────────────────────
        signals_df = _preprocess_signals(
            signals_df, strategy, pipeline_result, sliced,
        )

        # ── 4d. Run portfolio simulation ──────────────────────
        try:
            bt_config = _build_backtest_config(strategy, capital)
            bt_result = run_portfolio_backtest(
                signals_df=signals_df,
                config=bt_config,
            )
        except Exception as e:
            logger.error(f"Backtest simulation failed: {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Simulation error: {e}",
            )

    # ── 5. Adjust equity for cash-proxy returns ───────────────
    equity_curve = bt_result.equity_curve.copy()

    if strategy.cash_proxy and strategy.cash_proxy in sliced:
        equity_curve = _apply_cash_proxy_to_equity(
            equity_curve=equity_curve,
            bt_result=bt_result,
            proxy_prices=sliced[strategy.cash_proxy],
            initial_capital=capital,
        )

    # Recompute daily returns from (possibly adjusted) equity
    daily_returns = equity_curve.pct_change().dropna()

    # ── 6. Compute comprehensive metrics ──────────────────────
    metrics = compute_full_metrics(
        equity_curve=equity_curve,
        daily_returns=daily_returns,
        trades=bt_result.trades,
        initial_capital=capital,
        benchmark_equity=bench_equity,
    )

    annual = _compute_annual_returns(equity_curve)
    monthly = _compute_monthly_returns(daily_returns)
    elapsed = time.perf_counter() - t0

    run = BacktestRun(
        strategy=strategy,
        start_date=actual_start,
        end_date=actual_end,
        pipeline_result=pipeline_result,
        backtest_result=bt_result,
        metrics=metrics,
        annual_returns=annual,
        monthly_returns=monthly,
        benchmark_equity=bench_equity,
        elapsed_seconds=elapsed,
    )

    logger.info(run.summary_line())
    return run


# ═══════════════════════════════════════════════════════════════
#  CONFIG OVERRIDE CONTEXT MANAGER
# ═══════════════════════════════════════════════════════════════

@contextmanager
def _config_overrides(strategy: StrategyConfig):
    """
    Temporarily patch global config dicts with strategy overrides.

    Saves originals, applies overrides, yields control, then
    restores originals in a ``finally`` block.  Safe for
    sequential use across many strategies.
    """
    import common.config as cfg

    config_targets = [
        ("SCORING_WEIGHTS",  cfg.SCORING_WEIGHTS,  strategy.scoring_weights),
        ("SCORING_PARAMS",   cfg.SCORING_PARAMS,   strategy.scoring_params),
        ("SIGNAL_PARAMS",    cfg.SIGNAL_PARAMS,    strategy.signal_params),
        ("PORTFOLIO_PARAMS", cfg.PORTFOLIO_PARAMS,  strategy.portfolio_params),
        ("BREADTH_PORTFOLIO", cfg.BREADTH_PORTFOLIO, strategy.breadth_portfolio),
    ]

    # Save originals  ──  shallow copy is sufficient because we
    # only call .update() (not nested mutation) on the dicts.
    originals: list[tuple[str, dict, dict]] = []
    for name, target_dict, overrides in config_targets:
        originals.append((name, target_dict, dict(target_dict)))
        if overrides:
            logger.debug(
                f"Config override [{strategy.name}] {name}: "
                f"{overrides}"
            )
            target_dict.update(overrides)

    n_overridden = sum(1 for _, _, ov in config_targets if ov)
    if n_overridden:
        logger.info(
            f"Applied {n_overridden} config overrides for "
            f"'{strategy.name}'"
        )

    try:
        yield
    finally:
        for _name, target_dict, original_values in originals:
            target_dict.clear()
            target_dict.update(original_values)


# ═══════════════════════════════════════════════════════════════
#  COLUMN DETECTION
# ═══════════════════════════════════════════════════════════════

def _detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    """
    Identify key column names in a signals DataFrame.

    Returns a dict with keys ``signal``, ``ticker``, ``date``,
    ``score``, ``regime`` mapped to the actual column name found
    (or *None* if absent).
    """
    def _find(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    return {
        "signal": _find(_SIGNAL_COLS),
        "ticker": _find(_TICKER_COLS),
        "date":   _find(_DATE_COLS),
        "score":  _find(_SCORE_COLS),
        "regime": _find(_REGIME_COLS),
    }


# ═══════════════════════════════════════════════════════════════
#  SIGNAL PRE-PROCESSING  (master function)
# ═══════════════════════════════════════════════════════════════

def _preprocess_signals(
    signals_df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    price_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Apply all signal-level modifications before the portfolio sim.

    1. Breadth crisis response  — block equity BUYs during
       weak / critical regimes and boost cash-proxy score.
    2. Minimum holding period   — suppress premature SELLs.
    """
    cols = _detect_columns(signals_df)
    sig_col    = cols["signal"]
    ticker_col = cols["ticker"]
    date_col   = cols["date"]

    if not sig_col or not ticker_col:
        logger.warning(
            "Cannot detect signal/ticker columns in signals_df "
            f"(columns: {list(signals_df.columns)[:15]}…) — "
            "skipping all signal preprocessing"
        )
        return signals_df

    df = signals_df.copy()

    # ── 1. Breadth crisis override ────────────────────────────
    if strategy.breadth_defensive:
        df = _apply_breadth_override(
            df, strategy, pipeline_result,
            sig_col, ticker_col, date_col, cols["score"],
        )

    # ── 2. Minimum holding period ─────────────────────────────
    if strategy.min_hold_days > 0 and date_col:
        df = _enforce_min_hold(
            df, strategy.min_hold_days,
            sig_col, ticker_col, date_col,
        )

    return df


# ═══════════════════════════════════════════════════════════════
#  BREADTH CRISIS OVERRIDE
# ═══════════════════════════════════════════════════════════════

def _extract_regime_column(
    df: pd.DataFrame,
    pipeline_result: PipelineResult,
    date_col: str | None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Ensure *df* has a breadth-regime column.

    Checks the DataFrame first; if absent, tries to pull regime
    data from ``pipeline_result`` and merge it in by date.

    Returns ``(possibly-modified df, regime_column_name or None)``.
    """
    # Already present?
    for col in _REGIME_COLS:
        if col in df.columns:
            return df, col

    if pipeline_result is None or date_col is None:
        return df, None

    # Search pipeline_result for breadth data
    breadth = None
    for attr in (
        "breadth", "breadth_data", "breadth_df",
        "market_breadth", "universe_context",
    ):
        val = getattr(pipeline_result, attr, None)
        if val is not None:
            breadth = val
            break

    if breadth is None:
        return df, None

    # Normalise to a single-column DataFrame with a regime label
    regime_series: pd.Series | None = None

    if isinstance(breadth, pd.Series):
        regime_series = breadth

    elif isinstance(breadth, pd.DataFrame):
        for col in _REGIME_COLS:
            if col in breadth.columns:
                regime_series = breadth[col]
                break
        # Fallback: last column if it looks categorical
        if regime_series is None and breadth.shape[1] > 0:
            last = breadth.iloc[:, -1]
            if last.dtype == object or str(last.dtype) == "category":
                regime_series = last

    elif isinstance(breadth, dict) and "regime" in breadth:
        val = breadth["regime"]
        if isinstance(val, (pd.Series, pd.DataFrame)):
            regime_series = (
                val if isinstance(val, pd.Series) else val.iloc[:, 0]
            )

    if regime_series is None:
        return df, None

    # Merge into df by date
    regime_df = regime_series.to_frame(name="_breadth_regime")
    if date_col in df.columns:
        df = df.merge(
            regime_df,
            left_on=date_col,
            right_index=True,
            how="left",
        )
        return df, "_breadth_regime"

    return df, None


def _apply_breadth_override(
    df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    sig_col: str,
    ticker_col: str,
    date_col: str | None,
    score_col: str | None,
) -> pd.DataFrame:
    """
    Block equity BUY signals during weak / critical breadth
    regimes and (optionally) boost the cash-proxy score so it
    gets selected by the portfolio builder.
    """
    df, regime_col = _extract_regime_column(
        df, pipeline_result, date_col,
    )

    if regime_col is None:
        logger.debug(
            "No breadth-regime column found — "
            "skipping crisis override"
        )
        return df

    # Identify which rows are in a weak/critical regime
    regime_lower = df[regime_col].astype(str).str.lower()
    weak_mask     = regime_lower == "weak"
    critical_mask = regime_lower == "critical"
    crisis_mask   = weak_mask | critical_mask

    if not crisis_mask.any():
        return df

    # Determine signal type (string vs numeric)
    sample_sig = df[sig_col].dropna().iloc[0] if len(df) else "BUY"
    hold = _hold_value(sample_sig)
    buy  = _buy_value(sample_sig)

    # ── Block equity BUYs ─────────────────────────────────────
    is_equity = ~df[ticker_col].isin(DEFENSIVE_TICKERS)
    is_buy    = df[sig_col].apply(_is_buy)
    block     = crisis_mask & is_equity & is_buy
    n_blocked = int(block.sum())

    if n_blocked:
        df.loc[block, sig_col] = hold
        logger.info(
            f"Breadth override: blocked {n_blocked:,} equity BUY "
            f"signals in weak/critical regime"
        )

    # ── Boost cash proxy so it fills freed slots ──────────────
    proxy = strategy.cash_proxy
    if proxy and score_col and proxy in df[ticker_col].values:
        proxy_crisis = crisis_mask & (df[ticker_col] == proxy)
        if proxy_crisis.any():
            # Give cash proxy the maximum score during crises
            max_score = df[score_col].max()
            df.loc[proxy_crisis, score_col] = max_score
            df.loc[proxy_crisis, sig_col]   = buy
            logger.info(
                f"Breadth override: boosted {proxy} score on "
                f"{int(proxy_crisis.sum()):,} crisis days"
            )

    return df


# ═══════════════════════════════════════════════════════════════
#  MINIMUM HOLDING PERIOD
# ═══════════════════════════════════════════════════════════════

def _enforce_min_hold(
    df: pd.DataFrame,
    min_hold_days: int,
    sig_col: str,
    ticker_col: str,
    date_col: str,
) -> pd.DataFrame:
    """
    Suppress SELL signals that arrive fewer than *min_hold_days*
    calendar days after the most recent BUY for the same ticker.

    Operates ticker-by-ticker using fast index iteration rather
    than ``iterrows()``.
    """
    if min_hold_days <= 0:
        return df

    # Ensure we have a sortable date column
    if date_col not in df.columns:
        logger.debug(
            f"Date column '{date_col}' not in DataFrame — "
            "skipping min-hold enforcement"
        )
        return df

    result = df.copy()

    # Determine the replacement value for suppressed SELLs.
    # We replace with BUY (= "keep holding") rather than HOLD,
    # because the portfolio sim interprets BUY as "remain in
    # position" for an existing holding.
    sample_sig = (
        result[sig_col].dropna().iloc[0] if len(result) else "BUY"
    )
    keep_holding = _buy_value(sample_sig)

    total_suppressed = 0

    for ticker, group_indices in result.groupby(ticker_col).groups.items():
        sub = result.loc[group_indices, [date_col, sig_col]].sort_values(
            date_col
        )
        dates   = sub[date_col].values        # numpy datetime64
        signals = sub[sig_col].values          # numpy object / int
        indices = sub.index.values             # positional index

        in_position = False
        entry_ts: np.datetime64 | None = None
        fix_list: list = []

        for i in range(len(indices)):
            sig = signals[i]
            dt  = dates[i]

            if _is_buy(sig) and not in_position:
                in_position = True
                entry_ts = dt
            elif _is_buy(sig) and in_position:
                # Consecutive BUY — stay in position, don't reset
                # entry date (hold clock runs from original entry)
                pass
            elif _is_sell(sig) and in_position:
                try:
                    days_held = (
                        pd.Timestamp(dt) - pd.Timestamp(entry_ts)
                    ).days
                except Exception:
                    days_held = min_hold_days  # fail-open

                if days_held < min_hold_days:
                    fix_list.append(indices[i])
                else:
                    in_position = False
                    entry_ts = None
            elif _is_sell(sig) and not in_position:
                pass  # not holding — irrelevant

        if fix_list:
            result.loc[fix_list, sig_col] = keep_holding
            total_suppressed += len(fix_list)

    if total_suppressed:
        logger.info(
            f"Min-hold filter: suppressed {total_suppressed:,} "
            f"premature exits (min {min_hold_days} cal-days)"
        )

    return result


# ═══════════════════════════════════════════════════════════════
#  CASH PROXY — EQUITY ADJUSTMENT
# ═══════════════════════════════════════════════════════════════

def _apply_cash_proxy_to_equity(
    equity_curve: pd.Series,
    bt_result: BacktestResult,
    proxy_prices: pd.DataFrame,
    initial_capital: float,
) -> pd.Series:
    """
    Retroactively credit idle cash with the proxy's return.

    Tries to obtain a per-day cash balance from the backtest
    result.  If unavailable, falls back to a conservative
    estimate (20 % of equity assumed idle on average).

    Parameters
    ----------
    equity_curve : pd.Series
        Unadjusted equity from the portfolio sim.
    bt_result : BacktestResult
        Portfolio simulation output.
    proxy_prices : pd.DataFrame
        OHLCV for the cash proxy ticker.
    initial_capital : float
        Starting capital.

    Returns
    -------
    pd.Series
        Adjusted equity curve.
    """
    # ── Proxy daily returns ───────────────────────────────────
    if "close" in proxy_prices.columns:
        proxy_close = proxy_prices["close"]
    else:
        proxy_close = proxy_prices.iloc[:, 0]

    proxy_ret = proxy_close.pct_change().fillna(0.0)

    # ── Try to get actual cash balance ────────────────────────
    cash_series: pd.Series | None = None

    for attr in (
        "cash", "cash_series", "cash_balance",
        "cash_values", "available_cash",
    ):
        val = getattr(bt_result, attr, None)
        if isinstance(val, pd.Series) and len(val) > 0:
            cash_series = val
            break

    # Try reconstructing: equity minus position values
    if cash_series is None:
        for attr in (
            "positions_value", "invested_value",
            "position_values", "gross_exposure",
        ):
            val = getattr(bt_result, attr, None)
            if isinstance(val, pd.Series) and len(val) > 0:
                cash_series = (equity_curve - val).clip(lower=0.0)
                break

    if cash_series is None:
        # Last resort: assume 20 % of equity is idle (conservative)
        logger.debug(
            "No cash-balance data in BacktestResult — "
            "using 20 %% fallback for cash-proxy adjustment"
        )
        cash_series = equity_curve * 0.20

    # ── Align indices ─────────────────────────────────────────
    common_idx = (
        equity_curve.index
        .intersection(proxy_ret.index)
        .intersection(cash_series.index)
    )
    if len(common_idx) < 2:
        logger.debug("Not enough overlap for cash-proxy adjustment")
        return equity_curve

    cash   = cash_series.reindex(common_idx).ffill().fillna(0.0)
    p_ret  = proxy_ret.reindex(common_idx).fillna(0.0)

    # ── Walk forward: compound cash PnL day-by-day ────────────
    adjusted = equity_curve.reindex(common_idx).copy()
    cum_cash_pnl = 0.0

    values = adjusted.values.copy()  # numpy for speed
    cash_vals = cash.values
    ret_vals  = p_ret.values

    for i in range(1, len(values)):
        # Cash at start-of-day earns the proxy's return
        cash_pnl = cash_vals[i - 1] * ret_vals[i]
        cum_cash_pnl += cash_pnl
        values[i] += cum_cash_pnl

    adjusted = pd.Series(values, index=common_idx, name=equity_curve.name)

    added_pct = (
        (adjusted.iloc[-1] / equity_curve.reindex(common_idx).iloc[-1])
        - 1.0
    ) * 100
    logger.info(
        f"Cash proxy adjustment: +{added_pct:.2f} %% total return "
        f"from idle cash in {strategy_cash_proxy_name(proxy_prices)}"
    )

    return adjusted


def strategy_cash_proxy_name(proxy_prices: pd.DataFrame) -> str:
    """Best-effort human-readable name for the proxy ticker."""
    if hasattr(proxy_prices, "name") and proxy_prices.name:
        return str(proxy_prices.name)
    if hasattr(proxy_prices, "attrs") and "ticker" in proxy_prices.attrs:
        return proxy_prices.attrs["ticker"]
    return "SHY"


# ═══════════════════════════════════════════════════════════════
#  CONFIG BUILDERS
# ═══════════════════════════════════════════════════════════════

def _build_backtest_config(
    strategy: StrategyConfig,
    capital: float,
) -> BacktestConfig:
    """Build ``BacktestConfig`` from strategy overrides."""
    sizing_kw    = strategy.sizing_config_overrides or {}
    bt_overrides = strategy.backtest_config_overrides or {}

    sizing = SizingConfig(**{
        k: v for k, v in sizing_kw.items()
        if k in SizingConfig.__dataclass_fields__
    }) if sizing_kw else SizingConfig()

    rebalance_kw = {
        k: v for k, v in bt_overrides.items()
        if k in RebalanceConfig.__dataclass_fields__
    }
    rebalance = RebalanceConfig(**rebalance_kw) if rebalance_kw else RebalanceConfig()

    # Build the BacktestConfig, passing through only the
    # fields that BacktestConfig actually accepts.
    bc_fields = set(BacktestConfig.__dataclass_fields__)
    bc_kwargs: dict[str, Any] = {
        "initial_capital": capital,
        "sizing": sizing,
        "rebalance": rebalance,
    }

    # Standard optional fields
    for key in ("execution_delay", "rebalance_holds"):
        if key in bt_overrides and key in bc_fields:
            bc_kwargs[key] = bt_overrides[key]

    # Pass through min_hold_days and cash_proxy if BacktestConfig
    # supports them (future-proof).
    if "min_hold_days" in bc_fields:
        bc_kwargs["min_hold_days"] = strategy.min_hold_days
    if "cash_proxy" in bc_fields:
        bc_kwargs["cash_proxy"] = strategy.cash_proxy

    return BacktestConfig(**bc_kwargs)


# ═══════════════════════════════════════════════════════════════
#  RETURN COMPUTATIONS
# ═══════════════════════════════════════════════════════════════

def _compute_annual_returns(equity: pd.Series) -> pd.Series:
    """Year-by-year returns from an equity curve."""
    if equity.empty:
        return pd.Series(dtype=float)

    yearly = equity.resample("YE").last()
    returns = yearly.pct_change().dropna()
    returns.index = returns.index.year
    returns.name = "annual_return"
    return returns


def _compute_monthly_returns(
    daily_returns: pd.Series,
) -> pd.DataFrame:
    """
    Monthly returns as a year × month pivot table.

    Returns a DataFrame with years as rows, months (1–12) as
    columns, and monthly percentage returns as values.
    """
    if daily_returns.empty:
        return pd.DataFrame()

    monthly = (1 + daily_returns).resample("ME").prod() - 1
    monthly_df = pd.DataFrame({
        "year":   monthly.index.year,
        "month":  monthly.index.month,
        "return": monthly.values,
    })

    pivot = monthly_df.pivot_table(
        values="return",
        index="year",
        columns="month",
        aggfunc="first",
    )
    pivot.columns = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][: len(pivot.columns)]

    return pivot

##########################################

"""
backtest/data_loader.py
-----------------------
Download, cache, and serve 20 years of OHLCV data for backtesting.

Primary source is yfinance (``period="max"``).  Data is cached as a
single parquet file at ``data/backtest/backtest_universe.parquet`` so
subsequent runs load in < 2 seconds.

The default backtest universe is a subset of the full CASH universe
consisting of tickers with 15–25 years of history.  The user can
override with any ticker list.

Integration
-----------
Returns data in the same ``{ticker: DataFrame}`` format that
``src/db/loader.py`` produces, so the pipeline accepts it seamlessly
via ``Orchestrator.load_data(preloaded=data)``.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import DATA_DIR, BENCHMARK_TICKER

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  PATHS
# ═══════════════════════════════════════════════════════════════

BACKTEST_DIR = DATA_DIR / "backtest"
CACHE_PATH = BACKTEST_DIR / "backtest_universe.parquet"

# ═══════════════════════════════════════════════════════════════
#  DEFAULT BACKTEST UNIVERSE
#  Tickers with 15–25 years of Yahoo Finance history.
#  Intentionally smaller than the full CASH universe so that
#  20-year backtests are meaningful (no survivorship bias from
#  tickers that didn't exist yet).
# ═══════════════════════════════════════════════════════════════

BACKTEST_CORE_UNIVERSE = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "MDY",
    # Sectors
    "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLY", "XLP", "XLU", "XLB",
    # International
    "EFA", "EEM", "EWJ", "EWZ",
    # Fixed income
    "TLT", "IEF", "SHY", "LQD", "HYG", "AGG", "TIP",
    # Commodities / alternatives
    "GLD", "SLV", "USO", "DBC", "VNQ",
    # Thematic (10+ years)
    "SOXX", "XBI", "IBB", "IGV",
    "HACK", "TAN", "ICLN", "URA",
    "IBIT",
    # Communication (newer but important)
    "XLC", "XLRE",
]

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def ensure_history(
    tickers: list[str] | None = None,
    force_refresh: bool = False,
    max_age_days: int = 7,
) -> dict[str, pd.DataFrame]:
    """
    Ensure 20-year OHLCV data is available.  Downloads from
    yfinance if the cache is missing or stale.

    Parameters
    ----------
    tickers : list[str] or None
        Symbols to download.  Defaults to ``BACKTEST_CORE_UNIVERSE``.
    force_refresh : bool
        If True, re-download even if cache exists.
    max_age_days : int
        Re-download if the cache is older than this many days.

    Returns
    -------
    dict[str, pd.DataFrame]
        ``{ticker: OHLCV DataFrame}`` with DatetimeIndex, lowercase
        columns, sorted ascending.  Ready to pass to
        ``Orchestrator.load_data(preloaded=...)``.
    """
    tickers = tickers or list(BACKTEST_CORE_UNIVERSE)

    # Ensure benchmark is included
    if BENCHMARK_TICKER not in tickers:
        tickers = [BENCHMARK_TICKER] + tickers

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)

    needs_download = (
        force_refresh
        or not CACHE_PATH.exists()
        or _cache_age_days() > max_age_days
    )

    if needs_download:
        logger.info(
            f"Downloading {len(tickers)} tickers from yfinance "
            f"(period=max) ..."
        )
        _download_and_cache(tickers)
    else:
        logger.info(
            f"Using cached data: {CACHE_PATH.name} "
            f"(age: {_cache_age_days():.0f} days)"
        )

    return load_cached_history(tickers)


def load_cached_history(
    tickers: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load previously cached backtest data from parquet.

    Parameters
    ----------
    tickers : list[str] or None
        Filter to these tickers.  None = return all cached.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    if not CACHE_PATH.exists():
        logger.warning(
            f"Cache not found: {CACHE_PATH}.  "
            f"Call ensure_history() first."
        )
        return {}

    raw = pd.read_parquet(CACHE_PATH)

    # Find symbol column
    sym_col = _find_symbol_col(raw)
    if sym_col is None:
        logger.error(
            f"No symbol column found in {CACHE_PATH}.  "
            f"Columns: {list(raw.columns)}"
        )
        return {}

    # Filter tickers
    if tickers is not None:
        upper = {t.upper() for t in tickers}
        raw = raw[raw[sym_col].str.upper().isin(upper)]

    # Split into per-ticker DataFrames
    result: dict[str, pd.DataFrame] = {}
    for ticker, group in raw.groupby(sym_col):
        df = _normalise(group.drop(columns=[sym_col]))
        if not df.empty and len(df) >= 60:
            result[str(ticker)] = df

    logger.info(
        f"Loaded {len(result)} tickers from cache "
        f"({sum(len(d) for d in result.values()):,} total bars)"
    )
    return result


def slice_period(
    data: dict[str, pd.DataFrame],
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Slice every DataFrame in the universe to a date range.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        ``{ticker: OHLCV}`` from ``ensure_history()`` or
        ``load_cached_history()``.
    start : str or Timestamp or None
        Inclusive start date.  None = earliest available.
    end : str or Timestamp or None
        Inclusive end date.  None = latest available.

    Returns
    -------
    dict[str, pd.DataFrame]
        Sliced data.  Tickers with < 60 bars after slicing
        are dropped.
    """
    result: dict[str, pd.DataFrame] = {}

    for ticker, df in data.items():
        sliced = df.loc[start:end] if start or end else df.copy()
        if len(sliced) >= 60:
            result[ticker] = sliced

    n_dropped = len(data) - len(result)
    if n_dropped > 0:
        logger.info(
            f"Period slice: {len(result)} tickers retained, "
            f"{n_dropped} dropped (< 60 bars)"
        )

    return result


def data_summary(data: dict[str, pd.DataFrame]) -> dict:
    """Quick summary of loaded backtest data."""
    if not data:
        return {"n_tickers": 0}

    all_starts = []
    all_ends = []
    total_bars = 0

    for ticker, df in data.items():
        all_starts.append(df.index[0])
        all_ends.append(df.index[-1])
        total_bars += len(df)

    return {
        "n_tickers": len(data),
        "total_bars": total_bars,
        "earliest_start": min(all_starts),
        "latest_end": max(all_ends),
        "median_bars": int(np.median([len(d) for d in data.values()])),
        "tickers": sorted(data.keys()),
    }


# ═══════════════════════════════════════════════════════════════
#  DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def _download_and_cache(tickers: list[str]) -> None:
    """Download max-period data from yfinance and save as parquet."""
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError(
            "yfinance is required for backtest data download.  "
            "pip install yfinance"
        )

    t0 = time.time()

    # Batch download — yfinance handles multi-ticker efficiently
    logger.info(f"yfinance batch download: {len(tickers)} tickers")

    raw = yf.download(
        tickers=tickers,
        period="max",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=True,
    )

    if raw.empty:
        logger.error("yfinance returned empty DataFrame")
        return

    # Reshape from MultiIndex columns to long format with symbol column
    records: list[pd.DataFrame] = []

    if len(tickers) == 1:
        sym = tickers[0]
        tmp = raw.copy().reset_index()
        tmp["symbol"] = sym
        records.append(tmp)
    else:
        for sym in tickers:
            try:
                tmp = raw[sym].copy()
                tmp = tmp.dropna(how="all")
                if tmp.empty:
                    logger.warning(f"  {sym}: no data")
                    continue
                tmp = tmp.reset_index()
                tmp["symbol"] = sym
                records.append(tmp)
            except KeyError:
                logger.warning(f"  {sym}: not in download result")
                continue

    if not records:
        logger.error("No data collected from yfinance")
        return

    combined = pd.concat(records, ignore_index=True)

    # Normalise column names to lowercase
    combined.columns = [str(c).lower().strip() for c in combined.columns]
    rename_map = {"adj close": "adj_close"}
    combined.rename(
        columns={k: v for k, v in rename_map.items() if k in combined.columns},
        inplace=True,
    )

    # Save
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(CACHE_PATH, index=False)

    elapsed = time.time() - t0
    size_mb = CACHE_PATH.stat().st_size / (1024 * 1024)
    n_syms = combined["symbol"].nunique()

    logger.info(
        f"Saved → {CACHE_PATH} "
        f"({size_mb:.1f} MB, {len(combined):,} rows, "
        f"{n_syms} symbols, {elapsed:.0f}s)"
    )


# ═══════════════════════════════════════════════════════════════
#  NORMALISATION
# ═══════════════════════════════════════════════════════════════

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise to the standard format expected by compute/:
    lowercase columns, DatetimeIndex, no NaN closes.
    """
    df = df.copy()

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    df.columns = [str(c).lower().strip() for c in df.columns]
    df.rename(columns={"adj close": "adj_close"}, inplace=True)

    # Set date index
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            return pd.DataFrame()

    df.index.name = "date"

    # Keep only OHLCV
    keep = [c for c in _REQUIRED_COLS if c in df.columns]
    if len(keep) < 5:
        return pd.DataFrame()
    df = df[keep]

    # Clean
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df["close"].notna() & (df["close"] > 0)]
    for col in _REQUIRED_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype(np.int64)

    return df


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _cache_age_days() -> float:
    """How old is the cache file in days."""
    if not CACHE_PATH.exists():
        return float("inf")
    mtime = CACHE_PATH.stat().st_mtime
    age = time.time() - mtime
    return age / 86400.0


def _find_symbol_col(df: pd.DataFrame) -> str | None:
    """Find the symbol/ticker column."""
    for candidate in ["symbol", "Symbol", "ticker", "Ticker", "SYMBOL"]:
        if candidate in df.columns:
            return candidate
    return None


######################################################


"""
backtest/strategies.py
----------------------
Predefined strategy parameter variants for comparison.

IMPORTANT: The baseline now uses tuned defaults that reduce
churning.  The original "academic" defaults that caused -5%
CAGR are preserved as ORIGINAL_DEFAULTS for reference.
"""

from __future__ import annotations

from backtest.engine import StrategyConfig
from common.universe import SECTORS, BROAD_MARKET, FIXED_INCOME, COMMODITIES


# ═══════════════════════════════════════════════════════════════
#  BASELINE — tuned defaults (reduced churn)
# ═══════════════════════════════════════════════════════════════

BASELINE = StrategyConfig(
    name="baseline",
    description="Tuned CASH defaults — wide hysteresis, low churn",
)


# ═══════════════════════════════════════════════════════════════
#  ORIGINAL DEFAULTS — what shipped originally (for reference)
# ═══════════════════════════════════════════════════════════════

ORIGINAL_DEFAULTS = StrategyConfig(
    name="original_defaults",
    description="Original parameters (high churn — for comparison only)",
    signal_params={
        "entry_score_min":     0.60,
        "exit_score_max":      0.40,
        "confirmation_streak": 3,
        "cooldown_days":       5,
        "max_position_pct":    0.08,
        "min_position_pct":    0.02,
        "base_position_pct":   0.05,
        "max_positions":       15,
    },
    portfolio_params={
        "max_positions":       15,
        "max_single_pct":      0.08,
        "min_single_pct":      0.02,
        "target_invested_pct": 0.95,
        "rebalance_threshold": 0.015,
        "incumbent_bonus":     0.02,
    },
)


# ═══════════════════════════════════════════════════════════════
#  MOMENTUM HEAVY — overweight momentum pillar
# ═══════════════════════════════════════════════════════════════

MOMENTUM_HEAVY = StrategyConfig(
    name="momentum_heavy",
    description="Overweight momentum pillar (40%), wider entry",
    scoring_weights={
        "pillar_rotation":       0.20,
        "pillar_momentum":       0.40,
        "pillar_volatility":     0.10,
        "pillar_microstructure": 0.20,
        "pillar_breadth":        0.10,
    },
    signal_params={
        "entry_score_min":     0.50,
        "exit_score_max":      0.30,
        "confirmation_streak": 2,
        "cooldown_days":       20,
    },
)


# ═══════════════════════════════════════════════════════════════
#  CONSERVATIVE — higher bar, defensive
# ═══════════════════════════════════════════════════════════════

CONSERVATIVE = StrategyConfig(
    name="conservative",
    description="High conviction only, strong hold bias",
    signal_params={
        "entry_score_min":     0.65,
        "exit_score_max":      0.40,
        "confirmation_streak": 4,
        "cooldown_days":       25,
        "max_position_pct":    0.12,
        "max_positions":       6,
    },
    portfolio_params={
        "max_positions":       6,
        "max_single_pct":      0.12,
        "target_invested_pct": 0.75,
        "incumbent_bonus":     0.08,
    },
    breadth_portfolio={
        "strong_exposure":     0.85,
        "neutral_exposure":    0.60,
        "weak_exposure":       0.25,
        "weak_block_new":      True,
        "weak_raise_entry":    0.10,
        "neutral_raise_entry": 0.05,
    },
)


# ═══════════════════════════════════════════════════════════════
#  BROAD DIVERSIFIED — more positions, equal weight
# ═══════════════════════════════════════════════════════════════

BROAD_DIVERSIFIED = StrategyConfig(
    name="broad_diversified",
    description="12 positions, equal weight, wide net, low turnover",
    signal_params={
        "entry_score_min":     0.48,
        "exit_score_max":      0.30,
        "cooldown_days":       20,
        "max_positions":       12,
        "max_position_pct":    0.12,
    },
    portfolio_params={
        "max_positions":       12,
        "max_single_pct":      0.12,
        "min_single_pct":      0.03,
        "max_sector_pct":      0.40,
        "target_invested_pct": 0.92,
        "incumbent_bonus":     0.06,
    },
    sizing_config_overrides={
        "method":           "equal_weight",
        "max_position_pct": 0.12,
        "min_position_pct": 0.03,
    },
)


# ═══════════════════════════════════════════════════════════════
#  CONCENTRATED — top 3 only
# ═══════════════════════════════════════════════════════════════

CONCENTRATED = StrategyConfig(
    name="concentrated",
    description="Top 3 high-conviction, strong hold bias",
    signal_params={
        "entry_score_min":     0.62,
        "exit_score_max":      0.38,
        "confirmation_streak": 3,
        "cooldown_days":       30,
        "max_positions":       3,
        "max_position_pct":    0.30,
    },
    portfolio_params={
        "max_positions":       3,
        "max_single_pct":      0.30,
        "min_single_pct":      0.10,
        "max_sector_pct":      0.50,
        "target_invested_pct": 0.85,
        "incumbent_bonus":     0.10,
    },
)


# ═══════════════════════════════════════════════════════════════
#  RISK PARITY — volatility-scaled
# ═══════════════════════════════════════════════════════════════

RISK_PARITY = StrategyConfig(
    name="risk_parity",
    description="Inverse-volatility sizing, moderate turnover",
    signal_params={
        "cooldown_days":       20,
    },
    portfolio_params={
        "max_positions":       8,
        "max_single_pct":      0.18,
        "target_invested_pct": 0.88,
        "incumbent_bonus":     0.06,
    },
    sizing_config_overrides={
        "method":           "risk_parity",
        "max_position_pct": 0.18,
        "min_position_pct": 0.04,
    },
)


# ═══════════════════════════════════════════════════════════════
#  ROTATION PURE — RS-dominant
# ═══════════════════════════════════════════════════════════════

ROTATION_PURE = StrategyConfig(
    name="rotation_pure",
    description="Rotation pillar dominant (45%), RS-driven",
    scoring_weights={
        "pillar_rotation":       0.45,
        "pillar_momentum":       0.20,
        "pillar_volatility":     0.10,
        "pillar_microstructure": 0.15,
        "pillar_breadth":        0.10,
    },
    scoring_params={
        "rs_zscore_w":      0.45,
        "rs_regime_w":      0.30,
        "rs_momentum_w":    0.15,
        "rs_vol_confirm_w": 0.10,
    },
    signal_params={
        "cooldown_days":    20,
    },
    portfolio_params={
        "incumbent_bonus":  0.06,
    },
)


# ═══════════════════════════════════════════════════════════════
#  SECTOR ROTATION — sector ETFs only
# ═══════════════════════════════════════════════════════════════

_SECTOR_UNIVERSE = list(SECTORS) + ["SPY"]

SECTOR_ROTATION = StrategyConfig(
    name="sector_rotation",
    description="Pure sector ETF rotation (11 sectors)",
    universe_filter=_SECTOR_UNIVERSE,
    signal_params={
        "entry_score_min":     0.52,
        "exit_score_max":      0.32,
        "cooldown_days":       20,
        "max_positions":       4,
    },
    portfolio_params={
        "max_positions":       4,
        "max_single_pct":      0.25,
        "max_sector_pct":      0.30,
        "target_invested_pct": 0.92,
        "incumbent_bonus":     0.08,
    },
)


# ═══════════════════════════════════════════════════════════════
#  ALL-WEATHER — cross-asset
# ═══════════════════════════════════════════════════════════════

_ALL_WEATHER_UNI = (
    list(BROAD_MARKET) + list(SECTORS)
    + list(FIXED_INCOME) + list(COMMODITIES)
    + ["SPY"]
)

ALL_WEATHER = StrategyConfig(
    name="all_weather",
    description="Cross-asset rotation: equities + bonds + commodities",
    universe_filter=list(set(_ALL_WEATHER_UNI)),
    signal_params={
        "entry_score_min":     0.50,
        "exit_score_max":      0.32,
        "cooldown_days":       20,
        "max_positions":       8,
    },
    portfolio_params={
        "max_positions":       8,
        "max_single_pct":      0.18,
        "max_sector_pct":      0.40,
        "target_invested_pct": 0.90,
        "incumbent_bonus":     0.06,
    },
)


# ═══════════════════════════════════════════════════════════════
#  MONTHLY REBALANCE — trade less often
# ═══════════════════════════════════════════════════════════════

MONTHLY_REBALANCE = StrategyConfig(
    name="monthly_rebalance",
    description="Rebalance only on large drift (simulates monthly)",
    signal_params={
        "entry_score_min":     0.52,
        "exit_score_max":      0.30,
        "cooldown_days":       25,
    },
    portfolio_params={
        "max_positions":       8,
        "max_single_pct":      0.15,
        "target_invested_pct": 0.90,
        "rebalance_threshold": 0.08,
        "incumbent_bonus":     0.08,
    },
    backtest_config_overrides={
        "rebalance_holds": False,
        "drift_threshold": 0.20,
        "min_trade_pct":   0.05,
    },
)


# ═══════════════════════════════════════════════════════════════
#  REGISTRY
# ═══════════════════════════════════════════════════════════════

ALL_STRATEGIES: dict[str, StrategyConfig] = {
    "baseline":           BASELINE,
    "original_defaults":  ORIGINAL_DEFAULTS,
    "momentum_heavy":     MOMENTUM_HEAVY,
    "conservative":       CONSERVATIVE,
    "broad_diversified":  BROAD_DIVERSIFIED,
    "concentrated":       CONCENTRATED,
    "risk_parity":        RISK_PARITY,
    "rotation_pure":      ROTATION_PURE,
    "sector_rotation":    SECTOR_ROTATION,
    "all_weather":        ALL_WEATHER,
    "monthly_rebalance":  MONTHLY_REBALANCE,
}


def get_strategy(name: str) -> StrategyConfig:
    """Look up a strategy by name.  Raises KeyError if not found."""
    if name not in ALL_STRATEGIES:
        available = ", ".join(ALL_STRATEGIES.keys())
        raise KeyError(
            f"Unknown strategy '{name}'.  Available: {available}"
        )
    return ALL_STRATEGIES[name]


def list_strategies() -> list[str]:
    """Return available strategy names."""
    return list(ALL_STRATEGIES.keys())


##################################

"""
backtest/comparison.py
----------------------
Run multiple strategy variants over the same period and produce
a side-by-side comparison ranked by CAGR (or any metric).

Usage
-----
    from backtest.comparison import compare_strategies
    from backtest.data_loader import ensure_history

    data = ensure_history()
    results = compare_strategies(data, start="2010-01-01")
    print(results["report"])
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtest.engine import BacktestRun, StrategyConfig, run_backtest_period
from backtest.strategies import ALL_STRATEGIES, BASELINE
from backtest.metrics import metrics_report

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  COMPARE ALL STRATEGIES
# ═══════════════════════════════════════════════════════════════

def compare_strategies(
    data: dict[str, pd.DataFrame],
    *,
    strategies: list[StrategyConfig] | None = None,
    start: str | None = None,
    end: str | None = None,
    capital: float = 100_000.0,
    rank_by: str = "cagr",
) -> dict[str, Any]:
    """
    Run every strategy variant over the same period and return
    a comparison.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        From ``ensure_history()``.
    strategies : list[StrategyConfig] or None
        Strategies to test.  None = all from ``ALL_STRATEGIES``.
    start, end : str or None
        Date range.
    capital : float
        Initial capital for each run.
    rank_by : str
        Metric to rank by.  Default ``"cagr"``.

    Returns
    -------
    dict with keys:
        runs          list[BacktestRun]  — individual results
        table         pd.DataFrame       — comparison table
        report        str                — formatted text report
        best          BacktestRun        — best by rank_by metric
        worst         BacktestRun        — worst by rank_by metric
    """
    if strategies is None:
        strategies = list(ALL_STRATEGIES.values())

    logger.info(
        f"Comparing {len(strategies)} strategies "
        f"({start or 'earliest'} → {end or 'latest'})"
    )

    runs: list[BacktestRun] = []

    for i, strat in enumerate(strategies, 1):
        logger.info(
            f"[{i}/{len(strategies)}] Running: {strat.name}"
        )
        run = run_backtest_period(
            data,
            start=start,
            end=end,
            strategy=strat,
            capital=capital,
        )
        runs.append(run)

    # ── Build comparison table ────────────────────────────────
    table = _build_comparison_table(runs, rank_by=rank_by)

    # ── Text report ───────────────────────────────────────────
    report = _comparison_report(runs, table, rank_by)

    # ── Best / worst ──────────────────────────────────────────
    valid_runs = [r for r in runs if r.ok]
    best = max(valid_runs, key=lambda r: r.metrics.get(rank_by, -999)) if valid_runs else None
    worst = min(valid_runs, key=lambda r: r.metrics.get(rank_by, 999)) if valid_runs else None

    return {
        "runs": runs,
        "table": table,
        "report": report,
        "best": best,
        "worst": worst,
    }


# ═══════════════════════════════════════════════════════════════
#  COMPARISON TABLE
# ═══════════════════════════════════════════════════════════════

def _build_comparison_table(
    runs: list[BacktestRun],
    rank_by: str = "cagr",
) -> pd.DataFrame:
    """Build a DataFrame comparing all strategy runs."""
    rows = []
    for run in runs:
        m = run.metrics
        rows.append({
            "strategy":       run.strategy.name,
            "cagr":           m.get("cagr", None),
            "total_return":   m.get("total_return", None),
            "sharpe":         m.get("sharpe_ratio", None),
            "sortino":        m.get("sortino_ratio", None),
            "calmar":         m.get("calmar_ratio", None),
            "max_drawdown":   m.get("max_drawdown", None),
            "annual_vol":     m.get("annual_volatility", None),
            "win_rate":       m.get("win_rate", None),
            "total_trades":   m.get("total_trades", None),
            "profit_factor":  m.get("profit_factor", None),
            "excess_cagr":    m.get("excess_cagr", None),
            "info_ratio":     m.get("information_ratio", None),
            "final_capital":  m.get("final_capital", None),
            "best_year":      m.get("best_year", None),
            "worst_year":     m.get("worst_year", None),
            "elapsed_s":      run.elapsed_seconds,
            "error":          run.error,
        })

    df = pd.DataFrame(rows)

    # Sort by rank_by metric (descending for returns/ratios)
    if rank_by in df.columns and df[rank_by].notna().any():
        ascending = rank_by in ("max_drawdown", "annual_vol")
        df = df.sort_values(rank_by, ascending=ascending, na_position="last")

    df = df.reset_index(drop=True)
    df.index = df.index + 1  # 1-based ranking
    df.index.name = "rank"

    return df


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def _comparison_report(
    runs: list[BacktestRun],
    table: pd.DataFrame,
    rank_by: str,
) -> str:
    """Format the comparison as a text report."""
    ln: list[str] = []
    div = "=" * 90
    sub = "-" * 90

    # Header
    valid = [r for r in runs if r.ok]
    if not valid:
        return "No successful backtest runs to compare."

    first = valid[0]
    ln.append(div)
    ln.append("  STRATEGY COMPARISON")
    ln.append(div)
    ln.append(
        f"  Period:     {first.start_date.date()} → "
        f"{first.end_date.date()}"
    )
    ln.append(
        f"  Capital:    ${first.metrics.get('initial_capital', 0):,.0f}"
    )
    ln.append(
        f"  Strategies: {len(runs)} tested, "
        f"{len(valid)} successful"
    )
    ln.append(f"  Ranked by:  {rank_by}")

    # Comparison table
    ln.append("")
    ln.append(sub)
    ln.append(
        f"  {'#':>2}  {'Strategy':<24s} {'CAGR':>8} {'Sharpe':>7} "
        f"{'MaxDD':>8} {'Win%':>6} {'Trades':>7} "
        f"{'Final$':>12} {'ExcessCAGR':>10}"
    )
    ln.append(sub)

    for idx, row in table.iterrows():
        if row.get("error"):
            ln.append(
                f"  {idx:>2}  {row['strategy']:<24s}  "
                f"ERROR: {row['error']}"
            )
            continue

        cagr_s = f"{row['cagr']:>+7.2%}" if pd.notna(row.get("cagr")) else "    N/A"
        sharpe_s = f"{row['sharpe']:>6.2f}" if pd.notna(row.get("sharpe")) else "   N/A"
        dd_s = f"{row['max_drawdown']:>7.2%}" if pd.notna(row.get("max_drawdown")) else "    N/A"
        wr_s = f"{row['win_rate']:>5.1%}" if pd.notna(row.get("win_rate")) else "  N/A"
        trades_s = f"{int(row['total_trades']):>6d}" if pd.notna(row.get("total_trades")) else "   N/A"
        final_s = f"${row['final_capital']:>10,.0f}" if pd.notna(row.get("final_capital")) else "       N/A"
        excess_s = f"{row['excess_cagr']:>+9.2%}" if pd.notna(row.get("excess_cagr")) else "      N/A"

        ln.append(
            f"  {idx:>2}  {row['strategy']:<24s} {cagr_s} {sharpe_s} "
            f"{dd_s} {wr_s} {trades_s} {final_s} {excess_s}"
        )

    # Best/worst summary
    ln.append("")
    ln.append(sub)
    ln.append("  HIGHLIGHTS")
    ln.append(sub)

    if not table.empty and table["cagr"].notna().any():
        best_idx = table["cagr"].idxmax()
        worst_idx = table["cagr"].idxmin()
        best_row = table.loc[best_idx]
        worst_row = table.loc[worst_idx]

        ln.append(
            f"  Best CAGR:    {best_row['strategy']:<20s} "
            f"{best_row['cagr']:>+7.2%}"
        )
        ln.append(
            f"  Worst CAGR:   {worst_row['strategy']:<20s} "
            f"{worst_row['cagr']:>+7.2%}"
        )

    if not table.empty and table["sharpe"].notna().any():
        best_sh = table.loc[table["sharpe"].idxmax()]
        ln.append(
            f"  Best Sharpe:  {best_sh['strategy']:<20s} "
            f"{best_sh['sharpe']:>6.2f}"
        )

    if not table.empty and table["max_drawdown"].notna().any():
        best_dd = table.loc[table["max_drawdown"].idxmax()]
        ln.append(
            f"  Smallest DD:  {best_dd['strategy']:<20s} "
            f"{best_dd['max_drawdown']:>7.2%}"
        )

    ln.append("")
    ln.append(div)
    return "\n".join(ln)


###################

"""
backtest/runner.py
------------------
CLI entry point for backtesting.

Usage:
    python -m backtest.runner                                 # 20Y default
    python -m backtest.runner --start 2010 --end 2020         # custom period
    python -m backtest.runner --strategy momentum_heavy       # single variant
    python -m backtest.runner --compare                       # all strategies
    python -m backtest.runner --compare --rank-by sharpe      # rank by Sharpe
    python -m backtest.runner --list                          # list strategies
    python -m backtest.runner --refresh                       # re-download data
    python -m backtest.runner --capital 500000                # custom capital
    python -m backtest.runner --output backtest_results/      # save reports
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from backtest.data_loader import (
    ensure_history,
    data_summary,
    BACKTEST_CORE_UNIVERSE,
)
from backtest.engine import run_backtest_period, StrategyConfig
from backtest.strategies import (
    ALL_STRATEGIES,
    get_strategy,
    list_strategies,
)
from backtest.comparison import compare_strategies
from backtest.metrics import metrics_report

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="backtest",
        description="CASH — Historical Backtesting Harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backtest.runner                          # 20Y backtest, baseline
  python -m backtest.runner --start 2015 --end 2024  # custom period
  python -m backtest.runner --compare                # compare all strategies
  python -m backtest.runner --strategy conservative  # single variant
  python -m backtest.runner --list                   # show available strategies
        """,
    )

    p.add_argument(
        "--start", type=str, default=None,
        help="Backtest start date (YYYY or YYYY-MM-DD).  Default: earliest available",
    )
    p.add_argument(
        "--end", type=str, default=None,
        help="Backtest end date.  Default: latest available",
    )
    p.add_argument(
        "--strategy", "-s", type=str, default="baseline",
        help="Strategy name to run (default: baseline)",
    )
    p.add_argument(
        "--compare", action="store_true",
        help="Run all strategies and compare side-by-side",
    )
    p.add_argument(
        "--rank-by", type=str, default="cagr",
        choices=["cagr", "sharpe", "sortino", "calmar", "max_drawdown"],
        help="Metric to rank strategies by (default: cagr)",
    )
    p.add_argument(
        "--capital", type=float, default=100_000,
        help="Initial capital (default: 100,000)",
    )
    p.add_argument(
        "--output", "-o", type=str, default=None,
        help="Directory to save backtest reports",
    )
    p.add_argument(
        "--refresh", action="store_true",
        help="Force re-download of historical data",
    )
    p.add_argument(
        "--list", action="store_true", dest="list_strats",
        help="List available strategies and exit",
    )
    p.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override the backtest universe with specific tickers",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Debug logging",
    )

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── Logging ───────────────────────────────────────────────
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    # ── List strategies ───────────────────────────────────────
    if args.list_strats:
        print("\nAvailable strategies:")
        print("-" * 60)
        for name, strat in ALL_STRATEGIES.items():
            print(f"  {name:<24s} {strat.description}")
        print()
        return

    # ── Load data ─────────────────────────────────────────────
    t0 = time.time()
    tickers = (
        [t.upper() for t in args.tickers]
        if args.tickers
        else None
    )

    print("\n" + "=" * 70)
    print("  CASH — BACKTESTING HARNESS")
    print("=" * 70)

    data = ensure_history(
        tickers=tickers,
        force_refresh=args.refresh,
    )

    if not data:
        print("ERROR: No data loaded.  Check your internet connection.")
        sys.exit(1)

    summary = data_summary(data)
    print(f"\n  Data loaded: {summary['n_tickers']} tickers")
    print(f"  Range:       {summary['earliest_start'].date()} → "
          f"{summary['latest_end'].date()}")
    print(f"  Total bars:  {summary['total_bars']:,}")

    # ── Normalise date args ───────────────────────────────────
    start = _normalise_date(args.start)
    end = _normalise_date(args.end)

    # ── Compare mode ──────────────────────────────────────────
    if args.compare:
        print(f"\n  Running comparison of {len(ALL_STRATEGIES)} strategies...")
        print()

        result = compare_strategies(
            data,
            start=start,
            end=end,
            capital=args.capital,
            rank_by=args.rank_by,
        )

        print(result["report"])

        # Detailed report for best strategy
        if result["best"]:
            print("\n" + metrics_report(result["best"]))

        # Save if output dir specified
        if args.output:
            _save_comparison(result, args.output)

        elapsed = time.time() - t0
        print(f"\n  Total time: {elapsed:.0f}s")
        return

    # ── Single strategy mode ──────────────────────────────────
    try:
        strategy = get_strategy(args.strategy)
    except KeyError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"\n  Strategy:    {strategy.name}")
    print(f"  Description: {strategy.description}")
    print(f"  Period:      {start or 'earliest'} → {end or 'latest'}")
    print(f"  Capital:     ${args.capital:,.0f}")
    print()

    run = run_backtest_period(
        data,
        start=start,
        end=end,
        strategy=strategy,
        capital=args.capital,
    )

    if run.ok:
        print(metrics_report(run))
    else:
        print(f"\n  ERROR: {run.error}")

    # Save if output dir specified
    if args.output and run.ok:
        _save_single(run, args.output)

    elapsed = time.time() - t0
    print(f"\n  Total time: {elapsed:.0f}s")


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _normalise_date(date_str: str | None) -> str | None:
    """Convert 'YYYY' to 'YYYY-01-01' for convenience."""
    if date_str is None:
        return None
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return date_str


def _save_comparison(result: dict, output_dir: str) -> None:
    """Save comparison report and CSV to output directory."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    # ── Text report ───────────────────────────────────────
    report_path = os.path.join(output_dir, "comparison_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(result["report"])

    # ── CSV summary ───────────────────────────────────────
    if "summary_df" in result:
        csv_path = os.path.join(output_dir, "comparison_summary.csv")
        result["summary_df"].to_csv(csv_path, index=False, encoding="utf-8")

    # ── Per-strategy equity curves ────────────────────────
    if "equity_curves" in result:
        eq_path = os.path.join(output_dir, "equity_curves.csv")
        result["equity_curves"].to_csv(eq_path, encoding="utf-8")

    print(f"\n  Results saved to: {output_dir}/")


def _save_single(run: "BacktestRun", output_dir: str) -> None:
    """Save a single backtest run."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_path = out / f"backtest_{run.strategy.name}_{ts}.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(metrics_report(run))
    print(f"  Report saved → {report_path}")

    # Save equity curve as CSV
    if run.backtest_result and not run.backtest_result.equity_curve.empty:
        eq_path = out / f"equity_{run.strategy.name}_{ts}.csv"
        eq_df = pd.DataFrame({
            "equity": run.backtest_result.equity_curve,
        })
        if not run.benchmark_equity.empty:
            eq_df["benchmark"] = run.benchmark_equity
        eq_df.to_csv(eq_path)
        print(f"  Equity saved → {eq_path}")


# Need pandas import for _save_single
import pandas as pd

if __name__ == "__main__":
    main()

#####################################################