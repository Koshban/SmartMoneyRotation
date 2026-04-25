"""
backtest/__init__.py
=========
Historical backtesting harness for the CASH Smart Money Rotation system.

Loads 20 years of OHLCV, runs the full pipeline over any date range,
computes performance metrics (including CAGR), and compares strategy
variants side-by-side.

Supports multi-market backtesting (US, HK, India) and convergence-
aware signal generation.

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
    python -m backtest.runner --market HK              # Hong Kong
    python -m backtest.runner --market IN              # India
"""

from backtest.phase1.engine import run_backtest_period, BacktestRun, StrategyConfig
from backtest.phase1.metrics import (
    compute_cagr,
    compute_full_metrics,
    cagr_from_equity,
    rolling_cagr,
    compute_monthly_returns_heatmap,
    compute_regime_metrics,
)
from backtest.phase1.comparison import compare_strategies
from backtest.phase1.data_loader import ensure_history, load_cached_history

__all__ = [
    "run_backtest_period",
    "BacktestRun",
    "StrategyConfig",
    "compute_cagr",
    "compute_full_metrics",
    "cagr_from_equity",
    "rolling_cagr",
    "compute_monthly_returns_heatmap",
    "compute_regime_metrics",
    "compare_strategies",
    "ensure_history",
    "load_cached_history",
]