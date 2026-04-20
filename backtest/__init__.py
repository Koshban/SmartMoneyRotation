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