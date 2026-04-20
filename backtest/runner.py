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