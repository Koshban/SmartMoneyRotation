"""
backtest/runner.py
------------------
CLI entry point for backtesting.

Enhanced with multi-market support and equity curve export.

Usage:
    python -m backtest.runner                                 # 20Y US default
    python -m backtest.runner --market HK --start 2015        # HK from 2015
    python -m backtest.runner --market IN --compare           # India comparison
    python -m backtest.runner --compare --rank-by sharpe      # rank by Sharpe
    python -m backtest.runner --strategy convergence_strong   # convergence
    python -m backtest.runner --list                          # list strategies
    python -m backtest.runner --list --market HK              # HK strategies
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from backtest.data_loader import ensure_history, data_summary
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
  python -m backtest.runner                              # 20Y US backtest
  python -m backtest.runner --market HK --start 2018     # HK from 2018
  python -m backtest.runner --compare                    # compare all US strategies
  python -m backtest.runner --compare --market HK        # compare HK strategies
  python -m backtest.runner --strategy convergence_strong # convergence-aware
  python -m backtest.runner --list --market IN            # list India strategies
        """,
    )

    p.add_argument("--start", type=str, default=None)
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--strategy", "-s", type=str, default="baseline")
    p.add_argument("--market", "-m", type=str, default="US",
                   choices=["US", "HK", "IN"],
                   help="Market to backtest (default: US)")
    p.add_argument("--compare", action="store_true")
    p.add_argument("--rank-by", type=str, default="cagr",
                   choices=["cagr", "sharpe", "sortino", "calmar", "max_drawdown"])
    p.add_argument("--capital", type=float, default=100_000)
    p.add_argument("--output", "-o", type=str, default=None)
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--list", action="store_true", dest="list_strats")
    p.add_argument("--tickers", nargs="+", default=None)
    p.add_argument("--holdings", type=str, default="",
                   help="Comma-separated current holdings for rotation evaluation")
    p.add_argument("--verbose", "-v", action="store_true")

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

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

    market = args.market.upper()

    # ── List strategies ───────────────────────────────────────
    if args.list_strats:
        strats = list_strategies(market)
        print(f"\nAvailable strategies for {market}:")
        print("-" * 60)
        for name in strats:
            strat = ALL_STRATEGIES[name]
            print(f"  {name:<24s} {strat.description}")
        print()
        return

    # ── Load data ─────────────────────────────────────────────
    t0 = time.time()
    tickers = [t.upper() for t in args.tickers] if args.tickers else None

    print("\n" + "=" * 70)
    print(f"  CASH — BACKTESTING HARNESS [{market}]")
    print("=" * 70)

    data = ensure_history(
        tickers=tickers,
        market=market,
        force_refresh=args.refresh,
    )

    if not data:
        print("ERROR: No data loaded.")
        sys.exit(1)

    summary = data_summary(data)
    print(f"\n  Data loaded: {summary['n_tickers']} tickers")
    print(f"  Range:       {summary['earliest_start'].date()} → "
          f"{summary['latest_end'].date()}")
    print(f"  Total bars:  {summary['total_bars']:,}")

    start = _normalise_date(args.start)
    end = _normalise_date(args.end)

    holdings = (
        [t.strip().upper() for t in args.holdings.split(",") if t.strip()]
        if args.holdings else None
    )

    # ── Compare mode ──────────────────────────────────────────
    if args.compare:
        market_strats = [
            v for v in ALL_STRATEGIES.values()
            if v.market == market
        ]
        print(f"\n  Running comparison of {len(market_strats)} "
              f"{market} strategies...")

        result = compare_strategies(
            data, strategies=market_strats, market=market,
            start=start, end=end, capital=args.capital,
            rank_by=args.rank_by, current_holdings=holdings,
        )

        print(result["report"])

        if result["best"]:
            print("\n" + metrics_report(result["best"]))

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

    if strategy.market != market:
        print(f"WARNING: Strategy '{strategy.name}' is for {strategy.market}, "
              f"but --market is {market}. Using strategy's market.")
        market = strategy.market

    print(f"\n  Strategy:    {strategy.name}")
    print(f"  Description: {strategy.description}")
    print(f"  Market:      {strategy.market}")
    print(f"  Period:      {start or 'earliest'} → {end or 'latest'}")
    print(f"  Capital:     ${args.capital:,.0f}")
    print()

    run = run_backtest_period(
        data, start=start, end=end,
        strategy=strategy, capital=args.capital,
        current_holdings=holdings,
    )

    if run.ok:
        print(metrics_report(run))
    else:
        print(f"\n  ERROR: {run.error}")

    if args.output and run.ok:
        _save_single(run, args.output)

    elapsed = time.time() - t0
    print(f"\n  Total time: {elapsed:.0f}s")


def _normalise_date(date_str: str | None) -> str | None:
    if date_str is None:
        return None
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return date_str


def _save_comparison(result: dict, output_dir: str) -> None:
    import os
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, "comparison_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(result["report"])

    if isinstance(result.get("table"), pd.DataFrame):
        csv_path = os.path.join(output_dir, "comparison_summary.csv")
        result["table"].to_csv(csv_path, index=True, encoding="utf-8")

    eq_curves = result.get("equity_curves")
    if isinstance(eq_curves, pd.DataFrame) and not eq_curves.empty:
        eq_path = os.path.join(output_dir, "equity_curves.csv")
        eq_curves.to_csv(eq_path, encoding="utf-8")

    print(f"\n  Results saved to: {output_dir}/")


def _save_single(run: BacktestRun, output_dir: str) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_path = out / f"backtest_{run.strategy.name}_{ts}.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(metrics_report(run))
    print(f"  Report saved → {report_path}")

    if run.backtest_result and not run.backtest_result.equity_curve.empty:
        eq_path = out / f"equity_{run.strategy.name}_{ts}.csv"
        eq_df = pd.DataFrame({"equity": run.backtest_result.equity_curve})
        if not run.benchmark_equity.empty:
            eq_df["benchmark"] = run.benchmark_equity
        eq_df.to_csv(eq_path)
        print(f"  Equity saved → {eq_path}")

    if not run.monthly_returns.empty:
        monthly_path = out / f"monthly_{run.strategy.name}_{ts}.csv"
        run.monthly_returns.to_csv(monthly_path)
        print(f"  Monthly returns saved → {monthly_path}")


if __name__ == "__main__":
    main()