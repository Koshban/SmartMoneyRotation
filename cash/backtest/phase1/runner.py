"""
backtest/runner.py
------------------
CLI entry point for backtesting.

The --universe flag is the primary control:
  us / hk / in       → full universe from common.universe
  us_core / hk_core / …  → hardcoded ETF/stock sets

Market, benchmark, and strategy filtering are all derived from it.

Usage:
    python -m backtest.runner --universe us                          # full US
    python -m backtest.runner --universe hk                          # full HK
    python -m backtest.runner --universe in                       # full India
    python -m backtest.runner --universe us_core                     # 41 ETFs
    python -m backtest.runner --show-universe --universe hk          # inspect HK
    python -m backtest.runner --show-universe --universe in       # inspect India
    python -m backtest.runner --compare --universe us                # compare US
    python -m backtest.runner --compare --universe hk                # compare HK
    python -m backtest.runner --strategy regime_adaptive --universe us
    python -m backtest.runner --list --universe in                # India strats
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from cash.backtest.phase1.data_loader import (
    ensure_history,
    data_summary,
    get_universe_tickers,
    list_universe_tickers,
    build_full_universe,
    resolve_universe,
    MARKET_BENCHMARKS,
    VALID_UNIVERSES,
    _MARKET_CORE,
)
from cash.backtest.phase1.engine import run_backtest_period, StrategyConfig
from cash.backtest.phase1.strategies import (
    ALL_STRATEGIES,
    get_strategy,
    list_strategies,
)
from cash.backtest.phase1.comparison import compare_strategies
from cash.backtest.phase1.metrics import metrics_report

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    valid_str = ", ".join(VALID_UNIVERSES)

    p = argparse.ArgumentParser(
        prog="backtest",
        description="CASH — Historical Backtesting Harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Universe choices: {valid_str}

Examples:
  python -m backtest.runner --universe us                            # full US
  python -m backtest.runner --universe hk --start 2020               # full HK
  python -m backtest.runner --universe in --start 2022            # full India
  python -m backtest.runner --universe us_core                       # 41 hardcoded ETFs
  python -m backtest.runner --show-universe --universe hk            # inspect HK
  python -m backtest.runner --show-universe --universe in         # inspect India
  python -m backtest.runner --compare --universe us                  # compare US strats
  python -m backtest.runner --compare --universe hk                  # compare HK strats
  python -m backtest.runner --strategy regime_adaptive --universe us
  python -m backtest.runner --list --universe in                  # India strategies
  python -m backtest.runner --tickers AAPL MSFT GOOG --start 2020   # custom tickers
        """,
    )

    p.add_argument(
        "--universe", "-u", type=str, default="us_core",
        help=f"Universe: {valid_str} (default: us_core)",
    )
    p.add_argument(
        "--start", type=str, default=None,
        help="Backtest start date (YYYY or YYYY-MM-DD)",
    )
    p.add_argument(
        "--end", type=str, default=None,
        help="Backtest end date",
    )
    p.add_argument(
        "--strategy", "-s", type=str, default="baseline",
        help="Strategy name (default: baseline)",
    )
    p.add_argument(
        "--show-universe", action="store_true",
        help="Print the ticker list for the selected universe and exit",
    )
    p.add_argument(
        "--compare", action="store_true",
        help="Run all strategies for the market and compare",
    )
    p.add_argument(
        "--rank-by", type=str, default="cagr",
        choices=["cagr", "sharpe", "sortino", "calmar", "max_drawdown"],
        help="Metric to rank by in compare mode (default: cagr)",
    )
    p.add_argument(
        "--capital", type=float, default=100_000,
        help="Initial capital (default: 100,000)",
    )
    p.add_argument(
        "--output", "-o", type=str, default=None,
        help="Directory to save reports",
    )
    p.add_argument(
        "--refresh", action="store_true",
        help="Force re-download of historical data",
    )
    p.add_argument(
        "--list", action="store_true", dest="list_strats",
        help="List available strategies for the market",
    )
    p.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override universe with specific tickers (market auto-detected)",
    )
    p.add_argument(
        "--holdings", type=str, default="",
        help="Comma-separated current holdings for rotation",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Debug logging",
    )

    # Hidden backward-compat alias
    p.add_argument(
        "--market", "-m", type=str, default=None,
        help=argparse.SUPPRESS,
    )

    return p


def _resolve_market_and_scope(args) -> tuple[str, str]:
    """
    Derive (market, scope) from CLI args.

    Priority:
      1. --tickers  → custom scope, market auto-detected from suffixes
      2. --universe → resolved via UNIVERSE_MAP
      3. --market   → backward compat fallback (maps to core scope)
    """
    if args.tickers:
        # Auto-detect market from first non-US-looking ticker
        from cash.backtest.phase1.data_loader import _ticker_market
        tickers = [t.upper() for t in args.tickers]
        markets = {_ticker_market(t) for t in tickers}
        markets.discard("US")
        if "HK" in markets:
            return "HK", "custom"
        if "IN" in markets:
            return "IN", "custom"
        return "US", "custom"

    # --universe flag (primary)
    universe_str = args.universe.lower().strip()

    # Handle backward-compat --market flag
    if args.market and universe_str in ("core", "full", "us_core"):
        market_map = {"US": "us", "HK": "hk", "IN": "in"}
        base = market_map.get(args.market.upper(), "us")
        if universe_str == "full":
            universe_str = base
        else:
            universe_str = f"{base}_core"

    try:
        return resolve_universe(universe_str)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)


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

    market, scope = _resolve_market_and_scope(args)
    benchmark = MARKET_BENCHMARKS.get(market, "SPY")

    # ── Show universe ─────────────────────────────────────────
    if args.show_universe:
        uni_name = args.universe.lower().strip()
        # Backward-compat: if they used --market
        if args.market and uni_name in ("core", "full", "us_core"):
            market_map = {"US": "us", "HK": "hk", "IN": "in"}
            base = market_map.get(args.market.upper(), "us")
            uni_name = base if uni_name == "full" else f"{base}_core"

        list_universe_tickers(universe=uni_name)
        return

    # ── List strategies ───────────────────────────────────────
    if args.list_strats:
        strats = list_strategies(market)
        print(f"\nAvailable strategies for {market} "
              f"(benchmark: {benchmark}):")
        print("-" * 60)
        for name in strats:
            strat = ALL_STRATEGIES[name]
            print(f"  {name:<24s} {strat.description}")
        print()
        return

    # ── Resolve tickers ───────────────────────────────────────
    custom_tickers = (
        [t.upper() for t in args.tickers] if args.tickers else None
    )

    # Build description for header
    if custom_tickers:
        universe_desc = f"custom ({len(custom_tickers)} tickers)"
    elif scope == "full":
        full_list = get_universe_tickers(market=market, scope="full")
        core_count = len(_MARKET_CORE.get(market, []))
        universe_desc = (
            f"{market} full ({len(full_list)} tickers, "
            f"{core_count} core + "
            f"{len(full_list) - core_count} from universe.py)"
        )
    else:
        core_list = get_universe_tickers(market=market, scope="core")
        universe_desc = f"{market} core ({len(core_list)} tickers)"

    # ── Load data ─────────────────────────────────────────────
    t0 = time.time()

    print("\n" + "=" * 70)
    print(f"  CASH — BACKTESTING HARNESS [{market}]")
    print("=" * 70)

    data = ensure_history(
        tickers=custom_tickers,
        market=market,
        scope=scope,
        force_refresh=args.refresh,
    )

    if not data:
        print("ERROR: No data loaded.")
        sys.exit(1)

    summary = data_summary(data)
    print(f"\n  Universe:    {universe_desc}")
    print(f"  Benchmark:   {benchmark}")
    print(f"  Data loaded: {summary['n_tickers']} tickers")
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
        print(
            f"\n  Comparing {len(market_strats)} {market} strategies "
            f"on {universe_desc}..."
        )

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
    strategy_name = argsrefactor.strategy

    # Auto-select default strategy for non-US markets
    if market != "US" and strategy_name == "baseline":
        market_defaults = {
            "HK": "hk_baseline",
            "IN": "in_baseline",
        }
        if market in market_defaults:
            strategy_name = market_defaults[market]
            logger.info(
                f"Auto-selected '{strategy_name}' for {market} market"
            )

    try:
        strategy = get_strategy(strategy_name)
    except KeyError as e:
        print(f"ERROR: {e}")
        available = list_strategies(market)
        if available:
            print(f"\nAvailable {market} strategies: "
                  f"{', '.join(available)}")
        sys.exit(1)

    # Warn if strategy market doesn't match universe market
    if strategy.market != market:
        print(
            f"  WARNING: Strategy '{strategy.name}' is for "
            f"{strategy.market}, but universe is {market}."
        )
        response = input("  Continue anyway? [y/N] ").strip().lower()
        if response != "y":
            sys.exit(0)

    print(f"\n  Strategy:    {strategy.name}")
    print(f"  Description: {strategy.description}")
    print(f"  Market:      {market}")
    print(f"  Benchmark:   {benchmark}")
    print(f"  Period:      {start or 'earliest'} → {end or 'latest'}")
    print(f"  Capital:     ${args.capital:,.0f}")
    print()

    run = run_backtest_period(
        data, start=start, end=end,
        strategy=strategy, capital=args.capital,
        benchmark=benchmark,
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


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

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


def _save_single(run, output_dir: str) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_path = out / f"backtest_{runrefactor.strategy.name}_{ts}.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(metrics_report(run))
    print(f"  Report saved → {report_path}")

    if run.backtest_result and not run.backtest_result.equity_curve.empty:
        eq_path = out / f"equity_{runrefactor.strategy.name}_{ts}.csv"
        eq_df = pd.DataFrame({"equity": run.backtest_result.equity_curve})
        if not run.benchmark_equity.empty:
            eq_df["benchmark"] = run.benchmark_equity
        eq_df.to_csv(eq_path)
        print(f"  Equity saved → {eq_path}")

    if not run.monthly_returns.empty:
        monthly_path = out / f"monthly_{runrefactor.strategy.name}_{ts}.csv"
        run.monthly_returns.to_csv(monthly_path)
        print(f"  Monthly returns saved → {monthly_path}")


if __name__ == "__main__":
    main()