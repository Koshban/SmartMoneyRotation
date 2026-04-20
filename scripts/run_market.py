#!/usr/bin/env python3
"""
scripts/run_market.py
─────────────────────
Run the CASH pipeline for one market and generate an HTML
recommendations report.

Usage
─────
  # US (default) — scoring + rotation convergence
  python -m scripts.run_market

  # Hong Kong — scoring only, 180 days data
  python -m scripts.run_market -m HK -n 180

  # India — open report in browser automatically
  python -m scripts.run_market -m IN --open

  # US with current holdings for rotation sell evaluation
  python -m scripts.run_market -m US --holdings NVDA,CRWD,PANW

  # Custom output path
  python -m scripts.run_market -m US -o ~/reports/us_today.html
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import webbrowser
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Pipeline imports ──────────────────────────────────────────
from pipeline.orchestrator import Orchestrator, run_full_pipeline
from strategy.convergence import convergence_report
from reports.html_report import generate_html_report
from common.config import MARKET_CONFIG, ACTIVE_MARKETS


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="run_market",
        description="Run CASH pipeline and generate HTML report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m scripts.run_market
  python -m scripts.run_market -m HK -n 180
  python -m scripts.run_market -m US --holdings NVDA,CRWD --open
        """,
    )

    p.add_argument(
        "-m", "--market",
        type=str,
        default="US",
        choices=list(MARKET_CONFIG.keys()),
        help="Market to analyse (default: US)",
    )
    p.add_argument(
        "-n", "--days",
        type=int,
        default=365,
        help="Lookback days for data loading (default: 365)",
    )
    p.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output HTML path (default: cash_{market}_{date}.html)",
    )
    p.add_argument(
        "--holdings",
        type=str,
        default="",
        help="Comma-separated current holdings for rotation "
             "sell evaluation (e.g. NVDA,CRWD,PANW)",
    )
    p.add_argument(
        "--capital",
        type=float,
        default=None,
        help="Portfolio capital (default: from config)",
    )
    p.add_argument(
        "--open",
        action="store_true",
        help="Open the report in default browser after generation",
    )
    p.add_argument(
        "--text",
        action="store_true",
        help="Also print text convergence report to stdout",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--no-backtest",
        action="store_true",
        default=True,
        help="Skip backtest phase (default: skip)",
    )

    return p.parse_args()


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()
    _setup_logging(args.verbose)

    logger = logging.getLogger("run_market")

    market = args.market.upper()
    mcfg = MARKET_CONFIG.get(market)
    if mcfg is None:
        logger.error(f"Unknown market: {market}")
        sys.exit(1)

    # Parse holdings
    holdings: list[str] = []
    if args.holdings:
        holdings = [
            t.strip().upper()
            for t in args.holdings.split(",")
            if t.strip()
        ]

    # ── Info banner ───────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  CASH Pipeline — {market}")
    logger.info(f"  Benchmark  : {mcfg['benchmark']}")
    logger.info(f"  Universe   : {len(mcfg['universe'])} tickers")
    logger.info(f"  Engines    : {mcfg['engines']}")
    logger.info(f"  Lookback   : {args.days} days")
    if holdings:
        logger.info(f"  Holdings   : {holdings}")
    logger.info("=" * 60)

    # ── Run pipeline ──────────────────────────────────────────
    t0 = time.perf_counter()

    try:
        result = run_full_pipeline(
            market=market,
            capital=args.capital,
            current_holdings=holdings if holdings else None,
            enable_backtest=not args.no_backtest,
        )
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.perf_counter() - t0
    logger.info(f"Pipeline completed in {elapsed:.1f}s")

    # ── Text report (optional) ────────────────────────────────
    if args.text and result.convergence:
        print()
        print(convergence_report(result.convergence))
        print()

    # ── HTML report ───────────────────────────────────────────
    logger.info("Generating HTML report…")

    html = generate_html_report(result)

    # Determine output path
    if args.output:
        out_path = Path(args.output)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        out_path = Path(f"cash_{market.lower()}_{ts}.html")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    logger.info(f"Report written: {out_path}  ({size_kb:.0f} KB)")

    # ── Summary to stdout ─────────────────────────────────────
    if result.convergence:
        conv = result.convergence
        print()
        print(f"  {market}  |  {conv.n_tickers} tickers")
        print(f"  STRONG_BUY : {len(conv.strong_buys)}")
        print(f"  BUY        : {len(conv.buys)}")
        print(f"  CONFLICT   : {len(conv.conflicts)}")
        print(f"  SELL        : {len(conv.sells)}")
        print(f"  HOLD       : {len(conv.holds)}")

        if conv.strong_buys:
            print()
            print("  Top STRONG_BUY:")
            for s in conv.strong_buys[:5]:
                print(f"    #{s.rank}  {s.ticker:<8s}  "
                      f"adj={s.adjusted_score:.3f}")

    print()
    print(f"  Report → {out_path.resolve()}")
    print()

    # ── Open in browser ───────────────────────────────────────
    if args.open:
        url = f"file://{out_path.resolve()}"
        logger.info(f"Opening {url}")
        webbrowser.open(url)


if __name__ == "__main__":
    main()