#!/usr/bin/env python3
"""
scripts/run_market.py
─────────────────────
Run the CASH pipeline for one market and generate an HTML
recommendations report.

Usage
─────
  # US, default 365 days
  python -m scripts.run_market

  # Hong Kong, 180 days lookback
  python -m scripts.run_market -m HK --days 180

  # India, 90 days, open browser
  python -m scripts.run_market -m IN --days 90 --open

  # US with current holdings
  python -m scripts.run_market -m US --days 365 --holdings NVDA,CRWD,PANW --open
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import webbrowser
from datetime import datetime
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────
_SCRIPTS = Path(__file__).resolve().parent
_ROOT    = _SCRIPTS.parent
_SRC     = _ROOT / "src"
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cash.pipeline.orchestrator import run_full_pipeline
from cash.strategy_phase1.convergence import convergence_report
from cash.reports.html_report import generate_html_report
from common.config import MARKET_CONFIG


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="run_market",
        description=(
            "Run the CASH pipeline: load N days of data from "
            "existing parquet / DB / yfinance, analyse, and "
            "generate an HTML recommendations report."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m scripts.run_market                                  # US, 365d
  python -m scripts.run_market -m HK --days 180                 # HK, 180d
  python -m scripts.run_market -m IN --days 90 --open           # IN, 90d, open
  python -m scripts.run_market -m US --days 365 --holdings NVDA,CRWD --open

NOTE: Data must already exist in parquet/DB.  To download first:
  python ingest/ingest_cash.py --market us --days 365
        """,
    )

    p.add_argument(
        "-m", "--market",
        type=str,
        default="US",
        choices=sorted(MARKET_CONFIG.keys()),
        help="Market / region to analyse (default: US)",
    )
    p.add_argument(
        "-n", "--days",
        type=int,
        default=365,
        help=(
            "Calendar days of data to analyse.  An extra warm-up "
            "buffer (~220 days) is added automatically for "
            "indicator initialisation. (default: 365)"
        ),
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
        help=(
            "Comma-separated current holdings for rotation "
            "sell evaluation (e.g. NVDA,CRWD,PANW)"
        ),
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
        "--backtest",
        action="store_true",
        default=False,
        help="Run backtest phase (off by default for quick runs)",
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

    # ── Banner ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  CASH Pipeline — {market}")
    logger.info(f"  Benchmark   : {mcfg['benchmark']}")
    logger.info(f"  Universe    : {len(mcfg['universe'])} tickers")
    logger.info(f"  Engines     : {mcfg.get('engines', ['scoring'])}")
    logger.info(f"  Lookback    : {args.days} days")
    if holdings:
        logger.info(f"  Holdings    : {holdings}")
    logger.info("=" * 60)

    # ── Run pipeline ──────────────────────────────────────────
    wall_t0 = time.perf_counter()

    try:
        result = run_full_pipeline(
            market=market,
            lookback_days=args.days,
            capital=args.capital,
            current_holdings=holdings or None,
            enable_backtest=args.backtest,
        )
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    wall_elapsed = time.perf_counter() - wall_t0

    # ── Pipeline errors? ──────────────────────────────────────
    if result.n_errors > 0:
        logger.warning(
            f"{result.n_errors} error(s) during pipeline:"
        )
        for e in result.errors:
            logger.warning(f"  • {e}")

    if result.convergence is None:
        logger.error("No convergence result — cannot generate report")
        sys.exit(1)

    # ── Text report (optional) ────────────────────────────────
    if args.text:
        print()
        print(convergence_report(result.convergence))
        print()

    # ── HTML report ───────────────────────────────────────────
    logger.info("Generating HTML report…")
    html = generate_html_report(result)

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
    conv = result.convergence
    print()
    print(f"  {'─' * 44}")
    print(f"  {market}  │  {conv.n_tickers} tickers  │  "
          f"{args.days}d lookback  │  {wall_elapsed:.1f}s")
    print(f"  {'─' * 44}")
    print(f"  STRONG BUY : {len(conv.strong_buys):>3}")
    print(f"  BUY        : {len(conv.buys):>3}")
    print(f"  CONFLICT   : {len(conv.conflicts):>3}")
    print(f"  SELL       : {len(conv.sells):>3}")
    print(f"  HOLD       : {len(conv.holds):>3}")

    if conv.strong_buys:
        print()
        print("  Top Strong-Buy picks:")
        for s in conv.strong_buys[:5]:
            print(
                f"    #{s.rank:<3}  {s.ticker:<8s}  "
                f"adj={s.adjusted_score:.3f}  "
                f"{s.scoring_regime}"
            )

    print()
    print(f"  📄  {out_path.resolve()}")
    print()

    # ── Open in browser ───────────────────────────────────────
    if args.open:
        url = f"file://{out_path.resolve()}"
        logger.info(f"Opening {url}")
        webbrowser.open(url)


if __name__ == "__main__":
    main()