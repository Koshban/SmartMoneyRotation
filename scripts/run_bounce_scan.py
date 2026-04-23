#!/usr/bin/env python3
"""
run_bounce_scan.py
──────────────────
Standalone script to run the bounce scanner.

Usage:
    python run_bounce_scan.py                    # default market (US)
    python run_bounce_scan.py --market IN        # Indian market
    python run_bounce_scan.py --market US --top 15
    python run_bounce_scan.py --csv              # also save to CSV
    python run_bounce_scan.py --relaxed          # relax filters for more hits
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Add project root to path if needed ────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Project imports ───────────────────────────────────────────
from strategy.bounce import (
    scan_bounce_candidates,
    bounce_report,
    BounceScanResult,
)

# Import your orchestrator — adjust the import path to match
# your actual module name:
#   from orchestrator import MomentumPipeline
#   from pipeline import Pipeline
#   from main_pipeline import Orchestrator
# Pick whichever matches your project structure:

try:
    from pipeline.orchestrator import Orchestrator  as Pipeline
except ImportError:
    try:
        from pipeline import Pipeline
    except ImportError:
        print(
            "ERROR: Cannot import your pipeline class.\n"
            "Edit the import at the top of run_bounce_scan.py\n"
            "to match your actual orchestrator module and class name."
        )
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s │ %(levelname)-7s │ %(name)s │ %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    # Quieten noisy libraries
    for lib in ("urllib3", "yfinance", "requests", "filelock"):
        logging.getLogger(lib).setLevel(logging.WARNING)


logger = logging.getLogger("bounce_scan")


# ═══════════════════════════════════════════════════════════════
#  BUILD SCORED UNIVERSE VIA EXISTING PIPELINE
# ═══════════════════════════════════════════════════════════════

def build_scored_universe(
    market: str = "US",
) -> dict[str, "pd.DataFrame"]:
    """
    Run the pipeline through Phase 2 (scoring) to produce
    the scored_universe dict needed by the bounce scanner.

    This reuses ALL your existing data-fetching, indicator
    calculation, and RS-regime logic — no duplication.
    """
    logger.info(f"Building scored universe for market={market} ...")
    t0 = time.perf_counter()

    pipe = Pipeline(market=market)

    # Phase 0 — Load OHLCV data
    pipe.load_data()

    # Phase 1 — Breadth + sector RS context
    pipe.compute_universe_context()

    # Phase 2 — Score each ticker
    pipe.run_tickers()

    # Extract the scored universe dict
    scored = pipe._scored_universe

    elapsed = time.perf_counter() - t0
    logger.info(
        f"Scored universe ready: {len(scored)} tickers "
        f"in {elapsed:.1f}s"
    )
    return scored


# ═══════════════════════════════════════════════════════════════
#  RELAXED PARAMETER PRESETS
# ═══════════════════════════════════════════════════════════════

RELAXED_PARAMS = {
    "rsi2_oversold":        15,       # up from 10
    "rsi5_oversold":        30,       # up from 25
    "rsi14_max":            45,       # up from 40
    "vol_ratio_max":        0.85,     # up from 0.70
    "min_consecutive_down": 2,        # down from 3
    "max_drawdown_pct":     0.20,     # up from 0.15
    "min_bounce_score":     0.30,     # down from 0.40
    "require_above_ma200":  True,     # keep this safety rail
}


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bounce Scanner — find oversold dip setups",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_bounce_scan.py
  python run_bounce_scan.py --market IN
  python run_bounce_scan.py --market US --top 15 --csv
  python run_bounce_scan.py --relaxed --top 20
        """,
    )
    parser.add_argument(
        "--market", "-m",
        default="US",
        help="Market code, e.g. US, IN  (default: US)",
    )
    parser.add_argument(
        "--top", "-n",
        type=int,
        default=10,
        help="Max candidates to show  (default: 10)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Save results to CSV",
    )
    parser.add_argument(
        "--relaxed",
        action="store_true",
        help="Use relaxed thresholds (more hits, lower quality)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Debug-level logging",
    )
    parser.add_argument(
        "--no-ma200",
        action="store_true",
        help="Remove the above-MA200 requirement (aggressive)",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    # ── Build parameters ──────────────────────────────────────
    params = {}

    if args.relaxed:
        params.update(RELAXED_PARAMS)
        logger.info("Using RELAXED parameter preset")

    params["max_candidates"] = args.top

    if args.no_ma200:
        params["require_above_ma200"] = False
        logger.warning(
            "MA200 filter disabled — you may see structurally "
            "broken names. Use with caution."
        )

    # ── Build scored universe ─────────────────────────────────
    try:
        scored_universe = build_scored_universe(market=args.market)
    except Exception as e:
        logger.error(f"Failed to build scored universe: {e}")
        logger.error(
            "Check that your pipeline import and method names "
            "are correct at the top of this script."
        )
        sys.exit(1)

    if not scored_universe:
        logger.error("Scored universe is empty — nothing to scan.")
        sys.exit(1)

    # ── Run bounce scanner ────────────────────────────────────
    logger.info("Running bounce scanner ...")
    t0 = time.perf_counter()

    result: BounceScanResult = scan_bounce_candidates(
        scored_universe, params=params
    )

    elapsed = time.perf_counter() - t0
    logger.info(f"Bounce scan completed in {elapsed:.2f}s")

    # ── Print report ──────────────────────────────────────────
    print()
    print(bounce_report(result))

    # ── Save CSV if requested ─────────────────────────────────
    if args.csv and result.candidates:
        today = datetime.now().strftime("%Y%m%d")
        fname = f"bounce_scan_{args.market}_{today}.csv"
        df = result.to_dataframe()
        df.to_csv(fname, index=False)
        logger.info(f"Results saved to {fname}")

    # ── Exit code: 0 if candidates found, 1 if none ──────────
    sys.exit(0 if result.candidates else 1)


if __name__ == "__main__":
    main()