"""
main.py
CASH — Composite Adaptive Signal Hierarchy
===========================================

Entry point.  One command → full pipeline → reports on disk.

Usage:
    python main.py                                # default run
    python main.py --portfolio 150000             # custom portfolio size
    python main.py --positions positions.json     # with current holdings
    python main.py --output-dir reports/          # custom output folder
    python main.py --text-only                    # skip HTML
    python main.py --universe universes/core.json # custom universe
    python main.py --regime bear_mild             # force regime override
    python main.py --dry-run                      # score only, no sizing
    python main.py --verbose                      # debug logging
"""

import argparse
import json
import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

# ── CASH modules ────────────────────────────────────────────────
from config.settings import get_config, override_config
from common.universe import load_universe
from pipeline.orchestrator import (
    run_full_pipeline,
    run_score_only,
    print_pipeline_summary,
)
from reports.recommendations import (
    build_report,
    to_text,
    to_html,
    save_text,
    save_html,
    print_report,
)
from reports.portfolio_view import (
    build_rebalance_plan,
    rebalance_to_text,
    save_rebalance_text,
    save_rebalance_html,
    print_rebalance,
    quick_diff,
)


# ═════════════════════════════════════════════════════════════════
#  LOGGING
# ═════════════════════════════════════════════════════════════════

def setup_logging(verbose: bool = False, log_file: str = None):
    """Configure logging for the run."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s"
    datefmt = "%H:%M:%S"

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode="w"))

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
    )
    # quiet down noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)


logger = logging.getLogger("cash.main")


# ═════════════════════════════════════════════════════════════════
#  CLI ARGUMENT PARSER
# ═════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cash",
        description="CASH — Composite Adaptive Signal Hierarchy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --portfolio 200000 --positions holdings.json
  python main.py --dry-run --verbose
  python main.py --output-dir ~/reports --universe universes/growth.json
        """,
    )

    # ── portfolio ───────────────────────────────────────────────
    p.add_argument(
        "--portfolio", "-p",
        type=float,
        default=None,
        help="Total portfolio value in dollars (overrides config)",
    )
    p.add_argument(
        "--positions",
        type=str,
        default=None,
        help="Path to JSON file with current holdings "
             "(enables rebalance report)",
    )

    # ── universe ────────────────────────────────────────────────
    p.add_argument(
        "--universe", "-u",
        type=str,
        default=None,
        help="Path to universe JSON file (overrides default)",
    )
    p.add_argument(
        "--tickers", "-t",
        type=str,
        nargs="+",
        default=None,
        help="Run on specific tickers only (space-separated)",
    )

    # ── regime ──────────────────────────────────────────────────
    p.add_argument(
        "--regime",
        type=str,
        choices=["bull_confirmed", "bull_cautious", "bear_mild", "bear_severe"],
        default=None,
        help="Force a specific regime (skips auto-detection)",
    )

    # ── output ──────────────────────────────────────────────────
    p.add_argument(
        "--output-dir", "-o",
        type=str,
        default="output",
        help="Directory for report files (default: output/)",
    )
    p.add_argument(
        "--text-only",
        action="store_true",
        help="Generate text reports only, skip HTML",
    )
    p.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress terminal output (files still saved)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Also save structured report as JSON",
    )

    # ── run mode ────────────────────────────────────────────────
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Score and rank only — no position sizing or allocation",
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Only show top N buy candidates",
    )

    # ── debug ───────────────────────────────────────────────────
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Write log to file",
    )

    return p


# ═════════════════════════════════════════════════════════════════
#  POSITION LOADING
# ═════════════════════════════════════════════════════════════════

def load_positions(filepath: str) -> list[dict]:
    """
    Load current holdings from a JSON file.

    Expected format — list of objects:
    [
        {
            "ticker": "AAPL",
            "shares": 50,
            "avg_cost": 142.30,
            "current_price": 178.50,
            "category": "Core Equity",
            "bucket": "core_equity"
        },
        ...
    ]

    Also supports a wrapper format:
    {
        "positions": [ ... ],
        "cash": 25000.0
    }
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Positions file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    # handle wrapper format
    if isinstance(data, dict):
        positions = data.get("positions", [])
        cash = data.get("cash", None)
    elif isinstance(data, list):
        positions = data
        cash = None
    else:
        logger.error(f"Unexpected positions format in {filepath}")
        sys.exit(1)

    # validate
    required_fields = {"ticker", "shares", "avg_cost", "current_price"}
    for i, pos in enumerate(positions):
        missing = required_fields - set(pos.keys())
        if missing:
            logger.error(
                f"Position {i} ({pos.get('ticker', '?')}) missing fields: {missing}"
            )
            sys.exit(1)

    logger.info(f"Loaded {len(positions)} positions from {filepath}")
    if cash is not None:
        logger.info(f"Cash from positions file: ${cash:,.0f}")

    return positions, cash


# ═════════════════════════════════════════════════════════════════
#  OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def ensure_output_dir(output_dir: str) -> Path:
    """Create output directory if it doesn't exist."""
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_filenames(output_dir: Path, date_str: str) -> dict:
    """Generate timestamped filenames for all output files."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "report_txt":    output_dir / f"cash_report_{date_str}_{ts}.txt",
        "report_html":   output_dir / f"cash_report_{date_str}_{ts}.html",
        "report_json":   output_dir / f"cash_report_{date_str}_{ts}.json",
        "rebalance_txt": output_dir / f"cash_rebalance_{date_str}_{ts}.txt",
        "rebalance_html": output_dir / f"cash_rebalance_{date_str}_{ts}.html",
        "rebalance_json": output_dir / f"cash_rebalance_{date_str}_{ts}.json",
        "log":           output_dir / f"cash_log_{date_str}_{ts}.log",
        "pipeline_json": output_dir / f"cash_pipeline_{date_str}_{ts}.json",
    }


# ═════════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ═════════════════════════════════════════════════════════════════

def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── setup ───────────────────────────────────────────────────
    output_dir = ensure_output_dir(args.output_dir)
    date_str = datetime.now().strftime("%Y%m%d")
    filenames = generate_filenames(output_dir, date_str)

    log_file = args.log_file or str(filenames["log"])
    setup_logging(verbose=args.verbose, log_file=log_file)

    logger.info("=" * 60)
    logger.info("  CASH — Composite Adaptive Signal Hierarchy")
    logger.info("=" * 60)

    t_start = time.time()

    # ── load config ─────────────────────────────────────────────
    config = get_config()
    if args.portfolio:
        config = override_config(config, portfolio_value=args.portfolio)
        logger.info(f"Portfolio override: ${args.portfolio:,.0f}")
    if args.regime:
        config = override_config(config, regime_override=args.regime)
        logger.info(f"Regime override: {args.regime}")

    portfolio_value = config.portfolio_value
    logger.info(f"Portfolio value: ${portfolio_value:,.0f}")

    # ── load universe ───────────────────────────────────────────
    if args.tickers:
        # ad-hoc ticker list from CLI
        tickers = [t.upper() for t in args.tickers]
        universe = [{"ticker": t, "category": "", "bucket": "core_equity"}
                    for t in tickers]
        logger.info(f"CLI tickers: {', '.join(tickers)}")
    elif args.universe:
        universe = load_universe(args.universe)
        logger.info(f"Universe loaded from {args.universe}: "
                    f"{len(universe)} tickers")
    else:
        universe = load_universe()
        logger.info(f"Default universe: {len(universe)} tickers")

    # ── load current positions (optional) ───────────────────────
    current_positions = None
    positions_cash = None
    if args.positions:
        current_positions, positions_cash = load_positions(args.positions)
        if positions_cash is not None:
            # use cash from positions file if not overridden
            logger.info(f"Using cash balance from positions file: "
                        f"${positions_cash:,.0f}")

    # ── run pipeline ────────────────────────────────────────────
    logger.info("")
    logger.info("─── RUNNING PIPELINE ────────────────────────────────────")

    if args.dry_run:
        logger.info("DRY RUN mode — scoring only, no position sizing")
        pipeline_output = run_score_only(
            universe=universe,
            config=config,
        )
    else:
        pipeline_output = run_full_pipeline(
            universe=universe,
            config=config,
        )

    t_pipeline = time.time()
    logger.info(f"Pipeline completed in {t_pipeline - t_start:.1f}s")

    # ── print pipeline summary ──────────────────────────────────
    if not args.quiet:
        print_pipeline_summary(pipeline_output)

    # ── build recommendation report ─────────────────────────────
    logger.info("")
    logger.info("─── BUILDING REPORTS ────────────────────────────────────")

    report = build_report(pipeline_output)

    # apply --top-n filter if specified
    if args.top_n and args.top_n > 0:
        report["buy_list"] = report["buy_list"][:args.top_n]
        report["portfolio_snapshot"]["buy_count"] = len(report["buy_list"])
        logger.info(f"Filtered to top {args.top_n} buys")

    # ── terminal output ─────────────────────────────────────────
    if not args.quiet:
        print()
        print_report(report)

    # ── save recommendation report ──────────────────────────────
    save_text(report, str(filenames["report_txt"]))
    logger.info(f"Text report  → {filenames['report_txt']}")

    if not args.text_only:
        save_html(report, str(filenames["report_html"]))
        logger.info(f"HTML report  → {filenames['report_html']}")

    if args.json:
        _save_json(report, str(filenames["report_json"]))
        logger.info(f"JSON report  → {filenames['report_json']}")

    # ── save raw pipeline output ────────────────────────────────
    if args.verbose:
        _save_pipeline_json(pipeline_output, str(filenames["pipeline_json"]))
        logger.info(f"Pipeline JSON → {filenames['pipeline_json']}")

    # ── rebalance plan (if positions provided) ──────────────────
    if current_positions is not None:
        logger.info("")
        logger.info("─── BUILDING REBALANCE PLAN ─────────────────────────────")

        cash_for_rebalance = (
            positions_cash
            if positions_cash is not None
            else pipeline_output["summary"].get("cash_remaining", portfolio_value * 0.1)
        )

        plan = build_rebalance_plan(
            report=report,
            current_positions=current_positions,
            cash_balance=cash_for_rebalance,
            portfolio_value=portfolio_value,
        )

        if not args.quiet:
            print()
            print_rebalance(plan)

        save_rebalance_text(plan, str(filenames["rebalance_txt"]))
        logger.info(f"Rebalance text → {filenames['rebalance_txt']}")

        if not args.text_only:
            save_rebalance_html(plan, str(filenames["rebalance_html"]))
            logger.info(f"Rebalance HTML → {filenames['rebalance_html']}")

        if args.json:
            _save_json(plan.to_dict(), str(filenames["rebalance_json"]))
            logger.info(f"Rebalance JSON → {filenames['rebalance_json']}")

        # quick summary
        logger.info(f"Trades required: {plan.trade_count}")
        logger.info(f"Net cash impact: ${plan.net_cash_impact:+,.0f}")
        if plan.warnings:
            for w in plan.warnings:
                logger.warning(w)
    else:
        logger.info("")
        logger.info("No positions file provided — skipping rebalance plan.")
        logger.info("Use --positions <file.json> to generate one.")

    # ── done ────────────────────────────────────────────────────
    t_end = time.time()
    elapsed = t_end - t_start

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  CASH run complete in {elapsed:.1f}s")
    logger.info(f"  Reports saved to: {output_dir}/")
    logger.info("=" * 60)

    # return report for programmatic use
    return report


# ═════════════════════════════════════════════════════════════════
#  PROGRAMMATIC ENTRY POINTS
# ═════════════════════════════════════════════════════════════════

def run(
    portfolio_value: float = None,
    universe_path: str = None,
    tickers: list[str] = None,
    regime_override: str = None,
    positions: list[dict] = None,
    cash_balance: float = None,
    output_dir: str = "output",
    save_files: bool = True,
    verbose: bool = False,
) -> dict:
    """
    Programmatic entry point — call from notebooks, scripts, or
    other Python code without going through CLI.

    Returns dict with keys: report, plan (if positions given),
    pipeline_output, filenames.
    """
    setup_logging(verbose=verbose)

    config = get_config()
    if portfolio_value:
        config = override_config(config, portfolio_value=portfolio_value)
    if regime_override:
        config = override_config(config, regime_override=regime_override)

    pv = config.portfolio_value

    # universe
    if tickers:
        universe = [{"ticker": t.upper(), "category": "", "bucket": "core_equity"}
                    for t in tickers]
    elif universe_path:
        universe = load_universe(universe_path)
    else:
        universe = load_universe()

    # pipeline
    pipeline_output = run_full_pipeline(universe=universe, config=config)

    # report
    report = build_report(pipeline_output)

    result = {
        "report":          report,
        "pipeline_output": pipeline_output,
        "plan":            None,
        "filenames":       {},
    }

    # rebalance
    if positions is not None:
        cb = cash_balance if cash_balance is not None else pv * 0.1
        plan = build_rebalance_plan(
            report=report,
            current_positions=positions,
            cash_balance=cb,
            portfolio_value=pv,
        )
        result["plan"] = plan

    # save
    if save_files:
        out = ensure_output_dir(output_dir)
        date_str = datetime.now().strftime("%Y%m%d")
        fnames = generate_filenames(out, date_str)
        save_text(report, str(fnames["report_txt"]))
        save_html(report, str(fnames["report_html"]))
        result["filenames"]["report_txt"] = str(fnames["report_txt"])
        result["filenames"]["report_html"] = str(fnames["report_html"])

        if result["plan"]:
            save_rebalance_text(result["plan"], str(fnames["rebalance_txt"]))
            save_rebalance_html(result["plan"], str(fnames["rebalance_html"]))
            result["filenames"]["rebalance_txt"] = str(fnames["rebalance_txt"])
            result["filenames"]["rebalance_html"] = str(fnames["rebalance_html"])

    return result


# ═════════════════════════════════════════════════════════════════
#  SERIALISATION HELPERS
# ═════════════════════════════════════════════════════════════════

def _save_json(data: dict, filepath: str):
    """Save dict to JSON with sane defaults."""
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=_json_serialiser)


def _save_pipeline_json(pipeline_output: dict, filepath: str):
    """
    Save raw pipeline output.  Strips large DataFrames to just
    summary stats to keep file size reasonable.
    """
    serialisable = {}
    for key, value in pipeline_output.items():
        if hasattr(value, "to_dict"):
            # pandas DataFrame or Series — convert
            serialisable[key] = value.to_dict()
        elif isinstance(value, (dict, list, str, int, float, bool, type(None))):
            serialisable[key] = value
        else:
            serialisable[key] = str(value)

    with open(filepath, "w") as f:
        json.dump(serialisable, f, indent=2, default=_json_serialiser)


def _json_serialiser(obj):
    """Fallback serialiser for JSON encoding."""
    import numpy as np
    import pandas as pd

    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


# ═════════════════════════════════════════════════════════════════
#  ENTRY
# ═════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()