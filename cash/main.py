"""
main.py
CASH — Composite Adaptive Signal Hierarchy
===========================================

Entry point.  One command → full pipeline → reports on disk.

Usage:
    python main.py                                # default run
    python main.py --portfolio 150000             # custom capital
    python main.py --positions positions.json     # with holdings
    python main.py --output-dir reports/          # custom output
    python main.py --text-only                    # skip HTML
    python main.py --tickers AAPL MSFT NVDA       # specific tickers
    python main.py --universe universes/core.json # custom universe
    python main.py --dry-run                      # score only
    python main.py --backtest                     # include backtest
    python main.py --verbose                      # debug logging
"""

import argparse
import json
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ── CASH modules ────────────────────────────────────────────────
from common.config import (
    UNIVERSE,
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    LOGS_DIR,
)
from cash.pipeline.orchestrator import (
    Orchestrator,
    PipelineResult,
    run_full_pipeline,
)
from cash.pipeline.runner import results_errors
from cash.reports.recommendations import (
    build_report,
    to_text,
    to_html,
    save_text,
    save_html,
    print_report,
)

# Optional: portfolio rebalance view
try:
    from cash.reports.portfolio_view import (
        build_rebalance_plan,
        save_rebalance_text,
        save_rebalance_html,
        print_rebalance,
    )
    _HAS_REBALANCE = True
except ImportError:
    _HAS_REBALANCE = False


# ═════════════════════════════════════════════════════════════════
#  LOGGING
# ═════════════════════════════════════════════════════════════════

def setup_logging(
    verbose: bool = False,
    log_file: str | None = None,
) -> str:
    """
    Configure root logger.

    If *log_file* is ``None`` the log is written to ``LOGS_DIR``
    (from ``common.config``) with a timestamped filename.

    Returns the resolved log-file path.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s"
    datefmt = "%H:%M:%S"

    # Ensure LOGS_DIR exists
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)

    # Default log location: LOGS_DIR/cash_<timestamp>.log
    if log_file is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(logs_path / f"cash_{ts}.log")
    else:
        # Relative paths resolve inside LOGS_DIR
        lf = Path(log_file)
        if not lf.is_absolute():
            log_file = str(logs_path / lf)

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="w"),
    ]

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
        force=True,
    )
    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    return log_file


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
  python main.py --tickers AAPL MSFT NVDA GOOG
  python main.py --output-dir ~/reports --text-only
        """,
    )

    # ── portfolio ───────────────────────────────────────────────
    p.add_argument(
        "--portfolio", "-p", type=float, default=None,
        help="Total portfolio value in dollars (overrides config)",
    )
    p.add_argument(
        "--positions", type=str, default=None,
        help="Path to JSON file with current holdings "
             "(enables rebalance report)",
    )

    # ── universe ────────────────────────────────────────────────
    p.add_argument(
        "--universe", "-u", type=str, default=None,
        help="Path to universe JSON file (list of tickers)",
    )
    p.add_argument(
        "--tickers", "-t", type=str, nargs="+", default=None,
        help="Run on specific tickers only (space-separated)",
    )

    # ── output ──────────────────────────────────────────────────
    p.add_argument(
        "--output-dir", "-o", type=str, default="output",
        help="Directory for report files (default: output/)",
    )
    p.add_argument(
        "--text-only", action="store_true",
        help="Generate text reports only, skip HTML",
    )
    p.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress terminal output (files still saved)",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Also save structured report as JSON",
    )

    # ── run mode ────────────────────────────────────────────────
    p.add_argument(
        "--dry-run", action="store_true",
        help="Score and rank only — no portfolio or signals",
    )
    p.add_argument(
        "--backtest", action="store_true",
        help="Run historical backtest after pipeline",
    )
    p.add_argument(
        "--top-n", type=int, default=None,
        help="Only show top N buy candidates in output",
    )

    # ── debug ───────────────────────────────────────────────────
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--log-file", type=str, default=None,
        help="Custom log filename (written inside LOGS_DIR)",
    )

    return p


# ═════════════════════════════════════════════════════════════════
#  POSITION LOADING
# ═════════════════════════════════════════════════════════════════

def load_positions(
    filepath: str,
) -> tuple[list[dict], float | None]:
    """
    Load current holdings from a JSON file.

    Supports two formats:

    Plain list::

        [
            {"ticker": "AAPL", "shares": 50,
             "avg_cost": 142.30, "current_price": 178.50},
            ...
        ]

    Wrapper with cash::

        {
            "positions": [ ... ],
            "cash": 25000.0
        }

    Returns ``(positions_list, cash_or_None)``.
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Positions file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        positions = data.get("positions", [])
        cash = data.get("cash", None)
    elif isinstance(data, list):
        positions = data
        cash = None
    else:
        logger.error(
            f"Unexpected positions format in {filepath}"
        )
        sys.exit(1)

    required = {"ticker", "shares", "avg_cost", "current_price"}
    for i, pos in enumerate(positions):
        missing = required - set(pos.keys())
        if missing:
            logger.error(
                f"Position {i} ({pos.get('ticker', '?')}) "
                f"missing fields: {missing}"
            )
            sys.exit(1)

    logger.info(
        f"Loaded {len(positions)} positions from {filepath}"
    )
    if cash is not None:
        logger.info(f"Cash from positions file: ${cash:,.0f}")

    return positions, cash


def load_universe_file(filepath: str) -> list[str]:
    """
    Load a universe JSON file and return ticker strings.

    Accepts a plain list of strings::

        ["AAPL", "MSFT", "NVDA"]

    or a list of objects with a ``"ticker"`` key::

        [{"ticker": "AAPL", "category": "Tech"}, ...]
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Universe file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    if not isinstance(data, list) or len(data) == 0:
        logger.error("Universe file must be a non-empty list")
        sys.exit(1)

    if isinstance(data[0], str):
        return [t.upper() for t in data]
    elif isinstance(data[0], dict) and "ticker" in data[0]:
        return [d["ticker"].upper() for d in data]
    else:
        logger.error(
            "Universe entries must be strings or dicts "
            "with a 'ticker' key"
        )
        sys.exit(1)


# ═════════════════════════════════════════════════════════════════
#  OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def ensure_output_dir(output_dir: str) -> Path:
    """Create the output directory if it doesn't exist."""
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_filenames(
    output_dir: Path, date_str: str,
) -> dict:
    """Generate timestamped filenames for all output files."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "report_txt":     output_dir / f"cash_report_{date_str}_{ts}.txt",
        "report_html":    output_dir / f"cash_report_{date_str}_{ts}.html",
        "report_json":    output_dir / f"cash_report_{date_str}_{ts}.json",
        "rebalance_txt":  output_dir / f"cash_rebalance_{date_str}_{ts}.txt",
        "rebalance_html": output_dir / f"cash_rebalance_{date_str}_{ts}.html",
        "rebalance_json": output_dir / f"cash_rebalance_{date_str}_{ts}.json",
        "pipeline_json":  output_dir / f"cash_pipeline_{date_str}_{ts}.json",
    }


# ═════════════════════════════════════════════════════════════════
#  DRY-RUN HELPERS
# ═════════════════════════════════════════════════════════════════

def _run_dry(
    tickers: list[str],
    capital: float,
) -> PipelineResult:
    """
    Execute Phases 0–2 only (load → breadth/sector → score).

    Skips portfolio construction, signal generation, and
    report building.  Useful for inspecting raw scores.
    """
    orch = Orchestrator(
        universe=tickers,
        capital=capital,
        enable_breadth=True,
        enable_sectors=True,
        enable_signals=False,
        enable_backtest=False,
    )
    orch.load_data()
    orch.compute_universe_context()
    orch.run_tickers()

    errors = results_errors(orch._ticker_results)

    return PipelineResult(
        ticker_results=orch._ticker_results,
        scored_universe=orch._scored_universe,
        snapshots=orch._snapshots,
        breadth=orch._breadth,
        breadth_scores=orch._breadth_scores,
        sector_rs=orch._sector_rs,
        bench_df=orch._bench_df,                    # ← NEW
        errors=errors,
        timings=orch._timings,
        run_date=pd.Timestamp.now(),
    )


def _print_dry_run_summary(
    result: PipelineResult,
    top_n: int = 20,
) -> None:
    """Print a compact scoring table for dry-run mode."""
    snaps = result.snapshots[:top_n]
    if not snaps:
        print("  No scored tickers.")
        return

    print()
    print("  DRY RUN — Top Scored Tickers")
    print("  " + "─" * 58)
    print(
        f"  {'Rank':<5} {'Ticker':<8} {'Composite':>10} "
        f"{'Signal':<10} {'Close':>10}"
    )
    print("  " + "─" * 58)

    for i, s in enumerate(snaps, 1):
        ticker = s.get("ticker", "???")
        score = s.get("composite", 0)
        signal = s.get("signal", "—")
        close = s.get("close", 0)
        print(
            f"  {i:<5} {ticker:<8} {score:>10.1f} "
            f"{signal:<10} {close:>10.2f}"
        )

    print("  " + "─" * 58)
    print(
        f"  {result.n_tickers} scored, "
        f"{result.n_errors} errors"
    )
    print()


# ═════════════════════════════════════════════════════════════════
#  REBALANCE HANDLER
# ═════════════════════════════════════════════════════════════════

def _handle_rebalance(
    args: argparse.Namespace,
    report: dict,
    current_positions: list[dict],
    positions_cash: float | None,
    capital: float,
    filenames: dict,
) -> None:
    """Build and save the rebalance plan."""
    logger.info("")
    logger.info(
        "─── BUILDING REBALANCE PLAN ───────────────────────"
    )

    cash_for_rebalance = (
        positions_cash
        if positions_cash is not None
        else capital * 0.10
    )

    try:
        plan = build_rebalance_plan(
            report=report,
            current_positions=current_positions,
            cash_balance=cash_for_rebalance,
            portfolio_value=capital,
        )
    except Exception as e:
        logger.warning(f"build_rebalance_plan failed: {e}")
        return

    # Terminal
    if not args.quiet:
        try:
            print()
            print_rebalance(plan)
        except Exception as e:
            logger.warning(f"print_rebalance failed: {e}")

    # Text
    try:
        save_rebalance_text(
            plan, str(filenames["rebalance_txt"])
        )
        logger.info(
            f"Rebalance text → {filenames['rebalance_txt']}"
        )
    except Exception as e:
        logger.warning(f"save_rebalance_text failed: {e}")

    # HTML
    if not args.text_only:
        try:
            save_rebalance_html(
                plan, str(filenames["rebalance_html"])
            )
            logger.info(
                f"Rebalance HTML → {filenames['rebalance_html']}"
            )
        except Exception as e:
            logger.warning(
                f"save_rebalance_html failed: {e}"
            )

    # JSON
    if args.json and hasattr(plan, "to_dict"):
        _save_json(
            plan.to_dict(),
            str(filenames["rebalance_json"]),
        )
        logger.info(
            f"Rebalance JSON → {filenames['rebalance_json']}"
        )

    # Summary log
    if hasattr(plan, "trade_count"):
        logger.info(f"Trades required: {plan.trade_count}")
    if hasattr(plan, "net_cash_impact"):
        logger.info(
            f"Net cash impact: ${plan.net_cash_impact:+,.0f}"
        )
    if hasattr(plan, "warnings") and plan.warnings:
        for w in plan.warnings:
            logger.warning(w)


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

    log_file = setup_logging(
        verbose=args.verbose,
        log_file=args.log_file,
    )

    logger.info("=" * 60)
    logger.info("  CASH — Composite Adaptive Signal Hierarchy")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)

    t_start = time.time()

    # ── capital ─────────────────────────────────────────────────
    capital = args.portfolio or PORTFOLIO_PARAMS.get(
        "total_capital", 100_000
    )
    logger.info(f"Portfolio value: ${capital:,.0f}")

    # ── universe ────────────────────────────────────────────────
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
        logger.info(f"CLI tickers: {', '.join(tickers)}")
    elif args.universe:
        tickers = load_universe_file(args.universe)
        logger.info(
            f"Universe from {args.universe}: "
            f"{len(tickers)} tickers"
        )
    else:
        tickers = list(UNIVERSE)
        logger.info(
            f"Default universe: {len(tickers)} tickers"
        )

    # ── load positions (optional) ───────────────────────────────
    current_positions = None
    positions_cash = None
    if args.positions:
        current_positions, positions_cash = load_positions(
            args.positions
        )

    # ── run pipeline ────────────────────────────────────────────
    logger.info("")
    logger.info(
        "─── RUNNING PIPELINE ──────────────────────────────"
    )

    if args.dry_run:
        logger.info(
            "DRY RUN — scoring only, no portfolio/signals"
        )
        result = _run_dry(tickers, capital)
    else:
        result = run_full_pipeline(
            universe=tickers,
            capital=capital,
            enable_breadth=True,
            enable_sectors=True,
            enable_signals=True,
            enable_backtest=args.backtest,
        )

    t_pipeline = time.time()
    logger.info(
        f"Pipeline completed in {t_pipeline - t_start:.1f}s"
    )
    logger.info(result.summary())

    # ── terminal summary ────────────────────────────────────────
    if not args.quiet:
        print()
        print(result.summary())

    # ── dry-run: print scores and exit ──────────────────────────
    if args.dry_run:
        if not args.quiet:
            _print_dry_run_summary(
                result, top_n=args.top_n or 20
            )
        _finish(t_start, output_dir, log_file)
        return result

    # ── reports ─────────────────────────────────────────────────
    logger.info("")
    logger.info(
        "─── SAVING REPORTS ────────────────────────────────"
    )

    # The orchestrator already calls build_report() internally
    # and stores the result in PipelineResult.recommendation_report
    report = result.recommendation_report

    if report is None:
        logger.warning(
            "No recommendation report was generated — "
            "check pipeline logs for errors"
        )
    else:
        # Apply --top-n filter to the buy list
        if args.top_n and args.top_n > 0:
            for key in ("ranked_buys", "buy_list"):
                if key in report and isinstance(report[key], list):
                    report[key] = report[key][: args.top_n]
            logger.info(f"Filtered to top {args.top_n} buys")

        # Terminal output
        if not args.quiet:
            try:
                print()
                print_report(report)
            except Exception as e:
                logger.warning(f"print_report failed: {e}")

        # Save text report
        try:
            save_text(report, str(filenames["report_txt"]))
            logger.info(
                f"Text report  → {filenames['report_txt']}"
            )
        except Exception as e:
            logger.warning(f"save_text failed: {e}")

        # Save HTML report
        if not args.text_only:
            try:
                save_html(
                    report, str(filenames["report_html"])
                )
                logger.info(
                    f"HTML report  → {filenames['report_html']}"
                )
            except Exception as e:
                logger.warning(f"save_html failed: {e}")

        # Save JSON report
        if args.json:
            _save_json(report, str(filenames["report_json"]))
            logger.info(
                f"JSON report  → {filenames['report_json']}"
            )

    # Pipeline JSON (verbose only — for debugging)
    if args.verbose:
        _save_pipeline_result(
            result, str(filenames["pipeline_json"])
        )
        logger.info(
            f"Pipeline JSON → {filenames['pipeline_json']}"
        )

    # ── rebalance plan (if positions provided) ──────────────────
    if current_positions is not None:
        if _HAS_REBALANCE and report is not None:
            _handle_rebalance(
                args, report, current_positions,
                positions_cash, capital, filenames,
            )
        elif not _HAS_REBALANCE:
            logger.warning(
                "reports.portfolio_view not available — "
                "skipping rebalance plan"
            )
        else:
            logger.warning(
                "No report available — "
                "skipping rebalance plan"
            )
    else:
        logger.info(
            "No --positions file — "
            "skipping rebalance plan. "
            "Use --positions <file.json> to generate one."
        )

    # ── done ────────────────────────────────────────────────────
    _finish(t_start, output_dir, log_file)
    return result


def _finish(
    t_start: float, output_dir: Path, log_file: str,
) -> None:
    """Log the closing banner."""
    elapsed = time.time() - t_start
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  CASH run complete in {elapsed:.1f}s")
    logger.info(f"  Reports: {output_dir}/")
    logger.info(f"  Log:     {log_file}")
    logger.info("=" * 60)


# ═════════════════════════════════════════════════════════════════
#  PROGRAMMATIC ENTRY POINT
# ═════════════════════════════════════════════════════════════════

def run(
    portfolio_value: float | None = None,
    tickers: list[str] | None = None,
    universe_path: str | None = None,
    positions: list[dict] | None = None,
    cash_balance: float | None = None,
    output_dir: str = "output",
    save_files: bool = True,
    verbose: bool = False,
    enable_backtest: bool = False,
) -> dict:
    """
    Programmatic entry point for notebooks and scripts.

    Returns a dict with keys: ``result``, ``report``,
    ``plan``, ``filenames``.
    """
    setup_logging(verbose=verbose)

    capital = portfolio_value or PORTFOLIO_PARAMS.get(
        "total_capital", 100_000
    )

    # Resolve universe
    if tickers:
        uni = [t.upper() for t in tickers]
    elif universe_path:
        uni = load_universe_file(universe_path)
    else:
        uni = list(UNIVERSE)

    # Run
    result = run_full_pipeline(
        universe=uni,
        capital=capital,
        enable_backtest=enable_backtest,
    )

    output = {
        "result":   result,
        "report":   result.recommendation_report,
        "plan":     None,
        "filenames": {},
    }

    # Rebalance
    if (
        positions is not None
        and _HAS_REBALANCE
        and result.recommendation_report
    ):
        cb = (
            cash_balance
            if cash_balance is not None
            else capital * 0.10
        )
        try:
            plan = build_rebalance_plan(
                report=result.recommendation_report,
                current_positions=positions,
                cash_balance=cb,
                portfolio_value=capital,
            )
            output["plan"] = plan
        except Exception as e:
            logger.warning(f"Rebalance plan failed: {e}")

    # Save files
    if save_files:
        out = ensure_output_dir(output_dir)
        date_str = datetime.now().strftime("%Y%m%d")
        fnames = generate_filenames(out, date_str)

        if result.recommendation_report:
            try:
                save_text(
                    result.recommendation_report,
                    str(fnames["report_txt"]),
                )
                output["filenames"]["report_txt"] = str(
                    fnames["report_txt"]
                )
            except Exception as e:
                logger.warning(f"save_text failed: {e}")

            try:
                save_html(
                    result.recommendation_report,
                    str(fnames["report_html"]),
                )
                output["filenames"]["report_html"] = str(
                    fnames["report_html"]
                )
            except Exception as e:
                logger.warning(f"save_html failed: {e}")

        if output["plan"] and _HAS_REBALANCE:
            try:
                save_rebalance_text(
                    output["plan"],
                    str(fnames["rebalance_txt"]),
                )
                save_rebalance_html(
                    output["plan"],
                    str(fnames["rebalance_html"]),
                )
                output["filenames"]["rebalance_txt"] = str(
                    fnames["rebalance_txt"]
                )
                output["filenames"]["rebalance_html"] = str(
                    fnames["rebalance_html"]
                )
            except Exception as e:
                logger.warning(f"Rebalance save failed: {e}")

    return output


# ═════════════════════════════════════════════════════════════════
#  SERIALISATION HELPERS
# ═════════════════════════════════════════════════════════════════

def _save_json(data: dict, filepath: str) -> None:
    """Save a dict to JSON with numpy/pandas fallback."""
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=_json_serialiser)


def _save_pipeline_result(
    result: PipelineResult, filepath: str,
) -> None:
    """
    Serialise a ``PipelineResult`` to JSON for debugging.

    Large DataFrames are summarised (shape + columns) to keep
    the file size manageable.
    """
    out: dict = {}

    out["run_date"] = result.run_date.isoformat()
    out["as_of"] = (
        result.as_of.isoformat() if result.as_of else None
    )
    out["n_tickers"] = result.n_tickers
    out["n_errors"] = result.n_errors
    out["total_time"] = result.total_time
    out["timings"] = result.timings
    out["errors"] = result.errors
    out["snapshots"] = result.snapshots
    out["portfolio"] = result.portfolio

    # Summarise DataFrames instead of dumping full contents
    for name, df in [
        ("rankings", result.rankings),
        ("signals", result.signals),
        ("breadth", result.breadth),
    ]:
        if df is not None and hasattr(df, "shape"):
            out[name] = {
                "shape": list(df.shape),
                "columns": list(df.columns),
            }
        else:
            out[name] = None

    with open(filepath, "w") as f:
        json.dump(out, f, indent=2, default=_json_serialiser)


def _json_serialiser(obj):
    """Fallback JSON serialiser for numpy / pandas types."""
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