"""
reports/weekly_report.py
Weekly report wrapper for the CASH pipeline.

Runs the standard pipeline, saves output with ISO-week filenames,
and optionally compares against the previous week's JSON to
surface new / removed positions and regime changes.

Usage — programmatic::

    from reports.weekly_report import generate_weekly_report
    report = generate_weekly_report()

Usage — command-line::

    python -m reports.weekly_report
    python -m reports.weekly_report --capital 200000
    python -m reports.weekly_report --output-dir output/weekly
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from common.config import LOGS_DIR, UNIVERSE, PORTFOLIO_PARAMS
from pipeline.orchestrator import run_full_pipeline
from reports.recommendations import (
    build_report,
    save_text,
    save_html,
    to_text,
)

logger = logging.getLogger(__name__)

__all__ = [
    "generate_weekly_report",
    "load_previous_week",
    "compare_weeks",
    "weekly_diff_text",
]


# ═════════════════════════════════════════════════════════════════
#  GENERATE
# ═════════════════════════════════════════════════════════════════

def generate_weekly_report(
    universe: list[str] | None = None,
    capital: float | None = None,
    output_dir: str = "output/weekly",
    save: bool = True,
    include_diff: bool = True,
) -> dict:
    """
    Run the full pipeline and save a weekly report.

    Parameters
    ----------
    universe : list[str], optional
        Ticker list.  Defaults to ``UNIVERSE`` from config.
    capital : float, optional
        Portfolio value.  Defaults to config.
    output_dir : str
        Where to write the weekly files.
    save : bool
        Write files to disk.
    include_diff : bool
        Append a week-over-week diff section if a previous
        weekly JSON is found.

    Returns
    -------
    dict
        The structured report (same shape as ``build_report``
        output), with an extra ``"weekly_diff"`` key when
        *include_diff* is True and a previous week exists.
    """
    uni = universe or list(UNIVERSE)
    cap = capital or PORTFOLIO_PARAMS.get("total_capital", 100_000)

    logger.info(
        f"Weekly report: {len(uni)} tickers, ${cap:,.0f}"
    )

    # ── pipeline ────────────────────────────────────────────────
    result = run_full_pipeline(
        universe=uni,
        capital=cap,
        enable_breadth=True,
        enable_sectors=True,
        enable_signals=True,
        enable_backtest=False,
    )

    report = result.recommendation_report
    if report is None:
        report = build_report(result)

    # ── week-over-week diff ─────────────────────────────────────
    if include_diff:
        prev = load_previous_week(output_dir)
        if prev is not None:
            diff = compare_weeks(report, prev)
            report["weekly_diff"] = diff
            logger.info(
                f"Week diff: {len(diff['new_buys'])} new buys, "
                f"{len(diff['removed_buys'])} removed, "
                f"regime_changed={diff['regime_change']}"
            )
        else:
            report["weekly_diff"] = None
            logger.info("No previous weekly report found for diff")

    # ── save ────────────────────────────────────────────────────
    if save:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        week_str = datetime.now().strftime("%Y_W%V")
        date_str = datetime.now().strftime("%Y%m%d")

        txt_path  = out / f"weekly_{week_str}_{date_str}.txt"
        html_path = out / f"weekly_{week_str}_{date_str}.html"
        json_path = out / f"weekly_{week_str}_{date_str}.json"

        # Text — append diff section if available
        text = to_text(report)
        if report.get("weekly_diff"):
            text += "\n" + weekly_diff_text(report["weekly_diff"])
        with open(txt_path, "w") as f:
            f.write(text)
        logger.info(f"Weekly text  → {txt_path}")

        save_html(report, str(html_path))
        logger.info(f"Weekly HTML  → {html_path}")

        _save_json(report, str(json_path))
        logger.info(f"Weekly JSON  → {json_path}")

    return report


# ═════════════════════════════════════════════════════════════════
#  WEEK-OVER-WEEK COMPARISON
# ═════════════════════════════════════════════════════════════════

def load_previous_week(
    output_dir: str = "output/weekly",
) -> dict | None:
    """
    Load the most recent *previous* weekly JSON.

    Looks in *output_dir* for ``weekly_*.json`` files,
    sorts descending, and returns the second-newest
    (the newest is assumed to be the current run or an
    in-progress write).
    """
    out = Path(output_dir)
    if not out.exists():
        return None

    files = sorted(out.glob("weekly_*.json"), reverse=True)
    # Need at least one completed previous file
    target = files[1] if len(files) >= 2 else (
        files[0] if len(files) == 1 else None
    )
    if target is None:
        return None

    try:
        with open(target, "r") as f:
            data = json.load(f)
        logger.info(f"Previous week loaded: {target.name}")
        return data
    except (json.JSONDecodeError, IOError) as exc:
        logger.warning(f"Could not load previous week: {exc}")
        return None


def compare_weeks(
    current: dict, previous: dict,
) -> dict:
    """
    Compare two weekly report dicts and summarise changes.

    Returns
    -------
    dict with keys:
        new_buys, removed_buys, new_sells, removed_sells,
        regime_change, prev_regime, curr_regime,
        buy_count_delta, sell_count_delta
    """
    curr_buys = {
        b["ticker"] for b in current.get("buy_list", [])
    }
    prev_buys = {
        b["ticker"] for b in previous.get("buy_list", [])
    }
    curr_sells = {
        s["ticker"] for s in current.get("sell_list", [])
    }
    prev_sells = {
        s["ticker"] for s in previous.get("sell_list", [])
    }

    curr_regime = (
        current.get("header", {}).get("regime", "unknown")
    )
    prev_regime = (
        previous.get("header", {}).get("regime", "unknown")
    )

    return {
        "new_buys":         sorted(curr_buys - prev_buys),
        "removed_buys":     sorted(prev_buys - curr_buys),
        "retained_buys":    sorted(curr_buys & prev_buys),
        "new_sells":        sorted(curr_sells - prev_sells),
        "removed_sells":    sorted(prev_sells - curr_sells),
        "regime_change":    curr_regime != prev_regime,
        "prev_regime":      prev_regime,
        "curr_regime":      curr_regime,
        "buy_count_delta":  len(curr_buys) - len(prev_buys),
        "sell_count_delta": len(curr_sells) - len(prev_sells),
    }


def weekly_diff_text(diff: dict) -> str:
    """Render the week-over-week diff as a plain-text section."""
    lines = [
        "",
        "─── WEEK-OVER-WEEK CHANGES "
        "──────────────────────────────────────────",
    ]

    if diff["regime_change"]:
        lines.append(
            f"  ⚠ REGIME CHANGED: "
            f"{diff['prev_regime'].upper()} → "
            f"{diff['curr_regime'].upper()}"
        )
    else:
        lines.append(
            f"  Regime unchanged: {diff['curr_regime'].upper()}"
        )

    lines.append("")
    if diff["new_buys"]:
        lines.append(
            f"  NEW buys ({len(diff['new_buys'])}):     "
            + ", ".join(diff["new_buys"])
        )
    if diff["removed_buys"]:
        lines.append(
            f"  REMOVED buys ({len(diff['removed_buys'])}): "
            + ", ".join(diff["removed_buys"])
        )
    if diff["retained_buys"]:
        lines.append(
            f"  Retained buys ({len(diff['retained_buys'])}): "
            + ", ".join(diff["retained_buys"])
        )

    if diff["new_sells"]:
        lines.append(
            f"  NEW sells ({len(diff['new_sells'])}):    "
            + ", ".join(diff["new_sells"])
        )
    if diff["removed_sells"]:
        lines.append(
            f"  REMOVED sells ({len(diff['removed_sells'])}): "
            + ", ".join(diff["removed_sells"])
        )

    lines.append(
        f"  Buy count delta:  {diff['buy_count_delta']:+d}    "
        f"Sell count delta: {diff['sell_count_delta']:+d}"
    )
    lines.append("─" * 72)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  JSON SERIALISATION
# ═════════════════════════════════════════════════════════════════

def _save_json(data: dict, filepath: str) -> None:
    """Save report dict to JSON with numpy/pandas fallback."""
    import numpy as np

    def _ser(obj: Any) -> Any:
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=_ser)


# ═════════════════════════════════════════════════════════════════
#  CLI
# ═════════════════════════════════════════════════════════════════

def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CASH — Weekly report generator",
    )
    p.add_argument(
        "--capital", type=float, default=None,
        help="Portfolio value (overrides config)",
    )
    p.add_argument(
        "--output-dir", type=str, default="output/weekly",
        help="Directory for weekly reports",
    )
    p.add_argument(
        "--no-diff", action="store_true",
        help="Skip week-over-week comparison",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Debug logging",
    )
    return p


def _cli_main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)-24s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    report = generate_weekly_report(
        capital=args.capital,
        output_dir=args.output_dir,
        include_diff=not args.no_diff,
    )

    snap = report.get("portfolio_snapshot", {})
    print(
        f"\nWeekly report complete: "
        f"{snap.get('buy_count', 0)} buys, "
        f"{snap.get('sell_count', 0)} sells"
    )

    diff = report.get("weekly_diff")
    if diff:
        print(weekly_diff_text(diff))


if __name__ == "__main__":
    _cli_main()