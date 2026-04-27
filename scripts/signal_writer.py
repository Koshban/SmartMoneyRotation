#!/usr/bin/env python3
"""
signal_writer.py — Standardized signal output for multi-model pipeline
======================================================================

Both Phase 1 and Phase 2 import this to write signals in a common format.
The combiner (run_combined.py) reads these to produce a unified report.

Usage in your model:
    from signal_writer import write_signals

    write_signals(
        phase="phase1",
        market="US",
        run_date="2025-04-25",
        signals={"NVDA": {"action": "BUY", "score": 0.95, "rank": 1}, ...},
        model_name="Top-Down RS + Quality",
    )
"""

import json
from pathlib import Path
from datetime import date, datetime

SIGNAL_DIR = Path("results") / "signals"


def write_signals(
    phase: str,
    market: str,
    run_date,
    signals: dict,
    model_name: str = "",
    meta: dict = None,
    output_dir=None,
) -> Path:
    """
    Write standardized signal JSON.

    Parameters
    ----------
    phase : str          "phase1" or "phase2"
    market : str         "US", "IN", "HK"
    run_date : date|str  Trading date these signals apply to
    signals : dict       {ticker: {"action": "BUY"|"SELL"|"HOLD", "score": float, ...}}
    model_name : str     Human-readable model description
    meta : dict          Extra metadata (universe size, runtime, config, etc.)
    output_dir : Path    Override default results/signals/

    Returns
    -------
    Path to the written JSON file

    Expected signal dict per ticker
    --------------------------------
    {
        "action":   "BUY" | "SELL" | "HOLD",
        "score":    0.85,           # composite score (0-1 or raw)
        "rank":     3,              # rank among buys, 1 = best
        "rs_rank":  5,              # relative-strength percentile rank
        "sector":   "Technology",   # optional
        "regime":   "bull",         # optional: bull / bear / neutral
        "notes":    "RS top-10 ..."  # optional free-text
    }
    """
    out_dir = Path(output_dir) if output_dir else SIGNAL_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(run_date, (date, datetime)):
        run_date_str = run_date.strftime("%Y-%m-%d")
    else:
        run_date_str = str(run_date)

    # count by action
    counts = {}
    for sig in signals.values():
        a = sig.get("action", "HOLD").upper()
        counts[a] = counts.get(a, 0) + 1

    payload = {
        "phase": phase,
        "market": market.upper(),
        "run_date": run_date_str,
        "model_name": model_name or f"{phase} model",
        "generated_at": datetime.now().isoformat(),
        "signals": signals,
        "meta": {
            "universe_size": len(signals),
            "n_buys": counts.get("BUY", 0),
            "n_sells": counts.get("SELL", 0),
            "n_holds": counts.get("HOLD", 0),
            **(meta or {}),
        },
    }

    filename = f"{phase}_{market.upper()}_{run_date_str}.json"
    path = out_dir / filename

    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    return path


def read_signals(path) -> dict:
    """Read a signal JSON file."""
    with open(path) as f:
        return json.load(f)


def find_latest_signal(phase: str, market: str, signal_dir=None) -> Path:
    """Find the most recent signal file for a phase + market."""
    d = Path(signal_dir) if signal_dir else SIGNAL_DIR
    if not d.exists():
        return None
    pattern = f"{phase}_{market.upper()}_*.json"
    files = sorted(d.glob(pattern))
    return files[-1] if files else None


def list_available(signal_dir=None):
    """List all available signal files grouped by market & phase."""
    d = Path(signal_dir) if signal_dir else SIGNAL_DIR
    if not d.exists():
        return {}
    result = {}
    for f in sorted(d.glob("*.json")):
        data = read_signals(f)
        key = (data.get("market", "?"), data.get("phase", "?"))
        result.setdefault(key, []).append(
            {"path": str(f), "date": data.get("run_date"), "buys": data["meta"]["n_buys"]}
        )
    return result