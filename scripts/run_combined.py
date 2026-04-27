#!/usr/bin/env python3
"""
run_combined.py — Run Phase 1 + Phase 2 and produce a combined signal report
=============================================================================

Usage
-----
    # Run both models for US market, print combined report
    python run_combined.py --market US

    # With holdings context + verbose
    python run_combined.py --market US --holdings NVDA,CRWD,CEG -v

    # India market
    python run_combined.py --market IN

    # Specific date
    python run_combined.py --market US --date 2025-04-25

    # Generate HTML report
    python run_combined.py --market US --html results/combined_report.html

    # Just combine existing signal files (skip re-running models)
    python run_combined.py --market US --combine-only

    # Skip one model
    python run_combined.py --market US --skip-phase1
    python run_combined.py --market US --skip-phase2

    # Custom phase1 / phase2 commands (if your paths differ)
    python run_combined.py --market US \
        --p1-cmd "python -m scripts.run_strategy top-down" \
        --p2-cmd "python refactor/runner_v2.py"
"""

import argparse
import json
import subprocess
import sys
import os
import time
import shutil
from pathlib import Path
from datetime import datetime, date, timedelta
from textwrap import dedent

# ══════════════════════════════════════════════════════════════════════
#  Path bootstrap — import PROJECT_ROOT from common.config
# ══════════════════════════════════════════════════════════════════════

_SCRIPT_DIR = Path(__file__).resolve().parent          # .../SmartMoneyRotation/scripts
_PROJECT_ROOT_BOOT = _SCRIPT_DIR.parent                # .../SmartMoneyRotation

if str(_PROJECT_ROOT_BOOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_BOOT))

from common.config import PROJECT_ROOT                 # canonical source of truth

SIGNAL_DIR   = PROJECT_ROOT / "results" / "signals"
COMBINED_DIR = PROJECT_ROOT / "results" / "combined"
REFACTOR_DIR = PROJECT_ROOT / "refactor"

# Ensure output dirs exist
SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
COMBINED_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════
#  Windows UTF-8 fix — build a subprocess env that forces utf-8
# ══════════════════════════════════════════════════════════════════════

def _subprocess_env():
    """Return a copy of os.environ with PYTHONIOENCODING=utf-8 set."""
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    return env


# ══════════════════════════════════════════════════════════════════════
#  ANSI helpers
# ══════════════════════════════════════════════════════════════════════

class S:
    """Terminal styling."""
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAG = "\033[95m"
    WHITE = "\033[97m"
    BG_GREEN = "\033[42m"
    BG_RED = "\033[41m"
    BG_YELLOW = "\033[43m"
    BG_CYAN = "\033[46m"
    R = "\033[0m"

    @staticmethod
    def strip_if_no_tty(text):
        import re
        if not sys.stdout.isatty():
            return re.sub(r"\033\[[0-9;]*m", "", text)
        return text


def _safe_print(text):
    """Print with fallback for terminals that can't handle Unicode."""
    try:
        print(text)
    except UnicodeEncodeError:
        # Strip problematic chars on old Windows consoles
        safe = text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
            sys.stdout.encoding or "utf-8"
        )
        print(safe)


def hline(char="-", width=72, color=S.DIM):
    return f"{color}{char * width}{S.R}"


def header_box(title, subtitle="", width=72):
    border = "=" * width
    lines = [
        f"{S.BOLD}{S.CYAN}{border}{S.R}",
        f"{S.BOLD}{S.WHITE}  {title}{S.R}",
    ]
    if subtitle:
        lines.append(f"{S.DIM}  {subtitle}{S.R}")
    lines.append(f"{S.BOLD}{S.CYAN}{border}{S.R}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════
#  Model runners
# ══════════════════════════════════════════════════════════════════════

def run_phase1(market, holdings=None, quality=True, verbose=False, custom_cmd=None):
    """
    Run Phase 1 (top-down RS ranking) via subprocess.
    Expects Phase 1 to write signal JSON via signal_writer.
    """
    if custom_cmd:
        parts = custom_cmd.split()
    else:
        parts = [sys.executable, "-m", "scripts.run_strategy", "full"]

    cmd = parts + ["--market", market.upper()]

    if quality and market.upper() == "US":
        if "--quality" not in cmd:
            cmd.append("--quality")
    if holdings:
        cmd.extend(["--holdings", holdings])

    _safe_print(f"\n{S.CYAN}{hline('-', 72, S.CYAN)}{S.R}")
    _safe_print(f"{S.BOLD}{S.CYAN}  > Phase 1{S.R}  |  {market.upper()}  |  full RS Ranking")
    _safe_print(f"{S.CYAN}{hline('-', 72, S.CYAN)}{S.R}")
    _safe_print(f"{S.DIM}  cmd: {' '.join(cmd)}{S.R}")
    _safe_print(f"{S.DIM}  cwd: {PROJECT_ROOT}{S.R}")

    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=not verbose,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
            timeout=300,
            env=_subprocess_env(),
        )
        elapsed = time.time() - t0

        if result.returncode != 0:
            _safe_print(f"{S.RED}  x Phase 1 failed (exit {result.returncode}){S.R}")
            if not verbose and result.stderr:
                for line in result.stderr.strip().split("\n")[-5:]:
                    _safe_print(f"    {S.DIM}{line}{S.R}")
            return None

        _safe_print(f"{S.GREEN}  ok Phase 1 completed in {elapsed:.1f}s{S.R}")
        return _find_latest("phase1", market)

    except subprocess.TimeoutExpired:
        _safe_print(f"{S.RED}  x Phase 1 timed out (300s){S.R}")
        return None
    except FileNotFoundError:
        _safe_print(f"{S.RED}  x Phase 1 command not found: {parts[0]}{S.R}")
        return None


def run_phase2(market, run_date=None, verbose=False, custom_cmd=None):
    """
    Run Phase 2 (momentum + exit-gated signals) via subprocess.
    Expects Phase 2 to write signal JSON via signal_writer.
    """
    if custom_cmd:
        parts = custom_cmd.split()
    else:
        runner_path = REFACTOR_DIR / "runner_v2.py"
        parts = [sys.executable, str(runner_path)]

    cmd = parts + ["--market", market.upper()]

    if run_date:
        cmd.extend(["--start-date", run_date, "--end-date", run_date])
    cmd.append("--print-report")
    if verbose:
        cmd.append("-v")

    _safe_print(f"\n{S.MAG}{hline('-', 72, S.MAG)}{S.R}")
    _safe_print(f"{S.BOLD}{S.MAG}  > Phase 2{S.R}  |  {market.upper()}  |  Momentum + Exit Gating")
    _safe_print(f"{S.MAG}{hline('-', 72, S.MAG)}{S.R}")
    _safe_print(f"{S.DIM}  cmd: {' '.join(cmd)}{S.R}")
    _safe_print(f"{S.DIM}  cwd: {PROJECT_ROOT}{S.R}")

    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=not verbose,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
            timeout=600,
            env=_subprocess_env(),
        )
        elapsed = time.time() - t0

        if result.returncode != 0:
            _safe_print(f"{S.RED}  x Phase 2 failed (exit {result.returncode}){S.R}")
            if not verbose and result.stderr:
                for line in result.stderr.strip().split("\n")[-5:]:
                    _safe_print(f"    {S.DIM}{line}{S.R}")
            return None

        _safe_print(f"{S.GREEN}  ok Phase 2 completed in {elapsed:.1f}s{S.R}")
        return _find_latest("phase2", market)

    except subprocess.TimeoutExpired:
        _safe_print(f"{S.RED}  x Phase 2 timed out (600s){S.R}")
        return None
    except FileNotFoundError:
        _safe_print(f"{S.RED}  x Phase 2 command not found: {parts[0]}{S.R}")
        return None


def _find_latest(phase, market):
    """Find most recent signal JSON for phase + market."""
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(SIGNAL_DIR.glob(f"{phase}_{market.upper()}_*.json"))
    return files[-1] if files else None


def load_json(path):
    if path is None or not Path(path).exists():
        return None
    with open(path) as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════════════
#  Signal combination
# ══════════════════════════════════════════════════════════════════════

def combine(p1_data, p2_data, market):
    """
    Merge Phase 1 + Phase 2 signals into a combined view.

    Returns dict with keys:
        consensus_buys   - both models say BUY
        p1_only_buys     - Phase 1 BUY, Phase 2 not BUY
        p2_only_buys     - Phase 2 BUY, Phase 1 not BUY
        consensus_sells  - both models say SELL
        p1_only_sells    - Phase 1 SELL only
        p2_only_sells    - Phase 2 SELL only
        all_tickers      - union of all tickers with merged info
        meta             - combined metadata
    """
    p1_signals = (p1_data or {}).get("signals", {})
    p2_signals = (p2_data or {}).get("signals", {})

    all_tickers = set(p1_signals.keys()) | set(p2_signals.keys())

    def get_action(signals_dict, ticker):
        sig = signals_dict.get(ticker, {})
        return sig.get("action", "NONE").upper()

    consensus_buys = []
    p1_only_buys = []
    p2_only_buys = []
    consensus_sells = []
    p1_only_sells = []
    p2_only_sells = []

    merged = {}

    for t in sorted(all_tickers):
        a1 = get_action(p1_signals, t)
        a2 = get_action(p2_signals, t)
        s1 = p1_signals.get(t, {})
        s2 = p2_signals.get(t, {})

        entry = {
            "ticker": t,
            "p1_action": a1,
            "p2_action": a2,
            "p1_score": s1.get("score"),
            "p2_score": s2.get("score"),
            "p1_rank": s1.get("rank"),
            "p2_rank": s2.get("rank"),
            "sector": s1.get("sector") or s2.get("sector"),
            "regime": s1.get("regime") or s2.get("regime"),
            "p1_notes": s1.get("notes", ""),
            "p2_notes": s2.get("notes", ""),
        }

        # compute combined score (average of available scores, normalized)
        scores = [
            v for v in [s1.get("score"), s2.get("score")] if v is not None
        ]
        entry["combined_score"] = sum(scores) / len(scores) if scores else None

        merged[t] = entry

        # classify
        if a1 == "BUY" and a2 == "BUY":
            consensus_buys.append(entry)
        elif a1 == "BUY" and a2 != "BUY":
            p1_only_buys.append(entry)
        elif a2 == "BUY" and a1 != "BUY":
            p2_only_buys.append(entry)

        if a1 == "SELL" and a2 == "SELL":
            consensus_sells.append(entry)
        elif a1 == "SELL" and a2 != "SELL":
            p1_only_sells.append(entry)
        elif a2 == "SELL" and a1 != "SELL":
            p2_only_sells.append(entry)

    # sort each bucket by combined score desc
    def sort_key(e):
        return -(e.get("combined_score") or 0)

    consensus_buys.sort(key=sort_key)
    p1_only_buys.sort(key=sort_key)
    p2_only_buys.sort(key=sort_key)

    p1_date = (p1_data or {}).get("run_date", "?")
    p2_date = (p2_data or {}).get("run_date", "?")

    return {
        "market": market.upper(),
        "p1_date": p1_date,
        "p2_date": p2_date,
        "p1_model": (p1_data or {}).get("model_name", "Phase 1"),
        "p2_model": (p2_data or {}).get("model_name", "Phase 2"),
        "p1_meta": (p1_data or {}).get("meta", {}),
        "p2_meta": (p2_data or {}).get("meta", {}),
        "consensus_buys": consensus_buys,
        "p1_only_buys": p1_only_buys,
        "p2_only_buys": p2_only_buys,
        "consensus_sells": consensus_sells,
        "p1_only_sells": p1_only_sells,
        "p2_only_sells": p2_only_sells,
        "all_tickers": merged,
        "generated_at": datetime.now().isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════
#  Terminal report
# ══════════════════════════════════════════════════════════════════════

def _fmt_score(val, width=7):
    if val is None:
        return f"{'---':>{width}}"
    return f"{val:>{width}.3f}"


def _fmt_rank(val, width=5):
    if val is None:
        return f"{'---':>{width}}"
    return f"{'#' + str(val):>{width}}"


def _fmt_action(action):
    if action == "BUY":
        return f"{S.GREEN}BUY {S.R}"
    elif action == "SELL":
        return f"{S.RED}SELL{S.R}"
    elif action == "HOLD":
        return f"{S.YELLOW}HOLD{S.R}"
    return f"{S.DIM}{action:4}{S.R}"


def print_signal_table(entries, title, icon, color, show_actions=False, max_rows=30):
    """Print a section of the combined report."""
    n = len(entries)
    _safe_print(f"\n  {color}{S.BOLD}{icon} {title}{S.R}  {S.DIM}({n} name{'s' if n != 1 else ''}){S.R}")
    _safe_print(f"  {hline('-', 68, S.DIM)}")

    if not entries:
        _safe_print(f"  {S.DIM}  (none){S.R}")
        return

    # header
    hdr = f"  {'Ticker':<16}"
    if show_actions:
        hdr += f"{'P1':>6} {'P2':>6}  "
    hdr += f"{'P1 Scr':>8} {'P2 Scr':>8} {'Combo':>8} {'P1 Rnk':>7} {'Sector':<14}"
    _safe_print(f"  {S.DIM}{hdr.strip()}{S.R}")
    _safe_print(f"  {hline('.', 68, S.DIM)}")

    for i, e in enumerate(entries[:max_rows]):
        ticker = e["ticker"]
        line = f"  {S.BOLD}{ticker:<16}{S.R}"
        if show_actions:
            line += f"{_fmt_action(e['p1_action'])}  {_fmt_action(e['p2_action'])}  "
        line += (
            f"{_fmt_score(e.get('p1_score'))}"
            f" {_fmt_score(e.get('p2_score'))}"
            f" {_fmt_score(e.get('combined_score'))}"
            f" {_fmt_rank(e.get('p1_rank'))}"
            f"  {S.DIM}{(e.get('sector') or ''):14}{S.R}"
        )
        _safe_print(line)

    if n > max_rows:
        _safe_print(f"  {S.DIM}  ... and {n - max_rows} more{S.R}")


def print_combined_report(combined):
    """Pretty-print the full combined report to terminal."""
    mkt = combined["market"]
    p1d = combined["p1_date"]
    p2d = combined["p2_date"]
    p1m = combined["p1_model"]
    p2m = combined["p2_model"]

    n_consensus = len(combined["consensus_buys"])
    n_p1 = len(combined["p1_only_buys"])
    n_p2 = len(combined["p2_only_buys"])
    total_buys = n_consensus + n_p1 + n_p2

    _safe_print("\n")
    _safe_print(header_box(
        f"COMBINED SIGNAL REPORT  |  {mkt} Market",
        f"Phase 1: {p1m} ({p1d})  .  Phase 2: {p2m} ({p2d})"
    ))

    # summary bar
    _safe_print(f"\n  {S.BOLD}Summary{S.R}")
    _safe_print(f"  {hline('-', 68, S.DIM)}")
    p1_meta = combined["p1_meta"]
    p2_meta = combined["p2_meta"]
    _safe_print(
        f"  Phase 1:  "
        f"{S.GREEN}{p1_meta.get('n_buys', '?')} buys{S.R}  "
        f"{S.RED}{p1_meta.get('n_sells', '?')} sells{S.R}  "
        f"{S.DIM}({p1_meta.get('universe_size', '?')} universe){S.R}"
    )
    _safe_print(
        f"  Phase 2:  "
        f"{S.GREEN}{p2_meta.get('n_buys', '?')} buys{S.R}  "
        f"{S.RED}{p2_meta.get('n_sells', '?')} sells{S.R}  "
        f"{S.DIM}({p2_meta.get('universe_size', '?')} universe){S.R}"
    )
    _safe_print(
        f"\n  {S.BOLD}{S.GREEN}* Consensus BUYs: {n_consensus}{S.R}"
        f"   {S.CYAN}Phase 1 only: {n_p1}{S.R}"
        f"   {S.MAG}Phase 2 only: {n_p2}{S.R}"
        f"   {S.DIM}Total unique buys: {total_buys}{S.R}"
    )

    # consensus buys (highest conviction)
    print_signal_table(
        combined["consensus_buys"],
        "CONSENSUS BUYS -- Both Models Agree (Highest Conviction)",
        "*", S.GREEN,
    )

    # phase 1 only
    print_signal_table(
        combined["p1_only_buys"],
        "PHASE 1 ONLY BUYS -- Top-Down RS",
        ">", S.CYAN,
    )

    # phase 2 only
    print_signal_table(
        combined["p2_only_buys"],
        "PHASE 2 ONLY BUYS -- Momentum / Exit-Gated",
        ">", S.MAG,
    )

    # sells
    all_sells = (
        combined["consensus_sells"]
        + combined["p1_only_sells"]
        + combined["p2_only_sells"]
    )
    if all_sells:
        print_signal_table(
            all_sells,
            "SELL SIGNALS (Either Model)",
            "v", S.RED,
            show_actions=True,
            max_rows=20,
        )

    _safe_print(f"\n{hline('=', 72, S.DIM)}\n")


# ══════════════════════════════════════════════════════════════════════
#  HTML report generation
# ══════════════════════════════════════════════════════════════════════

def generate_html(combined, output_path):
    """Generate a self-contained HTML report."""
    mkt = combined["market"]
    p1d = combined["p1_date"]
    p2d = combined["p2_date"]
    generated = combined["generated_at"][:19]

    def ticker_rows(entries, section_class=""):
        if not entries:
            return '<tr><td colspan="7" class="empty">No signals</td></tr>'
        rows = []
        for e in entries:
            p1s = f"{e['p1_score']:.3f}" if e.get("p1_score") is not None else "---"
            p2s = f"{e['p2_score']:.3f}" if e.get("p2_score") is not None else "---"
            cs = f"{e['combined_score']:.3f}" if e.get("combined_score") is not None else "---"
            rk = f"#{e['p1_rank']}" if e.get("p1_rank") is not None else "---"
            sect = e.get("sector") or "---"
            rows.append(
                f'<tr class="{section_class}">'
                f'<td class="ticker">{e["ticker"]}</td>'
                f'<td class="action {e["p1_action"].lower()}">{e["p1_action"]}</td>'
                f'<td class="action {e["p2_action"].lower()}">{e["p2_action"]}</td>'
                f"<td>{p1s}</td><td>{p2s}</td><td>{cs}</td>"
                f'<td class="sector">{sect}</td>'
                f"</tr>"
            )
        return "\n".join(rows)

    n_cons = len(combined["consensus_buys"])
    n_p1 = len(combined["p1_only_buys"])
    n_p2 = len(combined["p2_only_buys"])
    all_sells = (
        combined["consensus_sells"]
        + combined["p1_only_sells"]
        + combined["p2_only_sells"]
    )

    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Combined Signal Report - {mkt}</title>
<style>
  :root {{
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --dim: #8b949e; --green: #3fb950;
    --red: #f85149; --cyan: #58a6ff; --mag: #d2a8ff;
    --yellow: #d29922; --gold: #e3b341;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', monospace;
    background: var(--bg); color: var(--text); padding: 24px;
    max-width: 1100px; margin: 0 auto; font-size: 13px;
  }}
  .header {{
    border: 1px solid var(--border); border-radius: 8px;
    padding: 24px; margin-bottom: 24px; background: var(--surface);
    text-align: center;
  }}
  .header h1 {{ font-size: 20px; margin-bottom: 8px; }}
  .header .sub {{ color: var(--dim); font-size: 12px; }}
  .stats {{
    display: flex; gap: 16px; justify-content: center;
    flex-wrap: wrap; margin: 20px 0 0;
  }}
  .stat {{
    padding: 12px 20px; border-radius: 6px; text-align: center;
    border: 1px solid var(--border); background: var(--bg);
    min-width: 140px;
  }}
  .stat .num {{ font-size: 28px; font-weight: bold; }}
  .stat .lbl {{ font-size: 11px; color: var(--dim); margin-top: 4px; }}
  .stat.consensus .num {{ color: var(--gold); }}
  .stat.p1 .num {{ color: var(--cyan); }}
  .stat.p2 .num {{ color: var(--mag); }}
  .stat.sell .num {{ color: var(--red); }}

  .section {{
    border: 1px solid var(--border); border-radius: 8px;
    margin-bottom: 16px; overflow: hidden;
  }}
  .section-header {{
    padding: 12px 16px; font-weight: bold; font-size: 13px;
    display: flex; justify-content: space-between; align-items: center;
  }}
  .section-header .count {{
    font-weight: normal; color: var(--dim); font-size: 12px;
  }}
  .consensus .section-header {{ background: #1a2a1a; border-bottom: 1px solid #2a4a2a; color: var(--gold); }}
  .p1only .section-header {{ background: #152238; border-bottom: 1px solid #1e3a5f; color: var(--cyan); }}
  .p2only .section-header {{ background: #1f1633; border-bottom: 1px solid #352a5c; color: var(--mag); }}
  .sells .section-header {{ background: #2a1515; border-bottom: 1px solid #4a2020; color: var(--red); }}

  table {{ width: 100%; border-collapse: collapse; }}
  th {{
    text-align: left; padding: 8px 12px; font-size: 11px;
    color: var(--dim); background: var(--surface);
    border-bottom: 1px solid var(--border); font-weight: normal;
    text-transform: uppercase; letter-spacing: 0.5px;
  }}
  td {{
    padding: 7px 12px; border-bottom: 1px solid var(--border);
    font-size: 12px;
  }}
  tr:hover {{ background: rgba(255,255,255,0.03); }}
  .ticker {{ font-weight: bold; }}
  .action.buy {{ color: var(--green); font-weight: bold; }}
  .action.sell {{ color: var(--red); }}
  .action.hold {{ color: var(--yellow); }}
  .action.none {{ color: var(--dim); }}
  .sector {{ color: var(--dim); }}
  .empty {{ text-align: center; color: var(--dim); padding: 20px; }}
  .footer {{ text-align: center; color: var(--dim); font-size: 11px; margin-top: 24px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Combined Signal Report &mdash; {mkt}</h1>
  <div class="sub">
    Phase 1: {combined['p1_model']} ({p1d}) &nbsp;&middot;&nbsp;
    Phase 2: {combined['p2_model']} ({p2d})
  </div>
  <div class="stats">
    <div class="stat consensus">
      <div class="num">{n_cons}</div>
      <div class="lbl">&#9733; Consensus BUYs</div>
    </div>
    <div class="stat p1">
      <div class="num">{n_p1}</div>
      <div class="lbl">Phase 1 Only</div>
    </div>
    <div class="stat p2">
      <div class="num">{n_p2}</div>
      <div class="lbl">Phase 2 Only</div>
    </div>
    <div class="stat sell">
      <div class="num">{len(all_sells)}</div>
      <div class="lbl">&#9660; Sell Signals</div>
    </div>
  </div>
</div>

<div class="section consensus">
  <div class="section-header">
    <span>&#9733; Consensus BUYs &mdash; Both Models Agree</span>
    <span class="count">{n_cons} names</span>
  </div>
  <table>
    <thead><tr><th>Ticker</th><th>P1</th><th>P2</th>
    <th>P1 Score</th><th>P2 Score</th><th>Combined</th><th>Sector</th></tr></thead>
    <tbody>{ticker_rows(combined['consensus_buys'], 'consensus-row')}</tbody>
  </table>
</div>

<div class="section p1only">
  <div class="section-header">
    <span>&#9650; Phase 1 Only BUYs &mdash; Top-Down RS</span>
    <span class="count">{n_p1} names</span>
  </div>
  <table>
    <thead><tr><th>Ticker</th><th>P1</th><th>P2</th>
    <th>P1 Score</th><th>P2 Score</th><th>Combined</th><th>Sector</th></tr></thead>
    <tbody>{ticker_rows(combined['p1_only_buys'])}</tbody>
  </table>
</div>

<div class="section p2only">
  <div class="section-header">
    <span>&#9650; Phase 2 Only BUYs &mdash; Momentum / Exit-Gated</span>
    <span class="count">{n_p2} names</span>
  </div>
  <table>
    <thead><tr><th>Ticker</th><th>P1</th><th>P2</th>
    <th>P1 Score</th><th>P2 Score</th><th>Combined</th><th>Sector</th></tr></thead>
    <tbody>{ticker_rows(combined['p2_only_buys'])}</tbody>
  </table>
</div>

<div class="section sells">
  <div class="section-header">
    <span>&#9660; Sell Signals</span>
    <span class="count">{len(all_sells)} names</span>
  </div>
  <table>
    <thead><tr><th>Ticker</th><th>P1</th><th>P2</th>
    <th>P1 Score</th><th>P2 Score</th><th>Combined</th><th>Sector</th></tr></thead>
    <tbody>{ticker_rows(all_sells)}</tbody>
  </table>
</div>

<div class="footer">Generated {generated} &nbsp;&middot;&nbsp; run_combined.py</div>
</body>
</html>"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    _safe_print(f"\n  {S.GREEN}ok HTML report saved: {output_path}{S.R}")


# ══════════════════════════════════════════════════════════════════════
#  Save combined JSON
# ══════════════════════════════════════════════════════════════════════

def save_combined_json(combined, output_dir=None):
    """Write combined result as JSON for downstream use."""
    out = Path(output_dir) if output_dir else COMBINED_DIR
    out.mkdir(parents=True, exist_ok=True)

    mkt = combined["market"]
    d = combined.get("p2_date") or combined.get("p1_date") or "unknown"
    path = out / f"combined_{mkt}_{d}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, default=str)

    _safe_print(f"  {S.GREEN}ok Combined JSON saved: {path}{S.R}")
    return path


# ══════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="Run Phase 1 + Phase 2 models and produce a combined signal report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent("""\
        Examples:
          python run_combined.py --market US
          python run_combined.py --market US --holdings NVDA,CRWD,CEG -v
          python run_combined.py --market IN --date 2025-04-25
          python run_combined.py --market US --html results/combined.html
          python run_combined.py --market US --combine-only
        """),
    )
    p.add_argument("--market", "-m", required=True, help="Market: US, IN, HK")
    p.add_argument("--date", "-d", default=None, help="Run date (YYYY-MM-DD). Default: latest available")
    p.add_argument("--holdings", default=None, help="Comma-separated current holdings (for Phase 1)")
    p.add_argument("--html", default=None, help="Path for HTML report output")
    p.add_argument("--open", action="store_true", help="Open HTML report in browser")
    p.add_argument("-v", "--verbose", action="store_true", help="Show subprocess output")
    p.add_argument("--no-quality", action="store_true", help="Skip quality filter in Phase 1")
    p.add_argument("--combine-only", action="store_true", help="Skip running models, just combine existing signals")
    p.add_argument("--skip-phase1", action="store_true", help="Skip Phase 1, only run Phase 2")
    p.add_argument("--skip-phase2", action="store_true", help="Skip Phase 2, only run Phase 1")
    p.add_argument("--p1-cmd", default=None, help="Custom Phase 1 command (before --market)")
    p.add_argument("--p2-cmd", default=None, help="Custom Phase 2 command (before --market)")
    p.add_argument("--signal-dir", default=None, help="Override signal directory")
    return p.parse_args()


def main():
    args = parse_args()
    market = args.market.upper()

    global SIGNAL_DIR
    if args.signal_dir:
        SIGNAL_DIR = Path(args.signal_dir)

    t_start = time.time()

    _safe_print(header_box(
        f"Combined Model Runner  |  {market}",
        f"Date: {args.date or 'latest'}  .  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))
    _safe_print(f"  {S.DIM}Project root: {PROJECT_ROOT}{S.R}")

    p1_path = None
    p2_path = None

    # -- Run models --
    if not args.combine_only:
        if not args.skip_phase1:
            p1_path = run_phase1(
                market,
                holdings=args.holdings,
                quality=not args.no_quality,
                verbose=args.verbose,
                custom_cmd=args.p1_cmd,
            )
        if not args.skip_phase2:
            p2_path = run_phase2(
                market,
                run_date=args.date,
                verbose=args.verbose,
                custom_cmd=args.p2_cmd,
            )
    else:
        _safe_print(f"\n  {S.YELLOW}--combine-only: loading existing signal files{S.R}")

    # -- Load signals --
    if p1_path is None:
        p1_path = _find_latest("phase1", market)
    if p2_path is None:
        p2_path = _find_latest("phase2", market)

    p1_data = load_json(p1_path)
    p2_data = load_json(p2_path)

    if p1_data is None and p2_data is None:
        _safe_print(f"\n{S.RED}  x No signal files found for {market}. Run at least one model first.{S.R}")
        _safe_print(f"  {S.DIM}  Expected files in: {SIGNAL_DIR}{S.R}")
        sys.exit(1)

    if p1_data:
        _safe_print(f"\n  {S.CYAN}Phase 1 signals: {p1_path}{S.R}")
    else:
        _safe_print(f"\n  {S.YELLOW}Phase 1 signals: not available{S.R}")

    if p2_data:
        _safe_print(f"  {S.MAG}Phase 2 signals: {p2_path}{S.R}")
    else:
        _safe_print(f"  {S.YELLOW}Phase 2 signals: not available{S.R}")

    # -- Combine --
    combined = combine(p1_data, p2_data, market)

    # -- Output --
    print_combined_report(combined)
    save_combined_json(combined)

    if args.html or args.open:
        html_path = args.html or str(
            COMBINED_DIR / f"combined_{market}_{combined.get('p2_date', 'latest')}.html"
        )
        generate_html(combined, html_path)
        if args.open:
            import webbrowser
            webbrowser.open(f"file://{Path(html_path).resolve()}")

    elapsed = time.time() - t_start
    _safe_print(f"  {S.DIM}Total time: {elapsed:.1f}s{S.R}\n")


if __name__ == "__main__":
    main()