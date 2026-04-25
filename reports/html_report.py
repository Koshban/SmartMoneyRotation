"""
reports/html_report.py
──────────────────────
Self-contained HTML recommendation report for CASH pipeline.

Produces a dark-themed responsive dashboard:
  • Summary cards with signal counts
  • Strong-buy highlight cards
  • Per-category signal tables with scores, reasons, sector data
  • Interactive sort, collapse, and ticker search
  • No external JS/CSS dependencies (Google Fonts optional)

Usage
─────
    from reports.html_report import generate_html_report
    html = generate_html_report(pipeline_result)
    Path("report.html").write_text(html)
"""

from __future__ import annotations

import html as _html
from datetime import datetime
from typing import Any

from strategy_phase1.convergence import (
    MarketSignalResult,
    ConvergedSignal,
    STRONG_BUY,
    BUY_SCORING,
    BUY_ROTATION,
    CONFLICT,
    HOLD,
    NEUTRAL,
    SELL_SCORING,
    SELL_ROTATION,
    STRONG_SELL,
)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _esc(s: Any) -> str:
    if s is None:
        return "—"
    return _html.escape(str(s))


def _pct(v: float) -> str:
    return f"{max(0, min(100, v * 100)):.0f}"


_LABEL_COLORS = {
    STRONG_BUY:    ("#238636", "#3fb950"),
    BUY_SCORING:   ("#1a7f37", "#56d364"),
    BUY_ROTATION:  ("#0d5524", "#7ee787"),
    CONFLICT:      ("#7a4e05", "#d29922"),
    HOLD:          ("#30363d", "#8b949e"),
    NEUTRAL:       ("#30363d", "#8b949e"),
    SELL_SCORING:  ("#8b2c22", "#f85149"),
    SELL_ROTATION: ("#6e1d18", "#ff7b72"),
    STRONG_SELL:   ("#b62324", "#ff7b72"),
}

_LABEL_DISPLAY = {
    STRONG_BUY:    "Strong Buy",
    BUY_SCORING:   "Buy (Scoring)",
    BUY_ROTATION:  "Buy (Rotation)",
    CONFLICT:      "Conflict",
    HOLD:          "Hold",
    NEUTRAL:       "Neutral",
    SELL_SCORING:  "Sell (Scoring)",
    SELL_ROTATION: "Sell (Rotation)",
    STRONG_SELL:   "Strong Sell",
}

_SECTION_ORDER = [
    (STRONG_BUY,    "🟢🟢", "Strong Buy",       "Both engines agree — highest conviction"),
    (BUY_SCORING,   "🟢",   "Buy — Scoring",    "Strong individual profile; sector not leading"),
    (BUY_ROTATION,  "🟢",   "Buy — Rotation",   "In a leading sector; individual metrics not yet confirmed"),
    (CONFLICT,      "⚠️",    "Conflict",          "Engines disagree — review manually"),
    (SELL_SCORING,  "🔴",   "Sell — Scoring",   "Weak individual profile"),
    (SELL_ROTATION, "🔴",   "Sell — Rotation",  "Sector lagging; consider exit"),
    (STRONG_SELL,   "🔴🔴", "Strong Sell",       "Both engines agree — highest conviction exit"),
    (HOLD,          "⚪",   "Hold",              "No actionable signal"),
    (NEUTRAL,       "⚪",   "Neutral",           "Insufficient data or edge"),
]


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY
# ═══════════════════════════════════════════════════════════════

def generate_html_report(
    pipeline_result: Any,
    title: str | None = None,
) -> str:
    """
    Generate a complete self-contained HTML report.

    Parameters
    ----------
    pipeline_result : PipelineResult
        Must have ``.convergence`` (MarketSignalResult) set.
    title : str, optional
        Page title override.

    Returns
    -------
    str
        Complete HTML document.
    """
    conv: MarketSignalResult | None = getattr(
        pipeline_result, "convergence", None
    )
    market   = getattr(pipeline_result, "market", "US")
    timings  = getattr(pipeline_result, "timings", {})
    run_date = getattr(pipeline_result, "run_date", None)
    n_errors = getattr(pipeline_result, "n_errors", 0)

    if conv is None:
        return _error_page("No convergence data — pipeline may have failed.")

    date_str = (
        run_date.strftime("%B %d, %Y at %H:%M")
        if run_date
        else datetime.now().strftime("%B %d, %Y at %H:%M")
    )
    has_rotation = "rotation" in timings
    total_time   = sum(timings.values()) if timings else 0.0
    page_title   = title or f"CASH — {market} Recommendations"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(page_title)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
{_CSS}
</style>
</head>
<body>

{_header_html(market, date_str, conv, n_errors, total_time, has_rotation)}

<main class="container">

  <div class="toolbar">
    <input type="text" id="search-input" class="search-box"
           placeholder="Search ticker…" oninput="filterAll(this.value)">
    <button class="btn" onclick="expandAll()">Expand All</button>
    <button class="btn" onclick="collapseAll()">Collapse All</button>
  </div>

  {_summary_cards_html(conv)}
  {_all_sections_html(conv, has_rotation)}

  <footer class="footer">
    <p>Generated {_esc(date_str)} &nbsp;·&nbsp; CASH Pipeline v2.0</p>
    {_timings_html(timings)}
    <p class="muted">For informational purposes only. Not financial advice.</p>
  </footer>
</main>

<script>
{_JS}
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════
#  HEADER
# ═══════════════════════════════════════════════════════════════

def _header_html(
    market: str,
    date_str: str,
    conv: MarketSignalResult,
    n_errors: int,
    total_time: float,
    has_rotation: bool,
) -> str:
    engines = "Scoring + Rotation" if has_rotation else "Scoring Only"
    err_badge = (
        f'<span class="header-badge badge-err">{n_errors} errors</span>'
        if n_errors > 0 else ""
    )

    return f"""
<header class="header">
  <div class="container header-inner">
    <div>
      <h1 class="logo">CASH
        <span class="market-badge">{_esc(market)}</span>
      </h1>
      <p class="subtitle">Convergence Analysis &amp; Signal Hierarchy</p>
    </div>
    <div class="header-meta">
      <span class="header-badge">{_esc(engines)}</span>
      <span class="header-badge">{conv.n_tickers} tickers</span>
      <span class="header-badge">{total_time:.1f}s</span>
      {err_badge}
      <div class="header-date">{_esc(date_str)}</div>
    </div>
  </div>
</header>"""


# ═══════════════════════════════════════════════════════════════
#  SUMMARY CARDS
# ═══════════════════════════════════════════════════════════════

def _summary_cards_html(conv: MarketSignalResult) -> str:
    counts: dict[str, int] = {}
    for s in conv.signals:
        counts[s.convergence_label] = counts.get(s.convergence_label, 0) + 1

    cards_data = [
        ("Strong Buy",  STRONG_BUY,   "card-green"),
        ("Buy",         "_ALL_BUYS",  "card-green-dim"),
        ("Conflict",    CONFLICT,     "card-amber"),
        ("Sell",        "_ALL_SELLS", "card-red"),
        ("Hold",        HOLD,         "card-gray"),
    ]

    html_parts = ['<div class="cards-row">']
    for label, key, css in cards_data:
        if key == "_ALL_BUYS":
            v = counts.get(BUY_SCORING, 0) + counts.get(BUY_ROTATION, 0)
        elif key == "_ALL_SELLS":
            v = (counts.get(SELL_SCORING, 0) + counts.get(SELL_ROTATION, 0)
                 + counts.get(STRONG_SELL, 0))
        else:
            v = counts.get(key, 0)
        html_parts.append(f"""
    <div class="card {css}">
      <div class="card-value">{v}</div>
      <div class="card-label">{_esc(label)}</div>
    </div>""")
    html_parts.append("</div>")
    return "\n".join(html_parts)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL SECTIONS
# ═══════════════════════════════════════════════════════════════

def _all_sections_html(conv: MarketSignalResult, has_rotation: bool) -> str:
    # Group signals by label
    groups: dict[str, list[ConvergedSignal]] = {}
    for s in conv.signals:
        groups.setdefault(s.convergence_label, []).append(s)

    parts: list[str] = []
    for label, emoji, heading, desc in _SECTION_ORDER:
        sigs = groups.get(label, [])
        if not sigs:
            continue

        collapsed = label in (HOLD, NEUTRAL)
        section_id = label.lower().replace("_", "-")
        bg, fg = _LABEL_COLORS.get(label, ("#30363d", "#8b949e"))

        if label == STRONG_BUY:
            body = _strong_buy_cards_html(sigs, has_rotation)
        else:
            body = _signal_table_html(sigs, has_rotation, section_id)

        display = "none" if collapsed else "block"
        chevron = "▸" if collapsed else "▾"

        parts.append(f"""
<section class="signal-section" id="section-{section_id}">
  <div class="section-header" onclick="toggleSection('{section_id}')"
       style="border-left: 4px solid {fg}">
    <div>
      <h2>{emoji} {_esc(heading)}
        <span class="count-badge" style="background:{bg};color:{fg}">{len(sigs)}</span>
      </h2>
      <p class="section-desc">{_esc(desc)}</p>
    </div>
    <span class="chevron" id="chev-{section_id}">{chevron}</span>
  </div>
  <div class="section-body" id="body-{section_id}" style="display:{display}">
    {body}
  </div>
</section>""")

    return "\n".join(parts)


# ── Strong Buy Cards ──────────────────────────────────────────

def _strong_buy_cards_html(
    signals: list[ConvergedSignal],
    has_rotation: bool,
) -> str:
    parts = ['<div class="sb-cards">']
    for s in signals:
        confirmed_icon = "✓" if s.scoring_confirmed else "✗"
        score_w = _pct(s.adjusted_score)

        rot_html = ""
        if has_rotation and s.rotation_signal:
            rot_html = f"""
      <div class="sb-row">
        <span class="sb-dim">Rotation</span>
        <span>{_esc(s.rotation_signal)}&nbsp; RS {s.rotation_rs:+.3f}</span>
      </div>
      <div class="sb-row">
        <span class="sb-dim">Sector</span>
        <span>{_esc(s.rotation_sector)} (#{s.rotation_sector_rank} {_esc(s.rotation_sector_tier)})</span>
      </div>"""

        reason = _build_reason(s)

        parts.append(f"""
    <div class="sb-card" data-ticker="{_esc(s.ticker)}">
      <div class="sb-top">
        <span class="sb-rank">#{s.rank}</span>
        <span class="sb-ticker">{_esc(s.ticker)}</span>
        <span class="sb-score">{s.adjusted_score:.3f}</span>
      </div>
      <div class="score-bar"><div class="score-fill score-fill-green" style="width:{score_w}%"></div></div>
      <div class="sb-details">
        <div class="sb-row">
          <span class="sb-dim">Scoring</span>
          <span>{_esc(s.scoring_signal)} [{confirmed_icon}] &nbsp;{_esc(s.scoring_regime)}</span>
        </div>
        {rot_html}
      </div>
      <div class="sb-reason">{_esc(reason)}</div>
    </div>""")

    parts.append("</div>")
    return "\n".join(parts)


# ── Signal Table ──────────────────────────────────────────────

def _signal_table_html(
    signals: list[ConvergedSignal],
    has_rotation: bool,
    table_id: str,
) -> str:
    rot_cols = ""
    if has_rotation:
        rot_cols = """
        <th onclick="sortTable('{tid}',5)" class="sortable">Rot Signal</th>
        <th onclick="sortTable('{tid}',6)" class="sortable">Sector</th>
        <th onclick="sortTable('{tid}',7)" class="sortable">Sect Rank</th>
        <th onclick="sortTable('{tid}',8)" class="sortable">RS</th>""".replace(
            "{tid}", table_id
        )

    reason_col = 9 if has_rotation else 5

    header = f"""
    <table class="signal-table" id="table-{table_id}">
      <thead><tr>
        <th onclick="sortTable('{table_id}',0)" class="sortable">#</th>
        <th onclick="sortTable('{table_id}',1)" class="sortable">Ticker</th>
        <th onclick="sortTable('{table_id}',2)" class="sortable">Score</th>
        <th onclick="sortTable('{table_id}',3)" class="sortable">Adj</th>
        <th onclick="sortTable('{table_id}',4)" class="sortable">Scoring</th>
        {rot_cols}
        <th>Reason</th>
      </tr></thead>
      <tbody>"""

    rows: list[str] = []
    for s in signals:
        confirmed_icon = "✓" if s.scoring_confirmed else "✗"
        score_w = _pct(s.composite_score)
        adj_w   = _pct(s.adjusted_score)
        reason  = _build_reason(s)

        rot_cells = ""
        if has_rotation:
            rot_cells = f"""
        <td>{_esc(s.rotation_signal)}</td>
        <td>{_esc(s.rotation_sector)}</td>
        <td class="center">{s.rotation_sector_rank if s.rotation_sector_rank < 99 else '—'}</td>
        <td class="mono">{s.rotation_rs:+.3f}</td>"""

        rows.append(f"""
      <tr data-ticker="{_esc(s.ticker)}">
        <td class="center">{s.rank}</td>
        <td class="ticker-cell">{_esc(s.ticker)}</td>
        <td>
          <div class="score-bar-sm"><div class="score-fill score-fill-auto" style="width:{score_w}%"></div></div>
          <span class="mono">{s.composite_score:.3f}</span>
        </td>
        <td class="mono">{s.adjusted_score:.3f}</td>
        <td>{_esc(s.scoring_signal)}&nbsp;<span class="dim">[{confirmed_icon}]</span></td>
        {rot_cells}
        <td class="reason-cell">{_esc(reason)}</td>
      </tr>""")

    return header + "\n".join(rows) + "\n</tbody></table>"


# ── Reason Builder ────────────────────────────────────────────

def _build_reason(s: ConvergedSignal) -> str:
    parts: list[str] = []

    if s.scoring_confirmed:
        parts.append(f"Scoring confirmed ({s.scoring_regime})")
    elif s.scoring_signal and s.scoring_signal not in ("HOLD", "NEUTRAL"):
        parts.append(f"Scoring {s.scoring_signal} ({s.scoring_regime}, unconfirmed)")

    if s.rotation_reason:
        parts.append(s.rotation_reason)
    elif s.rotation_signal and s.rotation_sector:
        parts.append(
            f"Rotation {s.rotation_signal}: "
            f"{s.rotation_sector} "
            f"(#{s.rotation_sector_rank} {s.rotation_sector_tier})"
        )

    return " · ".join(parts) if parts else "—"


# ── Timings ───────────────────────────────────────────────────

def _timings_html(timings: dict[str, float]) -> str:
    if not timings:
        return ""
    items = " &nbsp;·&nbsp; ".join(
        f"{k}: {v:.1f}s" for k, v in timings.items()
    )
    return f'<p class="muted">Timings: {items}</p>'


# ── Error page ────────────────────────────────────────────────

def _error_page(message: str) -> str:
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>CASH — Error</title>
<style>body{{font-family:sans-serif;background:#0d1117;color:#f85149;
display:flex;align-items:center;justify-content:center;height:100vh;}}
.box{{background:#161b22;padding:40px;border-radius:12px;text-align:center;}}
</style></head><body><div class="box"><h1>Pipeline Error</h1>
<p>{_esc(message)}</p></div></body></html>"""


# ═══════════════════════════════════════════════════════════════
#  CSS
# ═══════════════════════════════════════════════════════════════

_CSS = """
/* ── Reset & Base ──────────────────────────────────────── */
*, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }

html { font-size: 15px; }

body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #0d1117;
  color: #e6edf3;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
}

.mono { font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: 0.85rem; }
.dim  { color: #8b949e; }
.muted { color: #484f58; font-size: 0.82rem; margin-top: 6px; }
.center { text-align: center; }

.container { max-width: 1280px; margin: 0 auto; padding: 0 24px; }

/* ── Header ────────────────────────────────────────────── */
.header {
  background: #161b22;
  border-bottom: 1px solid #30363d;
  padding: 20px 0;
}
.header-inner {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
}
.logo {
  font-size: 1.6rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  color: #e6edf3;
}
.market-badge {
  display: inline-block;
  background: #238636;
  color: #fff;
  font-size: 0.75rem;
  font-weight: 600;
  padding: 2px 10px;
  border-radius: 20px;
  vertical-align: middle;
  margin-left: 8px;
}
.subtitle { color: #8b949e; font-size: 0.9rem; margin-top: 2px; }
.header-meta { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.header-badge {
  display: inline-block;
  background: #21262d;
  color: #8b949e;
  font-size: 0.78rem;
  padding: 3px 10px;
  border-radius: 20px;
}
.badge-err { background: #3d1a1a; color: #f85149; }
.header-date { color: #8b949e; font-size: 0.82rem; }

/* ── Toolbar ───────────────────────────────────────────── */
.toolbar {
  display: flex; gap: 10px; align-items: center;
  margin: 24px 0 16px 0; flex-wrap: wrap;
}
.search-box {
  flex: 1; min-width: 200px; max-width: 360px;
  background: #161b22; border: 1px solid #30363d;
  color: #e6edf3; padding: 8px 14px;
  border-radius: 8px; font-size: 0.9rem;
  outline: none;
}
.search-box:focus { border-color: #58a6ff; }
.search-box::placeholder { color: #484f58; }
.btn {
  background: #21262d; color: #8b949e;
  border: 1px solid #30363d; padding: 7px 14px;
  border-radius: 8px; font-size: 0.82rem;
  cursor: pointer;
}
.btn:hover { background: #30363d; color: #e6edf3; }

/* ── Summary Cards ─────────────────────────────────────── */
.cards-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 14px;
  margin-bottom: 28px;
}
.card {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 12px;
  padding: 18px 16px;
  text-align: center;
}
.card-value {
  font-size: 2rem; font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
}
.card-label { font-size: 0.82rem; color: #8b949e; margin-top: 4px; }
.card-green     .card-value { color: #3fb950; }
.card-green-dim .card-value { color: #56d364; }
.card-amber     .card-value { color: #d29922; }
.card-red       .card-value { color: #f85149; }
.card-gray      .card-value { color: #8b949e; }

/* ── Signal Section ────────────────────────────────────── */
.signal-section {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 12px;
  margin-bottom: 18px;
  overflow: hidden;
}
.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  cursor: pointer;
  user-select: none;
  transition: background 0.15s;
}
.section-header:hover { background: #1c2128; }
.section-header h2 {
  font-size: 1.05rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 8px;
}
.count-badge {
  display: inline-block;
  font-size: 0.72rem;
  font-weight: 600;
  padding: 2px 9px;
  border-radius: 20px;
  vertical-align: middle;
}
.section-desc { color: #8b949e; font-size: 0.82rem; margin-top: 2px; }
.chevron { font-size: 1.2rem; color: #484f58; }
.section-body { padding: 0 20px 18px 20px; }

/* ── Strong-Buy Cards ──────────────────────────────────── */
.sb-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 14px;
}
.sb-card {
  background: #0d1117;
  border: 1px solid #238636;
  border-radius: 10px;
  padding: 16px;
  transition: border-color 0.2s;
}
.sb-card:hover { border-color: #3fb950; }
.sb-top {
  display: flex;
  align-items: baseline;
  gap: 10px;
  margin-bottom: 8px;
}
.sb-rank {
  color: #8b949e;
  font-size: 0.82rem;
  font-family: 'JetBrains Mono', monospace;
}
.sb-ticker {
  font-size: 1.2rem;
  font-weight: 700;
  color: #3fb950;
  letter-spacing: 0.03em;
}
.sb-score {
  margin-left: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.1rem;
  color: #3fb950;
}
.sb-details { margin-top: 10px; }
.sb-row {
  display: flex;
  justify-content: space-between;
  font-size: 0.85rem;
  padding: 3px 0;
  border-bottom: 1px solid #21262d;
}
.sb-dim { color: #8b949e; }
.sb-reason {
  margin-top: 10px;
  font-size: 0.82rem;
  color: #8b949e;
  font-style: italic;
  line-height: 1.5;
}

/* ── Score Bar ─────────────────────────────────────────── */
.score-bar {
  height: 6px;
  background: #21262d;
  border-radius: 3px;
  overflow: hidden;
}
.score-bar-sm {
  display: inline-block;
  width: 50px;
  height: 4px;
  background: #21262d;
  border-radius: 2px;
  overflow: hidden;
  vertical-align: middle;
  margin-right: 6px;
}
.score-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.4s;
}
.score-fill-green {
  background: linear-gradient(90deg, #238636, #3fb950);
}
.score-fill-auto {
  background: linear-gradient(90deg, #1f6feb, #58a6ff);
}

/* ── Table ─────────────────────────────────────────────── */
.signal-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}
.signal-table th {
  text-align: left;
  padding: 10px 10px;
  color: #8b949e;
  font-weight: 500;
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  border-bottom: 1px solid #30363d;
  white-space: nowrap;
}
.signal-table th.sortable { cursor: pointer; }
.signal-table th.sortable:hover { color: #e6edf3; }
.signal-table td {
  padding: 9px 10px;
  border-bottom: 1px solid #21262d;
  vertical-align: middle;
}
.signal-table tbody tr:hover { background: #1c2128; }
.ticker-cell {
  font-weight: 600;
  font-family: 'JetBrains Mono', monospace;
  letter-spacing: 0.02em;
  color: #e6edf3;
}
.reason-cell {
  color: #8b949e;
  font-size: 0.82rem;
  max-width: 340px;
  line-height: 1.45;
}

/* ── Footer ────────────────────────────────────────────── */
.footer {
  margin-top: 36px;
  padding: 20px 0 40px 0;
  text-align: center;
  color: #484f58;
  font-size: 0.82rem;
  border-top: 1px solid #21262d;
}

/* ── Responsive ────────────────────────────────────────── */
@media (max-width: 768px) {
  html { font-size: 14px; }
  .container { padding: 0 12px; }
  .header-inner { flex-direction: column; align-items: flex-start; }
  .sb-cards { grid-template-columns: 1fr; }
  .signal-table { display: block; overflow-x: auto; }
  .section-body { padding: 0 12px 14px 12px; }
}
"""


# ═══════════════════════════════════════════════════════════════
#  JAVASCRIPT
# ═══════════════════════════════════════════════════════════════

_JS = """
/* ── Section toggle ────────────────────────────────────── */
function toggleSection(id) {
  var body = document.getElementById('body-' + id);
  var chev = document.getElementById('chev-' + id);
  if (!body) return;
  if (body.style.display === 'none') {
    body.style.display = 'block';
    if (chev) chev.textContent = '▾';
  } else {
    body.style.display = 'none';
    if (chev) chev.textContent = '▸';
  }
}

function expandAll() {
  document.querySelectorAll('.section-body').forEach(function(el) {
    el.style.display = 'block';
  });
  document.querySelectorAll('.chevron').forEach(function(el) {
    el.textContent = '▾';
  });
}

function collapseAll() {
  document.querySelectorAll('.section-body').forEach(function(el) {
    el.style.display = 'none';
  });
  document.querySelectorAll('.chevron').forEach(function(el) {
    el.textContent = '▸';
  });
}

/* ── Table sort ────────────────────────────────────────── */
var sortState = {};

function sortTable(tableId, colIdx) {
  var table = document.getElementById('table-' + tableId);
  if (!table) return;
  var tbody = table.querySelector('tbody');
  if (!tbody) return;
  var rows = Array.from(tbody.querySelectorAll('tr'));

  var key = tableId + '-' + colIdx;
  var asc = sortState[key] === 'asc' ? false : true;
  sortState[key] = asc ? 'asc' : 'desc';

  rows.sort(function(a, b) {
    var aText = a.cells[colIdx] ? a.cells[colIdx].textContent.trim() : '';
    var bText = b.cells[colIdx] ? b.cells[colIdx].textContent.trim() : '';
    var aNum = parseFloat(aText.replace(/[^\\d.\\-+]/g, ''));
    var bNum = parseFloat(bText.replace(/[^\\d.\\-+]/g, ''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return asc ? aNum - bNum : bNum - aNum;
    }
    return asc ? aText.localeCompare(bText) : bText.localeCompare(aText);
  });

  rows.forEach(function(row) { tbody.appendChild(row); });
}

/* ── Ticker search / filter ────────────────────────────── */
function filterAll(query) {
  var q = query.toUpperCase().trim();

  /* Filter table rows */
  document.querySelectorAll('.signal-table tbody tr').forEach(function(row) {
    var ticker = row.getAttribute('data-ticker') || '';
    row.style.display = (!q || ticker.toUpperCase().indexOf(q) !== -1)
      ? '' : 'none';
  });

  /* Filter strong-buy cards */
  document.querySelectorAll('.sb-card').forEach(function(card) {
    var ticker = card.getAttribute('data-ticker') || '';
    card.style.display = (!q || ticker.toUpperCase().indexOf(q) !== -1)
      ? '' : 'none';
  });
}
"""