"""
reports/recommendations.py
Recommendation report generator.

Takes the raw output dict from pipeline/orchestrator.run_full_pipeline()
and produces:
  1. A structured dict (for programmatic consumption / storage)
  2. A plain-text report (for terminal / logging)
  3. An HTML report (for browser / email)

This is the final deliverable — the thing you read and trade from.
"""

import pandas as pd
from datetime import datetime
from textwrap import dedent


# ═════════════════════════════════════════════════════════════════
#  1.  STRUCTURED REPORT  (dict)
# ═════════════════════════════════════════════════════════════════

def build_report(pipeline_output: dict) -> dict:
    """
    Transform raw pipeline output into a clean report structure.

    Parameters
    ----------
    pipeline_output : dict
        Return value of orchestrator.run_full_pipeline().

    Returns
    -------
    dict with sections:
        header, regime, allocation, buy_list, sell_list,
        hold_list, risk_flags, portfolio_snapshot
    """
    summary = pipeline_output["summary"]
    regime  = pipeline_output["regime"]
    flags   = pipeline_output["risk_flags"]
    actions = pipeline_output["portfolio_actions"]
    buys    = pipeline_output["ranked_buys"]
    sells   = pipeline_output["sells"]
    holds   = pipeline_output["holds"]
    buckets = pipeline_output["bucket_weights"]

    # ── header ──────────────────────────────────────────────────
    header = {
        "title":           "CASH — Composite Adaptive Signal Hierarchy",
        "subtitle":        "Recommendation Report",
        "date":            _fmt_date(summary["date"]),
        "portfolio_value": summary["portfolio_value"],
        "regime":          summary["regime"],
        "regime_desc":     summary["regime_desc"],
        "spy_close":       summary["spy_close"],
    }

    # ── regime section ──────────────────────────────────────────
    regime_section = {
        "label":       regime["label"],
        "description": regime["description"],
        "spy_close":   regime["spy_close"],
        "spy_sma200":  regime.get("spy_sma200"),
        "breadth":     regime.get("breadth"),
        "guidance":    _regime_guidance(regime["label"]),
    }

    # ── allocation section ──────────────────────────────────────
    allocation_section = {
        "bucket_weights": buckets,
        "actual_fill":    summary.get("bucket_breakdown", {}),
        "cash_pct":       summary["cash_pct"],
    }

    # ── buy list ────────────────────────────────────────────────
    buy_list = []
    for b in buys:
        buy_list.append({
            "rank":           len(buy_list) + 1,
            "ticker":         b["ticker"],
            "category":       b.get("category", ""),
            "bucket":         b.get("bucket", ""),
            "themes":         b.get("themes", []),
            "close":          b["close"],
            "composite":      b["composite"],
            "confidence":     b["confidence"],
            "signal":         "BUY",
            "shares":         b["shares"],
            "dollar_alloc":   b["dollar_alloc"],
            "weight_pct":     b["weight_pct"],
            "stop_price":     b.get("stop_price"),
            "risk_per_share": b.get("risk_per_share"),
            "sub_scores": {
                "trend":        b["sub_scores"]["trend"],
                "momentum":     b["sub_scores"]["momentum"],
                "volatility":   b["sub_scores"]["volatility"],
                "rel_strength": b["sub_scores"]["rel_strength"],
            },
            "key_indicators": {
                "rsi":          b["indicators"]["rsi"],
                "adx":          b["indicators"]["adx"],
                "macd_hist":    b["indicators"]["macd_hist"],
                "rs_percentile": b["rs"]["rs_percentile"],
                "rs_regime":     b["rs"]["rs_regime"],
            },
        })

    # ── sell list ───────────────────────────────────────────────
    sell_list = []
    for s in sells:
        sell_list.append({
            "ticker":       s["ticker"],
            "category":     s.get("category", ""),
            "close":        s["close"],
            "composite":    s["composite"],
            "confidence":   s["confidence"],
            "signal":       "SELL",
            "sub_scores":   s["sub_scores"],
            "key_indicators": {
                "rsi":          s["indicators"]["rsi"],
                "adx":          s["indicators"]["adx"],
                "macd_hist":    s["indicators"]["macd_hist"],
                "rs_percentile": s["rs"]["rs_percentile"],
                "rs_regime":     s["rs"]["rs_regime"],
            },
        })

    # ── hold list (condensed) ───────────────────────────────────
    hold_list = []
    for h in holds:
        hold_list.append({
            "ticker":    h["ticker"],
            "category":  h.get("category", ""),
            "close":     h["close"],
            "composite": h["composite"],
            "signal":    "HOLD",
        })
    # sort holds by composite descending so near-buys are visible
    hold_list.sort(key=lambda x: x["composite"], reverse=True)

    # ── risk flags ──────────────────────────────────────────────
    risk_section = {
        "flags":           flags,
        "circuit_breaker": any("CIRCUIT_BREAKER" in f for f in flags),
        "exposure_warn":   any("EXPOSURE" in f for f in flags),
    }

    # ── portfolio snapshot ──────────────────────────────────────
    snapshot = {
        "tickers_analysed": summary["tickers_analysed"],
        "buy_count":        summary["buy_count"],
        "sell_count":       summary["sell_count"],
        "hold_count":       summary["hold_count"],
        "error_count":      summary["error_count"],
        "total_buy_dollar": summary["total_buy_dollar"],
        "cash_remaining":   summary["cash_remaining"],
        "cash_pct":         summary["cash_pct"],
    }

    return {
        "header":             header,
        "regime":             regime_section,
        "allocation":         allocation_section,
        "buy_list":           buy_list,
        "sell_list":          sell_list,
        "hold_list":          hold_list,
        "risk":               risk_section,
        "portfolio_snapshot": snapshot,
    }


# ═════════════════════════════════════════════════════════════════
#  2.  PLAIN-TEXT REPORT  (terminal / log)
# ═════════════════════════════════════════════════════════════════

def to_text(report: dict) -> str:
    """
    Render the structured report as a plain-text string.
    Suitable for terminal output or log files.
    """
    lines = []
    h = report["header"]
    r = report["regime"]
    a = report["allocation"]
    snap = report["portfolio_snapshot"]
    risk = report["risk"]

    # ── title block ─────────────────────────────────────────────
    lines.append("=" * 72)
    lines.append(f"  {h['title']}")
    lines.append(f"  {h['subtitle']}")
    lines.append(f"  Date: {h['date']}    Portfolio: ${h['portfolio_value']:,.0f}")
    lines.append("=" * 72)

    # ── regime ──────────────────────────────────────────────────
    lines.append("")
    lines.append("─── MARKET REGIME ───────────────────────────────────────────────")
    lines.append(f"  Regime:      {r['label'].upper()}  —  {r['description']}")
    lines.append(f"  SPY Close:   ${r['spy_close']:,.2f}")
    if r.get("spy_sma200"):
        lines.append(f"  SPY SMA200:  ${r['spy_sma200']:,.2f}")
    lines.append(f"  Guidance:    {r['guidance']}")

    # ── risk flags ──────────────────────────────────────────────
    if risk["flags"]:
        lines.append("")
        lines.append("─── ⚠  RISK FLAGS ──────────────────────────────────────────────")
        for f in risk["flags"]:
            lines.append(f"  ▸ {f}")
        if risk["circuit_breaker"]:
            lines.append("  *** CIRCUIT BREAKER ACTIVE — ALL BUYS DOWNGRADED TO HOLD ***")

    # ── allocation targets ──────────────────────────────────────
    lines.append("")
    lines.append("─── ALLOCATION TARGETS (this regime) ────────────────────────────")
    for bucket, weight in sorted(a["bucket_weights"].items()):
        actual = a["actual_fill"].get(bucket, 0)
        lines.append(f"  {bucket:22s}  target {weight:5.0%}    filled ${actual:>10,.0f}")
    lines.append(f"  {'Cash':22s}          {a['cash_pct']:5.1f}%")

    # ── buy recommendations ─────────────────────────────────────
    lines.append("")
    lines.append("─── BUY RECOMMENDATIONS ────────────────────────────────────────")
    if report["buy_list"]:
        lines.append(
            f"  {'#':>3s}  {'Ticker':<8s} {'Cat':<18s} "
            f"{'Comp':>5s} {'Conf':>5s} {'Shares':>6s} "
            f"{'Dollar':>10s} {'Wt%':>5s} {'Stop':>8s}"
        )
        lines.append("  " + "-" * 68)
        for b in report["buy_list"]:
            stop_str = f"${b['stop_price']:>7.2f}" if b["stop_price"] else "    n/a"
            lines.append(
                f"  {b['rank']:3d}  {b['ticker']:<8s} {b['category'][:18]:<18s} "
                f"{b['composite']:5.2f} {b['confidence']:5.0%} {b['shares']:6d} "
                f"${b['dollar_alloc']:>9,.0f} {b['weight_pct']:5.1f} {stop_str}"
            )
        lines.append("")
        # detail block for top 10
        for b in report["buy_list"][:10]:
            lines.append(f"  {b['ticker']}:")
            sc = b["sub_scores"]
            ki = b["key_indicators"]
            lines.append(
                f"    Scores  → trend {sc['trend']:+.2f}  mom {sc['momentum']:+.2f}  "
                f"vol {sc['volatility']:+.2f}  RS {sc['rel_strength']:+.2f}"
            )
            lines.append(
                f"    Indicators → RSI {ki['rsi']:.0f}  ADX {ki['adx']:.0f}  "
                f"MACD-H {ki['macd_hist']:+.3f}  RS%ile {ki['rs_percentile']:.0%}  "
                f"RS-regime {ki['rs_regime']}"
            )
            if b["themes"]:
                lines.append(f"    Themes  → {', '.join(b['themes'])}")
            lines.append("")
    else:
        lines.append("  No BUY signals this run.")

    # ── sell recommendations ────────────────────────────────────
    lines.append("─── SELL RECOMMENDATIONS ───────────────────────────────────────")
    if report["sell_list"]:
        lines.append(
            f"  {'Ticker':<8s} {'Cat':<18s} {'Comp':>5s} "
            f"{'Conf':>5s} {'RSI':>4s} {'RS%':>5s}"
        )
        lines.append("  " + "-" * 48)
        for s in report["sell_list"]:
            ki = s["key_indicators"]
            lines.append(
                f"  {s['ticker']:<8s} {s['category'][:18]:<18s} "
                f"{s['composite']:5.2f} {s['confidence']:5.0%} "
                f"{ki['rsi']:4.0f} {ki['rs_percentile']:5.0%}"
            )
    else:
        lines.append("  No SELL signals this run.")

    # ── holds (near-buy) ────────────────────────────────────────
    lines.append("")
    lines.append("─── HOLD (top 15 by composite — watchlist) ──────────────────────")
    for h in report["hold_list"][:15]:
        lines.append(
            f"  {h['ticker']:<8s} {h['category'][:18]:<18s} "
            f"composite {h['composite']:5.2f}"
        )

    # ── snapshot ────────────────────────────────────────────────
    lines.append("")
    lines.append("─── PORTFOLIO SNAPSHOT ─────────────────────────────────────────")
    lines.append(f"  Tickers analysed:  {snap['tickers_analysed']}")
    lines.append(f"  BUY:  {snap['buy_count']}    SELL:  {snap['sell_count']}    "
                 f"HOLD:  {snap['hold_count']}    Errors:  {snap['error_count']}")
    lines.append(f"  Total buy allocation:  ${snap['total_buy_dollar']:,.0f}")
    lines.append(f"  Cash remaining:        ${snap['cash_remaining']:,.0f}  "
                 f"({snap['cash_pct']:.1f}%)")
    lines.append("")
    lines.append("=" * 72)

    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  3.  HTML REPORT
# ═════════════════════════════════════════════════════════════════

def to_html(report: dict) -> str:
    """
    Render the structured report as a self-contained HTML string.
    Can be saved to file, opened in browser, or emailed.
    """
    h = report["header"]
    r = report["regime"]
    a = report["allocation"]
    snap = report["portfolio_snapshot"]
    risk = report["risk"]

    regime_color = _regime_color(r["label"])

    # ── buy rows ────────────────────────────────────────────────
    buy_rows = ""
    for b in report["buy_list"]:
        stop_str = f"${b['stop_price']:.2f}" if b["stop_price"] else "—"
        themes_str = ", ".join(b["themes"]) if b["themes"] else "—"
        sc = b["sub_scores"]
        ki = b["key_indicators"]
        buy_rows += f"""
        <tr>
            <td class="rank">{b['rank']}</td>
            <td class="ticker">{b['ticker']}</td>
            <td class="cat">{b['category']}</td>
            <td class="num">{b['composite']:.2f}</td>
            <td class="num">{b['confidence']:.0%}</td>
            <td class="num">{b['shares']}</td>
            <td class="num">${b['dollar_alloc']:,.0f}</td>
            <td class="num">{b['weight_pct']:.1f}%</td>
            <td class="num">{stop_str}</td>
            <td class="detail">
                T {sc['trend']:+.2f} M {sc['momentum']:+.2f}
                V {sc['volatility']:+.2f} RS {sc['rel_strength']:+.2f}<br>
                RSI {ki['rsi']:.0f} ADX {ki['adx']:.0f}
                RS%ile {ki['rs_percentile']:.0%}
                ({ki['rs_regime']})<br>
                <span class="themes">{themes_str}</span>
            </td>
        </tr>"""

    # ── sell rows ───────────────────────────────────────────────
    sell_rows = ""
    for s in report["sell_list"]:
        ki = s["key_indicators"]
        sell_rows += f"""
        <tr>
            <td class="ticker">{s['ticker']}</td>
            <td class="cat">{s['category']}</td>
            <td class="num">{s['composite']:.2f}</td>
            <td class="num">{s['confidence']:.0%}</td>
            <td class="num">{ki['rsi']:.0f}</td>
            <td class="num">{ki['rs_percentile']:.0%}</td>
        </tr>"""

    # ── hold rows ───────────────────────────────────────────────
    hold_rows = ""
    for ho in report["hold_list"][:20]:
        hold_rows += f"""
        <tr>
            <td class="ticker">{ho['ticker']}</td>
            <td class="cat">{ho['category']}</td>
            <td class="num">{ho['composite']:.2f}</td>
        </tr>"""

    # ── allocation rows ─────────────────────────────────────────
    alloc_rows = ""
    for bucket, weight in sorted(a["bucket_weights"].items()):
        actual = a["actual_fill"].get(bucket, 0)
        alloc_rows += f"""
        <tr>
            <td>{bucket.replace('_', ' ').title()}</td>
            <td class="num">{weight:.0%}</td>
            <td class="num">${actual:,.0f}</td>
        </tr>"""

    # ── risk flags ──────────────────────────────────────────────
    risk_html = ""
    if risk["flags"]:
        flag_items = "".join(f"<li>{f}</li>" for f in risk["flags"])
        cb_banner = ""
        if risk["circuit_breaker"]:
            cb_banner = (
                '<div class="circuit-breaker">'
                '⚠ CIRCUIT BREAKER ACTIVE — ALL BUYS DOWNGRADED TO HOLD'
                '</div>'
            )
        risk_html = f"""
        <div class="risk-section">
            <h2>⚠ Risk Flags</h2>
            {cb_banner}
            <ul>{flag_items}</ul>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CASH Report — {h['date']}</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, Helvetica, Arial, sans-serif;
        background: #0d1117; color: #c9d1d9;
        padding: 24px; max-width: 1200px; margin: 0 auto;
        font-size: 14px; line-height: 1.5;
    }}
    h1 {{ color: #58a6ff; font-size: 22px; margin-bottom: 4px; }}
    h2 {{ color: #8b949e; font-size: 16px; margin: 24px 0 12px; border-bottom: 1px solid #30363d; padding-bottom: 6px; }}
    .subtitle {{ color: #8b949e; font-size: 14px; }}
    .header-row {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; }}
    .regime-badge {{
        display: inline-block; padding: 6px 16px;
        border-radius: 6px; font-weight: 700; font-size: 14px;
        background: {regime_color}22; color: {regime_color};
        border: 1px solid {regime_color};
    }}
    .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin: 16px 0; }}
    .stat-card {{ background: #161b22; padding: 12px; border-radius: 8px; border: 1px solid #30363d; }}
    .stat-card .label {{ font-size: 11px; color: #8b949e; text-transform: uppercase; }}
    .stat-card .value {{ font-size: 20px; font-weight: 700; color: #e6edf3; }}
    table {{ width: 100%; border-collapse: collapse; margin: 8px 0; }}
    th {{ text-align: left; font-size: 11px; color: #8b949e; text-transform: uppercase;
          padding: 8px 6px; border-bottom: 2px solid #30363d; }}
    td {{ padding: 7px 6px; border-bottom: 1px solid #21262d; font-size: 13px; }}
    tr:hover {{ background: #161b22; }}
    .rank {{ text-align: center; color: #8b949e; }}
    .ticker {{ font-weight: 700; color: #58a6ff; }}
    .cat {{ color: #8b949e; font-size: 12px; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .detail {{ font-size: 11px; color: #8b949e; line-height: 1.4; }}
    .themes {{ color: #a371f7; }}
    .risk-section {{ background: #2d1b1b; border: 1px solid #f85149; border-radius: 8px; padding: 16px; margin: 16px 0; }}
    .risk-section h2 {{ color: #f85149; border: none; margin-top: 0; }}
    .risk-section li {{ margin: 4px 0 4px 20px; }}
    .circuit-breaker {{ background: #f8514922; color: #f85149; padding: 10px; border-radius: 6px;
                         font-weight: 700; text-align: center; margin-bottom: 12px; }}
    .sell-table .ticker {{ color: #f85149; }}
    .guidance {{ background: #161b22; padding: 12px 16px; border-radius: 8px;
                  border-left: 4px solid {regime_color}; margin: 12px 0; color: #e6edf3; }}
    .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid #30363d;
               font-size: 11px; color: #484f58; text-align: center; }}
    @media (max-width: 700px) {{
        body {{ padding: 12px; font-size: 13px; }}
        .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
        table {{ font-size: 11px; }}
        td, th {{ padding: 5px 3px; }}
    }}
</style>
</head>
<body>

<!-- ── HEADER ──────────────────────────────────────── -->
<div class="header-row">
    <div>
        <h1>{h['title']}</h1>
        <div class="subtitle">{h['subtitle']}  —  {h['date']}</div>
    </div>
    <div class="regime-badge">{r['label'].upper()}</div>
</div>

<!-- ── STAT CARDS ──────────────────────────────────── -->
<div class="stat-grid">
    <div class="stat-card">
        <div class="label">Portfolio</div>
        <div class="value">${h['portfolio_value']:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">SPY Close</div>
        <div class="value">${r['spy_close']:,.2f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Buys</div>
        <div class="value">{snap['buy_count']}</div>
    </div>
    <div class="stat-card">
        <div class="label">Sells</div>
        <div class="value">{snap['sell_count']}</div>
    </div>
    <div class="stat-card">
        <div class="label">Allocated</div>
        <div class="value">${snap['total_buy_dollar']:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Cash</div>
        <div class="value">{snap['cash_pct']:.1f}%</div>
    </div>
</div>

<!-- ── REGIME ──────────────────────────────────────── -->
<h2>Market Regime</h2>
<div class="guidance">{r['guidance']}</div>

<!-- ── RISK FLAGS ──────────────────────────────────── -->
{risk_html}

<!-- ── ALLOCATION ──────────────────────────────────── -->
<h2>Allocation Targets</h2>
<table>
    <thead><tr><th>Bucket</th><th>Target</th><th>Filled</th></tr></thead>
    <tbody>{alloc_rows}</tbody>
</table>

<!-- ── BUY RECOMMENDATIONS ─────────────────────────── -->
<h2>Buy Recommendations ({snap['buy_count']})</h2>
<table>
    <thead>
        <tr>
            <th>#</th><th>Ticker</th><th>Category</th>
            <th>Comp</th><th>Conf</th><th>Shares</th>
            <th>Dollar</th><th>Wt%</th><th>Stop</th><th>Detail</th>
        </tr>
    </thead>
    <tbody>{buy_rows if buy_rows else '<tr><td colspan="10">No BUY signals this run.</td></tr>'}</tbody>
</table>

<!-- ── SELL RECOMMENDATIONS ────────────────────────── -->
<h2>Sell Recommendations ({snap['sell_count']})</h2>
<table class="sell-table">
    <thead>
        <tr><th>Ticker</th><th>Category</th><th>Comp</th><th>Conf</th><th>RSI</th><th>RS%</th></tr>
    </thead>
    <tbody>{sell_rows if sell_rows else '<tr><td colspan="6">No SELL signals this run.</td></tr>'}</tbody>
</table>

<!-- ── HOLD / WATCHLIST ────────────────────────────── -->
<h2>Hold / Watchlist (top 20)</h2>
<table>
    <thead><tr><th>Ticker</th><th>Category</th><th>Composite</th></tr></thead>
    <tbody>{hold_rows}</tbody>
</table>

<!-- ── FOOTER ──────────────────────────────────────── -->
<div class="footer">
    Generated by CASH v1.0 &nbsp;|&nbsp; {h['date']} &nbsp;|&nbsp;
    {snap['tickers_analysed']} tickers analysed &nbsp;|&nbsp;
    {snap['error_count']} errors
</div>

</body>
</html>"""

    return html


# ═════════════════════════════════════════════════════════════════
#  4.  FILE OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def save_text(report: dict, filepath: str) -> str:
    """Save plain-text report to file. Returns filepath."""
    text = to_text(report)
    with open(filepath, "w") as f:
        f.write(text)
    return filepath


def save_html(report: dict, filepath: str) -> str:
    """Save HTML report to file. Returns filepath."""
    html = to_html(report)
    with open(filepath, "w") as f:
        f.write(html)
    return filepath


def print_report(report: dict):
    """Print plain-text report to stdout."""
    print(to_text(report))


# ═════════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═════════════════════════════════════════════════════════════════

def _fmt_date(dt) -> str:
    """Format date for display."""
    if isinstance(dt, pd.Timestamp):
        return dt.strftime("%Y-%m-%d")
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    return str(dt)


def _regime_color(label: str) -> str:
    """Return hex color for regime badge."""
    colors = {
        "bull_confirmed": "#3fb950",
        "bull_cautious":  "#d29922",
        "bear_mild":      "#db6d28",
        "bear_severe":    "#f85149",
    }
    return colors.get(label, "#8b949e")


def _regime_guidance(label: str) -> str:
    """Human-readable guidance string per regime."""
    guidance = {
        "bull_confirmed": (
            "Strong uptrend confirmed. Favour equity-heavy allocation. "
            "Lean into momentum, growth sectors, and thematic plays. "
            "Tighten stops only on extended names."
        ),
        "bull_cautious": (
            "Uptrend intact but showing signs of fatigue. "
            "Maintain equity exposure but shift toward quality and "
            "begin adding fixed income / alternatives. "
            "Widen watchlist for rotation opportunities."
        ),
        "bear_mild": (
            "Trend is deteriorating. Reduce equity exposure significantly. "
            "Favour defensive sectors (Staples, Utilities, Healthcare), "
            "increase fixed income and gold. "
            "Only take high-conviction new positions."
        ),
        "bear_severe": (
            "Market in severe drawdown. Capital preservation is priority. "
            "Minimal equity exposure — only inverse or ultra-defensive. "
            "Heavy fixed income and alternatives. "
            "Circuit breaker may be active."
        ),
    }
    return guidance.get(label, "Regime not recognised — proceed with caution.")