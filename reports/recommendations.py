"""
reports/recommendations.py
Recommendation report generator for the CASH pipeline.

Accepts either a ``PipelineResult`` object (from
``pipeline.orchestrator``) **or** a legacy dict, and produces:

  1. A structured report dict (for programmatic use / JSON)
  2. A plain-text report  (for terminal / logging)
  3. An HTML report        (for browser / email)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = [
    "build_report",
    "to_text",
    "to_html",
    "save_text",
    "save_html",
    "print_report",
]


# ═════════════════════════════════════════════════════════════════
#  0.  PUBLIC ENTRY POINT
# ═════════════════════════════════════════════════════════════════

def build_report(source: Any) -> dict:
    """
    Build a structured recommendation report.

    Parameters
    ----------
    source : PipelineResult  **or**  dict
        • ``PipelineResult`` returned by
          ``orchestrator.run_full_pipeline()``
        • Legacy dict with keys *summary, regime, risk_flags,
          ranked_buys, sells, holds, bucket_weights*

    Returns
    -------
    dict
        Sections: *header, regime, allocation, buy_list,
        sell_list, hold_list, risk, portfolio_snapshot*
    """
    # Duck-type for PipelineResult (avoids circular import)
    if hasattr(source, "snapshots") and hasattr(source, "run_date"):
        raw = _from_pipeline_result(source)
    elif isinstance(source, dict):
        raw = source
    else:
        raise TypeError(
            f"build_report expects PipelineResult or dict, "
            f"got {type(source).__name__}"
        )
    return _assemble_report(raw)


# ═════════════════════════════════════════════════════════════════
#  1.  PipelineResult → RAW DICT TRANSFORMER  (FIXED)
# ═════════════════════════════════════════════════════════════════

def _from_pipeline_result(result: Any) -> dict:
    """
    Convert a ``PipelineResult`` into the flat dict format that
    ``_assemble_report()`` expects.

    Reads from the correct PipelineResult fields:
      result.snapshots       — per-ticker snapshot dicts
      result.portfolio       — output of build_portfolio():
                               metadata, target_weights,
                               sector_exposure, holdings (DataFrame),
                               candidates, rejected, excluded, trades
      result.breadth         — universe-level breadth DataFrame
      result.scored_universe — {ticker: DataFrame} for SPY lookup
      result.errors          — list of error strings
      result.run_date        — pd.Timestamp
    """
    snapshots = result.snapshots or []
    portfolio = result.portfolio or {}
    errors    = result.errors or []

    # ── Read from build_portfolio() output structure ─────────────
    #
    # build_portfolio() returns:
    #   metadata        dict  (num_holdings, total_invested,
    #                          cash_pct, breadth_regime, etc.)
    #   target_weights  {ticker: weight_fraction}
    #   sector_exposure {sector: total_weight}
    #   holdings        DataFrame of selected positions
    #   candidates      DataFrame of confirmed tickers
    #   rejected        DataFrame of rejected tickers
    #   excluded        DataFrame of candidates that didn't fit
    #   trades          DataFrame or None
    #
    meta             = portfolio.get("metadata", {})
    target_weights   = portfolio.get("target_weights", {})
    sector_exposure  = portfolio.get("sector_exposure", {})

    # ── Normalise every snapshot ────────────────────────────────
    normalised = [_normalise_snapshot(s) for s in snapshots]

    # ── Infer capital ───────────────────────────────────────────
    capital = _infer_capital_from_snapshots(normalised, meta)

    # ── Back-fill allocations from target_weights ───────────────
    if target_weights:
        _backfill_allocations(normalised, target_weights, capital)

    # ── Classify by signal ──────────────────────────────────────
    _buys, _sells, _holds = [], [], []
    for s in normalised:
        sig = s["signal"].upper()
        if sig in ("BUY", "STRONG_BUY"):
            _buys.append(s)
        elif sig in ("SELL", "STRONG_SELL"):
            _sells.append(s)
        else:
            _holds.append(s)
    _buys.sort(key=lambda x: x["composite"], reverse=True)

    # ── Breadth from universe-level DataFrame ───────────────────
    breadth_df = getattr(result, "breadth", None)

    breadth_regime = "unknown"
    breadth_score  = None
    if (
        breadth_df is not None
        and hasattr(breadth_df, "empty")
        and not breadth_df.empty
    ):
        if "breadth_regime" in breadth_df.columns:
            breadth_regime = str(
                breadth_df["breadth_regime"].iloc[-1]
            )
        if "breadth_score" in breadth_df.columns:
            breadth_score = float(
                breadth_df["breadth_score"].iloc[-1]
            )

    # ── Regime detection ────────────────────────────────────────
    spy_close, spy_sma200, above_sma200 = _spy_from_scored_universe(
        result,
    )
    regime_label, regime_desc = _detect_regime_fallback(
        above_sma200, breadth_regime,
    )

    # ── Capital figures ─────────────────────────────────────────
    total_buy = sum(
        _num(b.get("dollar_alloc"), 0) for b in _buys
    )
    cash_remaining = capital - total_buy
    cash_pct = (
        (cash_remaining / capital * 100)
        if capital > 0 else 100.0
    )

    # ── Bucket weights from sector exposure ─────────────────────
    bucket_weights: dict[str, float] = {}
    if sector_exposure:
        for sector, weight in sector_exposure.items():
            bucket_weights[sector] = weight
        bucket_weights["cash"] = _num(meta.get("cash_pct"), 0.05)
    else:
        bucket_weights = _default_bucket_weights(regime_label)

    # ── Risk flags ──────────────────────────────────────────────
    risk_flags: list[str] = []
    if breadth_regime == "weak":
        risk_flags.append(
            "BREADTH_WEAK: Market breadth is weak — "
            "reduced exposure recommended"
        )
    if regime_label in ("bear_mild", "bear_severe"):
        risk_flags.append(
            f"REGIME: {regime_label} — defensive "
            f"positioning recommended"
        )
    if regime_label == "bear_severe":
        risk_flags.append(
            "CIRCUIT_BREAKER: Severe bear — "
            "consider halting new buys"
        )

    return {
        "summary": {
            "date":             result.run_date,
            "portfolio_value":  capital,
            "regime":           regime_label,
            "regime_desc":      regime_desc,
            "spy_close":        spy_close,
            "tickers_analysed": len(snapshots),
            "buy_count":        len(_buys),
            "sell_count":       len(_sells),
            "hold_count":       len(_holds),
            "error_count":      len(errors),
            "total_buy_dollar": total_buy,
            "cash_remaining":   cash_remaining,
            "cash_pct":         cash_pct,
            "bucket_breakdown": {},
        },
        "regime": {
            "label":       regime_label,
            "description": regime_desc,
            "spy_close":   spy_close,
            "spy_sma200":  spy_sma200,
            "breadth":     breadth_score,
        },
        "risk_flags":        risk_flags,
        "portfolio_actions": [],
        "ranked_buys":       _buys,
        "sells":             _sells,
        "holds":             _holds,
        "bucket_weights":    bucket_weights,
    }


# ═════════════════════════════════════════════════════════════════
#  2.  ASSEMBLE STRUCTURED REPORT  (raw dict → report dict)
# ═════════════════════════════════════════════════════════════════

def _assemble_report(raw: dict) -> dict:
    """
    Transform the raw pipeline dict into the final report
    structure consumed by ``to_text()`` and ``to_html()``.
    """
    summary = raw.get("summary", {})
    regime  = raw.get("regime", {})
    flags   = raw.get("risk_flags", [])
    buys    = raw.get("ranked_buys", [])
    sells   = raw.get("sells", [])
    holds   = raw.get("holds", [])
    buckets = raw.get("bucket_weights", {})

    # ── header ──────────────────────────────────────────────────
    regime_label = regime.get(
        "label", summary.get("regime", "bull_cautious"),
    )
    header = {
        "title":           "CASH — Composite Adaptive Signal Hierarchy",
        "subtitle":        "Recommendation Report",
        "date":            _fmt_date(summary.get("date", datetime.now())),
        "portfolio_value": _num(summary.get("portfolio_value"), 0),
        "regime":          regime_label,
        "regime_desc":     summary.get("regime_desc", ""),
        "spy_close":       _num(summary.get("spy_close"), 0),
    }

    # ── regime section ──────────────────────────────────────────
    regime_section = {
        "label":       regime_label,
        "description": regime.get(
            "description", summary.get("regime_desc", ""),
        ),
        "spy_close":   _num(
            regime.get("spy_close", summary.get("spy_close")), 0,
        ),
        "spy_sma200":  regime.get("spy_sma200"),
        "breadth":     regime.get("breadth"),
        "guidance":    _regime_guidance(regime_label),
    }

    # ── allocation ──────────────────────────────────────────────
    allocation_section = {
        "bucket_weights": buckets,
        "actual_fill":    summary.get("bucket_breakdown", {}),
        "cash_pct":       _num(summary.get("cash_pct"), 0),
    }

    # ── buy list ────────────────────────────────────────────────
    buy_list = []
    for b in buys:
        sub = b.get("sub_scores", {})
        ind = b.get("indicators", {})
        rs  = b.get("rs", {})
        buy_list.append({
            "rank":           len(buy_list) + 1,
            "ticker":         b.get("ticker", "???"),
            "category":       b.get("category", ""),
            "bucket":         b.get("bucket", ""),
            "themes":         b.get("themes", []),
            "close":          _num(b.get("close"), 0),
            "composite":      _num(b.get("composite"), 0),
            "confidence":     _num(b.get("confidence"), 0),
            "signal":         "BUY",
            "shares":         int(_num(b.get("shares"), 0)),
            "dollar_alloc":   _num(b.get("dollar_alloc"), 0),
            "weight_pct":     _num(b.get("weight_pct"), 0),
            "stop_price":     b.get("stop_price"),
            "risk_per_share": b.get("risk_per_share"),
            "sub_scores": {
                "trend":        _num(sub.get("trend"), 0),
                "momentum":     _num(sub.get("momentum"), 0),
                "volatility":   _num(sub.get("volatility"), 0),
                "rel_strength": _num(sub.get("rel_strength"), 0),
            },
            "key_indicators": {
                "rsi":           _num(ind.get("rsi"), 50),
                "adx":           _num(ind.get("adx"), 20),
                "macd_hist":     _num(ind.get("macd_hist"), 0),
                "rs_percentile": _num(rs.get("rs_percentile"), 0.5),
                "rs_regime":     rs.get("rs_regime", "neutral"),
            },
        })

    # ── sell list ───────────────────────────────────────────────
    sell_list = []
    for s in sells:
        sub = s.get("sub_scores", {})
        ind = s.get("indicators", {})
        rs  = s.get("rs", {})
        sell_list.append({
            "ticker":     s.get("ticker", "???"),
            "category":   s.get("category", ""),
            "close":      _num(s.get("close"), 0),
            "composite":  _num(s.get("composite"), 0),
            "confidence": _num(s.get("confidence"), 0),
            "signal":     "SELL",
            "sub_scores": {
                "trend":        _num(sub.get("trend"), 0),
                "momentum":     _num(sub.get("momentum"), 0),
                "volatility":   _num(sub.get("volatility"), 0),
                "rel_strength": _num(sub.get("rel_strength"), 0),
            },
            "key_indicators": {
                "rsi":           _num(ind.get("rsi"), 50),
                "adx":           _num(ind.get("adx"), 20),
                "macd_hist":     _num(ind.get("macd_hist"), 0),
                "rs_percentile": _num(rs.get("rs_percentile"), 0.5),
                "rs_regime":     rs.get("rs_regime", "neutral"),
            },
        })

    # ── hold list (condensed) ───────────────────────────────────
    hold_list = []
    for h in holds:
        hold_list.append({
            "ticker":    h.get("ticker", "???"),
            "category":  h.get("category", ""),
            "close":     _num(h.get("close"), 0),
            "composite": _num(h.get("composite"), 0),
            "signal":    "HOLD",
        })
    hold_list.sort(key=lambda x: x["composite"], reverse=True)

    # ── risk flags ──────────────────────────────────────────────
    risk_section = {
        "flags":           flags,
        "circuit_breaker": any("CIRCUIT_BREAKER" in str(f) for f in flags),
        "exposure_warn":   any("EXPOSURE" in str(f) for f in flags),
    }

    # ── portfolio snapshot ──────────────────────────────────────
    snapshot = {
        "tickers_analysed": int(_num(summary.get("tickers_analysed"), 0)),
        "buy_count":        int(_num(summary.get("buy_count"), len(buy_list))),
        "sell_count":       int(_num(summary.get("sell_count"), len(sell_list))),
        "hold_count":       int(_num(summary.get("hold_count"), len(hold_list))),
        "error_count":      int(_num(summary.get("error_count"), 0)),
        "total_buy_dollar": _num(summary.get("total_buy_dollar"), 0),
        "cash_remaining":   _num(summary.get("cash_remaining"), 0),
        "cash_pct":         _num(summary.get("cash_pct"), 0),
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
#  3.  PLAIN-TEXT REPORT
# ═════════════════════════════════════════════════════════════════

def to_text(report: dict) -> str:
    """
    Render the structured report as a plain-text string.
    Suitable for terminal output or log files.
    """
    lines: list[str] = []
    h    = report["header"]
    r    = report["regime"]
    a    = report["allocation"]
    snap = report["portfolio_snapshot"]
    risk = report["risk"]

    # ── title block ─────────────────────────────────────────────
    lines.append("=" * 72)
    lines.append(f"  {h['title']}")
    lines.append(f"  {h['subtitle']}")
    lines.append(
        f"  Date: {h['date']}    "
        f"Portfolio: ${h['portfolio_value']:,.0f}"
    )
    lines.append("=" * 72)

    # ── regime ──────────────────────────────────────────────────
    lines.append("")
    lines.append(
        "─── MARKET REGIME "
        "───────────────────────────────────────────────────"
    )
    lines.append(
        f"  Regime:      {r['label'].upper()}  —  {r['description']}"
    )
    lines.append(f"  SPY Close:   ${r['spy_close']:,.2f}")
    if r.get("spy_sma200"):
        lines.append(f"  SPY SMA200:  ${r['spy_sma200']:,.2f}")
    lines.append(f"  Guidance:    {r['guidance']}")

    # ── risk flags ──────────────────────────────────────────────
    if risk["flags"]:
        lines.append("")
        lines.append(
            "─── ⚠  RISK FLAGS "
            "─────────────────────────────────────────────────"
        )
        for f in risk["flags"]:
            lines.append(f"  ▸ {f}")
        if risk["circuit_breaker"]:
            lines.append(
                "  *** CIRCUIT BREAKER ACTIVE — "
                "ALL BUYS DOWNGRADED TO HOLD ***"
            )

    # ── allocation targets ──────────────────────────────────────
    lines.append("")
    lines.append(
        "─── ALLOCATION TARGETS (this regime) "
        "────────────────────────────────"
    )
    for bucket, weight in sorted(a["bucket_weights"].items()):
        actual = a["actual_fill"].get(bucket, 0)
        lines.append(
            f"  {bucket:22s}  target {weight:5.0%}    "
            f"filled ${actual:>10,.0f}"
        )
    lines.append(f"  {'Cash':22s}          {a['cash_pct']:5.1f}%")

    # ── buy recommendations ─────────────────────────────────────
    lines.append("")
    lines.append(
        "─── BUY RECOMMENDATIONS "
        "────────────────────────────────────────────"
    )
    if report["buy_list"]:
        lines.append(
            f"  {'#':>3s}  {'Ticker':<8s} {'Cat':<18s} "
            f"{'Comp':>5s} {'Conf':>5s} {'Shares':>6s} "
            f"{'Dollar':>10s} {'Wt%':>5s} {'Stop':>8s}"
        )
        lines.append("  " + "-" * 68)
        for b in report["buy_list"]:
            stop_str = (
                f"${b['stop_price']:>7.2f}"
                if b["stop_price"] else "    n/a"
            )
            lines.append(
                f"  {b['rank']:3d}  {b['ticker']:<8s} "
                f"{str(b['category'])[:18]:<18s} "
                f"{b['composite']:5.2f} {b['confidence']:5.0%} "
                f"{b['shares']:6d} "
                f"${b['dollar_alloc']:>9,.0f} "
                f"{b['weight_pct']:5.1f} {stop_str}"
            )

        # detail block for top 10
        lines.append("")
        for b in report["buy_list"][:10]:
            sc = b["sub_scores"]
            ki = b["key_indicators"]
            lines.append(f"  {b['ticker']}:")
            lines.append(
                f"    Scores  → trend {sc['trend']:+.2f}  "
                f"mom {sc['momentum']:+.2f}  "
                f"vol {sc['volatility']:+.2f}  "
                f"RS {sc['rel_strength']:+.2f}"
            )
            lines.append(
                f"    Indicators → RSI {ki['rsi']:.0f}  "
                f"ADX {ki['adx']:.0f}  "
                f"MACD-H {ki['macd_hist']:+.3f}  "
                f"RS%ile {ki['rs_percentile']:.0%}  "
                f"RS-regime {ki['rs_regime']}"
            )
            themes = b.get("themes", [])
            if themes:
                lines.append(
                    f"    Themes  → {', '.join(themes)}"
                )
            lines.append("")
    else:
        lines.append("  No BUY signals this run.")

    # ── sell recommendations ────────────────────────────────────
    lines.append(
        "─── SELL RECOMMENDATIONS "
        "───────────────────────────────────────────"
    )
    if report["sell_list"]:
        lines.append(
            f"  {'Ticker':<8s} {'Cat':<18s} {'Comp':>5s} "
            f"{'Conf':>5s} {'RSI':>4s} {'RS%':>5s}"
        )
        lines.append("  " + "-" * 48)
        for s in report["sell_list"]:
            ki = s["key_indicators"]
            lines.append(
                f"  {s['ticker']:<8s} "
                f"{str(s['category'])[:18]:<18s} "
                f"{s['composite']:5.2f} {s['confidence']:5.0%} "
                f"{ki['rsi']:4.0f} {ki['rs_percentile']:5.0%}"
            )
    else:
        lines.append("  No SELL signals this run.")

    # ── holds (near-buy watchlist) ──────────────────────────────
    lines.append("")
    lines.append(
        "─── HOLD (top 15 by composite — watchlist) "
        "────────────────────────────"
    )
    for ho in report["hold_list"][:15]:
        lines.append(
            f"  {ho['ticker']:<8s} "
            f"{str(ho['category'])[:18]:<18s} "
            f"composite {ho['composite']:5.2f}"
        )

    # ── snapshot ────────────────────────────────────────────────
    lines.append("")
    lines.append(
        "─── PORTFOLIO SNAPSHOT "
        "─────────────────────────────────────────────"
    )
    lines.append(
        f"  Tickers analysed:  {snap['tickers_analysed']}"
    )
    lines.append(
        f"  BUY:  {snap['buy_count']}    "
        f"SELL:  {snap['sell_count']}    "
        f"HOLD:  {snap['hold_count']}    "
        f"Errors:  {snap['error_count']}"
    )
    lines.append(
        f"  Total buy allocation:  "
        f"${snap['total_buy_dollar']:,.0f}"
    )
    lines.append(
        f"  Cash remaining:        "
        f"${snap['cash_remaining']:,.0f}  "
        f"({snap['cash_pct']:.1f}%)"
    )
    lines.append("")
    lines.append("=" * 72)

    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  4.  HTML REPORT
# ═════════════════════════════════════════════════════════════════

def to_html(report: dict) -> str:
    """
    Render the structured report as self-contained HTML.
    Can be saved to file, opened in a browser, or emailed.
    """
    h    = report["header"]
    r    = report["regime"]
    a    = report["allocation"]
    snap = report["portfolio_snapshot"]
    risk = report["risk"]

    regime_color = _regime_color(r["label"])

    # ── buy rows ────────────────────────────────────────────────
    buy_rows = ""
    for b in report["buy_list"]:
        stop_str = (
            f"${b['stop_price']:.2f}" if b["stop_price"] else "—"
        )
        themes_str = (
            ", ".join(b["themes"]) if b.get("themes") else "—"
        )
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
                T&nbsp;{sc['trend']:+.2f} M&nbsp;{sc['momentum']:+.2f}
                V&nbsp;{sc['volatility']:+.2f} RS&nbsp;{sc['rel_strength']:+.2f}<br>
                RSI&nbsp;{ki['rsi']:.0f} ADX&nbsp;{ki['adx']:.0f}
                RS%ile&nbsp;{ki['rs_percentile']:.0%}
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
        flag_items = "".join(
            f"<li>{f}</li>" for f in risk["flags"]
        )
        cb_banner = ""
        if risk["circuit_breaker"]:
            cb_banner = (
                '<div class="circuit-breaker">'
                "⚠ CIRCUIT BREAKER ACTIVE — "
                "ALL BUYS DOWNGRADED TO HOLD"
                "</div>"
            )
        risk_html = f"""
        <div class="risk-section">
            <h2>⚠ Risk Flags</h2>
            {cb_banner}
            <ul>{flag_items}</ul>
        </div>"""

    # ── assemble page ───────────────────────────────────────────
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
    h2 {{
        color: #8b949e; font-size: 16px; margin: 24px 0 12px;
        border-bottom: 1px solid #30363d; padding-bottom: 6px;
    }}
    .subtitle {{ color: #8b949e; font-size: 14px; }}
    .header-row {{
        display: flex; justify-content: space-between;
        align-items: center; flex-wrap: wrap; gap: 12px;
        margin-bottom: 16px;
    }}
    .regime-badge {{
        display: inline-block; padding: 6px 16px;
        border-radius: 6px; font-weight: 700; font-size: 14px;
        background: {regime_color}22; color: {regime_color};
        border: 1px solid {regime_color};
    }}
    .stat-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 12px; margin: 16px 0;
    }}
    .stat-card {{
        background: #161b22; padding: 12px; border-radius: 8px;
        border: 1px solid #30363d;
    }}
    .stat-card .label {{
        font-size: 11px; color: #8b949e; text-transform: uppercase;
    }}
    .stat-card .value {{
        font-size: 20px; font-weight: 700; color: #e6edf3;
    }}
    table {{ width: 100%; border-collapse: collapse; margin: 8px 0; }}
    th {{
        text-align: left; font-size: 11px; color: #8b949e;
        text-transform: uppercase; padding: 8px 6px;
        border-bottom: 2px solid #30363d;
    }}
    td {{
        padding: 7px 6px; border-bottom: 1px solid #21262d;
        font-size: 13px;
    }}
    tr:hover {{ background: #161b22; }}
    .rank {{ text-align: center; color: #8b949e; }}
    .ticker {{ font-weight: 700; color: #58a6ff; }}
    .cat {{ color: #8b949e; font-size: 12px; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .detail {{ font-size: 11px; color: #8b949e; line-height: 1.4; }}
    .themes {{ color: #a371f7; }}
    .risk-section {{
        background: #2d1b1b; border: 1px solid #f85149;
        border-radius: 8px; padding: 16px; margin: 16px 0;
    }}
    .risk-section h2 {{ color: #f85149; border: none; margin-top: 0; }}
    .risk-section li {{ margin: 4px 0 4px 20px; }}
    .circuit-breaker {{
        background: #f8514922; color: #f85149; padding: 10px;
        border-radius: 6px; font-weight: 700; text-align: center;
        margin-bottom: 12px;
    }}
    .sell-table .ticker {{ color: #f85149; }}
    .guidance {{
        background: #161b22; padding: 12px 16px; border-radius: 8px;
        border-left: 4px solid {regime_color}; margin: 12px 0;
        color: #e6edf3;
    }}
    .footer {{
        margin-top: 32px; padding-top: 16px;
        border-top: 1px solid #30363d;
        font-size: 11px; color: #484f58; text-align: center;
    }}
    @media (max-width: 700px) {{
        body {{ padding: 12px; font-size: 13px; }}
        .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
        table {{ font-size: 11px; }}
        td, th {{ padding: 5px 3px; }}
    }}
</style>
</head>
<body>

<!-- HEADER -->
<div class="header-row">
    <div>
        <h1>{h['title']}</h1>
        <div class="subtitle">{h['subtitle']}  —  {h['date']}</div>
    </div>
    <div class="regime-badge">{r['label'].upper()}</div>
</div>

<!-- STAT CARDS -->
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

<!-- REGIME -->
<h2>Market Regime</h2>
<div class="guidance">{r['guidance']}</div>

<!-- RISK FLAGS -->
{risk_html}

<!-- ALLOCATION -->
<h2>Allocation Targets</h2>
<table>
    <thead><tr><th>Bucket</th><th>Target</th><th>Filled</th></tr></thead>
    <tbody>{alloc_rows}</tbody>
</table>

<!-- BUY RECOMMENDATIONS -->
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

<!-- SELL RECOMMENDATIONS -->
<h2>Sell Recommendations ({snap['sell_count']})</h2>
<table class="sell-table">
    <thead>
        <tr>
            <th>Ticker</th><th>Category</th><th>Comp</th>
            <th>Conf</th><th>RSI</th><th>RS%</th>
        </tr>
    </thead>
    <tbody>{sell_rows if sell_rows else '<tr><td colspan="6">No SELL signals this run.</td></tr>'}</tbody>
</table>

<!-- HOLD / WATCHLIST -->
<h2>Hold / Watchlist (top 20)</h2>
<table>
    <thead>
        <tr><th>Ticker</th><th>Category</th><th>Composite</th></tr>
    </thead>
    <tbody>{hold_rows}</tbody>
</table>

<!-- FOOTER -->
<div class="footer">
    Generated by CASH v1.0 &nbsp;|&nbsp; {h['date']} &nbsp;|&nbsp;
    {snap['tickers_analysed']} tickers analysed &nbsp;|&nbsp;
    {snap['error_count']} errors
</div>

</body>
</html>"""

    return html


# ═════════════════════════════════════════════════════════════════
#  5.  FILE OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def save_text(report: dict, filepath: str) -> str:
    """Save plain-text report to *filepath*.  Returns filepath."""
    text = to_text(report)
    with open(filepath, "w") as f:
        f.write(text)
    return filepath


def save_html(report: dict, filepath: str) -> str:
    """Save HTML report to *filepath*.  Returns filepath."""
    html = to_html(report)
    with open(filepath, "w") as f:
        f.write(html)
    return filepath


def print_report(report: dict) -> None:
    """Print the plain-text report to stdout."""
    print(to_text(report))


# ═════════════════════════════════════════════════════════════════
#  6.  SNAPSHOT NORMALISER
# ═════════════════════════════════════════════════════════════════

def _normalise_snapshot(snap: dict) -> dict:
    """
    Normalise a per-ticker snapshot dict to a consistent schema.

    Handles divergent key names that arise from different
    pipeline phases (scoring vs portfolio construction vs
    signal generation).
    """

    def _get(*keys, default=None):
        for k in keys:
            if k in snap and snap[k] is not None:
                return snap[k]
        return default

    # Sub-scores — may be nested under "sub_scores" or flat
    raw_sub = snap.get("sub_scores", {})
    sub_scores = {
        "trend": _num(
            raw_sub.get("trend", _get("trend_score")), 0.0,
        ),
        "momentum": _num(
            raw_sub.get("momentum", _get("momentum_score")), 0.0,
        ),
        "volatility": _num(
            raw_sub.get("volatility", _get("volatility_score")), 0.0,
        ),
        "rel_strength": _num(
            raw_sub.get(
                "rel_strength",
                _get("rs_score", "relative_strength_score"),
            ),
            0.0,
        ),
    }

    # Indicators — may be nested or flat
    raw_ind = snap.get("indicators", {})
    indicators = {
        "rsi": _num(
            raw_ind.get("rsi", _get("rsi")), 50.0,
        ),
        "adx": _num(
            raw_ind.get("adx", _get("adx")), 20.0,
        ),
        "macd_hist": _num(
            raw_ind.get(
                "macd_hist",
                _get("macd_hist", "macd_histogram"),
            ),
            0.0,
        ),
    }

    # Relative strength — may be nested or flat
    raw_rs = snap.get("rs", {})
    rs = {
        "rs_percentile": _num(
            raw_rs.get("rs_percentile", _get("rs_percentile")),
            0.5,
        ),
        "rs_regime": (
            raw_rs.get("rs_regime")
            or _get("rs_regime")
            or "neutral"
        ),
    }

    return {
        "ticker":         _get("ticker", default="???"),
        "category":       _get("category", default=""),
        "bucket":         _get("bucket", default="core_equity"),
        "themes":         _get("themes", default=[]),
        "close":          float(_num(
            _get("close", "last_close", "price"), 0.0,
        )),
        "composite":      float(_num(
            _get("composite", "composite_score"), 0.0,
        )),
        "confidence":     float(_num(
            _get("confidence"), 0.5,
        )),
        "signal":         _get("signal", default="HOLD"),
        "shares":         int(_num(_get("shares"), 0)),
        "dollar_alloc":   float(_num(
            _get("dollar_alloc", "allocation", "dollar_allocation"),
            0.0,
        )),
        "weight_pct":     float(_num(
            _get("weight_pct", "weight", "portfolio_weight"),
            0.0,
        )),
        "stop_price":     _get("stop_price", "stop", default=None),
        "risk_per_share": _get("risk_per_share", default=None),
        "sub_scores":     sub_scores,
        "indicators":     indicators,
        "rs":             rs,
    }


# ═════════════════════════════════════════════════════════════════
#  7.  PRIVATE HELPERS
# ═════════════════════════════════════════════════════════════════

def _num(val: Any, default: float = 0) -> float:
    """Return *val* if it is a usable number, else *default*."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _fmt_date(dt: Any) -> str:
    """Format a date for display."""
    if isinstance(dt, pd.Timestamp):
        return dt.strftime("%Y-%m-%d")
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    return str(dt)


def _regime_color(label: str) -> str:
    """Hex colour for regime badge."""
    return {
        "bull_confirmed": "#3fb950",
        "bull_cautious":  "#d29922",
        "bear_mild":      "#db6d28",
        "bear_severe":    "#f85149",
    }.get(label, "#8b949e")


def _regime_guidance(label: str) -> str:
    """Human-readable guidance per regime."""
    return {
        "bull_confirmed": (
            "Strong uptrend confirmed.  Favour equity-heavy allocation.  "
            "Lean into momentum, growth sectors, and thematic plays.  "
            "Tighten stops only on extended names."
        ),
        "bull_cautious": (
            "Uptrend intact but showing signs of fatigue.  "
            "Maintain equity exposure but shift toward quality; "
            "begin adding fixed income / alternatives.  "
            "Widen watchlist for rotation opportunities."
        ),
        "bear_mild": (
            "Trend is deteriorating.  Reduce equity exposure significantly.  "
            "Favour defensive sectors (Staples, Utilities, Healthcare), "
            "increase fixed income and gold.  "
            "Only take high-conviction new positions."
        ),
        "bear_severe": (
            "Market in severe drawdown.  Capital preservation is priority.  "
            "Minimal equity exposure — only inverse or ultra-defensive.  "
            "Heavy fixed income and alternatives.  "
            "Circuit breaker may be active."
        ),
    }.get(label, "Regime not recognised — proceed with caution.")


def _regime_description(label: str) -> str:
    """Short description for a regime label."""
    return {
        "bull_confirmed": "Strong uptrend confirmed across breadth indicators",
        "bull_cautious":  "Uptrend intact but showing fatigue",
        "bear_mild":      "Trend deteriorating — defensive posture recommended",
        "bear_severe":    "Severe drawdown — capital preservation priority",
    }.get(label, "Market regime unclear")


def _default_bucket_weights(regime: str) -> dict:
    """Sensible fallback bucket weights when config is unavailable."""
    defaults = {
        "bull_confirmed": {
            "core_equity": 0.55, "tactical": 0.20,
            "fixed_income": 0.10, "alternatives": 0.10,
            "cash": 0.05,
        },
        "bull_cautious": {
            "core_equity": 0.45, "tactical": 0.15,
            "fixed_income": 0.20, "alternatives": 0.10,
            "cash": 0.10,
        },
        "bear_mild": {
            "core_equity": 0.25, "tactical": 0.10,
            "fixed_income": 0.30, "alternatives": 0.15,
            "cash": 0.20,
        },
        "bear_severe": {
            "core_equity": 0.10, "tactical": 0.05,
            "fixed_income": 0.35, "alternatives": 0.20,
            "cash": 0.30,
        },
    }
    return defaults.get(regime, defaults["bull_cautious"])


# ═════════════════════════════════════════════════════════════════
#  8.  PipelineResult BRIDGE HELPERS
# ═════════════════════════════════════════════════════════════════

def _infer_capital_from_snapshots(
    normalised: list[dict],
    meta: dict,
) -> float:
    """
    Infer total portfolio capital from snapshot allocation data.

    The orchestrator writes dollar_alloc and weight_pct into
    snapshots via ``_enrich_snapshots_with_allocations()``, but
    PipelineResult doesn't store capital directly.

    Strategies (in priority order):
      1. Derive from any position's dollar_alloc / weight_pct
      2. Sum all allocations and scale by metadata cash_pct
      3. Default to 100,000
    """
    # Strategy 1: single-position ratio
    for s in normalised:
        da = _num(s.get("dollar_alloc"), 0)
        wp = _num(s.get("weight_pct"), 0)
        if da > 0 and wp > 0.1:
            return da / (wp / 100.0)

    # Strategy 2: aggregate allocations + cash fraction
    total_alloc = sum(
        _num(s.get("dollar_alloc"), 0)
        for s in normalised
        if _num(s.get("dollar_alloc"), 0) > 0
    )
    cash_frac = _num(meta.get("cash_pct"), 0.05)
    if total_alloc > 0 and 0 < cash_frac < 1.0:
        return total_alloc / (1.0 - cash_frac)

    return 100_000


def _backfill_allocations(
    normalised: list[dict],
    target_weights: dict[str, float],
    capital: float,
) -> None:
    """
    Enrich normalised snapshots with allocation data from
    ``target_weights`` when not already present.

    Modifies *normalised* in place.
    """
    for s in normalised:
        ticker = s.get("ticker", "")
        weight = target_weights.get(ticker, 0.0)

        if weight > 0 and _num(s.get("dollar_alloc"), 0) == 0:
            close = _num(s.get("close"), 0)
            dollar = weight * capital
            shares = int(dollar / close) if close > 0 else 0

            s["weight_pct"]   = round(weight * 100, 2)
            s["dollar_alloc"] = round(dollar, 2)
            s["shares"]       = shares


def _spy_from_scored_universe(
    result: Any,
) -> tuple:
    """
    Extract SPY close and 200-day SMA from the best available source.

    Priority order:
      1. result.bench_df  — always present when orchestrator ran
      2. result.scored_universe["SPY"]  — only if SPY wasn't skipped
      3. result.snapshots  — last resort

    Returns ``(spy_close, spy_sma200, above_sma200)``.
    """
    spy_close    = 0.0
    spy_sma200   = None
    above_sma200 = True

    # ── 1. Benchmark DataFrame (primary — always populated) ───
    bench_df = getattr(result, "bench_df", None)
    if (
        bench_df is not None
        and hasattr(bench_df, "empty")
        and not bench_df.empty
        and "close" in bench_df.columns
    ):
        spy_close = float(bench_df["close"].iloc[-1])
        if len(bench_df) >= 200:
            sma = float(
                bench_df["close"].rolling(200).mean().iloc[-1]
            )
            spy_sma200   = sma
            above_sma200 = spy_close > sma
        return spy_close, spy_sma200, above_sma200

    # ── 2. Scored universe (SPY present if skip_benchmark=False) ─
    scored = getattr(result, "scored_universe", None) or {}
    if "SPY" in scored:
        spy_df = scored["SPY"]
        if (
            spy_df is not None
            and not spy_df.empty
            and "close" in spy_df.columns
        ):
            spy_close = float(spy_df["close"].iloc[-1])
            if len(spy_df) >= 200:
                sma = float(
                    spy_df["close"].rolling(200).mean().iloc[-1]
                )
                spy_sma200   = sma
                above_sma200 = spy_close > sma
            return spy_close, spy_sma200, above_sma200

    # ── 3. Snapshots (last resort) ────────────────────────────
    for s in (getattr(result, "snapshots", None) or []):
        if s.get("ticker") == "SPY":
            spy_close = _num(s.get("close"), 0.0)
            break

    return spy_close, spy_sma200, above_sma200


def _detect_regime_fallback(
    above_sma200: bool,
    breadth_regime: str,
) -> tuple:
    """
    Simple market regime detection from SPY trend and breadth.

    Mirrors ``orchestrator._detect_regime()`` logic so that
    reports generated via the fallback path produce consistent
    regime labels.

    Returns ``(regime_label, regime_description)``.
    """
    if above_sma200 and breadth_regime == "strong":
        return (
            "bull_confirmed",
            "SPY above 200d SMA, breadth strong",
        )
    elif above_sma200:
        return (
            "bull_cautious",
            f"SPY above 200d SMA, breadth {breadth_regime}",
        )
    elif breadth_regime == "weak":
        return (
            "bear_severe",
            "SPY below 200d SMA, breadth weak",
        )
    else:
        return (
            "bear_mild",
            f"SPY below 200d SMA, breadth {breadth_regime}",
        )