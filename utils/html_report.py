"""utils/html_report.py — Build a self-contained HTML report from runner_v2 result."""
from __future__ import annotations

import html as _html
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from common.config import LOGS_DIR

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  COLOUR / STYLE CONSTANTS
# ═══════════════════════════════════════════════════════════════

ACTION_STYLES: dict[str, tuple[str, str]] = {
    "STRONG_BUY": ("#2e7d32", "#e8f5e9"),
    "BUY":        ("#1565c0", "#e3f2fd"),
    "HOLD":       ("#616161", "#f5f5f5"),
    "SELL":       ("#c62828", "#ffebee"),
}

REGIME_STYLES: dict[str, tuple[str, str]] = {
    "leading":   ("#2e7d32", "🟢"),
    "improving": ("#1565c0", "🔵"),
    "weakening": ("#e6a700", "🟡"),
    "lagging":   ("#c62828", "🔴"),
    "unknown":   ("#9e9e9e", "⚪"),
}

VOLFAV_THRESHOLDS = {
    "favorable":   0.55,
    "neutral":     0.35,
    # below 0.35 → unfavorable
}

VOLFAV_STYLES: dict[str, tuple[str, str]] = {
    "favorable":    ("#2e7d32", "#e8f5e9"),
    "neutral":      ("#616161", "#f5f5f5"),
    "unfavorable":  ("#c62828", "#ffebee"),
}

CSS = """
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
     background:#f4f6f9;color:#1a1a2e;line-height:1.5;padding:20px;max-width:1440px;margin:0 auto}
.rh{background:linear-gradient(135deg,#1a237e,#283593);color:#fff;padding:28px 32px;
    border-radius:12px;margin-bottom:24px;box-shadow:0 4px 12px rgba(0,0,0,.15)}
.rh h1{font-size:22px;font-weight:700;margin-bottom:10px}
.rh .meta{display:flex;flex-wrap:wrap;gap:20px;font-size:14px;opacity:.92}
.rh .meta b{opacity:.75;margin-right:4px}
.nav{background:#fff;border-radius:8px;padding:12px 18px;margin-bottom:20px;
     box-shadow:0 1px 4px rgba(0,0,0,.08);display:flex;flex-wrap:wrap;gap:8px}
.nav a{text-decoration:none;color:#1a237e;font-size:13px;font-weight:500;
       padding:4px 12px;border-radius:4px;background:#e8eaf6}
.nav a:hover{background:#c5cae9}
.sec{background:#fff;border-radius:10px;padding:24px;margin-bottom:20px;
     box-shadow:0 1px 4px rgba(0,0,0,.08)}
.sec h2{font-size:17px;font-weight:700;color:#1a237e;margin-bottom:14px;
        padding-bottom:8px;border-bottom:2px solid #e8eaf6}
.og{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px;margin-bottom:16px}
.oc{background:#f8f9ff;border-radius:8px;padding:12px 16px;border-left:3px solid #3f51b5}
.oc .v{font-size:22px;font-weight:700;color:#1a237e}
.oc .l{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px}
.qs{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:14px}
.qi{padding:8px 14px;border-radius:6px;font-size:13px;font-weight:500;border:1px solid #e0e0e0}
.ac{display:flex;flex-wrap:wrap;gap:10px;margin:12px 0}
table.dt{width:100%;border-collapse:collapse;font-size:12px}
table.dt th{background:#f0f2f8;color:#333;font-weight:600;padding:8px 10px;text-align:left;
            border-bottom:2px solid #d0d4e0;font-size:11px;text-transform:uppercase;letter-spacing:.3px}
table.dt td{padding:6px 10px;border-bottom:1px solid #eee;vertical-align:middle}
table.dt tr:hover{background:#f8f9ff}
table.dt tr:nth-child(even){background:#fafbfd}
table.dt tr:nth-child(even):hover{background:#f0f2ff}
.n{text-align:right;font-variant-numeric:tabular-nums}
.tk{font-weight:600;color:#1a237e}
.ft{text-align:center;color:#999;font-size:12px;padding:20px 0;margin-top:10px}
@media(max-width:768px){body{padding:10px}.sec{padding:14px}
 .og{grid-template-columns:repeat(2,1fr)}
 table.dt{font-size:11px}table.dt th,table.dt td{padding:4px 6px}}
</style>
"""

# ═══════════════════════════════════════════════════════════════
#  TINY HELPERS
# ═══════════════════════════════════════════════════════════════

def _e(v) -> str:
    return _html.escape(str(v)) if v is not None else ""


def _col(df: pd.DataFrame, *names) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _ff(v, d: int = 2) -> str:
    try:
        return f"{float(v):.{d}f}"
    except (TypeError, ValueError):
        return "—"


def _fp(v, d: int = 1) -> str:
    try:
        return f"{float(v) * 100:.{d}f}%"
    except (TypeError, ValueError):
        return "—"


def _fsp(v, d: int = 1) -> str:
    try:
        return f"{float(v) * 100:+.{d}f}%"
    except (TypeError, ValueError):
        return "—"


def _badge_action(act: str) -> str:
    key = str(act).upper().replace(" ", "_")
    c, bg = ACTION_STYLES.get(key, ("#616161", "#f5f5f5"))
    lbl = key.replace("_", " ")
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
        f'font-size:11px;font-weight:600;color:{c};background:{bg};'
        f'border:1px solid {c}22">{lbl}</span>'
    )


def _badge_regime(reg: str) -> str:
    r = str(reg).lower().strip()
    c, icon = REGIME_STYLES.get(r, ("#9e9e9e", "⚪"))
    return (
        f'<span style="display:inline-block;padding:1px 6px;border-radius:3px;'
        f'font-size:11px;color:{c}">{icon} {r}</span>'
    )


def _classify_volfav(val) -> tuple[str, float | None]:
    """Return (label, numeric_value) from either a number or a string."""
    # Try numeric first
    try:
        fv = float(val)
        if fv >= VOLFAV_THRESHOLDS["favorable"]:
            return "favorable", fv
        elif fv >= VOLFAV_THRESHOLDS["neutral"]:
            return "neutral", fv
        else:
            return "unfavorable", fv
    except (TypeError, ValueError):
        pass
    # Fallback: it's already a string label
    v = str(val).lower().strip()
    if v in VOLFAV_STYLES:
        return v, None
    return "neutral", None


def _badge_volfav(val) -> str:
    """Coloured badge for volfavorability — handles both numeric and string values."""
    label, numeric = _classify_volfav(val)
    c, bg = VOLFAV_STYLES.get(label, ("#616161", "#f5f5f5"))
    display = f"{label} ({numeric:.2f})" if numeric is not None else label
    return (
        f'<span style="display:inline-block;padding:2px 7px;border-radius:4px;'
        f'font-size:11px;font-weight:500;color:{c};background:{bg}">{_e(display)}</span>'
    )


def _bar(val: float, mx: float = 1.0, w: int = 72) -> str:
    try:
        v = float(val)
    except (TypeError, ValueError):
        return ""
    ratio = max(0.0, min(1.0, v / mx)) if mx > 0 else 0
    if ratio < 0.5:
        red, grn = 200, int(200 * ratio * 2)
    else:
        red, grn = int(200 * (1 - ratio) * 2), 180
    pw = int(ratio * w)
    return (
        f'<div style="display:flex;align-items:center;gap:6px">'
        f'<div style="width:{w}px;height:13px;background:#e0e0e0;border-radius:3px;overflow:hidden">'
        f'<div style="width:{pw}px;height:100%;background:rgb({red},{grn},60);border-radius:3px"></div>'
        f'</div><span style="font-size:12px;font-weight:500">{v:.3f}</span></div>'
    )


# ═══════════════════════════════════════════════════════════════
#  DEEP-SEARCH HELPERS FOR REGIME VALUES
# ═══════════════════════════════════════════════════════════════

def _resolve(result: dict, *paths, default="?"):
    """Walk multiple dot-paths into *result* and return the first truthy hit.

    Each path is a tuple of keys, e.g. ("report_v2","header","breadth_regime").
    Also accepts a plain string key for top-level lookups.
    """
    for p in paths:
        if isinstance(p, str):
            p = (p,)
        obj = result
        for k in p:
            if isinstance(obj, dict):
                obj = obj.get(k)
            else:
                obj = None
                break
        if obj is not None and str(obj).strip() and str(obj).strip() != "?":
            return obj
    return default


def _find_breadth_regime(result: dict) -> str:
    return str(_resolve(
        result,
        ("report_v2", "header", "breadth_regime"),
        ("report_v2", "header", "breadth"),
        ("breadth_info", "regime"),
        ("breadth_info", "breadth_regime"),
        ("regime_info", "breadth_regime"),
        ("regime", "breadth_regime"),
        "breadth_regime",
    ))


def _find_vol_regime(result: dict) -> str:
    return str(_resolve(
        result,
        ("report_v2", "header", "vol_regime"),
        ("report_v2", "header", "volatility_regime"),
        ("report_v2", "header", "volatility"),
        ("vol_info", "regime"),
        ("vol_info", "vol_regime"),
        ("regime_info", "vol_regime"),
        ("regime", "vol_regime"),
        "vol_regime",
        "volatility_regime",
    ))


def _find_vol_favorability(result: dict) -> float | None:
    """Retrieve market-level vol favorability from result dict."""
    raw = _resolve(
        result,
        ("vol_info", "vol_favorability"),
        ("report_v2", "header", "vol_favorability"),
        ("regime_info", "vol_favorability"),
        "vol_favorability",
        default=None,
    )
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _find_breadth_score(result: dict):
    return _resolve(
        result,
        ("report_v2", "header", "breadth_score"),
        ("breadth_info", "score"),
        ("breadth_info", "breadth_score"),
        ("breadth_info", "breadthscore"),
        ("regime_info", "breadth_score"),
        "breadth_score",
        default=None,
    )


def _find_dispersion(result: dict):
    return _resolve(
        result,
        ("report_v2", "header", "dispersion"),
        ("report_v2", "header", "dispersion_20d"),
        ("breadth_info", "dispersion"),
        ("breadth_info", "dispersion20"),
        ("regime_info", "dispersion"),
        "dispersion",
        "dispersion_20d",
        default=None,
    )


# ═══════════════════════════════════════════════════════════════
#  SECTION BUILDERS
# ═══════════════════════════════════════════════════════════════

def _sec_overview(result: dict) -> str:
    rpt = result.get("report_v2", {})
    hdr = rpt.get("header", {})
    pf = rpt.get("portfolio", {})
    act = rpt.get("actions", {})
    mkt = result.get("market", hdr.get("market", "?"))

    breadth_regime = _find_breadth_regime(result)
    vol_regime     = _find_vol_regime(result)
    breadth_score  = _find_breadth_score(result)
    dispersion     = _find_dispersion(result)
    vol_fav        = _find_vol_favorability(result)

    sb = act.get("STRONG_BUY", 0)
    bu = act.get("BUY", 0)
    ho = act.get("HOLD", 0)
    se = act.get("SELL", 0)

    # optional breadth-score / dispersion / vol-fav cards
    extra_cards = ""
    if breadth_score is not None:
        extra_cards += f'<div class="oc"><div class="v">{_ff(breadth_score, 3)}</div><div class="l">Breadth Score</div></div>'
    if dispersion is not None:
        extra_cards += f'<div class="oc"><div class="v">{_ff(dispersion, 4)}</div><div class="l">Dispersion 20d</div></div>'
    if vol_fav is not None:
        vf_label, _ = _classify_volfav(vol_fav)
        vf_c, vf_bg = VOLFAV_STYLES.get(vf_label, ("#616161", "#f5f5f5"))
        extra_cards += (
            f'<div class="oc" style="border-left-color:{vf_c}">'
            f'<div class="v" style="color:{vf_c}">{_ff(vol_fav, 3)}</div>'
            f'<div class="l">Vol Favorability ({vf_label})</div></div>'
        )

    return f"""
    <div class="sec" id="overview">
      <h2>📊 Market Overview — {_e(mkt)}</h2>
      <div class="og">
        <div class="oc"><div class="v">{_e(breadth_regime)}</div><div class="l">Breadth</div></div>
        <div class="oc"><div class="v">{_e(vol_regime)}</div><div class="l">Volatility</div></div>
        <div class="oc"><div class="v">{_fp(pf.get('target_exposure',0),0)}</div><div class="l">Exposure</div></div>
        <div class="oc"><div class="v">{_fp(pf.get('cash_reserve',0),0)}</div><div class="l">Cash</div></div>
        <div class="oc"><div class="v">{hdr.get('processed_names',0)}</div><div class="l">Universe</div></div>
        <div class="oc"><div class="v">{pf.get('candidate_count',0)}</div><div class="l">Candidates</div></div>
        <div class="oc"><div class="v">{pf.get('selected_count',0)}</div><div class="l">Selected</div></div>
        {extra_cards}
      </div>
      <div class="ac">
        <div style="padding:6px 14px;border-radius:6px;font-size:13px;font-weight:600;background:#e8f5e9;color:#2e7d32">STRONG BUY: {sb}</div>
        <div style="padding:6px 14px;border-radius:6px;font-size:13px;font-weight:600;background:#e3f2fd;color:#1565c0">BUY: {bu}</div>
        <div style="padding:6px 14px;border-radius:6px;font-size:13px;font-weight:600;background:#f5f5f5;color:#616161">HOLD: {ho}</div>
        <div style="padding:6px 14px;border-radius:6px;font-size:13px;font-weight:600;background:#ffebee;color:#c62828">SELL: {se}</div>
      </div>
    </div>"""


def _sec_rotation(result: dict) -> str:
    ss = result.get("sector_summary", pd.DataFrame())
    if not isinstance(ss, pd.DataFrame) or ss.empty:
        return ""
    grp: dict[str, list[str]] = {}
    for _, r in ss.iterrows():
        grp.setdefault(str(r.get("regime", "unknown")), []).append(str(r.get("sector", "?")))
    qh = '<div class="qs">'
    for q in ("leading", "improving", "weakening", "lagging"):
        m = grp.get(q, [])
        c, ic = REGIME_STYLES.get(q, ("#9e9e9e", "⚪"))
        qh += (
            f'<div class="qi" style="border-color:{c};background:{c}10">'
            f"{ic} <b>{q.upper()}</b> ({len(m)}) {', '.join(_e(x) for x in m)}</div>"
        )
    qh += "</div>"
    mx = max(0.80, ss.get("blended_score", pd.Series([0.80])).max() * 1.15)
    rows = ""
    for i, (_, r) in enumerate(ss.iterrows(), 1):
        rows += f"""<tr>
          <td class="n">{i}</td><td><b>{_e(r.get('sector','?'))}</b></td>
          <td class="tk">{_e(r.get('etf','?'))}</td>
          <td>{_badge_regime(str(r.get('regime','')))}</td>
          <td>{_bar(float(r.get('blended_score',0)), mx)}</td>
          <td class="n">{_ff(r.get('rs_level',0),4)}</td>
          <td class="n">{_ff(r.get('rs_mom',0),4)}</td>
          <td class="n">{_ff(r.get('etf_composite',0),3)}</td>
          <td class="n">{_fsp(r.get('excess_20d',0))}</td>
          <td>{_badge_regime(str(r.get('rrg_quadrant','')))}</td></tr>"""
    return f"""
    <div class="sec" id="rotation">
      <h2>📊 Sector Rotation — Blended RRG + ETF Composite</h2>
      {qh}
      <table class="dt"><thead><tr>
        <th>#</th><th>Sector</th><th>ETF</th><th>Regime</th><th>Blended</th>
        <th>RS Lvl</th><th>RS Mom</th><th>ETF Scr</th><th>Excess 20d</th><th>RRG</th>
      </tr></thead><tbody>{rows}</tbody></table>
    </div>"""


def _sec_etf(result: dict, max_rows: int = 40) -> str:
    er = result.get("etf_ranking", pd.DataFrame())
    if not isinstance(er, pd.DataFrame) or er.empty:
        return ""
    cc = "etf_composite"
    if cc not in er.columns:
        return ""
    d = er.copy()
    if "is_regional" in d.columns:
        d = d[~d["is_regional"]].copy()
    d = d.head(max_rows)
    mx = max(0.6, d[cc].max() * 1.10) if not d.empty else 0.8
    n = len(er)
    mn = er[cc].mean()
    rows = ""
    for i, (_, r) in enumerate(d.iterrows(), 1):
        tk = str(r.get("ticker", "?"))
        mark = ""
        if r.get("is_sector_etf"):
            mark = ' <span style="color:#1a237e;font-size:10px">●</span>'
        elif r.get("is_broad"):
            mark = ' <span style="color:#999;font-size:10px">○</span>'
        rows += f"""<tr>
          <td class="n">{i}</td><td class="tk">{_e(tk)}{mark}</td>
          <td>{_e(str(r.get('theme',''))[:20])}</td>
          <td>{_e(str(r.get('parent_sector',''))[:18])}</td>
          <td>{_bar(float(r.get(cc,0)), mx)}</td>
          <td class="n">{_ff(r.get('sub_momentum',0),3)}</td>
          <td class="n">{_ff(r.get('sub_trend',0),3)}</td>
          <td class="n">{_ff(r.get('sub_participation',0),3)}</td>
          <td class="n">{_ff(r.get('rsi14',50),1)}</td>
          <td class="n">{_ff(r.get('relativevolume',1),2)}</td>
          <td class="n">{_fsp(r.get('return20d',0))}</td></tr>"""
    return f"""
    <div class="sec" id="etf">
      <h2>📈 ETF Ranking — by Composite Score</h2>
      <p style="margin-bottom:12px;color:#666;font-size:13px">
        Scored: <b>{n}</b> &nbsp;|&nbsp; Mean: <b>{mn:.3f}</b></p>
      <table class="dt"><thead><tr>
        <th>#</th><th>Ticker</th><th>Theme</th><th>Sector</th><th>Score</th>
        <th>Mom</th><th>Trend</th><th>Part</th><th>RSI</th><th>RVol</th><th>Ret 20d</th>
      </tr></thead><tbody>{rows}</tbody></table>
    </div>"""


# ── generic names-table builder used by portfolio / buys / universe ──

def _names_table(df: pd.DataFrame, tid: str = "") -> str:
    if df is None or df.empty:
        return '<p style="color:#999">No data.</p>'
    tc  = _col(df, "ticker", "symbol")
    sc  = _col(df, "scoreadjusted_v2", "scorecomposite_v2", "score")
    ac  = _col(df, "action_v2", "action")
    sec = _col(df, "sector")
    rc  = _col(df, "rsregime", "sectrsregime", "rs_regime")
    ri  = _col(df, "rsi14", "rsi_14")
    ad  = _col(df, "adx14", "adx_14")
    rv  = _col(df, "relativevolume", "relative_volume")
    vf  = _col(df, "volfavorability", "vol_favorability")
    tr  = _col(df, "scoretrend")
    pa  = _col(df, "scoreparticipation")
    wc  = _col(df, "weight", "alloc_weight")
    hw  = wc is not None and not df[wc].isna().all()

    # Only show volfavorability column if it actually varies across rows
    show_vf = False
    if vf and not df[vf].isna().all():
        _vf_vals = pd.to_numeric(df[vf], errors="coerce").dropna()
        if not _vf_vals.empty:
            show_vf = (_vf_vals.max() - _vf_vals.min()) > 0.005

    mx  = max(0.6, df[sc].max() * 1.05) if sc and not df[sc].isna().all() else 0.9

    h = "<tr><th>#</th>"
    if tc:      h += "<th>Ticker</th>"
    if sc:      h += "<th>Score</th>"
    if ac:      h += "<th>Action</th>"
    if sec:     h += "<th>Sector</th>"
    if rc:      h += "<th>RS Regime</th>"
    if ri:      h += "<th>RSI</th>"
    if ad:      h += "<th>ADX</th>"
    if rv:      h += "<th>RVol</th>"
    if show_vf: h += "<th>Vol Fav</th>"
    if tr:      h += "<th>Trend</th>"
    if pa:      h += "<th>Partic.</th>"
    if hw:      h += "<th>Weight</th>"
    h += "</tr>"

    rows = ""
    for i, (_, r) in enumerate(df.iterrows(), 1):
        rows += f'<tr><td class="n">{i}</td>'
        if tc:      rows += f'<td class="tk">{_e(r.get(tc,"?"))}</td>'
        if sc:      rows += f"<td>{_bar(float(r.get(sc,0)), mx)}</td>"
        if ac:      rows += f"<td>{_badge_action(str(r.get(ac,'')))}</td>"
        if sec:     rows += f"<td>{_e(r.get(sec,''))}</td>"
        if rc:      rows += f"<td>{_badge_regime(str(r.get(rc,'')))}</td>"
        if ri:      rows += f'<td class="n">{_ff(r.get(ri),1)}</td>'
        if ad:      rows += f'<td class="n">{_ff(r.get(ad),1)}</td>'
        if rv:      rows += f'<td class="n">{_ff(r.get(rv),2)}</td>'
        if show_vf: rows += f"<td>{_badge_volfav(r.get(vf,''))}</td>"
        if tr:      rows += f'<td class="n">{_ff(r.get(tr),3)}</td>'
        if pa:      rows += f'<td class="n">{_ff(r.get(pa),3)}</td>'
        if hw:      rows += f'<td class="n">{_fp(r.get(wc),1)}</td>'
        rows += "</tr>"
    return f'<table class="dt" id="{tid}"><thead>{h}</thead><tbody>{rows}</tbody></table>'


def _sec_portfolio(result: dict) -> str:
    pf = result.get("portfolio", {})
    sel = pf.get("selected", pd.DataFrame())
    if not isinstance(sel, pd.DataFrame) or sel.empty:
        return ""
    n = pf.get("meta", {}).get("selected_count", len(sel))
    return f"""
    <div class="sec" id="portfolio">
      <h2>🏆 Portfolio Allocations ({n} names)</h2>
      {_names_table(sel, "tbl-port")}
    </div>"""


def _sec_buys(result: dict) -> str:
    at = result.get("action_table", pd.DataFrame())
    if not isinstance(at, pd.DataFrame) or at.empty:
        return ""
    ac = _col(at, "action_v2", "action")
    if ac is None:
        return ""
    mask = at[ac].astype(str).str.upper().isin(["STRONG_BUY", "BUY"])
    bd = at[mask].copy()
    if bd.empty:
        return ""
    return f"""
    <div class="sec" id="buys">
      <h2>📈 All Buy-Rated Names ({len(bd)})</h2>
      {_names_table(bd, "tbl-buys")}
    </div>"""


def _sec_universe(result: dict) -> str:
    at = result.get("action_table", pd.DataFrame())
    if not isinstance(at, pd.DataFrame) or at.empty:
        return ""
    return f"""
    <div class="sec" id="universe">
      <h2>📋 Full Universe — {len(at)} Names</h2>
      {_names_table(at, "tbl-uni")}
    </div>"""


def _sec_exhaustion(result: dict) -> str:
    ex = result.get("selling_exhaustion_table", pd.DataFrame())
    if not isinstance(ex, pd.DataFrame) or ex.empty:
        return ""
    tc  = _col(ex, "ticker", "symbol")
    stc = _col(ex, "status")
    qlc = _col(ex, "quality_label")
    esc = _col(ex, "selling_exhaustion_score")
    rsc = _col(ex, "reversal_trigger_score")
    ric = _col(ex, "rsi_14", "rsi14")
    chc = _col(ex, "price_5d_change")
    sec = _col(ex, "sector")
    h = "<tr><th>#</th>"
    if tc:  h += "<th>Ticker</th>"
    if stc: h += "<th>Status</th>"
    if qlc: h += "<th>Quality</th>"
    if esc: h += "<th>Exh.</th>"
    if rsc: h += "<th>Rev.</th>"
    if ric: h += "<th>RSI</th>"
    if chc: h += "<th>5d Chg</th>"
    if sec: h += "<th>Sector</th>"
    h += "</tr>"
    rows = ""
    for i, (_, r) in enumerate(ex.iterrows(), 1):
        rows += f'<tr><td class="n">{i}</td>'
        if tc:
            rows += f'<td class="tk">{_e(r.get(tc,"?"))}</td>'
        if stc:
            sv = str(r.get(stc, ""))
            sc = "#2e7d32" if "TRIGGERED" in sv.upper() else "#e6a700" if "EARLY" in sv.upper() else "#999"
            rows += f'<td style="color:{sc};font-weight:500;font-size:11px">{_e(sv)}</td>'
        if qlc:
            qv = str(r.get(qlc, ""))
            qc = "#2e7d32" if "HIGH" in qv.upper() else "#e6a700" if qv.upper() == "EARLY" else "#999"
            rows += f'<td style="color:{qc};font-weight:500">{_e(qv)}</td>'
        if esc: rows += f'<td class="n">{_ff(r.get(esc),0)}</td>'
        if rsc: rows += f'<td class="n">{_ff(r.get(rsc),0)}</td>'
        if ric: rows += f'<td class="n">{_ff(r.get(ric),1)}</td>'
        if chc: rows += f'<td class="n">{_fsp(r.get(chc))}</td>'
        if sec: rows += f'<td>{_e(r.get(sec,""))}</td>'
        rows += "</tr>"
    return f"""
    <div class="sec" id="exhaustion">
      <h2>🔻 Selling Exhaustion — {len(ex)} Candidates</h2>
      <table class="dt"><thead>{h}</thead><tbody>{rows}</tbody></table>
    </div>"""


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def build_html_report(result: dict[str, Any], market: str) -> str:
    """Return a complete, self-contained HTML string."""
    now = datetime.now()
    ds = now.strftime("%Y-%m-%d")
    ts = now.strftime("%H:%M:%S")

    body = "".join([
        _sec_overview(result),
        _sec_rotation(result),
        _sec_etf(result),
        _sec_portfolio(result),
        _sec_buys(result),
        _sec_universe(result),
        _sec_exhaustion(result),
    ])

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Smart Money Rotation — {_e(market)} — {ds}</title>
{CSS}
</head><body>
<div class="rh"><h1>Smart Money Rotation Report</h1>
<div class="meta"><span><b>Market:</b> {_e(market)}</span>
<span><b>Date:</b> {ds}</span><span><b>Generated:</b> {ts}</span></div></div>
<div class="nav">
  <a href="#overview">Overview</a><a href="#rotation">Rotation</a>
  <a href="#etf">ETFs</a><a href="#portfolio">Portfolio</a>
  <a href="#buys">Buys</a><a href="#universe">Universe</a>
  <a href="#exhaustion">Exhaustion</a>
</div>
{body}
<div class="ft">Smart Money Rotation &bull; {ds} {ts} &bull; {_e(market)}</div>
</body></html>"""


def save_html_report(html_content: str, market: str) -> Path:
    """Write HTML to *LOGS_DIR* and return the resolved path."""
    d = Path(LOGS_DIR)
    d.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    p = d / f"report_{market.upper()}_{stamp}.html"
    p.write_text(html_content, encoding="utf-8")
    logger.info("HTML report saved: %s  (%d bytes)", p, len(html_content))
    return p.resolve()