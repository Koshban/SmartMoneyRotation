"""
utils/display_results.py
════════════════════════
Rich-formatted display of pipeline_v2 results via RunLogger.

    from utils.display_results import print_run_summary
    print_run_summary(result, market, log)
"""
from __future__ import annotations

import pandas as pd
from utils.run_logger import RunLogger


# ── helpers ───────────────────────────────────────────────────

def _resolve(df: pd.DataFrame, *candidates: str) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _fmt(val, spec: str = ".4f", default: str = "—") -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    try:
        return f"{val:{spec}}"
    except (ValueError, TypeError):
        return str(val)


def _action_style(action) -> str:
    a = str(action).upper()
    if "STRONG_BUY" in a:
        return f"[bold green]{action}[/]"
    if "BUY" in a:
        return f"[green]{action}[/]"
    if "HOLD" in a:
        return f"[yellow]{action}[/]"
    if "SELL" in a:
        return f"[red]{action}[/]"
    return str(action)


def _score_style(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    s = f"{val:.4f}"
    if val >= 0.62:
        return f"[bold green]{s}[/]"
    if val >= 0.50:
        return f"[green]{s}[/]"
    if val >= 0.44:
        return f"[yellow]{s}[/]"
    return f"[red]{s}[/]"


def _rsi_style(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    s = f"{val:.1f}"
    if val >= 70:
        return f"[bold red]{s}[/]"
    if val <= 30:
        return f"[bold green]{s}[/]"
    return s


def _rvol_style(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    s = f"{val:.2f}"
    if val >= 1.5:
        return f"[bold green]{s}[/]"
    if val >= 1.0:
        return f"[green]{s}[/]"
    return f"[dim]{s}[/]"


# ── names table (reused for portfolio, buys, universe) ────────

def _names_table(
    log: RunLogger,
    df: pd.DataFrame,
    title: str,
    sym: str,
    score: str,
    action: str,
    rs_regime: str,
    weight_col: str | None = None,
):
    # resolve column name variants
    RSI = _resolve(df, "rsi14", "rsi_14", "RSI14", "RSI_14") or "rsi14"
    ADX = _resolve(df, "adx14", "adx_14", "ADX14", "ADX_14") or "adx14"
    RVOL = _resolve(df, "relativevolume", "rvol", "relvol", "rel_vol") or "relativevolume"
    TREND = _resolve(df, "scoretrend", "score_trend") or "scoretrend"
    PARTIC = _resolve(df, "scoreparticipation", "score_participation") or "scoreparticipation"

    cols = [
        {"header": "#", "justify": "right", "style": "dim"},
        {"header": "Ticker", "style": "bold cyan"},
    ]
    has_weight = weight_col and weight_col in df.columns
    if has_weight:
        cols.append({"header": "Weight", "justify": "right"})
    cols.extend([
        {"header": "Score", "justify": "right"},
        {"header": "Action", "justify": "center"},
        {"header": "RS Regime", "justify": "center"},
        {"header": "RSI", "justify": "right"},
        {"header": "ADX", "justify": "right"},
        {"header": "RVol", "justify": "right"},
        {"header": "Trend", "justify": "right"},
        {"header": "Partic.", "justify": "right"},
    ])

    rows = []
    # add this right before the for loop in _names_table
    #rvol_candidates = [c for c in df.columns if "vol" in c.lower() and "adx" not in c.lower()]
    #log.info(f"[dim]Volume-related columns: {rvol_candidates}[/]")
    for i, (_, row) in enumerate(df.iterrows(), 1):
        r: list[str] = [str(i), str(row.get(sym, "?"))]

        if has_weight:
            w = row.get(weight_col)
            r.append(f"{w:.1%}" if pd.notna(w) else "—")

        r.append(_score_style(row.get(score)))
        r.append(_action_style(row.get(action, "—")))

        regime = row.get(rs_regime, "—")
        r.append(log.regime_badge(regime) if pd.notna(regime) and regime != "—" else "—")

        r.append(_rsi_style(row.get(RSI)))
        r.append(_fmt(row.get(ADX), ".1f"))
        r.append(_rvol_style(row.get(RVOL)))
        r.append(_fmt(row.get(TREND), ".3f"))
        r.append(_fmt(row.get(PARTIC), ".3f"))

        rows.append(r)

    log.table(title, cols, rows)


# ── public entry point ────────────────────────────────────────

def print_run_summary(result: dict, market: str, log: RunLogger):
    """
    Pretty-print the complete pipeline_v2 output.

    Sections
    --------
    1. Market Overview
    2. Portfolio Picks  +  all BUY-rated names
    3. Full Universe (sorted by adjusted score)
    4. Selling Exhaustion watch list
    """
    portfolio = result["portfolio"]
    meta = portfolio["meta"]
    selected_df: pd.DataFrame = portfolio["selected"]
    action_df: pd.DataFrame = result["action_table"]
    exhaustion_df: pd.DataFrame = result.get(
        "selling_exhaustion_table", pd.DataFrame()
    )

    # resolve column names (handles naming variants)
    SYM = _resolve(action_df, "symbol", "ticker", "name") or "symbol"
    SCORE = _resolve(action_df, "scoreadjusted_v2", "composite",
                     "scorecomposite_v2") or "scoreadjusted_v2"
    ACTION = _resolve(action_df, "action_v2", "action") or "action_v2"
    WEIGHT = _resolve(selected_df, "weight", "weight_v2")
    RS_REG = _resolve(action_df, "rsregime", "rs_regime") or "rsregime"

    # ──────────────────────────────────────────────────────────
    # 1.  MARKET OVERVIEW
    # ──────────────────────────────────────────────────────────
    log.h1(f"📊  MARKET OVERVIEW — {market}")
    log.kv("Breadth regime",
           log.regime_badge(meta.get("breadth_regime", "unknown")))
    log.kv("Vol regime", meta.get("vol_regime", "unknown"))
    log.kv("Target exposure", f"{meta.get('target_exposure', 0):.0%}")
    log.kv("Cash reserve", f"{meta.get('cash_reserve', 0):.0%}")
    log.kv("Universe size", meta.get("total_universe", "—"))
    log.kv("Candidates (BUY)", meta.get("candidate_count", "—"))
    log.kv("Selected", meta.get("selected_count", "—"))

    # action distribution one-liner
    if ACTION in action_df.columns:
        dist = action_df[ACTION].value_counts().to_dict()
        parts = []
        if dist.get("STRONG_BUY", 0):
            parts.append(
                f"[bold green]{dist['STRONG_BUY']} STRONG_BUY[/]")
        parts.extend([
            f"[green]{dist.get('BUY', 0)} BUY[/]",
            f"[yellow]{dist.get('HOLD', 0)} HOLD[/]",
            f"[red]{dist.get('SELL', 0)} SELL[/]",
        ])
        log.kv("Actions", "  ".join(parts))

    # rotation exposure
    for r in meta.get("rotation_exposure", []):
        log.kv(f"  {r['quadrant']}",
               f"{r['weight_pct']:.0f}%  ({r['count']} names)")

    # ──────────────────────────────────────────────────────────
    # 2.  TOP RECOMMENDATIONS
    # ──────────────────────────────────────────────────────────
    log.h1("🏆  TOP RECOMMENDATIONS")

    # 2a — portfolio allocations (with weights)
    if not selected_df.empty:
        log.h2(f"Portfolio Allocations  ({len(selected_df)} names)")
        sel_sorted = selected_df.copy()
        if SCORE in sel_sorted.columns:
            sel_sorted = sel_sorted.sort_values(SCORE, ascending=False)
        _names_table(log, sel_sorted, "Selected", SYM, SCORE, ACTION,
                     RS_REG, weight_col=WEIGHT)
    else:
        log.warning("No names selected for portfolio.")

    # 2b — all BUY-rated (superset of selected)
    if ACTION in action_df.columns:
        buy_mask = action_df[ACTION].isin(["BUY", "STRONG_BUY"])
        buy_df = action_df.loc[buy_mask].copy()
        if SCORE in buy_df.columns:
            buy_df = buy_df.sort_values(SCORE, ascending=False)
        if not buy_df.empty:
            log.h2(f"All BUY-Rated Names  ({len(buy_df)})")
            _names_table(log, buy_df, "Buy Candidates", SYM, SCORE,
                         ACTION, RS_REG)

    # ──────────────────────────────────────────────────────────
    # 3.  FULL UNIVERSE
    # ──────────────────────────────────────────────────────────
    log.h1(f"📋  FULL UNIVERSE — {len(action_df)} NAMES")

    universe = action_df.copy()
    if SCORE in universe.columns:
        universe = universe.sort_values(SCORE, ascending=False)

    _names_table(log, universe, "All Names (by score)", SYM, SCORE,
                 ACTION, RS_REG)

    # ──────────────────────────────────────────────────────────
    # 4.  SELLING EXHAUSTION
    # ──────────────────────────────────────────────────────────
    log.h1(f"🔻  SELLING EXHAUSTION — {len(exhaustion_df)} CANDIDATES")

    if exhaustion_df.empty:
        log.info("No selling-exhaustion candidates detected.")
    else:
        ex_sym = _resolve(exhaustion_df, "ticker", "symbol") or "ticker"
        cols = [
            {"header": "#",  "justify": "right", "style": "dim"},
            {"header": "Ticker", "style": "bold cyan"},
            {"header": "Status", "justify": "center"},
            {"header": "Quality", "justify": "center"},
            {"header": "Exh.", "justify": "right"},
            {"header": "Rev.", "justify": "right"},
            {"header": "RSI", "justify": "right"},
            {"header": "5d Chg", "justify": "right"},
            {"header": "Sector"},
        ]
        rows = []
        for i, (_, row) in enumerate(exhaustion_df.iterrows(), 1):
            status = str(row.get("status", "—"))
            quality = str(row.get("quality_label", "—"))

            if quality in ("CONFIRMED", "READY"):
                q_styled = f"[bold green]{quality}[/]"
            elif quality == "EARLY":
                q_styled = f"[yellow]{quality}[/]"
            else:
                q_styled = f"[dim]{quality}[/]"

            if "REVERSAL" in status:
                s_styled = f"[cyan]{status}[/]"
            else:
                s_styled = f"[dim]{status}[/]"

            pchg = row.get("price_5d_change")
            pchg_str = _fmt(pchg, "+.1%")
            if pd.notna(pchg) and pchg < 0:
                pchg_str = f"[red]{pchg_str}[/]"
            elif pd.notna(pchg) and pchg > 0:
                pchg_str = f"[green]{pchg_str}[/]"

            rows.append([
                str(i),
                str(row.get(ex_sym, "?")),
                s_styled,
                q_styled,
                _fmt(row.get("selling_exhaustion_score"), ".0f"),
                _fmt(row.get("reversal_trigger_score"), ".0f"),
                _rsi_style(row.get("rsi_14")),
                pchg_str,
                str(row.get("sector", "—")),
            ])

        log.table("Exhaustion Watch List", cols, rows)

    # ── footer ────────────────────────────────────────────────
    log.divider()
    log.save()