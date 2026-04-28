"""
refactor/report_v2.py

Report builder for the v2 pipeline.

Consumes the dict returned by ``run_pipeline_v2`` and produces:

    1.  A structured ``dict`` (``build_report_v2``) suitable for JSON
        serialisation, dashboards, or downstream programmatic use.
    2.  A plain-text rendering (``to_text_v2``) for logging, email, or
        terminal display.

The report is organised into sections:

    header              – market, universe sizes, processing counts.
    regime              – breadth, volatility, target exposure.
    rotation            – sector rotation heatmap and quadrant summary.
    scoring             – sub-score distribution statistics.
    actions             – action count breakdown.
    portfolio           – selected names, weights, sector tilt,
                          rotation exposure.
    review              – top-ranked review table rows.
    selling_exhaustion  – reversal watch candidates.
    skipped             – names excluded from scoring and why.
"""
from __future__ import annotations

import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def _safe_str(value, default: str = "unknown") -> str:
    if value is None:
        return default
    s = str(value).strip()
    return default if s.lower() in ("", "nan", "none") else s


def _df_shape(df: pd.DataFrame | None) -> tuple[int, int]:
    if df is None:
        return (0, 0)
    return tuple(df.shape)


def _df_cols(df: pd.DataFrame | None) -> list[str]:
    if df is None:
        return []
    return list(df.columns)


def _preview_df(
    df: pd.DataFrame | None,
    cols: list[str],
    n: int = 10,
) -> list[dict]:
    if df is None or df.empty:
        return []
    use = [c for c in cols if c in df.columns]
    if not use:
        return []
    return df[use].head(n).to_dict(orient="records")


def _count_actions(actions: pd.DataFrame | None) -> dict[str, int]:
    summary = {"STRONG_BUY": 0, "BUY": 0, "HOLD": 0, "SELL": 0}
    if actions is None or actions.empty or "action_v2" not in actions.columns:
        return summary
    counts = actions["action_v2"].value_counts(dropna=False).to_dict()
    for k in summary:
        summary[k] = int(counts.get(k, 0))
    return summary


# ── Column-name resolution helper ────────────────────────────────────────
# rotation_v2 uses short names (rs_mom, etf_composite, …) while earlier
# report code assumed longer names (rs_momentum, excess_return_20d, …).
# _resolve_col tries a prioritised list and returns the first match.

def _resolve_col(row_or_dict, *candidates, default=""):
    """Return the value for the first key in *candidates* that exists
    and is not None / NaN.  Works with dicts and pd.Series."""
    for key in candidates:
        val = None
        if isinstance(row_or_dict, dict):
            val = row_or_dict.get(key)
        else:
            val = getattr(row_or_dict, key, None)
        if val is not None:
            try:
                if isinstance(val, float) and math.isnan(val):
                    continue
            except (TypeError, ValueError):
                pass
            return val
    return default


# ═══════════════════════════════════════════════════════════════════════════════
# Section builders
# ═══════════════════════════════════════════════════════════════════════════════

def _build_header(result: dict, latest: pd.DataFrame) -> dict:
    market = result.get("market", "UNKNOWN")
    leadership = result.get("leadership_snapshot", pd.DataFrame())
    return {
        "market": market,
        "tradable_universe_size": len(result.get("scored", pd.DataFrame())),
        "leadership_universe_size": len(leadership) if leadership is not None else 0,
        "processed_names": 0 if latest is None else len(latest),
        "skipped_names": len(result.get("skipped_table", pd.DataFrame())),
        "rsi_field": "rsi14",
    }


def _build_regime_section(meta: dict, breadth_info: dict | None) -> dict:
    breadth_info = breadth_info or {}
    return {
        "breadth_regime": _safe_str(meta.get("breadth_regime", breadth_info.get("breadth_regime"))),
        "breadth_score": _safe_float(breadth_info.get("breadthscore")),
        "vol_regime": _safe_str(meta.get("vol_regime")),
        "target_exposure": _safe_float(meta.get("target_exposure", 0.0)),
        "dispersion20": _safe_float(breadth_info.get("dispersion20")),
    }


def _build_rotation_section(result: dict) -> dict:
    """Build sector rotation heatmap and quadrant summary.

    .. note::
       Column names emitted by ``rotation_v2`` are short-form
       (``rs_mom``, ``blended_score``, …).  This function resolves
       both short and legacy long-form names so the heatmap is
       populated regardless of which version of rotation was used.
    """
    sector_summary = result.get("sector_summary", pd.DataFrame())
    sector_regimes = result.get("sector_regimes", {})

    if sector_summary is None or sector_summary.empty:
        return {
            "available": False,
            "heatmap": [],
            "quadrant_counts": {},
        }

    # ── Columns we want in the heatmap (short-form preferred) ─────────
    # We list both the canonical short name AND legacy aliases so that
    # the intersection with actual columns picks up whatever is present.
    display_cols = [
        "sector", "etf", "regime",
        "rs_rank", "momentum_rank",
        "rs_level",
        "rs_mom", "rs_momentum",                         # ← CHANGED: added short-form
        "blended_score",                                  # ← ADDED
        "rrg_quadrant",                                   # ← ADDED
        "etf_composite",                                  # ← ADDED
        "theme_avg_score",                                # ← ADDED
        "excess_20d", "excess_return_20d",                # ← CHANGED: try both names
        "excess_ret_20d", "ret_vs_bench_20d",             # ← ADDED extra fallbacks
    ]
    cols = [c for c in display_cols if c in sector_summary.columns]

    # Sort by blended_score (descending) or rs_rank (ascending) ────────
    if "blended_score" in sector_summary.columns:                       # ← CHANGED
        sorted_df = sector_summary.sort_values(
            "blended_score", ascending=False,
        )
    elif "rs_rank" in sector_summary.columns:
        sorted_df = sector_summary.sort_values("rs_rank")
    else:
        sorted_df = sector_summary

    heatmap = []
    for _, row in sorted_df.iterrows():
        entry = {}
        for c in cols:
            val = row.get(c)
            if isinstance(val, float) and not math.isnan(val):
                entry[c] = (
                    round(val, 4)
                    if c not in ("rs_rank", "momentum_rank")
                    else int(val)
                )
            elif isinstance(val, (int,)):
                entry[c] = int(val)
            else:
                entry[c] = _safe_str(val, "")
        heatmap.append(entry)

    # Quadrant summary
    quadrant_counts = {}
    if "regime" in sector_summary.columns:
        for regime in ("leading", "improving", "weakening", "lagging"):
            names = sector_summary.loc[
                sector_summary["regime"] == regime, "sector"
            ].tolist()
            quadrant_counts[regime] = {
                "count": len(names),
                "sectors": names,
            }

    return {
        "available": True,
        "heatmap": heatmap,
        "quadrant_counts": quadrant_counts,
    }


def _build_scoring_section(scored: pd.DataFrame) -> dict:
    """Sub-score distribution statistics."""
    if scored is None or scored.empty:
        return {"available": False}

    sub_scores = [
        "scoretrend", "scoreparticipation", "scorerisk",
        "scoreregime", "scorerotation", "scorepenalty",
        "scorecomposite_v2",
    ]
    stats = {}
    for col in sub_scores:
        if col not in scored.columns:
            continue
        s = pd.to_numeric(scored[col], errors="coerce").dropna()
        if s.empty:
            continue
        stats[col] = {
            "mean": round(float(s.mean()), 4),
            "median": round(float(s.median()), 4),
            "min": round(float(s.min()), 4),
            "max": round(float(s.max()), 4),
            "std": round(float(s.std()), 4),
        }

    # Composite distribution buckets
    comp = pd.to_numeric(
        scored.get("scorecomposite_v2", pd.Series(dtype=float)),
        errors="coerce",
    ).dropna()
    buckets = {}
    if not comp.empty:
        buckets = {
            ">=0.75": int((comp >= 0.75).sum()),
            "0.62-0.75": int(((comp >= 0.62) & (comp < 0.75)).sum()),
            "0.50-0.62": int(((comp >= 0.50) & (comp < 0.62)).sum()),
            "<0.50": int((comp < 0.50).sum()),
        }

    return {
        "available": True,
        "sub_scores": stats,
        "composite_buckets": buckets,
    }


def _build_portfolio_section(
    portfolio: dict,
    review: pd.DataFrame,
) -> dict:
    selected = portfolio.get("selected", pd.DataFrame())
    meta = portfolio.get("meta", {}) or {}

    # Selected preview
    selected_preview = []
    if selected is not None and not selected.empty:
        keep = [
            c for c in [
                "selection_rank", "ticker", "portfolio_weight_pct",
                "action_v2", "conviction_v2",
                "scoreadjusted_v2", "scorecomposite_v2",
                "sector", "sectrsregime", "theme",
                "rsi14", "adx14", "relativevolume",
                "effective_sector_cap", "selection_reason",
            ] if c in selected.columns
        ]
        selected_preview = selected[keep].head(15).to_dict(orient="records")

    # Top picks from review table
    top_picks = []
    if review is not None and not review.empty:
        review_keep = [
            c for c in [
                "ticker", "recommendation", "composite_score",
                "score_percentile", "rsi_14", "adx_14",
                "relative_volume", "price_vs_ema30_pct",
                "leadership_strength", "overextended_flag",
                "sector", "sectrsregime", "theme",
                "conviction_v2", "why_this_name",
            ] if c in review.columns
        ]
        top_picks = review[review_keep].head(15).to_dict(orient="records")

    return {
        "selected_count": int(meta.get("selected_count", 0)),
        "candidate_count": int(meta.get("candidate_count", 0)),
        "actual_exposure": _safe_float(meta.get("actual_exposure", 0.0)),
        "cash_reserve": _safe_float(meta.get("cash_reserve", 1.0)),
        "sector_tilt": meta.get("sector_tilt", []),
        "rotation_exposure": meta.get("rotation_exposure", []),
        "top_picks": top_picks,
        "selected_preview": selected_preview,
    }


def _build_exhaustion_section(result: dict) -> dict:
    df = result.get("selling_exhaustion_table", pd.DataFrame())
    if df is None or df.empty:
        return {"available": False, "count": 0, "names": []}

    keep = [
        c for c in [
            "ticker", "status", "quality_label",
            "selling_exhaustion_score", "reversal_trigger_score",
            "rsi_14", "rsi_turn_up_1d", "bullish_close_1d",
            "close_above_prior_high", "volume_reexpansion_1d",
            "relative_volume", "adx_14", "price_5d_change",
            "sector", "theme", "decision_hint",
        ] if c in df.columns
    ]
    names = df[keep].head(15).to_dict(orient="records")

    return {
        "available": True,
        "count": len(df),
        "names": names,
    }


def _build_skipped_section(result: dict) -> dict:
    df = result.get("skipped_table", pd.DataFrame())
    if df is None or df.empty:
        return {"count": 0, "names": []}

    keep = [
        c for c in [
            "ticker", "instrument_type", "status_v2",
            "missing_critical_count_v2", "missing_critical_fields_v2",
            "scoreability_reason_v2", "sector", "sectrsregime",
        ] if c in df.columns
    ]
    names = df[keep].head(20).to_dict(orient="records")
    return {"count": len(df), "names": names}


# ═══════════════════════════════════════════════════════════════════════════════
# Logging helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _log_report_inputs(
    result: dict,
    portfolio: dict,
    selected: pd.DataFrame,
    actions: pd.DataFrame,
    review: pd.DataFrame,
    latest: pd.DataFrame,
) -> None:
    logger.info(
        "build_report_v2 input shapes: selected=%s actions=%s "
        "review=%s latest=%s",
        _df_shape(selected),
        _df_shape(actions),
        _df_shape(review),
        _df_shape(latest),
    )
    logger.info(
        "build_report_v2 keys: portfolio_keys=%s result_keys=%s",
        sorted(list(portfolio.keys())) if isinstance(portfolio, dict) else [],
        sorted(list(result.keys())) if isinstance(result, dict) else [],
    )


def _log_portfolio_meta(meta: dict, market: str) -> None:
    logger.info(
        "build_report_v2 market=%s selected_count=%s "
        "candidate_count=%s target_exposure=%s",
        market,
        meta.get("selected_count", 0),
        meta.get("candidate_count", 0),
        meta.get("target_exposure", 0.0),
    )
    logger.info(
        "build_report_v2 regime meta: breadth_regime=%s vol_regime=%s",
        meta.get("breadth_regime", "unknown"),
        meta.get("vol_regime", "unknown"),
    )


def _log_review_summary(review: pd.DataFrame | None) -> None:
    if review is None or review.empty:
        logger.warning("build_report_v2: review table is empty")
        return
    logger.info(
        "build_report_v2 review summary: rows=%d cols=%d",
        len(review), len(review.columns),
    )
    if "recommendation" in review.columns:
        logger.info(
            "build_report_v2 review recommendations=%s",
            review["recommendation"].value_counts(dropna=False).to_dict(),
        )


def _log_selected_summary(selected: pd.DataFrame | None) -> None:
    if selected is None or selected.empty:
        logger.warning("build_report_v2: selected portfolio is empty")
        return
    logger.info(
        "build_report_v2 selected summary: rows=%d cols=%d",
        len(selected), len(selected.columns),
    )
    if "sector" in selected.columns:
        logger.info(
            "build_report_v2 selected sector=%s",
            selected["sector"].value_counts(dropna=False).to_dict(),
        )
    if "sectrsregime" in selected.columns:
        logger.info(
            "build_report_v2 selected sectrsregime=%s",
            selected["sectrsregime"].value_counts(dropna=False).to_dict(),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def build_report_v2(result: dict) -> dict:
    """
    Build a structured report from the pipeline output dict.

    Parameters
    ----------
    result : dict
        Output of ``run_pipeline_v2``.

    Returns
    -------
    dict
        Structured report with sections: header, regime, rotation,
        scoring, actions, portfolio, selling_exhaustion, skipped.
    """
    portfolio = result.get("portfolio", {}) or {}
    selected = portfolio.get("selected", pd.DataFrame())
    actions = result.get("action_table", pd.DataFrame())
    review = result.get("review_table", pd.DataFrame())
    meta = portfolio.get("meta", {}) or {}
    latest = result.get("latest", pd.DataFrame())
    scored = result.get("scored", pd.DataFrame())
    breadth_info = result.get("breadth_info", {}) or {}
    market = result.get("market", "UNKNOWN")

    _log_report_inputs(result, portfolio, selected, actions, review, latest)
    _log_portfolio_meta(meta, market)
    _log_review_summary(review)
    _log_selected_summary(selected)

    action_summary = _count_actions(actions)
    logger.info("build_report_v2 action summary=%s", action_summary)

    report = {
        "header": _build_header(result, latest),
        "regime": _build_regime_section(meta, breadth_info),
        "rotation": _build_rotation_section(result),
        "scoring": _build_scoring_section(scored),
        "actions": action_summary,
        "portfolio": _build_portfolio_section(portfolio, review),
        "selling_exhaustion": _build_exhaustion_section(result),
        "skipped": _build_skipped_section(result),
    }

    logger.info(
        "build_report_v2 output ready: top_picks=%d selected=%d "
        "exhaustion=%d skipped=%d rotation_heatmap=%d",
        len(report["portfolio"]["top_picks"]),
        len(report["portfolio"]["selected_preview"]),
        report["selling_exhaustion"].get("count", 0),
        report["skipped"].get("count", 0),
        len(report["rotation"].get("heatmap", [])),
    )
    return report


def to_text_v2(report: dict) -> str:
    """
    Render the structured report dict as a human-readable plain-text
    string.
    """
    h = report.get("header", {})
    r = report.get("regime", {})
    rot = report.get("rotation", {})
    sc = report.get("scoring", {})
    a = report.get("actions", {})
    p = report.get("portfolio", {})
    exh = report.get("selling_exhaustion", {})
    skp = report.get("skipped", {})

    sep = "=" * 92
    thin = "-" * 92

    lines = []

    # ── HEADER ────────────────────────────────────────────────────────────────
    lines.append(sep)
    lines.append("  CASH V2 PIPELINE REPORT")
    lines.append(sep)
    lines.append(f"  Market                  : {h.get('market', 'UNKNOWN')}")
    lines.append(f"  Tradable universe       : {h.get('tradable_universe_size', 0)}")
    lines.append(f"  Leadership universe     : {h.get('leadership_universe_size', 0)}")
    lines.append(f"  Processed (scored)      : {h.get('processed_names', 0)}")
    lines.append(f"  Skipped (not scoreable) : {h.get('skipped_names', 0)}")
    lines.append(f"  RSI field               : {h.get('rsi_field', 'rsi14')} (RSI 14)")
    lines.append("")

    # ── REGIME ────────────────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  MARKET REGIME")
    lines.append(thin)
    lines.append(f"  Breadth regime          : {r.get('breadth_regime', 'unknown')}")
    lines.append(f"  Breadth score           : {_safe_float(r.get('breadth_score')):.3f}")
    lines.append(f"  Volatility regime       : {r.get('vol_regime', 'unknown')}")
    lines.append(f"  Target exposure         : {_safe_float(r.get('target_exposure')):.1%}")
    lines.append(f"  Dispersion (20d)        : {_safe_float(r.get('dispersion20')):.4f}")
    lines.append("")

    # ── SECTOR ROTATION ───────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SECTOR ROTATION (RRG Quadrants)")
    lines.append(thin)

    if rot.get("available"):
        # Quadrant summary
        qc = rot.get("quadrant_counts", {})
        for quadrant in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(quadrant, {})
            count = info.get("count", 0)
            sectors = ", ".join(info.get("sectors", [])) or "none"
            lines.append(f"  {quadrant.upper():<12s}  ({count})  {sectors}")
        lines.append("")

        # Heatmap table
        heatmap = rot.get("heatmap", [])
        if heatmap:
            lines.append(
                f"  {'Rank':>4s}  {'Sector':<22s}  {'ETF':<5s}  "
                f"{'Regime':<11s}  {'RS Level':>9s}  {'RS Mom':>8s}  "
                f"{'Blended':>8s}  {'Excess 20d':>10s}"       # ← CHANGED: added Blended
            )
            lines.append("  " + "-" * 88)                     # ← CHANGED: wider rule
            for idx, entry in enumerate(heatmap, 1):
                sector  = _resolve_col(entry, "sector", default="")
                etf     = _resolve_col(entry, "etf", default="")
                regime  = _resolve_col(entry, "regime", default="")
                rs_lvl  = _resolve_col(entry, "rs_level", default="")
                # ── CHANGED: resolve rs_mom with fallback to rs_momentum ──
                rs_mom  = _resolve_col(
                    entry, "rs_mom", "rs_momentum", default="",
                )
                blended = _resolve_col(
                    entry, "blended_score", default="",
                )
                # ── CHANGED: resolve excess with multiple fallback names ──
                excess  = _resolve_col(
                    entry,
                    "excess_20d", "excess_return_20d",
                    "excess_ret_20d", "ret_vs_bench_20d",
                    default="",
                )
                lines.append(
                    f"  {idx:>4d}  {str(sector):<22s}  {str(etf):<5s}  "
                    f"{str(regime):<11s}  "
                    f"{_safe_float(rs_lvl):>9.4f}  "
                    f"{_safe_float(rs_mom):>8.4f}  "
                    f"{_safe_float(blended):>8.4f}  "
                    f"{_safe_float(excess):>10.4f}"
                )
    else:
        lines.append("  Sector rotation data not available")
    lines.append("")

    # ── SCORING SUMMARY ───────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SCORING SUMMARY")
    lines.append(thin)

    if sc.get("available"):
        sub = sc.get("sub_scores", {})
        for col in (
            "scoretrend", "scoreparticipation", "scorerisk",
            "scoreregime", "scorerotation", "scorepenalty",
            "scorecomposite_v2",
        ):
            s = sub.get(col)
            if s:
                lines.append(
                    f"  {col:<22s}  mean={s['mean']:.4f}  "
                    f"median={s['median']:.4f}  "
                    f"min={s['min']:.4f}  max={s['max']:.4f}  "
                    f"std={s['std']:.4f}"
                )

        buckets = sc.get("composite_buckets", {})
        if buckets:
            bucket_str = "  ".join(
                f"{label}: {count}" for label, count in buckets.items()
            )
            lines.append(f"  Distribution: {bucket_str}")
    else:
        lines.append("  Scoring data not available")
    lines.append("")

    # ── ACTIONS ───────────────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  ACTION SUMMARY")
    lines.append(thin)
    lines.append(
        f"  STRONG_BUY={a.get('STRONG_BUY', 0)}  "
        f"BUY={a.get('BUY', 0)}  "
        f"HOLD={a.get('HOLD', 0)}  "
        f"SELL={a.get('SELL', 0)}"
    )
    lines.append(
        f"  Candidates: {p.get('candidate_count', 0)}  "
        f"Selected: {p.get('selected_count', 0)}  "
        f"Exposure: {_safe_float(p.get('actual_exposure')):.1%}  "
        f"Cash reserve: {_safe_float(p.get('cash_reserve')):.1%}"
    )
    lines.append("")

    # ── REVIEW TABLE ──────────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  REVIEW TABLE (Top Picks)")
    lines.append(thin)

    top = p.get("top_picks", [])
    if not top:
        lines.append("  No signals")
    else:
        for i, row in enumerate(top, 1):
            ticker = str(row.get("ticker", "?"))
            rec = str(row.get("recommendation", "?"))
            score = _safe_float(row.get("composite_score"))
            pct = _safe_float(row.get("score_percentile"))
            rsi = _safe_float(row.get("rsi_14"))
            adx = _safe_float(row.get("adx_14"))
            rv = _safe_float(row.get("relative_volume"))
            lead = _safe_float(row.get("leadership_strength"))
            ext = row.get("overextended_flag", "")
            sect = row.get("sector", "Unknown")
            sr = row.get("sectrsregime", "unknown")
            conv = row.get("conviction_v2", "")

            lines.append(
                f"  {i:>2}. {ticker:<10s}  {rec:<10s}  "
                f"score={score:.3f}  pct={pct:.0%}  "
                f"rsi={rsi:.0f}  adx={adx:.0f}  rv={rv:.2f}  "
                f"lead={lead:.2f}  ext={ext}  "
                f"conv={conv}  {sect}({sr})"
            )
    lines.append("")

    # ── SELECTED PORTFOLIO ────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SELECTED PORTFOLIO")
    lines.append(thin)

    sel = p.get("selected_preview", [])
    if not sel:
        lines.append("  No positions selected")
    else:
        for i, row in enumerate(sel, 1):
            ticker = str(row.get("ticker", "?"))
            weight = _safe_float(row.get("portfolio_weight_pct"))
            score = _safe_float(
                row.get("scoreadjusted_v2", row.get("scorecomposite_v2")),
            )
            action = row.get("action_v2", "?")
            conv = row.get("conviction_v2", "")
            sect = row.get("sector", "Unknown")
            sr = row.get("sectrsregime", "unknown")
            theme = row.get("theme", "Unknown")

            lines.append(
                f"  {i:>2}. {ticker:<10s}  wt={weight:>5.1f}%  "
                f"score={score:.3f}  {action:<10s}  conv={conv}  "
                f"{sect}({sr})  theme={theme}"
            )

        # Sector tilt
        tilt = p.get("sector_tilt", [])
        if tilt:
            lines.append("")
            lines.append("  Sector tilt:")
            for entry in tilt:
                lines.append(
                    f"    {entry.get('sector', '?'):<20s}  "
                    f"regime={entry.get('regime', '?'):<10s}  "
                    f"wt={entry.get('weight_pct', 0):>5.1f}%  "
                    f"cap={entry.get('effective_cap_pct', 0):>5.1f}%  "
                    f"room={entry.get('headroom_pct', 0):>+5.1f}%  "
                    f"names={entry.get('count', 0)}"
                )

        # Rotation exposure
        rot_exp = p.get("rotation_exposure", [])
        if rot_exp:
            lines.append("")
            lines.append("  Rotation quadrant exposure:")
            for entry in rot_exp:
                tickers = ", ".join(entry.get("tickers", []))
                lines.append(
                    f"    {entry.get('quadrant', '?'):<10s}  "
                    f"wt={entry.get('weight_pct', 0):>5.1f}%  "
                    f"names={entry.get('count', 0)}  "
                    f"[{tickers}]"
                )
    lines.append("")

    # ── SELLING EXHAUSTION ────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SELLING EXHAUSTION (Reversal Watch)")
    lines.append(thin)

    if exh.get("available"):
        lines.append(f"  Total candidates: {exh.get('count', 0)}")
        for i, row in enumerate(exh.get("names", []), 1):
            ticker = str(row.get("ticker", "?"))
            status = row.get("status", "?")
            quality = row.get("quality_label", "?")
            exh_score = _safe_float(row.get("selling_exhaustion_score"))
            trig = _safe_float(row.get("reversal_trigger_score"))
            rsi_val = _safe_float(row.get("rsi_14"))
            rsi_up = row.get("rsi_turn_up_1d", "")
            bull_close = row.get("bullish_close_1d", "")
            vol_reexp = row.get("volume_reexpansion_1d", "")
            p5d = _safe_float(row.get("price_5d_change"))
            sect = row.get("sector", "Unknown")

            lines.append(
                f"  {i:>2}. {ticker:<10s}  {status:<22s}  "
                f"quality={quality}  exh={exh_score:.0f}  "
                f"trig={trig:.0f}  rsi={rsi_val:.0f}  "
                f"rsi_up={rsi_up}  bull={bull_close}  "
                f"vol_re={vol_reexp}  5d={p5d:+.1%}  {sect}"
            )
    else:
        lines.append("  No selling exhaustion candidates")
    lines.append("")

    # ── SKIPPED NAMES ─────────────────────────────────────────────────────────
    skipped_count = skp.get("count", 0)
    if skipped_count > 0:
        lines.append(thin)
        lines.append(f"  SKIPPED NAMES ({skipped_count})")
        lines.append(thin)
        for row in skp.get("names", []):
            ticker = str(row.get("ticker", "?"))
            reason = row.get("scoreability_reason_v2", "unknown")
            missing = row.get("missing_critical_fields_v2", "")
            sr = row.get("sectrsregime", "unknown")
            lines.append(
                f"  {ticker:<10s}  sect_regime={sr}  "
                f"reason={reason}  missing=[{missing}]"
            )
        lines.append("")

    lines.append(sep)
    lines.append("  END OF REPORT")
    lines.append(sep)

    text = "\n".join(lines)
    logger.info(
        "to_text_v2 complete: sections=8 lines=%d chars=%d",
        len(lines), len(text),
    )
    return text