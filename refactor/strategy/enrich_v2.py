"""refactor/strategy/enrich_v2.py – Enrich scored tickers with blended rotation data.

Revision notes
--------------
- **Compressed regime scores**: lagging raised from 0.00 → 0.15,
  weakening from 0.30 → 0.40, improving from 0.65 → 0.70.
  This reduces the punitive spread while still meaningfully
  differentiating sectors.
- **Incremental composite update**: instead of recomputing the full
  composite from sub-scores (which silently drops the leadership
  boost applied in pipeline_v2), only the rotation delta is applied.
  This preserves all prior adjustments and keeps the enrichment
  delta small and predictable.
"""
from __future__ import annotations

import logging
import math
from typing import Any

import pandas as pd

from common.sector_map import get_sector_or_class

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  REGIME → scorerotation mapping
# ═══════════════════════════════════════════════════════════════
#  Old behaviour: leading=1.0, everything else=0.0
#  V2 behaviour:  graded so "improving" and "weakening" get partial
#                 credit; lagging has a non-zero floor so the
#                 rotation penalty doesn't dominate the composite.
#
#  These values are intentionally aligned with the initial rotation
#  scores set in scoring_v2.py so that the enrichment delta is
#  small (dominated by the ETF composite boost, not a regime
#  reclassification).

DEFAULT_REGIME_SCORES: dict[str, float] = {
    "leading":    1.00,
    "improving":  0.70,       # ← CHANGED from 0.65
    "weakening":  0.40,       # ← CHANGED from 0.30
    "lagging":    0.15,       # ← CHANGED from 0.00
    "unknown":    0.30,       # ← CHANGED from 0.15
}


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════
#  Sector lookup helper
# ═══════════════════════════════════════════════════════════════

def _resolve_sector(row: dict | pd.Series, ticker: str) -> str:
    """
    Try multiple column names to find the sector for a ticker row.
    Falls back to the common.sector_map lookup.
    """
    for col in ("sector", "gics_sector", "industry_sector", "sector_name"):
        val = row.get(col) if isinstance(row, dict) else getattr(row, col, None)
        if val is not None and str(val).strip() and str(val).strip().lower() != "nan":
            return str(val).strip()
    # Fallback to static map
    mapped = get_sector_or_class(ticker)
    if mapped:
        return mapped
    return "Unknown"


# ═══════════════════════════════════════════════════════════════
#  ETF composite boost
# ═══════════════════════════════════════════════════════════════

def _compute_etf_boost(
    sector: str,
    sector_summary: pd.DataFrame,
    etf_ranking: pd.DataFrame,
) -> float:
    """
    Optional small boost/penalty from the ETF composite score for the
    sector's canonical ETF.  Returns a value in [-0.10, +0.10] that
    can be added to scorerotation.

    If the sector ETF composite is above the universe median → positive.
    If below → negative.  Magnitude scaled by distance from median.
    """
    if sector_summary is None or sector_summary.empty:
        return 0.0

    # Find this sector's row in sector_summary
    match = sector_summary[sector_summary["sector"] == sector]
    if match.empty:
        return 0.0

    etf_composite = _safe_float(match.iloc[0].get("etf_composite"), 0.5)

    # Universe median from etf_ranking
    if etf_ranking is not None and not etf_ranking.empty and "etf_composite" in etf_ranking.columns:
        median = etf_ranking["etf_composite"].median()
    else:
        median = 0.50

    # Scale: distance from median, capped at ±0.10
    raw = (etf_composite - median) * 0.50  # half the distance
    return max(-0.10, min(0.10, raw))


# ═══════════════════════════════════════════════════════════════
#  Main enrichment function
# ═══════════════════════════════════════════════════════════════

def enrich_with_rotation(
    scored_df: pd.DataFrame,
    rotation_result: dict[str, Any],
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Enrich a scored DataFrame with blended sector rotation data.

    Reads from rotation_result:
      - sector_regimes   : {sector_name: regime_str}
      - ticker_regimes   : {ticker: regime_str}
      - sector_summary   : DataFrame with per-sector detail
      - etf_ranking      : DataFrame with per-ETF scores

    Updates / adds columns on scored_df:
      - rotation_regime      : str  ("leading", "improving", …)
      - scorerotation        : float (graded 0.0–1.0, replaces old binary)
      - etf_boost            : float (±0.10 from ETF composite vs median)
      - rotation_blended     : float (blended score from sector_summary)
      - scorecomposite_v2    : float (incrementally updated)

    Returns the enriched DataFrame (modified in place for efficiency,
    but also returned for chaining).
    """
    params = params or {}
    regime_scores = params.get("regime_scores", DEFAULT_REGIME_SCORES)
    apply_etf_boost = params.get("apply_etf_boost", True)
    recompute_composite = params.get("recompute_composite", True)

    sector_regimes: dict[str, str] = rotation_result.get("sector_regimes", {})
    ticker_regimes: dict[str, str] = rotation_result.get("ticker_regimes", {})
    sector_summary: pd.DataFrame = rotation_result.get("sector_summary", pd.DataFrame())
    etf_ranking: pd.DataFrame = rotation_result.get("etf_ranking", pd.DataFrame())

    if scored_df is None or scored_df.empty:
        logger.warning("enrich_with_rotation: scored_df is empty, nothing to enrich")
        return scored_df

    n = len(scored_df)
    ticker_col = None
    for col in ("ticker", "symbol"):
        if col in scored_df.columns:
            ticker_col = col
            break
    if ticker_col is None:
        logger.warning("enrich_with_rotation: no ticker column found in scored_df")
        return scored_df

    # ── Save old scorerotation for incremental update ─────────────────── # ← CHANGED
    if "scorerotation" in scored_df.columns:
        old_rotation = scored_df["scorerotation"].copy()
        old_mean = float(old_rotation.mean())
        old_median = float(old_rotation.median())
    else:
        old_rotation = pd.Series(0.0, index=scored_df.index)
        old_mean = old_median = 0.0

    # ── Build sector lookup from sector_summary for ETF boost ─────────────
    sector_etf_composite: dict[str, float] = {}
    if isinstance(sector_summary, pd.DataFrame) and not sector_summary.empty:
        for _, srow in sector_summary.iterrows():
            sec = str(srow.get("sector", ""))
            if sec:
                sector_etf_composite[sec] = _safe_float(srow.get("etf_composite"), 0.5)

    # ── Enrich each row ───────────────────────────────────────────────────
    regimes: list[str] = []
    rot_scores: list[float] = []
    etf_boosts: list[float] = []
    blended_vals: list[float] = []

    for _, row in scored_df.iterrows():
        ticker = str(row[ticker_col])

        # 1. Resolve regime
        regime = ticker_regimes.get(ticker)
        if regime is None:
            sector = _resolve_sector(row, ticker)
            regime = sector_regimes.get(sector, "unknown")
        regimes.append(regime)

        # 2. Graded scorerotation
        base_score = regime_scores.get(regime, regime_scores.get("unknown", 0.30))

        # 3. ETF composite boost
        if apply_etf_boost:
            sector = _resolve_sector(row, ticker)
            boost = _compute_etf_boost(sector, sector_summary, etf_ranking)
        else:
            boost = 0.0
        etf_boosts.append(round(boost, 4))

        final_rot = max(0.0, min(1.0, base_score + boost))
        rot_scores.append(round(final_rot, 4))

        # 4. Blended score from sector_summary (for diagnostics)
        sector = _resolve_sector(row, ticker)
        blended = 0.0
        if isinstance(sector_summary, pd.DataFrame) and not sector_summary.empty:
            match = sector_summary[sector_summary["sector"] == sector]
            if not match.empty:
                blended = _safe_float(match.iloc[0].get("blended_score"), 0.0)
        blended_vals.append(round(blended, 4))

    scored_df["rotation_regime"] = regimes
    scored_df["scorerotation"] = rot_scores
    scored_df["etf_boost"] = etf_boosts
    scored_df["rotation_blended"] = blended_vals

    # ── Incremental composite update ──────────────────────────────────── # ← CHANGED
    #
    # Instead of recomputing the full composite from sub-scores (which
    # drops the leadership boost applied in pipeline_v2), apply only
    # the rotation delta to the existing composite.  This preserves
    # all prior adjustments and keeps the enrichment effect predictable.
    if recompute_composite and "scorecomposite_v2" in scored_df.columns:
        _incremental_composite_update(scored_df, old_rotation, params)

    # ── Post-enrichment stats ─────────────────────────────────────────────
    new_mean = scored_df["scorerotation"].mean()
    new_median = scored_df["scorerotation"].median()

    regime_dist: dict[str, int] = {}
    for r in regimes:
        regime_dist[r] = regime_dist.get(r, 0) + 1

    logger.info(
        "enrich_with_rotation: n=%d  "
        "scorerotation old(mean=%.3f median=%.3f) → new(mean=%.3f median=%.3f)  "
        "regime_dist=%s  etf_boost(mean=%.4f)",
        n,
        old_mean, old_median,
        new_mean, new_median,
        regime_dist,
        sum(etf_boosts) / max(len(etf_boosts), 1),
    )

    return scored_df


# ═══════════════════════════════════════════════════════════════
#  Incremental composite update                    ← CHANGED
# ═══════════════════════════════════════════════════════════════

def _incremental_composite_update(
    scored_df: pd.DataFrame,
    old_rotation: pd.Series,
    params: dict,
) -> None:
    """
    Apply the scorerotation delta to scorecomposite_v2 *incrementally*.

    Instead of recomputing the entire weighted sum (which drops the
    leadership boost and any other post-scoring adjustments), this
    computes::

        delta = (new_scorerotation − old_scorerotation) × rotation_weight
        scorecomposite_v2 += delta

    This preserves all prior adjustments and only changes the
    composite by the rotation enrichment amount.
    """
    rotation_weight = params.get("composite_weights", {}).get(
        "scorerotation", 0.20,
    )

    old_composite_mean = float(scored_df["scorecomposite_v2"].mean())

    delta = (scored_df["scorerotation"] - old_rotation) * rotation_weight
    scored_df["scorecomposite_v2"] = (
        scored_df["scorecomposite_v2"] + delta
    ).clip(0.0, 1.0)

    new_composite_mean = float(scored_df["scorecomposite_v2"].mean())

    logger.info(
        "enrich_with_rotation: recomputed scorecomposite_v2  "
        "mean %.4f → %.4f  (delta=%+.4f)  "
        "[incremental: rotation_weight=%.2f avg_rot_delta=%+.4f]",
        old_composite_mean,
        new_composite_mean,
        new_composite_mean - old_composite_mean,
        rotation_weight,
        float(delta.mean()),
    )