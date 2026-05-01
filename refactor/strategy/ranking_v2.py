"""
refactor/strategy/ranking_v2.py

Candidate filtering and momentum-tilt ranking for the v2 pipeline.

This module implements the same ranking logic used in the backtest's
phase2 tracker, so that the real-world strategy (strategY + pipeline_v2)
produces identical candidate ordering.

Logic:
    1. FILTER: remove candidates with trailing_vol < min_vol
    2. FILTER: remove candidates with RS z-score < min_rs
    3. RANK: blended = tilt * momentum_rank + (1 - tilt) * composite_rank
    4. BOOST: optionally add vol_pref * normalized_vol to blended score
    5. SORT descending by final blended rank
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults (match backtest phase2 tracker) ──────────────────────────────────
DEFAULT_RANKING_PARAMS = {
    "tilt": 0.50,              # weight on momentum_rank vs composite_rank
    "vol_pref": 0.30,          # boost for higher-vol names (options benefit)
    "min_vol": 0.25,           # minimum trailing annualized vol (filter)
    "min_rs": -0.5,            # minimum RS z-score (filter)
    "vol_col": "realizedvol20d",   # column for trailing vol
    "rs_col": "rszscore",          # column for RS z-score
    "score_col": "scoreadjusted_v2",  # primary composite score column
    "momentum_col": "rszscore",       # momentum signal for ranking
}


def rank_and_filter_candidates(
    candidates: pd.DataFrame,
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Filter low-beta/low-RS candidates and re-rank by momentum tilt.

    Parameters
    ----------
    candidates : DataFrame
        Pre-filtered to STRONG_BUY / BUY only. Must have ticker column.
    params : dict, optional
        Override DEFAULT_RANKING_PARAMS keys.

    Returns
    -------
    DataFrame
        Filtered and sorted by blended rank (descending). Additional
        columns added: composite_rank, mom_rank, blended_rank, 
        trailing_vol, ranking_rs.
    """
    if candidates.empty:
        return candidates

    p = {**DEFAULT_RANKING_PARAMS, **(params or {})}
    tilt = p["tilt"]
    vol_pref = p["vol_pref"]
    min_vol = p["min_vol"]
    min_rs = p["min_rs"]
    vol_col = p["vol_col"]
    rs_col = p["rs_col"]
    score_col = p["score_col"]
    momentum_col = p["momentum_col"]

    # Fallback score column
    if score_col not in candidates.columns:
        score_col = "scorecomposite_v2"
    if score_col not in candidates.columns:
        logger.warning(
            "rank_and_filter: no score column found (%s), using zeros",
            p["score_col"],
        )
        candidates = candidates.copy()
        candidates[score_col] = 0.0

    out = candidates.copy()
    n_start = len(out)

    # ── Extract trailing vol and RS ───────────────────────────────────────────
    out["_trailing_vol"] = (
        pd.to_numeric(out.get(vol_col, pd.Series(0.0, index=out.index)), errors="coerce")
        .fillna(0.0)
    )
    out["_ranking_rs"] = (
        pd.to_numeric(out.get(rs_col, pd.Series(0.0, index=out.index)), errors="coerce")
        .fillna(0.0)
    )

    # ── FILTER 1: minimum trailing volatility ─────────────────────────────────
    vol_mask = out["_trailing_vol"] >= min_vol
    filtered_vol = out[~vol_mask]
    if not filtered_vol.empty:
        for _, row in filtered_vol.iterrows():
            ticker = row.get("ticker", "?")
            vol_val = row["_trailing_vol"]
            logger.debug(
                "FILTER %-8s: trailing_vol=%.3f < min_vol=%.3f",
                ticker, vol_val, min_vol,
            )
    out = out[vol_mask].copy()

    # ── FILTER 2: minimum RS z-score ─────────────────────────────────────────
    rs_mask = out["_ranking_rs"] >= min_rs
    filtered_rs = out[~rs_mask]
    if not filtered_rs.empty:
        for _, row in filtered_rs.iterrows():
            ticker = row.get("ticker", "?")
            rs_val = row["_ranking_rs"]
            logger.debug(
                "FILTER %-8s: rs_zscore=%.3f < min_rs=%.3f",
                ticker, rs_val, min_rs,
            )
    out = out[rs_mask].copy()

    n_after = len(out)
    logger.info(
        "rank_and_filter: filtered %d → %d candidates "
        "(removed %d: %d low_vol, %d low_rs)",
        n_start, n_after, n_start - n_after,
        len(filtered_vol), len(filtered_rs),
    )

    if out.empty:
        logger.warning(
            "rank_and_filter: ALL candidates filtered out! "
            "Consider lowering min_vol=%.3f or min_rs=%.3f",
            min_vol, min_rs,
        )
        return out

    # ── RANK: composite rank (from score) ─────────────────────────────────────
    scores = pd.to_numeric(out[score_col], errors="coerce").fillna(0.0)
    out["composite_rank"] = scores.rank(pct=True)

    # ── RANK: momentum rank (from RS z-score) ────────────────────────────────
    momentum = out["_ranking_rs"]
    out["mom_rank"] = momentum.rank(pct=True)

    # ── BLEND: tilt * mom_rank + (1-tilt) * composite_rank ───────────────────
    out["blended_rank"] = tilt * out["mom_rank"] + (1 - tilt) * out["composite_rank"]

    # ── VOL PREFERENCE BOOST ─────────────────────────────────────────────────
    # Normalize vol to [0, 1] within the candidate set, then add scaled boost
    if vol_pref > 0 and out["_trailing_vol"].std() > 1e-6:
        vol_min = out["_trailing_vol"].min()
        vol_max = out["_trailing_vol"].max()
        vol_range = vol_max - vol_min
        if vol_range > 1e-6:
            vol_norm = (out["_trailing_vol"] - vol_min) / vol_range
            out["blended_rank"] = out["blended_rank"] + vol_pref * vol_norm

    # ── SORT descending by blended rank ───────────────────────────────────────
    out = out.sort_values("blended_rank", ascending=False).reset_index(drop=True)

    # ── Rename internal columns for external visibility ──────────────────────
    out["trailing_vol"] = out["_trailing_vol"]
    out["ranking_rs"] = out["_ranking_rs"]
    out = out.drop(columns=["_trailing_vol", "_ranking_rs"], errors="ignore")

    # ── LOGGING: show final ranking ──────────────────────────────────────────
    logger.info(
        "RANKING (tilt=%.2f, vol_pref=%.2f, min_vol=%.3f, min_rs=%.1f):",
        tilt, vol_pref, min_vol, min_rs,
    )
    for _, row in out.iterrows():
        ticker = row.get("ticker", "?")
        logger.info(
            "  %-12s  composite_rank=%.2f  mom_rank=%.2f  "
            "blended=%.3f  score=%.3f  rs=%.2f  vol=%.3f",
            ticker,
            row.get("composite_rank", 0),
            row.get("mom_rank", 0),
            row.get("blended_rank", 0),
            row.get(score_col, 0),
            row.get("ranking_rs", 0),
            row.get("trailing_vol", 0),
        )
    logger.info(
        "  (filtered %d → %d candidates)", n_start, n_after,
    )

    return out