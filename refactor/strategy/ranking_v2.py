"""refactor/strategy/ranking_v2.py"""
from __future__ import annotations

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


DEFAULT_RANKING_PARAMS = {
    "tilt": 0.50,           # blend weight toward momentum_rank vs composite_rank
    "vol_pref": 0.30,       # bonus for higher-beta names (options leverage)
    "min_beta": 1.3,        # CHANGED: filter on beta, not raw vol
    "min_rs": -0.5,         # minimum RS z-score to keep
    "beta_col": "beta_60d", # NEW: column name for beta
    "rs_col": "rszscore",
    "score_col": "scoreadjusted_v2",
    "momentum_col": "rszscore",
}


def rank_and_filter_candidates(
    candidates: pd.DataFrame,
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Filter and rank BUY/STRONG_BUY candidates by blended momentum + composite
    with a beta preference for options-friendly names.

    Filtering:
      - Remove names with beta < min_beta (low market sensitivity = bad for options)
      - Remove names with RS z-score < min_rs (underperforming benchmark)

    Ranking:
      - composite_rank: percentile rank by composite score (higher = better)
      - momentum_rank:  percentile rank by RS z-score (higher = better)
      - beta_bonus:     percentile rank by beta (higher beta = bonus)
      - blended_rank = tilt * momentum_rank + (1-tilt) * composite_rank + vol_pref * beta_bonus

    Returns filtered + sorted DataFrame (best first).
    """
    p = {**DEFAULT_RANKING_PARAMS, **(params or {})}

    if candidates.empty:
        return candidates

    tilt = float(p["tilt"])
    vol_pref = float(p["vol_pref"])
    min_beta = float(p["min_beta"])
    min_rs = float(p["min_rs"])
    beta_col = str(p["beta_col"])
    rs_col = str(p["rs_col"])
    score_col = str(p["score_col"])
    momentum_col = str(p["momentum_col"])

    df = candidates.copy()
    n_start = len(df)

    # ── Resolve columns with fallbacks ────────────────────────────────────
    if score_col not in df.columns:
        score_col = "scorecomposite_v2" if "scorecomposite_v2" in df.columns else None
    if momentum_col not in df.columns:
        momentum_col = rs_col if rs_col in df.columns else None

    # ── Get beta values ───────────────────────────────────────────────────
    has_beta = beta_col in df.columns
    if has_beta:
        df["_beta"] = pd.to_numeric(df[beta_col], errors="coerce").fillna(0.0)
    else:
        # Fallback: estimate beta from realized vol vs SPY's typical vol (~0.16)
        # This is crude but better than nothing
        vol_col = "realizedvol20d"
        if vol_col in df.columns:
            df["_beta"] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0.16) / 0.16
            logger.warning(
                "ranking_v2: beta_col '%s' not found, estimating from %s / 0.16",
                beta_col, vol_col,
            )
        else:
            df["_beta"] = 1.0
            logger.warning("ranking_v2: no beta or vol column found, assuming beta=1.0")

    # ── Get RS values ─────────────────────────────────────────────────────
    if rs_col in df.columns:
        df["_rs"] = pd.to_numeric(df[rs_col], errors="coerce").fillna(0.0)
    else:
        df["_rs"] = 0.0

    # ── FILTER: beta gate ─────────────────────────────────────────────────
    beta_mask = df["_beta"] >= min_beta
    filtered_beta = df[~beta_mask]
    if not filtered_beta.empty and logger.isEnabledFor(logging.DEBUG):
        for _, row in filtered_beta.iterrows():
            ticker = row.get("ticker", "?")
            logger.debug(
                "  FILTER %-10s: beta=%.3f < min_beta=%.3f",
                ticker, float(row["_beta"]), min_beta,
            )

    # ── FILTER: RS gate ───────────────────────────────────────────────────
    rs_mask = df["_rs"] >= min_rs
    filtered_rs = df[beta_mask & ~rs_mask]
    if not filtered_rs.empty and logger.isEnabledFor(logging.DEBUG):
        for _, row in filtered_rs.iterrows():
            ticker = row.get("ticker", "?")
            logger.debug(
                "  FILTER %-10s: %s=%.3f < min_rs=%.3f",
                ticker, rs_col, float(row["_rs"]), min_rs,
            )

    # ── Apply filters ─────────────────────────────────────────────────────
    combined_mask = beta_mask & rs_mask
    df_filtered = df[combined_mask].copy()

    n_filtered = len(df_filtered)
    n_removed = n_start - n_filtered

    # ── Fallback: if everything filtered, relax to top-beta names ─────────
    if df_filtered.empty:
        logger.warning(
            "RANKING: all %d candidates filtered out (min_beta=%.2f, min_rs=%.2f). "
            "Relaxing to top %d by beta.",
            n_start, min_beta, min_rs, min(5, n_start),
        )
        # Take top 5 by beta regardless of threshold
        df_filtered = df.nlargest(min(5, n_start), "_beta").copy()
        n_filtered = len(df_filtered)

    # ── Compute ranks ─────────────────────────────────────────────────────
    if score_col and score_col in df_filtered.columns:
        scores = pd.to_numeric(df_filtered[score_col], errors="coerce").fillna(0.0)
        df_filtered["_composite_rank"] = scores.rank(pct=True, method="average")
    else:
        df_filtered["_composite_rank"] = 0.5

    if momentum_col and momentum_col in df_filtered.columns:
        mom = pd.to_numeric(df_filtered[momentum_col], errors="coerce").fillna(0.0)
        df_filtered["_momentum_rank"] = mom.rank(pct=True, method="average")
    else:
        df_filtered["_momentum_rank"] = df_filtered["_composite_rank"]

    # Beta bonus: higher beta = higher rank (good for options leverage)
    df_filtered["_beta_rank"] = df_filtered["_beta"].rank(pct=True, method="average")

    # ── Blended rank ──────────────────────────────────────────────────────
    df_filtered["blended_rank"] = (
        tilt * df_filtered["_momentum_rank"]
        + (1.0 - tilt) * df_filtered["_composite_rank"]
        + vol_pref * df_filtered["_beta_rank"]
    )

    # ── Sort by blended rank descending (best first) ──────────────────────
    df_filtered = df_filtered.sort_values("blended_rank", ascending=False).reset_index(drop=True)

    # ── Log ranking table ─────────────────────────────────────────────────
    logger.info(
        "  RANKING (tilt=%.2f, vol_pref=%.2f, min_beta=%.3f, min_rs=%.1f):",
        tilt, vol_pref, min_beta, min_rs,
    )
    for _, row in df_filtered.head(10).iterrows():
        ticker = row.get("ticker", "?")
        score_val = float(row.get(score_col, 0)) if score_col else 0.0
        rs_val = float(row["_rs"])
        beta_val = float(row["_beta"])
        logger.info(
            "    %-14s composite_rank=%.2f  mom_rank=%.2f  beta_rank=%.2f  "
            "blended=%.3f  score=%.3f  rs=%.2f  beta=%.2f",
            ticker,
            float(row["_composite_rank"]),
            float(row["_momentum_rank"]),
            float(row["_beta_rank"]),
            float(row["blended_rank"]),
            score_val,
            rs_val,
            beta_val,
        )
    logger.info("    (filtered %d → %d candidates)", n_start, n_filtered)

    # ── Clean up internal columns ─────────────────────────────────────────
    drop_cols = ["_beta", "_rs", "_composite_rank", "_momentum_rank", "_beta_rank"]
    df_filtered = df_filtered.drop(columns=[c for c in drop_cols if c in df_filtered.columns])

    return df_filtered