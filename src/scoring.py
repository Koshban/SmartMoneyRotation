# scoring.py
"""
Cross-sectional scoring and ranking engine.
Takes per-ticker indicator DataFrames and produces a final Rotation Score.
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional

from config import (
    BENCHMARKS,
    CATEGORY_WEIGHTS,
    TICKER_THEME,
    UNIVERSE,
    ZSCORE_WINDOW,
)


def cross_sectional_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Z-score each column across all rows (tickers) for a single date snapshot.
    """
    mu = df.mean()
    sigma = df.std()
    return (df - mu) / (sigma + 1e-12)


def build_snapshot(
    indicators: Dict[str, pd.DataFrame],
    date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Pull the latest row from each ticker's indicator DataFrame
    and assemble a cross-sectional snapshot.
    """
    rows = []
    for ticker, ind_df in indicators.items():
        if ticker in BENCHMARKS:
            continue
        if date not in ind_df.index:
            # Try the closest earlier date
            valid = ind_df.index[ind_df.index <= date]
            if len(valid) == 0:
                continue
            date_use = valid[-1]
        else:
            date_use = date

        row = ind_df.loc[date_use].to_dict()
        row["ticker"] = ticker
        row["theme"] = TICKER_THEME.get(ticker, "UNKNOWN")
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).set_index("ticker")


def compute_category_scores(
    snapshot: pd.DataFrame,
    theme_breadth: Dict[str, pd.DataFrame],
    theme_corr: Dict[str, pd.Series],
    score_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Given a cross-sectional snapshot for one date:
      1. Z-score the raw metrics across all tickers.
      2. Average z-scores within each category.
      3. Assign theme-level breadth and correlation.
      4. Compute weighted composite Rotation Score.
    Returns the snapshot augmented with score columns.
    """
    df = snapshot.copy()

    # ── Columns to z-score per category ─────────────────────────────
    trend_cols  = ["ret_5d", "ret_10d", "ret_15d",
                   "dist_ema_30", "dist_sma_50", "dist_avwap"]
    mom_cols    = ["rsi", "macd_hist", "adx"]
    rs_cols     = [c for c in df.columns if c.startswith("rs_")]
    vol_cols    = ["rvol", "obv_slope", "ad_slope"]
    volq_cols   = ["atr_slope", "realized_vol"]
    flow_cols   = ["flow_proxy"]

    # ── Cross-sectional z-scores ────────────────────────────────────
    all_metric_cols = trend_cols + mom_cols + rs_cols + vol_cols + volq_cols + flow_cols
    present_cols = [c for c in all_metric_cols if c in df.columns]
    z = cross_sectional_zscore(df[present_cols].astype(float))
    z.columns = [f"{c}_z" for c in z.columns]
    df = df.join(z)

    # ── Category means ──────────────────────────────────────────────
    def _safe_mean(cols):
        z_names = [f"{c}_z" for c in cols if f"{c}_z" in df.columns]
        if z_names:
            return df[z_names].mean(axis=1)
        return pd.Series(0.0, index=df.index)

    df["score_trend"]    = _safe_mean(trend_cols)
    df["score_momentum"] = _safe_mean(mom_cols)
    df["score_rs"]       = _safe_mean(rs_cols)
    df["score_volume"]   = _safe_mean(vol_cols)

    # Volatility quality: we WANT expanding ATR (trending) but
    # PENALISE excessive realised vol (noisy / blow-up risk).
    atr_z = df.get("atr_slope_z", pd.Series(0.0, index=df.index))
    rv_z  = df.get("realized_vol_z", pd.Series(0.0, index=df.index))
    df["score_vol_quality"] = atr_z - 0.3 * rv_z.clip(upper=2.0)

    # Flow proxy score
    df["score_flow"] = _safe_mean(flow_cols)

    # ── Theme-level breadth ─────────────────────────────────────────
    breadth_vals = []
    corr_vals = []
    for ticker in df.index:
        theme = df.loc[ticker, "theme"]

        # Breadth
        if theme in theme_breadth:
            tb = theme_breadth[theme]
            valid_dates = tb.index[tb.index <= score_date]
            if len(valid_dates) > 0:
                row_b = tb.loc[valid_dates[-1]]
                # Combine 20d and 50d breadth (weighted toward 20d for speed)
                breadth_vals.append(
                    0.6 * row_b["pct_above_20d"] + 0.4 * row_b["pct_above_50d"]
                )
            else:
                breadth_vals.append(0.5)
        else:
            breadth_vals.append(0.5)

        # Intra-theme correlation
        if theme in theme_corr:
            tc = theme_corr[theme]
            valid_dates = tc.index[tc.index <= score_date]
            if len(valid_dates) > 0:
                corr_vals.append(tc.loc[valid_dates[-1]])
            else:
                corr_vals.append(0.5)
        else:
            corr_vals.append(0.5)

    df["theme_breadth"] = breadth_vals
    df["theme_corr"]    = corr_vals

    # Z-score breadth and correlation cross-sectionally
    for col in ["theme_breadth", "theme_corr"]:
        mu = df[col].astype(float).mean()
        sigma = df[col].astype(float).std() + 1e-12
        df[f"{col}_z"] = (df[col].astype(float) - mu) / sigma

    # Breadth score: combine breadth + correlation bonus
    df["score_breadth"] = (
        0.7 * df["theme_breadth_z"] + 0.3 * df["theme_corr_z"]
    )

    # ── Composite Rotation Score ────────────────────────────────────
    w = CATEGORY_WEIGHTS
    df["rotation_score"] = (
        w["trend"]              * df["score_trend"]
        + w["momentum"]         * df["score_momentum"]
        + w["relative_strength"] * df["score_rs"]
        + w["volume_accumulation"] * df["score_volume"]
        + w["breadth"]          * df["score_breadth"]
        + w["volatility_quality"] * df["score_vol_quality"]
        + w["options_proxy"]    * df["score_flow"]
    )

    # ── Liquidity filter: penalise illiquid names ───────────────────
    if "amihud" in df.columns:
        amihud_pctile = df["amihud"].astype(float).rank(pct=True)
        # Top-quartile illiquidity gets a penalty
        penalty = np.where(amihud_pctile > 0.75, -0.15, 0.0)
        df["rotation_score"] += penalty

    # ── Rank ────────────────────────────────────────────────────────
    df["rank"] = df["rotation_score"].rank(ascending=False).astype(int)

    return df.sort_values("rank")


def compute_score_delta(
    today_scores: pd.DataFrame,
    past_scores: Optional[pd.DataFrame],
) -> pd.Series:
    """
    Delta of Rotation Score vs a previous date (e.g. 5 days ago).
    Positive delta = rotation accelerating into this name.
    """
    if past_scores is None or past_scores.empty:
        return pd.Series(0.0, index=today_scores.index, name="score_delta")

    past = past_scores["rotation_score"].reindex(today_scores.index)
    return (today_scores["rotation_score"] - past).fillna(0.0).rename("score_delta")