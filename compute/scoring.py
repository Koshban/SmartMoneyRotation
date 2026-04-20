"""
compute/scoring.py
-------------------
Five-pillar composite scoring engine.

Takes a DataFrame that already has indicator columns (from indicators.py)
and relative-strength columns (from relative_strength.py), plus an
optional breadth score series (from breadth.py), and produces sub-scores
plus a weighted composite score per row (ticker-day).

Pillar 1 — Rotation       : Is smart money rotating into this name?
Pillar 2 — Momentum       : Is price action confirming the rotation?
Pillar 3 — Volatility     : Is risk / reward favorable for entry?
Pillar 4 — Microstructure : Is institutional volume backing the move?
Pillar 5 — Breadth        : Is the broad market confirming the move?

Each pillar returns a Series in [0, 1].
Weighted average → composite in [0, 1].
All weights live in common/config.py → SCORING_WEIGHTS.

When breadth data is unavailable the engine falls back to a four-pillar
mode, renormalising the remaining weights so they still sum to 1.0.

This module does NOT make trade decisions — it ranks.
The strategy layer downstream decides what score threshold triggers action.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SCORING_WEIGHTS, SCORING_PARAMS


# ═══════════════════════════════════════════════════════════════
#  COLUMN NAME MAP
#  Indicators.py outputs specific suffixed names.
#  Map logical names → actual column names so the scoring
#  engine stays decoupled from naming conventions.
# ═══════════════════════════════════════════════════════════════

COL = {
    # Pillar 1 — Rotation (from relative_strength.py)
    "rs_zscore":        "rs_zscore",
    "rs_regime":        "rs_regime",
    "rs_momentum":      "rs_momentum",
    "rs_vol_confirmed": "rs_vol_confirmed",

    # Pillar 2 — Momentum
    "rsi":              "rsi_14",
    "macd_hist":        "macd_hist",
    "adx":              "adx_14",

    # Pillar 3 — Volatility / Risk
    "realized_vol":     "realized_vol_20d",
    "atr":              "atr_14",
    "close":            "close",
    "amihud":           "amihud_20d",

    # Pillar 4 — Microstructure
    "obv":              "obv",
    "obv_slope":        "obv_slope_10d",
    "ad_line":          "ad_line",
    "ad_slope":         "ad_line_slope_10d",
    "relative_volume":  "relative_volume",
}


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSORS
# ═══════════════════════════════════════════════════════════════

def _w(key: str) -> float:
    """Fetch weight from SCORING_WEIGHTS."""
    return SCORING_WEIGHTS[key]


def _sp(key: str):
    """Fetch parameter from SCORING_PARAMS."""
    return SCORING_PARAMS[key]


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _sigmoid(x):
    """Map real values → (0, 1).  Vectorised, clipped to avoid overflow."""
    return 1.0 / (1.0 + np.exp(-np.clip(x, -10, 10)))


def _rolling_pctrank(series: pd.Series, window: int) -> pd.Series:
    """
    Rolling percentile rank in [0, 1].

    0.80 means today's value exceeds 80 % of values in the lookback.
    Self-normalising — no assumptions about scale or distribution.
    """
    def _pct(arr):
        if np.isnan(arr[-1]):
            return np.nan
        return np.sum(arr <= arr[-1]) / len(arr)

    return series.rolling(window, min_periods=window // 2).apply(
        _pct, raw=True
    )


def _col(key: str) -> str:
    """Resolve logical column name → actual DataFrame column name."""
    return COL[key]


# ═══════════════════════════════════════════════════════════════
#  PILLAR 1 — ROTATION
#  "Is smart money rotating into this stock vs benchmark?"
# ═══════════════════════════════════════════════════════════════

def _pillar_rotation(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    rs_zscore        Standardised RS slope → sigmoid.
                     z = 0 → 0.50,  z = +2 → 0.88,  z = -2 → 0.12.

    rs_regime        Categorical phase map:
                     leading 1.0 | improving 0.75 | weakening 0.25
                     lagging 0.0 | unknown   0.50

    rs_momentum      Acceleration of rotation → sigmoid.
                     Positive = RS improvement is speeding up.

    rs_vol_confirmed Binary: is above-average volume backing the move?
    """
    # ── rs_zscore ────────────────────────────────────────────
    zs = pd.Series(
        _sigmoid(df[_col("rs_zscore")].fillna(0).values),
        index=df.index,
    )

    # ── rs_regime ────────────────────────────────────────────
    regime_map = {
        "leading":   1.00,
        "improving": 0.75,
        "weakening": 0.25,
        "lagging":   0.00,
        "unknown":   0.50,
    }
    regime = df[_col("rs_regime")].map(regime_map).fillna(0.5)

    # ── rs_momentum ──────────────────────────────────────────
    mom = pd.Series(
        _sigmoid(
            df[_col("rs_momentum")].fillna(0).values
            * _sp("rs_momentum_scale")
        ),
        index=df.index,
    )

    # ── rs_vol_confirmed ─────────────────────────────────────
    vol_conf = df[_col("rs_vol_confirmed")].astype(float).fillna(0.0)

    return (
        _w("rs_zscore_w")       * zs
        + _w("rs_regime_w")     * regime
        + _w("rs_momentum_w")   * mom
        + _w("rs_vol_confirm_w") * vol_conf
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 2 — MOMENTUM
#  "Is price action confirming the rotation?"
# ═══════════════════════════════════════════════════════════════

def _pillar_momentum(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    RSI   Piecewise linear.  Sweet spot 40-70 (trending, not overbought).
          25-40 ramps up, 70-80 ramps down, extremes score 0.10.

    MACD  Histogram rolling percentile rank.
          Top of range = strong bullish momentum.

    ADX   Piecewise linear.  >25 = trending = good for momentum.
          <15 = choppy = low score.
    """
    # ── RSI ──────────────────────────────────────────────────
    rsi_score = pd.Series(
        np.interp(
            df[_col("rsi")].fillna(50).values,
            [0, 25, 40, 55, 70, 80, 100],
            [0.10, 0.10, 0.60, 1.00, 1.00, 0.50, 0.10],
        ),
        index=df.index,
    )

    # ── MACD histogram ───────────────────────────────────────
    macd_score = _rolling_pctrank(
        df[_col("macd_hist")].fillna(0.0), _sp("rank_window")
    ).fillna(0.5)

    # ── ADX ──────────────────────────────────────────────────
    adx_score = pd.Series(
        np.interp(
            df[_col("adx")].fillna(15).values,
            [0, 10, 15, 25, 40, 60],
            [0.10, 0.20, 0.40, 0.85, 1.00, 1.00],
        ),
        index=df.index,
    )

    return (
        _w("rsi_w")   * rsi_score
        + _w("macd_w") * macd_score
        + _w("adx_w")  * adx_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 3 — VOLATILITY / RISK
#  "Is risk / reward favorable for entry?"
# ═══════════════════════════════════════════════════════════════

def _pillar_volatility(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    realized_vol  Rolling percentile → tent function.
                  Moderate vol (near median) scores highest.
                  Extremes (dead money OR panic) score low.

    atr_pct       ATR ÷ close, same tent logic.
                  Captures intraday range risk vs close-to-close risk.

    amihud        Illiquidity.  Inverted rank — lower is better.
                  Liquid names get capital first.
    """
    rw = _sp("rank_window")

    # ── Realized vol → tent ──────────────────────────────────
    vol_rank  = _rolling_pctrank(
        df[_col("realized_vol")].ffill(), rw
    ).fillna(0.5)
    vol_score = 1.0 - 2.0 * (vol_rank - 0.5).abs()

    # ── ATR percent → tent ───────────────────────────────────
    atr_pct   = df[_col("atr")] / df[_col("close")].replace(0, np.nan)
    atr_rank  = _rolling_pctrank(atr_pct.ffill(), rw).fillna(0.5)
    atr_score = 1.0 - 2.0 * (atr_rank - 0.5).abs()

    # ── Amihud → inverted rank ───────────────────────────────
    amihud_rank  = _rolling_pctrank(
        df[_col("amihud")].ffill(), rw
    ).fillna(0.5)
    amihud_score = 1.0 - amihud_rank

    return (
        _w("realized_vol_w") * vol_score
        + _w("atr_pct_w")   * atr_score
        + _w("amihud_w")    * amihud_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 4 — MICROSTRUCTURE
#  "Is institutional volume backing the move?"
# ═══════════════════════════════════════════════════════════════

def _pillar_microstructure(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    OBV slope    Pre-computed rolling slope → percentile rank.
                 Rising OBV = accumulation (buying pressure).

    A/D slope    Pre-computed rolling slope → percentile rank.
                 Weighted by intra-bar position — more granular than OBV.

    Rel volume   Piecewise linear.  1.5-2.5× average = institutional
                 interest.  >4× could be panic or event-driven.
    """
    rw = _sp("rank_window")

    # ── OBV slope (pre-computed by indicators.py) ────────────
    obv_score = _rolling_pctrank(
        df[_col("obv_slope")].ffill(), rw
    ).fillna(0.5)

    # ── A/D line slope (pre-computed by indicators.py) ───────
    ad_score = _rolling_pctrank(
        df[_col("ad_slope")].ffill(), rw
    ).fillna(0.5)

    # ── Relative volume ──────────────────────────────────────
    rvol_score = pd.Series(
        np.interp(
            df[_col("relative_volume")].fillna(1.0).values,
            [0.0, 0.5, 1.0, 1.5, 2.5, 4.0, 8.0],
            [0.10, 0.20, 0.50, 0.85, 1.00, 0.80, 0.50],
        ),
        index=df.index,
    )

    return (
        _w("obv_slope_w")   * obv_score
        + _w("ad_slope_w")  * ad_score
        + _w("rel_volume_w") * rvol_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 5 — BREADTH
#  "Is the broad market confirming the move?"
# ═══════════════════════════════════════════════════════════════

def _pillar_breadth(
    df: pd.DataFrame,
    breadth_scores: pd.Series,
) -> pd.Series:
    """
    Market breadth overlay.

    The breadth pillar is unique: it is a universe-level signal
    (the same value for every symbol on a given day), not a
    per-symbol indicator.  It acts as a tide gauge — when broad
    participation is strong, all boats get a lift; when breadth
    deteriorates, conviction is dampened across the board.

    Parameters
    ----------
    df : pd.DataFrame
        The per-symbol indicator DataFrame (used only for its index).
    breadth_scores : pd.Series
        Daily breadth scores on a 0–100 scale, as produced by
        ``breadth_to_pillar_scores()`` for this symbol's column.

    Returns
    -------
    pd.Series
        Values in [0, 1] aligned to df's index.
    """
    # Align to the symbol's date index, forward-fill gaps
    # (breadth may have fewer rows if the constituent universe
    # started trading later than this symbol)
    aligned = breadth_scores.reindex(df.index).ffill().fillna(50.0)

    # Rescale 0–100 → 0–1
    return (aligned / 100.0).clip(0, 1)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_composite_score(
    df: pd.DataFrame,
    breadth_scores: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute all pillar sub-scores and the weighted composite.

    Parameters
    ----------
    df : pd.DataFrame
        Date-indexed OHLCV with indicator columns (from
        compute_all_indicators) **and** RS columns (from
        compute_all_rs) already present.
    breadth_scores : pd.Series or None
        Daily breadth score for this symbol (0–100 scale),
        typically one column from the output of
        ``breadth_to_pillar_scores()``.  If None, the engine
        falls back to four-pillar mode with renormalised weights.

    Returns
    -------
    pd.DataFrame
        Input frame with columns appended:
          score_rotation       [0-1]   Pillar 1
          score_momentum       [0-1]   Pillar 2
          score_volatility     [0-1]   Pillar 3
          score_microstructure [0-1]   Pillar 4
          score_breadth        [0-1]   Pillar 5 (if breadth provided)
          score_composite      [0-1]   Weighted average
          score_percentile     [0-1]   Time-series pct rank of composite
          breadth_available    bool    Whether breadth was used

    Raises
    ------
    ValueError
        Missing columns needed by pillars 1–4.
    """
    # ── Validate required columns (pillars 1–4) ──────────────
    required = {
        "pillar_rotation":       [
            _col("rs_zscore"), _col("rs_regime"),
            _col("rs_momentum"), _col("rs_vol_confirmed"),
        ],
        "pillar_momentum":       [
            _col("rsi"), _col("macd_hist"), _col("adx"),
        ],
        "pillar_volatility":     [
            _col("realized_vol"), _col("atr"),
            _col("close"), _col("amihud"),
        ],
        "pillar_microstructure": [
            _col("obv_slope"), _col("ad_slope"),
            _col("relative_volume"),
        ],
    }
    missing = []
    for pillar, cols in required.items():
        for c in cols:
            if c not in df.columns:
                missing.append(f"{c} (needed by {pillar})")
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # ── Determine mode: 5-pillar or 4-pillar fallback ────────
    use_breadth = (
        breadth_scores is not None
        and not breadth_scores.empty
        and "pillar_breadth" in SCORING_WEIGHTS
    )

    pillar_keys = [
        "pillar_rotation", "pillar_momentum",
        "pillar_volatility", "pillar_microstructure",
    ]
    if use_breadth:
        pillar_keys.append("pillar_breadth")

    raw_weights = {k: _w(k) for k in pillar_keys}
    total_w = sum(raw_weights.values())

    # Renormalise so active pillars sum to exactly 1.0
    if total_w <= 0:
        raise ValueError("Pillar weights sum to zero.")
    weights = {k: v / total_w for k, v in raw_weights.items()}

    # ── Compute pillar scores ─────────────────────────────────
    out = df.copy()

    out["score_rotation"]       = _pillar_rotation(out).clip(0, 1)
    out["score_momentum"]       = _pillar_momentum(out).clip(0, 1)
    out["score_volatility"]     = _pillar_volatility(out).clip(0, 1)
    out["score_microstructure"] = _pillar_microstructure(out).clip(0, 1)

    if use_breadth:
        out["score_breadth"] = _pillar_breadth(out, breadth_scores).clip(0, 1)

    # ── Composite ─────────────────────────────────────────────
    composite = (
        weights["pillar_rotation"]       * out["score_rotation"]
        + weights["pillar_momentum"]     * out["score_momentum"]
        + weights["pillar_volatility"]   * out["score_volatility"]
        + weights["pillar_microstructure"] * out["score_microstructure"]
    )

    if use_breadth:
        composite += weights["pillar_breadth"] * out["score_breadth"]

    out["score_composite"] = composite.clip(0, 1)

    # ── Time-series percentile ────────────────────────────────
    out["score_percentile"] = _rolling_pctrank(
        out["score_composite"], 252
    )

    # ── Metadata ──────────────────────────────────────────────
    out["breadth_available"] = use_breadth

    return out