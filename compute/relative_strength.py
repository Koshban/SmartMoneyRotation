"""
compute/relative_strength.py
-----------------------------
Relative strength vs benchmark — the core rotation signal (Pillar 1).

Pure functions.  Takes a stock OHLCV DataFrame and a benchmark OHLCV
DataFrame, returns the stock DataFrame with RS columns appended.

No database knowledge.  No scoring opinions.  Just math.

Key concepts
------------
RS ratio   : stock_close / bench_close — rising means outperforming.
RS slope   : linear regression slope of the smoothed RS ratio.
             Positive  = money rotating IN.
             Negative  = money rotating OUT.
RS z-score : standardised slope for cross-ticker comparison.
RS momentum: short-term slope minus long-term slope (acceleration).
RS regime  : categorical label — leading / weakening / lagging / improving.
             "improving" is the sweet spot: early rotation before the crowd.

Why EMA / SMA / Volume here?
-----------------------------
indicators.py smooths the stock's own price.  This module smooths the
RS *ratio* (stock ÷ benchmark) — a completely different series.  Volume
is used to confirm whether RS improvement has institutional participation
or is just low-liquidity drift.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import INDICATOR_PARAMS

# ═══════════════════════════════════════════════════════════════
#  DEFAULTS  (fallback if config keys are missing)
# ═══════════════════════════════════════════════════════════════

_DEFAULTS = {
    "rs_ema_span":               10,
    "rs_sma_span":               50,
    "rs_slope_window":           20,
    "rs_zscore_window":          60,
    "rs_momentum_short":         10,
    "rs_momentum_long":          30,
    "rs_vol_confirm_threshold": 1.3,
    "volume_avg_window":         20,
}


def _p(key: str):
    """Fetch parameter from config, fall back to module default."""
    return INDICATOR_PARAMS.get(key, _DEFAULTS[key])


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window, min_periods=window).mean()


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """Slope of least-squares linear fit over a rolling window."""
    def _slope(arr):
        if len(arr) < window or np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]

    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True
    )


# ═══════════════════════════════════════════════════════════════
#  1. RS RATIO  (raw + smoothed)
# ═══════════════════════════════════════════════════════════════

def add_rs_ratio(
    df: pd.DataFrame,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS ratio = stock close / benchmark close.
    Normalised to 1.0 at the first valid data point so the
    absolute level is interpretable (>1 = outperforming since start).

    EMA smoothing removes daily noise.
    SMA provides a longer-term trend baseline.

    Outputs: rs_raw, rs_ema, rs_sma
    """
    raw = df["close"] / bench_close

    # Normalise to 1.0 at first valid observation
    first_valid = raw.first_valid_index()
    if first_valid is not None:
        raw = raw / raw.loc[first_valid]

    df["rs_raw"] = raw
    df["rs_ema"] = _ema(raw, _p("rs_ema_span"))
    df["rs_sma"] = _sma(raw, _p("rs_sma_span"))
    return df


# ═══════════════════════════════════════════════════════════════
#  2. RS SLOPE — the rotation signal
# ═══════════════════════════════════════════════════════════════

def add_rs_slope(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling linear-regression slope of the smoothed RS ratio.

    This IS the rotation signal:
      slope > 0 → outperforming benchmark → money rotating in
      slope < 0 → underperforming → money rotating out
      slope flips neg→pos → early rotation detected

    Outputs: rs_slope
    """
    if "rs_ema" not in df.columns:
        raise ValueError("Run add_rs_ratio first — rs_ema column missing")

    df["rs_slope"] = _rolling_slope(df["rs_ema"], _p("rs_slope_window"))
    return df


# ═══════════════════════════════════════════════════════════════
#  3. RS Z-SCORE — cross-ticker comparison
# ═══════════════════════════════════════════════════════════════

def add_rs_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Z-score of RS slope over a longer lookback.

    Allows apples-to-apples comparison across tickers:
      z > +1.5  →  strong relative outperformance
      z < -1.5  →  strong relative underperformance
      z flips from -1 to +1  →  meaningful regime change

    Outputs: rs_zscore
    """
    if "rs_slope" not in df.columns:
        raise ValueError("Run add_rs_slope first — rs_slope column missing")

    w         = _p("rs_zscore_window")
    roll_mean = df["rs_slope"].rolling(w).mean()
    roll_std  = df["rs_slope"].rolling(w).std()

    df["rs_zscore"] = (
        (df["rs_slope"] - roll_mean) / roll_std.replace(0.0, np.nan)
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  4. RS MOMENTUM — acceleration of rotation
# ═══════════════════════════════════════════════════════════════

def add_rs_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Short-term RS slope minus long-term RS slope.

    Positive momentum = RS improvement is accelerating.
    This catches early rotation before the slope itself turns
    positive — like a MACD for relative strength.

    Outputs: rs_momentum
    """
    if "rs_ema" not in df.columns:
        raise ValueError("Run add_rs_ratio first — rs_ema column missing")

    slope_short = _rolling_slope(df["rs_ema"], _p("rs_momentum_short"))
    slope_long  = _rolling_slope(df["rs_ema"], _p("rs_momentum_long"))

    df["rs_momentum"] = slope_short - slope_long
    return df


# ═══════════════════════════════════════════════════════════════
#  5. VOLUME-CONFIRMED RS
# ═══════════════════════════════════════════════════════════════

def add_rs_volume_confirmation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks whether improving RS is backed by above-average volume.

    Smart-money rotation shows up as RS improvement on elevated
    volume.  Without volume confirmation, RS improvement could be
    low-liquidity drift — a trap.

    If indicators.py has already run, reuses relative_volume.
    Otherwise computes it here from raw volume.

    Outputs: rs_rel_volume, rs_vol_confirmed
    """
    if "rs_slope" not in df.columns:
        raise ValueError("Run add_rs_slope first — rs_slope column missing")

    threshold = _p("rs_vol_confirm_threshold")

    # Reuse relative_volume from indicators.py if available
    if "relative_volume" in df.columns:
        df["rs_rel_volume"] = df["relative_volume"]
    else:
        vol_avg = _sma(df["volume"], _p("volume_avg_window"))
        df["rs_rel_volume"] = df["volume"] / vol_avg.replace(0.0, np.nan)

    df["rs_vol_confirmed"] = (
        (df["rs_slope"] > 0) & (df["rs_rel_volume"] > threshold)
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  6. RS REGIME — categorical label
# ═══════════════════════════════════════════════════════════════

def add_rs_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorical label based on two dimensions:
      • RS trend  : rs_ema vs rs_sma  (above = uptrend)
      • RS direction: rs_slope sign   (positive = improving)

    Four regimes:
      ┌────────────┬──────────────────┬──────────────────┐
      │            │  slope > 0       │  slope ≤ 0       │
      ├────────────┼──────────────────┼──────────────────┤
      │ EMA > SMA  │  LEADING         │  WEAKENING       │
      │ EMA ≤ SMA  │  IMPROVING  ★    │  LAGGING         │
      └────────────┴──────────────────┴──────────────────┘

      ★ "improving" is the sweet spot for entry — smart money
        is rotating in before the RS line crosses above its
        trend.  This is where the edge lives.

    Outputs: rs_regime
    """
    for col in ("rs_ema", "rs_sma", "rs_slope"):
        if col not in df.columns:
            raise ValueError(
                f"Run prerequisite functions first — {col} missing"
            )

    above_sma = df["rs_ema"] > df["rs_sma"]
    slope_pos = df["rs_slope"] > 0

    conditions = [
        above_sma & slope_pos,     # leading
        above_sma & ~slope_pos,    # weakening
        ~above_sma & ~slope_pos,   # lagging
        ~above_sma & slope_pos,    # improving
    ]
    labels = ["leading", "weakening", "lagging", "improving"]

    df["rs_regime"] = np.select(conditions, labels, default="unknown")
    return df


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_rs(
    stock_df: pd.DataFrame,
    bench_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute all relative-strength metrics for a stock vs its benchmark.

    Parameters
    ----------
    stock_df : pd.DataFrame
        Stock OHLCV, date-indexed, sorted ascending.
        Must have: open, high, low, close, volume.
        May already have indicator columns from indicators.py.

    bench_df : pd.DataFrame
        Benchmark OHLCV (SPY / QQQ / IWM), same format.
        Which benchmark to use per ticker is a pipeline decision,
        not a concern of this module.

    Returns
    -------
    pd.DataFrame
        stock_df (date-aligned) with columns appended:
        rs_raw, rs_ema, rs_sma, rs_slope, rs_zscore,
        rs_momentum, rs_rel_volume, rs_vol_confirmed, rs_regime

    Raises
    ------
    ValueError
        If inputs are empty, missing 'close', or have fewer
        than 30 overlapping dates.
    """
    # ── Validate ────────────────────────────────────────────
    for name, d in [("stock", stock_df), ("bench", bench_df)]:
        if "close" not in d.columns:
            raise ValueError(f"{name}_df missing 'close' column")
        if d.empty:
            raise ValueError(f"{name}_df is empty")

    # ── Align on common dates ───────────────────────────────
    common = stock_df.index.intersection(bench_df.index)
    if len(common) < 30:
        raise ValueError(
            f"Only {len(common)} overlapping dates — need at least 30"
        )

    df          = stock_df.loc[common].copy()
    bench_close = bench_df.loc[common, "close"]

    # ── Compute in dependency order ─────────────────────────
    df = add_rs_ratio(df, bench_close)       # rs_raw, rs_ema, rs_sma
    df = add_rs_slope(df)                    # rs_slope
    df = add_rs_zscore(df)                   # rs_zscore
    df = add_rs_momentum(df)                 # rs_momentum
    df = add_rs_volume_confirmation(df)      # rs_rel_volume, rs_vol_confirmed
    df = add_rs_regime(df)                   # rs_regime

    return df