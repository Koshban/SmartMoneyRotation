"""
compute/indicators.py
---------------------
Pure functions that compute technical indicators on OHLCV DataFrames.

No database knowledge, no scoring opinions — just math.

Every function:
  • takes a DataFrame with columns: open, high, low, close, volume
  • returns the same DataFrame with new indicator columns appended
  • pulls default parameters from common.config.INDICATOR_PARAMS

Master entry point:
    df = compute_all_indicators(df)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import INDICATOR_PARAMS

# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

_REQUIRED_COLS = {"open", "high", "low", "close", "volume"}


def _validate_ohlcv(df: pd.DataFrame) -> None:
    """Raise if the DataFrame is missing any required OHLCV columns."""
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    if df.empty:
        raise ValueError("DataFrame is empty")


def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window, min_periods=window).mean()


def _wilder_smooth(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing method (used by RSI, ADX, ATR)."""
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """
    Slope of a least-squares linear fit over a rolling window.
    Positive slope = series trending upward.
    """
    def _slope(arr):
        if len(arr) < window or np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]

    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True
    )


# ═══════════════════════════════════════════════════════════════
#  1. RETURNS
# ═══════════════════════════════════════════════════════════════

def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """N-day percentage returns for each window in config."""
    for w in INDICATOR_PARAMS["return_windows"]:
        df[f"ret_{w}d"] = df["close"].pct_change(w, fill_method=None)
    return df


# ═══════════════════════════════════════════════════════════════
#  2. RSI
# ═══════════════════════════════════════════════════════════════

def add_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Relative Strength Index (Wilder's smoothing).
    Output: rsi_{period}  (0–100 scale)
    """
    period = INDICATOR_PARAMS["rsi_period"]
    delta = df["close"].diff()

    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = _wilder_smooth(gain, period)
    avg_loss = _wilder_smooth(loss, period)

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    df[f"rsi_{period}"] = 100.0 - (100.0 / (1.0 + rs))
    return df


# ═══════════════════════════════════════════════════════════════
#  3. MACD
# ═══════════════════════════════════════════════════════════════

def add_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    MACD line, signal line, histogram.
    Outputs: macd_line, macd_signal, macd_hist
    """
    fast   = INDICATOR_PARAMS["macd_fast"]
    slow   = INDICATOR_PARAMS["macd_slow"]
    signal = INDICATOR_PARAMS["macd_signal"]

    ema_fast = _ema(df["close"], fast)
    ema_slow = _ema(df["close"], slow)

    df["macd_line"]   = ema_fast - ema_slow
    df["macd_signal"] = _ema(df["macd_line"], signal)
    df["macd_hist"]   = df["macd_line"] - df["macd_signal"]
    return df


# ═══════════════════════════════════════════════════════════════
#  4. ADX  (Average Directional Index)
# ═══════════════════════════════════════════════════════════════

def add_adx(df: pd.DataFrame) -> pd.DataFrame:
    """
    ADX with +DI / -DI.
    Outputs: adx_{period}, plus_di, minus_di
    """
    period = INDICATOR_PARAMS["adx_period"]
    high   = df["high"]
    low    = df["low"]
    close  = df["close"]

    # ── True Range ──────────────────────────────────────────
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = _wilder_smooth(tr, period)

    # ── Directional Movement ────────────────────────────────
    up_move   = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=df.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=df.index,
    )

    plus_di  = 100.0 * _wilder_smooth(plus_dm,  period) / atr.replace(0, np.nan)
    minus_di = 100.0 * _wilder_smooth(minus_dm, period) / atr.replace(0, np.nan)

    di_sum  = plus_di + minus_di
    di_diff = (plus_di - minus_di).abs()
    dx      = 100.0 * di_diff / di_sum.replace(0, np.nan)

    df[f"adx_{period}"] = _wilder_smooth(dx, period)
    df["plus_di"]       = plus_di
    df["minus_di"]      = minus_di
    return df


# ═══════════════════════════════════════════════════════════════
#  5. MOVING AVERAGES
# ═══════════════════════════════════════════════════════════════

def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    EMA and SMA lines, plus price-to-MA distance (%).
    Outputs: ema_{p}, sma_{s}, sma_{l},
             close_vs_ema_{p}_pct, close_vs_sma_{l}_pct
    """
    ema_p = INDICATOR_PARAMS["ema_period"]
    sma_s = INDICATOR_PARAMS["sma_short"]
    sma_l = INDICATOR_PARAMS["sma_long"]

    df[f"ema_{ema_p}"] = _ema(df["close"], ema_p)
    df[f"sma_{sma_s}"] = _sma(df["close"], sma_s)
    df[f"sma_{sma_l}"] = _sma(df["close"], sma_l)

    # Distance from MA (positive = price above MA)
    df[f"close_vs_ema_{ema_p}_pct"] = (
        (df["close"] / df[f"ema_{ema_p}"] - 1.0) * 100.0
    )
    df[f"close_vs_sma_{sma_l}_pct"] = (
        (df["close"] / df[f"sma_{sma_l}"] - 1.0) * 100.0
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  6. ATR  (Average True Range)
# ═══════════════════════════════════════════════════════════════

def add_atr(df: pd.DataFrame) -> pd.DataFrame:
    """
    ATR in absolute and percentage-of-price terms.
    Percentage ATR makes cross-asset comparison possible.
    Outputs: atr_{period}, atr_{period}_pct
    """
    period     = INDICATOR_PARAMS["atr_period"]
    prev_close = df["close"].shift(1)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)

    df[f"atr_{period}"]     = _wilder_smooth(tr, period)
    df[f"atr_{period}_pct"] = df[f"atr_{period}"] / df["close"] * 100.0
    return df


# ═══════════════════════════════════════════════════════════════
#  7. REALIZED VOLATILITY
# ═══════════════════════════════════════════════════════════════

def add_realized_vol(df: pd.DataFrame) -> pd.DataFrame:
    """
    Annualised realized volatility from log returns.
    Also computes 5-day change to detect vol expansion / contraction.
    Outputs: realized_vol_{w}d, realized_vol_{w}d_chg5
    """
    window  = INDICATOR_PARAMS["realized_vol_window"]
    log_ret = np.log(df["close"] / df["close"].shift(1))

    col = f"realized_vol_{window}d"
    df[col]           = log_ret.rolling(window).std() * np.sqrt(252) * 100.0
    df[f"{col}_chg5"] = df[col].diff(5)
    return df


# ═══════════════════════════════════════════════════════════════
#  8. OBV  (On-Balance Volume)
# ═══════════════════════════════════════════════════════════════

def add_obv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cumulative OBV plus its 10-day slope.
    Rising OBV slope on an up-trend = accumulation confirmation.
    Outputs: obv, obv_slope_10d
    """
    if not INDICATOR_PARAMS.get("obv", True):
        return df

    sign = np.sign(df["close"].diff()).fillna(0.0)
    df["obv"] = (sign * df["volume"]).cumsum()
    df["obv_slope_10d"] = _rolling_slope(df["obv"], 10)
    return df


# ═══════════════════════════════════════════════════════════════
#  9. ACCUMULATION / DISTRIBUTION LINE
# ═══════════════════════════════════════════════════════════════

def add_ad_line(df: pd.DataFrame) -> pd.DataFrame:
    """
    A/D line using the Close Location Value (CLV) multiplier.
    CLV = [(close-low) - (high-close)] / (high-low)
    Outputs: ad_line, ad_line_slope_10d
    """
    if not INDICATOR_PARAMS.get("ad_line", True):
        return df

    hl_range = df["high"] - df["low"]
    clv = (
        (df["close"] - df["low"]) - (df["high"] - df["close"])
    ) / hl_range.replace(0.0, np.nan)
    clv = clv.fillna(0.0)

    df["ad_line"]            = (clv * df["volume"]).cumsum()
    df["ad_line_slope_10d"]  = _rolling_slope(df["ad_line"], 10)
    return df


# ═══════════════════════════════════════════════════════════════
# 10. VOLUME METRICS
# ═══════════════════════════════════════════════════════════════

def add_volume_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average volume, relative volume, dollar volume.
    Relative volume > 1.5 often indicates institutional activity.
    Outputs: volume_avg_{w}, relative_volume,
             dollar_volume, dollar_volume_avg_{w}
    """
    w = INDICATOR_PARAMS["volume_avg_window"]

    df[f"volume_avg_{w}"]        = _sma(df["volume"], w)
    df["relative_volume"]        = (
        df["volume"] / df[f"volume_avg_{w}"].replace(0.0, np.nan)
    )
    df["dollar_volume"]          = df["close"] * df["volume"]
    df[f"dollar_volume_avg_{w}"] = _sma(df["dollar_volume"], w)
    return df


# ═══════════════════════════════════════════════════════════════
# 11. AMIHUD ILLIQUIDITY
# ═══════════════════════════════════════════════════════════════

def add_amihud(df: pd.DataFrame) -> pd.DataFrame:
    """
    Amihud (2002) illiquidity ratio: |return| / dollar_volume.
    Higher = more illiquid.  Scaled by 1e6 for readability.
    Requires dollar_volume column (call add_volume_metrics first).
    Outputs: amihud_{w}d
    """
    w = INDICATOR_PARAMS["amihud_window"]

    if "dollar_volume" not in df.columns:
        df["dollar_volume"] = df["close"] * df["volume"]

    daily_illiq = (
        df["close"].pct_change(fill_method=None).abs()
        / df["dollar_volume"].replace(0.0, np.nan)
    )
    df[f"amihud_{w}d"] = daily_illiq.rolling(w).mean() * 1e6
    return df


# ═══════════════════════════════════════════════════════════════
# 12. VWAP DISTANCE  (daily-bar proxy)
# ═══════════════════════════════════════════════════════════════

def add_vwap_distance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling VWAP proxy for daily data.

    True VWAP needs intraday bars.  With daily data we approximate
    using a N-day volume-weighted average of the typical price
    (H+L+C)/3.  Distance > 0 means close is above VWAP — a sign
    of sustained buying pressure (accumulation).

    Outputs: vwap_{w}d, vwap_{w}d_dist_pct
    """
    w       = INDICATOR_PARAMS["volume_avg_window"]     # reuse 20D
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    tp_vol  = typical * df["volume"]

    rolling_tp_vol = tp_vol.rolling(w).sum()
    rolling_vol    = df["volume"].rolling(w).sum()

    vwap_col = f"vwap_{w}d"
    df[vwap_col]              = rolling_tp_vol / rolling_vol.replace(0.0, np.nan)
    df[f"{vwap_col}_dist_pct"] = (df["close"] / df[vwap_col] - 1.0) * 100.0
    return df


# ═══════════════════════════════════════════════════════════════
# MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute every technical indicator on an OHLCV DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: open, high, low, close, volume.
        Must be sorted by date ascending.

    Returns
    -------
    pd.DataFrame
        Copy of the input with ~30 indicator columns appended.
        Early rows will contain NaN where lookback is insufficient.
    """
    _validate_ohlcv(df)
    df = df.copy()

    # ── Order matters: volume_metrics before amihud ─────────
    df = add_returns(df)
    df = add_rsi(df)
    df = add_macd(df)
    df = add_adx(df)
    df = add_moving_averages(df)
    df = add_atr(df)
    df = add_realized_vol(df)
    df = add_obv(df)
    df = add_ad_line(df)
    df = add_volume_metrics(df)      # creates dollar_volume
    df = add_amihud(df)              # needs dollar_volume
    df = add_vwap_distance(df)

    return df