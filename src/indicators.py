# indicators.py
"""
Pure-function indicator library.
Every function takes Series/DataFrames in, returns Series/DataFrames out.
No side effects, no data fetching, no config imports at computation time
(parameters are passed as arguments with defaults from config).
"""

import numpy as np
import pandas as pd

from config import (
    ADX_PERIOD,
    AMIHUD_WINDOW,
    ATR_PERIOD,
    AVWAP_ANCHOR,
    EMA_PERIOD,
    MACD_FAST,
    MACD_SIGNAL,
    MACD_SLOW,
    OBV_SLOPE_WINDOW,
    REALIZED_VOL_WINDOW,
    RETURN_WINDOWS,
    RS_WINDOW,
    RSI_PERIOD,
    RVOL_WINDOW,
    SMA_PERIODS,
    ZSCORE_WINDOW,
)


# ════════════════════════════════════════════════════════════════════
#  RETURNS
# ════════════════════════════════════════════════════════════════════
def multi_period_returns(
    close: pd.Series, windows: list = RETURN_WINDOWS
) -> pd.DataFrame:
    """Percentage returns over multiple lookback windows."""
    out = pd.DataFrame(index=close.index)
    for w in windows:
        out[f"ret_{w}d"] = close.pct_change(w)
    return out


# ════════════════════════════════════════════════════════════════════
#  MOMENTUM
# ════════════════════════════════════════════════════════════════════
def rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period).mean()
    rs = avg_gain / (avg_loss + 1e-12)
    return pd.Series(100.0 - 100.0 / (1.0 + rs), index=close.index, name="rsi")


def macd(
    close: pd.Series,
    fast: int = MACD_FAST,
    slow: int = MACD_SLOW,
    signal: int = MACD_SIGNAL,
) -> pd.DataFrame:
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    line = ema_f - ema_s
    sig = line.ewm(span=signal, adjust=False).mean()
    hist = line - sig
    return pd.DataFrame(
        {"macd_line": line, "macd_signal": sig, "macd_hist": hist},
        index=close.index,
    )


def adx(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = ADX_PERIOD,
) -> pd.Series:
    """Average Directional Index."""
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, min_periods=period).mean()

    up = high.diff()
    down = -low.diff()
    plus_dm = pd.Series(
        np.where((up > down) & (up > 0), up, 0.0), index=high.index
    )
    minus_dm = pd.Series(
        np.where((down > up) & (down > 0), down, 0.0), index=high.index
    )

    smooth_plus = plus_dm.ewm(alpha=1.0 / period, min_periods=period).mean()
    smooth_minus = minus_dm.ewm(alpha=1.0 / period, min_periods=period).mean()

    plus_di = 100.0 * smooth_plus / (atr + 1e-12)
    minus_di = 100.0 * smooth_minus / (atr + 1e-12)

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-12)
    return dx.ewm(alpha=1.0 / period, min_periods=period).mean().rename("adx")


# ════════════════════════════════════════════════════════════════════
#  MOVING AVERAGES & DISTANCE
# ════════════════════════════════════════════════════════════════════
def moving_averages(
    close: pd.Series,
    ema_period: int = EMA_PERIOD,
    sma_periods: list = SMA_PERIODS,
) -> pd.DataFrame:
    out = pd.DataFrame(index=close.index)
    ema = close.ewm(span=ema_period, adjust=False).mean()
    out[f"ema_{ema_period}"] = ema
    out[f"dist_ema_{ema_period}"] = (close - ema) / (ema + 1e-12)
    for p in sma_periods:
        sma = close.rolling(p).mean()
        out[f"sma_{p}"] = sma
        out[f"dist_sma_{p}"] = (close - sma) / (sma + 1e-12)
    return out


# ════════════════════════════════════════════════════════════════════
#  VOLUME & ACCUMULATION
# ════════════════════════════════════════════════════════════════════
def relative_volume(volume: pd.Series, window: int = RVOL_WINDOW) -> pd.Series:
    avg = volume.rolling(window).mean()
    return (volume / (avg + 1e-12)).rename("rvol")


def on_balance_volume(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff())
    return (direction * volume).cumsum().rename("obv")


def accumulation_distribution(
    high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series
) -> pd.Series:
    mfm = ((close - low) - (high - close)) / (high - low + 1e-12)
    return (mfm * volume).cumsum().rename("ad_line")


def slope_proxy(series: pd.Series, window: int) -> pd.Series:
    """
    Simple slope proxy: net change over `window` periods,
    normalised by rolling mean absolute value so the magnitude
    is comparable across tickers.
    """
    delta = series.diff(window)
    norm = series.abs().rolling(window).mean() + 1e-12
    return (delta / norm).rename(f"{series.name}_slope")


# ════════════════════════════════════════════════════════════════════
#  VWAP (DAILY-DATA PROXY)
# ════════════════════════════════════════════════════════════════════
def anchored_vwap(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    anchor: int = AVWAP_ANCHOR,
) -> pd.Series:
    """Rolling anchored VWAP from daily typical-price * volume."""
    tp = (high + low + close) / 3.0
    cum_tpv = (tp * volume).rolling(anchor).sum()
    cum_v = volume.rolling(anchor).sum()
    return (cum_tpv / (cum_v + 1e-12)).rename("avwap")


# ════════════════════════════════════════════════════════════════════
#  VOLATILITY & LIQUIDITY
# ════════════════════════════════════════════════════════════════════
def average_true_range(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = ATR_PERIOD
) -> pd.DataFrame:
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr_val = tr.ewm(alpha=1.0 / period, min_periods=period).mean()
    atr_pct = atr_val / (close + 1e-12)
    return pd.DataFrame(
        {"atr": atr_val, "atr_pct": atr_pct}, index=close.index
    )


def realized_volatility(
    close: pd.Series, window: int = REALIZED_VOL_WINDOW
) -> pd.Series:
    log_ret = np.log(close / close.shift(1))
    return (log_ret.rolling(window).std() * np.sqrt(252)).rename("realized_vol")


def realized_vol_rank(close: pd.Series, short: int = 20, long: int = 252) -> pd.Series:
    """
    Percentile rank of current short-window realised vol
    within its own long-window range.  Cheap IV-rank proxy.
    """
    rv = realized_volatility(close, short)
    rv_min = rv.rolling(long, min_periods=60).min()
    rv_max = rv.rolling(long, min_periods=60).max()
    rank = (rv - rv_min) / (rv_max - rv_min + 1e-12)
    return rank.rename("rv_rank")


def amihud_illiquidity(
    close: pd.Series, volume: pd.Series, window: int = AMIHUD_WINDOW
) -> pd.Series:
    abs_ret = close.pct_change().abs()
    dollar_vol = close * volume
    daily = abs_ret / (dollar_vol + 1e-12)
    return daily.rolling(window).mean().rename("amihud")


def atr_slope(
    high: pd.Series, low: pd.Series, close: pd.Series, window: int = 5
) -> pd.Series:
    """Rate of change of ATR over `window` days."""
    atr_df = average_true_range(high, low, close)
    return atr_df["atr"].pct_change(window).rename("atr_slope")


# ════════════════════════════════════════════════════════════════════
#  FLOW PROXY (replaces expensive options/dark-pool feeds)
# ════════════════════════════════════════════════════════════════════
def smart_flow_proxy(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    rvol_window: int = RVOL_WINDOW,
) -> pd.Series:
    """
    Combines three cheap signals into one "smart flow" indicator:
      1. Relative volume > 1  (institutional size)
      2. Close in upper quartile of day's range  (directional conviction)
      3. Close above anchored VWAP  (net buyer dominance)
    Each flag is 0/1; the sum (0-3) is the proxy score.
    """
    rvol = relative_volume(volume, rvol_window)
    day_range = high - low + 1e-12
    close_position = (close - low) / day_range          # 0..1
    avwap = anchored_vwap(high, low, close, volume)

    flag_rvol   = (rvol > 1.0).astype(float)
    flag_close  = (close_position > 0.75).astype(float)
    flag_vwap   = (close > avwap).astype(float)

    return (flag_rvol + flag_close + flag_vwap).rename("flow_proxy")


# ════════════════════════════════════════════════════════════════════
#  RELATIVE STRENGTH
# ════════════════════════════════════════════════════════════════════
def relative_strength(
    ticker_close: pd.Series,
    benchmark_close: pd.Series,
    window: int = RS_WINDOW,
) -> pd.Series:
    """
    RS ratio = (1 + ticker_return) / (1 + benchmark_return) - 1
    over `window` trading days.  Positive = outperforming.
    """
    t_ret = ticker_close.pct_change(window)
    b_ret = benchmark_close.pct_change(window)
    return ((1 + t_ret) / (1 + b_ret + 1e-12) - 1).rename("rs")


# ════════════════════════════════════════════════════════════════════
#  NORMALISATION
# ════════════════════════════════════════════════════════════════════
def rolling_zscore(
    series: pd.Series, window: int = ZSCORE_WINDOW
) -> pd.Series:
    """Time-series z-score: how unusual is today's value vs own history."""
    mu = series.rolling(window, min_periods=20).mean()
    sigma = series.rolling(window, min_periods=20).std()
    return ((series - mu) / (sigma + 1e-12)).rename(f"{series.name}_tz")


# ════════════════════════════════════════════════════════════════════
#  MASTER FUNCTION: compute everything for one ticker
# ════════════════════════════════════════════════════════════════════
def compute_all(
    ohlcv: pd.DataFrame,
    benchmark_closes: dict,
) -> pd.DataFrame:
    """
    Given a single ticker's OHLCV DataFrame and a dict of
    {benchmark_name: close Series}, return a DataFrame with
    every indicator column on a shared date index.
    """
    o = ohlcv["Open"]
    h = ohlcv["High"]
    l = ohlcv["Low"]
    c = ohlcv["Close"]
    v = ohlcv["Volume"]

    out = pd.DataFrame(index=ohlcv.index)
    out["close"] = c
    out["volume"] = v

    # ── Returns ─────────────────────────────────────────────────────
    rets = multi_period_returns(c)
    out = out.join(rets)

    # ── Momentum ────────────────────────────────────────────────────
    out["rsi"] = rsi(c)
    out = out.join(macd(c))
    out["adx"] = adx(h, l, c)

    # ── Moving averages ─────────────────────────────────────────────
    out = out.join(moving_averages(c))

    # ── Volume & accumulation ───────────────────────────────────────
    out["rvol"] = relative_volume(v)
    obv_s = on_balance_volume(c, v)
    ad_s  = accumulation_distribution(h, l, c, v)
    out["obv"] = obv_s
    out["ad_line"] = ad_s
    out["obv_slope"] = slope_proxy(obv_s, OBV_SLOPE_WINDOW)
    out["ad_slope"]  = slope_proxy(ad_s, OBV_SLOPE_WINDOW)

    # ── VWAP ────────────────────────────────────────────────────────
    avwap = anchored_vwap(h, l, c, v)
    out["avwap"] = avwap
    out["dist_avwap"] = (c - avwap) / (avwap + 1e-12)

    # ── Volatility & liquidity ──────────────────────────────────────
    atr_df = average_true_range(h, l, c)
    out["atr"]          = atr_df["atr"]
    out["atr_pct"]      = atr_df["atr_pct"]
    out["atr_slope"]    = atr_slope(h, l, c)
    out["realized_vol"] = realized_volatility(c)
    out["rv_rank"]      = realized_vol_rank(c)
    out["amihud"]       = amihud_illiquidity(c, v)

    # ── Flow proxy ──────────────────────────────────────────────────
    out["flow_proxy"] = smart_flow_proxy(h, l, c, v)

    # ── Relative strength vs benchmarks ─────────────────────────────
    for bm_name, bm_close in benchmark_closes.items():
        # Align benchmark to ticker's index
        aligned = bm_close.reindex(out.index).ffill()
        out[f"rs_{bm_name}"] = relative_strength(c, aligned)

    return out