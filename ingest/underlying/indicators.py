from __future__ import annotations
""" ingest/underlying/indicators.py"""
"""Technical indicators matching {region}_underlying_daily schema.
Pure functions; input DataFrame indexed by date with OHLCV columns."""


import numpy as np
import pandas as pd

TRADING_DAYS = 252


def _wilder_ema(s: pd.Series, period: int) -> pd.Series:
    """Wilder smoothing — alpha = 1/period. Used by RSI, ATR, ADX."""
    return s.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def add_smas(df: pd.DataFrame) -> pd.DataFrame:
    for n in (20, 50, 100, 200):
        df[f"sma_{n}"] = df["close"].rolling(n, min_periods=n).mean()
    return df


def add_ema(df: pd.DataFrame) -> pd.DataFrame:
    df["ema_20"] = df["close"].ewm(span=20, adjust=False, min_periods=20).mean()
    return df


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = _wilder_ema(gain, period)
    avg_loss = _wilder_ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df[f"rsi_{period}"] = 100 - (100 / (1 + rs))
    return df


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    df["macd"] = ema_fast - ema_slow
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    return df


def _true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    return pd.concat(
        [
            (df["high"] - df["low"]),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def add_atr(df: pd.DataFrame) -> pd.DataFrame:
    tr = _true_range(df)
    df["atr_14"] = _wilder_ema(tr, 14)
    df["atr_20"] = _wilder_ema(tr, 20)
    return df


def add_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    high, low = df["high"], df["low"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=df.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=df.index,
    )
    atr = _wilder_ema(_true_range(df), period)
    plus_di = 100 * _wilder_ema(plus_dm, period) / atr
    minus_di = 100 * _wilder_ema(minus_dm, period) / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df[f"adx_{period}"] = _wilder_ema(dx, period)
    return df


def add_realized_vol(df: pd.DataFrame) -> pd.DataFrame:
    log_ret = np.log(df["close"] / df["close"].shift(1))
    for n in (20, 60, 252):
        df[f"rv_{n}"] = log_ret.rolling(n, min_periods=n).std() * np.sqrt(TRADING_DAYS)
    return df


def add_levels(df: pd.DataFrame) -> pd.DataFrame:
    """Rolling extreme-based S/R. Replace later with pivot-based if needed."""
    df["support_20"] = df["low"].rolling(20, min_periods=5).min()
    df["resistance_20"] = df["high"].rolling(20, min_periods=5).max()
    df["support_60"] = df["low"].rolling(60, min_periods=10).min()
    df["resistance_60"] = df["high"].rolling(60, min_periods=10).max()
    return df


def compute_all(df: pd.DataFrame) -> pd.DataFrame:
    """Full pipeline. Input: OHLCV indexed by date, ascending."""
    df = df.copy()
    df = add_smas(df)
    df = add_ema(df)
    df = add_rsi(df)
    df = add_macd(df)
    df = add_atr(df)
    df = add_adx(df)
    df = add_realized_vol(df)
    df = add_levels(df)
    return df