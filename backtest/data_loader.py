"""
backtest/data_loader.py
-----------------------
Download, cache, and serve historical OHLCV data for backtesting.

Enhanced with multi-market support (US, HK, India).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import DATA_DIR, BENCHMARK_TICKER, MARKET_CONFIG

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  PATHS
# ═══════════════════════════════════════════════════════════════

BACKTEST_DIR = DATA_DIR / "backtest"

def _cache_path(market: str = "US") -> Path:
    return BACKTEST_DIR / f"backtest_{market.lower()}_universe.parquet"


# ═══════════════════════════════════════════════════════════════
#  DEFAULT BACKTEST UNIVERSES
# ═══════════════════════════════════════════════════════════════

BACKTEST_CORE_UNIVERSE = [
    "SPY", "QQQ", "IWM", "DIA", "MDY",
    "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLY", "XLP", "XLU", "XLB",
    "EFA", "EEM", "EWJ", "EWZ",
    "TLT", "IEF", "SHY", "LQD", "HYG", "AGG", "TIP",
    "GLD", "SLV", "USO", "DBC", "VNQ",
    "SOXX", "XBI", "IBB", "IGV",
    "HACK", "TAN", "ICLN", "URA",
    "IBIT",
    "XLC", "XLRE",
]

BACKTEST_HK_UNIVERSE = [
    "2800.HK", "0700.HK", "9988.HK", "3690.HK", "9618.HK",
    "1810.HK", "1299.HK", "0005.HK", "0388.HK", "2318.HK",
    "0883.HK", "1211.HK", "0941.HK", "9888.HK", "0939.HK",
    "1398.HK", "9999.HK", "0001.HK", "0016.HK", "0823.HK",
    "0857.HK", "0002.HK", "0003.HK", "9633.HK", "2020.HK",
    "3033.HK", "3067.HK", "2828.HK",
]

BACKTEST_IN_UNIVERSE = [
    "NIFTYBEES.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS",
    "ICICIBANK.NS", "RELIANCE.NS", "SBIN.NS", "KOTAKBANK.NS",
    "AXISBANK.NS", "LT.NS", "BHARTIARTL.NS", "HINDUNILVR.NS",
    "ITC.NS", "SUNPHARMA.NS", "TATAMOTORS.NS", "MARUTI.NS",
    "NTPC.NS", "TITAN.NS", "BAJFINANCE.NS", "WIPRO.NS",
]

_MARKET_UNIVERSES = {
    "US": BACKTEST_CORE_UNIVERSE,
    "HK": BACKTEST_HK_UNIVERSE,
    "IN": BACKTEST_IN_UNIVERSE,
}

_MARKET_BENCHMARKS = {
    "US": "SPY",
    "HK": "2800.HK",
    "IN": "NIFTYBEES.NS",
}

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def ensure_history(
    tickers: list[str] | None = None,
    market: str = "US",
    force_refresh: bool = False,
    max_age_days: int = 7,
) -> dict[str, pd.DataFrame]:
    """
    Ensure historical OHLCV data is available for a market.

    Downloads from yfinance if the cache is missing or stale.
    """
    default_tickers = _MARKET_UNIVERSES.get(market, BACKTEST_CORE_UNIVERSE)
    tickers = tickers or list(default_tickers)

    benchmark = _MARKET_BENCHMARKS.get(market, BENCHMARK_TICKER)
    if benchmark not in tickers:
        tickers = [benchmark] + tickers

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    cache = _cache_path(market)

    needs_download = (
        force_refresh
        or not cache.exists()
        or _cache_age_days(cache) > max_age_days
    )

    if needs_download:
        logger.info(
            f"[{market}] Downloading {len(tickers)} tickers "
            f"from yfinance (period=max)..."
        )
        _download_and_cache(tickers, cache)
    else:
        logger.info(
            f"[{market}] Using cached data: {cache.name} "
            f"(age: {_cache_age_days(cache):.0f} days)"
        )

    return load_cached_history(tickers, market=market)


def load_cached_history(
    tickers: list[str] | None = None,
    market: str = "US",
) -> dict[str, pd.DataFrame]:
    cache = _cache_path(market)
    if not cache.exists():
        logger.warning(f"Cache not found: {cache}. Call ensure_history() first.")
        return {}

    raw = pd.read_parquet(cache)
    sym_col = _find_symbol_col(raw)
    if sym_col is None:
        logger.error(f"No symbol column found in {cache}")
        return {}

    if tickers is not None:
        upper = {t.upper() for t in tickers}
        raw = raw[raw[sym_col].str.upper().isin(upper)]

    result: dict[str, pd.DataFrame] = {}
    for ticker, group in raw.groupby(sym_col):
        df = _normalise(group.drop(columns=[sym_col]))
        if not df.empty and len(df) >= 60:
            result[str(ticker)] = df

    logger.info(
        f"[{market}] Loaded {len(result)} tickers from cache "
        f"({sum(len(d) for d in result.values()):,} total bars)"
    )
    return result


def slice_period(
    data: dict[str, pd.DataFrame],
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for ticker, df in data.items():
        sliced = df.loc[start:end] if start or end else df.copy()
        if len(sliced) >= 60:
            result[ticker] = sliced

    n_dropped = len(data) - len(result)
    if n_dropped > 0:
        logger.info(
            f"Period slice: {len(result)} tickers retained, "
            f"{n_dropped} dropped (< 60 bars)"
        )
    return result


def data_summary(data: dict[str, pd.DataFrame]) -> dict:
    if not data:
        return {"n_tickers": 0}
    all_starts, all_ends = [], []
    total_bars = 0
    for ticker, df in data.items():
        all_starts.append(df.index[0])
        all_ends.append(df.index[-1])
        total_bars += len(df)
    return {
        "n_tickers": len(data),
        "total_bars": total_bars,
        "earliest_start": min(all_starts),
        "latest_end": max(all_ends),
        "median_bars": int(np.median([len(d) for d in data.values()])),
        "tickers": sorted(data.keys()),
    }


# ═══════════════════════════════════════════════════════════════
#  DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def _download_and_cache(tickers: list[str], cache_path: Path) -> None:
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance is required. pip install yfinance")

    t0 = time.time()
    logger.info(f"yfinance batch download: {len(tickers)} tickers")

    raw = yf.download(
        tickers=tickers, period="max", interval="1d",
        group_by="ticker", auto_adjust=False,
        threads=True, progress=True,
    )

    if raw.empty:
        logger.error("yfinance returned empty DataFrame")
        return

    records: list[pd.DataFrame] = []

    if len(tickers) == 1:
        sym = tickers[0]
        tmp = raw.copy().reset_index()
        tmp["symbol"] = sym
        records.append(tmp)
    else:
        for sym in tickers:
            try:
                tmp = raw[sym].copy()
                tmp = tmp.dropna(how="all")
                if tmp.empty:
                    continue
                tmp = tmp.reset_index()
                tmp["symbol"] = sym
                records.append(tmp)
            except KeyError:
                logger.warning(f"  {sym}: not in download result")

    if not records:
        logger.error("No data collected from yfinance")
        return

    combined = pd.concat(records, ignore_index=True)
    combined.columns = [str(c).lower().strip() for c in combined.columns]
    combined.rename(columns={"adj close": "adj_close"}, inplace=True)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(cache_path, index=False)

    elapsed = time.time() - t0
    size_mb = cache_path.stat().st_size / (1024 * 1024)
    n_syms = combined["symbol"].nunique()
    logger.info(
        f"Saved → {cache_path} "
        f"({size_mb:.1f} MB, {len(combined):,} rows, "
        f"{n_syms} symbols, {elapsed:.0f}s)"
    )


# ═══════════════════════════════════════════════════════════════
#  NORMALISATION
# ═══════════════════════════════════════════════════════════════

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c).lower().strip() for c in df.columns]
    df.rename(columns={"adj close": "adj_close"}, inplace=True)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            return pd.DataFrame()

    df.index.name = "date"
    keep = [c for c in _REQUIRED_COLS if c in df.columns]
    if len(keep) < 5:
        return pd.DataFrame()
    df = df[keep]
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df["close"].notna() & (df["close"] > 0)]
    for col in _REQUIRED_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype(np.int64)
    return df


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _cache_age_days(cache_path: Path) -> float:
    if not cache_path.exists():
        return float("inf")
    return (time.time() - cache_path.stat().st_mtime) / 86400.0


def _find_symbol_col(df: pd.DataFrame) -> str | None:
    for candidate in ["symbol", "Symbol", "ticker", "Ticker", "SYMBOL"]:
        if candidate in df.columns:
            return candidate
    return None