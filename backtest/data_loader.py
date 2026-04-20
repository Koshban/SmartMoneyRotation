"""
backtest/data_loader.py
-----------------------
Download, cache, and serve 20 years of OHLCV data for backtesting.

Primary source is yfinance (``period="max"``).  Data is cached as a
single parquet file at ``data/backtest/backtest_universe.parquet`` so
subsequent runs load in < 2 seconds.

The default backtest universe is a subset of the full CASH universe
consisting of tickers with 15–25 years of history.  The user can
override with any ticker list.

Integration
-----------
Returns data in the same ``{ticker: DataFrame}`` format that
``src/db/loader.py`` produces, so the pipeline accepts it seamlessly
via ``Orchestrator.load_data(preloaded=data)``.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import DATA_DIR, BENCHMARK_TICKER

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  PATHS
# ═══════════════════════════════════════════════════════════════

BACKTEST_DIR = DATA_DIR / "backtest"
CACHE_PATH = BACKTEST_DIR / "backtest_universe.parquet"

# ═══════════════════════════════════════════════════════════════
#  DEFAULT BACKTEST UNIVERSE
#  Tickers with 15–25 years of Yahoo Finance history.
#  Intentionally smaller than the full CASH universe so that
#  20-year backtests are meaningful (no survivorship bias from
#  tickers that didn't exist yet).
# ═══════════════════════════════════════════════════════════════

BACKTEST_CORE_UNIVERSE = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "MDY",
    # Sectors
    "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLY", "XLP", "XLU", "XLB",
    # International
    "EFA", "EEM", "EWJ", "EWZ",
    # Fixed income
    "TLT", "IEF", "SHY", "LQD", "HYG", "AGG", "TIP",
    # Commodities / alternatives
    "GLD", "SLV", "USO", "DBC", "VNQ",
    # Thematic (10+ years)
    "SOXX", "XBI", "IBB", "IGV",
    "HACK", "TAN", "ICLN", "URA",
    "IBIT",
    # Communication (newer but important)
    "XLC", "XLRE",
]

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def ensure_history(
    tickers: list[str] | None = None,
    force_refresh: bool = False,
    max_age_days: int = 7,
) -> dict[str, pd.DataFrame]:
    """
    Ensure 20-year OHLCV data is available.  Downloads from
    yfinance if the cache is missing or stale.

    Parameters
    ----------
    tickers : list[str] or None
        Symbols to download.  Defaults to ``BACKTEST_CORE_UNIVERSE``.
    force_refresh : bool
        If True, re-download even if cache exists.
    max_age_days : int
        Re-download if the cache is older than this many days.

    Returns
    -------
    dict[str, pd.DataFrame]
        ``{ticker: OHLCV DataFrame}`` with DatetimeIndex, lowercase
        columns, sorted ascending.  Ready to pass to
        ``Orchestrator.load_data(preloaded=...)``.
    """
    tickers = tickers or list(BACKTEST_CORE_UNIVERSE)

    # Ensure benchmark is included
    if BENCHMARK_TICKER not in tickers:
        tickers = [BENCHMARK_TICKER] + tickers

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)

    needs_download = (
        force_refresh
        or not CACHE_PATH.exists()
        or _cache_age_days() > max_age_days
    )

    if needs_download:
        logger.info(
            f"Downloading {len(tickers)} tickers from yfinance "
            f"(period=max) ..."
        )
        _download_and_cache(tickers)
    else:
        logger.info(
            f"Using cached data: {CACHE_PATH.name} "
            f"(age: {_cache_age_days():.0f} days)"
        )

    return load_cached_history(tickers)


def load_cached_history(
    tickers: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load previously cached backtest data from parquet.

    Parameters
    ----------
    tickers : list[str] or None
        Filter to these tickers.  None = return all cached.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    if not CACHE_PATH.exists():
        logger.warning(
            f"Cache not found: {CACHE_PATH}.  "
            f"Call ensure_history() first."
        )
        return {}

    raw = pd.read_parquet(CACHE_PATH)

    # Find symbol column
    sym_col = _find_symbol_col(raw)
    if sym_col is None:
        logger.error(
            f"No symbol column found in {CACHE_PATH}.  "
            f"Columns: {list(raw.columns)}"
        )
        return {}

    # Filter tickers
    if tickers is not None:
        upper = {t.upper() for t in tickers}
        raw = raw[raw[sym_col].str.upper().isin(upper)]

    # Split into per-ticker DataFrames
    result: dict[str, pd.DataFrame] = {}
    for ticker, group in raw.groupby(sym_col):
        df = _normalise(group.drop(columns=[sym_col]))
        if not df.empty and len(df) >= 60:
            result[str(ticker)] = df

    logger.info(
        f"Loaded {len(result)} tickers from cache "
        f"({sum(len(d) for d in result.values()):,} total bars)"
    )
    return result


def slice_period(
    data: dict[str, pd.DataFrame],
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Slice every DataFrame in the universe to a date range.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        ``{ticker: OHLCV}`` from ``ensure_history()`` or
        ``load_cached_history()``.
    start : str or Timestamp or None
        Inclusive start date.  None = earliest available.
    end : str or Timestamp or None
        Inclusive end date.  None = latest available.

    Returns
    -------
    dict[str, pd.DataFrame]
        Sliced data.  Tickers with < 60 bars after slicing
        are dropped.
    """
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
    """Quick summary of loaded backtest data."""
    if not data:
        return {"n_tickers": 0}

    all_starts = []
    all_ends = []
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

def _download_and_cache(tickers: list[str]) -> None:
    """Download max-period data from yfinance and save as parquet."""
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError(
            "yfinance is required for backtest data download.  "
            "pip install yfinance"
        )

    t0 = time.time()

    # Batch download — yfinance handles multi-ticker efficiently
    logger.info(f"yfinance batch download: {len(tickers)} tickers")

    raw = yf.download(
        tickers=tickers,
        period="max",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=True,
    )

    if raw.empty:
        logger.error("yfinance returned empty DataFrame")
        return

    # Reshape from MultiIndex columns to long format with symbol column
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
                    logger.warning(f"  {sym}: no data")
                    continue
                tmp = tmp.reset_index()
                tmp["symbol"] = sym
                records.append(tmp)
            except KeyError:
                logger.warning(f"  {sym}: not in download result")
                continue

    if not records:
        logger.error("No data collected from yfinance")
        return

    combined = pd.concat(records, ignore_index=True)

    # Normalise column names to lowercase
    combined.columns = [str(c).lower().strip() for c in combined.columns]
    rename_map = {"adj close": "adj_close"}
    combined.rename(
        columns={k: v for k, v in rename_map.items() if k in combined.columns},
        inplace=True,
    )

    # Save
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(CACHE_PATH, index=False)

    elapsed = time.time() - t0
    size_mb = CACHE_PATH.stat().st_size / (1024 * 1024)
    n_syms = combined["symbol"].nunique()

    logger.info(
        f"Saved → {CACHE_PATH} "
        f"({size_mb:.1f} MB, {len(combined):,} rows, "
        f"{n_syms} symbols, {elapsed:.0f}s)"
    )


# ═══════════════════════════════════════════════════════════════
#  NORMALISATION
# ═══════════════════════════════════════════════════════════════

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise to the standard format expected by compute/:
    lowercase columns, DatetimeIndex, no NaN closes.
    """
    df = df.copy()

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    df.columns = [str(c).lower().strip() for c in df.columns]
    df.rename(columns={"adj close": "adj_close"}, inplace=True)

    # Set date index
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            return pd.DataFrame()

    df.index.name = "date"

    # Keep only OHLCV
    keep = [c for c in _REQUIRED_COLS if c in df.columns]
    if len(keep) < 5:
        return pd.DataFrame()
    df = df[keep]

    # Clean
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

def _cache_age_days() -> float:
    """How old is the cache file in days."""
    if not CACHE_PATH.exists():
        return float("inf")
    mtime = CACHE_PATH.stat().st_mtime
    age = time.time() - mtime
    return age / 86400.0


def _find_symbol_col(df: pd.DataFrame) -> str | None:
    """Find the symbol/ticker column."""
    for candidate in ["symbol", "Symbol", "ticker", "Ticker", "SYMBOL"]:
        if candidate in df.columns:
            return candidate
    return None