"""
backtest/data_loader.py
-----------------------
Download, cache, and serve historical OHLCV data for backtesting.

Enhanced:
  - Region-aware universe building from common.universe
  - --universe us|hk|india derives market, tickers, benchmark
  - Suffix-based ticker routing (.HK → Hong Kong, .NS/.BO → India)
  - Batched downloads for large universes (100+ tickers)
  - Separate cache files per (market, scope) pair
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import DATA_DIR, BENCHMARK_TICKER, MARKET_CONFIG

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  PATHS
# ═══════════════════════════════════════════════════════════════

BACKTEST_DIR = DATA_DIR / "backtest"


def _cache_path(market: str = "US", scope: str = "core") -> Path:
    if scope == "core":
        return BACKTEST_DIR / f"backtest_{market.lower()}_universe.parquet"
    return BACKTEST_DIR / f"backtest_{market.lower()}_{scope}_universe.parquet"


# ═══════════════════════════════════════════════════════════════
#  UNIVERSE ↔ MARKET MAPPING
# ═══════════════════════════════════════════════════════════════

UNIVERSE_MAP: dict[str, tuple[str, str]] = {
    # Full universes (from common.universe)
    "us":           ("US", "full"),
    "hk":           ("HK", "full"),
    "india":        ("IN", "full"),
    "in":           ("IN", "full"),
    # Core universes (hardcoded sets — backward compatible)
    "us_core":      ("US", "core"),
    "hk_core":      ("HK", "core"),
    "india_core":   ("IN", "core"),
    "in_core":      ("IN", "core"),
    # Legacy aliases
    "core":         ("US", "core"),
    "full":         ("US", "full"),
}

VALID_UNIVERSES = sorted(UNIVERSE_MAP.keys())

MARKET_BENCHMARKS: dict[str, str] = {
    "US": "SPY",
    "HK": "2800.HK",
    "IN": "NIFTYBEES.NS",
}


def resolve_universe(universe: str) -> tuple[str, str]:
    """
    Resolve a universe name to (market, scope).

    >>> resolve_universe("hk")
    ('HK', 'full')
    >>> resolve_universe("india_core")
    ('IN', 'core')
    """
    key = universe.lower().strip()
    if key not in UNIVERSE_MAP:
        raise ValueError(
            f"Unknown universe '{universe}'. "
            f"Valid choices: {', '.join(VALID_UNIVERSES)}"
        )
    return UNIVERSE_MAP[key]


def get_benchmark(market: str) -> str:
    return MARKET_BENCHMARKS.get(market, BENCHMARK_TICKER)


# ═══════════════════════════════════════════════════════════════
#  CORE UNIVERSES — hardcoded backward-compatible sets
# ═══════════════════════════════════════════════════════════════

BACKTEST_CORE_US = [
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

BACKTEST_CORE_HK = [
    "2800.HK", "0700.HK", "9988.HK", "3690.HK", "9618.HK",
    "1810.HK", "1299.HK", "0005.HK", "0388.HK", "2318.HK",
    "0883.HK", "1211.HK", "0941.HK", "9888.HK", "0939.HK",
    "1398.HK", "9999.HK", "0001.HK", "0016.HK", "0823.HK",
    "0857.HK", "0002.HK", "0003.HK", "9633.HK", "2020.HK",
    "3033.HK", "3067.HK", "2828.HK",
]

BACKTEST_CORE_IN = [
    "NIFTYBEES.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS",
    "ICICIBANK.NS", "RELIANCE.NS", "SBIN.NS", "KOTAKBANK.NS",
    "AXISBANK.NS", "LT.NS", "BHARTIARTL.NS", "HINDUNILVR.NS",
    "ITC.NS", "SUNPHARMA.NS", "TATAMOTORS.NS", "MARUTI.NS",
    "NTPC.NS", "TITAN.NS", "BAJFINANCE.NS", "WIPRO.NS",
]

# Backward compat aliases
BACKTEST_CORE_UNIVERSE = BACKTEST_CORE_US

_MARKET_CORE = {
    "US": BACKTEST_CORE_US,
    "HK": BACKTEST_CORE_HK,
    "IN": BACKTEST_CORE_IN,
}

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]


# ═══════════════════════════════════════════════════════════════
#  TICKER MARKET DETECTION (suffix-based)
# ═══════════════════════════════════════════════════════════════

def _ticker_market(ticker: str) -> str:
    """
    Detect market from ticker suffix.

    .HK           → HK
    .NS / .BO     → IN
    everything else → US
    """
    t = ticker.upper().strip()
    if t.endswith(".HK"):
        return "HK"
    if t.endswith(".NS") or t.endswith(".BO"):
        return "IN"
    return "US"


def _filter_tickers_for_market(
    tickers: set[str], market: str,
) -> set[str]:
    """Keep only tickers that belong to the given market."""
    return {t for t in tickers if _ticker_market(t) == market}


# ═══════════════════════════════════════════════════════════════
#  FULL UNIVERSE — dynamically built from common.universe
# ═══════════════════════════════════════════════════════════════

def _extract_tickers_from_value(val) -> set[str]:
    """
    Recursively extract ticker strings from any data structure.

    Handles: str, list, tuple, set, frozenset, dict (both
    {ticker: meta} and {group: [tickers]}), and nested combos.
    """
    tickers: set[str] = set()

    if isinstance(val, str):
        cleaned = val.strip().upper()
        if cleaned and 1 <= len(cleaned) <= 15:
            tickers.add(cleaned)

    elif isinstance(val, (list, tuple, set, frozenset)):
        for item in val:
            tickers.update(_extract_tickers_from_value(item))

    elif isinstance(val, dict):
        for k, v in val.items():
            if isinstance(k, str):
                cleaned = k.strip().upper()
                if 1 <= len(cleaned) <= 15:
                    tickers.add(cleaned)
            tickers.update(_extract_tickers_from_value(v))

    return tickers


# Known attribute patterns per market
_MARKET_KNOWN_ATTRS: dict[str, list[str]] = {
    "US": [
        "SECTORS", "BROAD_MARKET", "FIXED_INCOME", "COMMODITIES",
        "THEMATIC", "ALTERNATIVES", "INTERNATIONAL",
        "GROWTH", "VALUE", "DIVIDEND", "DEFENSIVE",
        "SECTOR_TICKERS", "SECTOR_TO_TICKERS", "SECTOR_STOCKS",
        "SECTOR_MEMBERS", "TICKER_SECTOR", "TICKER_TO_SECTOR",
        "US_UNIVERSE", "US_TICKERS", "US_STOCKS",
        "ALL_TICKERS", "UNIVERSE", "FULL_UNIVERSE",
        "BACKTEST_UNIVERSE", "WATCHLIST", "US_WATCHLIST",
        "ETF_UNIVERSE", "EQUITY_UNIVERSE",
    ],
    "HK": [
        "HK_UNIVERSE", "HK_TICKERS", "HK_STOCKS", "HONG_KONG",
        "HSI_COMPONENTS", "HKEX", "HK_SECTORS", "HK_WATCHLIST",
        "HK_ETF", "HK_BROAD", "HK_ALL",
        "HANG_SENG", "HSI", "HSCEI",
    ],
    "IN": [
        "IN_UNIVERSE", "INDIA_UNIVERSE", "INDIA_TICKERS",
        "IN_TICKERS", "IN_STOCKS", "INDIA_STOCKS",
        "NSE_TICKERS", "BSE_TICKERS",
        "NIFTY", "NIFTY_50", "NIFTY50", "NIFTY_NEXT50",
        "INDIA_SECTORS", "IN_SECTORS",
        "INDIA_WATCHLIST", "IN_WATCHLIST", "IN_ALL",
    ],
}

# Substrings that suggest an attribute belongs to a specific market
_MARKET_ATTR_HINTS: dict[str, list[str]] = {
    "HK": ["HK", "HONG_KONG", "HANG_SENG", "HSI", "HSCEI", "HKEX"],
    "IN": ["INDIA", "IN_", "NSE", "BSE", "NIFTY", "_NS", "_BO"],
}


def build_full_universe(market: str = "US") -> list[str]:
    """
    Build the complete backtest universe for a market from common.universe.

    Strategy:
      1. Check known attribute names for the target market
      2. Discover unknown attributes by name hints (e.g. 'HK_' prefix)
      3. For US: also harvest general attributes (SECTORS, etc.)
      4. From every discovered collection, extract ticker strings
      5. Filter by market suffix (.HK for HK, .NS/.BO for IN)
      6. Always include the core hardcoded set as a floor
      7. Deduplicate and sort

    Returns
    -------
    list[str]
        Sorted, deduplicated ticker list for the market.
    """
    tickers: set[str] = set()
    core = _MARKET_CORE.get(market, [])

    # ── 1. Import common.universe ─────────────────────────────
    try:
        import common.universe as uni
    except ImportError:
        logger.warning(
            "Cannot import common.universe — "
            "falling back to core universe"
        )
        return sorted(set(core))

    # ── 2. Check known attribute names for this market ────────
    known = _MARKET_KNOWN_ATTRS.get(market, [])
    for attr_name in known:
        val = getattr(uni, attr_name, None)
        if val is not None:
            extracted = _extract_tickers_from_value(val)
            if extracted:
                logger.debug(
                    f"  common.universe.{attr_name}: "
                    f"{len(extracted)} raw tickers"
                )
                tickers.update(extracted)

    # ── 3. Discover attributes by name hints ──────────────────
    hints = _MARKET_ATTR_HINTS.get(market, [])
    checked = set(known)

    for attr_name in dir(uni):
        if attr_name.startswith("_") or attr_name in checked:
            continue

        # Check if attribute name matches market hints
        attr_upper = attr_name.upper()
        is_hint_match = any(h in attr_upper for h in hints)

        # For US, also check general (non-market-specific) attributes
        is_general = (
            market == "US"
            and not any(
                h in attr_upper
                for hints_list in _MARKET_ATTR_HINTS.values()
                for h in hints_list
            )
        )

        if not (is_hint_match or is_general):
            continue

        # Try callable (e.g. get_all_tickers())
        obj = getattr(uni, attr_name, None)
        if obj is None:
            continue

        if callable(obj) and attr_name.lower() in (
            "get_all_tickers", "all_tickers", "get_universe",
            "get_full_universe", "get_us_tickers",
            "get_hk_tickers", "get_india_tickers",
            "get_in_tickers",
        ):
            try:
                result = obj()
                extracted = _extract_tickers_from_value(result)
                if len(extracted) >= 2:
                    logger.debug(
                        f"  common.universe.{attr_name}(): "
                        f"{len(extracted)} raw tickers"
                    )
                    tickers.update(extracted)
            except Exception:
                pass
            continue

        if callable(obj):
            continue

        if isinstance(obj, (list, tuple, set, frozenset, dict)):
            extracted = _extract_tickers_from_value(obj)
            if len(extracted) >= 2:
                logger.debug(
                    f"  common.universe.{attr_name}: "
                    f"{len(extracted)} raw tickers (discovered)"
                )
                tickers.update(extracted)

    # ── 4. Filter by market suffix ────────────────────────────
    #    For US, keep tickers with no region suffix.
    #    For HK/IN, keep tickers with the right suffix.
    #    This is critical when common.universe has a mixed ALL_TICKERS.
    market_tickers = _filter_tickers_for_market(tickers, market)

    # ── 5. Always include core ────────────────────────────────
    market_tickers.update(core)

    # ── 6. Clean ──────────────────────────────────────────────
    cleaned = _clean_ticker_set(market_tickers)

    result = sorted(cleaned)
    n_additional = len(result) - len(set(core))

    logger.info(
        f"[{market}] Built full universe: {len(result)} tickers "
        f"(core={len(core)}, additional={max(n_additional, 0)} "
        f"from common.universe)"
    )

    return result


def _clean_ticker_set(tickers: set[str]) -> set[str]:
    """Remove false positives from extracted tickers."""
    _FALSE_POSITIVES = frozenset({
        "TRUE", "FALSE", "NONE", "NAN", "NULL",
        "BUY", "SELL", "HOLD", "STRONG", "WEAK",
        "ENTRY", "EXIT", "LONG", "SHORT",
        "LEADING", "LAGGING", "IMPROVING", "WEAKENING",
        "NEUTRAL", "CRITICAL", "STRONG_BUY", "STRONG_SELL",
        "TECHNOLOGY", "ENERGY", "FINANCIALS", "HEALTHCARE",
        "MATERIALS", "INDUSTRIALS", "UTILITIES",
        "CONSUMER", "COMMUNICATION", "REAL",
    })

    cleaned: set[str] = set()
    for t in tickers:
        t = t.strip().upper()
        if not t or len(t) > 15:
            continue
        if t in _FALSE_POSITIVES:
            continue
        # Must start with a letter or digit
        if not (t[0].isalpha() or t[0].isdigit()):
            continue
        cleaned.add(t)
    return cleaned


# ═══════════════════════════════════════════════════════════════
#  TICKER LIST RESOLUTION
# ═══════════════════════════════════════════════════════════════

def get_universe_tickers(
    market: str = "US",
    scope: str = "core",
    custom_tickers: list[str] | None = None,
) -> list[str]:
    """
    Resolve the ticker list for a (market, scope).

    Parameters
    ----------
    market : str
        US, HK, or IN.
    scope : str
        "core" — hardcoded sets
        "full" — everything from common.universe for this market
        "custom" — use custom_tickers
    custom_tickers : list[str] or None
        Only used when scope="custom".

    Returns
    -------
    list[str]
        Deduplicated list including the benchmark.
    """
    if scope == "custom" and custom_tickers:
        tickers = [t.upper().strip() for t in custom_tickers]
    elif scope == "full":
        tickers = build_full_universe(market)
    else:
        tickers = list(_MARKET_CORE.get(market, BACKTEST_CORE_US))

    # Ensure benchmark
    benchmark = MARKET_BENCHMARKS.get(market, BENCHMARK_TICKER)
    if benchmark not in tickers:
        tickers = [benchmark] + tickers

    # Deduplicate preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            deduped.append(t)

    return deduped


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def ensure_history(
    tickers: list[str] | None = None,
    market: str = "US",
    scope: str = "core",
    force_refresh: bool = False,
    max_age_days: int = 7,
) -> dict[str, pd.DataFrame]:
    """
    Ensure historical OHLCV data is available for a market.

    Parameters
    ----------
    tickers : list[str] or None
        Explicit ticker list.  Overrides scope.
    market : str
        US, HK, or IN.
    scope : str
        "core" or "full".
    force_refresh : bool
        Force re-download even if cache exists.
    max_age_days : int
        Re-download if cache older than this.

    Returns
    -------
    dict[str, pd.DataFrame]
        {ticker: OHLCV DataFrame}
    """
    if tickers is not None:
        resolved = [t.upper().strip() for t in tickers]
        effective_scope = "custom"
    else:
        resolved = get_universe_tickers(market=market, scope=scope)
        effective_scope = scope

    benchmark = MARKET_BENCHMARKS.get(market, BENCHMARK_TICKER)
    if benchmark not in resolved:
        resolved = [benchmark] + resolved

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    cache = _cache_path(market, effective_scope)

    needs_download = (
        force_refresh
        or not cache.exists()
        or _cache_age_days(cache) > max_age_days
    )

    if needs_download:
        logger.info(
            f"[{market}] Downloading {len(resolved)} tickers "
            f"(scope={effective_scope}) from yfinance (period=max)..."
        )
        _download_and_cache(resolved, cache)
    else:
        logger.info(
            f"[{market}] Using cached data: {cache.name} "
            f"(age: {_cache_age_days(cache):.0f} days)"
        )

    return load_cached_history(
        resolved, market=market, scope=effective_scope,
    )


def load_cached_history(
    tickers: list[str] | None = None,
    market: str = "US",
    scope: str = "core",
) -> dict[str, pd.DataFrame]:
    """Load previously cached backtest data from parquet."""
    cache = _cache_path(market, scope)

    # Fall back to core cache if scope-specific doesn't exist
    if not cache.exists() and scope != "core":
        core_cache = _cache_path(market, "core")
        if core_cache.exists():
            logger.info(
                f"[{market}] {scope} cache not found, "
                f"falling back to core cache"
            )
            cache = core_cache

    if not cache.exists():
        logger.warning(
            f"Cache not found: {cache}. Call ensure_history() first."
        )
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
    """Slice every DataFrame in the universe to a date range."""
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


def list_universe_tickers(
    universe: str = "us",
) -> None:
    """Print the ticker list for a universe (for --show-universe)."""
    market, scope = resolve_universe(universe)
    tickers = get_universe_tickers(market=market, scope=scope)
    benchmark = MARKET_BENCHMARKS.get(market, "SPY")
    core_count = len(_MARKET_CORE.get(market, []))

    label = f"{market} {'full' if scope == 'full' else 'core'}"

    print(f"\n  Universe: {universe} → market={market}, scope={scope}")
    print(f"  Benchmark: {benchmark}")
    print(f"  Tickers: {len(tickers)} "
          f"(core={core_count}"
          f"{f', additional={len(tickers) - core_count}' if scope == 'full' else ''})")
    print(f"  {'─' * 50}")

    for i, t in enumerate(tickers, 1):
        marker = " ★" if t == benchmark else ""
        print(f"  {i:>4d}. {t}{marker}")
    print()


# ═══════════════════════════════════════════════════════════════
#  DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def _download_and_cache(
    tickers: list[str],
    cache_path: Path,
    batch_size: int = 50,
) -> None:
    """
    Download max-period data from yfinance and save as parquet.

    For large universes (100+ tickers), downloads in batches.
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance is required. pip install yfinance")

    t0 = time.time()
    n = len(tickers)
    logger.info(f"yfinance batch download: {n} tickers")

    records: list[pd.DataFrame] = []

    if n <= batch_size:
        batches = [tickers]
    else:
        batches = [
            tickers[i : i + batch_size]
            for i in range(0, n, batch_size)
        ]
        logger.info(
            f"Large universe — downloading in {len(batches)} "
            f"batches of ≤{batch_size}"
        )

    for batch_idx, batch in enumerate(batches, 1):
        if len(batches) > 1:
            logger.info(
                f"  Batch {batch_idx}/{len(batches)}: "
                f"{len(batch)} tickers "
                f"({batch[0]}…{batch[-1]})"
            )

        try:
            raw = yf.download(
                tickers=batch,
                period="max",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=True,
            )
        except Exception as e:
            logger.error(f"  Batch {batch_idx} download failed: {e}")
            continue

        if raw.empty:
            logger.warning(f"  Batch {batch_idx}: empty result")
            continue

        if len(batch) == 1:
            sym = batch[0]
            tmp = raw.copy().reset_index()
            tmp["symbol"] = sym
            _close_cols = [
                c for c in tmp.columns
                if str(c).lower() in ("close", "adj close")
            ]
            if _close_cols:
                tmp_check = tmp.dropna(subset=_close_cols, how="all")
                if not tmp_check.empty:
                    records.append(tmp)
        else:
            for sym in batch:
                try:
                    tmp = raw[sym].copy()
                    tmp = tmp.dropna(how="all")
                    if tmp.empty:
                        logger.debug(f"  {sym}: no data")
                        continue
                    tmp = tmp.reset_index()
                    tmp["symbol"] = sym
                    records.append(tmp)
                except KeyError:
                    logger.debug(f"  {sym}: not in download result")

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
    """Normalise to standard OHLCV with DatetimeIndex."""
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            c[0] if isinstance(c, tuple) else c for c in df.columns
        ]
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