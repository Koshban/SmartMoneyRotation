"""
backtest/__init__.py
=========
Historical backtesting harness for the CASH Smart Money Rotation system.

Loads 20 years of OHLCV, runs the full pipeline over any date range,
computes performance metrics (including CAGR), and compares strategy
variants side-by-side.

Supports multi-market backtesting (US, HK, India) and convergence-
aware signal generation.

Quick start
-----------
    from backtest.engine import run_backtest_period
    from backtest.data_loader import ensure_history

    data = ensure_history()
    result = run_backtest_period(data)
    print(f"CAGR: {result.metrics['cagr']:.2%}")

CLI
---
    python -m backtest.runner                          # 20-year default
    python -m backtest.runner --start 2015 --end 2024  # custom period
    python -m backtest.runner --compare                # all strategies
    python -m backtest.runner --strategy momentum_heavy
    python -m backtest.runner --market HK              # Hong Kong
    python -m backtest.runner --market IN              # India
"""

from backtest.phase1.engine import run_backtest_period, BacktestRun, StrategyConfig
from backtest.phase1.metrics import (
    compute_cagr,
    compute_full_metrics,
    cagr_from_equity,
    rolling_cagr,
    compute_monthly_returns_heatmap,
    compute_regime_metrics,
)
from backtest.phase1.comparison import compare_strategies
from backtest.phase1.data_loader import ensure_history, load_cached_history

__all__ = [
    "run_backtest_period",
    "BacktestRun",
    "StrategyConfig",
    "compute_cagr",
    "compute_full_metrics",
    "cagr_from_equity",
    "rolling_cagr",
    "compute_monthly_returns_heatmap",
    "compute_regime_metrics",
    "compare_strategies",
    "ensure_history",
    "load_cached_history",
]

###########################################
"""
backtest/comparison.py
----------------------
Run multiple strategy variants over the same period and produce
a side-by-side comparison.

Enhanced with equity curve export and per-period analysis.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtest.phase1.engine import BacktestRun, StrategyConfig, run_backtest_period
from backtest.phase1.strategies import ALL_STRATEGIES, US_STRATEGIES
from backtest.phase1.metrics import metrics_report

logger = logging.getLogger(__name__)


def compare_strategies(
    data: dict[str, pd.DataFrame],
    *,
    strategies: list[StrategyConfig] | None = None,
    market: str = "US",
    start: str | None = None,
    end: str | None = None,
    capital: float = 100_000.0,
    rank_by: str = "cagr",
    current_holdings: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run every strategy variant over the same period and return
    a comparison.

    Enhanced: filters strategies by market, exports equity curves.
    """
    if strategies is None:
        market_strats = {
            k: v for k, v in ALL_STRATEGIES.items()
            if v.market == market.upper()
        }
        strategies = list(market_strats.values())

    logger.info(
        f"Comparing {len(strategies)} strategies [{market}] "
        f"({start or 'earliest'} → {end or 'latest'})"
    )

    runs: list[BacktestRun] = []

    for i, strat in enumerate(strategies, 1):
        logger.info(f"[{i}/{len(strategies)}] Running: {strat.name}")
        run = run_backtest_period(
            data, start=start, end=end,
            strategy=strat, capital=capital,
            current_holdings=current_holdings,
        )
        runs.append(run)

    table = _build_comparison_table(runs, rank_by=rank_by)
    report = _comparison_report(runs, table, rank_by)
    equity_curves = _build_equity_curves(runs)

    valid_runs = [r for r in runs if r.ok]
    best = max(valid_runs, key=lambda r: r.metrics.get(rank_by, -999)) if valid_runs else None
    worst = min(valid_runs, key=lambda r: r.metrics.get(rank_by, 999)) if valid_runs else None

    return {
        "runs": runs,
        "table": table,
        "report": report,
        "equity_curves": equity_curves,
        "best": best,
        "worst": worst,
    }


def _build_equity_curves(runs: list[BacktestRun]) -> pd.DataFrame:
    """Build a DataFrame of equity curves for all successful runs."""
    curves: dict[str, pd.Series] = {}
    for run in runs:
        if run.ok and run.backtest_result:
            eq = run.backtest_result.equity_curve
            if not eq.empty:
                curves[runrefactor.strategy.name] = eq
    if not curves:
        return pd.DataFrame()
    return pd.DataFrame(curves)


def _build_comparison_table(
    runs: list[BacktestRun],
    rank_by: str = "cagr",
) -> pd.DataFrame:
    rows = []
    for run in runs:
        m = run.metrics
        rows.append({
            "strategy":       runrefactor.strategy.name,
            "market":         runrefactor.strategy.market,
            "cagr":           m.get("cagr"),
            "total_return":   m.get("total_return"),
            "sharpe":         m.get("sharpe_ratio"),
            "sortino":        m.get("sortino_ratio"),
            "calmar":         m.get("calmar_ratio"),
            "max_drawdown":   m.get("max_drawdown"),
            "annual_vol":     m.get("annual_volatility"),
            "win_rate":       m.get("win_rate"),
            "total_trades":   m.get("total_trades"),
            "profit_factor":  m.get("profit_factor"),
            "expectancy":     m.get("expectancy"),
            "excess_cagr":    m.get("excess_cagr"),
            "alpha":          m.get("alpha"),
            "beta":           m.get("beta"),
            "info_ratio":     m.get("information_ratio"),
            "final_capital":  m.get("final_capital"),
            "best_year":      m.get("best_year"),
            "worst_year":     m.get("worst_year"),
            "elapsed_s":      run.elapsed_seconds,
            "error":          run.error,
        })

    df = pd.DataFrame(rows)
    if rank_by in df.columns and df[rank_by].notna().any():
        ascending = rank_by in ("max_drawdown", "annual_vol")
        df = df.sort_values(rank_by, ascending=ascending, na_position="last")
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "rank"
    return df


def _comparison_report(
    runs: list[BacktestRun],
    table: pd.DataFrame,
    rank_by: str,
) -> str:
    ln: list[str] = []
    div = "=" * 94
    sub = "-" * 94

    valid = [r for r in runs if r.ok]
    if not valid:
        return "No successful backtest runs to compare."

    first = valid[0]
    ln.append(div)
    ln.append(f"  STRATEGY COMPARISON [{firstrefactor.strategy.market}]")
    ln.append(div)
    ln.append(f"  Period:     {first.start_date.date()} → {first.end_date.date()}")
    ln.append(f"  Capital:    ${first.metrics.get('initial_capital', 0):,.0f}")
    ln.append(f"  Strategies: {len(runs)} tested, {len(valid)} successful")
    ln.append(f"  Ranked by:  {rank_by}")

    ln.append("")
    ln.append(sub)
    ln.append(
        f"  {'#':>2}  {'Strategy':<24s} {'CAGR':>8} {'Sharpe':>7} "
        f"{'MaxDD':>8} {'Win%':>6} {'Trades':>7} "
        f"{'Final$':>12} {'ExcessCAGR':>10}"
    )
    ln.append(sub)

    for idx, row in table.iterrows():
        if row.get("error"):
            ln.append(f"  {idx:>2}  {row['strategy']:<24s}  ERROR: {row['error']}")
            continue

        cagr_s = f"{row['cagr']:>+7.2%}" if pd.notna(row.get("cagr")) else "    N/A"
        sharpe_s = f"{row['sharpe']:>6.2f}" if pd.notna(row.get("sharpe")) else "   N/A"
        dd_s = f"{row['max_drawdown']:>7.2%}" if pd.notna(row.get("max_drawdown")) else "    N/A"
        wr_s = f"{row['win_rate']:>5.1%}" if pd.notna(row.get("win_rate")) else "  N/A"
        trades_s = f"{int(row['total_trades']):>6d}" if pd.notna(row.get("total_trades")) else "   N/A"
        final_s = f"${row['final_capital']:>10,.0f}" if pd.notna(row.get("final_capital")) else "       N/A"
        excess_s = f"{row['excess_cagr']:>+9.2%}" if pd.notna(row.get("excess_cagr")) else "      N/A"

        ln.append(
            f"  {idx:>2}  {row['strategy']:<24s} {cagr_s} {sharpe_s} "
            f"{dd_s} {wr_s} {trades_s} {final_s} {excess_s}"
        )

    ln.append("")
    ln.append(sub)
    ln.append("  HIGHLIGHTS")
    ln.append(sub)

    if not table.empty and table["cagr"].notna().any():
        best_row = table.loc[table["cagr"].idxmax()]
        worst_row = table.loc[table["cagr"].idxmin()]
        ln.append(f"  Best CAGR:    {best_row['strategy']:<20s} {best_row['cagr']:>+7.2%}")
        ln.append(f"  Worst CAGR:   {worst_row['strategy']:<20s} {worst_row['cagr']:>+7.2%}")

    if not table.empty and table["sharpe"].notna().any():
        best_sh = table.loc[table["sharpe"].idxmax()]
        ln.append(f"  Best Sharpe:  {best_sh['strategy']:<20s} {best_sh['sharpe']:>6.2f}")

    if not table.empty and table["max_drawdown"].notna().any():
        best_dd = table.loc[table["max_drawdown"].idxmax()]
        ln.append(f"  Smallest DD:  {best_dd['strategy']:<20s} {best_dd['max_drawdown']:>7.2%}")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)


###########################

"""
backtest/data_loader.py
-----------------------
Download, cache, and serve historical OHLCV data for backtesting.

Enhanced:
  - Dynamically builds universe from common.universe (--universe full)
  - Separate cache files per universe scope (core vs full)
  - Multi-market support (US, HK, India)
  - Robust ticker extraction from any universe.py structure
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


def _cache_path(market: str = "US", scope: str = "core") -> Path:
    """
    Cache file per market + scope.

    core → backtest_us_universe.parquet        (backward compatible)
    full → backtest_us_full_universe.parquet    (everything from universe.py)
    """
    if scope == "core":
        return BACKTEST_DIR / f"backtest_{market.lower()}_universe.parquet"
    return BACKTEST_DIR / f"backtest_{market.lower()}_{scope}_universe.parquet"


# ═══════════════════════════════════════════════════════════════
#  CORE UNIVERSE — the original hardcoded ETF set (backward compat)
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

_MARKET_CORE_UNIVERSES = {
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
#  FULL UNIVERSE — dynamically built from common.universe
# ═══════════════════════════════════════════════════════════════

def _extract_tickers_from_value(val) -> set[str]:
    """
    Extract ticker strings from any universe.py data structure.

    Handles: list, tuple, set, frozenset, dict (both
    {ticker: meta} and {group: [tickers]} forms), and
    nested combinations.
    """
    tickers: set[str] = set()

    if isinstance(val, str):
        # Single ticker string
        cleaned = val.strip().upper()
        if cleaned and len(cleaned) <= 12:
            tickers.add(cleaned)

    elif isinstance(val, (list, tuple, set, frozenset)):
        for item in val:
            tickers.update(_extract_tickers_from_value(item))

    elif isinstance(val, dict):
        for k, v in val.items():
            # Key might be a ticker (e.g. {"XLK": "Technology"})
            if isinstance(k, str):
                cleaned = k.strip().upper()
                # Looks like a ticker if short, all-caps, no spaces
                if 1 <= len(cleaned) <= 12 and cleaned.isalnum() or "." in cleaned:
                    tickers.add(cleaned)
            # Value might contain tickers
            tickers.update(_extract_tickers_from_value(v))

    return tickers


def build_full_universe(market: str = "US") -> list[str]:
    """
    Build the complete backtest universe from common.universe.

    Inspects every public attribute of common.universe and extracts
    any ticker-like strings.  Always includes the core ETF set as
    a safety net.

    For non-US markets, falls back to the hardcoded universe.

    Returns
    -------
    list[str]
        Sorted, deduplicated list of tickers.
    """
    if market != "US":
        return list(_MARKET_CORE_UNIVERSES.get(market, []))

    tickers: set[str] = set()

    # ── 1. Import common.universe ─────────────────────────────
    try:
        import common.universe as uni
    except ImportError:
        logger.warning(
            "Cannot import common.universe — "
            "falling back to core universe"
        )
        return list(BACKTEST_CORE_UNIVERSE)

    # ── 2. Known named exports ────────────────────────────────
    _KNOWN_ATTRS = [
        # Groupings we know about from the import statements
        "SECTORS", "BROAD_MARKET", "FIXED_INCOME", "COMMODITIES",
        # Other likely groupings
        "THEMATIC", "ALTERNATIVES", "INTERNATIONAL",
        "GROWTH", "VALUE", "DIVIDEND", "DEFENSIVE",
        # Sector-to-ticker mappings
        "SECTOR_TICKERS", "SECTOR_TO_TICKERS", "SECTOR_STOCKS",
        "SECTOR_MEMBERS", "TICKER_SECTOR",
        # Catch-all lists
        "ALL_TICKERS", "UNIVERSE", "FULL_UNIVERSE", "US_UNIVERSE",
        "BACKTEST_UNIVERSE", "WATCHLIST",
    ]

    for attr_name in _KNOWN_ATTRS:
        val = getattr(uni, attr_name, None)
        if val is not None:
            extracted = _extract_tickers_from_value(val)
            if extracted:
                logger.debug(
                    f"  common.universe.{attr_name}: "
                    f"{len(extracted)} tickers"
                )
                tickers.update(extracted)

    # ── 3. Discover additional public attributes ──────────────
    #    Scan anything we haven't already checked.
    checked = set(_KNOWN_ATTRS)
    for attr_name in dir(uni):
        if attr_name.startswith("_") or attr_name in checked:
            continue
        if callable(getattr(uni, attr_name, None)):
            # Try calling get_all_tickers() etc.
            if attr_name.lower() in (
                "get_all_tickers", "all_tickers", "get_universe",
                "get_full_universe",
            ):
                try:
                    result = getattr(uni, attr_name)()
                    extracted = _extract_tickers_from_value(result)
                    if extracted:
                        logger.debug(
                            f"  common.universe.{attr_name}(): "
                            f"{len(extracted)} tickers"
                        )
                        tickers.update(extracted)
                except Exception:
                    pass
            continue

        val = getattr(uni, attr_name, None)
        if val is None:
            continue

        # Only inspect things that look like ticker collections
        if isinstance(val, (list, tuple, set, frozenset, dict)):
            extracted = _extract_tickers_from_value(val)
            # Only add if it looks like a real ticker collection
            # (at least 2 tickers, not some random config dict)
            if len(extracted) >= 2:
                logger.debug(
                    f"  common.universe.{attr_name}: "
                    f"{len(extracted)} tickers (discovered)"
                )
                tickers.update(extracted)

    # ── 4. Always include core ETFs ───────────────────────────
    tickers.update(BACKTEST_CORE_UNIVERSE)

    # ── 5. Clean and sort ─────────────────────────────────────
    # Remove anything that doesn't look like a valid ticker
    cleaned: set[str] = set()
    for t in tickers:
        t = t.strip().upper()
        if not t:
            continue
        # Basic sanity: 1-12 chars, alphanumeric (with . for HK/IN)
        if len(t) > 12:
            continue
        # Remove common false positives
        if t in (
            "TRUE", "FALSE", "NONE", "NAN", "NULL",
            "BUY", "SELL", "HOLD", "STRONG", "WEAK",
        ):
            continue
        cleaned.add(t)

    result = sorted(cleaned)

    logger.info(
        f"Built full universe: {len(result)} tickers "
        f"from common.universe "
        f"(core={len(BACKTEST_CORE_UNIVERSE)}, "
        f"additional={len(result) - len(BACKTEST_CORE_UNIVERSE)})"
    )

    return result


def get_universe_tickers(
    market: str = "US",
    scope: str = "core",
    custom_tickers: list[str] | None = None,
) -> list[str]:
    """
    Resolve the ticker list for a given universe scope.

    Parameters
    ----------
    market : str
        US, HK, or IN.
    scope : str
        "core" — hardcoded ETF set (41 tickers for US)
        "full" — everything from common.universe
        "custom" — use custom_tickers list
    custom_tickers : list[str] or None
        Only used when scope="custom".

    Returns
    -------
    list[str]
        Deduplicated ticker list including the benchmark.
    """
    if scope == "custom" and custom_tickers:
        tickers = [t.upper().strip() for t in custom_tickers]
    elif scope == "full":
        tickers = build_full_universe(market)
    else:
        tickers = list(_MARKET_CORE_UNIVERSES.get(market, BACKTEST_CORE_UNIVERSE))

    # Ensure benchmark is included
    benchmark = _MARKET_BENCHMARKS.get(market, BENCHMARK_TICKER)
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
        "core" — hardcoded ETF set (default, backward compatible)
        "full" — everything from common.universe
    force_refresh : bool
        Re-download even if cache exists.
    max_age_days : int
        Re-download if cache is older than this.

    Returns
    -------
    dict[str, pd.DataFrame]
        {ticker: OHLCV DataFrame} ready for the pipeline.
    """
    if tickers is not None:
        resolved = [t.upper().strip() for t in tickers]
        effective_scope = "custom"
    else:
        resolved = get_universe_tickers(market=market, scope=scope)
        effective_scope = scope

    # Ensure benchmark
    benchmark = _MARKET_BENCHMARKS.get(market, BENCHMARK_TICKER)
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

    return load_cached_history(resolved, market=market, scope=effective_scope)


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
            f"Cache not found: {cache}. "
            f"Call ensure_history() first."
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
    market: str = "US",
    scope: str = "full",
) -> None:
    """Print the universe tickers for inspection."""
    tickers = get_universe_tickers(market=market, scope=scope)
    print(f"\n  Universe [{market}] scope={scope}: "
          f"{len(tickers)} tickers\n")
    for i, t in enumerate(tickers, 1):
        print(f"  {i:>4d}. {t}")
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

    For large universes (100+ tickers), downloads in batches to
    avoid yfinance timeouts.
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance is required. pip install yfinance")

    t0 = time.time()
    n = len(tickers)
    logger.info(f"yfinance batch download: {n} tickers")

    records: list[pd.DataFrame] = []

    # Download in batches for large universes
    if n <= batch_size:
        batches = [tickers]
    else:
        batches = [
            tickers[i : i + batch_size]
            for i in range(0, n, batch_size)
        ]
        logger.info(
            f"Large universe — downloading in {len(batches)} "
            f"batches of {batch_size}"
        )

    for batch_idx, batch in enumerate(batches, 1):
        if len(batches) > 1:
            logger.info(
                f"  Batch {batch_idx}/{len(batches)}: "
                f"{len(batch)} tickers "
                f"({batch[0]}...{batch[-1]})"
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
            if not tmp.dropna(subset=["Close"] if "Close" in tmp.columns
                              else ["close"] if "close" in tmp.columns
                              else [], how="all").empty:
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
    """Normalise to standard OHLCV format with DatetimeIndex."""
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


#####################################################


"""
backtest/engine.py
------------------
Core backtesting engine — ties together data loading, the full
CASH pipeline (including rotation + convergence), and the
portfolio simulation.

Key improvements over the original:
  - Multi-market support (US, HK, IN) via market-aware StrategyConfig
  - lookback_days passthrough to Orchestrator for indicator warm-up
  - Rotation engine and convergence automatically invoked for US
  - Regime-conditional metrics when breadth data is available
  - Fixed cash-proxy equity adjustment bug
  - Cleaner signal column detection
"""

from __future__ import annotations

import copy
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SCORING_WEIGHTS,
    SCORING_PARAMS,
    SIGNAL_PARAMS,
    BREADTH_PORTFOLIO,
    MARKET_CONFIG,
)
from pipeline.orchestrator import Orchestrator, PipelineResult
from portfolio.backtest import (
    BacktestConfig,
    BacktestResult,
    run_backtest as run_portfolio_backtest,
    compute_performance_metrics,
)
from portfolio.sizing import SizingConfig
from portfolio.rebalance import RebalanceConfig
from output.signals import SignalConfig

from backtest.phase1.data_loader import slice_period, data_summary
from backtest.phase1.metrics import compute_full_metrics

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

DEFENSIVE_TICKERS = frozenset({
    "AGG", "SHY", "TLT", "IEF", "GLD", "BIL",
})

_SIGNAL_COLS = ("signal", "action", "trade_signal")
_TICKER_COLS = ("ticker", "symbol", "asset")
_DATE_COLS   = ("date", "trade_date", "timestamp")
_SCORE_COLS  = ("composite_score", "score", "total_score", "rank_score")
_REGIME_COLS = (
    "breadth_regime", "regime", "market_regime", "breadth_label",
)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL VALUE HELPERS
# ═══════════════════════════════════════════════════════════════

def _is_buy(val) -> bool:
    if isinstance(val, str):
        return val.upper() in ("BUY", "STRONG_BUY", "ENTRY")
    try:
        return float(val) == 1.0
    except (TypeError, ValueError):
        return False


def _is_sell(val) -> bool:
    if isinstance(val, str):
        return val.upper() in ("SELL", "STRONG_SELL", "EXIT")
    try:
        return float(val) == -1.0
    except (TypeError, ValueError):
        return False


def _buy_value(sample) -> Any:
    return "BUY" if isinstance(sample, str) else 1


def _hold_value(sample) -> Any:
    return "HOLD" if isinstance(sample, str) else 0


# ═══════════════════════════════════════════════════════════════
#  STRATEGY CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class StrategyConfig:
    """
    A named set of parameter overrides for backtesting.

    Enhanced with:
      - ``market`` — target market (US / HK / IN)
      - ``enable_rotation`` — whether to run rotation engine
      - ``enable_convergence`` — whether to merge scoring + rotation
      - ``lookback_days`` — analysis window for the pipeline
    """
    name: str = "baseline"
    description: str = "Default CASH parameters"

    # ── Market ────────────────────────────────────────────────
    market: str = "US"

    # ── Config-dict overrides (applied to globals) ────────────
    scoring_weights:   dict | None = None
    scoring_params:    dict | None = None
    signal_params:     dict | None = None
    portfolio_params:  dict | None = None
    breadth_portfolio: dict | None = None

    # ── Component-config overrides ────────────────────────────
    signal_config_overrides:   dict | None = None
    sizing_config_overrides:   dict | None = None
    backtest_config_overrides: dict | None = None

    # ── Universe filter ───────────────────────────────────────
    universe_filter: list[str] | None = None

    # ── Pipeline feature flags ────────────────────────────────
    enable_rotation: bool = True
    enable_convergence: bool = True
    lookback_days: int | None = None

    # ── Trading rules ─────────────────────────────────────────
    min_hold_days: int = 20
    cash_proxy: str | None = "SHY"

    # ── Breadth crisis response ───────────────────────────────
    breadth_defensive: bool = True
    max_equity_weak: float = 0.40
    max_equity_critical: float = 0.15


# ═══════════════════════════════════════════════════════════════
#  BACKTEST RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestRun:
    """Complete output of a single backtest run."""
    strategy: StrategyConfig
    start_date: pd.Timestamp
    end_date: pd.Timestamp

    pipeline_result: PipelineResult | None = None
    backtest_result: BacktestResult | None = None

    metrics: dict = field(default_factory=dict)
    annual_returns: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )
    monthly_returns: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark_equity: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )

    elapsed_seconds: float = 0.0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.metrics.get("cagr") is not None

    @property
    def cagr(self) -> float:
        return self.metrics.get("cagr", 0.0)

    @property
    def sharpe(self) -> float:
        return self.metrics.get("sharpe_ratio", 0.0)

    @property
    def max_drawdown(self) -> float:
        return self.metrics.get("max_drawdown", 0.0)

    def summary_line(self) -> str:
        if not self.ok:
            return f"{selfrefactor.strategy.name:<24s}  ERROR: {self.error}"
        return (
            f"{selfrefactor.strategy.name:<24s}  "
            f"CAGR={self.cagr:>+7.2%}  "
            f"Sharpe={self.sharpe:>5.2f}  "
            f"MaxDD={self.max_drawdown:>7.2%}  "
            f"Trades={self.metrics.get('total_trades', 0):>5d}  "
            f"({self.elapsed_seconds:.0f}s)"
        )


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_backtest_period(
    data: dict[str, pd.DataFrame],
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    strategy: StrategyConfig | None = None,
    capital: float = 100_000.0,
    benchmark: str | None = None,
    current_holdings: list[str] | None = None,
) -> BacktestRun:
    """
    Run a complete backtest over a date range.

    Steps
    -----
    1. Slice data to [start, end]
    2. Apply strategy parameter overrides
    3. Run the full CASH pipeline (with rotation + convergence for US)
    4. Pre-process signals (min-hold, breadth override)
    5. Run the portfolio simulation
    6. Adjust equity for cash-proxy returns
    7. Compute performance metrics (including regime-conditional)
    """
    if strategy is None:
        strategy = StrategyConfig()

    market = strategy.market
    mcfg = MARKET_CONFIG.get(market, MARKET_CONFIG["US"])
    benchmark = benchmark or mcfg.get("benchmark", BENCHMARK_TICKER)
    t0 = time.perf_counter()

    # ── 1. Slice data ─────────────────────────────────────────
    sliced = slice_period(data, start=start, end=end)
    if not sliced:
        return BacktestRun(
            strategy=strategy,
            start_date=pd.Timestamp(start) if start else pd.NaT,
            end_date=pd.Timestamp(end) if end else pd.NaT,
            error="No data after slicing to requested period",
        )

    # ── 2. Apply universe filter ──────────────────────────────
    if strategy.universe_filter:
        must_keep = {benchmark}
        if strategy.cash_proxy and strategy.cash_proxy in data:
            must_keep.add(strategy.cash_proxy)
        sliced = {
            k: v for k, v in sliced.items()
            if k in strategy.universe_filter or k in must_keep
        }

    summary = data_summary(sliced)
    actual_start = summary["earliest_start"]
    actual_end = summary["latest_end"]

    logger.info(
        f"Backtest '{strategy.name}' [{market}]: "
        f"{summary['n_tickers']} tickers, "
        f"{actual_start.date()} → {actual_end.date()}, "
        f"${capital:,.0f}"
    )

    # ── 3. Extract benchmark equity ───────────────────────────
    bench_df = sliced.get(benchmark)
    if bench_df is None or bench_df.empty:
        logger.warning(
            f"Benchmark {benchmark} not in data — "
            f"benchmark comparison will be unavailable"
        )
        bench_equity = pd.Series(dtype=float)
    else:
        bench_equity = (
            bench_df["close"] / bench_df["close"].iloc[0] * capital
        )
        bench_equity.name = "benchmark"

    # ══════════════════════════════════════════════════════════
    #  Config overrides applied to BOTH pipeline AND simulation
    # ══════════════════════════════════════════════════════════
    with _config_overrides(strategy):

        # ── 4a. Determine pipeline feature flags ──────────────
        engines = mcfg.get("engines", ["scoring"])
        run_rotation = (
            strategy.enable_rotation
            and "rotation" in engines
        )
        run_convergence = (
            strategy.enable_convergence
            and run_rotation
        )

        # ── 4b. Run CASH pipeline ────────────────────────────
        try:
            orch = Orchestrator(
                market=market,
                universe=list(sliced.keys()),
                benchmark=benchmark,
                capital=capital,
                lookback_days=strategy.lookback_days,
                enable_breadth=True,
                enable_sectors=mcfg.get("sector_rs_enabled", True),
                enable_signals=True,
                enable_backtest=False,
            )

            orch.load_data(preloaded=sliced, bench_df=bench_df)
            orch.compute_universe_context()
            orch.run_tickers()

            # Rotation engine (US only, when enabled)
            if run_rotation:
                try:
                    orch.run_rotation_engine(
                        current_holdings=current_holdings or [],
                    )
                except Exception as e:
                    logger.warning(
                        f"Rotation engine failed: {e} — "
                        f"continuing without rotation"
                    )

            # Convergence merge
            if run_convergence:
                try:
                    orch.apply_convergence()
                except Exception as e:
                    logger.warning(
                        f"Convergence failed: {e} — "
                        f"continuing without convergence"
                    )

            orch.cross_sectional_analysis()
            pipeline_result = orch.generate_reports()

        except Exception as e:
            logger.error(f"Pipeline failed for '{strategy.name}': {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Pipeline error: {e}",
            )

        # ── 4c. Validate signals ─────────────────────────────
        signals_df = pipeline_result.signals
        if signals_df is None or signals_df.empty:
            logger.warning(f"No signals generated for '{strategy.name}'")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error="No signals generated — check pipeline logs",
            )

        # ── 4d. Pre-process signals ──────────────────────────
        signals_df = _preprocess_signals(
            signals_df, strategy, pipeline_result, sliced,
        )

        # ── 4e. Run portfolio simulation ──────────────────────
        try:
            bt_config = _build_backtest_config(strategy, capital)
            bt_result = run_portfolio_backtest(
                signals_df=signals_df,
                config=bt_config,
            )
        except Exception as e:
            logger.error(f"Backtest simulation failed: {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Simulation error: {e}",
            )

    # ── 5. Adjust equity for cash-proxy returns ───────────────
    equity_curve = bt_result.equity_curve.copy()

    if strategy.cash_proxy and strategy.cash_proxy in sliced:
        equity_curve = _apply_cash_proxy_to_equity(
            equity_curve=equity_curve,
            bt_result=bt_result,
            proxy_prices=sliced[strategy.cash_proxy],
            initial_capital=capital,
            proxy_ticker=strategy.cash_proxy,
        )

    daily_returns = equity_curve.pct_change().dropna()

    # ── 6. Extract breadth regime for conditional metrics ─────
    breadth_regime = None
    if (
        pipeline_result.breadth is not None
        and not pipeline_result.breadth.empty
        and "breadth_regime" in pipeline_result.breadth.columns
    ):
        breadth_regime = pipeline_result.breadth["breadth_regime"]

    # ── 7. Compute comprehensive metrics ──────────────────────
    metrics = compute_full_metrics(
        equity_curve=equity_curve,
        daily_returns=daily_returns,
        trades=bt_result.trades,
        initial_capital=capital,
        benchmark_equity=bench_equity,
        breadth_regime=breadth_regime,
    )

    annual = _compute_annual_returns(equity_curve)
    monthly = _compute_monthly_returns(daily_returns)
    elapsed = time.perf_counter() - t0

    run = BacktestRun(
        strategy=strategy,
        start_date=actual_start,
        end_date=actual_end,
        pipeline_result=pipeline_result,
        backtest_result=bt_result,
        metrics=metrics,
        annual_returns=annual,
        monthly_returns=monthly,
        benchmark_equity=bench_equity,
        elapsed_seconds=elapsed,
    )

    logger.info(run.summary_line())
    return run


# ═══════════════════════════════════════════════════════════════
#  CONFIG OVERRIDE CONTEXT MANAGER
# ═══════════════════════════════════════════════════════════════

@contextmanager
def _config_overrides(strategy: StrategyConfig):
    import common.config as cfg

    config_targets = [
        ("SCORING_WEIGHTS",  cfg.SCORING_WEIGHTS,  strategy.scoring_weights),
        ("SCORING_PARAMS",   cfg.SCORING_PARAMS,   strategy.scoring_params),
        ("SIGNAL_PARAMS",    cfg.SIGNAL_PARAMS,    strategy.signal_params),
        ("PORTFOLIO_PARAMS", cfg.PORTFOLIO_PARAMS,  strategy.portfolio_params),
        ("BREADTH_PORTFOLIO", cfg.BREADTH_PORTFOLIO, strategy.breadth_portfolio),
    ]

    originals: list[tuple[str, dict, dict]] = []
    for name, target_dict, overrides in config_targets:
        originals.append((name, target_dict, dict(target_dict)))
        if overrides:
            logger.debug(f"Config override [{strategy.name}] {name}: {overrides}")
            target_dict.update(overrides)

    n_overridden = sum(1 for _, _, ov in config_targets if ov)
    if n_overridden:
        logger.info(f"Applied {n_overridden} config overrides for '{strategy.name}'")

    try:
        yield
    finally:
        for _name, target_dict, original_values in originals:
            target_dict.clear()
            target_dict.update(original_values)


# ═══════════════════════════════════════════════════════════════
#  COLUMN DETECTION
# ═══════════════════════════════════════════════════════════════

def _detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    def _find(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None
    return {
        "signal": _find(_SIGNAL_COLS),
        "ticker": _find(_TICKER_COLS),
        "date":   _find(_DATE_COLS),
        "score":  _find(_SCORE_COLS),
        "regime": _find(_REGIME_COLS),
    }


# ═══════════════════════════════════════════════════════════════
#  SIGNAL PRE-PROCESSING
# ═══════════════════════════════════════════════════════════════

def _preprocess_signals(
    signals_df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    price_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    cols = _detect_columns(signals_df)
    sig_col    = cols["signal"]
    ticker_col = cols["ticker"]
    date_col   = cols["date"]

    if not sig_col or not ticker_col:
        logger.warning(
            "Cannot detect signal/ticker columns — "
            "skipping all signal preprocessing"
        )
        return signals_df

    df = signals_df.copy()

    if strategy.breadth_defensive:
        df = _apply_breadth_override(
            df, strategy, pipeline_result,
            sig_col, ticker_col, date_col, cols["score"],
        )

    if strategy.min_hold_days > 0 and date_col:
        df = _enforce_min_hold(
            df, strategy.min_hold_days,
            sig_col, ticker_col, date_col,
        )

    return df


# ═══════════════════════════════════════════════════════════════
#  BREADTH CRISIS OVERRIDE
# ═══════════════════════════════════════════════════════════════

def _extract_regime_column(
    df: pd.DataFrame,
    pipeline_result: PipelineResult,
    date_col: str | None,
) -> tuple[pd.DataFrame, str | None]:
    for col in _REGIME_COLS:
        if col in df.columns:
            return df, col

    if pipeline_result is None or date_col is None:
        return df, None

    breadth = getattr(pipeline_result, "breadth", None)
    if breadth is None:
        return df, None

    regime_series: pd.Series | None = None

    if isinstance(breadth, pd.DataFrame):
        for col in _REGIME_COLS:
            if col in breadth.columns:
                regime_series = breadth[col]
                break

    if regime_series is None:
        return df, None

    regime_df = regime_series.to_frame(name="_breadth_regime")
    if date_col in df.columns:
        df = df.merge(
            regime_df, left_on=date_col, right_index=True, how="left",
        )
        return df, "_breadth_regime"

    return df, None


def _apply_breadth_override(
    df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    sig_col: str,
    ticker_col: str,
    date_col: str | None,
    score_col: str | None,
) -> pd.DataFrame:
    df, regime_col = _extract_regime_column(df, pipeline_result, date_col)

    if regime_col is None:
        return df

    regime_lower = df[regime_col].astype(str).str.lower()
    weak_mask     = regime_lower == "weak"
    critical_mask = regime_lower == "critical"
    crisis_mask   = weak_mask | critical_mask

    if not crisis_mask.any():
        return df

    sample_sig = df[sig_col].dropna().iloc[0] if len(df) else "BUY"
    hold = _hold_value(sample_sig)
    buy  = _buy_value(sample_sig)

    is_equity = ~df[ticker_col].isin(DEFENSIVE_TICKERS)
    is_buy    = df[sig_col].apply(_is_buy)
    block     = crisis_mask & is_equity & is_buy
    n_blocked = int(block.sum())

    if n_blocked:
        df.loc[block, sig_col] = hold
        logger.info(f"Breadth override: blocked {n_blocked:,} equity BUY signals")

    proxy = strategy.cash_proxy
    if proxy and score_col and proxy in df[ticker_col].values:
        proxy_crisis = crisis_mask & (df[ticker_col] == proxy)
        if proxy_crisis.any():
            max_score = df[score_col].max()
            df.loc[proxy_crisis, score_col] = max_score
            df.loc[proxy_crisis, sig_col]   = buy

    return df


# ═══════════════════════════════════════════════════════════════
#  MINIMUM HOLDING PERIOD
# ═══════════════════════════════════════════════════════════════

def _enforce_min_hold(
    df: pd.DataFrame,
    min_hold_days: int,
    sig_col: str,
    ticker_col: str,
    date_col: str,
) -> pd.DataFrame:
    if min_hold_days <= 0 or date_col not in df.columns:
        return df

    result = df.copy()
    sample_sig = result[sig_col].dropna().iloc[0] if len(result) else "BUY"
    keep_holding = _buy_value(sample_sig)

    total_suppressed = 0

    for ticker, group_indices in result.groupby(ticker_col).groups.items():
        sub = result.loc[group_indices, [date_col, sig_col]].sort_values(date_col)
        dates   = sub[date_col].values
        signals = sub[sig_col].values
        indices = sub.index.values

        in_position = False
        entry_ts = None
        fix_list: list = []

        for i in range(len(indices)):
            sig = signals[i]
            dt  = dates[i]

            if _is_buy(sig) and not in_position:
                in_position = True
                entry_ts = dt
            elif _is_buy(sig) and in_position:
                pass
            elif _is_sell(sig) and in_position:
                try:
                    days_held = (pd.Timestamp(dt) - pd.Timestamp(entry_ts)).days
                except Exception:
                    days_held = min_hold_days

                if days_held < min_hold_days:
                    fix_list.append(indices[i])
                else:
                    in_position = False
                    entry_ts = None

        if fix_list:
            result.loc[fix_list, sig_col] = keep_holding
            total_suppressed += len(fix_list)

    if total_suppressed:
        logger.info(
            f"Min-hold filter: suppressed {total_suppressed:,} "
            f"premature exits (min {min_hold_days} cal-days)"
        )

    return result


# ═══════════════════════════════════════════════════════════════
#  CASH PROXY — EQUITY ADJUSTMENT  (BUG FIXED)
# ═══════════════════════════════════════════════════════════════

def _apply_cash_proxy_to_equity(
    equity_curve: pd.Series,
    bt_result: BacktestResult,
    proxy_prices: pd.DataFrame,
    initial_capital: float,
    proxy_ticker: str = "SHY",
) -> pd.Series:
    """
    Retroactively credit idle cash with the proxy's return.

    Fixed: proxy_ticker is now passed explicitly instead of
    relying on a misnamed helper function.
    """
    if "close" in proxy_prices.columns:
        proxy_close = proxy_prices["close"]
    else:
        proxy_close = proxy_prices.iloc[:, 0]

    proxy_ret = proxy_close.pct_change().fillna(0.0)

    # Try to get actual cash balance from backtest result
    cash_series: pd.Series | None = None

    for attr in ("cash", "cash_series", "cash_balance"):
        val = getattr(bt_result, attr, None)
        if isinstance(val, pd.Series) and len(val) > 0:
            cash_series = val
            break

    if cash_series is None:
        for attr in ("positions_value", "invested_value"):
            val = getattr(bt_result, attr, None)
            if isinstance(val, pd.Series) and len(val) > 0:
                cash_series = (equity_curve - val).clip(lower=0.0)
                break

    if cash_series is None:
        # Try to reconstruct from positions DataFrame
        if hasattr(bt_result, "positions") and not bt_result.positions.empty:
            pos_df = bt_result.positions
            if "_cash" in pos_df.columns:
                cash_series = pos_df["_cash"]

    if cash_series is None:
        logger.debug(
            "No cash-balance data — using 20% fallback for cash-proxy"
        )
        cash_series = equity_curve * 0.20

    # Align indices
    common_idx = (
        equity_curve.index
        .intersection(proxy_ret.index)
        .intersection(cash_series.index)
    )
    if len(common_idx) < 2:
        return equity_curve

    cash   = cash_series.reindex(common_idx).ffill().fillna(0.0)
    p_ret  = proxy_ret.reindex(common_idx).fillna(0.0)

    adjusted = equity_curve.reindex(common_idx).copy()
    cum_cash_pnl = 0.0

    values = adjusted.values.copy()
    cash_vals = cash.values
    ret_vals  = p_ret.values

    for i in range(1, len(values)):
        cash_pnl = cash_vals[i - 1] * ret_vals[i]
        cum_cash_pnl += cash_pnl
        values[i] += cum_cash_pnl

    adjusted = pd.Series(values, index=common_idx, name=equity_curve.name)

    added_pct = (
        (adjusted.iloc[-1] / equity_curve.reindex(common_idx).iloc[-1]) - 1.0
    ) * 100
    logger.info(
        f"Cash proxy adjustment: +{added_pct:.2f}% total return "
        f"from idle cash in {proxy_ticker}"
    )

    return adjusted


# ═══════════════════════════════════════════════════════════════
#  CONFIG BUILDERS
# ═══════════════════════════════════════════════════════════════

def _build_backtest_config(
    strategy: StrategyConfig,
    capital: float,
) -> BacktestConfig:
    sizing_kw    = strategy.sizing_config_overrides or {}
    bt_overrides = strategy.backtest_config_overrides or {}

    sizing = SizingConfig(**{
        k: v for k, v in sizing_kw.items()
        if k in SizingConfig.__dataclass_fields__
    }) if sizing_kw else SizingConfig()

    rebalance_kw = {
        k: v for k, v in bt_overrides.items()
        if k in RebalanceConfig.__dataclass_fields__
    }
    rebalance = RebalanceConfig(**rebalance_kw) if rebalance_kw else RebalanceConfig()

    bc_fields = set(BacktestConfig.__dataclass_fields__)
    bc_kwargs: dict[str, Any] = {
        "initial_capital": capital,
        "sizing": sizing,
        "rebalance": rebalance,
    }

    for key in ("execution_delay", "rebalance_holds"):
        if key in bt_overrides and key in bc_fields:
            bc_kwargs[key] = bt_overrides[key]

    if "min_hold_days" in bc_fields:
        bc_kwargs["min_hold_days"] = strategy.min_hold_days
    if "cash_proxy" in bc_fields:
        bc_kwargs["cash_proxy"] = strategy.cash_proxy

    return BacktestConfig(**bc_kwargs)


# ═══════════════════════════════════════════════════════════════
#  RETURN COMPUTATIONS
# ═══════════════════════════════════════════════════════════════

def _compute_annual_returns(equity: pd.Series) -> pd.Series:
    if equity.empty:
        return pd.Series(dtype=float)
    yearly = equity.resample("YE").last()
    returns = yearly.pct_change().dropna()
    returns.index = returns.index.year
    returns.name = "annual_return"
    return returns


def _compute_monthly_returns(daily_returns: pd.Series) -> pd.DataFrame:
    if daily_returns.empty:
        return pd.DataFrame()
    monthly = (1 + daily_returns).resample("ME").prod() - 1
    monthly_df = pd.DataFrame({
        "year":   monthly.index.year,
        "month":  monthly.index.month,
        "return": monthly.values,
    })
    pivot = monthly_df.pivot_table(
        values="return", index="year", columns="month", aggfunc="first",
    )
    month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    pivot.columns = [month_names[c - 1] for c in pivot.columns]
    return pivot


##################

"""
backtest/metrics.py
-------------------
Comprehensive performance analytics for backtesting.

Standalone functions — no state.  Each takes an equity curve,
daily returns, or trade list and returns metrics.

Enhancements over the original:
  - Monthly returns heatmap data (year × month pivot)
  - Regime-conditional metrics (performance during strong/weak breadth)
  - Rolling Sharpe and rolling drawdown
  - Win/loss streak analysis
  - Expectancy (avg win × win_rate - avg loss × loss_rate)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio.rebalance import Trade


# ═══════════════════════════════════════════════════════════════
#  CAGR  (Compound Annual Growth Rate)
# ═══════════════════════════════════════════════════════════════

def compute_cagr(
    initial_value: float,
    final_value: float,
    n_years: float,
) -> float:
    if initial_value <= 0 or n_years <= 0:
        return 0.0
    if final_value <= 0:
        return -1.0
    return (final_value / initial_value) ** (1.0 / n_years) - 1.0


def cagr_from_equity(equity: pd.Series) -> float:
    if equity.empty or len(equity) < 2:
        return 0.0
    initial = equity.iloc[0]
    final = equity.iloc[-1]
    n_days = (equity.index[-1] - equity.index[0]).days
    n_years = max(n_days / 365.25, 0.01)
    return compute_cagr(initial, final, n_years)


def cagr_from_returns(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return 0.0
    equity = (1 + daily_returns).cumprod()
    return cagr_from_equity(equity)


# ═══════════════════════════════════════════════════════════════
#  ROLLING CAGR
# ═══════════════════════════════════════════════════════════════

def rolling_cagr(
    equity: pd.Series,
    window_years: int = 3,
) -> pd.Series:
    if equity.empty:
        return pd.Series(dtype=float)
    window_days = int(window_years * 252)
    if len(equity) < window_days:
        return pd.Series(dtype=float, index=equity.index)
    result = pd.Series(np.nan, index=equity.index)
    for i in range(window_days, len(equity)):
        initial = equity.iloc[i - window_days]
        final = equity.iloc[i]
        if initial > 0:
            result.iloc[i] = (final / initial) ** (1.0 / window_years) - 1
    result.name = f"rolling_{window_years}y_cagr"
    return result


# ═══════════════════════════════════════════════════════════════
#  ROLLING SHARPE
# ═══════════════════════════════════════════════════════════════

def rolling_sharpe(
    daily_returns: pd.Series,
    window: int = 252,
) -> pd.Series:
    if daily_returns.empty or len(daily_returns) < window:
        return pd.Series(dtype=float, index=daily_returns.index)
    roll_mean = daily_returns.rolling(window).mean()
    roll_std = daily_returns.rolling(window).std()
    result = (roll_mean / roll_std.replace(0, np.nan)) * np.sqrt(252)
    result.name = f"rolling_{window}d_sharpe"
    return result


# ═══════════════════════════════════════════════════════════════
#  MONTHLY RETURNS HEATMAP
# ═══════════════════════════════════════════════════════════════

def compute_monthly_returns_heatmap(
    daily_returns: pd.Series,
) -> pd.DataFrame:
    """
    Monthly returns as a year × month pivot table.

    Returns a DataFrame with years as rows, months (Jan–Dec) as
    columns, and monthly percentage returns as values.
    """
    if daily_returns.empty:
        return pd.DataFrame()
    monthly = (1 + daily_returns).resample("ME").prod() - 1
    monthly_df = pd.DataFrame({
        "year": monthly.index.year,
        "month": monthly.index.month,
        "return": monthly.values,
    })
    pivot = monthly_df.pivot_table(
        values="return", index="year", columns="month", aggfunc="first",
    )
    month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    pivot.columns = [month_names[c - 1] for c in pivot.columns]
    return pivot


# ═══════════════════════════════════════════════════════════════
#  REGIME-CONDITIONAL METRICS
# ═══════════════════════════════════════════════════════════════

def compute_regime_metrics(
    daily_returns: pd.Series,
    regime_series: pd.Series,
) -> dict[str, dict]:
    """
    Compute performance metrics conditioned on breadth regime.

    Parameters
    ----------
    daily_returns : pd.Series
        Daily return series (DatetimeIndex).
    regime_series : pd.Series
        Breadth regime labels (DatetimeIndex), e.g. 'strong', 'neutral', 'weak'.

    Returns
    -------
    dict[str, dict]
        {regime_label: {cagr, sharpe, vol, n_days, pct_time}}
    """
    if daily_returns.empty or regime_series.empty:
        return {}

    common = daily_returns.index.intersection(regime_series.index)
    if len(common) < 30:
        return {}

    rets = daily_returns.reindex(common)
    regimes = regime_series.reindex(common).ffill()

    result: dict[str, dict] = {}
    total_days = len(common)

    for regime in regimes.unique():
        if pd.isna(regime):
            continue
        mask = regimes == regime
        r = rets[mask]
        n = len(r)
        if n < 5:
            continue

        ann_ret = r.mean() * 252
        ann_vol = r.std() * np.sqrt(252)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

        result[str(regime)] = {
            "annualised_return": float(ann_ret),
            "annualised_vol": float(ann_vol),
            "sharpe": float(sharpe),
            "n_days": n,
            "pct_time": n / total_days,
            "mean_daily": float(r.mean()),
            "worst_day": float(r.min()),
            "best_day": float(r.max()),
        }

    return result


# ═══════════════════════════════════════════════════════════════
#  WIN/LOSS STREAK ANALYSIS
# ═══════════════════════════════════════════════════════════════

def compute_streak_stats(trade_pnls: list[float]) -> dict:
    """Analyse consecutive win/loss streaks."""
    if not trade_pnls:
        return {"max_win_streak": 0, "max_loss_streak": 0,
                "current_streak": 0, "current_streak_type": "none"}

    max_win = max_loss = cur = 0
    cur_type = "none"

    for pnl in trade_pnls:
        if pnl > 0:
            if cur_type == "win":
                cur += 1
            else:
                cur = 1
                cur_type = "win"
            max_win = max(max_win, cur)
        elif pnl < 0:
            if cur_type == "loss":
                cur += 1
            else:
                cur = 1
                cur_type = "loss"
            max_loss = max(max_loss, cur)
        else:
            cur = 0
            cur_type = "flat"

    return {
        "max_win_streak": max_win,
        "max_loss_streak": max_loss,
        "current_streak": cur,
        "current_streak_type": cur_type,
    }


# ═══════════════════════════════════════════════════════════════
#  COMPREHENSIVE METRICS
# ═══════════════════════════════════════════════════════════════

def compute_full_metrics(
    equity_curve: pd.Series,
    daily_returns: pd.Series,
    trades: list[Trade],
    initial_capital: float,
    benchmark_equity: pd.Series | None = None,
    breadth_regime: pd.Series | None = None,
) -> dict:
    """
    Compute every performance metric needed for strategy evaluation.

    Enhanced with: monthly returns, regime-conditional metrics,
    streak analysis, expectancy, and Ulcer index.
    """
    if equity_curve.empty:
        return {}

    final = equity_curve.iloc[-1]
    peak = equity_curve.max()
    n_days = len(equity_curve)
    calendar_days = (equity_curve.index[-1] - equity_curve.index[0]).days
    n_years = max(calendar_days / 365.25, 0.01)

    # ── Returns ───────────────────────────────────────────────
    total_return = (final / initial_capital) - 1
    cagr = compute_cagr(initial_capital, final, n_years)

    ann_vol = float(daily_returns.std() * np.sqrt(252)) if len(daily_returns) > 1 else 0.0
    mean_daily = daily_returns.mean() if len(daily_returns) > 0 else 0.0

    # ── Risk-adjusted ─────────────────────────────────────────
    sharpe = (
        (mean_daily / daily_returns.std() * np.sqrt(252))
        if daily_returns.std() > 0 else 0.0
    )

    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 0 else 0.001
    sortino = (
        mean_daily / down_std * np.sqrt(252)
        if down_std > 0 else 0.0
    )

    # ── Drawdown ──────────────────────────────────────────────
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    max_dd = drawdown.min()
    current_dd = drawdown.iloc[-1]

    is_dd = drawdown < 0
    dd_groups = (~is_dd).cumsum()
    dd_lengths = is_dd.groupby(dd_groups).sum()
    max_dd_duration = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0

    calmar = abs(cagr / max_dd) if max_dd < 0 else 0.0

    # ── Ulcer Index ───────────────────────────────────────────
    dd_squared = drawdown ** 2
    ulcer_index = float(np.sqrt(dd_squared.mean())) if len(dd_squared) > 0 else 0.0
    ulcer_perf = cagr / ulcer_index if ulcer_index > 0 else 0.0

    # ── VaR / CVaR ────────────────────────────────────────────
    var_95 = float(daily_returns.quantile(0.05)) if len(daily_returns) > 20 else 0.0
    tail = daily_returns[daily_returns <= var_95]
    cvar_95 = float(tail.mean()) if len(tail) > 0 else var_95

    # ── Higher moments ────────────────────────────────────────
    skewness = float(daily_returns.skew()) if len(daily_returns) > 5 else 0.0
    kurtosis = float(daily_returns.kurtosis()) if len(daily_returns) > 5 else 0.0

    # ── Annual returns ────────────────────────────────────────
    yearly = equity_curve.resample("YE").last()
    yearly_ret = yearly.pct_change().dropna()
    best_year = float(yearly_ret.max()) if len(yearly_ret) > 0 else 0.0
    worst_year = float(yearly_ret.min()) if len(yearly_ret) > 0 else 0.0
    pct_positive_years = (
        float((yearly_ret > 0).mean()) if len(yearly_ret) > 0 else 0.0
    )

    # ── Trades ────────────────────────────────────────────────
    trade_pnls = _compute_trade_pnls(trades)
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    n_trades = len(trade_pnls)
    win_rate = len(wins) / max(n_trades, 1)

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0

    # Expectancy
    loss_rate = 1.0 - win_rate
    expectancy = (avg_win * win_rate) + (avg_loss * loss_rate)

    total_commission = sum(t.commission + t.slippage for t in trades)

    # Streak analysis
    streak_stats = compute_streak_stats(trade_pnls)

    # Holding period (from trade timestamps)
    holding_days = _compute_avg_holding_days(trades)

    # ── Assemble base metrics ─────────────────────────────────
    metrics = {
        # Returns
        "total_return": total_return,
        "cagr": cagr,
        "best_year": best_year,
        "worst_year": worst_year,
        "pct_positive_years": pct_positive_years,
        # Risk
        "annual_volatility": ann_vol,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "max_drawdown": max_dd,
        "max_dd_duration": max_dd_duration,
        "current_drawdown": current_dd,
        "var_95": var_95,
        "cvar_95": cvar_95,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "ulcer_index": ulcer_index,
        "ulcer_performance": ulcer_perf,
        # Trading
        "total_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "total_commission": total_commission,
        "avg_holding_days": holding_days,
        "max_win_streak": streak_stats["max_win_streak"],
        "max_loss_streak": streak_stats["max_loss_streak"],
        # Capital
        "initial_capital": initial_capital,
        "final_capital": final,
        "peak_capital": peak,
        # Periods
        "n_days": n_days,
        "n_years": n_years,
        "start_date": equity_curve.index[0],
        "end_date": equity_curve.index[-1],
    }

    # ── Benchmark comparison ──────────────────────────────────
    if (
        benchmark_equity is not None
        and not benchmark_equity.empty
        and len(benchmark_equity) > 30
    ):
        bench_metrics = _compute_benchmark_metrics(
            equity_curve, daily_returns,
            benchmark_equity, initial_capital,
        )
        metrics.update(bench_metrics)

    # ── Regime-conditional metrics ────────────────────────────
    if breadth_regime is not None and not breadth_regime.empty:
        regime_m = compute_regime_metrics(daily_returns, breadth_regime)
        if regime_m:
            metrics["regime_metrics"] = regime_m

    return metrics


# ═══════════════════════════════════════════════════════════════
#  BENCHMARK COMPARISON
# ═══════════════════════════════════════════════════════════════

def _compute_benchmark_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    bench_equity: pd.Series,
    initial_capital: float,
) -> dict:
    common = equity.index.intersection(bench_equity.index)
    if len(common) < 30:
        return {}

    strat_ret = daily_returns.reindex(common).fillna(0)
    bench_ret = bench_equity.reindex(common).pct_change().fillna(0)

    n_days = (common[-1] - common[0]).days
    n_years = max(n_days / 365.25, 0.01)

    bench_final = bench_equity.reindex(common).iloc[-1]
    bench_initial = bench_equity.reindex(common).iloc[0]
    bench_cagr = compute_cagr(bench_initial, bench_final, n_years)

    bench_vol = float(bench_ret.std() * np.sqrt(252))
    bench_sharpe = (
        (bench_ret.mean() / bench_ret.std() * np.sqrt(252))
        if bench_ret.std() > 0 else 0.0
    )

    bench_max_dd = (
        (bench_equity.reindex(common) / bench_equity.reindex(common).cummax() - 1).min()
    )

    strat_cagr = cagr_from_equity(equity.reindex(common))
    excess_cagr = strat_cagr - bench_cagr

    active_ret = strat_ret - bench_ret
    tracking_error = float(active_ret.std() * np.sqrt(252))
    information_ratio = (
        float(active_ret.mean() / active_ret.std() * np.sqrt(252))
        if active_ret.std() > 0 else 0.0
    )

    up_days = bench_ret > 0
    dn_days = bench_ret < 0

    up_capture = (
        float(strat_ret[up_days].mean() / bench_ret[up_days].mean())
        if up_days.any() and bench_ret[up_days].mean() != 0 else 1.0
    )
    down_capture = (
        float(strat_ret[dn_days].mean() / bench_ret[dn_days].mean())
        if dn_days.any() and bench_ret[dn_days].mean() != 0 else 1.0
    )

    # Beta and alpha
    if bench_ret.var() > 0:
        beta = float(strat_ret.cov(bench_ret) / bench_ret.var())
        alpha = float((strat_ret.mean() - beta * bench_ret.mean()) * 252)
    else:
        beta = 1.0
        alpha = 0.0

    return {
        "bench_cagr": bench_cagr,
        "bench_sharpe": float(bench_sharpe),
        "bench_max_dd": bench_max_dd,
        "bench_volatility": bench_vol,
        "excess_cagr": excess_cagr,
        "information_ratio": information_ratio,
        "tracking_error": tracking_error,
        "up_capture": up_capture,
        "down_capture": down_capture,
        "beta": beta,
        "alpha": alpha,
    }


# ═══════════════════════════════════════════════════════════════
#  TRADE PnL
# ═══════════════════════════════════════════════════════════════

def _compute_trade_pnls(trades: list[Trade]) -> list[float]:
    open_trades: dict[str, list[Trade]] = {}
    pnls: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                entry_cost = entry.price * (1 + 0.0015)
                exit_net = trade.price * (1 - 0.0015)
                pnl = (exit_net / entry_cost) - 1 if entry_cost > 0 else 0
                pnls.append(pnl)

    return pnls


def _compute_avg_holding_days(trades: list[Trade]) -> float:
    """Compute average holding period from BUY→SELL pairs."""
    open_trades: dict[str, list[Trade]] = {}
    holding_days: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                try:
                    days = (trade.date - entry.date).days
                    holding_days.append(max(days, 1))
                except Exception:
                    pass

    return float(np.mean(holding_days)) if holding_days else 0.0


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def metrics_report(run: "BacktestRun") -> str:
    m = run.metrics
    if not m:
        return f"No metrics for '{runrefactor.strategy.name}'"

    ln: list[str] = []
    div = "=" * 70
    sub = "-" * 70

    ln.append(div)
    ln.append(f"  BACKTEST REPORT: {runrefactor.strategy.name}")
    ln.append(f"  {runrefactor.strategy.description}")
    if runrefactor.strategy.market != "US":
        ln.append(f"  Market: {runrefactor.strategy.market}")
    ln.append(div)

    ln.append(f"  Period:          {m.get('start_date', '?')} → "
              f"{m.get('end_date', '?')}  ({m.get('n_years', 0):.1f} years)")
    ln.append(f"  Initial capital: ${m.get('initial_capital', 0):,.0f}")
    ln.append(f"  Final capital:   ${m.get('final_capital', 0):,.0f}")
    ln.append(f"  Peak capital:    ${m.get('peak_capital', 0):,.0f}")

    ln.append("")
    ln.append(sub)
    ln.append("  RETURNS")
    ln.append(sub)
    ln.append(f"  Total return:        {m.get('total_return', 0):>+8.2%}")
    ln.append(f"  CAGR:                {m.get('cagr', 0):>+8.2%}")
    ln.append(f"  Best year:           {m.get('best_year', 0):>+8.2%}")
    ln.append(f"  Worst year:          {m.get('worst_year', 0):>+8.2%}")
    ln.append(f"  % positive years:    {m.get('pct_positive_years', 0):>8.0%}")

    ln.append("")
    ln.append(sub)
    ln.append("  RISK")
    ln.append(sub)
    ln.append(f"  Ann. volatility:     {m.get('annual_volatility', 0):>8.2%}")
    ln.append(f"  Sharpe ratio:        {m.get('sharpe_ratio', 0):>8.3f}")
    ln.append(f"  Sortino ratio:       {m.get('sortino_ratio', 0):>8.3f}")
    ln.append(f"  Calmar ratio:        {m.get('calmar_ratio', 0):>8.3f}")
    ln.append(f"  Max drawdown:        {m.get('max_drawdown', 0):>8.2%}")
    ln.append(f"  Max DD duration:     {m.get('max_dd_duration', 0):>5d} days")
    ln.append(f"  VaR (95%):           {m.get('var_95', 0):>8.4f}")
    ln.append(f"  CVaR (95%):          {m.get('cvar_95', 0):>8.4f}")
    ln.append(f"  Ulcer Index:         {m.get('ulcer_index', 0):>8.4f}")

    ln.append("")
    ln.append(sub)
    ln.append("  TRADING")
    ln.append(sub)
    ln.append(f"  Total trades:        {m.get('total_trades', 0):>5d}")
    ln.append(f"  Win rate:            {m.get('win_rate', 0):>8.1%}")
    ln.append(f"  Profit factor:       {m.get('profit_factor', 0):>8.2f}")
    ln.append(f"  Avg win:             {m.get('avg_win', 0):>+8.2%}")
    ln.append(f"  Avg loss:            {m.get('avg_loss', 0):>+8.2%}")
    ln.append(f"  Expectancy:          {m.get('expectancy', 0):>+8.4f}")
    ln.append(f"  Avg holding days:    {m.get('avg_holding_days', 0):>8.1f}")
    ln.append(f"  Max win streak:      {m.get('max_win_streak', 0):>5d}")
    ln.append(f"  Max loss streak:     {m.get('max_loss_streak', 0):>5d}")
    ln.append(f"  Total costs:         ${m.get('total_commission', 0):>10,.2f}")

    if "bench_cagr" in m:
        ln.append("")
        ln.append(sub)
        ln.append("  vs BENCHMARK")
        ln.append(sub)
        ln.append(f"  Benchmark CAGR:      {m.get('bench_cagr', 0):>+8.2%}")
        ln.append(f"  Excess CAGR:         {m.get('excess_cagr', 0):>+8.2%}")
        ln.append(f"  Alpha (ann):         {m.get('alpha', 0):>+8.2%}")
        ln.append(f"  Beta:                {m.get('beta', 0):>8.3f}")
        ln.append(f"  Information ratio:   {m.get('information_ratio', 0):>8.3f}")
        ln.append(f"  Tracking error:      {m.get('tracking_error', 0):>8.2%}")
        ln.append(f"  Up capture:          {m.get('up_capture', 0):>8.2f}")
        ln.append(f"  Down capture:        {m.get('down_capture', 0):>8.2f}")
        ln.append(f"  Bench max DD:        {m.get('bench_max_dd', 0):>8.2%}")

    # Regime-conditional metrics
    regime_m = m.get("regime_metrics", {})
    if regime_m:
        ln.append("")
        ln.append(sub)
        ln.append("  REGIME-CONDITIONAL PERFORMANCE")
        ln.append(sub)
        for regime, rm in sorted(regime_m.items()):
            ln.append(
                f"  {regime:<10s}  "
                f"ret={rm['annualised_return']:>+7.2%}  "
                f"vol={rm['annualised_vol']:>6.2%}  "
                f"sharpe={rm['sharpe']:>6.2f}  "
                f"({rm['pct_time']:.0%} of time)"
            )

    # Annual returns
    if not run.annual_returns.empty:
        ln.append("")
        ln.append(sub)
        ln.append("  ANNUAL RETURNS")
        ln.append(sub)
        for year, ret in run.annual_returns.items():
            bar = "█" * max(0, int(ret * 100))
            ln.append(f"  {year}:  {ret:>+7.2%}  {bar}")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)


#########################

"""
backtest/runner.py
------------------
CLI entry point for backtesting.

Enhanced:
  - --universe core|full flag to select ticker scope
  - --show-universe to inspect what's in each scope
  - Universe summary in header output
  - Multi-market support

Usage:
    python -m backtest.runner                                  # core US
    python -m backtest.runner --universe full                  # full universe.py
    python -m backtest.runner --universe full --start 2022     # full from 2022
    python -m backtest.runner --show-universe                  # list core tickers
    python -m backtest.runner --show-universe --universe full  # list all tickers
    python -m backtest.runner --compare --universe full        # compare on full
    python -m backtest.runner --market HK --start 2018         # HK from 2018
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from backtest.phase1.data_loader import (
    ensure_history,
    data_summary,
    get_universe_tickers,
    list_universe_tickers,
    build_full_universe,
    BACKTEST_CORE_UNIVERSE,
)
from backtest.phase1.engine import run_backtest_period, StrategyConfig
from backtest.phase1.strategies import (
    ALL_STRATEGIES,
    get_strategy,
    list_strategies,
)
from backtest.phase1.comparison import compare_strategies
from backtest.phase1.metrics import metrics_report

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="backtest",
        description="CASH — Historical Backtesting Harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backtest.runner                                  # core ETFs, baseline
  python -m backtest.runner --universe full                  # full universe.py
  python -m backtest.runner --universe full --start 2022     # full from 2022
  python -m backtest.runner --show-universe                  # inspect core universe
  python -m backtest.runner --show-universe --universe full  # inspect full universe
  python -m backtest.runner --compare                        # compare US strategies
  python -m backtest.runner --compare --universe full        # compare on full universe
  python -m backtest.runner --strategy regime_adaptive       # single variant
  python -m backtest.runner --market HK --start 2018         # HK from 2018
  python -m backtest.runner --list                           # list strategies
        """,
    )

    p.add_argument(
        "--start", type=str, default=None,
        help="Backtest start date (YYYY or YYYY-MM-DD)",
    )
    p.add_argument(
        "--end", type=str, default=None,
        help="Backtest end date",
    )
    p.add_argument(
        "--strategy", "-s", type=str, default="baseline",
        help="Strategy name (default: baseline)",
    )
    p.add_argument(
        "--market", "-m", type=str, default="US",
        choices=["US", "HK", "IN"],
        help="Market to backtest (default: US)",
    )
    p.add_argument(
        "--universe", "-u", type=str, default="core",
        choices=["core", "full"],
        help="Universe scope: core=hardcoded ETFs, full=all from universe.py",
    )
    p.add_argument(
        "--show-universe", action="store_true",
        help="Print the ticker list for the selected universe and exit",
    )
    p.add_argument(
        "--compare", action="store_true",
        help="Run all strategies and compare side-by-side",
    )
    p.add_argument(
        "--rank-by", type=str, default="cagr",
        choices=["cagr", "sharpe", "sortino", "calmar", "max_drawdown"],
        help="Metric to rank strategies by (default: cagr)",
    )
    p.add_argument(
        "--capital", type=float, default=100_000,
        help="Initial capital (default: 100,000)",
    )
    p.add_argument(
        "--output", "-o", type=str, default=None,
        help="Directory to save backtest reports",
    )
    p.add_argument(
        "--refresh", action="store_true",
        help="Force re-download of historical data",
    )
    p.add_argument(
        "--list", action="store_true", dest="list_strats",
        help="List available strategies and exit",
    )
    p.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override universe with specific tickers",
    )
    p.add_argument(
        "--holdings", type=str, default="",
        help="Comma-separated current holdings for rotation",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Debug logging",
    )

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── Logging ───────────────────────────────────────────────
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    market = args.market.upper()
    scope = args.universe

    # ── Show universe ─────────────────────────────────────────
    if args.show_universe:
        list_universe_tickers(market=market, scope=scope)
        return

    # ── List strategies ───────────────────────────────────────
    if args.list_strats:
        strats = list_strategies(market)
        print(f"\nAvailable strategies for {market}:")
        print("-" * 60)
        for name in strats:
            strat = ALL_STRATEGIES[name]
            print(f"  {name:<24s} {strat.description}")
        print()
        return

    # ── Resolve tickers ───────────────────────────────────────
    custom_tickers = (
        [t.upper() for t in args.tickers] if args.tickers else None
    )

    # Show the universe scope being used
    if custom_tickers:
        universe_desc = f"custom ({len(custom_tickers)} tickers)"
    elif scope == "full":
        full_list = build_full_universe(market)
        universe_desc = f"full ({len(full_list)} tickers from universe.py)"
    else:
        core_list = get_universe_tickers(market=market, scope="core")
        universe_desc = f"core ({len(core_list)} ETFs)"

    # ── Load data ─────────────────────────────────────────────
    t0 = time.time()

    print("\n" + "=" * 70)
    print(f"  CASH — BACKTESTING HARNESS [{market}]")
    print("=" * 70)

    data = ensure_history(
        tickers=custom_tickers,
        market=market,
        scope=scope,
        force_refresh=args.refresh,
    )

    if not data:
        print("ERROR: No data loaded.")
        sys.exit(1)

    summary = data_summary(data)
    print(f"\n  Universe:    {universe_desc}")
    print(f"  Data loaded: {summary['n_tickers']} tickers")
    print(f"  Range:       {summary['earliest_start'].date()} → "
          f"{summary['latest_end'].date()}")
    print(f"  Total bars:  {summary['total_bars']:,}")

    start = _normalise_date(args.start)
    end = _normalise_date(args.end)

    holdings = (
        [t.strip().upper() for t in args.holdings.split(",") if t.strip()]
        if args.holdings else None
    )

    # ── Compare mode ──────────────────────────────────────────
    if args.compare:
        market_strats = [
            v for v in ALL_STRATEGIES.values()
            if v.market == market
        ]
        print(f"\n  Running comparison of {len(market_strats)} "
              f"{market} strategies on {universe_desc}...")

        result = compare_strategies(
            data, strategies=market_strats, market=market,
            start=start, end=end, capital=args.capital,
            rank_by=args.rank_by, current_holdings=holdings,
        )

        print(result["report"])

        if result["best"]:
            print("\n" + metrics_report(result["best"]))

        if args.output:
            _save_comparison(result, args.output)

        elapsed = time.time() - t0
        print(f"\n  Total time: {elapsed:.0f}s")
        return

    # ── Single strategy mode ──────────────────────────────────
    try:
        strategy = get_strategy(argsrefactor.strategy)
    except KeyError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if strategy.market != market:
        print(
            f"WARNING: Strategy '{strategy.name}' is for "
            f"{strategy.market}, but --market is {market}. "
            f"Using strategy's market."
        )
        market = strategy.market

    print(f"\n  Strategy:    {strategy.name}")
    print(f"  Description: {strategy.description}")
    print(f"  Market:      {strategy.market}")
    print(f"  Period:      {start or 'earliest'} → {end or 'latest'}")
    print(f"  Capital:     ${args.capital:,.0f}")
    print()

    run = run_backtest_period(
        data, start=start, end=end,
        strategy=strategy, capital=args.capital,
        current_holdings=holdings,
    )

    if run.ok:
        print(metrics_report(run))
    else:
        print(f"\n  ERROR: {run.error}")

    if args.output and run.ok:
        _save_single(run, args.output)

    elapsed = time.time() - t0
    print(f"\n  Total time: {elapsed:.0f}s")


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _normalise_date(date_str: str | None) -> str | None:
    if date_str is None:
        return None
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return date_str


def _save_comparison(result: dict, output_dir: str) -> None:
    import os
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, "comparison_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(result["report"])

    if isinstance(result.get("table"), pd.DataFrame):
        csv_path = os.path.join(output_dir, "comparison_summary.csv")
        result["table"].to_csv(csv_path, index=True, encoding="utf-8")

    eq_curves = result.get("equity_curves")
    if isinstance(eq_curves, pd.DataFrame) and not eq_curves.empty:
        eq_path = os.path.join(output_dir, "equity_curves.csv")
        eq_curves.to_csv(eq_path, encoding="utf-8")

    print(f"\n  Results saved to: {output_dir}/")


def _save_single(run, output_dir: str) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_path = out / f"backtest_{runrefactor.strategy.name}_{ts}.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(metrics_report(run))
    print(f"  Report saved → {report_path}")

    if run.backtest_result and not run.backtest_result.equity_curve.empty:
        eq_path = out / f"equity_{runrefactor.strategy.name}_{ts}.csv"
        eq_df = pd.DataFrame({"equity": run.backtest_result.equity_curve})
        if not run.benchmark_equity.empty:
            eq_df["benchmark"] = run.benchmark_equity
        eq_df.to_csv(eq_path)
        print(f"  Equity saved → {eq_path}")

    if not run.monthly_returns.empty:
        monthly_path = out / f"monthly_{runrefactor.strategy.name}_{ts}.csv"
        run.monthly_returns.to_csv(monthly_path)
        print(f"  Monthly returns saved → {monthly_path}")


if __name__ == "__main__":
    main()


##############

"""
backtest/strategies.py
----------------------
Predefined strategy parameter variants for comparison.

Enhanced with:
  - Strategies designed to beat SPY (regime_adaptive, trend_following)
  - Low-churn strategies that reduce the $17K friction drag
  - Full-universe strategies for testing single-name picks
  - Market-specific strategies (HK, India)
  - Convergence-aware strategies

Diagnosis from baseline (2022-2026):
  - 497 trades cost $17,368 (17% of capital) → need <150 trades
  - neutral regime (57% of time) loses 3.94% → need neutral to be flat or positive
  - up_capture 0.65 < down_capture 0.68 → need asymmetry in our favour
  - incumbent_bonus too low → too much unnecessary rotation
"""

from __future__ import annotations

from backtest.phase1.engine import StrategyConfig
from common.universe import SECTORS, BROAD_MARKET, FIXED_INCOME, COMMODITIES


# ═══════════════════════════════════════════════════════════════
#  US STRATEGIES — ORIGINAL
# ═══════════════════════════════════════════════════════════════

BASELINE = StrategyConfig(
    name="baseline",
    description="Tuned CASH defaults — wide hysteresis, low churn",
)

ORIGINAL_DEFAULTS = StrategyConfig(
    name="original_defaults",
    description="Original parameters (high churn — for comparison only)",
    signal_params={
        "entry_score_min": 0.60, "exit_score_max": 0.40,
        "confirmation_streak": 3, "cooldown_days": 5,
        "max_position_pct": 0.08, "min_position_pct": 0.02,
        "base_position_pct": 0.05, "max_positions": 15,
    },
    portfolio_params={
        "max_positions": 15, "max_single_pct": 0.08,
        "min_single_pct": 0.02, "target_invested_pct": 0.95,
        "rebalance_threshold": 0.015, "incumbent_bonus": 0.02,
    },
)

MOMENTUM_HEAVY = StrategyConfig(
    name="momentum_heavy",
    description="Overweight momentum pillar (40%), wider entry",
    scoring_weights={
        "pillar_rotation": 0.20, "pillar_momentum": 0.40,
        "pillar_volatility": 0.10, "pillar_microstructure": 0.20,
        "pillar_breadth": 0.10,
    },
    signal_params={
        "entry_score_min": 0.50, "exit_score_max": 0.30,
        "confirmation_streak": 2, "cooldown_days": 20,
    },
)

CONSERVATIVE = StrategyConfig(
    name="conservative",
    description="High conviction only, strong hold bias",
    signal_params={
        "entry_score_min": 0.65, "exit_score_max": 0.40,
        "confirmation_streak": 4, "cooldown_days": 25,
        "max_position_pct": 0.12, "max_positions": 6,
    },
    portfolio_params={
        "max_positions": 6, "max_single_pct": 0.12,
        "target_invested_pct": 0.75, "incumbent_bonus": 0.08,
    },
    breadth_portfolio={
        "strong_exposure": 0.85, "neutral_exposure": 0.60,
        "weak_exposure": 0.25, "weak_block_new": True,
        "weak_raise_entry": 0.10, "neutral_raise_entry": 0.05,
    },
)

BROAD_DIVERSIFIED = StrategyConfig(
    name="broad_diversified",
    description="12 positions, equal weight, wide net, low turnover",
    signal_params={
        "entry_score_min": 0.48, "exit_score_max": 0.30,
        "cooldown_days": 20, "max_positions": 12,
        "max_position_pct": 0.12,
    },
    portfolio_params={
        "max_positions": 12, "max_single_pct": 0.12,
        "min_single_pct": 0.03, "max_sector_pct": 0.40,
        "target_invested_pct": 0.92, "incumbent_bonus": 0.06,
    },
    sizing_config_overrides={
        "method": "equal_weight", "max_position_pct": 0.12,
    },
)

CONCENTRATED = StrategyConfig(
    name="concentrated",
    description="Top 3 high-conviction, strong hold bias",
    signal_params={
        "entry_score_min": 0.62, "exit_score_max": 0.38,
        "confirmation_streak": 3, "cooldown_days": 30,
        "max_positions": 3, "max_position_pct": 0.30,
    },
    portfolio_params={
        "max_positions": 3, "max_single_pct": 0.30,
        "min_single_pct": 0.10, "max_sector_pct": 0.50,
        "target_invested_pct": 0.85, "incumbent_bonus": 0.10,
    },
)

RISK_PARITY = StrategyConfig(
    name="risk_parity",
    description="Inverse-volatility sizing, moderate turnover",
    signal_params={"cooldown_days": 20},
    portfolio_params={
        "max_positions": 8, "max_single_pct": 0.18,
        "target_invested_pct": 0.88, "incumbent_bonus": 0.06,
    },
    sizing_config_overrides={
        "method": "risk_parity", "max_position_pct": 0.18,
    },
)

ROTATION_PURE = StrategyConfig(
    name="rotation_pure",
    description="Rotation pillar dominant (45%), RS-driven",
    scoring_weights={
        "pillar_rotation": 0.45, "pillar_momentum": 0.20,
        "pillar_volatility": 0.10, "pillar_microstructure": 0.15,
        "pillar_breadth": 0.10,
    },
    signal_params={"cooldown_days": 20},
    portfolio_params={"incumbent_bonus": 0.06},
)

_SECTOR_UNIVERSE = list(SECTORS) + ["SPY"]
SECTOR_ROTATION = StrategyConfig(
    name="sector_rotation",
    description="Pure sector ETF rotation (11 sectors)",
    universe_filter=_SECTOR_UNIVERSE,
    signal_params={
        "entry_score_min": 0.52, "exit_score_max": 0.32,
        "cooldown_days": 20, "max_positions": 4,
    },
    portfolio_params={
        "max_positions": 4, "max_single_pct": 0.25,
        "max_sector_pct": 0.30, "target_invested_pct": 0.92,
        "incumbent_bonus": 0.08,
    },
)

_ALL_WEATHER_UNI = (
    list(BROAD_MARKET) + list(SECTORS)
    + list(FIXED_INCOME) + list(COMMODITIES) + ["SPY"]
)
ALL_WEATHER = StrategyConfig(
    name="all_weather",
    description="Cross-asset rotation: equities + bonds + commodities",
    universe_filter=list(set(_ALL_WEATHER_UNI)),
    signal_params={
        "entry_score_min": 0.50, "exit_score_max": 0.32,
        "cooldown_days": 20, "max_positions": 8,
    },
    portfolio_params={
        "max_positions": 8, "max_single_pct": 0.18,
        "max_sector_pct": 0.40, "target_invested_pct": 0.90,
        "incumbent_bonus": 0.06,
    },
)

MONTHLY_REBALANCE = StrategyConfig(
    name="monthly_rebalance",
    description="Rebalance only on large drift (simulates monthly)",
    signal_params={
        "entry_score_min": 0.52, "exit_score_max": 0.30,
        "cooldown_days": 25,
    },
    portfolio_params={
        "max_positions": 8, "max_single_pct": 0.15,
        "target_invested_pct": 0.90, "rebalance_threshold": 0.08,
        "incumbent_bonus": 0.08,
    },
    backtest_config_overrides={
        "rebalance_holds": False, "drift_threshold": 0.20,
        "min_trade_pct": 0.05,
    },
)

# ── Convergence-aware (US only) ───────────────────────────────

CONVERGENCE_STRONG = StrategyConfig(
    name="convergence_strong",
    description="Only trade STRONG_BUY convergence signals",
    enable_rotation=True,
    enable_convergence=True,
    signal_params={
        "entry_score_min": 0.60, "exit_score_max": 0.35,
        "confirmation_streak": 2, "cooldown_days": 20,
        "max_positions": 6,
    },
    portfolio_params={
        "max_positions": 6, "max_single_pct": 0.18,
        "target_invested_pct": 0.85, "incumbent_bonus": 0.08,
    },
)

ROTATION_NO_QUALITY = StrategyConfig(
    name="rotation_no_quality",
    description="Rotation engine without quality filter (RS-only)",
    enable_rotation=True,
    enable_convergence=True,
)


# ═══════════════════════════════════════════════════════════════
#  US STRATEGIES — NEW: DESIGNED TO BEAT SPY
# ═══════════════════════════════════════════════════════════════
#
#  Targeting the specific weaknesses found in the baseline:
#   1. Too many trades (497) → massive friction ($17K)
#   2. Loses money in neutral regime (57% of time)
#   3. Down capture > up capture (catches more pain than gain)
#   4. Incumbent bonus too low → churns out of winning positions
#

REGIME_ADAPTIVE = StrategyConfig(
    name="regime_adaptive",
    description="Aggressive regime response: 95% strong, 60% neutral, 15% weak",
    scoring_weights={
        "pillar_rotation": 0.20, "pillar_momentum": 0.35,
        "pillar_volatility": 0.15, "pillar_microstructure": 0.15,
        "pillar_breadth": 0.15,
    },
    signal_params={
        "entry_score_min": 0.55,
        "exit_score_max": 0.25,       # Very low exit → ride winners
        "confirmation_streak": 3,
        "cooldown_days": 30,           # Long cooldown → fewer trades
        "max_positions": 5,
    },
    portfolio_params={
        "max_positions": 5,
        "max_single_pct": 0.22,
        "target_invested_pct": 0.92,
        "incumbent_bonus": 0.12,       # Strong hold bias
        "rebalance_threshold": 0.05,   # Ignore small drifts
    },
    breadth_portfolio={
        "strong_exposure": 0.95,
        "neutral_exposure": 0.60,      # Key: reduce neutral exposure
        "weak_exposure": 0.15,
        "weak_block_new": True,
        "weak_raise_entry": 0.15,
        "neutral_raise_entry": 0.08,   # Raise bar during neutral
    },
    breadth_defensive=True,
    min_hold_days=30,
)


TREND_FOLLOWING = StrategyConfig(
    name="trend_following",
    description="Ride trends 45d+, heavy momentum, few positions",
    scoring_weights={
        "pillar_rotation": 0.10,
        "pillar_momentum": 0.50,       # Dominant momentum weight
        "pillar_volatility": 0.15,
        "pillar_microstructure": 0.15,
        "pillar_breadth": 0.10,
    },
    signal_params={
        "entry_score_min": 0.58,
        "exit_score_max": 0.22,        # Extremely low exit
        "confirmation_streak": 2,
        "cooldown_days": 35,
        "max_positions": 4,
    },
    portfolio_params={
        "max_positions": 4,
        "max_single_pct": 0.25,
        "target_invested_pct": 0.90,
        "incumbent_bonus": 0.15,       # Very strong hold bias
        "rebalance_threshold": 0.06,
    },
    breadth_portfolio={
        "strong_exposure": 0.95,
        "neutral_exposure": 0.65,
        "weak_exposure": 0.10,
        "weak_block_new": True,
    },
    min_hold_days=45,                  # Long minimum hold
)


LOW_CHURN = StrategyConfig(
    name="low_churn",
    description="<100 trades target, massive hold bias, quarterly rotation",
    scoring_weights={
        "pillar_rotation": 0.25, "pillar_momentum": 0.30,
        "pillar_volatility": 0.15, "pillar_microstructure": 0.15,
        "pillar_breadth": 0.15,
    },
    signal_params={
        "entry_score_min": 0.58,
        "exit_score_max": 0.20,        # Almost never exit on score alone
        "confirmation_streak": 4,      # Need 4 days confirmation
        "cooldown_days": 40,           # 40 day cooldown
        "max_positions": 5,
    },
    portfolio_params={
        "max_positions": 5,
        "max_single_pct": 0.22,
        "min_single_pct": 0.08,
        "target_invested_pct": 0.88,
        "incumbent_bonus": 0.18,       # Enormous hold bias
        "rebalance_threshold": 0.10,   # Only rebalance on big drifts
    },
    breadth_portfolio={
        "strong_exposure": 0.92,
        "neutral_exposure": 0.70,
        "weak_exposure": 0.20,
        "weak_block_new": True,
    },
    min_hold_days=60,                  # Hold at least 60 days
)


QUALITY_MOMENTUM = StrategyConfig(
    name="quality_momentum",
    description="Momentum + microstructure focus, avoid volatile junk",
    scoring_weights={
        "pillar_rotation": 0.15,
        "pillar_momentum": 0.35,
        "pillar_volatility": 0.20,     # Higher vol weight = avoid volatile
        "pillar_microstructure": 0.20,
        "pillar_breadth": 0.10,
    },
    scoring_params={
        "vol_regime_w": 0.40,          # Penalise high-vol tickers
        "vol_trend_w": 0.30,
        "vol_relative_w": 0.30,
    },
    signal_params={
        "entry_score_min": 0.60,
        "exit_score_max": 0.30,
        "confirmation_streak": 3,
        "cooldown_days": 25,
        "max_positions": 6,
    },
    portfolio_params={
        "max_positions": 6,
        "max_single_pct": 0.18,
        "target_invested_pct": 0.88,
        "incumbent_bonus": 0.10,
    },
    breadth_portfolio={
        "strong_exposure": 0.92,
        "neutral_exposure": 0.65,
        "weak_exposure": 0.15,
        "weak_block_new": True,
        "neutral_raise_entry": 0.05,
    },
    min_hold_days=30,
)


ASYMMETRIC_CAPTURE = StrategyConfig(
    name="asymmetric_capture",
    description="Maximise up_capture / down_capture ratio",
    scoring_weights={
        "pillar_rotation": 0.20, "pillar_momentum": 0.30,
        "pillar_volatility": 0.20, "pillar_microstructure": 0.15,
        "pillar_breadth": 0.15,
    },
    signal_params={
        "entry_score_min": 0.55,
        "exit_score_max": 0.32,
        "confirmation_streak": 2,
        "cooldown_days": 25,
        "max_positions": 5,
    },
    portfolio_params={
        "max_positions": 5,
        "max_single_pct": 0.20,
        "target_invested_pct": 0.85,
        "incumbent_bonus": 0.10,
        "rebalance_threshold": 0.04,
    },
    breadth_portfolio={
        # Key: aggressively cut exposure in anything non-strong
        "strong_exposure": 0.95,
        "neutral_exposure": 0.55,      # Low neutral = less down capture
        "weak_exposure": 0.10,
        "critical_exposure": 0.05,
        "weak_block_new": True,
        "neutral_raise_entry": 0.10,   # High bar during neutral
    },
    breadth_defensive=True,
    min_hold_days=25,
)


BUY_AND_HOLD_TOP = StrategyConfig(
    name="buy_and_hold_top",
    description="Pick top 5 and hold until score collapses — minimal trading",
    signal_params={
        "entry_score_min": 0.62,
        "exit_score_max": 0.18,        # Only exit on severe score drop
        "confirmation_streak": 5,      # Need strong confirmation
        "cooldown_days": 50,
        "max_positions": 5,
    },
    portfolio_params={
        "max_positions": 5,
        "max_single_pct": 0.22,
        "target_invested_pct": 0.90,
        "incumbent_bonus": 0.20,       # Massive hold bias
        "rebalance_threshold": 0.12,
    },
    min_hold_days=90,                  # Hold at least a quarter
)


# ═══════════════════════════════════════════════════════════════
#  HK STRATEGIES
# ═══════════════════════════════════════════════════════════════

HK_BASELINE = StrategyConfig(
    name="hk_baseline",
    description="HK scoring-only baseline (vs 2800.HK)",
    market="HK",
    enable_rotation=False,
    enable_convergence=False,
    scoring_weights={
        "pillar_rotation": 0.25, "pillar_momentum": 0.30,
        "pillar_volatility": 0.15, "pillar_microstructure": 0.30,
        "pillar_breadth": 0.00,
    },
    signal_params={
        "allowed_rs_regimes": [
            "leading", "improving", "neutral",
        ],
        "allowed_sector_regimes": [
            "leading", "improving", "neutral",
            "weakening", "lagging",
        ],
        "max_positions": 8, "max_position_pct": 0.20,
    },
    portfolio_params={
        "max_positions": 8, "max_single_pct": 0.20,
        "target_invested_pct": 0.85,
    },
    cash_proxy=None,
)

HK_CONCENTRATED = StrategyConfig(
    name="hk_concentrated",
    description="HK top 4 picks, concentrated",
    market="HK",
    enable_rotation=False,
    enable_convergence=False,
    signal_params={
        "entry_score_min": 0.60, "max_positions": 4,
        "max_position_pct": 0.25,
    },
    portfolio_params={
        "max_positions": 4, "max_single_pct": 0.25,
        "target_invested_pct": 0.80, "incumbent_bonus": 0.08,
    },
    cash_proxy=None,
)


# ═══════════════════════════════════════════════════════════════
#  INDIA STRATEGIES
# ═══════════════════════════════════════════════════════════════

IN_BASELINE = StrategyConfig(
    name="in_baseline",
    description="India scoring-only baseline (vs NIFTYBEES.NS)",
    market="IN",
    enable_rotation=False,
    enable_convergence=False,
    scoring_weights={
        "pillar_rotation": 0.25, "pillar_momentum": 0.30,
        "pillar_volatility": 0.15, "pillar_microstructure": 0.30,
        "pillar_breadth": 0.00,
    },
    signal_params={
        "allowed_rs_regimes": [
            "leading", "improving", "neutral",
        ],
        "max_positions": 8, "max_position_pct": 0.20,
    },
    portfolio_params={
        "max_positions": 8, "max_single_pct": 0.20,
        "target_invested_pct": 0.85,
    },
    cash_proxy=None,
)


# ═══════════════════════════════════════════════════════════════
#  REGISTRY
# ═══════════════════════════════════════════════════════════════

ALL_STRATEGIES: dict[str, StrategyConfig] = {
    # US — original
    "baseline":              BASELINE,
    "original_defaults":     ORIGINAL_DEFAULTS,
    "momentum_heavy":        MOMENTUM_HEAVY,
    "conservative":          CONSERVATIVE,
    "broad_diversified":     BROAD_DIVERSIFIED,
    "concentrated":          CONCENTRATED,
    "risk_parity":           RISK_PARITY,
    "rotation_pure":         ROTATION_PURE,
    "sector_rotation":       SECTOR_ROTATION,
    "all_weather":           ALL_WEATHER,
    "monthly_rebalance":     MONTHLY_REBALANCE,
    "convergence_strong":    CONVERGENCE_STRONG,
    "rotation_no_quality":   ROTATION_NO_QUALITY,
    # US — new: designed to beat SPY
    "regime_adaptive":       REGIME_ADAPTIVE,
    "trend_following":       TREND_FOLLOWING,
    "low_churn":             LOW_CHURN,
    "quality_momentum":      QUALITY_MOMENTUM,
    "asymmetric_capture":    ASYMMETRIC_CAPTURE,
    "buy_and_hold_top":      BUY_AND_HOLD_TOP,
    # HK
    "hk_baseline":           HK_BASELINE,
    "hk_concentrated":       HK_CONCENTRATED,
    # India
    "in_baseline":           IN_BASELINE,
}

US_STRATEGIES = {k: v for k, v in ALL_STRATEGIES.items() if v.market == "US"}
HK_STRATEGIES = {k: v for k, v in ALL_STRATEGIES.items() if v.market == "HK"}
IN_STRATEGIES = {k: v for k, v in ALL_STRATEGIES.items() if v.market == "IN"}


def get_strategy(name: str) -> StrategyConfig:
    """Look up a strategy by name."""
    if name not in ALL_STRATEGIES:
        available = ", ".join(ALL_STRATEGIES.keys())
        raise KeyError(
            f"Unknown strategy '{name}'. Available: {available}"
        )
    return ALL_STRATEGIES[name]


def list_strategies(market: str | None = None) -> list[str]:
    """Return available strategy names, optionally filtered by market."""
    if market is None:
        return list(ALL_STRATEGIES.keys())
    return [
        k for k, v in ALL_STRATEGIES.items()
        if v.market == market.upper()
    ]

##################