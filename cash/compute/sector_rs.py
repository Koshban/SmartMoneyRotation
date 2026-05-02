"""
compute/sector_rs.py
--------------------
Sector-level relative-strength analysis.

Answers: "Which sectors are leading the market rotation?"

Pipeline
────────
  1. Fetch OHLCV for 11 GICS sector ETFs + benchmark (SPY).
  2. Compute RS ratio / slope / z-score / regime per sector
     (same math as stock-level RS in relative_strength.py).
  3. Cross-sectionally rank sectors each day — who's strongest?
  4. Derive a tailwind / headwind value per sector for the
     composite-score adjustment.
  5. Merge sector context into individual stock DataFrames.

All tuneable parameters live in common/config.py.
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from common.config import (
    BENCHMARK_TICKER,
    SECTOR_ETFS,
    SECTOR_RS_PARAMS,
    SECTOR_SCORE_ADJUSTMENT,
)


# ═══════════════════════════════════════════════════════════════
#  yfinance sector label → our SECTOR_ETFS key
# ═══════════════════════════════════════════════════════════════

_YF_SECTOR_MAP: dict[str, str] = {
    "Technology":             "Technology",
    "Healthcare":             "Healthcare",
    "Financial Services":     "Financials",
    "Consumer Cyclical":      "Consumer Disc",
    "Consumer Defensive":     "Consumer Staples",
    "Energy":                 "Energy",
    "Industrials":            "Industrials",
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Communication Services": "Communication",
}

# In-memory cache so we only hit yfinance once per ticker
_sector_cache: dict[str, str | None] = {}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _clean_download(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise a yfinance download to lower-case flat columns."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """OLS slope over a rolling window."""
    def _slope(arr):
        if np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]
    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True,
    )


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_sector_data(
    period: str = "2y",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Download OHLCV for every sector ETF **and** the benchmark.

    Parameters
    ----------
    period : str
        yfinance period string ("2y", "5y", "max" …).

    Returns
    -------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV DataFrame} for each sector.
    benchmark_df : pd.DataFrame
        OHLCV for the benchmark (SPY by default).
    """
    import yfinance as yf

    sector_data: dict[str, pd.DataFrame] = {}
    for name, etf in SECTOR_ETFS.items():
        raw = yf.download(etf, period=period, progress=False)
        if raw.empty:
            warnings.warn(f"No data for {etf} ({name}), skipping.")
            continue
        sector_data[name] = _clean_download(raw)

    bench_raw = yf.download(BENCHMARK_TICKER, period=period, progress=False)
    if bench_raw.empty:
        raise ValueError(f"Benchmark {BENCHMARK_TICKER} returned no data.")
    benchmark_df = _clean_download(bench_raw)

    return sector_data, benchmark_df


# ═══════════════════════════════════════════════════════════════
#  SINGLE-SECTOR RS COMPUTATION
# ═══════════════════════════════════════════════════════════════

def _compute_single_sector_rs(
    sector_close: pd.Series,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS metrics for **one** sector ETF vs the benchmark.

    Same mathematics as relative_strength.py:
      ratio → slope → z-score → regime.

    Returns DataFrame with columns prefixed ``sect_rs_``.
    """
    p = SECTOR_RS_PARAMS

    # ── RS ratio ─────────────────────────────────────────────
    ratio = sector_close / bench_close

    # ── RS slope (direction of relative performance) ─────────
    slope = _rolling_slope(ratio, p["slope_window"])

    # ── Z-score of slope ─────────────────────────────────────
    s_mean = slope.rolling(p["zscore_window"], min_periods=20).mean()
    s_std  = slope.rolling(p["zscore_window"], min_periods=20).std()
    zscore = (slope - s_mean) / s_std.replace(0, np.nan)

    # ── Momentum (acceleration of rotation) ──────────────────
    momentum = slope.diff(p["momentum_window"])

    # ── Regime ───────────────────────────────────────────────
    conditions = [
        (zscore > 0) & (momentum > 0),
        (zscore <= 0) & (momentum > 0),
        (zscore > 0)  & (momentum <= 0),
        (zscore <= 0) & (momentum <= 0),
    ]
    choices = ["leading", "improving", "weakening", "lagging"]
    regime = pd.Series(
        np.select(conditions, choices, default="unknown"),
        index=ratio.index,
    )

    return pd.DataFrame({
        "sect_rs_ratio":    ratio,
        "sect_rs_slope":    slope,
        "sect_rs_zscore":   zscore,
        "sect_rs_momentum": momentum,
        "sect_rs_regime":   regime,
    }, index=ratio.index)


# ═══════════════════════════════════════════════════════════════
#  MASTER COMPUTATION — ALL SECTORS + CROSS-SECTIONAL RANKS
# ═══════════════════════════════════════════════════════════════

def compute_all_sector_rs(
    sector_data: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute RS for every sector and add cross-sectional rankings.

    Parameters
    ----------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV} from :func:`fetch_sector_data`.
    benchmark_df : pd.DataFrame
        Benchmark OHLCV.

    Returns
    -------
    pd.DataFrame
        **MultiIndex (date, sector)** with columns:

        ============== ===============================================
        sect_rs_ratio  raw price ratio (sector ETF / benchmark)
        sect_rs_slope  regression slope of the ratio
        sect_rs_zscore standardised slope
        sect_rs_momentum acceleration of the slope
        sect_rs_regime leading / improving / weakening / lagging
        sect_rs_rank   integer rank, 1 = strongest (NaN during warmup)
        sect_rs_pctrank percentile rank 0–1, 1.0 = strongest (smoothed)
        sector_tailwind score adjustment value
        etf            sector ETF ticker symbol
        ============== ===============================================
    """
    frames: list[pd.DataFrame] = []

    for sector_name, sector_df in sector_data.items():
        common = sector_df.index.intersection(benchmark_df.index)
        if len(common) < 60:
            warnings.warn(
                f"{sector_name}: only {len(common)} overlapping dates, "
                f"need ≥60.  Skipping."
            )
            continue

        metrics = _compute_single_sector_rs(
            sector_df.loc[common, "close"],
            benchmark_df.loc[common, "close"],
        )
        metrics["sector"] = sector_name
        metrics["etf"]    = SECTOR_ETFS[sector_name]
        metrics.index.name = "date"
        frames.append(metrics)

    if not frames:
        raise ValueError("No sectors produced valid RS data.")

    combined = pd.concat(frames)

    # ── Cross-sectional rank per date ─────────────────────────
    #    rank 1 = highest z-score = strongest sector
    #    NaN zscore rows → NaN rank (warmup period)
    combined["sect_rs_rank"] = (
        combined
        .groupby(level=0)["sect_rs_zscore"]
        .rank(ascending=False, method="min", na_option="keep")
    )

    # ── Percentile rank (0–1, 1.0 = strongest) ───────────────
    #    Count only non-NaN ranks per date
    n_ranked = (
        combined
        .groupby(level=0)["sect_rs_rank"]
        .transform(lambda s: s.notna().sum())
    )
    combined["sect_rs_pctrank"] = np.where(
        combined["sect_rs_rank"].isna() | (n_ranked <= 1),
        np.nan,
        1.0 - (combined["sect_rs_rank"] - 1) / (n_ranked - 1),
    )

    # ── Smooth the percentile rank over time ──────────────────
    smooth_w = SECTOR_RS_PARAMS.get("rank_smoothing", 5)
    if smooth_w > 1:
        combined["sect_rs_pctrank"] = (
            combined
            .groupby("sector")["sect_rs_pctrank"]
            .transform(lambda s: s.rolling(smooth_w, min_periods=1).mean())
        )

    # ── Sector tailwind / headwind ────────────────────────────
    #    pctrank 1.0 → max_boost,  0.0 → max_penalty,  0.5 → 0
    adj = SECTOR_SCORE_ADJUSTMENT
    if adj["enabled"]:
        combined["sector_tailwind"] = np.where(
            combined["sect_rs_pctrank"].isna(),
            0.0,
            adj["max_penalty"]
            + (adj["max_boost"] - adj["max_penalty"])
            * combined["sect_rs_pctrank"],
        )
    else:
        combined["sector_tailwind"] = 0.0

    # ── Set MultiIndex (date, sector) ─────────────────────────
    combined = (
        combined
        .reset_index()
        .set_index(["date", "sector"])
        .sort_index()
    )

    return combined


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT — one-day cross-sectional view (for dashboards)
# ═══════════════════════════════════════════════════════════════

def sector_snapshot(
    sector_rs_df: pd.DataFrame,
    date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Sector rankings for a single date, sorted strongest → weakest.

    Parameters
    ----------
    sector_rs_df : pd.DataFrame
        Output of :func:`compute_all_sector_rs` (MultiIndex).
    date : str or Timestamp, optional
        Target date.  Defaults to the latest available date.

    Returns
    -------
    pd.DataFrame  – one row per sector, indexed by sector name.
    """
    dates = sector_rs_df.index.get_level_values("date")

    if date is None:
        target = dates.max()
    else:
        target = pd.Timestamp(date)
        if target not in dates:
            available = dates.unique().sort_values()
            mask = available <= target
            if not mask.any():
                raise ValueError(f"No data on or before {date}")
            target = available[mask][-1]

    snap = sector_rs_df.loc[target].copy()
    return snap.sort_values("sect_rs_rank")


# ═══════════════════════════════════════════════════════════════
#  SECTOR LOOKUP — ticker → sector name
# ═══════════════════════════════════════════════════════════════

def lookup_sector(ticker: str) -> str | None:
    """
    Look up the GICS sector for a stock ticker via yfinance.

    Returns the sector name matching a SECTOR_ETFS key,
    or ``None`` if the lookup fails.

    Results are cached in memory for the session.
    """
    import yfinance as yf

    ticker = ticker.upper()

    if ticker in _sector_cache:
        return _sector_cache[ticker]

    try:
        info = yf.Ticker(ticker).info
        yf_sector = info.get("sector", None)
        if yf_sector and yf_sector in _YF_SECTOR_MAP:
            result = _YF_SECTOR_MAP[yf_sector]
        else:
            warnings.warn(
                f"Could not map yfinance sector '{yf_sector}' "
                f"for {ticker}."
            )
            result = None
    except Exception as e:
        warnings.warn(f"Sector lookup failed for {ticker}: {e}")
        result = None

    _sector_cache[ticker] = result
    return result


# ═══════════════════════════════════════════════════════════════
#  MERGE — add sector context to an individual stock DataFrame
# ═══════════════════════════════════════════════════════════════

def merge_sector_context(
    stock_df: pd.DataFrame,
    sector_rs_df: pd.DataFrame,
    sector_name: str,
) -> pd.DataFrame:
    """
    Add sector-level columns to an individual stock's DataFrame.

    Columns added
    ─────────────
    sect_rs_zscore     Sector z-score vs benchmark
    sect_rs_regime     Sector regime
    sect_rs_rank       Sector rank (1 = best)
    sect_rs_pctrank    Sector percentile (1.0 = best)
    sector_tailwind    Score adjustment value
    sector_name        Sector label

    If ``score_composite`` already exists, also creates:

    score_adjusted     score_composite + sector_tailwind, clipped [0, 1]

    Parameters
    ----------
    stock_df : pd.DataFrame
        Date-indexed stock data (with or without score columns).
    sector_rs_df : pd.DataFrame
        MultiIndex (date, sector) from :func:`compute_all_sector_rs`.
    sector_name : str
        Must match a key in SECTOR_ETFS.

    Returns
    -------
    pd.DataFrame  – stock_df with sector columns appended.
    """
    available_sectors = (
        sector_rs_df.index
        .get_level_values("sector")
        .unique()
        .tolist()
    )
    if sector_name not in available_sectors:
        raise ValueError(
            f"Sector '{sector_name}' not found.  "
            f"Available: {available_sectors}"
        )

    # Extract this sector, drop the sector level → date-indexed
    sect = sector_rs_df.xs(sector_name, level="sector")

    merge_cols = [
        "sect_rs_zscore", "sect_rs_regime", "sect_rs_rank",
        "sect_rs_pctrank", "sector_tailwind",
    ]
    merge_cols = [c for c in merge_cols if c in sect.columns]

    out = stock_df.copy()
    out = out.join(sect[merge_cols], how="left")
    out["sector_name"] = sector_name

    # ── Adjusted composite score ─────────────────────────────
    if "score_composite" in out.columns and "sector_tailwind" in out.columns:
        out["score_adjusted"] = (
            out["score_composite"]
            + out["sector_tailwind"].fillna(0)
        ).clip(0, 1)

    return out