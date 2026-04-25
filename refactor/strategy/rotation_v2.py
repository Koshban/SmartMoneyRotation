"""
refactor/strategy/rotation_v2.py

Rotation engine with RRG-style quadrant classification.

US market  (sector-ETF mode)
----------------------------
Computes relative strength of the 11 GICS sector ETFs versus SPY and
classifies each sector into a rotation quadrant.  Every stock in the
universe inherits its parent sector's regime via TICKER_SECTOR_MAP.

HK / IN markets  (per-stock mode)
----------------------------------
No sector ETFs are available, so the engine computes per-stock relative
strength versus the market benchmark (2800.HK for HK, NIFTYBEES.NS for
IN) and classifies each stock directly into a rotation quadrant.

Quadrants (classic clockwise RRG rotation)
------------------------------------------
    leading    – RS above its trend AND accelerating
    improving  – RS below its trend BUT accelerating
    weakening  – RS above its trend BUT decelerating
    lagging    – RS below its trend AND decelerating

Output
------
``compute_sector_rotation`` returns a dict with:

    sector_summary  – DataFrame with rotation metrics and regime.
                      One row per sector (US) or per stock (HK / IN).
    sector_regimes  – dict[str, str]  sector → regime
                      (populated in sector-ETF mode only).
    ticker_regimes  – dict[str, str]  ticker → regime
                      (always populated).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── US sector map (imported if available) ─────────────────────────────────────
try:
    from common.sector_map import (
        SECTOR_ETFS as _US_SECTOR_ETFS,
        TICKER_SECTOR_MAP as _US_TICKER_SECTOR_MAP,
    )
except ImportError:
    _US_SECTOR_ETFS: dict[str, str] = {}
    _US_TICKER_SECTOR_MAP: dict[str, str] = {}
    logger.debug(
        "common.sector_map not available; US sector-ETF rotation disabled"
    )

# Per-market sector ETF definitions.
# Empty dict ⇒ no sector ETFs ⇒ per-stock rotation mode.
MARKET_SECTOR_ETFS: dict[str, dict[str, str]] = {
    "US": dict(_US_SECTOR_ETFS) if _US_SECTOR_ETFS else {},
    "HK": {},
    "IN": {},
}

MARKET_TICKER_SECTOR_MAP: dict[str, dict[str, str]] = {
    "US": dict(_US_TICKER_SECTOR_MAP) if _US_TICKER_SECTOR_MAP else {},
    "HK": {},
    "IN": {},
}

# ── Defaults ──────────────────────────────────────────────────────────────────
RS_SMA_PERIOD = 50
RS_MOMENTUM_PERIOD = 20
RS_SMOOTH_SPAN = 10
MIN_HISTORY = 60


# ═══════════════════════════════════════════════════════════════════════════════
# Market auto-detection
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_market(symbol_frames: dict[str, pd.DataFrame]) -> str:
    """
    Infer market from ticker suffixes in *symbol_frames*.

    Heuristic: count tickers ending in ``.HK``, ``.NS`` / ``.BO``,
    or neither.  Whichever suffix family has the majority wins.
    Falls back to ``"US"`` when ambiguous.
    """
    hk = 0
    india = 0
    other = 0
    for ticker in symbol_frames:
        t = ticker.upper()
        if t.endswith(".HK"):
            hk += 1
        elif t.endswith(".NS") or t.endswith(".BO"):
            india += 1
        else:
            other += 1
    total = hk + india + other
    if total == 0:
        return "US"
    if hk > total * 0.5:
        return "HK"
    if india > total * 0.5:
        return "IN"
    return "US"


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def compute_sector_rotation(
    symbol_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    market: str | None = None,
    rs_sma_period: int = RS_SMA_PERIOD,
    rs_momentum_period: int = RS_MOMENTUM_PERIOD,
    smooth_span: int = RS_SMOOTH_SPAN,
    min_history: int = MIN_HISTORY,
) -> dict:
    """
    Compute rotation quadrants.

    For US: sector-ETF rotation (11 GICS sectors vs SPY).
    For HK / IN: per-stock rotation (every ticker vs market benchmark).

    Parameters
    ----------
    symbol_frames : dict[str, DataFrame]
        Universe of symbol OHLCV frames keyed by ticker.
    bench_df : DataFrame
        Benchmark OHLCV data (SPY / 2800.HK / NIFTYBEES.NS).
    market : str or None
        Market code: ``"US"``, ``"HK"``, or ``"IN"``.
        If *None*, auto-detected from ticker suffixes.
    rs_sma_period : int
        Rolling window for the RS-ratio trend line (default 50).
    rs_momentum_period : int
        Lookback for rate-of-change of the RS ratio (default 20).
    smooth_span : int
        EMA span applied to the raw RS ratio (default 10).
    min_history : int
        Minimum aligned rows required to compute RS (default 60).

    Returns
    -------
    dict
        ``sector_summary`` (DataFrame), ``sector_regimes`` (dict),
        ``ticker_regimes`` (dict).
    """
    if bench_df is None or bench_df.empty or "close" not in bench_df.columns:
        logger.warning("compute_sector_rotation: benchmark missing or empty")
        return _empty_result()

    bench_close = pd.to_numeric(bench_df["close"], errors="coerce")
    if int(bench_close.notna().sum()) < min_history:
        logger.warning(
            "compute_sector_rotation: insufficient benchmark history (%d rows)",
            int(bench_close.notna().sum()),
        )
        return _empty_result()

    # ── resolve market ────────────────────────────────────────────────────
    if market is None:
        market = _detect_market(symbol_frames)
        logger.info(
            "Sector rotation: auto-detected market=%s from %d tickers",
            market, len(symbol_frames),
        )

    market = market.upper()
    sector_etfs = MARKET_SECTOR_ETFS.get(market, {})

    # ── sector-ETF mode (US) ──────────────────────────────────────────────
    if sector_etfs:
        # Check that at least some ETFs actually exist in symbol_frames
        # before committing to sector-ETF mode.
        available = sum(
            1 for etf in sector_etfs.values()
            if etf in symbol_frames
            and symbol_frames[etf] is not None
            and not symbol_frames[etf].empty
        )
        if available > 0:
            logger.info(
                "Sector rotation: sector-ETF mode  market=%s  "
                "etfs_available=%d/%d",
                market, available, len(sector_etfs),
            )
            result = _compute_sector_etf_rotation(
                symbol_frames=symbol_frames,
                bench_close=bench_close,
                sector_etfs=sector_etfs,
                ticker_sector_map=MARKET_TICKER_SECTOR_MAP.get(market, {}),
                rs_sma_period=rs_sma_period,
                rs_momentum_period=rs_momentum_period,
                smooth_span=smooth_span,
                min_history=min_history,
            )
            # If sector-ETF mode produced valid results, return them.
            if result["ticker_regimes"]:
                return result
            logger.warning(
                "Sector rotation: sector-ETF mode produced empty results; "
                "falling back to per-stock mode"
            )
        else:
            logger.info(
                "Sector rotation: market=%s but 0/%d sector ETFs "
                "present in symbol_frames; falling back to per-stock mode",
                market, len(sector_etfs),
            )

    # ── per-stock mode (HK, IN, or fallback) ─────────────────────────────
    logger.info(
        "Sector rotation: per-stock mode  market=%s  "
        "(%d tickers vs benchmark)",
        market, len(symbol_frames),
    )
    return _compute_per_stock_rotation(
        symbol_frames=symbol_frames,
        bench_close=bench_close,
        rs_sma_period=rs_sma_period,
        rs_momentum_period=rs_momentum_period,
        smooth_span=smooth_span,
        min_history=min_history,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_rs_for_one(
    target_close: pd.Series,
    bench_close: pd.Series,
    rs_sma_period: int,
    rs_momentum_period: int,
    smooth_span: int,
    min_history: int,
) -> dict | None:
    """
    Compute RRG-style RS metrics for a single close series vs benchmark.

    Returns a dict of scalar metrics, or ``None`` if data is insufficient.
    """
    combined = pd.DataFrame(
        {"target": target_close, "bench": bench_close},
    ).dropna()

    if len(combined) < min_history:
        return None

    # ── RS ratio (smoothed) ──────────────────────────────────────────────
    rs_raw = combined["target"] / combined["bench"]
    rs = rs_raw.ewm(
        span=smooth_span,
        min_periods=max(3, smooth_span // 2),
    ).mean()

    # ── RS level: fractional distance from its own SMA ──────────────────
    rs_sma = rs.rolling(
        rs_sma_period,
        min_periods=int(rs_sma_period * 0.7),
    ).mean()
    rs_level = rs / rs_sma - 1.0

    # ── RS momentum: rate-of-change of smoothed RS ratio ────────────────
    rs_mom = rs.pct_change(rs_momentum_period)

    # ── Secondary metrics ────────────────────────────────────────────────
    rs_roc_5 = rs.pct_change(5)
    excess_20 = (
        combined["target"].pct_change(20)
        - combined["bench"].pct_change(20)
    )

    last_level = rs_level.iloc[-1]
    last_mom = rs_mom.iloc[-1]

    if pd.isna(last_level) or pd.isna(last_mom):
        return None

    return {
        "rs_level": float(last_level),
        "rs_momentum": float(last_mom),
        "rs_roc_5d": (
            float(rs_roc_5.iloc[-1])
            if pd.notna(rs_roc_5.iloc[-1])
            else None
        ),
        "excess_return_20d": (
            float(excess_20.iloc[-1])
            if pd.notna(excess_20.iloc[-1])
            else None
        ),
        "rs_ratio_last": float(rs.iloc[-1]),
    }


def _classify_and_rank(summary: pd.DataFrame) -> pd.DataFrame:
    """Apply quadrant classification and cross-sectional ranks."""
    summary["regime"] = np.select(
        [
            (summary["rs_level"] > 0) & (summary["rs_momentum"] > 0),
            (summary["rs_level"] <= 0) & (summary["rs_momentum"] > 0),
            (summary["rs_level"] > 0) & (summary["rs_momentum"] <= 0),
        ],
        ["leading", "improving", "weakening"],
        default="lagging",
    )

    summary["rs_rank"] = (
        summary["rs_level"]
        .rank(ascending=False, method="min")
        .astype(int)
    )
    summary["momentum_rank"] = (
        summary["rs_momentum"]
        .rank(ascending=False, method="min")
        .astype(int)
    )

    return summary.sort_values("rs_rank").reset_index(drop=True)


def _log_regime_distribution(
    summary: pd.DataFrame,
    label_col: str,
    max_display: int = 20,
) -> None:
    """Log regime counts and per-regime members."""
    regime_dist = summary["regime"].value_counts().to_dict()
    logger.info("Rotation regime distribution: %s", regime_dist)

    for regime in ("leading", "improving", "weakening", "lagging"):
        members = summary.loc[
            summary["regime"] == regime, label_col
        ].tolist()
        display = members[:max_display]
        suffix = (
            f" ... (+{len(members) - max_display})"
            if len(members) > max_display
            else ""
        )
        logger.info(
            "  %-12s: %s%s",
            regime.title(),
            display or ["none"],
            suffix,
        )

    if logger.isEnabledFor(logging.DEBUG):
        display_cols = [
            label_col, "regime", "rs_rank", "momentum_rank",
            "rs_level", "rs_momentum", "excess_return_20d",
        ]
        cols = [c for c in display_cols if c in summary.columns]
        logger.debug(
            "Rotation summary:\n%s",
            summary[cols].to_string(index=False),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Sector-ETF mode (US)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_sector_etf_rotation(
    symbol_frames: dict[str, pd.DataFrame],
    bench_close: pd.Series,
    sector_etfs: dict[str, str],
    ticker_sector_map: dict[str, str],
    rs_sma_period: int,
    rs_momentum_period: int,
    smooth_span: int,
    min_history: int,
) -> dict:
    """One RS computation per GICS sector ETF; tickers inherit parent regime."""
    rows: list[dict] = []
    skipped_missing = 0
    skipped_short = 0

    for sector_name, etf_ticker in sector_etfs.items():
        etf_df = symbol_frames.get(etf_ticker)
        if etf_df is None or etf_df.empty or "close" not in etf_df.columns:
            logger.debug(
                "Sector rotation: %s (%s) not in symbol_frames",
                etf_ticker, sector_name,
            )
            skipped_missing += 1
            continue

        etf_close = pd.to_numeric(etf_df["close"], errors="coerce")
        metrics = _compute_rs_for_one(
            etf_close, bench_close,
            rs_sma_period, rs_momentum_period, smooth_span, min_history,
        )

        if metrics is None:
            logger.debug(
                "Sector rotation: %s (%s) insufficient data or NaN at tail",
                etf_ticker, sector_name,
            )
            skipped_short += 1
            continue

        rows.append({"sector": sector_name, "etf": etf_ticker, **metrics})

    logger.info(
        "Sector rotation: computed=%d  skipped_missing=%d  skipped_short=%d",
        len(rows), skipped_missing, skipped_short,
    )

    if not rows:
        logger.warning("compute_sector_rotation: no sectors could be computed")
        return _empty_result()

    summary = _classify_and_rank(pd.DataFrame(rows))

    # ── lookup dicts ─────────────────────────────────────────────────────
    sector_regimes: dict[str, str] = dict(
        zip(summary["sector"], summary["regime"]),
    )
    ticker_regimes: dict[str, str] = {
        ticker: sector_regimes.get(sector, "unknown")
        for ticker, sector in ticker_sector_map.items()
    }

    _log_regime_distribution(summary, "sector")

    return {
        "sector_summary": summary,
        "sector_regimes": sector_regimes,
        "ticker_regimes": ticker_regimes,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Per-stock mode (HK, IN, or any market without sector ETFs)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_per_stock_rotation(
    symbol_frames: dict[str, pd.DataFrame],
    bench_close: pd.Series,
    rs_sma_period: int,
    rs_momentum_period: int,
    smooth_span: int,
    min_history: int,
) -> dict:
    """
    One RS computation per universe ticker vs the market benchmark.

    Every stock is classified into its own RRG quadrant.
    To keep output schema compatible with sector-ETF mode,
    the ``sector`` column is set to the ticker itself, and
    ``sector_regimes`` mirrors ``ticker_regimes``.
    """
    rows: list[dict] = []
    skipped = 0

    for ticker, df in symbol_frames.items():
        if df is None or df.empty or "close" not in df.columns:
            skipped += 1
            continue

        stock_close = pd.to_numeric(df["close"], errors="coerce")
        metrics = _compute_rs_for_one(
            stock_close, bench_close,
            rs_sma_period, rs_momentum_period, smooth_span, min_history,
        )

        if metrics is None:
            logger.debug(
                "Per-stock rotation: %s skipped (insufficient data or NaN)",
                ticker,
            )
            skipped += 1
            continue

        rows.append({"ticker": ticker, "sector": ticker, **metrics})

    logger.info(
        "Per-stock rotation: computed=%d  skipped=%d  total=%d",
        len(rows), skipped, len(symbol_frames),
    )

    if not rows:
        logger.warning(
            "compute_sector_rotation: no stocks could be computed "
            "(per-stock mode)"
        )
        return _empty_result()

    summary = _classify_and_rank(pd.DataFrame(rows))

    ticker_regimes: dict[str, str] = dict(
        zip(summary["ticker"], summary["regime"]),
    )

    # sector_regimes mirrors ticker_regimes so downstream code
    # that looks up sectrsregime via sector_regimes gets real
    # quadrants instead of "unknown".
    sector_regimes: dict[str, str] = dict(ticker_regimes)

    _log_regime_distribution(summary, "ticker")

    return {
        "sector_summary": summary,
        "sector_regimes": sector_regimes,
        "ticker_regimes": ticker_regimes,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_result() -> dict:
    """Return a well-typed empty result when rotation cannot be computed."""
    return {
        "sector_summary": pd.DataFrame(),
        "sector_regimes": {},
        "ticker_regimes": {},
    }