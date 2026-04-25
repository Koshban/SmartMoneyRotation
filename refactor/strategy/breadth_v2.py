"""
refactor/strategy/breadth_v2.py

Cross-sectional market-breadth computation and regime classification.

Builds a date × symbol close-price panel from the full universe and
computes the following daily cross-sectional metrics:

    pct_above_sma20   – fraction of symbols above their 20-day SMA
    pct_above_sma50   – fraction above 50-day SMA
    pct_above_sma200  – fraction above 200-day SMA
    pct_advancing     – fraction with a positive daily return
    net_new_highs_pct – (20-d new highs − 20-d new lows) / universe size
    dispersion_daily  – cross-sectional std-dev of daily returns
    dispersion20      – 20-day rolling mean of dispersion_daily

A weighted composite ``breadthscore`` (0–1) is EMA-smoothed and
classified into one of four regimes:

    strong   – breadthscore ≥ 0.65
    moderate – 0.45 ≤ breadthscore < 0.65
    weak     – 0.25 ≤ breadthscore < 0.45
    critical – breadthscore < 0.25
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
BREADTH_MIN_SYMBOLS = 5
BREADTH_MIN_HISTORY = 55
BREADTH_EMA_SPAN = 5

# Regime thresholds
REGIME_STRONG = 0.65
REGIME_MODERATE = 0.45
REGIME_WEAK = 0.25


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def compute_breadth(
    symbol_frames: dict[str, pd.DataFrame],
    min_history: int = BREADTH_MIN_HISTORY,
    ema_span: int = BREADTH_EMA_SPAN,
) -> pd.DataFrame:
    """
    Compute market breadth from the universe of symbol frames.

    Parameters
    ----------
    symbol_frames : dict[str, DataFrame]
        Mapping of ticker → OHLCV-style DataFrame (must contain ``close``).
    min_history : int
        Minimum number of rows a symbol needs to enter the panel.
    ema_span : int
        Exponential-moving-average span used to smooth the composite score.

    Returns
    -------
    DataFrame
        Indexed by date with breadth metrics, ``breadthscore``, and
        ``breadthregime``.  Empty if fewer than ``BREADTH_MIN_SYMBOLS``
        symbols qualify.
    """
    if not symbol_frames:
        logger.warning("compute_breadth: no symbol frames provided")
        return pd.DataFrame()

    # ── 1. close-price panel ──────────────────────────────────────────────────
    close_dict: dict[str, pd.Series] = {}
    for ticker, df in symbol_frames.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        if len(df) < min_history:
            continue
        close_dict[ticker] = pd.to_numeric(df["close"], errors="coerce")

    if len(close_dict) < BREADTH_MIN_SYMBOLS:
        logger.warning(
            "compute_breadth: only %d symbols with >= %d rows (need >= %d)",
            len(close_dict), min_history, BREADTH_MIN_SYMBOLS,
        )
        return pd.DataFrame()

    close = pd.DataFrame(close_dict)
    n_valid = close.notna().sum(axis=1).replace(0, np.nan)
    logger.info(
        "compute_breadth: panel shape=(%d dates, %d symbols)",
        close.shape[0], close.shape[1],
    )

    # ── 2. moving-average breadth ─────────────────────────────────────────────
    sma20 = close.rolling(20, min_periods=15).mean()
    sma50 = close.rolling(50, min_periods=40).mean()
    sma200 = close.rolling(200, min_periods=150).mean()

    pct_above_sma20 = (close > sma20).sum(axis=1) / n_valid
    pct_above_sma50 = (close > sma50).sum(axis=1) / n_valid
    pct_above_sma200 = (close > sma200).sum(axis=1) / n_valid

    # ── 3. advance / decline ──────────────────────────────────────────────────
    daily_ret = close.pct_change()
    pct_advancing = (daily_ret > 0).sum(axis=1) / n_valid

    # ── 4. new-high / new-low (20-day rolling) ───────────────────────────────
    high20 = close.rolling(20, min_periods=15).max()
    low20 = close.rolling(20, min_periods=15).min()
    at_high = (close >= high20 - 1e-8).sum(axis=1) / n_valid
    at_low = (close <= low20 + 1e-8).sum(axis=1) / n_valid
    net_new_highs_pct = at_high - at_low

    # ── 5. cross-sectional dispersion ─────────────────────────────────────────
    dispersion_daily = daily_ret.std(axis=1, ddof=1)
    dispersion20 = dispersion_daily.rolling(20, min_periods=15).mean()

    # ── 6. composite score (0–1) ──────────────────────────────────────────────
    #
    # Each sub-metric is naturally [0, 1] except net_new_highs_pct which
    # is [−1, 1], so we rescale it to [0, 1].
    raw_score = (
        0.30 * pct_above_sma50.fillna(0.5)
        + 0.20 * pct_above_sma200.fillna(0.5)
        + 0.15 * pct_above_sma20.fillna(0.5)
        + 0.15 * pct_advancing.fillna(0.5)
        + 0.20 * ((net_new_highs_pct.fillna(0.0) + 1.0) / 2.0)
    ).clip(0.0, 1.0)

    breadthscore = raw_score.ewm(
        span=ema_span, min_periods=max(3, ema_span // 2),
    ).mean()

    # ── 7. regime classification ──────────────────────────────────────────────
    classification = pd.Series(
        np.select(
            [
                breadthscore >= REGIME_STRONG,
                breadthscore >= REGIME_MODERATE,
                breadthscore >= REGIME_WEAK,
                breadthscore < REGIME_WEAK,
            ],
            ["strong", "moderate", "weak", "critical"],
            default="unknown",
        ),
        index=close.index,
    )
    breadthregime = classification.where(breadthscore.notna(), "unknown")

    # ── 8. assemble output ────────────────────────────────────────────────────
    breadth = pd.DataFrame(
        {
            "pct_above_sma20": pct_above_sma20,
            "pct_above_sma50": pct_above_sma50,
            "pct_above_sma200": pct_above_sma200,
            "pct_advancing": pct_advancing,
            "pct_new_high_20": at_high,
            "pct_new_low_20": at_low,
            "net_new_highs_pct": net_new_highs_pct,
            "dispersion_daily": dispersion_daily,
            "dispersion20": dispersion20,
            "breadthscore_raw": raw_score,
            "breadthscore": breadthscore,
            "breadthregime": breadthregime,
        },
        index=close.index,
    )

    # ── 9. diagnostics ───────────────────────────────────────────────────────
    if not breadth.empty:
        last = breadth.iloc[-1]
        logger.info(
            "compute_breadth last-date: score=%.4f regime=%s "
            "above_sma50=%.1f%% above_sma200=%.1f%% advancing=%.1f%% "
            "net_highs=%.1f%% dispersion20=%.6f",
            float(last.get("breadthscore", 0)),
            str(last.get("breadthregime", "unknown")),
            float(last.get("pct_above_sma50", 0)) * 100,
            float(last.get("pct_above_sma200", 0)) * 100,
            float(last.get("pct_advancing", 0)) * 100,
            float(last.get("net_new_highs_pct", 0)) * 100,
            float(last.get("dispersion20", 0)),
        )
        recent_regimes = breadth["breadthregime"].tail(20).value_counts().to_dict()
        logger.info("compute_breadth last-20-day regime dist: %s", recent_regimes)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "compute_breadth tail(5):\n%s",
                breadth[
                    ["breadthscore", "breadthregime", "pct_above_sma50",
                     "pct_advancing", "dispersion20"]
                ].tail(5).to_string(),
            )

    return breadth