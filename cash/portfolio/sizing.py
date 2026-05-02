"""
portfolio/sizing.py
-------------------
Position sizing algorithms.

Given a set of active tickers and their signal strengths,
compute target portfolio weights.  All methods respect
max / min position limits and normalise to a target exposure.

Methods
───────
  equal_weight        1/N for each active position
  score_weighted      proportional to signal_strength
  inverse_volatility  inversely proportional to recent vol
  risk_parity         equal risk contribution (approx)

Pipeline
────────
  active tickers + metadata
       ↓
  _raw_weights()     — method-specific raw weights
       ↓
  _apply_limits()    — clip to [min, max] per position
       ↓
  _normalise()       — scale so weights sum to target_exposure
       ↓
  compute_target_weights()  ← master function
"""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class SizingConfig:
    """Position sizing parameters."""

    method: str = "score_weighted"

    # Per-position limits (fraction of portfolio)
    max_position_pct: float = 0.25
    min_position_pct: float = 0.02

    # Total target equity exposure (rest stays cash)
    target_exposure: float = 1.00

    # Volatility lookback for vol-based methods
    vol_lookback: int = 20


# ═══════════════════════════════════════════════════════════════
#  RAW WEIGHT METHODS
# ═══════════════════════════════════════════════════════════════

def equal_weight(
    tickers: list[str],
    **kwargs,
) -> dict[str, float]:
    """1/N equal weight."""
    if not tickers:
        return {}
    w = 1.0 / len(tickers)
    return {t: w for t in tickers}


def score_weighted(
    tickers: list[str],
    strengths: dict[str, float] | None = None,
    **kwargs,
) -> dict[str, float]:
    """Weight proportional to signal strength."""
    if not tickers:
        return {}
    if strengths is None:
        return equal_weight(tickers)

    raw = {t: max(strengths.get(t, 0.0), 0.001) for t in tickers}
    total = sum(raw.values())
    if total <= 0:
        return equal_weight(tickers)
    return {t: v / total for t, v in raw.items()}


def inverse_volatility(
    tickers: list[str],
    volatilities: dict[str, float] | None = None,
    **kwargs,
) -> dict[str, float]:
    """Weight inversely proportional to volatility."""
    if not tickers:
        return {}
    if volatilities is None:
        return equal_weight(tickers)

    inv = {}
    for t in tickers:
        vol = volatilities.get(t, 0.0)
        inv[t] = 1.0 / max(vol, 0.001)

    total = sum(inv.values())
    if total <= 0:
        return equal_weight(tickers)
    return {t: v / total for t, v in inv.items()}


def risk_parity(
    tickers: list[str],
    volatilities: dict[str, float] | None = None,
    **kwargs,
) -> dict[str, float]:
    """
    Approximate risk parity: equal risk contribution.

    Uses inverse-variance weighting as a simple approximation
    (exact risk parity requires a covariance matrix and
    iterative optimisation).
    """
    if not tickers:
        return {}
    if volatilities is None:
        return equal_weight(tickers)

    inv_var = {}
    for t in tickers:
        vol = volatilities.get(t, 0.0)
        inv_var[t] = 1.0 / max(vol ** 2, 1e-8)

    total = sum(inv_var.values())
    if total <= 0:
        return equal_weight(tickers)
    return {t: v / total for t, v in inv_var.items()}


_METHODS = {
    "equal":              equal_weight,
    "equal_weight":       equal_weight,
    "score_weighted":     score_weighted,
    "inverse_volatility": inverse_volatility,
    "risk_parity":        risk_parity,
}


# ═══════════════════════════════════════════════════════════════
#  LIMIT AND NORMALISE
# ═══════════════════════════════════════════════════════════════

def _apply_limits(
    weights: dict[str, float],
    config: SizingConfig,
) -> dict[str, float]:
    """Clip weights to [min, max] and drop sub-minimum."""
    result = {}
    for t, w in weights.items():
        if w < config.min_position_pct:
            continue
        result[t] = min(w, config.max_position_pct)
    return result


def _normalise(
    weights: dict[str, float],
    target: float,
) -> dict[str, float]:
    """Scale weights to sum to target exposure."""
    total = sum(weights.values())
    if total <= 0 or not weights:
        return weights
    scale = target / total
    return {t: w * scale for t, w in weights.items()}


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_target_weights(
    tickers: list[str],
    config: SizingConfig | None = None,
    strengths: dict[str, float] | None = None,
    volatilities: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Compute target portfolio weights for active tickers.

    Parameters
    ----------
    tickers : list[str]
        Active tickers (BUY or HOLD signals).
    config : SizingConfig or None
        Sizing parameters.
    strengths : dict or None
        {ticker: signal_strength} for score-weighted method.
    volatilities : dict or None
        {ticker: annualised_vol} for vol-based methods.

    Returns
    -------
    dict[str, float]
        {ticker: target_weight} where weights sum to
        ``config.target_exposure`` (or less if positions
        are dropped for being below minimum).
    """
    if config is None:
        config = SizingConfig()
    if not tickers:
        return {}

    method_fn = _METHODS.get(config.method, score_weighted)

    raw = method_fn(
        tickers,
        strengths=strengths,
        volatilities=volatilities,
    )

    # First normalise to target, then clip, then re-normalise
    raw = _normalise(raw, config.target_exposure)
    clipped = _apply_limits(raw, config)

    if not clipped:
        return {}

    return _normalise(clipped, config.target_exposure)