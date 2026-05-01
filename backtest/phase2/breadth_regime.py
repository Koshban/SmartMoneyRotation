# backtest/phase2/breadth_regime.py

import pandas as pd
import numpy as np


def compute_breadth_regime(
    prices: pd.DataFrame,
    params: dict,
) -> pd.DataFrame:
    """
    Compute daily breadth regime for the full universe.
    
    Returns DataFrame with columns: [date, pct_above_sma50, regime]
    Regime ∈ {'strong', 'neutral', 'weak'}
    """
    sma_period = params.get("ma_short", 50)
    strong_thr = params.get("strong_threshold", 0.65)
    weak_thr = params.get("weak_threshold", 0.35)
    smooth = params.get("smoothing_window", 5)
    
    # For each ticker, is price above its SMA50?
    sma50 = prices.rolling(sma_period).mean()
    above_sma = (prices > sma50).astype(float)
    
    # Percent of universe above SMA50
    pct_above = above_sma.mean(axis=1)
    
    # Smooth to avoid whipsaws
    pct_smooth = pct_above.rolling(smooth, min_periods=1).mean()
    
    # Classify regime
    regime = pd.Series("neutral", index=pct_smooth.index)
    regime[pct_smooth >= strong_thr] = "strong"
    regime[pct_smooth <= weak_thr] = "weak"
    
    result = pd.DataFrame({
        "pct_above_sma50": pct_smooth,
        "regime": regime,
    })
    
    return result


def get_exposure_multiplier(regime: str, params: dict) -> float:
    """Map breadth regime to exposure multiplier."""
    mapping = {
        "strong": params.get("strong_exposure", 1.0),
        "neutral": params.get("neutral_exposure", 0.75),
        "weak": params.get("weak_exposure", 0.40),
    }
    return mapping.get(regime, 0.75)