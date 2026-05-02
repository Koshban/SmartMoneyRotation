"""
portfolio/risk.py
-----------------
Portfolio risk metrics and analysis.

Pure functions — no state.  Take an equity curve, daily
returns, or position data and return risk metrics.

Functions
─────────
  compute_drawdown()          drawdown series + stats
  compute_var()               historical Value at Risk
  compute_cvar()              Conditional VaR (exp. shortfall)
  concentration_risk()        HHI and top-weight metrics
  rolling_volatility()        rolling annualised vol
  compute_portfolio_risk()    master function → full risk dict
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  DRAWDOWN
# ═══════════════════════════════════════════════════════════════

def compute_drawdown(
    equity_curve: pd.Series,
) -> pd.DataFrame:
    """
    Compute drawdown series from an equity curve.

    Returns DataFrame with columns:
      equity         the input equity curve
      peak           running peak
      drawdown       fractional drawdown (negative)
      drawdown_pct   drawdown as percentage
    """
    if equity_curve.empty:
        return pd.DataFrame()

    peak = equity_curve.cummax()
    dd = (equity_curve - peak) / peak

    return pd.DataFrame({
        "equity": equity_curve,
        "peak": peak,
        "drawdown": dd,
        "drawdown_pct": dd * 100,
    })


def drawdown_stats(
    equity_curve: pd.Series,
) -> dict:
    """
    Summary drawdown statistics.

    Returns dict with: max_drawdown, max_dd_start, max_dd_end,
    max_dd_duration, current_drawdown, recovery_days.
    """
    if equity_curve.empty:
        return {}

    dd_df = compute_drawdown(equity_curve)
    dd = dd_df["drawdown"]

    max_dd = dd.min()
    max_dd_idx = dd.idxmin()

    # Peak before max drawdown
    peak_before = dd_df["peak"].loc[:max_dd_idx]
    if not peak_before.empty:
        peak_idx = equity_curve.loc[
            :max_dd_idx
        ].idxmax()
    else:
        peak_idx = dd.index[0]

    # Duration: count trading days from peak to trough
    if hasattr(peak_idx, "strftime"):
        duration = len(dd.loc[peak_idx:max_dd_idx])
    else:
        duration = 0

    # Current drawdown
    current_dd = dd.iloc[-1] if len(dd) > 0 else 0.0

    return {
        "max_drawdown": max_dd,
        "max_dd_start": peak_idx,
        "max_dd_end": max_dd_idx,
        "max_dd_duration": duration,
        "current_drawdown": current_dd,
    }


# ═══════════════════════════════════════════════════════════════
#  VALUE AT RISK
# ═══════════════════════════════════════════════════════════════

def compute_var(
    returns: pd.Series,
    confidence: float = 0.95,
) -> float:
    """
    Historical Value at Risk.

    Returns the loss threshold at the given confidence level
    (negative number — e.g. -0.02 means 2% daily loss).
    """
    if returns.empty:
        return 0.0
    return float(returns.quantile(1 - confidence))


def compute_cvar(
    returns: pd.Series,
    confidence: float = 0.95,
) -> float:
    """
    Conditional VaR (Expected Shortfall).

    Average loss in the worst (1 - confidence) tail.
    """
    if returns.empty:
        return 0.0
    var = compute_var(returns, confidence)
    tail = returns[returns <= var]
    return float(tail.mean()) if len(tail) > 0 else var


# ═══════════════════════════════════════════════════════════════
#  CONCENTRATION RISK
# ═══════════════════════════════════════════════════════════════

def concentration_risk(
    weights: dict[str, float],
) -> dict:
    """
    Concentration metrics for current portfolio weights.

    Returns dict with: hhi (Herfindahl-Hirschman Index),
    effective_n (equivalent number of equal positions),
    max_weight, max_ticker.
    """
    if not weights:
        return {
            "hhi": 0.0,
            "effective_n": 0,
            "max_weight": 0.0,
            "max_ticker": None,
        }

    vals = np.array(list(weights.values()))
    total = vals.sum()
    if total <= 0:
        return {
            "hhi": 0.0,
            "effective_n": 0,
            "max_weight": 0.0,
            "max_ticker": None,
        }

    w = vals / total
    hhi = float((w ** 2).sum())
    effective_n = 1.0 / hhi if hhi > 0 else 0
    max_w = float(w.max())
    max_ticker = list(weights.keys())[int(w.argmax())]

    return {
        "hhi": hhi,
        "effective_n": effective_n,
        "max_weight": max_w,
        "max_ticker": max_ticker,
    }


# ═══════════════════════════════════════════════════════════════
#  ROLLING VOLATILITY
# ═══════════════════════════════════════════════════════════════

def rolling_volatility(
    returns: pd.Series,
    window: int = 20,
    annualise: bool = True,
) -> pd.Series:
    """Rolling annualised volatility."""
    vol = returns.rolling(window, min_periods=max(window // 2, 2)).std()
    if annualise:
        vol = vol * np.sqrt(252)
    return vol


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_portfolio_risk(
    equity_curve: pd.Series,
    daily_returns: pd.Series | None = None,
    current_weights: dict[str, float] | None = None,
) -> dict:
    """
    Comprehensive portfolio risk metrics.

    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio value.
    daily_returns : pd.Series or None
        Daily returns (computed from equity if not provided).
    current_weights : dict or None
        Current position weights for concentration analysis.

    Returns
    -------
    dict
        Full risk metrics including drawdown, VaR, CVaR,
        volatility, and concentration.
    """
    if equity_curve.empty:
        return {}

    if daily_returns is None:
        daily_returns = equity_curve.pct_change().dropna()

    result: dict = {}

    # Drawdown
    result.update(drawdown_stats(equity_curve))

    # VaR / CVaR
    result["var_95"] = compute_var(daily_returns, 0.95)
    result["var_99"] = compute_var(daily_returns, 0.99)
    result["cvar_95"] = compute_cvar(daily_returns, 0.95)

    # Volatility
    if len(daily_returns) > 0:
        result["daily_volatility"] = float(daily_returns.std())
        result["annual_volatility"] = float(
            daily_returns.std() * np.sqrt(252)
        )
    else:
        result["daily_volatility"] = 0.0
        result["annual_volatility"] = 0.0

    # Skew / kurtosis
    if len(daily_returns) > 5:
        result["skewness"] = float(daily_returns.skew())
        result["kurtosis"] = float(daily_returns.kurtosis())

    # Concentration
    if current_weights:
        result["concentration"] = concentration_risk(
            current_weights
        )

    return result