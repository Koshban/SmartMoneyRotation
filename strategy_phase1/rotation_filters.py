"""
strategy/rotation_filters.py
-----------------------------
Technical quality filters for the rotation engine's stock selection.

The rotation engine identifies which sectors are leading and which
stocks within those sectors have the strongest relative strength.
This module adds a second dimension: technical quality confirmation.

A stock with strong RS but exhausted technicals (RSI 85, extended
above all MAs, declining volume) is likely to mean-revert.  A stock
with strong RS AND confirmed technicals (trending MAs, RSI in the
sweet spot, rising volume) is the high-conviction pick.

Architecture
────────────
  rotation.py::_pick_stocks()
       ↓  candidate ticker + its indicator DataFrame
  quality_gate()        — hard pass/fail checks (all must pass)
       ↓
  quality_score()       — 0–1 weighted quality metric
       ↓
  blend_rs_quality()    — combine RS + quality for final ranking

All thresholds live in QualityConfig and are set via
RotationConfig.quality in rotation.py.

Column Dependencies (from compute/indicators.py)
─────────────────────────────────────────────────
  close, ema_30, sma_30, sma_50
  close_vs_ema_30_pct, close_vs_sma_50_pct
  rsi_14, adx_14, plus_di, minus_di
  macd_hist, macd_line, macd_signal
  obv_slope_10d, relative_volume
  atr_14_pct

All of these are produced by compute_all_indicators() which runs
during the standard bottom-up pipeline (Phase 2).  The rotation
engine accesses them via the optional ``indicator_data`` parameter
on run_rotation().

When indicator data is unavailable (e.g. the bottom-up pipeline
hasn't run, or a ticker wasn't in the scoring universe), all
filters degrade gracefully — the gate passes by default and quality
returns 0.5 (neutral), so ranking falls back to RS-only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class QualityConfig:
    """
    Quality filter configuration for rotation stock selection.

    Split into three sections:
      1. Gate thresholds   — hard pass/fail, all must be True
      2. Blending weights  — how much RS vs quality matters
      3. Sub-component wts — within the quality score itself
    """

    # ── Master switch ──────────────────────────────────────
    enabled: bool = True

    # ── Gate thresholds (hard pass / fail) ─────────────────
    gate_price_above_sma: bool = True     # close > sma_50
    gate_ema_above_sma: bool = True       # ema_30 > sma_50
    gate_rsi_min: float = 30.0
    gate_rsi_max: float = 75.0
    gate_adx_min: float = 18.0

    # When True, a ticker that fails the gate is excluded.
    # When False, the gate result is recorded but the ticker
    # still participates in ranking (quality score penalises).
    gate_required: bool = True

    # ── Blending weights (RS vs quality) ───────────────────
    # These control the final ranked score:
    #   blended = w_rs × sigmoid(RS) + w_quality × quality
    w_rs: float = 0.60
    w_quality: float = 0.40

    # Sigmoid scale factor for RS normalisation.
    # Higher = more spread.  With 10.0:
    #   RS +0.10 → 0.73,  RS 0 → 0.50,  RS -0.10 → 0.27
    rs_sigmoid_scale: float = 10.0

    # ── Quality sub-component weights (sum to 1.0) ─────────
    w_ma_position: float = 0.25       # MA alignment & distance
    w_rsi_zone: float = 0.20          # RSI sweet-spot
    w_volume: float = 0.20            # Volume + OBV confirmation
    w_macd: float = 0.15              # MACD histogram state
    w_adx_direction: float = 0.10     # ADX strength + DI direction
    w_volatility: float = 0.10        # ATR% regime (moderate best)


# ═══════════════════════════════════════════════════════════════
#  GATE RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class GateResult:
    """Outcome of the quality gate check for one ticker."""

    passed: bool
    gates: dict[str, bool] = field(default_factory=dict)

    @property
    def failed_gates(self) -> list[str]:
        """Names of gates that did not pass."""
        return [k for k, v in self.gates.items() if not v]

    @property
    def n_passed(self) -> int:
        return sum(1 for v in self.gates.values() if v)

    @property
    def n_total(self) -> int:
        return len(self.gates)

    def summary(self) -> str:
        if self.passed:
            return f"PASS ({self.n_passed}/{self.n_total})"
        return (
            f"FAIL ({self.n_passed}/{self.n_total}): "
            f"{', '.join(self.failed_gates)}"
        )


# ═══════════════════════════════════════════════════════════════
#  VALUE EXTRACTION HELPER
# ═══════════════════════════════════════════════════════════════

def _safe(row: pd.Series, col: str, default: float = 0.0) -> float:
    """
    Extract a float from a Series row, returning *default* on
    any failure (missing key, None, NaN, non-numeric).
    """
    val = row.get(col)
    if val is None:
        return default
    try:
        fval = float(val)
        return default if np.isnan(fval) else fval
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════
#  QUALITY GATE
# ═══════════════════════════════════════════════════════════════

def quality_gate(
    df: pd.DataFrame,
    config: QualityConfig | None = None,
) -> GateResult:
    """
    Hard pass/fail checks on the latest row of an indicator
    DataFrame.

    All enabled gates must pass for the ticker to be eligible
    as a rotation BUY (when ``config.gate_required`` is True).

    Gates
    ─────
    1. price_above_sma50  — close > 50-day SMA
       Why: confirms the stock is in a structural uptrend.
       A stock below its 50 SMA is in a downtrend regardless
       of sector strength.

    2. ema_above_sma      — 30 EMA > 50 SMA
       Why: confirms bullish moving-average alignment.  When
       the fast MA is below the slow MA, momentum has already
       broken down even if RS is still positive from earlier
       performance.

    3. rsi_in_range       — RSI between 30 and 75
       Why: RSI < 30 means collapse in progress (not momentum).
       RSI > 75 means overbought and at risk of mean reversion.
       The quality score handles fine-grained RSI positioning.

    4. adx_above_min      — ADX ≥ 18
       Why: ADX below ~18 means no trend — the market is
       choppy and RS readings are noise, not signal.

    Parameters
    ----------
    df : pd.DataFrame
        Indicator-enriched DataFrame for one ticker (output of
        ``compute_all_indicators()``).  Uses the latest row.
    config : QualityConfig, optional

    Returns
    -------
    GateResult
        .passed  — True if all enabled gates are True
        .gates   — dict of {gate_name: bool} for diagnostics
    """
    if config is None:
        config = QualityConfig()

    if df is None or df.empty:
        return GateResult(passed=False, gates={"data_available": False})

    last = df.iloc[-1]
    gates: dict[str, bool] = {}

    # ── Gate 1: Price above 50 SMA ────────────────────────
    if config.gate_price_above_sma:
        close = _safe(last, "close", 0.0)
        sma50 = _safe(last, "sma_50", np.nan)
        if np.isnan(sma50) or sma50 == 0:
            # No SMA data — pass by default (warmup period)
            gates["price_above_sma50"] = True
        else:
            gates["price_above_sma50"] = close > sma50

    # ── Gate 2: Short EMA > Long SMA (bullish alignment) ──
    if config.gate_ema_above_sma:
        ema = _safe(last, "ema_30", np.nan)
        sma = _safe(last, "sma_50", np.nan)
        if np.isnan(ema) or np.isnan(sma):
            gates["ema_above_sma"] = True
        else:
            gates["ema_above_sma"] = ema > sma

    # ── Gate 3: RSI in range ──────────────────────────────
    rsi = _safe(last, "rsi_14", 50.0)
    gates["rsi_in_range"] = config.gate_rsi_min <= rsi <= config.gate_rsi_max

    # ── Gate 4: ADX minimum trend strength ────────────────
    adx = _safe(last, "adx_14", 0.0)
    gates["adx_above_min"] = adx >= config.gate_adx_min

    passed = all(gates.values())
    return GateResult(passed=passed, gates=gates)


# ═══════════════════════════════════════════════════════════════
#  QUALITY SCORE
# ═══════════════════════════════════════════════════════════════

def quality_score(
    df: pd.DataFrame,
    config: QualityConfig | None = None,
) -> float:
    """
    Compute a 0–1 quality score from the latest row of a
    ticker's indicator DataFrame.

    Six sub-components are weighted and summed:

      1. MA positioning   (0.25) — trend structure health
      2. RSI zone         (0.20) — momentum sweet-spot
      3. Volume profile   (0.20) — institutional participation
      4. MACD state       (0.15) — momentum direction & strength
      5. ADX / direction  (0.10) — trend strength + bullish bias
      6. Volatility       (0.10) — risk regime (moderate best)

    Returns 0.5 (neutral) when input data is unavailable.
    """
    if config is None:
        config = QualityConfig()

    if df is None or df.empty:
        return 0.5

    last = df.iloc[-1]

    ma_sc   = _score_ma_position(last)
    rsi_sc  = _score_rsi_zone(last)
    vol_sc  = _score_volume(last)
    macd_sc = _score_macd(last)
    adx_sc  = _score_adx_direction(last)
    atr_sc  = _score_volatility(last)

    composite = (
        config.w_ma_position    * ma_sc
        + config.w_rsi_zone     * rsi_sc
        + config.w_volume       * vol_sc
        + config.w_macd         * macd_sc
        + config.w_adx_direction * adx_sc
        + config.w_volatility   * atr_sc
    )

    return float(np.clip(composite, 0.0, 1.0))


# ═══════════════════════════════════════════════════════════════
#  BLENDING
# ═══════════════════════════════════════════════════════════════

def blend_rs_quality(
    rs_score: float,
    quality: float,
    config: QualityConfig | None = None,
) -> float:
    """
    Blend composite RS with quality score for final ranking.

    RS is typically in the range [-0.2, +0.2] while quality
    is [0, 1].  We normalise RS to [0, 1] via a sigmoid so
    the two scales are comparable before weighting.

    The sigmoid scale factor controls discrimination:
      scale=10  →  RS ±0.10 maps to [0.27, 0.73]
      scale=15  →  RS ±0.10 maps to [0.18, 0.82]
      scale=5   →  RS ±0.10 maps to [0.38, 0.62]

    When all candidates have similar RS the sigmoid compresses
    them and quality differentiates — which is exactly what we
    want (the sector tide lifts all boats equally, so pick the
    mechanically soundest boat).

    Parameters
    ----------
    rs_score : float
        Raw composite RS vs benchmark (e.g. +0.05).
    quality : float
        Quality score from ``quality_score()`` (0–1).
    config : QualityConfig

    Returns
    -------
    float — blended ranking score (higher = better).
    """
    if config is None:
        config = QualityConfig()

    scale = config.rs_sigmoid_scale
    rs_norm = 1.0 / (1.0 + np.exp(-scale * rs_score))

    blended = config.w_rs * rs_norm + config.w_quality * quality
    return float(blended)


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: MA POSITIONING
# ═══════════════════════════════════════════════════════════════

def _score_ma_position(last: pd.Series) -> float:
    """
    Score trend structure from moving-average positioning.

    Best:  Price 0–5 % above EMA — near dynamic support, ideal
           pullback entry within a trend.
    Good:  5–10 % above — actively trending, not yet extended.
    Weak:  >15 % above — extended, risk of snapback.
    Weak:  Below EMA — trend weakening.

    MA alignment bonus: EMA_30 > SMA_50 adds +0.15 because
    bullish MA crossover confirms structural momentum.
    """
    close_vs_ema = _safe(last, "close_vs_ema_30_pct", 0.0)
    close_vs_sma = _safe(last, "close_vs_sma_50_pct", 0.0)

    # Distance from 30 EMA (% terms)
    ema_dist = float(np.interp(
        close_vs_ema,
        [-10, -3, 0, 3, 8, 15, 25],
        [0.05, 0.20, 0.70, 1.00, 0.80, 0.40, 0.10],
    ))

    # Distance from 50 SMA (% terms)
    sma_dist = float(np.interp(
        close_vs_sma,
        [-10, -2, 0, 5, 12, 20, 30],
        [0.05, 0.15, 0.60, 1.00, 0.70, 0.30, 0.10],
    ))

    # Bullish MA alignment bonus
    ema = _safe(last, "ema_30", 0.0)
    sma = _safe(last, "sma_50", 0.0)
    alignment = 0.15 if (ema > 0 and sma > 0 and ema > sma) else 0.0

    score = 0.50 * ema_dist + 0.35 * sma_dist + alignment
    return float(np.clip(score, 0.0, 1.0))


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: RSI ZONE
# ═══════════════════════════════════════════════════════════════

def _score_rsi_zone(last: pd.Series) -> float:
    """
    RSI sweet-spot scoring.

    The rotation strategy follows momentum, so the ideal RSI
    zone is 45–60: the stock is trending but not overbought.

      0–25   : 0.05  — collapsing, no momentum
      25–35  : ramp up to 0.50  — oversold, building
      35–45  : ramp to 0.90  — momentum starting
      45–55  : 1.00  — ideal trending zone
      55–65  : 0.85  — still strong, getting warm
      65–75  : ramp down to 0.50  — overbought risk
      75–100 : 0.05–0.15  — extreme, likely to revert
    """
    rsi = _safe(last, "rsi_14", 50.0)

    return float(np.interp(
        rsi,
        [0, 25, 35, 45, 55, 65, 75, 85, 100],
        [0.05, 0.10, 0.50, 0.90, 1.00, 0.85, 0.50, 0.15, 0.05],
    ))


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: VOLUME PROFILE
# ═══════════════════════════════════════════════════════════════

def _score_volume(last: pd.Series) -> float:
    """
    Volume profile scoring.

    Combines relative volume (vs 20-day average) and OBV slope.

    Relative volume 1.2–2.5 with positive OBV slope =
    institutional buying — the highest score.

    Rel vol < 0.5 = no interest (low-liquidity drift trap).
    Rel vol > 5.0 = panic / event-driven (unstable).
    Negative OBV slope = distribution (selling pressure).
    """
    rel_vol = _safe(last, "relative_volume", 1.0)
    obv_slope = _safe(last, "obv_slope_10d", 0.0)

    # Relative volume score (piecewise)
    vol_sc = float(np.interp(
        rel_vol,
        [0.0, 0.4, 0.8, 1.2, 2.0, 3.5, 6.0, 10.0],
        [0.05, 0.15, 0.40, 0.80, 1.00, 0.70, 0.40, 0.20],
    ))

    # OBV slope direction — positive = accumulation
    if obv_slope > 0:
        obv_sc = 0.80
    elif obv_slope == 0:
        obv_sc = 0.50
    else:
        obv_sc = 0.20

    return float(0.65 * vol_sc + 0.35 * obv_sc)


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: MACD STATE
# ═══════════════════════════════════════════════════════════════

def _score_macd(last: pd.Series) -> float:
    """
    MACD state scoring.

    Three signals are checked and combined:

    1. Histogram positive   (+0.35) — current momentum is bullish
    2. Line above signal    (+0.30) — bullish crossover intact
    3. Histogram strength   (+0.20) — magnitude relative to line

    Base score is 0.15 so even a fully bearish MACD doesn't
    score zero (other components may still justify the pick).
    """
    hist = _safe(last, "macd_hist", 0.0)
    line = _safe(last, "macd_line", 0.0)
    signal = _safe(last, "macd_signal", 0.0)

    score = 0.15  # base

    if hist > 0:
        score += 0.35

    if line > signal:
        score += 0.30

    # Histogram strength (relative to MACD line magnitude)
    if line != 0:
        hist_strength = abs(hist / line)
        score += 0.20 * min(hist_strength, 1.0)
    elif hist > 0:
        score += 0.10

    return float(np.clip(score, 0.0, 1.0))


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: ADX + DIRECTIONAL
# ═══════════════════════════════════════════════════════════════

def _score_adx_direction(last: pd.Series) -> float:
    """
    ADX trend strength + directional bias.

    ADX 25–40 with +DI > −DI = strong bullish trend (peak score).

    ADX < 15: no trend (RS is noise).
    ADX > 50: extreme trend (possible exhaustion or blow-off).

    +DI / −DI ratio determines bullish vs bearish directional
    bias within the trend.
    """
    adx = _safe(last, "adx_14", 15.0)
    plus_di = _safe(last, "plus_di", 0.0)
    minus_di = _safe(last, "minus_di", 0.0)

    # ADX value score (piecewise)
    adx_sc = float(np.interp(
        adx,
        [0, 12, 18, 25, 35, 50, 70],
        [0.05, 0.15, 0.50, 0.90, 1.00, 0.70, 0.40],
    ))

    # Directional bias: +DI share of total DI
    di_total = plus_di + minus_di
    if di_total > 0:
        di_ratio = plus_di / di_total  # 0.5 = neutral, >0.5 = bullish
        dir_sc = float(np.interp(
            di_ratio,
            [0.0, 0.30, 0.50, 0.65, 0.80, 1.0],
            [0.00, 0.15, 0.50, 0.80, 1.00, 1.00],
        ))
    else:
        dir_sc = 0.50

    return float(0.55 * adx_sc + 0.45 * dir_sc)


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: VOLATILITY
# ═══════════════════════════════════════════════════════════════

def _score_volatility(last: pd.Series) -> float:
    """
    Volatility regime scoring (tent function — moderate is best).

    Uses ATR as a percentage of price for cross-asset comparison.

    ATR% 1.5–3.0 %: healthy trending volatility.
    ATR% < 0.5 %:   dead money / no movement.
    ATR% > 8 %:     whipsaw territory, stops get hit.
    """
    atr_pct = _safe(last, "atr_14_pct", 2.0)

    return float(np.interp(
        atr_pct,
        [0.0, 0.3, 0.8, 1.5, 3.0, 5.0, 8.0, 15.0],
        [0.10, 0.25, 0.60, 0.90, 1.00, 0.60, 0.30, 0.10],
    ))


# ═══════════════════════════════════════════════════════════════
#  DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════

def quality_diagnostics(
    df: pd.DataFrame,
    config: QualityConfig | None = None,
) -> dict:
    """
    Full diagnostic breakdown for a single ticker.

    Returns a dict with gate results, sub-scores, the final
    quality score, and the raw indicator values that drove each
    decision.  Useful for debugging and reports.
    """
    if config is None:
        config = QualityConfig()

    gate = quality_gate(df, config)
    score = quality_score(df, config)

    if df is not None and not df.empty:
        last = df.iloc[-1]
        sub = {
            "ma_position":   round(_score_ma_position(last), 4),
            "rsi_zone":      round(_score_rsi_zone(last), 4),
            "volume":        round(_score_volume(last), 4),
            "macd":          round(_score_macd(last), 4),
            "adx_direction": round(_score_adx_direction(last), 4),
            "volatility":    round(_score_volatility(last), 4),
        }
        vals = {
            "close":             _safe(last, "close"),
            "ema_30":            _safe(last, "ema_30"),
            "sma_50":            _safe(last, "sma_50"),
            "close_vs_ema_pct":  _safe(last, "close_vs_ema_30_pct"),
            "close_vs_sma_pct":  _safe(last, "close_vs_sma_50_pct"),
            "rsi_14":            _safe(last, "rsi_14"),
            "adx_14":            _safe(last, "adx_14"),
            "plus_di":           _safe(last, "plus_di"),
            "minus_di":          _safe(last, "minus_di"),
            "macd_hist":         _safe(last, "macd_hist"),
            "macd_line":         _safe(last, "macd_line"),
            "relative_volume":   _safe(last, "relative_volume"),
            "obv_slope_10d":     _safe(last, "obv_slope_10d"),
            "atr_14_pct":        _safe(last, "atr_14_pct"),
        }
    else:
        sub = {}
        vals = {}

    return {
        "gate_passed":   gate.passed,
        "gate_summary":  gate.summary(),
        "gates":         gate.gates,
        "failed_gates":  gate.failed_gates,
        "quality_score": round(score, 4),
        "sub_scores":    sub,
        "key_values":    vals,
    }