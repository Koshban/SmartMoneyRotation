"""
refactor/strategy/scoring_v2.py

Composite scoring for the V2 pipeline.

Revision notes
--------------
- **UNIT FIX**: closevsema30pct and closevssma50pct are in PERCENTAGE-POINT
  units (e.g., 5.0 = "5% above the moving average"). Scaling ranges corrected.
- **Risk differentiation**: adaptive proxies for missing columns.
- **Participation weight redistribution**: dead columns have weight
  redistributed to live components.
- **Regime differentiation**: since scoreregime is constant across all stocks,
  its weight is reduced and redistributed to differentiating components.
- **RS momentum**: rs_accel scaling widened and weighted higher.
- **Score floor warning**: diagnostic emits warning when composite std < 0.08.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from refactor.common.config_refactor import SCORINGWEIGHTS_V2, SCORINGPARAMS_V2

logger = logging.getLogger(__name__)

# Below this std, fixed-threshold scaling is considered "flat" and
# the adaptive helpers fall back to cross-sectional percentile rank.
_ADAPTIVE_MIN_STD = 0.02

# Minimum acceptable composite std — below this we log a warning
_MIN_COMPOSITE_STD = 0.06


# ── Utility helpers ──────────────────────────────────────────────────────

def _s(x, lo, hi):
    """Scale *x* linearly from *lo* → 0 to *hi* → 1, clamped [0, 1]."""
    return pd.Series(np.clip((x - lo) / (hi - lo), 0.0, 1.0), index=x.index)


def _inv(x, lo, hi):
    """Inverse of :func:`_s`: *lo* → 1 (best) to *hi* → 0 (worst)."""
    return 1.0 - _s(x, lo, hi)


def _col_or_default(df: pd.DataFrame, name: str, default: float = 0.0):
    """Return ``(series, has_variance: bool)`` for column *name*."""
    if name in df.columns:
        s = df[name].fillna(default)
        return s, float(s.std()) > 1e-10
    return pd.Series(default, index=df.index, dtype=float), False


def _adaptive_inv(x: pd.Series, lo: float, hi: float, *, label: str = ""):
    """
    Inverse-scale with percentile-rank fallback.

    Primary path: ``1 − clip((x−lo)/(hi−lo), 0, 1)`` (lower x → higher score).
    Fallback: if the fixed mapping has std < ``_ADAPTIVE_MIN_STD``, use
    ``1 − rank(pct=True)`` so the component still differentiates stocks.
    """
    fixed = _inv(x, lo, hi)
    if float(fixed.std()) < _ADAPTIVE_MIN_STD and len(x) > 10:
        ranked = 1.0 - x.rank(pct=True).fillna(0.5)
        if label:
            logger.debug(
                "_adaptive_inv(%s): fixed std=%.4f < %.4f → rank fallback",
                label, float(fixed.std()), _ADAPTIVE_MIN_STD,
            )
        return ranked
    return fixed


def _adaptive_fwd(x: pd.Series, lo: float, hi: float, *, label: str = ""):
    """
    Forward-scale with percentile-rank fallback.

    Primary path: ``clip((x−lo)/(hi−lo), 0, 1)`` (higher x → higher score).
    Fallback: if the fixed mapping has std < ``_ADAPTIVE_MIN_STD``, use
    ``rank(pct=True)`` for differentiation.
    """
    fixed = _s(x, lo, hi)
    if float(fixed.std()) < _ADAPTIVE_MIN_STD and len(x) > 10:
        ranked = x.rank(pct=True).fillna(0.5)
        if label:
            logger.debug(
                "_adaptive_fwd(%s): fixed std=%.4f < %.4f → rank fallback",
                label, float(fixed.std()), _ADAPTIVE_MIN_STD,
            )
        return ranked
    return fixed


# ── Main scoring function ────────────────────────────────────────────────

def compute_composite_v2(
    df: pd.DataFrame,
    weights=None,
    params=None,
    *,
    market_breadth_score: float | None = None,
    market_vol_regime_score: float | None = None,
) -> pd.DataFrame:
    """
    Compute the V2 composite score for every row (stock) in *df*.

    Parameters
    ----------
    df : DataFrame
        Per-stock indicators and RS data.
    weights, params : dict, optional
        Override default scoring weights / params.
    market_breadth_score : float, optional
        Market-wide breadth score (0–1).  The breadth pipeline produces
        a single scalar per date — this is where the runner should pass
        it so that scoreregime actually differentiates from 0.5.
        If None, falls back to the per-row ``breadthscore`` column
        (which is almost always the ensure_columns default of 0.5).
    market_vol_regime_score : float, optional
        Market-wide volatility-regime score (0–1, higher = more volatile).
        If None, falls back to the per-row ``volregimescore`` column.
    """
    p = params if params is not None else SCORINGPARAMS_V2
    w = weights if weights is not None else SCORINGWEIGHTS_V2
    out = df.copy()

    # ══════════════════════════════════════════════════════════════════════
    #  TREND COMPONENT
    # ══════════════════════════════════════════════════════════════════════
    #
    # All sub-components should produce meaningful spread across the
    # universe.  closevsema30pct is in PERCENTAGE-POINT units:
    #   5.0 = stock is 5% above EMA30
    #  -3.0 = stock is 3% below EMA30
    #
    # rszscore is a standard z-score (mean ~0, std ~1).

    stock_rs = _s(out["rszscore"].fillna(0), -1.5, 2.5)
    sector_rs = _s(
        out.get("sectrszscore", pd.Series(0, index=out.index)).fillna(0),
        -1.5, 2.5,
    )
    rs_accel = _s(
        out.get("rsaccel20", pd.Series(0, index=out.index)).fillna(0),
        -0.10, 0.15,
    )

    # ── FIXED: closevsema30pct in PERCENTAGE-POINT units ──────────────
    # Old (broken): _s(x, -0.03, 0.10) → everything above 0.1% clips to 1.0
    # New (correct): _s(x, -3.0, 8.0) → smooth differentiation from
    #   "3% below EMA" (score=0) to "8% above EMA" (score=1).
    #   A stock exactly at EMA30 (x=0) → score = 3.0/11.0 ≈ 0.27
    #   A stock 3% above EMA (x=3) → score = 6.0/11.0 ≈ 0.55
    #   A stock 6% above EMA (x=6) → score = 9.0/11.0 ≈ 0.82
    trend_confirm = _adaptive_fwd(
        out.get("closevsema30pct", pd.Series(0, index=out.index)).fillna(0),
        -3.0, 8.0,
        label="trend_confirm",
    )

    out["scoretrend"] = (
        p["trend"]["w_stock_rs"] * stock_rs
        + p["trend"]["w_sector_rs"] * sector_rs
        + p["trend"]["w_rs_accel"] * rs_accel
        + p["trend"]["w_trend_confirm"] * trend_confirm
    ).clip(0, 1)

    # Trend diagnostics
    _st = out["scoretrend"]
    logger.info(
        "scoretrend: min=%.4f p10=%.4f med=%.4f p90=%.4f max=%.4f std=%.4f | "
        "sub-components: stock_rs std=%.4f sector_rs std=%.4f "
        "rs_accel std=%.4f trend_confirm std=%.4f",
        _st.min(), float(_st.quantile(0.10)), _st.median(),
        float(_st.quantile(0.90)), _st.max(), float(_st.std()),
        float(stock_rs.std()), float(sector_rs.std()),
        float(rs_accel.std()), float(trend_confirm.std()),
    )
    if float(trend_confirm.std()) < _ADAPTIVE_MIN_STD:
        logger.warning(
            "trend_confirm has near-zero std (%.4f) — closevsema30pct may "
            "not be populating. Check upstream data.",
            float(trend_confirm.std()),
        )

    # ══════════════════════════════════════════════════════════════════════
    #  PARTICIPATION COMPONENT (adaptive, with weight redistribution)
    # ══════════════════════════════════════════════════════════════════════
    #
    # When obvslope10d and/or adlineslope10d have zero variance (all-zero
    # because the data source doesn't populate them), their weights are
    # dead — every stock gets the same constant contribution (~0.294).
    # This collapses scoreparticipation differentiation to just rvol and
    # dvol, producing the 0.297-cluster seen in diagnostics.
    #
    # Fix: detect dead components, redistribute their weight proportionally
    # to the live components (rvol, dvol), and use _adaptive_fwd for the
    # live components so rank-based fallback is available if needed.

    rvol_raw = out.get(
        "relativevolume", pd.Series(1, index=out.index),
    ).fillna(1)

    obv_raw, obv_col_ok = _col_or_default(out, "obvslope10d", 0.0)
    adl_raw, adl_col_ok = _col_or_default(out, "adlineslope10d", 0.0)

    dvol_raw_input = out.get(
        "dollarvolume20d", pd.Series(0, index=out.index),
    ).fillna(0)
    dvol_raw = pd.Series(np.log1p(dvol_raw_input), index=out.index)

    # Check if OBV and ADL have meaningful variance
    obv_has_variance = obv_col_ok and float(obv_raw.std()) > 1e-8
    adl_has_variance = adl_col_ok and float(adl_raw.std()) > 1e-8

    # Base weights from params
    w_rvol = p["participation"]["w_rvol"]
    w_obv = p["participation"]["w_obv"]
    w_adl = p["participation"]["w_adline"]
    w_dvol = p["participation"]["w_dollar_volume"]

    # Redistribute dead weights to live components
    dead_weight = 0.0
    _part_sources = []

    if obv_has_variance:
        _part_sources.append("obvslope10d")
    else:
        dead_weight += w_obv
        w_obv = 0.0

    if adl_has_variance:
        _part_sources.append("adlineslope10d")
    else:
        dead_weight += w_adl
        w_adl = 0.0

    _part_sources.extend(["relativevolume", "dollarvolume20d"])

    if dead_weight > 0:
        live_total = w_rvol + w_dvol
        if live_total > 0:
            w_rvol += dead_weight * (w_rvol / live_total)
            w_dvol += dead_weight * (w_dvol / live_total)
        else:
            # Edge case: all components dead — split evenly
            w_rvol = 0.5
            w_dvol = 0.5
        logger.info(
            "scoreparticipation: redistributed %.3f dead weight "
            "(obv_ok=%s adl_ok=%s) → w_rvol=%.3f w_obv=%.3f "
            "w_adl=%.3f w_dvol=%.3f",
            dead_weight, obv_has_variance, adl_has_variance,
            w_rvol, w_obv, w_adl, w_dvol,
        )

    # Scale live components (adaptive for rvol/dvol, fixed for obv/adl)
    rvol = _adaptive_fwd(rvol_raw, 0.8, 2.2, label="rvol")
    dvol = _adaptive_fwd(dvol_raw, 10, 18, label="dvol")
    obv = (
        _s(obv_raw, -0.05, 0.12)
        if obv_has_variance
        else pd.Series(0.0, index=out.index)
    )
    adl = (
        _s(adl_raw, -0.05, 0.12)
        if adl_has_variance
        else pd.Series(0.0, index=out.index)
    )

    out["scoreparticipation"] = (
        w_rvol * rvol
        + w_obv * obv
        + w_adl * adl
        + w_dvol * dvol
    ).clip(0, 1)

    # Participation diagnostics
    _sp = out["scoreparticipation"]
    logger.info(
        "scoreparticipation: sources=%s  min=%.4f med=%.4f max=%.4f "
        "std=%.4f uniq=%d  weights=[rvol=%.3f obv=%.3f adl=%.3f dvol=%.3f]",
        _part_sources,
        _sp.min(), _sp.median(), _sp.max(),
        float(_sp.std()), int(_sp.nunique()),
        w_rvol, w_obv, w_adl, w_dvol,
    )

    # ══════════════════════════════════════════════════════════════════════
    #  RISK COMPONENT (adaptive, with proxy fallbacks)
    # ══════════════════════════════════════════════════════════════════════
    _risk_sources = []

    # — Sub-component 1: Volatility —
    atrp, atrp_ok = _col_or_default(out, "atr14pct", 0.0)
    if atrp_ok:
        vol_pen = _adaptive_inv(
            atrp, 0.01, p["penalties"]["atrp_high"], label="atrp",
        )
        _risk_sources.append("atr14pct")
    else:
        # Proxy: higher relative volume ≈ higher short-term risk
        _rvol_raw = out.get(
            "relativevolume", pd.Series(1.0, index=out.index),
        ).fillna(1.0)
        vol_pen = _adaptive_inv(_rvol_raw, 0.5, 2.5, label="rvol_proxy")
        _risk_sources.append("relativevolume(proxy)")

    # — Sub-component 2: Liquidity —
    illiq, illiq_ok = _col_or_default(out, "amihud20", 0.0)
    if illiq_ok:
        liq_pen = _adaptive_inv(
            illiq, 0.0, p["penalties"]["illiquidity_bad"], label="illiq",
        )
        _risk_sources.append("amihud20")
    else:
        # Proxy: higher dollar volume → more liquid → safer
        _dv = out.get(
            "dollarvolume20d", pd.Series(0, index=out.index),
        ).fillna(0)
        _log_dv = pd.Series(np.log1p(_dv), index=out.index)
        liq_pen = _adaptive_fwd(_log_dv, 12.0, 17.0, label="dvol_proxy")
        _risk_sources.append("dollarvolume20d(proxy)")

    # — Sub-component 3: Gap / tail risk —
    gap, gap_ok = _col_or_default(out, "gaprate20", 0.0)
    if gap_ok:
        gap_pen = _adaptive_inv(gap, 0.05, 0.30, label="gap")
        _risk_sources.append("gaprate20")
    else:
        # Proxy: RSI distance from neutral 50 (extreme readings ≈ tail risk)
        _rsi = out.get("rsi14", pd.Series(50, index=out.index)).fillna(50)
        _rsi_dist = (_rsi - 50).abs()
        gap_pen = _adaptive_inv(_rsi_dist, 5.0, 35.0, label="rsi_dist_proxy")
        _risk_sources.append("rsi_dist(proxy)")

    # — Sub-component 4: Extension from moving average —
    #   FIXED: closevssma50pct is in PERCENTAGE-POINT units, same as
    #   closevsema30pct.  e.g., 8.0 = "8% above SMA50".
    #
    #   Old (broken): ext_warn=0.08, ext_bad=0.20 (decimal, caught nothing)
    #   New (correct): warn at 8 pp, dangerous at 20 pp.
    ext_raw = (
        out.get("closevssma50pct", pd.Series(0, index=out.index))
        .fillna(0).abs()
    )
    # Use percentage-point scaling: 4 pp = starts getting risky, 20 pp = very extended
    ext_warn_pct = p["penalties"].get("extension_warn_pct", 4.0)
    ext_bad_pct = p["penalties"].get("extension_bad_pct", 20.0)
    ext_pen = _adaptive_inv(
        ext_raw, ext_warn_pct, ext_bad_pct, label="extension",
    )
    _risk_sources.append("closevssma50pct")

    out["scorerisk"] = (
        p["risk"]["w_vol_penalty"] * vol_pen
        + p["risk"]["w_liquidity_penalty"] * liq_pen
        + p["risk"]["w_gap_penalty"] * gap_pen
        + p["risk"]["w_extension_penalty"] * ext_pen
    ).clip(0, 1)

    # Risk diagnostics
    _sr = out["scorerisk"]
    logger.info(
        "scorerisk: sources=%s  min=%.4f med=%.4f max=%.4f std=%.4f uniq=%d",
        _risk_sources, _sr.min(), _sr.median(), _sr.max(),
        float(_sr.std()), int(_sr.nunique()),
    )
    if float(_sr.std()) < _ADAPTIVE_MIN_STD:
        logger.warning(
            "scorerisk still has near-zero variance (std=%.4f) after "
            "adaptive fallbacks — check risk inputs: %s",
            float(_sr.std()), _risk_sources,
        )

    # ══════════════════════════════════════════════════════════════════════
    #  REGIME COMPONENT
    # ══════════════════════════════════════════════════════════════════════
    #
    # IMPORTANT: This component is a MARKET-LEVEL scalar (identical for
    # all stocks on a given day). It does NOT differentiate stocks cross-
    # sectionally. Its purpose is to raise/lower the entire composite on
    # days with favorable/unfavorable market conditions.
    #
    # Because it adds no cross-sectional spread, its weight should be
    # modest (10-15%). If it's weighted too heavily, it compresses the
    # differentiating components' contribution.

    if market_breadth_score is not None:
        breadth = pd.Series(
            float(market_breadth_score), index=out.index, dtype=float,
        )
        logger.info(
            "scoreregime: using market_breadth_score=%.4f (from kwarg)",
            market_breadth_score,
        )
    else:
        breadth = out.get(
            "breadthscore", pd.Series(0.5, index=out.index)
        ).fillna(0.5)
        if float(breadth.std()) < 1e-8:
            logger.warning(
                "scoreregime: breadthscore is constant (%.4f) — "
                "pass market_breadth_score kwarg to fix this",
                float(breadth.iloc[0]),
            )

    if market_vol_regime_score is not None:
        volreg = pd.Series(
            float(market_vol_regime_score), index=out.index, dtype=float,
        )
        logger.info(
            "scoreregime: using market_vol_regime_score=%.4f (from kwarg)",
            market_vol_regime_score,
        )
    else:
        volreg = out.get(
            "volregimescore", pd.Series(0.0, index=out.index)
        ).fillna(0.0)
        if float(volreg.std()) < 1e-8:
            logger.warning(
                "scoreregime: volregimescore is constant (%.4f) — "
                "pass market_vol_regime_score kwarg to fix this",
                float(volreg.iloc[0]),
            )

    # Dampened vol-favorability mapping
    #   vol_regime_score semantics: 0.0 = calm, ~0.5 = elevated, 1.0 = chaotic
    #   For long-biased strategies, lower vol is favorable — but we dampen
    #   the mapping so "calm" is favorable without being maximal:
    #
    #     0.10 (calm)     → 0.64  (favorable)
    #     0.35 (normal)   → 0.49  (neutral)
    #     0.60 (elevated) → 0.34  (mildly unfavorable)
    #     1.0  (chaotic)  → 0.10  (very unfavorable)
    vol_favorable = (0.70 - 0.60 * volreg).clip(0.10, 0.70)
    out["volfavorability"] = vol_favorable

    out["scoreregime"] = (
        p["regime"]["w_breadth"] * breadth
        + p["regime"]["w_vol_regime"] * vol_favorable
    ).clip(0, 1)

    logger.info(
        "scoreregime: breadth=%.4f volreg=%.4f vol_favorable=%.4f "
        "→ scoreregime=%.4f  (constant across all %d stocks)",
        float(breadth.iloc[0]),
        float(volreg.iloc[0]),
        float(vol_favorable.iloc[0]),
        float(out["scoreregime"].iloc[0]),
        len(out),
    )

    # Stamp actual market-level values onto per-row columns
    if market_breadth_score is not None:
        out["breadthscore"] = float(market_breadth_score)
    if market_vol_regime_score is not None:
        out["volregimescore"] = float(market_vol_regime_score)

    # ══════════════════════════════════════════════════════════════════════
    #  ROTATION COMPONENT
    # ══════════════════════════════════════════════════════════════════════
    sect_regime = (
        out.get("sectrsregime", pd.Series("unknown", index=out.index))
        .fillna("unknown")
        .astype(str)
        .str.lower()
        .str.strip()
    )
    out["scorerotation"] = pd.Series(
        np.select(
            [
                sect_regime == "leading",
                sect_regime == "improving",
                sect_regime == "weakening",
                sect_regime == "lagging",
            ],
            [1.0, 0.70, 0.40, 0.15],
            default=0.30,
        ),
        index=out.index,
    )

    # ══════════════════════════════════════════════════════════════════════
    #  COMPOSITE SCORE
    # ══════════════════════════════════════════════════════════════════════
    rotation_weight = w.get("rotation", 0.0)
    if rotation_weight == 0.0:
        logger.warning(
            "compute_composite_v2: rotation weight is 0.0 — "
            "scorerotation will not affect composite. "
            "Add 'rotation' to SCORINGWEIGHTS_V2."
        )

    # Compute effective weights for diagnostics
    total_weight = (
        w["trend"] + w["participation"] + w["risk"]
        + w["regime"] + rotation_weight
    )
    differentiating_weight = (
        w["trend"] + w["participation"] + w["risk"] + rotation_weight
    )
    regime_weight = w["regime"]

    logger.info(
        "Composite weights: trend=%.3f participation=%.3f risk=%.3f "
        "regime=%.3f rotation=%.3f | total=%.3f | "
        "differentiating=%.1f%% constant(regime)=%.1f%%",
        w["trend"], w["participation"], w["risk"],
        regime_weight, rotation_weight,
        total_weight,
        100 * differentiating_weight / max(total_weight, 1e-9),
        100 * regime_weight / max(total_weight, 1e-9),
    )

    composite = (
        w["trend"] * out["scoretrend"]
        + w["participation"] * out["scoreparticipation"]
        + w["risk"] * out["scorerisk"]
        + w["regime"] * out["scoreregime"]
        + rotation_weight * out["scorerotation"]
    )

    # ══════════════════════════════════════════════════════════════════════
    #  PENALTIES
    # ══════════════════════════════════════════════════════════════════════
    rsi = out.get("rsi14", pd.Series(50, index=out.index)).fillna(50)
    adx = out.get("adx14", pd.Series(20, index=out.index)).fillna(20)
    rsi_low = p["penalties"]["rsi_soft_low"]
    rsi_high = p["penalties"]["rsi_soft_high"]

    rsi_penalty = pd.Series(
        np.where(
            rsi < rsi_low,
            (rsi_low - rsi) / 30.0,
            np.where(rsi > rsi_high, (rsi - rsi_high) / 30.0, 0.0),
        ),
        index=out.index,
    ).clip(0, 0.15)
    adx_penalty = pd.Series(
        np.where(
            adx < p["penalties"]["adx_soft_min"],
            (p["penalties"]["adx_soft_min"] - adx) / 30.0,
            0.0,
        ),
        index=out.index,
    ).clip(0, 0.10)

    out["scorepenalty"] = (rsi_penalty + adx_penalty).clip(0, 0.20)
    out["scorecomposite_v2"] = (composite - out["scorepenalty"]).clip(0, 1)

    # ══════════════════════════════════════════════════════════════════════
    #  DIAGNOSTIC SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    _sc = out["scorecomposite_v2"]
    composite_std = float(_sc.std())
    composite_iqr = float(_sc.quantile(0.90) - _sc.quantile(0.10))

    logger.info(
        "scorecomposite_v2: min=%.4f p10=%.4f p25=%.4f med=%.4f "
        "p75=%.4f p90=%.4f max=%.4f | std=%.4f iqr_90_10=%.4f",
        _sc.min(),
        float(_sc.quantile(0.10)),
        float(_sc.quantile(0.25)),
        _sc.median(),
        float(_sc.quantile(0.75)),
        float(_sc.quantile(0.90)),
        _sc.max(),
        composite_std,
        composite_iqr,
    )

    if composite_std < _MIN_COMPOSITE_STD:
        logger.warning(
            "SCORE COMPRESSION: composite std=%.4f < %.4f — scores are "
            "too clustered for effective differentiation. The top-ranked "
            "stock (%.4f) is only %.4f above median (%.4f). This means "
            "entry/exit thresholds must be razor-thin or nothing qualifies. "
            "Check: (1) trend_confirm getting variance from closevsema30pct, "
            "(2) scoreparticipation not collapsing, (3) weights distribution.",
            composite_std, _MIN_COMPOSITE_STD,
            _sc.max(), _sc.max() - _sc.median(), _sc.median(),
        )
    elif composite_std < 0.10:
        logger.info(
            "Score spread is moderate (std=%.4f). Adequate for ranking "
            "but consider widening if entry signals are too few/many.",
            composite_std,
        )
    else:
        logger.info(
            "Score spread is healthy (std=%.4f). Good differentiation.",
            composite_std,
        )

    for col in (
        "scoretrend", "scoreparticipation", "scorerisk",
        "scoreregime", "scorerotation", "scorepenalty",
        "scorecomposite_v2",
    ):
        if col in out.columns:
            s = out[col]
            logger.debug(
                "  %s: mean=%.4f std=%.4f min=%.4f p10=%.4f med=%.4f "
                "p90=%.4f max=%.4f",
                col, s.mean(), s.std(), s.min(),
                float(s.quantile(0.10)), s.median(),
                float(s.quantile(0.90)), s.max(),
            )

    # Log the contribution of each component to composite spread
    if logger.isEnabledFor(logging.DEBUG):
        for col, weight in [
            ("scoretrend", w["trend"]),
            ("scoreparticipation", w["participation"]),
            ("scorerisk", w["risk"]),
            ("scoreregime", regime_weight),
            ("scorerotation", rotation_weight),
        ]:
            component_contribution = weight * float(out[col].std())
            logger.debug(
                "  %s: weight=%.3f × std=%.4f = contribution_to_spread=%.4f",
                col, weight, float(out[col].std()), component_contribution,
            )

    return out