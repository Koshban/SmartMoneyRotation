from __future__ import annotations
import numpy as np
import pandas as pd
from refactor.common.config_refactor import SCORINGWEIGHTS_V2, SCORINGPARAMS_V2

def _s(x, lo, hi): return pd.Series(np.clip((x-lo)/(hi-lo), 0.0, 1.0), index=x.index)
def _inv(x, lo, hi): return 1.0 - _s(x, lo, hi)

def compute_composite_v2(
    df: pd.DataFrame,
    weights=None,
    params=None,
) -> pd.DataFrame:
    p = params if params is not None else SCORINGPARAMS_V2
    w = weights if weights is not None else SCORINGWEIGHTS_V2
    out = df.copy()

    # ── Trend ─────────────────────────────────────────────────────────────
    stock_rs = _s(out["rszscore"].fillna(0), -1.0, 2.0)
    sector_rs = _s(
        out.get("sectrszscore", pd.Series(0, index=out.index)).fillna(0),
        -1.0, 2.0,
    )
    rs_accel = _s(
        out.get("rsaccel20", pd.Series(0, index=out.index)).fillna(0),
        -0.10, 0.15,
    )
    trend_confirm = _s(
        out.get("closevsema30pct", pd.Series(0, index=out.index)).fillna(0),
        -0.03, 0.10,
    )
    out["scoretrend"] = (
        p["trend"]["w_stock_rs"] * stock_rs
        + p["trend"]["w_sector_rs"] * sector_rs
        + p["trend"]["w_rs_accel"] * rs_accel
        + p["trend"]["w_trend_confirm"] * trend_confirm
    ).clip(0, 1)

    # ── Participation ─────────────────────────────────────────────────────
    rvol = _s(
        out.get("relativevolume", pd.Series(1, index=out.index)).fillna(1),
        0.8, 2.2,
    )
    obv = _s(
        out.get("obvslope10d", pd.Series(0, index=out.index)).fillna(0),
        -0.05, 0.12,
    )
    adl = _s(
        out.get("adlineslope10d", pd.Series(0, index=out.index)).fillna(0),
        -0.05, 0.12,
    )
    dvol = _s(
        np.log1p(
            out.get("dollarvolume20d", pd.Series(0, index=out.index)).fillna(0)
        ),
        10, 18,
    )
    out["scoreparticipation"] = (
        p["participation"]["w_rvol"] * rvol
        + p["participation"]["w_obv"] * obv
        + p["participation"]["w_adline"] * adl
        + p["participation"]["w_dollar_volume"] * dvol
    ).clip(0, 1)

    # ── Risk ──────────────────────────────────────────────────────────────
    atrp = out.get("atr14pct", pd.Series(0, index=out.index)).fillna(0)
    illiq = out.get("amihud20", pd.Series(0, index=out.index)).fillna(0)
    gap = out.get("gaprate20", pd.Series(0, index=out.index)).fillna(0)
    extension = (
        out.get("closevssma50pct", pd.Series(0, index=out.index))
        .fillna(0).abs()
    )
    vol_pen = _inv(atrp, 0.02, p["penalties"]["atrp_high"])
    liq_pen = _inv(illiq, 0.0, p["penalties"]["illiquidity_bad"])
    gap_pen = _inv(gap, 0.05, 0.30)
    ext_pen = 1.0 - pd.Series(
        np.select(
            [
                extension >= p["penalties"]["extension_bad"],
                extension >= p["penalties"]["extension_warn"],
            ],
            [1.0, 0.5],
            default=0.0,
        ),
        index=out.index,
    )
    out["scorerisk"] = (
        p["risk"]["w_vol_penalty"] * vol_pen
        + p["risk"]["w_liquidity_penalty"] * liq_pen
        + p["risk"]["w_gap_penalty"] * gap_pen
        + p["risk"]["w_extension_penalty"] * ext_pen
    ).clip(0, 1)

    # ── Regime ────────────────────────────────────────────────────────────
    breadth = out.get("breadthscore", pd.Series(0.5, index=out.index)).fillna(0.5)
    volreg = out.get("volregimescore", pd.Series(0.0, index=out.index)).fillna(0.0)
    out["scoreregime"] = (
        p["regime"]["w_breadth"] * breadth
        + p["regime"]["w_vol_regime"] * (1.0 - volreg)
    ).clip(0, 1)

    # ── Rotation (NEW) ───────────────────────────────────────────────────
    # Maps the sector rotation quadrant to a 0-1 score.
    # "leading" sectors get the full bonus; "lagging" gets zero.
    # "unknown" gets a modest 0.30 so unmapped names aren't hammered
    # but don't benefit from rotation either.
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
            [1.0, 0.65, 0.35, 0.0],
            default=0.30,
        ),
        index=out.index,
    )

    # ── Composite ─────────────────────────────────────────────────────────
    rotation_weight = w.get("rotation", 0.0)
    if rotation_weight == 0.0:
        logger.warning(
            "compute_composite_v2: rotation weight is 0.0 — "
            "scorerotation will not affect composite.  "
            "Add 'rotation' to SCORINGWEIGHTS_V2."
        )

    composite = (
        w["trend"] * out["scoretrend"]
        + w["participation"] * out["scoreparticipation"]
        + w["risk"] * out["scorerisk"]
        + w["regime"] * out["scoreregime"]
        + rotation_weight * out["scorerotation"]
    )

    # ── Penalties ─────────────────────────────────────────────────────────
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

    return out
