""" refactor/strategy/signals_v2.py """
from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from refactor.common.config_refactor import CONVERGENCEPARAMS_V2, SIGNALPARAMS_V2
from .adapters_v2 import ensure_columns

logger = logging.getLogger(__name__)


def _log_bool_counts(out: pd.DataFrame, cols: list[str], prefix: str) -> None:
    vals = {c: int(out[c].sum()) if c in out.columns else 0 for c in cols}
    logger.info("%s counts=%s", prefix, vals)


def _log_preview(out: pd.DataFrame, cols: list[str], label: str, n: int = 40) -> None:
    if out.empty or not logger.isEnabledFor(logging.DEBUG):
        return
    cols = [c for c in cols if c in out.columns]
    if cols:
        logger.debug("%s:\n%s", label, out[cols].head(n).to_string(index=False))


def apply_signals_v2(df: pd.DataFrame, params=None) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        out["sig_vol_ok"] = []
        out["sig_breadth_ok"] = []
        out["sig_rs_ok"] = []
        out["sig_sector_ok"] = []
        out["sigeffectiveentrymin_v2"] = []
        out["sig_setup_continuation"] = []
        out["sig_setup_pullback"] = []
        out["sig_setup_any"] = []
        out["sigconfirmed_v2"] = []
        out["sigpositionpct_v2"] = []
        out["sigexit_v2"] = []
        return out

    p = params if params is not None else SIGNALPARAMS_V2
    logger.info("apply_signals_v2 start: rows=%d", len(out))
    logger.info(
        "Signal params: base_entry=%.4f base_exit=%.4f pullback_min_trend=%.4f continuation_min_trend=%.4f",
        float(p.get("base_entry_threshold", 0.0)),
        float(p.get("base_exit_threshold", 0.0)),
        float(p.get("pullback_min_trend", 0.0)),
        float(p.get("continuation_min_trend", 0.0)),
    )

    volreg = out.get("volregime", pd.Series("calm", index=out.index))
    breadthreg = out.get("breadthregime", pd.Series("unknown", index=out.index))
    rsreg = out.get("rsregime", pd.Series("unknown", index=out.index))
    sectreg = out.get("sectrsregime", pd.Series("unknown", index=out.index))

    out["sig_vol_ok"] = ~volreg.isin(p["hard_block_vol_regimes"])
    out["sig_breadth_ok"] = ~breadthreg.isin(p["hard_block_breadth_regimes"])
    out["sig_rs_ok"] = rsreg.isin(p["allowed_rs_regimes"])
    out["sig_sector_ok"] = ~sectreg.isin(p["blocked_sector_regimes"])

    vol_adj = volreg.map(p["regime_entry_adjustment"]).fillna(0)
    breadth_adj = breadthreg.map(p["breadth_entry_adjustment"]).fillna(0)
    out["sigeffectiveentrymin_v2"] = p["base_entry_threshold"] + vol_adj + breadth_adj

    pullback_shape = (
        (out.get("scoretrend", pd.Series(0, index=out.index)) >= p["pullback_min_trend"])
        & (out.get("closevsema30pct", pd.Series(0, index=out.index)).between(-0.05, p["pullback_max_short_extension"]))
        & (out.get("rsi14", pd.Series(50, index=out.index)) <= p["pullback_rsi_max"])
    )
    continuation_shape = (
        (out.get("scoretrend", pd.Series(0, index=out.index)) >= p["continuation_min_trend"])
        & (out.get("scoreparticipation", pd.Series(0, index=out.index)) >= 0.50)
    )

    out["sig_setup_continuation"] = continuation_shape
    #out["sig_setup_pullback"] = pullback_shape & volreg.isin(["volatile", "chaotic"])
    # For volatile market. During calm markets use the above one
    out["sig_setup_pullback"] = pullback_shape
    out["sig_setup_any"] = out["sig_setup_continuation"] | out["sig_setup_pullback"]

    base_ok = out["sig_vol_ok"] & out["sig_breadth_ok"] & out["sig_rs_ok"] & out["sig_sector_ok"] & out["sig_setup_any"]
    out["sigconfirmed_v2"] = (base_ok & (out["scorecomposite_v2"] >= out["sigeffectiveentrymin_v2"])).astype(int)

    size_mult = volreg.map(p["size_multipliers"]).fillna(1.0)
    raw_size = 0.04 + 0.08 * ((out["scorecomposite_v2"] - p["base_entry_threshold"]) / max(1 - p["base_entry_threshold"], 1e-9))
    out["sigpositionpct_v2"] = np.where(out["sigconfirmed_v2"].eq(1), np.clip(raw_size, 0.0, 0.12) * size_mult, 0.0)

    # volreg is a Series; extract the scalar (it's market-wide, same for all rows)
    vol_regime = volreg.iloc[0] if hasattr(volreg, 'iloc') else volreg

    exit_thresh = p["base_exit_threshold"]
    if vol_regime == "chaotic":
        exit_thresh = exit_thresh + 0.15

    out["sigexit_v2"] = (
        out["scorecomposite_v2"] <= exit_thresh
    ).astype(int)

    logger.info(
        "Signal summary: vol_ok=%d breadth_ok=%d rs_ok=%d sector_ok=%d any_setup=%d confirmed=%d exits=%d",
        int(out["sig_vol_ok"].sum()),
        int(out["sig_breadth_ok"].sum()),
        int(out["sig_rs_ok"].sum()),
        int(out["sig_sector_ok"].sum()),
        int(out["sig_setup_any"].sum()),
        int(out["sigconfirmed_v2"].sum()),
        int(out["sigexit_v2"].sum()),
    )
    total = len(out)
    if total > 0:
        logger.info(
            "Filter pass rates: vol_ok=%.1f%% breadth_ok=%.1f%% rs_ok=%.1f%% "
            "sector_ok=%.1f%% setup_any=%.1f%% score_ok=%.1f%%",
            100 * out["sig_vol_ok"].mean(),
            100 * out["sig_breadth_ok"].mean(),
            100 * out["sig_rs_ok"].mean(),
            100 * out["sig_sector_ok"].mean(),
            100 * out["sig_setup_any"].mean(),
            100 * (out["scorecomposite_v2"] >= out["sigeffectiveentrymin_v2"]).mean(),
        )
    logger.info(
        "Signal setup counts: continuation=%d pullback=%d any=%d",
        int(out["sig_setup_continuation"].sum()),
        int(out["sig_setup_pullback"].sum()),
        int(out["sig_setup_any"].sum()),
    )
    logger.info(
        "sigeffectiveentrymin_v2 stats: min=%.4f median=%.4f max=%.4f",
        float(out["sigeffectiveentrymin_v2"].min()),
        float(out["sigeffectiveentrymin_v2"].median()),
        float(out["sigeffectiveentrymin_v2"].max()),
    )
    _log_bool_counts(
        out,
        [
            "sig_vol_ok",
            "sig_breadth_ok",
            "sig_rs_ok",
            "sig_sector_ok",
            "sig_setup_continuation",
            "sig_setup_pullback",
            "sig_setup_any",
            "sigconfirmed_v2",
            "sigexit_v2",
        ],
        "Signal bool",
    )

    _log_preview(
        out,
        [
            "ticker",
            "sig_vol_ok",
            "sig_breadth_ok",
            "sig_rs_ok",
            "sig_sector_ok",
            "sig_setup_continuation",
            "sig_setup_pullback",
            "sig_setup_any",
            "scoretrend",
            "scoreparticipation",
            "scorecomposite_v2",
            "sigeffectiveentrymin_v2",
            "sigconfirmed_v2",
            "sigpositionpct_v2",
            "sigexit_v2",
            "volregime",
            "breadthregime",
            "rsregime",
            "sectrsregime",
            "rsi14",
            "adx14",
            "closevsema30pct",
        ],
        "Signal preview",
    )

    if logger.isEnabledFor(logging.DEBUG):
        failed = out[~out["sigconfirmed_v2"].eq(1)].copy()
        if not failed.empty:
            reasons = []
            for _, row in failed.iterrows():
                r = []
                if not row.get("sig_vol_ok", False):
                    r.append("vol_block")
                if not row.get("sig_breadth_ok", False):
                    r.append("breadth_block")
                if not row.get("sig_rs_ok", False):
                    r.append("rs_block")
                if not row.get("sig_sector_ok", False):
                    r.append("sector_block")
                if not row.get("sig_setup_any", False):
                    r.append("no_setup")
                if row.get("scorecomposite_v2", 0.0) < row.get("sigeffectiveentrymin_v2", 0.0):
                    r.append("below_entry")
                reasons.append(";".join(r))
            failed = failed.assign(rejection_reasons=reasons)
            logger.debug(
                "Signal rejects preview:\n%s",
                failed[
                    [
                        c
                        for c in [
                            "ticker",
                            "scorecomposite_v2",
                            "sigeffectiveentrymin_v2",
                            "sig_vol_ok",
                            "sig_breadth_ok",
                            "sig_rs_ok",
                            "sig_sector_ok",
                            "sig_setup_any",
                            "rejection_reasons",
                        ]
                        if c in failed.columns
                    ]
                ]
                .head(80)
                .to_string(index=False)
            )
    return out


def apply_convergence_v2(df: pd.DataFrame, params=None) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        out["convergence_label_v2"] = []
        out["convergence_tier_v2"] = []
        out["scoreadjusted_v2"] = []
        return out

    p = params if params is not None else CONVERGENCEPARAMS_V2
    logger.info("apply_convergence_v2 start: rows=%d", len(out))
    logger.info("Convergence params tiers=%s adjustments=%s", p.get("tiers"), p.get("adjustments"))

    rotationrec = out.get("rotationrec", pd.Series("HOLD", index=out.index))
    rotation_long = rotationrec.isin(["BUY", "STRONGBUY", "HOLD"])
    score_long = out.get("sigconfirmed_v2", pd.Series(0, index=out.index)).eq(1)

    labels = np.select(
        [
            rotation_long & score_long,
            rotation_long & ~score_long,
            ~rotation_long & score_long,
            rotationrec.eq("CONFLICT"),
        ],
        ["aligned_long", "rotation_long_only", "score_long_only", "mixed"],
        default="avoid",
    )

    out["convergence_label_v2"] = labels
    out["convergence_tier_v2"] = pd.Series(labels, index=out.index).map(p["tiers"]).fillna(0)

    adj = out.get("volregime", pd.Series("calm", index=out.index)).map(p["adjustments"]).fillna(0)
    boost = np.where(
        out["convergence_label_v2"] == "aligned_long",
        adj,
        np.where(out["convergence_label_v2"] == "mixed", -adj, 0.0),
    )
    out["scoreadjusted_v2"] = (out["scorecomposite_v2"] + boost).clip(0, 1)

    logger.info(
        "Convergence summary: aligned_long=%d rotation_long_only=%d score_long_only=%d mixed=%d avoid=%d",
        int((out["convergence_label_v2"] == "aligned_long").sum()),
        int((out["convergence_label_v2"] == "rotation_long_only").sum()),
        int((out["convergence_label_v2"] == "score_long_only").sum()),
        int((out["convergence_label_v2"] == "mixed").sum()),
        int((out["convergence_label_v2"] == "avoid").sum()),
    )
    logger.info("Convergence tiers: %s", out["convergence_tier_v2"].value_counts(dropna=False).to_dict())
    logger.info(
        "scoreadjusted_v2 stats: min=%.4f median=%.4f max=%.4f",
        float(out["scoreadjusted_v2"].min()),
        float(out["scoreadjusted_v2"].median()),
        float(out["scoreadjusted_v2"].max()),
    )

    if logger.isEnabledFor(logging.DEBUG):
        cols = [c for c in ["ticker", "rotationrec", "sigconfirmed_v2", "convergence_label_v2", "convergence_tier_v2", "scorecomposite_v2", "scoreadjusted_v2", "volregime"] if c in out.columns]
        logger.debug("Convergence preview:\n%s", out[cols].head(50).to_string(index=False))
        logger.debug("Highest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=False)[cols].head(50).to_string(index=False))
        logger.debug("Lowest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=True)[cols].head(50).to_string(index=False))

    return out.sort_values(["convergence_tier_v2", "scoreadjusted_v2"], ascending=[False, False])