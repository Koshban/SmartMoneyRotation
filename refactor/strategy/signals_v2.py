"""refactor/strategy/signals_v2.py"""
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
        out["sig_setup_relative"] = []
        out["sig_setup_any"] = []
        out["sigconfirmed_v2"] = []
        out["sigpositionpct_v2"] = []
        out["sigexit_v2"] = []
        out["score_rank"] = []
        return out

    p = params if params is not None else SIGNALPARAMS_V2
    logger.info("apply_signals_v2 start: rows=%d", len(out))
    logger.info(
        "Signal params: base_entry=%.4f base_exit=%.4f pullback_min_trend=%.4f "
        "continuation_min_trend=%.4f rs_fail_penalty=%.4f breadth_fail_penalty=%.4f "
        "min_rank_pct=%.4f pos_base=%.4f pos_range=%.4f pos_max=%.4f "
        "pullback_min_ext=%.4f cont_min_part=%.4f",
        float(p.get("base_entry_threshold", 0.0)),
        float(p.get("base_exit_threshold", 0.0)),
        float(p.get("pullback_min_trend", 0.0)),
        float(p.get("continuation_min_trend", 0.0)),
        float(p.get("rs_fail_penalty", 0.10)),
        float(p.get("breadth_fail_penalty", 0.05)),
        float(p.get("min_rank_pct", 0.85)),
        float(p.get("position_base_pct", 0.04)),           # FIX 4
        float(p.get("position_range_pct", 0.08)),           # FIX 4
        float(p.get("position_max_pct", 0.12)),             # FIX 4
        float(p.get("pullback_min_short_extension", -0.05)),# FIX 5
        float(p.get("continuation_min_participation", 0.50)),# FIX 6
    )

    # ── regime columns ──────────────────────────────────────────────
    volreg = out.get("volregime", pd.Series("calm", index=out.index))
    breadthreg = out.get("breadthregime", pd.Series("unknown", index=out.index))
    rsreg = out.get("rsregime", pd.Series("unknown", index=out.index))
    sectreg = out.get("sectrsregime", pd.Series("unknown", index=out.index))

    # ── boolean flags (kept for diagnostics, but RS & breadth are now soft) ──
    out["sig_vol_ok"] = ~volreg.isin(p["hard_block_vol_regimes"])
    out["sig_breadth_ok"] = ~breadthreg.isin(p["hard_block_breadth_regimes"])
    out["sig_rs_ok"] = rsreg.isin(p["allowed_rs_regimes"])
    out["sig_sector_ok"] = ~sectreg.isin(p["blocked_sector_regimes"])

    # ── effective entry threshold ────────────────────────────────────
    vol_adj = volreg.map(p["regime_entry_adjustment"]).fillna(0)
    breadth_adj = breadthreg.map(p["breadth_entry_adjustment"]).fillna(0)

    rs_penalty = np.where(
        out["sig_rs_ok"], 0.0, p.get("rs_fail_penalty", 0.10)
    )
    breadth_penalty = np.where(
        out["sig_breadth_ok"], 0.0, p.get("breadth_fail_penalty", 0.05)
    )

    out["sigeffectiveentrymin_v2"] = (
        p["base_entry_threshold"]
        + vol_adj
        + breadth_adj
        + rs_penalty
        + breadth_penalty
    )

    # ── setup shapes ────────────────────────────────────────────────
    score_trend = out.get("scoretrend", pd.Series(0, index=out.index))
    score_part = out.get("scoreparticipation", pd.Series(0, index=out.index))
    close_vs_ema = out.get("closevsema30pct", pd.Series(0, index=out.index))
    rsi = out.get("rsi14", pd.Series(50, index=out.index))
    composite = out["scorecomposite_v2"]

    # FIX 5: pullback lower bound from config
    pullback_min_ext = p.get("pullback_min_short_extension", -0.05)

    pullback_shape = (
        (score_trend >= p["pullback_min_trend"])
        & (close_vs_ema.between(pullback_min_ext, p["pullback_max_short_extension"]))
        & (rsi <= p["pullback_rsi_max"])
    )

    # FIX 6: continuation participation floor from config
    cont_min_part = p.get("continuation_min_participation", 0.50)

    continuation_shape = (
        (score_trend >= p["continuation_min_trend"])
        & (score_part >= cont_min_part)
    )

    rank_threshold_for_setup = p.get("relative_setup_rank_pct", 0.80)
    out["score_rank"] = composite.rank(pct=True)
    relative_strength_shape = out["score_rank"] >= rank_threshold_for_setup

    out["sig_setup_continuation"] = continuation_shape
    out["sig_setup_pullback"] = pullback_shape
    out["sig_setup_relative"] = relative_strength_shape
    out["sig_setup_any"] = (
        out["sig_setup_continuation"]
        | out["sig_setup_pullback"]
        | out["sig_setup_relative"]
    )

    # ── entry signal ────────────────────────────────────────────────
    base_ok = (
        out["sig_vol_ok"]
        & out["sig_sector_ok"]
        & out["sig_setup_any"]
    )

    min_rank = p.get("min_rank_pct", 0.85)
    score_passes_threshold = composite >= out["sigeffectiveentrymin_v2"]
    rank_passes = out["score_rank"] >= min_rank

    out["sigconfirmed_v2"] = (
        base_ok & score_passes_threshold & rank_passes
    ).astype(int)

    # ── position sizing (FIX 4: all from config) ────────────────────
    pos_base = p.get("position_base_pct", 0.04)
    pos_range = p.get("position_range_pct", 0.08)
    pos_max = p.get("position_max_pct", 0.12)

    size_mult = volreg.map(p["size_multipliers"]).fillna(1.0)
    entry_thresh = p["base_entry_threshold"]
    raw_size = pos_base + pos_range * (
        (composite - entry_thresh) / max(1 - entry_thresh, 1e-9)
    )
    out["sigpositionpct_v2"] = np.where(
        out["sigconfirmed_v2"].eq(1),
        np.clip(raw_size, 0.0, pos_max) * size_mult,
        0.0,
    )

    # ── exit signal ─────────────────────────────────────────────────
    exit_thresh = p.get("base_exit_threshold", 0.30)

    vol_regime_scalar = volreg.iloc[0] if len(volreg) > 0 else "calm"
    if vol_regime_scalar == "chaotic":
        exit_thresh = exit_thresh + p.get("chaotic_exit_bump", 0.10)

    exit_rank_floor = p.get("exit_rank_floor", 0.25)

    out["sigexit_v2"] = (
        (composite <= exit_thresh)
        | (out["score_rank"] <= exit_rank_floor)
    ).astype(int)

    # ── diagnostic logging ──────────────────────────────────────────
    logger.info(
        "Composite stats: max=%.4f p90=%.4f p75=%.4f median=%.4f min=%.4f",
        float(composite.max()),
        float(composite.quantile(0.9)),
        float(composite.quantile(0.75)),
        float(composite.median()),
        float(composite.min()),
    )
    logger.info(
        "Effective entry threshold: min=%.4f median=%.4f max=%.4f",
        float(out["sigeffectiveentrymin_v2"].min()),
        float(out["sigeffectiveentrymin_v2"].median()),
        float(out["sigeffectiveentrymin_v2"].max()),
    )
    logger.info(
        "Score rank stats: max=%.4f p90=%.4f median=%.4f | "
        "min_rank_pct=%.4f exit_rank_floor=%.4f",
        float(out["score_rank"].max()),
        float(out["score_rank"].quantile(0.9)),
        float(out["score_rank"].median()),
        min_rank,
        exit_rank_floor,
    )
    logger.info(
        "Gate pass counts (of %d): vol_ok=%d sector_ok=%d "
        "setup_any=%d (cont=%d pull=%d rel=%d) "
        "base_ok=%d score_ok=%d rank_ok=%d confirmed=%d",
        len(out),
        int(out["sig_vol_ok"].sum()),
        int(out["sig_sector_ok"].sum()),
        int(out["sig_setup_any"].sum()),
        int(out["sig_setup_continuation"].sum()),
        int(out["sig_setup_pullback"].sum()),
        int(out["sig_setup_relative"].sum()),
        int(base_ok.sum()),
        int(score_passes_threshold.sum()),
        int(rank_passes.sum()),
        int(out["sigconfirmed_v2"].sum()),
    )
    logger.info(
        "Soft penalty impact: rs_penalized=%d (of %d) breadth_penalized=%d (of %d)",
        int((~out["sig_rs_ok"]).sum()),
        len(out),
        int((~out["sig_breadth_ok"]).sum()),
        len(out),
    )
    logger.info(
        "Exit stats: exit_thresh=%.4f exit_signals=%d (of %d)",
        exit_thresh,
        int(out["sigexit_v2"].sum()),
        len(out),
    )

    total = len(out)
    if total > 0:
        logger.info(
            "Filter pass rates: vol_ok=%.1f%% sector_ok=%.1f%% "
            "setup_any=%.1f%% score_ok=%.1f%% rank_ok=%.1f%% confirmed=%.1f%%",
            100 * out["sig_vol_ok"].mean(),
            100 * out["sig_sector_ok"].mean(),
            100 * out["sig_setup_any"].mean(),
            100 * score_passes_threshold.mean(),
            100 * rank_passes.mean(),
            100 * out["sigconfirmed_v2"].mean(),
        )

    _log_bool_counts(
        out,
        [
            "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            "sig_setup_continuation", "sig_setup_pullback", "sig_setup_relative",
            "sig_setup_any", "sigconfirmed_v2", "sigexit_v2",
        ],
        "Signal bool",
    )

    _log_preview(
        out,
        [
            "ticker", "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            "sig_setup_continuation", "sig_setup_pullback", "sig_setup_relative",
            "sig_setup_any", "scoretrend", "scoreparticipation", "scorecomposite_v2",
            "score_rank", "sigeffectiveentrymin_v2", "sigconfirmed_v2",
            "sigpositionpct_v2", "sigexit_v2", "volregime", "breadthregime",
            "rsregime", "sectrsregime", "rsi14", "adx14", "closevsema30pct",
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
                if not row.get("sig_sector_ok", False):
                    r.append("sector_block")
                if not row.get("sig_setup_any", False):
                    r.append("no_setup")
                if row.get("scorecomposite_v2", 0.0) < row.get(
                    "sigeffectiveentrymin_v2", 0.0
                ):
                    r.append(
                        f"below_entry({row.get('scorecomposite_v2', 0):.3f}"
                        f"<{row.get('sigeffectiveentrymin_v2', 0):.3f})"
                    )
                if row.get("score_rank", 0.0) < min_rank:
                    r.append(f"below_rank({row.get('score_rank', 0):.3f}<{min_rank})")
                if not row.get("sig_rs_ok", False):
                    r.append("rs_penalized")
                if not row.get("sig_breadth_ok", False):
                    r.append("breadth_penalized")
                reasons.append(";".join(r))
            failed = failed.assign(rejection_reasons=reasons)
            logger.debug(
                "Signal rejects preview:\n%s",
                failed[
                    [
                        c
                        for c in [
                            "ticker", "scorecomposite_v2", "score_rank",
                            "sigeffectiveentrymin_v2", "sig_vol_ok", "sig_sector_ok",
                            "sig_setup_any", "sig_rs_ok", "sig_breadth_ok",
                            "rejection_reasons",
                        ]
                        if c in failed.columns
                    ]
                ]
                .head(80)
                .to_string(index=False),
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
        "Convergence summary: aligned_long=%d rotation_long_only=%d "
        "score_long_only=%d mixed=%d avoid=%d",
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
        cols = [c for c in [
            "ticker", "rotationrec", "sigconfirmed_v2", "convergence_label_v2",
            "convergence_tier_v2", "scorecomposite_v2", "scoreadjusted_v2", "volregime",
        ] if c in out.columns]
        logger.debug("Convergence preview:\n%s", out[cols].head(50).to_string(index=False))
        logger.debug("Highest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=False)[cols].head(50).to_string(index=False))
        logger.debug("Lowest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=True)[cols].head(50).to_string(index=False))

    return out.sort_values(["convergence_tier_v2", "scoreadjusted_v2"], ascending=[False, False])