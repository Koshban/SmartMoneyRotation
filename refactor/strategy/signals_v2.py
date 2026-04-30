"""refactor/strategy/signals_v2.py"""
from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from refactor.common.config_refactor import CONVERGENCEPARAMS_V2, SIGNALPARAMS_V2
from .adapters_v2 import ensure_columns

logger = logging.getLogger(__name__)


_SETUP_COLS = [
    "sig_setup_continuation",
    "sig_setup_pullback",
    "sig_setup_relative",
    "sig_setup_any",
]


def _log_bool_counts(out: pd.DataFrame, cols: list[str], prefix: str) -> None:
    vals = {c: int(out[c].sum()) if c in out.columns else 0 for c in cols}
    logger.info("%s counts=%s", prefix, vals)


def _log_preview(out: pd.DataFrame, cols: list[str], label: str, n: int = 40) -> None:
    if out.empty or not logger.isEnabledFor(logging.DEBUG):
        return
    cols = [c for c in cols if c in out.columns]
    if cols:
        logger.debug("%s:\n%s", label, out[cols].head(n).to_string(index=False))


# ═══════════════════════════════════════════════════════════════
#  Vol regime → exit tightening factor
# ═══════════════════════════════════════════════════════════════
#  In elevated/volatile regimes, exits should be TIGHTER (fire sooner)
#  because losses compound faster and recovery is less certain.
#  Factor > 1.0 means thresholds are loosened (easier to trigger exit).
_VOL_EXIT_TIGHTENING: dict[str, float] = {
    "calm":     1.00,   # normal thresholds
    "moderate": 1.10,   # slightly easier exits
    "elevated": 1.25,   # meaningfully easier exits
    "volatile": 1.40,   # much easier exits
    "chaotic":  1.60,   # aggressive exits
}


def apply_signals_v2(df: pd.DataFrame, params=None) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        for c in [
            "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            *_SETUP_COLS,
            "sigeffectiveentrymin_v2", "sigconfirmed_v2",
            "sigpositionpct_v2", "sigexit_v2", "score_rank",
            "exit_momentum", "exit_trend", "exit_rs",
            "exit_no_trend", "exit_score_floor", "exit_reason",
        ]:
            out[c] = []
        return out

    p = params if params is not None else SIGNALPARAMS_V2
    logger.info("apply_signals_v2 start: rows=%d", len(out))
    logger.info(
        "Signal params: base_entry=%.4f "
        "rs_fail_penalty=%.4f breadth_fail_penalty=%.4f "
        "min_rank_pct=%.4f pos_base=%.4f pos_range=%.4f pos_max=%.4f "
        "min_hold_days=%d cooldown=%d",
        float(p.get("base_entry_threshold", 0.0)),
        float(p.get("rs_fail_penalty", 0.10)),
        float(p.get("breadth_fail_penalty", 0.05)),
        float(p.get("min_rank_pct", 0.85)),
        float(p.get("position_base_pct", 0.04)),
        float(p.get("position_range_pct", 0.08)),
        float(p.get("position_max_pct", 0.12)),
        int(p.get("min_hold_days", 5)),
        int(p.get("cooldown_days", 3)),
    )

    # ── exit condition thresholds (TUNED for early deterioration) ───
    #
    # PHILOSOPHY: Exit when a position shows MODERATE deterioration on
    # multiple dimensions, not when it has already catastrophically failed.
    # The goal is to cut losses at -3% to -5%, not hold to -10%.
    #
    # closevsema30pct is in PERCENTAGE-POINT units.
    #   e.g., -3.0 means "close is 3% below EMA30"
    #
    # These defaults are calibrated for a universe of liquid ETFs.
    # For single stocks, you may want slightly looser thresholds.

    exit_rsi_thresh = float(p.get("exit_rsi_thresh", 43.0))           # ← was 35
    exit_ema_pct_thresh = float(p.get("exit_ema_pct_thresh", -2.5))   # ← was -5.0
    exit_rs_z_thresh = float(p.get("exit_rs_z_thresh", -0.7))        # ← was -1.5
    exit_adx_thresh = float(p.get("exit_adx_thresh", 18.0))          # ← was 12
    exit_composite_floor = float(p.get("exit_composite_floor", 0.38)) # ← was 0.15
    exit_min_conditions = int(p.get("exit_min_conditions", 2))        # keep at 2
    exit_vol_adaptive = bool(p.get("exit_vol_adaptive", True))        # ← NEW

    # ── NEW: severe single-condition immediate exits ────────────────
    # These bypass the convergence requirement for catastrophic events.
    exit_rsi_severe = float(p.get("exit_rsi_severe", 28.0))
    exit_ema_severe = float(p.get("exit_ema_severe", -7.0))
    exit_rs_severe = float(p.get("exit_rs_severe", -2.0))

    logger.info(
        "Exit params (per-position, TUNED): rsi_thresh=%.1f ema_pct_thresh=%.2f "
        "rs_z_thresh=%.2f adx_thresh=%.1f composite_floor=%.4f "
        "min_conditions=%d vol_adaptive=%s",
        exit_rsi_thresh, exit_ema_pct_thresh,
        exit_rs_z_thresh, exit_adx_thresh, exit_composite_floor,
        exit_min_conditions, exit_vol_adaptive,
    )
    logger.info(
        "Exit severe (bypass convergence): rsi=%.1f ema=%.2f rs=%.2f",
        exit_rsi_severe, exit_ema_severe, exit_rs_severe,
    )

    # ── verify exit-critical columns are present ────────────────────
    _EXIT_INDICATOR_COLS = ["rsi14", "adx14", "macdhist", "closevsema30pct", "rszscore"]
    _missing_exit_cols = [c for c in _EXIT_INDICATOR_COLS if c not in out.columns]
    if _missing_exit_cols:
        logger.warning(
            "EXIT INDICATORS MISSING: %s — these will use neutral fallbacks "
            "(no exit will fire on these dimensions). Check upstream feature "
            "pipeline.",
            _missing_exit_cols,
        )
    else:
        logger.info("Exit indicator columns all present: %s", _EXIT_INDICATOR_COLS)

    # ── regime columns ──────────────────────────────────────────────
    volreg = out.get("volregime", pd.Series("calm", index=out.index))
    breadthreg = out.get("breadthregime", pd.Series("unknown", index=out.index))
    rsreg = out.get("rsregime", pd.Series("unknown", index=out.index))
    sectreg = out.get("sectrsregime", pd.Series("unknown", index=out.index))

    # ── boolean flags (diagnostic; RS & breadth are soft penalties) ─
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

    # ── setup shapes: vestigial (always True for compat) ────────────
    for c in _SETUP_COLS:
        out[c] = True

    composite = out["scorecomposite_v2"]
    out["score_rank"] = composite.rank(pct=True)

    # ── entry signal ────────────────────────────────────────────────
    hard_blocks_ok = out["sig_vol_ok"] & out["sig_sector_ok"]

    min_rank = p.get("min_rank_pct", 0.85)
    score_passes_threshold = composite >= out["sigeffectiveentrymin_v2"]
    rank_passes = out["score_rank"] >= min_rank

    out["sigconfirmed_v2"] = (
        hard_blocks_ok & score_passes_threshold & rank_passes
    ).astype(int)

    # ── position sizing ─────────────────────────────────────────────
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

    # ══════════════════════════════════════════════════════════════════
    #  EXIT SIGNAL (per-position deterioration)
    # ══════════════════════════════════════════════════════════════════
    #
    # DESIGN PHILOSOPHY (REVISED):
    # ─────────────────────────────
    # The old exit logic was calibrated for catastrophic events (RSI<35,
    # 5% below EMA, etc.) which meant positions were held through -10%
    # losses before any exit fired. A momentum strategy MUST cut losers
    # early — accepting many small losses to capture fewer large wins.
    #
    # NEW APPROACH:
    # 1. MODERATE thresholds: detect early deterioration, not catastrophe
    # 2. CONVERGENCE: still require 2+ conditions (prevents noise exits)
    # 3. SEVERE bypass: single catastrophic condition = immediate exit
    # 4. VOL-ADAPTIVE: tighten exits in volatile regimes
    #
    # Expected behavior:
    # - Normal bull market day: 5-12% of universe flagged (3-8 names)
    # - Mild correction day:   15-25% flagged (10-16 names)
    # - Sharp selloff day:     30-50% flagged (20-32 names)
    #
    # The engine only applies sigexit to HELD positions, so even with
    # higher flag rates, actual exits remain controlled.

    rsi = out.get("rsi14", pd.Series(50.0, index=out.index)).astype(float)
    adx = out.get("adx14", pd.Series(25.0, index=out.index)).astype(float)
    macd_hist = out.get("macdhist", pd.Series(0.0, index=out.index)).astype(float)
    close_vs_ema30 = out.get("closevsema30pct", pd.Series(0.0, index=out.index)).astype(float)
    rs_z = out.get("rszscore", pd.Series(0.0, index=out.index)).astype(float)

    # ── Vol-adaptive threshold adjustment ───────────────────────────
    # In elevated/volatile regimes, we LOOSEN exit thresholds (i.e.,
    # make them trigger more easily) by multiplying thresholds that are
    # "less than" checks.  A tightening factor of 1.25 means RSI thresh
    # goes from 43 → 43*1.25 = 53.75 (easier to trigger).
    if exit_vol_adaptive:
        vol_tightening = volreg.map(_VOL_EXIT_TIGHTENING).fillna(1.0)
    else:
        vol_tightening = pd.Series(1.0, index=out.index)

    # Apply tightening: for "less than" conditions, multiply threshold
    # by tightening factor (higher factor = higher threshold = easier exit)
    eff_rsi_thresh = exit_rsi_thresh * vol_tightening
    eff_ema_thresh = exit_ema_pct_thresh / vol_tightening  # more negative = stricter, so divide
    eff_rs_thresh = exit_rs_z_thresh / vol_tightening      # same logic
    eff_adx_thresh = exit_adx_thresh * vol_tightening
    eff_composite_floor = exit_composite_floor * vol_tightening

    # ── Individual exit conditions ──────────────────────────────────
    #
    # 1. Momentum deterioration: RSI below threshold
    #    (CHANGED: removed the AND with MACD — RSI alone is sufficient
    #     when combined with convergence requirement)
    exit_momentum = rsi < eff_rsi_thresh

    # 2. Trend break: price meaningfully below 30-day EMA
    #    (CHANGED: threshold from -5.0 to -2.5 — catches early breakdowns)
    exit_trend = close_vs_ema30 < eff_ema_thresh

    # 3. Relative strength collapse: underperforming benchmark
    #    (CHANGED: threshold from -1.5 to -0.7 — catches early RS decay)
    exit_rs = rs_z < eff_rs_thresh

    # 4. Trend evaporation: no directional movement remaining
    #    (CHANGED: threshold from 12 to 18 — catches fading trends)
    exit_no_trend = adx < eff_adx_thresh

    # 5. Composite floor: overall score has degraded significantly
    #    (CHANGED: threshold from 0.15 to 0.38 — catches score decay
    #     before it becomes catastrophic)
    exit_score_floor = composite < eff_composite_floor

    # ── NEW: MACD direction as 6th condition ────────────────────────
    # MACD histogram negative AND declining (momentum actively worsening)
    macd_prev = out.get("macdhist", pd.Series(0.0, index=out.index)).astype(float)
    exit_macd_declining = (macd_hist < 0) & (close_vs_ema30 < 0)

    # Store component booleans for diagnostics
    out["exit_momentum"] = exit_momentum
    out["exit_trend"] = exit_trend
    out["exit_rs"] = exit_rs
    out["exit_no_trend"] = exit_no_trend
    out["exit_score_floor"] = exit_score_floor

    # Count how many conditions fire per ticker
    exit_condition_count = (
        exit_momentum.astype(int)
        + exit_trend.astype(int)
        + exit_rs.astype(int)
        + exit_no_trend.astype(int)
        + exit_score_floor.astype(int)
        + exit_macd_declining.astype(int)
    )

    # ── SEVERE BYPASS: single catastrophic condition → immediate exit ──
    # These represent truly broken positions that shouldn't wait for
    # convergence.
    severe_exit = (
        (rsi < exit_rsi_severe)
        | (close_vs_ema30 < exit_ema_severe)
        | (rs_z < exit_rs_severe)
    )

    # ── CONVERGENCE: require N moderate conditions OR 1 severe ─────
    out["sigexit_v2"] = (
        (exit_condition_count >= exit_min_conditions) | severe_exit
    ).astype(int)

    # Build human-readable exit reason string for logging/debug
    reason_parts = []
    for cond, label in [
        (exit_momentum, "momentum"),
        (exit_trend, "trend"),
        (exit_rs, "rs"),
        (exit_no_trend, "no_trend"),
        (exit_score_floor, "score_floor"),
        (exit_macd_declining, "macd_declining"),
        (severe_exit, "SEVERE"),
    ]:
        reason_parts.append((cond, label))

    reason_series = pd.Series("", index=out.index)
    for cond, label in reason_parts:
        reason_series = reason_series.where(
            ~cond, reason_series + label + ";"
        )
    out["exit_reason"] = reason_series.str.rstrip(";")

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
        "min_rank_pct=%.4f",
        float(out["score_rank"].max()),
        float(out["score_rank"].quantile(0.9)),
        float(out["score_rank"].median()),
        min_rank,
    )
    logger.info(
        "Gate pass counts (of %d): vol_ok=%d sector_ok=%d "
        "hard_blocks_ok=%d score_ok=%d rank_ok=%d confirmed=%d",
        len(out),
        int(out["sig_vol_ok"].sum()),
        int(out["sig_sector_ok"].sum()),
        int(hard_blocks_ok.sum()),
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

    # ── exit diagnostic logging ─────────────────────────────────────
    exit_count = int(out["sigexit_v2"].sum())
    exit_pct = 100 * out["sigexit_v2"].mean() if len(out) > 0 else 0.0
    severe_count = int(severe_exit.sum())

    n_momentum = int(exit_momentum.sum())
    n_trend = int(exit_trend.sum())
    n_rs = int(exit_rs.sum())
    n_no_trend = int(exit_no_trend.sum())
    n_score_floor = int(exit_score_floor.sum())
    n_macd = int(exit_macd_declining.sum())

    logger.info(
        "Exit stats (convergence=%d+ of 6, or severe): "
        "momentum=%d trend=%d rs=%d no_trend=%d score_floor=%d macd=%d "
        "severe=%d → sigexit_v2=%d (%.1f%% of %d)",
        exit_min_conditions,
        n_momentum, n_trend, n_rs, n_no_trend, n_score_floor, n_macd,
        severe_count,
        exit_count, exit_pct, len(out),
    )

    # Distribution of condition counts for understanding selectivity
    cond_count_dist = exit_condition_count.value_counts().sort_index().to_dict()
    logger.info(
        "Exit condition count distribution: %s (need >=%d to exit)",
        cond_count_dist, exit_min_conditions,
    )

    # Vol-adaptive tightening diagnostic
    if exit_vol_adaptive:
        vol_tight_dist = vol_tightening.value_counts().to_dict()
        logger.info("Vol exit tightening distribution: %s", vol_tight_dist)

    logger.info(
        "Exit input stats: rsi14 min=%.1f p25=%.1f med=%.1f | "
        "adx14 min=%.1f p25=%.1f med=%.1f | macdhist min=%.4f med=%.4f | "
        "closevsema30pct min=%.2f p25=%.2f med=%.2f | "
        "rszscore min=%.2f p25=%.2f med=%.2f",
        float(rsi.min()), float(rsi.quantile(0.25)), float(rsi.median()),
        float(adx.min()), float(adx.quantile(0.25)), float(adx.median()),
        float(macd_hist.min()), float(macd_hist.median()),
        float(close_vs_ema30.min()), float(close_vs_ema30.quantile(0.25)),
        float(close_vs_ema30.median()),
        float(rs_z.min()), float(rs_z.quantile(0.25)), float(rs_z.median()),
    )

    if exit_pct > 40:
        logger.warning(
            "EXIT CHURN RISK: %.1f%% of universe flagged for exit. "
            "Per-position thresholds may be too loose for this market "
            "regime. Breakdown: momentum=%d trend=%d rs=%d no_trend=%d "
            "score_floor=%d macd=%d severe=%d | min_conditions=%d",
            exit_pct,
            n_momentum, n_trend, n_rs, n_no_trend, n_score_floor,
            n_macd, severe_count,
            exit_min_conditions,
        )
    elif exit_pct < 3:
        logger.warning(
            "EXIT PASSIVITY RISK: only %.1f%% of universe flagged for "
            "exit. Thresholds may still be too strict. Consider loosening "
            "exit_rsi_thresh, exit_ema_pct_thresh, or exit_composite_floor.",
            exit_pct,
        )

    total = len(out)
    if total > 0:
        logger.info(
            "Filter pass rates: vol_ok=%.1f%% sector_ok=%.1f%% "
            "score_ok=%.1f%% rank_ok=%.1f%% confirmed=%.1f%% exit=%.1f%%",
            100 * out["sig_vol_ok"].mean(),
            100 * out["sig_sector_ok"].mean(),
            100 * score_passes_threshold.mean(),
            100 * rank_passes.mean(),
            100 * out["sigconfirmed_v2"].mean(),
            exit_pct,
        )

    _log_bool_counts(
        out,
        [
            "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            "sigconfirmed_v2", "sigexit_v2",
        ],
        "Signal bool",
    )

    _log_preview(
        out,
        [
            "ticker", "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            "scoretrend", "scoreparticipation", "scorecomposite_v2",
            "score_rank", "sigeffectiveentrymin_v2", "sigconfirmed_v2",
            "sigpositionpct_v2", "sigexit_v2", "exit_reason",
            "volregime", "breadthregime",
            "rsregime", "sectrsregime", "rsi14", "adx14", "closevsema30pct",
        ],
        "Signal preview",
    )

    if logger.isEnabledFor(logging.DEBUG):
        # ── rejection reasons for entries ───────────────────────────
        failed = out[~out["sigconfirmed_v2"].eq(1)].copy()
        if not failed.empty:
            reasons = []
            for _, row in failed.iterrows():
                r = []
                if not row.get("sig_vol_ok", False):
                    r.append("vol_block")
                if not row.get("sig_sector_ok", False):
                    r.append("sector_block")
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
                            "sig_rs_ok", "sig_breadth_ok",
                            "rejection_reasons",
                        ]
                        if c in failed.columns
                    ]
                ]
                .head(80)
                .to_string(index=False),
            )

        # ── exit flagged tickers detail ─────────────────────────────
        exiting = out[out["sigexit_v2"].eq(1)].copy()
        if not exiting.empty:
            exit_cols = [
                c for c in [
                    "ticker", "scorecomposite_v2", "rsi14", "adx14",
                    "macdhist", "closevsema30pct", "rszscore",
                    "exit_reason",
                ]
                if c in exiting.columns
            ]
            logger.debug(
                "Exit flagged tickers:\n%s",
                exiting[exit_cols].head(80).to_string(index=False),
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