"""
phase2/strategy/portfolio_v2.py

Portfolio construction for the v2 pipeline.

PURPOSE: Generate a CLEAN, LIMITED set of actionable signals.
    - STRONG_BUY: names that passed ALL pipeline gates (confirmation,
      RS regime, RSI ceiling) — ranked by blended score
    - BUY: names that passed score/percentile gates but not all
      STRONG_BUY qualifications — ranked by blended score
    - Everything else: not signalled

Design philosophy:
    - RESPECTS the action engine's tier assignments (RSI gates, regime gates)
    - Ranking is used for ORDERING within tiers, not for re-labeling
    - No parameter stuffing to beautify backtests
    - Equal-weight positions (operator decides sizing in real life)
    - Signal QUALITY over quantity
    - Breadth/vol regime scaling kept only as a leading indicator

Output
------
``build_portfolio_v2`` returns a dict with:

    selected   – DataFrame of the final portfolio with per-name
                 weights, risk metrics, and selection reasoning.
    meta       – dict with portfolio-level summary statistics.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from cash.phase2.strategy.ranking_v2 import rank_and_filter_candidates

logger = logging.getLogger(__name__)

# ── Signal Caps (the CORE constraint) ─────────────────────────────────────────
STRONG_BUY_LIMIT = 15       # Max names that get STRONG_BUY per day
MAX_BUY_SIGNALS = 25        # Total buy signals (STRONG_BUY + BUY) per day

# ── Portfolio Defaults ────────────────────────────────────────────────────────
DEFAULT_MAX_POSITIONS = 25   # Real portfolio holds 20-30 names
DEFAULT_MAX_SECTOR_WEIGHT = 0.40  # No more than 40% in one sector
DEFAULT_MAX_THEME_NAMES = 3
DEFAULT_MAX_SINGLE_WEIGHT = 0.08  # ~equal weight for 12-25 names
DEFAULT_MIN_WEIGHT = 0.02

# ── Rotation-aware sector cap multipliers ─────────────────────────────────────
# Set all rotation caps to 1.0 (no penalizing lagging sectors)
ROTATION_CAP_MULT: dict[str, float] = {
    "leading":   1.00,
    "improving": 1.00,
    "weakening": 1.00,
    "lagging":   1.00,
    "unknown":   1.00,
}

# ── Exposure scaling by market regime ─────────────────────────────────────────
# Set all exposure to 1.0 (no scaling)
BREADTH_EXPOSURE: dict[str, float] = {
    "strong": 1.00, "healthy": 1.00, "moderate": 1.00,
    "neutral": 1.00, "mixed": 1.00, "narrow": 1.00,
    "weak": 1.00, "critical": 1.00, "unknown": 1.00,
}
VOL_EXPOSURE: dict[str, float] = {
    "calm": 1.00, "low": 1.00, "normal": 1.00, "moderate": 1.00,
    "elevated": 1.00, "stressed": 1.00, "chaotic": 1.00,
    "unknown": 1.00,
}

# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        f = float(value)
        return default if np.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _effective_sector_cap(
    sector_regime: str,
    base_cap: float,
) -> float:
    """Return the dynamic sector weight cap given the rotation regime."""
    mult = ROTATION_CAP_MULT.get(
        str(sector_regime).lower(),
        ROTATION_CAP_MULT["unknown"],
    )
    return base_cap * mult


def _target_exposure(breadth: str, vol: str) -> float:
    """
    Compute target gross exposure from breadth and volatility regimes.
    Returns a value in roughly [0.20, 1.00].
    """
    b = BREADTH_EXPOSURE.get(str(breadth).lower(), BREADTH_EXPOSURE["unknown"])
    v = VOL_EXPOSURE.get(str(vol).lower(), VOL_EXPOSURE["unknown"])
    return round(b * v, 4)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def build_portfolio_v2(
    action_table: pd.DataFrame,
    max_positions: int = DEFAULT_MAX_POSITIONS,
    max_sector_weight: float = DEFAULT_MAX_SECTOR_WEIGHT,
    max_theme_names: int = DEFAULT_MAX_THEME_NAMES,
    max_single_weight: float = DEFAULT_MAX_SINGLE_WEIGHT,
    min_weight: float = DEFAULT_MIN_WEIGHT,
    strong_buy_limit: int = STRONG_BUY_LIMIT,
    max_buy_signals: int = MAX_BUY_SIGNALS,
) -> dict:
    """
    Build a concentrated target portfolio from the actioned universe.

    IMPORTANT: This module RESPECTS the action_v2 labels assigned by the
    pipeline's action engine. It does NOT re-derive STRONG_BUY/BUY based
    on ranking. The ranking is used solely for ORDERING within each tier.

    Parameters
    ----------
    action_table : DataFrame
        Output of the action generator (must have 'action_v2' column).
    max_positions : int
        Maximum number of names in the final portfolio.
    max_sector_weight : float
        Base maximum weight for any single GICS sector.
    max_theme_names : int
        Maximum number of names from any single theme.
    max_single_weight : float
        Hard cap on any individual position.
    min_weight : float
        Minimum weight for inclusion.
    strong_buy_limit : int
        Maximum names that receive STRONG_BUY (cap from pipeline).
    max_buy_signals : int
        Total buy signals emitted (STRONG_BUY + BUY combined).

    Returns
    -------
    dict with ``selected`` (DataFrame) and ``meta`` (dict).
    """
    # ── 0. empty guard ────────────────────────────────────────────────────────
    breadth_regime = "unknown"
    vol_regime = "unknown"

    if action_table.empty or "action_v2" not in action_table.columns:
        logger.info("build_portfolio_v2: empty action table, returning empty portfolio")
        return _empty_portfolio(breadth_regime, vol_regime)

    # Resolve market regimes from the first row (uniform across rows)
    breadth_regime = str(
        action_table["breadthregime"].iloc[0]
        if "breadthregime" in action_table.columns
        else "unknown"
    ).lower()
    vol_regime = str(
        action_table["volregime"].iloc[0]
        if "volregime" in action_table.columns
        else "unknown"
    ).lower()

    # ── 1. filter to eligible actions ─────────────────────────────────────────
    eligible_mask = action_table["action_v2"].isin(["STRONG_BUY", "BUY"])
    candidates = action_table.loc[eligible_mask].copy()

    if candidates.empty:
        logger.info(
            "build_portfolio_v2: no STRONG_BUY or BUY candidates "
            "(total rows=%d)",
            len(action_table),
        )
        return _empty_portfolio(breadth_regime, vol_regime)

    # Track original action labels from pipeline
    candidates["_pipeline_action"] = candidates["action_v2"].copy()

    n_sb_from_pipeline = int((candidates["_pipeline_action"] == "STRONG_BUY").sum())
    n_buy_from_pipeline = int((candidates["_pipeline_action"] == "BUY").sum())

    logger.info(
        "Portfolio candidates (raw): %d STRONG_BUY + BUY out of %d total",
        len(candidates), len(action_table),
    )
    logger.info(
        "Pipeline action breakdown: %d STRONG_BUY + %d BUY (preserving these labels)",
        n_sb_from_pipeline, n_buy_from_pipeline,
    )

    # ── 2. RANK candidates for ORDERING (not re-labeling) ─────────────────────
    #
    # The ranking determines the ORDER in which names are considered for
    # portfolio inclusion. It does NOT change their action tier.
    #
    # Key change: we use soft filters here (no hard beta/vol gate that
    # would exclude quality defensive names the action engine approved).
    #
    score_col = (
        "scoreadjusted_v2"
        if "scoreadjusted_v2" in candidates.columns
        else "scorecomposite_v2"
    )

    ranking_params = {
        "tilt": 0.40,           # reduced from 0.50 — less momentum chase
        "vol_pref": 0.20,       # reduced from 0.30 — less beta preference
        "min_vol": 0.0,         # NO hard vol filter (was 0.25, excluded defensives)
        "min_rs": -2.0,         # very loose (was -0.5, excluded legitimate names)
        "min_beta": 0.0,        # NO hard beta filter (was 1.3, the main culprit)
        "vol_col": "realizedvol20d",
        "rs_col": "rszscore",
        "score_col": score_col,
        "momentum_col": "rszscore",
    }
    candidates = rank_and_filter_candidates(candidates, ranking_params)

    if candidates.empty:
        logger.info(
            "build_portfolio_v2: all candidates filtered by ranking",
        )
        return _empty_portfolio(breadth_regime, vol_regime)

    # ── 3. SORT by action tier FIRST, then by rank WITHIN each tier ───────────
    #
    # This ensures:
    #   - STRONG_BUY names (which passed RSI, confirmation, regime gates)
    #     are ALWAYS considered before BUY names
    #   - Within each tier, the ranking determines priority
    #   - No BUY name can displace a STRONG_BUY name regardless of beta
    #
    candidates["_action_priority"] = candidates["_pipeline_action"].map(
        {"STRONG_BUY": 2, "BUY": 1}
    ).fillna(0).astype(int)

    # The ranking module should have added a 'blended_rank' or sort column.
    # If rank_and_filter_candidates sorted the df, use that order as tie-break.
    # We add a position-based rank to preserve the ranking module's ordering.
    candidates["_rank_order"] = range(len(candidates))

    candidates = candidates.sort_values(
        ["_action_priority", "_rank_order"],
        ascending=[False, True],  # STRONG_BUY first, then lowest rank_order first
    ).reset_index(drop=True)

    # ── 4. Apply signal caps (preserve pipeline labels) ───────────────────────
    #
    # Cap STRONG_BUY count to strong_buy_limit (from config).
    # Cap total signals to max_buy_signals.
    # But do NOT promote BUY→STRONG_BUY or demote STRONG_BUY→BUY.
    # If there are more STRONG_BUYs than the cap, keep only the top-ranked ones
    # as STRONG_BUY and demote the rest to BUY.
    #
    sb_mask = candidates["_pipeline_action"] == "STRONG_BUY"
    sb_candidates = candidates.loc[sb_mask]
    buy_candidates = candidates.loc[~sb_mask]

    if len(sb_candidates) > strong_buy_limit:
        # Keep top strong_buy_limit as STRONG_BUY, demote the rest to BUY
        sb_keep = sb_candidates.iloc[:strong_buy_limit].index
        sb_demote = sb_candidates.iloc[strong_buy_limit:].index
        candidates.loc[sb_demote, "action_v2"] = "BUY"
        logger.info(
            "STRONG_BUY cap applied in portfolio: %d → %d (demoted %d to BUY)",
            len(sb_candidates), strong_buy_limit, len(sb_demote),
        )
    else:
        # All STRONG_BUYs from pipeline are preserved
        logger.info(
            "STRONG_BUY from pipeline: %d (within cap=%d, all preserved)",
            len(sb_candidates), strong_buy_limit,
        )

    # Cap total signals
    if len(candidates) > max_buy_signals:
        logger.info(
            "Portfolio: capping total signals from %d → %d",
            len(candidates), max_buy_signals,
        )
        candidates = candidates.iloc[:max_buy_signals].copy()

    # Log final signal breakdown AFTER caps
    final_sb = int((candidates["action_v2"] == "STRONG_BUY").sum())
    final_buy = int((candidates["action_v2"] == "BUY").sum())
    logger.info(
        "Portfolio signals after cap: %d STRONG_BUY + %d BUY = %d total",
        final_sb, final_buy, len(candidates),
    )

    # ── 5. greedy selection with sector/theme constraints ─────────────────────
    selected_indices: list[int] = []
    sector_weight_used: dict[str, float] = {}
    theme_count: dict[str, int] = {}

    # Equal weight target per position
    equal_weight = 1.0 / max_positions

    for idx in candidates.index:
        if len(selected_indices) >= max_positions:
            break

        row = candidates.loc[idx]
        ticker = str(row.get("ticker", ""))
        sector = str(row.get("sector", "Unknown"))
        theme = str(row.get("theme", "Unknown"))
        sect_regime = str(row.get("sectrsregime", "unknown")).lower()

        # ── sector cap (rotation-aware) ───────────────────────────────────
        effective_cap = _effective_sector_cap(sect_regime, max_sector_weight)
        current_sector_w = sector_weight_used.get(sector, 0.0)

        if current_sector_w + equal_weight > effective_cap:
            room = effective_cap - current_sector_w
            if room < min_weight:
                logger.debug(
                    "Portfolio: skipping %s — sector %s at %.1f%% "
                    "(cap %.1f%% for %s regime)",
                    ticker, sector,
                    current_sector_w * 100, effective_cap * 100,
                    sect_regime,
                )
                continue

        # ── theme diversification ─────────────────────────────────────────
        if theme != "Unknown":
            current_theme_n = theme_count.get(theme, 0)
            if current_theme_n >= max_theme_names:
                logger.debug(
                    "Portfolio: skipping %s — theme %s already has %d names",
                    ticker, theme, current_theme_n,
                )
                continue

        # ── accept ────────────────────────────────────────────────────────
        selected_indices.append(idx)
        sector_weight_used[sector] = current_sector_w + equal_weight
        if theme != "Unknown":
            theme_count[theme] = theme_count.get(theme, 0) + 1

    if not selected_indices:
        logger.info("build_portfolio_v2: all candidates filtered by constraints")
        return _empty_portfolio(breadth_regime, vol_regime)

    selected = candidates.loc[selected_indices].copy()

    # ── 6. EQUAL WEIGHT allocation ───────────────────────────────────────────
    # Simple 1/N weighting. In real life, the operator decides sizing.
    n_selected = len(selected)
    base_weight = 1.0 / n_selected

    # ── 7. apply exposure scaling (leading indicator) ─────────────────────────
    gross_target = _target_exposure(breadth_regime, vol_regime)

    final_weights = pd.Series(
        base_weight * gross_target, index=selected.index
    )

    # Drop names below minimum weight (shouldn't happen with equal weight, but safety)
    keep_mask = final_weights >= min_weight
    if not keep_mask.all():
        dropped = selected.loc[~keep_mask, "ticker"].tolist()
        logger.info(
            "Portfolio: dropping %d names below min_weight %.2f%%: %s",
            len(dropped), min_weight * 100, dropped,
        )
        selected = selected.loc[keep_mask].copy()
        final_weights = final_weights.loc[keep_mask]

    selected["portfolio_weight"] = final_weights.values
    selected["portfolio_weight_pct"] = (final_weights.values * 100).round(2)

    # ── 8. enrich with selection metadata ─────────────────────────────────────
    selected["selection_rank"] = range(1, len(selected) + 1)

    selected["effective_sector_cap"] = selected.apply(
        lambda r: _effective_sector_cap(
            str(r.get("sectrsregime", "unknown")).lower(),
            max_sector_weight,
        ),
        axis=1,
    )

    def _reason(r):
        parts = [
            str(r.get("action_v2", "")),
            f"rank={r.get('selection_rank', '?')}",
            f"score={_safe_float(r.get(score_col), 0):.3f}",
            f"sect={r.get('sector', 'Unknown')}({r.get('sectrsregime', 'unknown')})",
            f"w={r.get('portfolio_weight_pct', 0):.1f}%",
        ]
        rsi_val = _safe_float(r.get("rsi14"), -1)
        if rsi_val > 0:
            parts.append(f"rsi={rsi_val:.1f}")
        return " | ".join(parts)

    selected["selection_reason"] = selected.apply(_reason, axis=1)

    # Clean up internal columns
    selected = selected.drop(
        columns=["_tier", "_sort_score", "_action_priority",
                 "_rank_order", "_pipeline_action"],
        errors="ignore",
    )
    selected = selected.reset_index(drop=True)

    # ── 9. build sector tilt summary ──────────────────────────────────────────
    sector_tilt = _build_sector_tilt(selected, max_sector_weight)

    # ── 10. build rotation exposure summary ───────────────────────────────────
    rotation_exposure = _build_rotation_exposure(selected)

    # ── 11. meta ──────────────────────────────────────────────────────────────
    meta = {
        "selected_count": len(selected),
        "candidate_count": len(candidates),
        "total_universe": len(action_table),
        "target_exposure": gross_target,
        "actual_exposure": round(float(selected["portfolio_weight"].sum()), 4),
        "cash_reserve": round(
            1.0 - float(selected["portfolio_weight"].sum()), 4,
        ),
        "breadth_regime": breadth_regime,
        "vol_regime": vol_regime,
        "max_positions": max_positions,
        "strong_buy_limit": strong_buy_limit,
        "max_buy_signals": max_buy_signals,
        "strong_buy_count": int(
            (selected["action_v2"] == "STRONG_BUY").sum()
        ),
        "buy_count": int((selected["action_v2"] == "BUY").sum()),
        "max_sector_weight_base": max_sector_weight,
        "max_single_weight": max_single_weight,
        "max_theme_names": max_theme_names,
        "sector_tilt": sector_tilt,
        "rotation_exposure": rotation_exposure,
    }

    # ── 12. logging ───────────────────────────────────────────────────────────
    logger.info(
        "Portfolio built: %d names (%d STRONG_BUY + %d BUY)  "
        "exposure=%.1f%%  cash=%.1f%%  breadth=%s  vol=%s",
        meta["selected_count"],
        meta["strong_buy_count"],
        meta["buy_count"],
        meta["actual_exposure"] * 100,
        meta["cash_reserve"] * 100,
        breadth_regime,
        vol_regime,
    )

    # Log which STRONG_BUY names made it in (for verification)
    sb_in_portfolio = selected.loc[
        selected["action_v2"] == "STRONG_BUY", "ticker"
    ].tolist()
    buy_in_portfolio = selected.loc[
        selected["action_v2"] == "BUY", "ticker"
    ].tolist()
    if sb_in_portfolio:
        logger.info(
            "STRONG_BUY in portfolio (%d): %s",
            len(sb_in_portfolio), sb_in_portfolio,
        )
    if buy_in_portfolio:
        logger.info(
            "BUY in portfolio (%d): %s",
            len(buy_in_portfolio), buy_in_portfolio,
        )

    if sector_tilt:
        logger.info("Sector tilt:")
        for entry in sector_tilt:
            logger.info(
                "  %-20s  regime=%-10s  weight=%5.1f%%  cap=%5.1f%%  "
                "names=%d",
                entry["sector"],
                entry["regime"],
                entry["weight_pct"],
                entry["effective_cap_pct"],
                entry["count"],
            )

    if rotation_exposure:
        logger.info("Rotation quadrant exposure:")
        for entry in rotation_exposure:
            logger.info(
                "  %-10s  weight=%5.1f%%  names=%d",
                entry["quadrant"],
                entry["weight_pct"],
                entry["count"],
            )

    if logger.isEnabledFor(logging.DEBUG):
        preview_cols = [c for c in [
            "selection_rank", "ticker", "action_v2", "portfolio_weight_pct",
            score_col, "sector", "sectrsregime", "rsi14",
            "effective_sector_cap", "theme",
            "selection_reason",
        ] if c in selected.columns]
        logger.debug(
            "Portfolio detail:\n%s",
            selected[preview_cols].to_string(index=False),
        )

    return {
        "selected": selected,
        "meta": meta,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Sector tilt summary
# ═══════════════════════════════════════════════════════════════════════════════

def _build_sector_tilt(
    selected: pd.DataFrame,
    base_cap: float,
) -> list[dict]:
    """Build a summary of portfolio weight by sector."""
    if selected.empty or "sector" not in selected.columns:
        return []

    rows = []
    for sector in sorted(selected["sector"].unique()):
        mask = selected["sector"] == sector
        subset = selected.loc[mask]
        weight = float(subset["portfolio_weight"].sum())
        count = len(subset)

        if "sectrsregime" in subset.columns:
            regimes = subset["sectrsregime"].astype(str).str.lower()
            regime = regimes.mode().iloc[0] if not regimes.empty else "unknown"
        else:
            regime = "unknown"

        cap = _effective_sector_cap(regime, base_cap)

        rows.append({
            "sector": sector,
            "regime": regime,
            "weight": round(weight, 4),
            "weight_pct": round(weight * 100, 1),
            "effective_cap": round(cap, 4),
            "effective_cap_pct": round(cap * 100, 1),
            "count": count,
            "headroom_pct": round((cap - weight) * 100, 1),
        })

    return sorted(rows, key=lambda r: r["weight"], reverse=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Rotation exposure summary
# ═══════════════════════════════════════════════════════════════════════════════

def _build_rotation_exposure(selected: pd.DataFrame) -> list[dict]:
    """Summarise portfolio weight by rotation quadrant."""
    if selected.empty or "sectrsregime" not in selected.columns:
        return []

    rows = []
    for quadrant in ("leading", "improving", "weakening", "lagging", "unknown"):
        mask = selected["sectrsregime"].astype(str).str.lower() == quadrant
        subset = selected.loc[mask]
        if subset.empty:
            continue
        weight = float(subset["portfolio_weight"].sum())
        rows.append({
            "quadrant": quadrant,
            "weight": round(weight, 4),
            "weight_pct": round(weight * 100, 1),
            "count": len(subset),
            "tickers": sorted(subset["ticker"].tolist()),
        })

    return sorted(rows, key=lambda r: r["weight"], reverse=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Empty result
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_portfolio(breadth_regime: str, vol_regime: str) -> dict:
    return {
        "selected": pd.DataFrame(),
        "meta": {
            "selected_count": 0,
            "candidate_count": 0,
            "total_universe": 0,
            "target_exposure": _target_exposure(breadth_regime, vol_regime),
            "actual_exposure": 0.0,
            "cash_reserve": 1.0,
            "breadth_regime": breadth_regime,
            "vol_regime": vol_regime,
            "max_positions": DEFAULT_MAX_POSITIONS,
            "strong_buy_limit": STRONG_BUY_LIMIT,
            "max_buy_signals": MAX_BUY_SIGNALS,
            "strong_buy_count": 0,
            "buy_count": 0,
            "max_sector_weight_base": DEFAULT_MAX_SECTOR_WEIGHT,
            "max_single_weight": DEFAULT_MAX_SINGLE_WEIGHT,
            "max_theme_names": DEFAULT_MAX_THEME_NAMES,
            "sector_tilt": [],
            "rotation_exposure": [],
        },
    }