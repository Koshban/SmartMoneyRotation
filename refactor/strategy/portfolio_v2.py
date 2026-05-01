"""
refactor/strategy/portfolio_v2.py

Portfolio construction for the v2 pipeline.

PURPOSE: Generate a CLEAN, LIMITED set of actionable signals.
    - STRONG_BUY: top 15 names by blended rank (the best of the best)
    - BUY: next 5-10 names (good but not top tier)
    - Everything else: not signalled

Design philosophy:
    - No parameter stuffing to beautify backtests
    - Equal-weight positions (operator decides sizing in real life)
    - Signal QUALITY over quantity
    - Breadth/vol regime scaling kept only as a leading indicator
      (it reflects CURRENT market health, not hindsight)

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
from refactor.strategy.ranking_v2 import rank_and_filter_candidates

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
# ROTATION_CAP_MULT: dict[str, float] = {
#     "leading":   1.00,
#     "improving": 1.00,
#     "weakening": 0.80,
#     "lagging":   0.55,
#     "unknown":   0.85,
# }

# ── Set all rotation caps to 1.0 (no penalizing lagging sectors) ──────────────
ROTATION_CAP_MULT: dict[str, float] = {
    "leading":   1.00,
    "improving": 1.00,
    "weakening": 1.00,
    "lagging":   1.00,
    "unknown":   1.00,
}

# ── Exposure scaling by market regime ─────────────────────────────────────────
# These ARE leading indicators: breadth divergence precedes crashes by weeks.
# When fewer stocks participate in a rally, it signals fragility BEFORE the drop.
# We keep this simple — not trying to time perfectly, just reducing exposure
# when the market's internal health is deteriorating.
# BREADTH_EXPOSURE: dict[str, float] = {
#     "strong": 1.00, "healthy": 1.00, "moderate": 0.95,
#     "neutral": 0.90, "mixed": 0.85, "narrow": 0.75,
#     "weak": 0.60, "critical": 0.40, "unknown": 0.90,
# }
# VOL_EXPOSURE: dict[str, float] = {
#     "calm": 1.00, "low": 1.00, "normal": 1.00, "moderate": 0.95,
#     "elevated": 0.85, "stressed": 0.70, "chaotic": 0.50,
#     "unknown": 0.95,
# }
# ── Set all exposure to 1.0 (no scaling) ─────────────────────────────────────
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

    The CORE job: produce at most `max_buy_signals` clean signals per day,
    with the top `strong_buy_limit` labelled STRONG_BUY and the rest as BUY.

    Parameters
    ----------
    action_table : DataFrame
        Output of the action generator.
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
        Maximum names that receive STRONG_BUY (top tier only).
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

    logger.info(
        "Portfolio candidates (raw): %d STRONG_BUY + BUY out of %d total",
        len(candidates), len(action_table),
    )

    # ── 2. RANK + FILTER candidates ──────────────────────────────────────────
    score_col = (
        "scoreadjusted_v2"
        if "scoreadjusted_v2" in candidates.columns
        else "scorecomposite_v2"
    )

    ranking_params = {
        "tilt": 0.50,
        "vol_pref": 0.30,
        "min_vol": 0.25,
        "min_rs": -0.5,
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

    # ── 3. HARD CAP: limit total signals ──────────────────────────────────────
    #
    # This is the MOST IMPORTANT step for real trading signal quality.
    # After ranking, we only keep the top `max_buy_signals` names.
    # Top `strong_buy_limit` get STRONG_BUY, the rest get BUY.
    #
    candidates = candidates.reset_index(drop=True)

    if len(candidates) > max_buy_signals:
        logger.info(
            "Portfolio: capping signals from %d → %d (STRONG_BUY=%d, BUY=%d)",
            len(candidates), max_buy_signals,
            strong_buy_limit, max_buy_signals - strong_buy_limit,
        )
        candidates = candidates.iloc[:max_buy_signals].copy()

    # Reassign action tiers based on RANK (not original pipeline action)
    # Top N = STRONG_BUY, rest = BUY
    new_actions = []
    for i in range(len(candidates)):
        if i < strong_buy_limit:
            new_actions.append("STRONG_BUY")
        else:
            new_actions.append("BUY")
    candidates["action_v2"] = new_actions

    logger.info(
        "Portfolio signals after cap: %d STRONG_BUY + %d BUY = %d total",
        sum(1 for a in new_actions if a == "STRONG_BUY"),
        sum(1 for a in new_actions if a == "BUY"),
        len(new_actions),
    )

    # ── 4. greedy selection with sector/theme constraints ─────────────────────
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

    # ── 5. EQUAL WEIGHT allocation ───────────────────────────────────────────
    # Simple 1/N weighting. In real life, the operator decides sizing.
    n_selected = len(selected)
    base_weight = 1.0 / n_selected

    # ── 6. apply exposure scaling (leading indicator) ─────────────────────────
    # Breadth + vol regime reflects CURRENT market health.
    # When breadth narrows (fewer stocks participating), it's a LEADING
    # signal of trouble — this happens BEFORE the crash, not after.
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

    # ── 7. enrich with selection metadata ─────────────────────────────────────
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
        return " | ".join(parts)

    selected["selection_reason"] = selected.apply(_reason, axis=1)

    # Clean up internal columns
    selected = selected.drop(
        columns=["_tier", "_sort_score"], errors="ignore"
    )
    selected = selected.reset_index(drop=True)

    # ── 8. build sector tilt summary ──────────────────────────────────────────
    sector_tilt = _build_sector_tilt(selected, max_sector_weight)

    # ── 9. build rotation exposure summary ────────────────────────────────────
    rotation_exposure = _build_rotation_exposure(selected)

    # ── 10. meta ──────────────────────────────────────────────────────────────
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

    # ── 11. logging ───────────────────────────────────────────────────────────
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
            score_col, "sector", "sectrsregime",
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