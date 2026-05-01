"""
refactor/strategy/portfolio_v2.py

Portfolio construction for the v2 pipeline.

Takes the scored-and-actioned universe and selects a concentrated
portfolio of up to ``max_positions`` names, subject to:

    1.  Only STRONG_BUY and BUY actions are eligible.
    2.  Sector concentration caps are *dynamic* — sectors in the
        ``leading`` or ``improving`` rotation quadrant receive the full
        ``max_sector_weight``, while ``weakening`` sectors get 60 % of
        the cap and ``lagging`` sectors get 30 %.
    3.  Theme diversification: no more than ``max_theme_names`` names
        from any single theme.
    4.  Position sizing is inverse-ATR weighted (lower volatility →
        larger weight), then capped per name at ``max_single_weight``.
    5.  Total exposure is scaled by a market-regime multiplier derived
        from the breadth and volatility regime columns already present
        on every row.

Output
------
``build_portfolio_v2`` returns a dict with:

    selected   – DataFrame of the final portfolio with per-name
                 weights, risk metrics, and selection reasoning.
    meta       – dict with portfolio-level summary statistics:
                 selected_count, candidate_count, target_exposure,
                 breadth_regime, vol_regime, sector_tilt, etc.

Design notes
------------
The builder is intentionally *stateless* — it takes a single snapshot
and produces a target portfolio.  It does not track prior holdings,
turnover cost, or rebalancing frequency.  Those concerns belong in
an execution layer that compares today's target with yesterday's
holdings.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_MAX_POSITIONS = 8
DEFAULT_MAX_SECTOR_WEIGHT = 0.35
DEFAULT_MAX_THEME_NAMES = 2
DEFAULT_MAX_SINGLE_WEIGHT = 0.20
DEFAULT_MIN_WEIGHT = 0.04

# ── Rotation-aware sector cap multipliers ─────────────────────────────────────
ROTATION_CAP_MULT: dict[str, float] = {
    "leading":   1.00,
    "improving": 1.00,
    "weakening": 0.85,
    "lagging":   0.60,
    "unknown":   0.85,
}

# ── Exposure scaling by market regime ─────────────────────────────────────────
# The product of breadth and vol multipliers gives target gross
# exposure.  In a strong/calm environment the portfolio can be fully
# invested; in a weak/chaotic one it scales down significantly.
BREADTH_EXPOSURE: dict[str, float] = {
    "strong": 1.00, "healthy": 1.00, "moderate": 0.95,
    "neutral": 0.92, "mixed": 0.88, "narrow": 0.82,
    "weak": 0.75, "critical": 0.60, "unknown": 0.90,
}
VOL_EXPOSURE: dict[str, float] = {
    "calm": 1.00, "low": 1.00, "normal": 0.97, "moderate": 0.95,
    "elevated": 0.88, "stressed": 0.75, "chaotic": 0.60,
    "unknown": 0.92,
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

    Returns a value in roughly [0.13, 1.00].
    """
    b = BREADTH_EXPOSURE.get(str(breadth).lower(), BREADTH_EXPOSURE["unknown"])
    v = VOL_EXPOSURE.get(str(vol).lower(), VOL_EXPOSURE["unknown"])
    return round(b * v, 4)


def _inverse_atr_weights(atr_pct_series: pd.Series) -> pd.Series:
    """
    Compute raw inverse-ATR weights.

    Names with lower ATR% get proportionally larger weights.
    A floor of 0.005 prevents division by zero for ultra-low-vol names.
    """
    clamped = atr_pct_series.clip(lower=0.005).fillna(0.03)
    inv = 1.0 / clamped
    total = inv.sum()
    if total <= 0:
        return pd.Series(1.0 / len(clamped), index=clamped.index)
    return inv / total


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
) -> dict:
    """
    Build a concentrated target portfolio from the actioned universe.

    Parameters
    ----------
    action_table : DataFrame
        Output of the action generator.  Must contain ``action_v2``,
        ``ticker``, and scoring columns.  Expected but optional:
        ``scoreadjusted_v2``, ``score_percentile_v2``, ``atr14pct``,
        ``sector``, ``theme``, ``sectrsregime``, ``breadthregime``,
        ``volregime``, ``conviction_v2``.
    max_positions : int
        Maximum number of names in the final portfolio.
    max_sector_weight : float
        Base maximum weight for any single GICS sector (before
        rotation-regime adjustment).
    max_theme_names : int
        Maximum number of names from any single theme.
    max_single_weight : float
        Hard cap on any individual position.
    min_weight : float
        Minimum weight for inclusion.  Names that would receive less
        than this after all caps are applied are dropped.

    Returns
    -------
    dict
        ``selected`` (DataFrame) and ``meta`` (dict).
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
        "Portfolio candidates: %d STRONG_BUY + BUY out of %d total",
        len(candidates), len(action_table),
    )

    # ── 2. rank candidates ────────────────────────────────────────────────────
    # Primary sort: action tier (STRONG_BUY first), then adjusted score
    tier_map = {"STRONG_BUY": 1, "BUY": 2}
    candidates["_tier"] = candidates["action_v2"].map(tier_map).fillna(3).astype(int)

    score_col = (
        "scoreadjusted_v2"
        if "scoreadjusted_v2" in candidates.columns
        else "scorecomposite_v2"
    )
    candidates["_sort_score"] = (
        pd.to_numeric(candidates.get(score_col, 0.0), errors="coerce")
        .fillna(0.0)
    )
    candidates = candidates.sort_values(
        ["_tier", "_sort_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    # ── 3. greedy selection with constraints ──────────────────────────────────
    selected_indices: list[int] = []
    sector_weight_used: dict[str, float] = {}
    theme_count: dict[str, int] = {}

    # Pre-compute inverse-ATR raw weights for the full candidate pool
    atr_col = (
        pd.to_numeric(candidates.get("atr14pct", 0.03), errors="coerce")
        .fillna(0.03)
    )
    raw_weights = _inverse_atr_weights(atr_col)

    for idx in candidates.index:
        if len(selected_indices) >= max_positions:
            break

        row = candidates.loc[idx]
        ticker = str(row.get("ticker", ""))
        sector = str(row.get("sector", "Unknown"))
        theme = str(row.get("theme", "Unknown"))
        sect_regime = str(row.get("sectrsregime", "unknown")).lower()
        raw_w = float(raw_weights.loc[idx])

        # ── sector cap (rotation-aware) ───────────────────────────────────
        effective_cap = _effective_sector_cap(sect_regime, max_sector_weight)
        current_sector_w = sector_weight_used.get(sector, 0.0)
        proposed_w = min(raw_w, max_single_weight)

        if current_sector_w + proposed_w > effective_cap:
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
            proposed_w = room

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
        sector_weight_used[sector] = current_sector_w + proposed_w
        if theme != "Unknown":
            theme_count[theme] = theme_count.get(theme, 0) + 1

    if not selected_indices:
        logger.info("build_portfolio_v2: all candidates filtered by constraints")
        return _empty_portfolio(breadth_regime, vol_regime)

    selected = candidates.loc[selected_indices].copy()

    # ── 4. final weight calculation ───────────────────────────────────────────
    # Re-derive inverse-ATR weights among selected names only
    sel_atr = (
        pd.to_numeric(selected.get("atr14pct", 0.03), errors="coerce")
        .fillna(0.03)
    )
    sel_raw_w = _inverse_atr_weights(sel_atr)

    # Apply per-name and per-sector caps iteratively
    final_w = sel_raw_w.clip(upper=max_single_weight).copy()

    for _ in range(5):  # iterate to redistribute excess
        # enforce rotation-aware sector caps
        for sector in selected["sector"].unique():
            sect_mask = selected["sector"] == sector
            sector_total = float(final_w.loc[sect_mask].sum())
            # all names in this sector share the same regime in practice,
            # but take the mode for safety
            regimes = (
                selected.loc[sect_mask, "sectrsregime"]
                .astype(str).str.lower()
            )
            regime_mode = regimes.mode().iloc[0] if not regimes.empty else "unknown"
            cap = _effective_sector_cap(regime_mode, max_sector_weight)

            if sector_total > cap and sector_total > 0:
                scale = cap / sector_total
                final_w.loc[sect_mask] *= scale

        # enforce single-name cap
        final_w = final_w.clip(upper=max_single_weight)

        # renormalise so total = 1
        total = final_w.sum()
        if total > 0:
            final_w = final_w / total
        else:
            final_w = pd.Series(
                1.0 / len(final_w), index=final_w.index,
            )

    # ── 5. apply exposure scaling ─────────────────────────────────────────────
    gross_target = _target_exposure(breadth_regime, vol_regime)
    final_w = final_w * gross_target

    # Drop names below minimum weight
    keep_mask = final_w >= min_weight
    if not keep_mask.all():
        dropped = selected.loc[~keep_mask, "ticker"].tolist()
        logger.info(
            "Portfolio: dropping %d names below min_weight %.2f%%: %s",
            len(dropped), min_weight * 100, dropped,
        )
        selected = selected.loc[keep_mask].copy()
        final_w = final_w.loc[keep_mask]

        # Renormalise to target exposure
        if final_w.sum() > 0:
            final_w = final_w / final_w.sum() * gross_target

    selected["portfolio_weight"] = final_w.values
    selected["portfolio_weight_pct"] = (final_w.values * 100).round(2)

    # ── 6. enrich with selection metadata ─────────────────────────────────────
    selected["selection_rank"] = range(1, len(selected) + 1)

    # per-name effective sector cap for transparency
    selected["effective_sector_cap"] = selected.apply(
        lambda r: _effective_sector_cap(
            str(r.get("sectrsregime", "unknown")).lower(),
            max_sector_weight,
        ),
        axis=1,
    )

    # selection reason
    def _reason(r):
        parts = [
            str(r.get("action_v2", "")),
            f"score={_safe_float(r.get(score_col), 0):.3f}",
            f"atr={_safe_float(r.get('atr14pct'), 0):.4f}",
            f"sect={r.get('sector', 'Unknown')}({r.get('sectrsregime', 'unknown')})",
            f"w={r.get('portfolio_weight_pct', 0):.1f}%",
        ]
        return " | ".join(parts)

    selected["selection_reason"] = selected.apply(_reason, axis=1)

    # Clean up internal sort columns
    selected = selected.drop(columns=["_tier", "_sort_score"], errors="ignore")
    selected = selected.reset_index(drop=True)

    # ── 7. build sector tilt summary ──────────────────────────────────────────
    sector_tilt = _build_sector_tilt(selected, max_sector_weight)

    # ── 8. build rotation exposure summary ────────────────────────────────────
    rotation_exposure = _build_rotation_exposure(selected)

    # ── 9. meta ───────────────────────────────────────────────────────────────
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
        "max_sector_weight_base": max_sector_weight,
        "max_single_weight": max_single_weight,
        "max_theme_names": max_theme_names,
        "sector_tilt": sector_tilt,
        "rotation_exposure": rotation_exposure,
    }

    # ── 10. logging ───────────────────────────────────────────────────────────
    logger.info(
        "Portfolio built: %d names  exposure=%.1f%%  cash=%.1f%%  "
        "breadth=%s  vol=%s",
        meta["selected_count"],
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
            score_col, "atr14pct", "sector", "sectrsregime",
            "effective_sector_cap", "theme", "conviction_v2",
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
    """
    Build a summary of portfolio weight by sector, including the
    effective rotation-adjusted cap for each.
    """
    if selected.empty or "sector" not in selected.columns:
        return []

    rows = []
    for sector in sorted(selected["sector"].unique()):
        mask = selected["sector"] == sector
        subset = selected.loc[mask]
        weight = float(subset["portfolio_weight"].sum())
        count = len(subset)

        # Determine the sector's rotation regime (mode among selected names)
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
    """
    Summarise portfolio weight by rotation quadrant.

    This gives a single-glance view of how the portfolio is positioned
    relative to the sector rotation cycle.
    """
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
            "max_sector_weight_base": DEFAULT_MAX_SECTOR_WEIGHT,
            "max_single_weight": DEFAULT_MAX_SINGLE_WEIGHT,
            "max_theme_names": DEFAULT_MAX_THEME_NAMES,
            "sector_tilt": [],
            "rotation_exposure": [],
        },
    }