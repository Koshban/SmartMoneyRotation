"""
backtest/strategies.py
----------------------
Predefined strategy parameter variants for comparison.

IMPORTANT: The baseline now uses tuned defaults that reduce
churning.  The original "academic" defaults that caused -5%
CAGR are preserved as ORIGINAL_DEFAULTS for reference.
"""

from __future__ import annotations

from backtest.engine import StrategyConfig
from common.universe import SECTORS, BROAD_MARKET, FIXED_INCOME, COMMODITIES


# ═══════════════════════════════════════════════════════════════
#  BASELINE — tuned defaults (reduced churn)
# ═══════════════════════════════════════════════════════════════

BASELINE = StrategyConfig(
    name="baseline",
    description="Tuned CASH defaults — wide hysteresis, low churn",
)


# ═══════════════════════════════════════════════════════════════
#  ORIGINAL DEFAULTS — what shipped originally (for reference)
# ═══════════════════════════════════════════════════════════════

ORIGINAL_DEFAULTS = StrategyConfig(
    name="original_defaults",
    description="Original parameters (high churn — for comparison only)",
    signal_params={
        "entry_score_min":     0.60,
        "exit_score_max":      0.40,
        "confirmation_streak": 3,
        "cooldown_days":       5,
        "max_position_pct":    0.08,
        "min_position_pct":    0.02,
        "base_position_pct":   0.05,
        "max_positions":       15,
    },
    portfolio_params={
        "max_positions":       15,
        "max_single_pct":      0.08,
        "min_single_pct":      0.02,
        "target_invested_pct": 0.95,
        "rebalance_threshold": 0.015,
        "incumbent_bonus":     0.02,
    },
)


# ═══════════════════════════════════════════════════════════════
#  MOMENTUM HEAVY — overweight momentum pillar
# ═══════════════════════════════════════════════════════════════

MOMENTUM_HEAVY = StrategyConfig(
    name="momentum_heavy",
    description="Overweight momentum pillar (40%), wider entry",
    scoring_weights={
        "pillar_rotation":       0.20,
        "pillar_momentum":       0.40,
        "pillar_volatility":     0.10,
        "pillar_microstructure": 0.20,
        "pillar_breadth":        0.10,
    },
    signal_params={
        "entry_score_min":     0.50,
        "exit_score_max":      0.30,
        "confirmation_streak": 2,
        "cooldown_days":       20,
    },
)


# ═══════════════════════════════════════════════════════════════
#  CONSERVATIVE — higher bar, defensive
# ═══════════════════════════════════════════════════════════════

CONSERVATIVE = StrategyConfig(
    name="conservative",
    description="High conviction only, strong hold bias",
    signal_params={
        "entry_score_min":     0.65,
        "exit_score_max":      0.40,
        "confirmation_streak": 4,
        "cooldown_days":       25,
        "max_position_pct":    0.12,
        "max_positions":       6,
    },
    portfolio_params={
        "max_positions":       6,
        "max_single_pct":      0.12,
        "target_invested_pct": 0.75,
        "incumbent_bonus":     0.08,
    },
    breadth_portfolio={
        "strong_exposure":     0.85,
        "neutral_exposure":    0.60,
        "weak_exposure":       0.25,
        "weak_block_new":      True,
        "weak_raise_entry":    0.10,
        "neutral_raise_entry": 0.05,
    },
)


# ═══════════════════════════════════════════════════════════════
#  BROAD DIVERSIFIED — more positions, equal weight
# ═══════════════════════════════════════════════════════════════

BROAD_DIVERSIFIED = StrategyConfig(
    name="broad_diversified",
    description="12 positions, equal weight, wide net, low turnover",
    signal_params={
        "entry_score_min":     0.48,
        "exit_score_max":      0.30,
        "cooldown_days":       20,
        "max_positions":       12,
        "max_position_pct":    0.12,
    },
    portfolio_params={
        "max_positions":       12,
        "max_single_pct":      0.12,
        "min_single_pct":      0.03,
        "max_sector_pct":      0.40,
        "target_invested_pct": 0.92,
        "incumbent_bonus":     0.06,
    },
    sizing_config_overrides={
        "method":           "equal_weight",
        "max_position_pct": 0.12,
        "min_position_pct": 0.03,
    },
)


# ═══════════════════════════════════════════════════════════════
#  CONCENTRATED — top 3 only
# ═══════════════════════════════════════════════════════════════

CONCENTRATED = StrategyConfig(
    name="concentrated",
    description="Top 3 high-conviction, strong hold bias",
    signal_params={
        "entry_score_min":     0.62,
        "exit_score_max":      0.38,
        "confirmation_streak": 3,
        "cooldown_days":       30,
        "max_positions":       3,
        "max_position_pct":    0.30,
    },
    portfolio_params={
        "max_positions":       3,
        "max_single_pct":      0.30,
        "min_single_pct":      0.10,
        "max_sector_pct":      0.50,
        "target_invested_pct": 0.85,
        "incumbent_bonus":     0.10,
    },
)


# ═══════════════════════════════════════════════════════════════
#  RISK PARITY — volatility-scaled
# ═══════════════════════════════════════════════════════════════

RISK_PARITY = StrategyConfig(
    name="risk_parity",
    description="Inverse-volatility sizing, moderate turnover",
    signal_params={
        "cooldown_days":       20,
    },
    portfolio_params={
        "max_positions":       8,
        "max_single_pct":      0.18,
        "target_invested_pct": 0.88,
        "incumbent_bonus":     0.06,
    },
    sizing_config_overrides={
        "method":           "risk_parity",
        "max_position_pct": 0.18,
        "min_position_pct": 0.04,
    },
)


# ═══════════════════════════════════════════════════════════════
#  ROTATION PURE — RS-dominant
# ═══════════════════════════════════════════════════════════════

ROTATION_PURE = StrategyConfig(
    name="rotation_pure",
    description="Rotation pillar dominant (45%), RS-driven",
    scoring_weights={
        "pillar_rotation":       0.45,
        "pillar_momentum":       0.20,
        "pillar_volatility":     0.10,
        "pillar_microstructure": 0.15,
        "pillar_breadth":        0.10,
    },
    scoring_params={
        "rs_zscore_w":      0.45,
        "rs_regime_w":      0.30,
        "rs_momentum_w":    0.15,
        "rs_vol_confirm_w": 0.10,
    },
    signal_params={
        "cooldown_days":    20,
    },
    portfolio_params={
        "incumbent_bonus":  0.06,
    },
)


# ═══════════════════════════════════════════════════════════════
#  SECTOR ROTATION — sector ETFs only
# ═══════════════════════════════════════════════════════════════

_SECTOR_UNIVERSE = list(SECTORS) + ["SPY"]

SECTOR_ROTATION = StrategyConfig(
    name="sector_rotation",
    description="Pure sector ETF rotation (11 sectors)",
    universe_filter=_SECTOR_UNIVERSE,
    signal_params={
        "entry_score_min":     0.52,
        "exit_score_max":      0.32,
        "cooldown_days":       20,
        "max_positions":       4,
    },
    portfolio_params={
        "max_positions":       4,
        "max_single_pct":      0.25,
        "max_sector_pct":      0.30,
        "target_invested_pct": 0.92,
        "incumbent_bonus":     0.08,
    },
)


# ═══════════════════════════════════════════════════════════════
#  ALL-WEATHER — cross-asset
# ═══════════════════════════════════════════════════════════════

_ALL_WEATHER_UNI = (
    list(BROAD_MARKET) + list(SECTORS)
    + list(FIXED_INCOME) + list(COMMODITIES)
    + ["SPY"]
)

ALL_WEATHER = StrategyConfig(
    name="all_weather",
    description="Cross-asset rotation: equities + bonds + commodities",
    universe_filter=list(set(_ALL_WEATHER_UNI)),
    signal_params={
        "entry_score_min":     0.50,
        "exit_score_max":      0.32,
        "cooldown_days":       20,
        "max_positions":       8,
    },
    portfolio_params={
        "max_positions":       8,
        "max_single_pct":      0.18,
        "max_sector_pct":      0.40,
        "target_invested_pct": 0.90,
        "incumbent_bonus":     0.06,
    },
)


# ═══════════════════════════════════════════════════════════════
#  MONTHLY REBALANCE — trade less often
# ═══════════════════════════════════════════════════════════════

MONTHLY_REBALANCE = StrategyConfig(
    name="monthly_rebalance",
    description="Rebalance only on large drift (simulates monthly)",
    signal_params={
        "entry_score_min":     0.52,
        "exit_score_max":      0.30,
        "cooldown_days":       25,
    },
    portfolio_params={
        "max_positions":       8,
        "max_single_pct":      0.15,
        "target_invested_pct": 0.90,
        "rebalance_threshold": 0.08,
        "incumbent_bonus":     0.08,
    },
    backtest_config_overrides={
        "rebalance_holds": False,
        "drift_threshold": 0.20,
        "min_trade_pct":   0.05,
    },
)


# ═══════════════════════════════════════════════════════════════
#  REGISTRY
# ═══════════════════════════════════════════════════════════════

ALL_STRATEGIES: dict[str, StrategyConfig] = {
    "baseline":           BASELINE,
    "original_defaults":  ORIGINAL_DEFAULTS,
    "momentum_heavy":     MOMENTUM_HEAVY,
    "conservative":       CONSERVATIVE,
    "broad_diversified":  BROAD_DIVERSIFIED,
    "concentrated":       CONCENTRATED,
    "risk_parity":        RISK_PARITY,
    "rotation_pure":      ROTATION_PURE,
    "sector_rotation":    SECTOR_ROTATION,
    "all_weather":        ALL_WEATHER,
    "monthly_rebalance":  MONTHLY_REBALANCE,
}


def get_strategy(name: str) -> StrategyConfig:
    """Look up a strategy by name.  Raises KeyError if not found."""
    if name not in ALL_STRATEGIES:
        available = ", ".join(ALL_STRATEGIES.keys())
        raise KeyError(
            f"Unknown strategy '{name}'.  Available: {available}"
        )
    return ALL_STRATEGIES[name]


def list_strategies() -> list[str]:
    """Return available strategy names."""
    return list(ALL_STRATEGIES.keys())