""" refactor/common/config_refactor.py """
from __future__ import annotations
VOLREGIMEPARAMS = {"atrp_window": 14, "realized_vol_window": 20, "dispersion_window": 20, "gap_window": 20, "calm_atrp_max": 0.035, "volatile_atrp_max": 0.060, 
                   "calm_rvol_max": 0.28, "volatile_rvol_max": 0.42, "volatile_gap_rate": 0.18, "chaotic_gap_rate": 0.28, "calm_dispersion_max": 0.022, 
                   "volatile_dispersion_max": 0.040, "score_weights": {"atrp": 0.35, "realized_vol": 0.35, "gap_rate": 0.15, "dispersion": 0.15}}
SCORINGWEIGHTS_V2 = {"trend": 0.36, "participation": 0.18, "risk": 0.26, "regime": 0.20}
SCORINGPARAMS_V2 = {
    "trend": {
        "w_stock_rs": 0.42,
        "w_sector_rs": 0.28,
        "w_rs_accel": 0.15,
        "w_trend_confirm": 0.15,
    },
    "participation": {
        "w_rvol": 0.35,
        "w_obv": 0.25,
        "w_adline": 0.20,
        "w_dollar_volume": 0.20,
    },
    "risk": {
        "w_vol_penalty": 0.32,
        "w_liquidity_penalty": 0.23,
        "w_gap_penalty": 0.20,
        "w_extension_penalty": 0.25,
    },
    "regime": {
        "w_breadth": 0.65,
        "w_vol_regime": 0.35,
    },
    "penalties": {
        "rsi_soft_low": 40.0,
        "rsi_soft_high": 76.0,
        "adx_soft_min": 18.0,
        "atrp_high": 0.065,
        "extension_warn": 0.10,
        "extension_bad": 0.18,
        "illiquidity_bad": 0.012,
    },
}
SIGNALPARAMS_V2 = {
    "base_entry_threshold": 0.60,
    "base_exit_threshold": 0.44,
    "allowed_rs_regimes": ("leading", "improving"),
    "blocked_sector_regimes": ("lagging",),
    "hard_block_breadth_regimes": ("critical",),
    "hard_block_vol_regimes": ("chaotic",),
    "continuation_min_trend": 0.64,
    "pullback_min_trend": 0.70,
    "pullback_max_short_extension": 0.06,
    "pullback_rsi_max": 62.0,
    "cooldown_days": 4,
    "regime_entry_adjustment": {
        "calm": 0.00,
        "volatile": 0.04,
        "chaotic": 0.12,
    },
    "breadth_entry_adjustment": {
        "strong": -0.01,
        "neutral": 0.02,
        "weak": 0.08,
        "critical": 0.14,
        "unknown": 0.03,
    },
    "size_multipliers": {
        "calm": 1.00,
        "volatile": 0.65,
        "chaotic": 0.30,
    },
}
CONVERGENCEPARAMS_V2 = {
    "tiers": {
        "aligned_long": 4,
        "rotation_long_only": 3,
        "score_long_only": 2,
        "mixed": 1,
        "avoid": 0,
    },
    "adjustments": {
        "calm": 0.04,
        "volatile": 0.01,
        "chaotic": 0.00,
    },
}

###################################################

""" refactor/common/market_config_v2.py"""
from __future__ import annotations

from common.universe import (
    ETF_UNIVERSE,
    HK_UNIVERSE,
    INDIA_UNIVERSE,
    get_all_single_names,
    get_hk_only,
    get_india_only,
    is_hk_ticker,
    is_india_ticker,
)


def _get_us_equities() -> list[str]:
    return sorted(s for s in get_all_single_names() if not is_hk_ticker(s) and not is_india_ticker(s))


def _get_us_tradable() -> list[str]:
    return sorted(set(list(ETF_UNIVERSE) + _get_us_equities()))


def _get_hk_tradable() -> list[str]:
    return sorted(set(get_hk_only()))


def _get_india_tradable() -> list[str]:
    return sorted(set(get_india_only()))


MARKET_CONFIG_V2 = {
    "US": {
        "market": "US",
        "benchmark": "SPY",
        "leadership_universe_fn": lambda: list(ETF_UNIVERSE),
        "tradable_universe_fn": _get_us_tradable,
        "enable_sector_rotation": True,
        "enable_breadth": True,
        "enable_vol_regime": True,
        "max_positions": 12,
        "max_sector_weight": 0.30,
        "max_theme_names": 3,
    },
    "HK": {
        "market": "HK",
        "benchmark": "2800.HK",
        "leadership_universe_fn": lambda: [s for s in HK_UNIVERSE if s.endswith('.HK')][: min(8, len(HK_UNIVERSE))],
        "tradable_universe_fn": _get_hk_tradable,
        "enable_sector_rotation": False,
        "enable_breadth": False,
        "enable_vol_regime": True,
        "max_positions": 10,
        "max_sector_weight": 0.35,
        "max_theme_names": 3,
    },
    "IN": {
        "market": "IN",
        "benchmark": "NIFTYBEES.NS",
        "leadership_universe_fn": lambda: [s for s in INDIA_UNIVERSE if s.endswith('.NS')][: min(10, len(INDIA_UNIVERSE))],
        "tradable_universe_fn": _get_india_tradable,
        "enable_sector_rotation": False,
        "enable_breadth": False,
        "enable_vol_regime": True,
        "max_positions": 12,
        "max_sector_weight": 0.30,
        "max_theme_names": 3,
    },
}


def get_market_config_v2(market: str) -> dict:
    m = market.upper()
    if m not in MARKET_CONFIG_V2:
        raise ValueError(f"Unknown market {market!r}")
    cfg = dict(MARKET_CONFIG_V2[m])
    cfg["leadership_universe"] = list(cfg["leadership_universe_fn"]())
    cfg["tradable_universe"] = list(cfg["tradable_universe_fn"]())
    return cfg

#################################################

""" refactor/common/universe_loader_v2.py """
from __future__ import annotations


def get_universe_for_market(market: str):
    from common.universe import get_universe_for_market as gufm
    return gufm(market)

###################################################

