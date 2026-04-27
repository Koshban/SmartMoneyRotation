""" refactor/common/config_refactor.py """
from __future__ import annotations


# ═══════════════════════════════════════════════════════════════════════════════
# ██  LOOSE CONFIG  (uncomment this block and comment out the Tight block)   ██
# ═══════════════════════════════════════════════════════════════════════════════
# Looser generates more BUY signals, holds positions longer, takes larger
# positions, and is more tolerant of mediocre momentum / hostile regimes.

# VOLREGIMEPARAMS = {
#     "atrp_window": 14,
#     "realized_vol_window": 20,
#     "dispersion_window": 20,
#     "gap_window": 20,
#     "calm_atrp_max": 0.035,
#     "volatile_atrp_max": 0.060,
#     "calm_rvol_max": 0.28,
#     "volatile_rvol_max": 0.42,
#     "volatile_gap_rate": 0.18,
#     "chaotic_gap_rate": 0.28,
#     "calm_dispersion_max": 0.022,
#     "volatile_dispersion_max": 0.040,
#     "score_weights": {
#         "atrp": 0.35,
#         "realized_vol": 0.35,
#         "gap_rate": 0.15,
#         "dispersion": 0.15,
#     },
#     # FIX: regime label thresholds (higher = more tolerant)
#     "chaotic_threshold": 0.75,
#     "volatile_threshold": 0.35,
# }

# SCORINGWEIGHTS_V2 = {
#     "trend": 0.38,
#     "participation": 0.22,
#     "risk": 0.25,
#     "regime": 0.15,
# }

# SCORINGPARAMS_V2 = {
#     "trend": {
#         "w_stock_rs": 0.45,
#         "w_sector_rs": 0.25,
#         "w_rs_accel": 0.15,
#         "w_trend_confirm": 0.15,
#     },
#     "participation": {
#         "w_rvol": 0.35,
#         "w_obv": 0.30,
#         "w_adline": 0.20,
#         "w_dollar_volume": 0.15,
#     },
#     "risk": {
#         "w_vol_penalty": 0.35,
#         "w_liquidity_penalty": 0.25,
#         "w_gap_penalty": 0.20,
#         "w_extension_penalty": 0.20,
#     },
#     "regime": {
#         "w_breadth": 0.60,
#         "w_vol_regime": 0.40,
#     },
#     "penalties": {
#         "rsi_soft_low": 38.0,
#         "rsi_soft_high": 78.0,
#         "adx_soft_min": 16.0,
#         "atrp_high": 0.07,
#         "extension_warn": 0.12,
#         "extension_bad": 0.22,
#         "illiquidity_bad": 0.015,
#     },
# }

# SIGNALPARAMS_V2 = {
#     "base_entry_threshold": 0.58,
#     "base_exit_threshold": 0.42,
#     "allowed_rs_regimes": ("leading", "improving"),
#     "blocked_sector_regimes": ("lagging",),
#     "hard_block_breadth_regimes": ("critical",),
#     "hard_block_vol_regimes": ("chaotic",),
#     "continuation_min_trend": 0.62,
#     "pullback_min_trend": 0.68,
#     "pullback_max_short_extension": 0.04,
#     "pullback_rsi_max": 58.0,
#     "cooldown_days": 4,
#     # ── soft-penalty & rank gates (loose) ──
#     "rs_fail_penalty": 0.08,
#     "breadth_fail_penalty": 0.03,
#     "min_rank_pct": 0.80,
#     "relative_setup_rank_pct": 0.75,
#     "exit_rank_floor": 0.20,
#     "chaotic_exit_bump": 0.08,
#     "regime_entry_adjustment": {
#         "calm": 0.00,
#         "volatile": 0.03,
#         "chaotic": 0.10,
#     },
#     "breadth_entry_adjustment": {
#         "strong": -0.01,
#         "neutral": 0.02,
#         "weak": 0.07,
#         "critical": 0.12,
#         "unknown": 0.00,
#     },
#     "size_multipliers": {
#         "calm": 1.00,
#         "volatile": 0.70,
#         "chaotic": 0.35,
#     },
#     # ── FIX 4: position sizing (loose — larger positions allowed) ──
#     "position_base_pct": 0.04,
#     "position_range_pct": 0.08,
#     "position_max_pct": 0.12,
#     # ── FIX 5: pullback lower bound (loose — allows more stretched-down names) ──
#     "pullback_min_short_extension": -0.06,
#     # ── FIX 6: continuation participation floor (loose — lower bar) ──
#     "continuation_min_participation": 0.45,
# }

# CONVERGENCEPARAMS_V2 = {
#     "tiers": {
#         "aligned_long": 4,
#         "rotation_long_only": 3,
#         "score_long_only": 2,
#         "mixed": 1,
#         "avoid": 0,
#     },
#     "adjustments": {
#         "calm": 0.04,
#         "volatile": 0.02,
#         "chaotic": 0.00,
#     },
#     # ── FIX 7: rotation recommendation mapping ──
#     "rotation_rec_map": {
#         "leading":   "STRONGBUY",
#         "improving": "BUY",
#         "weakening": "HOLD",           # loose: weakening gets HOLD not SELL
#         "lagging":   "SELL",
#     },
#     "rotation_rec_default": "HOLD",
# }

# ACTIONPARAMS_V2 = {
#     # ── STRONG_BUY tier ──────────────────────────────────────
#     "strong_buy": {
#         "min_percentile": 0.90,
#         "min_score": 0.76,
#         "score_above_entry": 0.08,
#         "min_rvol": 1.10,
#         "requires_confirmation": True,
#         "requires_strong_context": True,
#         "blocks_overextended": True,
#     },
#     # ── BUY tier ─────────────────────────────────────────────
#     "buy": {
#         "min_percentile": 0.65,
#         "min_score": 0.62,
#         "score_above_entry": 0.02,
#         "requires_confirmation": True,
#         "requires_decent_momentum": True,
#         "blocks_weak_context": True,
#     },
#     # ── HOLD tier ────────────────────────────────────────────
#     "hold": {
#         "min_percentile": 0.35,
#         "min_score": 0.54,
#         "score_below_entry": 0.06,
#         "blocks_weak_context": True,
#     },
#     # ── SELL triggers ────────────────────────────────────────
#     "sell": {
#         "floor_score": 0.50,
#         "floor_percentile": 0.15,
#         "exit_score_below_entry": 0.05,
#         "exit_percentile_floor": 0.20,
#     },
#     # ── Context definitions ──────────────────────────────────
#     "strong_context": {
#         "breadth_regimes": ["strong"],
#         "vol_regimes": ["calm"],
#         "min_leadership": 0.60,
#     },
#     "weak_context": {
#         "breadth_regimes": ["weak", "critical"],
#         "vol_regimes": ["chaotic"],
#         "sector_regimes": ["lagging"],
#     },
#     # ── Momentum definitions ─────────────────────────────────
#     "healthy_momentum": {
#         "allowed_rs": ["leading", "improving"],
#         "blocked_sector": ["lagging"],
#         "min_rsi": 52,
#         "min_adx": 22,
#     },
#     "decent_momentum": {
#         "allowed_rs": ["leading", "improving"],
#         "min_rsi": 45,
#         "min_adx": 16,
#     },
#     # ── Overextension ────────────────────────────────────────
#     "overextended": {
#         "max_ema_pct": 0.045,
#         "max_rsi": 74,
#     },
#     # ── Conviction thresholds ────────────────────────────────
#     "conviction": {
#         "high_pct": 0.90,
#         "high_score": 0.84,
#         "medium_pct": 0.60,
#         "medium_score": 0.68,
#     },
#     # ── Leadership ───────────────────────────────────────────
#     "leadership_boost_weight": 0.10,
# }

# BREADTHPARAMS = {
#     "min_symbols": 5,
#     "min_history": 55,
#     "ema_span": 5,
#     # loose: slightly easier regime thresholds
#     "regime_strong": 0.62,
#     "regime_moderate": 0.42,
#     "regime_weak": 0.22,
#     "composite_weights": {
#         "pct_above_sma50": 0.30,
#         "pct_above_sma200": 0.20,
#         "pct_above_sma20": 0.15,
#         "pct_advancing": 0.15,
#         "net_new_highs": 0.20,
#     },
# }

# ROTATIONPARAMS = {
#     "rs_sma_period": 50,
#     "rs_momentum_period": 20,
#     "smooth_span": 10,
#     "min_history": 60,
# }


# ═══════════════════════════════════════════════════════════════════════════════
# ██  TIGHT CONFIG  (active)                                                 ██
# ═══════════════════════════════════════════════════════════════════════════════
# Tighter generates fewer BUY signals, exits positions sooner, takes smaller
# positions in rough markets, and penalizes chasing, low liquidity, and
# overbought conditions more aggressively.

VOLREGIMEPARAMS = {
    "atrp_window": 14,
    "realized_vol_window": 20,
    "dispersion_window": 20,
    "gap_window": 20,
    "calm_atrp_max": 0.035,
    "volatile_atrp_max": 0.060,
    "calm_rvol_max": 0.28,
    "volatile_rvol_max": 0.42,
    "volatile_gap_rate": 0.18,
    "chaotic_gap_rate": 0.28,
    "calm_dispersion_max": 0.022,
    "volatile_dispersion_max": 0.040,
    "score_weights": {
        "atrp": 0.35,
        "realized_vol": 0.35,
        "gap_rate": 0.15,
        "dispersion": 0.15,
    },
    # FIX: regime label thresholds (lower = more sensitive to volatility)
    "chaotic_threshold": 0.70,
    "volatile_threshold": 0.30,
}

SCORINGWEIGHTS_V2 = {
    "trend": 0.36,
    "participation": 0.18,
    "risk": 0.26,
    "regime": 0.20,
}

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
    # ── soft-penalty & rank gates (tight) ──
    "rs_fail_penalty": 0.12,
    "breadth_fail_penalty": 0.06,
    "min_rank_pct": 0.88,
    "relative_setup_rank_pct": 0.82,
    "exit_rank_floor": 0.30,
    "chaotic_exit_bump": 0.12,
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
    # ── FIX 4: position sizing (tight — smaller positions, lower cap) ──
    "position_base_pct": 0.03,
    "position_range_pct": 0.07,
    "position_max_pct": 0.10,
    # ── FIX 5: pullback lower bound (tight — narrower acceptable range) ──
    "pullback_min_short_extension": -0.04,
    # ── FIX 6: continuation participation floor (tight — higher bar) ──
    "continuation_min_participation": 0.55,
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
    # ── FIX 7: rotation recommendation mapping ──
    "rotation_rec_map": {
        "leading":   "STRONGBUY",
        "improving": "BUY",
        "weakening": "SELL",            # tight: weakening gets SELL
        "lagging":   "SELL",
    },
    "rotation_rec_default": "HOLD",
}

ACTIONPARAMS_V2 = {
    # ── STRONG_BUY tier ──────────────────────────────────────
    "strong_buy": {
        "min_percentile": 0.90,
        "min_score": 0.76,
        "score_above_entry": 0.08,       # score must be >= entry + this
        "min_rvol": 1.10,
        "requires_confirmation": True,
        "requires_strong_context": True,
        "blocks_overextended": True,
    },
    # ── BUY tier ─────────────────────────────────────────────
    "buy": {
        "min_percentile": 0.65,
        "min_score": 0.62,
        "score_above_entry": 0.02,
        "requires_confirmation": True,
        "requires_decent_momentum": True,
        "blocks_weak_context": True,
    },
    # ── HOLD tier ────────────────────────────────────────────
    "hold": {
        "min_percentile": 0.35,
        "min_score": 0.54,
        "score_below_entry": 0.06,       # score must be >= entry - this
        "blocks_weak_context": True,
    },
    # ── SELL triggers ────────────────────────────────────────
    "sell": {
        "floor_score": 0.50,
        "floor_percentile": 0.15,
        "exit_score_below_entry": 0.05,  # with exit signal: sell if score < entry - this
        "exit_percentile_floor": 0.20,
    },
    # ── Context definitions ──────────────────────────────────
    "strong_context": {
        "breadth_regimes": ["strong"],
        "vol_regimes": ["calm"],
        "min_leadership": 0.60,
    },
    "weak_context": {
        "breadth_regimes": ["weak", "critical"],
        "vol_regimes": ["chaotic"],
        "sector_regimes": ["lagging"],
    },
    # ── Momentum definitions ─────────────────────────────────
    "healthy_momentum": {
        "allowed_rs": ["leading", "improving"],
        "blocked_sector": ["lagging"],
        "min_rsi": 52,
        "min_adx": 22,
    },
    "decent_momentum": {
        "allowed_rs": ["leading", "improving"],
        "min_rsi": 45,
        "min_adx": 16,
    },
    # ── Overextension ────────────────────────────────────────
    "overextended": {
        "max_ema_pct": 0.045,
        "max_rsi": 74,
    },
    # ── Conviction thresholds ────────────────────────────────
    "conviction": {
        "high_pct": 0.90,
        "high_score": 0.84,
        "medium_pct": 0.60,
        "medium_score": 0.68,
    },
    # ── Leadership ───────────────────────────────────────────
    "leadership_boost_weight": 0.10,
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  BREADTH PARAMS  (FIX 8)                                                ██
# ═══════════════════════════════════════════════════════════════════════════════
# Controls cross-sectional breadth computation: minimum universe size,
# EMA smoothing, regime classification boundaries, and composite weights.

BREADTHPARAMS = {
    "min_symbols": 5,
    "min_history": 55,
    "ema_span": 5,
    # tight: slightly higher bars to earn "strong" or "moderate"
    "regime_strong": 0.68,
    "regime_moderate": 0.48,
    "regime_weak": 0.28,
    "composite_weights": {
        "pct_above_sma50": 0.30,
        "pct_above_sma200": 0.20,
        "pct_above_sma20": 0.15,
        "pct_advancing": 0.15,
        "net_new_highs": 0.20,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  ROTATION PARAMS  (FIX 9)                                               ██
# ═══════════════════════════════════════════════════════════════════════════════
# Controls RRG-style relative-strength computation: SMA period for RS
# trend, momentum look-back, EWM smoothing span, and minimum history
# required before a symbol can be classified.

ROTATIONPARAMS = {
    "rs_sma_period": 50,
    "rs_momentum_period": 20,
    "smooth_span": 10,
    "min_history": 60,
}

##################################
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

##########################
""" refactor/common/universe_loader_v2.py """
from __future__ import annotations


def get_universe_for_market(market: str):
    from common.universe import get_universe_for_market as gufm
    return gufm(market)

######################