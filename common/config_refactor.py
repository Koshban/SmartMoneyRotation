# common/config_refactor.py
"""
Unified simplified config.
Built from the original Loose config, with structural simplifications:
- Regime weight slashed (irrelevant when breadth disabled for HK/IN)
- Boolean gate cascade in ACTIONPARAMS removed (folded into composite score)
- RS fail penalty reduced; "weakening" allowed for entry
- Unknown breadth adjustment neutral (was +0.00 loose, but still stacked)
- Rank gate relaxed from top-12% to top-30%
- SELL floor lowered for wider HOLD band
- Minimum hold period added (5 days or 5% profit)
"""
from __future__ import annotations


# ═══════════════════════════════════════════════════════════════════════════════
# ██  VOL REGIME                                                             ██
# ═══════════════════════════════════════════════════════════════════════════════
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
    "chaotic_threshold": 0.75,
    "volatile_threshold": 0.35,
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  SCORING                                                                ██
# ═══════════════════════════════════════════════════════════════════════════════
# Regime weight slashed: breadth is disabled for IN/HK so 20% of score
# was based on missing data.  Redistributed to trend + participation.

SCORINGWEIGHTS_V2 = {
    "trend":         0.30,
    "participation": 0.20,
    "risk":          0.20,
    "regime":        0.15,
    "rotation":      0.15,
}

SCORINGPARAMS_V2 = {
    "trend": {
        "w_stock_rs": 0.50,
        "w_sector_rs": 0.20,
        "w_rs_accel": 0.15,
        "w_trend_confirm": 0.15,
    },
    "participation": {
    "w_rvol": 0.25,          # was ~0.35
    "w_obv": 0.15,           # usually dead → redistributed
    "w_adline": 0.10,        # usually dead → redistributed
    "w_dollar_volume": 0.25, # was ~0.30
    "w_up_volume": 0.25,     # NEW — accumulation quality
        },
    "risk": {
        "w_vol_penalty": 0.35,
        "w_liquidity_penalty": 0.25,
        "w_gap_penalty": 0.20,
        "w_extension_penalty": 0.20,
    },
    "regime": {
        "w_breadth": 0.50,
        "w_vol_regime": 0.50,
    },
    "penalties": {
        "rsi_soft_low": 35.0,
        "rsi_soft_high": 80.0,
        "adx_soft_min": 14.0,
        "atrp_high": 0.07,
        "extension_warn": 0.12,
        "extension_bad": 0.22,
        "illiquidity_bad": 0.015,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  SIGNALS  (simplified from ~30 params to ~20)                           ██
# ═══════════════════════════════════════════════════════════════════════════════
SIGNALPARAMS_V2 = {
    # ── Mechanical risk controls (NEW) ──
    "trailing_stop_pct": 0.18,
    "max_hold_days": 120,
    "upgrade_min_score_gap": 999,

    # ── Core thresholds (these are the 3 real gates) ──
    "base_entry_threshold": 0.50,
    "base_exit_threshold": 0.35,

    # ── RS filter: soft penalty, not hard block ──
    "allowed_rs_regimes": ("leading", "improving", "weakening"),
    "blocked_sector_regimes": ("lagging",),
    "rs_fail_penalty": 0.04,
    "breadth_fail_penalty": 0.02,

    # ── Hard blocks: only extreme conditions ──
    "hard_block_breadth_regimes": ("critical",),
    "hard_block_vol_regimes": ("chaotic",),

    # ── Rank gate (relaxed) ──
    "min_rank_pct": 0.70,
    "exit_rank_floor": 0.15,

    # ── Regime adjustments (smaller bumps) ──
    "regime_entry_adjustment": {
        "calm": 0.00,
        "volatile": 0.02,
        "chaotic": 0.06,
    },
    "breadth_entry_adjustment": {
        "strong": -0.02,
        "neutral": 0.00,
        "weak": 0.04,
        "critical": 0.10,
        "unknown": 0.00,
    },

    # ── Minimum hold (NEW) ──
    "min_hold_days": 5,
    "min_profit_early_exit_pct": 0.05,

    # ── Cooldown ──
    "cooldown_days": 3,

    # ── Position sizing ──
    "position_base_pct": 0.04,
    "position_range_pct": 0.08,
    "position_max_pct": 0.12,
    "size_multipliers": {
        "calm": 1.00, "moderate": 0.95, "elevated": 0.88,
        "volatile": 0.75, "chaotic": 0.60,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  CONVERGENCE                                                            ██
# ═══════════════════════════════════════════════════════════════════════════════
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
        "volatile": 0.02,
        "chaotic": 0.00,
    },
    "rotation_rec_map": {
        "leading":   "STRONGBUY",
        "improving": "BUY",
        "weakening": "HOLD",
        "lagging":   "SELL",
    },
    "rotation_rec_default": "HOLD",
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  ACTION PARAMS  (drastically simplified)                                ██
# ═══════════════════════════════════════════════════════════════════════════════
# Old version had ~15 boolean gates (requires_confirmation,
# requires_decent_momentum, blocks_weak_context, blocks_overextended,
# strong_context, weak_context, healthy_momentum, decent_momentum,
# overextended, conviction, leadership_boost).
#
# These are ALL now folded into the composite score.  The action tier
# is determined by score + percentile only — no secondary gate cascade.

ACTIONPARAMS_V2 = {
    "strong_buy": {
        "min_percentile": 0.90,
        "min_score": 0.75,
        "score_above_entry": 0.06,
        "require_confirmed": True,
        "allowed_regimes": ["leading", "improving"],
        "max_rsi": 70.0, 
    },
    "max_strong_buy": 15,
    "buy": {
        "min_percentile": 0.50,
        "min_score": 0.52,
        "score_above_entry": 0.01,
    },
    "hold": {
        "min_percentile": 0.25,
        "min_score": 0.42,
    },
    "sell": {
        "floor_score": 0.35,
        "floor_percentile": 0.10,
    },
    "overextended": {
        "max_ema_pct": 0.15,
        "max_rsi": 80.0,
        "buy_top_n": 5,
        "buy_min_percentile": 0.85,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  BREADTH                                                                ██
# ═══════════════════════════════════════════════════════════════════════════════
BREADTHPARAMS = {
    "min_symbols": 5,
    "min_history": 55,
    "ema_span": 5,
    "regime_strong": 0.62,
    "regime_moderate": 0.42,
    "regime_weak": 0.22,
    "composite_weights": {
        "pct_above_sma50": 0.30,
        "pct_above_sma200": 0.20,
        "pct_above_sma20": 0.15,
        "pct_advancing": 0.15,
        "net_new_highs": 0.20,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  BREADTH → PORTFOLIO EXPOSURE SCALING                                   ██
# ═══════════════════════════════════════════════════════════════════════════════
# Maps breadth regime to capital deployment. In weak breadth, we slash
# exposure to 40% and block new entries — this is the primary 2022 defense.

BREADTH_PORTFOLIO = {
    "strong_exposure":     1.00,       # full deployment in strong breadth
    "neutral_exposure":    0.75,       # reduce 25% in neutral
    "weak_exposure":       0.40,       # aggressive reduction in weak breadth
    "weak_block_new":      True,       # no new positions in weak regime
    "weak_raise_entry":    0.10,       # raise entry threshold by 10pts in weak
    "neutral_raise_entry": 0.03,       # raise entry threshold by 3pts in neutral
}

# Simple breadth regime classification for the backtest engine.
# Uses % of universe above SMA50 as a proxy when full McClellan unavailable.

BREADTH_REGIME_PARAMS = {
    "breadth_proxy":       "pct_above_sma50",
    "strong_threshold":    0.65,       # >65% above SMA50 = strong
    "weak_threshold":      0.35,       # <35% above SMA50 = weak
    "smoothing_window":    5,          # smooth to avoid whipsaws
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  ROTATION                                                               ██
# ═══════════════════════════════════════════════════════════════════════════════
ROTATIONPARAMS = {
    "rs_lookback": 20,
    "rs_smooth": 5,
    "rs_weight": 0.65,
    "etf_score_weight": 0.35,
    "regime_thresholds": {
        "leading_min": 0.60,
        "moderate_min": 0.42,
        "weak_min": 0.30,
        "mom_threshold": -0.008,
        "etf_accel_override": 0.55,
    },
    "etf_scoring": {
        "trend_weight": 0.35,
        "momentum_weight": 0.30,
        "participation_weight": 0.20,
        "risk_weight": 0.15,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  STOP-LOSS & RISK MANAGEMENT                                           ██
# ═══════════════════════════════════════════════════════════════════════════════
# Three-layer defense:
#   1. Hard cap — unconditional max loss per position (never breached)
#   2. Trailing stop — ATR-based, activates after position gains +10%
#   3. Ratchet — locks in progressively more profit at milestones
#
# Together these ensure worst-case single trade loss is -20%, and
# big winners are never allowed to fully round-trip.

STOP_LOSS_PARAMS = {
    # ── Initial stop ─────────────────────────────────────────
    "atr_multiplier":        2.0,       # initial stop = entry - 2*ATR
    "atr_period":            14,

    # ── Hard cap (unconditional) ─────────────────────────────
    "max_loss_pct":          0.20,      # NEVER let a position lose more than 20%
    "max_loss_enabled":      True,

    # ── Trailing stop ────────────────────────────────────────
    "trailing_enabled":      True,
    "trail_activation_pct":  0.10,      # start trailing after +10% gain
    "trail_atr_multiplier":  2.5,       # trail at 2.5*ATR below high-water mark
    "trail_pct_fallback":    0.15,      # if ATR unavailable, trail at 15% below peak

    # ── Profit lock-in (ratchet) ─────────────────────────────
    "ratchet_enabled":       True,
    "ratchet_levels": [
        # (gain_threshold, minimum_lock_pct)
        (0.30, 0.10),                   # at +30%, lock in at least +10%
        (0.50, 0.25),                   # at +50%, lock in at least +25%
        (1.00, 0.50),                   # at +100%, lock in at least +50%
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  PORTFOLIO CONSTRUCTION                                                 ██
# ═══════════════════════════════════════════════════════════════════════════════
# Volatility-targeted position sizing: each position targets equal risk
# contribution. High-vol names get smaller weights, low-vol names get larger.

PORTFOLIO_PARAMS = {
    "total_capital":         100_000,
    "max_positions":         12,
    "min_positions":          4,
    "max_sector_pct":        0.30,
    "max_single_pct":        0.12,
    "min_single_pct":        0.03,
    "target_invested_pct":   0.90,
    "rebalance_threshold":   0.05,
    "incumbent_bonus":       0.05,

    # ── Volatility targeting ─────────────────────────────────
    "vol_target_enabled":    True,
    "portfolio_vol_target":  0.25,      # target 25% annualized portfolio vol
    "position_vol_target":   0.02,      # each position targets 2% daily risk contribution
    "vol_lookback":          20,        # realized vol calculation window
    "vol_floor":             0.10,      # minimum assumed vol (prevents over-leveraging)
    "vol_cap":               0.80,      # maximum assumed vol (prevents dust positions)
}


# ═══════════════════════════════════════════════════════════════════════════════
# ██  CORRELATION LIMITS                                                     ██
# ═══════════════════════════════════════════════════════════════════════════════
# Prevents portfolio from becoming a single-factor bet (e.g., all semis).

CORRELATION_LIMITS = {
    "enabled":              True,
    "lookback_days":        60,
    "max_avg_correlation":  0.60,       # portfolio avg pairwise corr must stay below
    "max_pair_correlation": 0.85,       # no two positions with corr > 0.85
    "cluster_max_pct":      0.40,       # max 40% in a correlated cluster (corr > 0.70)
}