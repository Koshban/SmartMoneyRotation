# refactor/common/config_refactor.py
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
    "rotation":      0.15,   # ← ADD THIS
}

SCORINGPARAMS_V2 = {
    "trend": {
        "w_stock_rs": 0.50,       # was 0.45 — stock's own momentum matters most
        "w_sector_rs": 0.20,      # was 0.25 — less weight when rotation disabled
        "w_rs_accel": 0.15,
        "w_trend_confirm": 0.15,
    },
    "participation": {
        "w_rvol": 0.35,
        "w_obv": 0.30,
        "w_adline": 0.20,
        "w_dollar_volume": 0.15,
    },
    "risk": {
        "w_vol_penalty": 0.35,
        "w_liquidity_penalty": 0.25,
        "w_gap_penalty": 0.20,
        "w_extension_penalty": 0.20,
    },
    "regime": {
        "w_breadth": 0.50,        # was 0.60 — less reliance on possibly-missing data
        "w_vol_regime": 0.50,     # was 0.40
    },
    "penalties": {
        "rsi_soft_low": 35.0,     # was 38 — allow slightly oversold names
        "rsi_soft_high": 80.0,    # was 78 — tolerate more stretch
        "adx_soft_min": 14.0,     # was 16 — don't punish early-stage trends
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
    "trailing_stop_pct": 0.18,        # sell if price drops 18% from peak
    "max_hold_days": 120,             # force review after 120 days
    "upgrade_min_score_gap": 999,    # swap if candidate beats held by 0.12+
    
    # ── Core thresholds (these are the 3 real gates) ──
    "base_entry_threshold": 0.50,     # was 0.58 — let the score do the work
    "base_exit_threshold": 0.35,      # was 0.42 — wider hold band

    # ── RS filter: soft penalty, not hard block ──
    "allowed_rs_regimes": ("leading", "improving", "weakening"),  # added weakening
    "blocked_sector_regimes": ("lagging",),
    "rs_fail_penalty": 0.04,          # was 0.08 — devastating when rotation disabled
    "breadth_fail_penalty": 0.02,     # was 0.03

    # ── Hard blocks: only extreme conditions ──
    "hard_block_breadth_regimes": ("critical",),
    "hard_block_vol_regimes": ("chaotic",),

    # ── Rank gate (relaxed) ──
    "min_rank_pct": 0.70,             # was 0.80 — top 30% can buy
    "exit_rank_floor": 0.15,          # was 0.20

    # ── Regime adjustments (smaller bumps) ──
    "regime_entry_adjustment": {
        "calm": 0.00,
        "volatile": 0.02,            # was 0.03
        "chaotic": 0.06,             # was 0.10
    },
    "breadth_entry_adjustment": {
        "strong": -0.02,             # was -0.01
        "neutral": 0.00,             # was 0.02
        "weak": 0.04,                # was 0.07
        "critical": 0.10,            # was 0.12
        "unknown": 0.00,             # was 0.00 in loose — kept neutral
    },

    # ── Minimum hold (NEW) ──
    "min_hold_days": 5,
    "min_profit_early_exit_pct": 0.05,   # can exit before 5d if ≥ +5%

    # ── Cooldown ──
    "cooldown_days": 3,               # was 4

    # ── Position sizing ──
    "position_base_pct": 0.04,
    "position_range_pct": 0.08,
    "position_max_pct": 0.12,
    "size_multipliers": {
    "calm": 1.00, "moderate": 0.95, "elevated": 0.88,
    "volatile": 0.75, "chaotic": 0.60,
    }
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
        "weakening": "HOLD",          # loose: weakening = HOLD not SELL
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
# If you ever want to loosen it back (e.g., broader market with 4 leading sectors), just lower min_score to 0.70 or raise max_strong_buy to 20.
    "strong_buy": {
    "min_percentile": 0.90,        # was 0.85
    "min_score": 0.75,             # was 0.68
    "score_above_entry": 0.06,
    "require_confirmed": True,     # NEW
    "allowed_regimes": ["leading", "improving"],  # NEW
},
"max_strong_buy": 15,              # NEW
    "buy": {
        "min_percentile": 0.50,       # was 0.65
        "min_score": 0.52,            # was 0.62
        "score_above_entry": 0.01,    # was 0.02
    },
    "hold": {
        "min_percentile": 0.25,       # was 0.35
        "min_score": 0.42,            # was 0.54
    },
    "sell": {
        "floor_score": 0.35,          # was 0.50
        "floor_percentile": 0.10,     # was 0.15
    },
    "overextended": {
    "max_ema_pct": 0.15,
    "max_rsi": 80.0,
    "buy_top_n": 5,              # only top 5 composite scores → BUY
    "buy_min_percentile": 0.85,  # or: must be in top 15%
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