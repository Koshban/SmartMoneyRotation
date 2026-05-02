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

#################################
"""
common/config.py
----------------
Central configuration for the smart-money rotation system.
All tunable parameters live here.  Every downstream module imports
from this file — nothing is hard-coded elsewhere.

Markets supported
─────────────────
  US  — Rotation engine + bottom-up scoring (dual-list with convergence)
  HK  — Bottom-up scoring only (vs local benchmark 2800.HK)
  IN  — Bottom-up scoring only (vs local benchmark NIFTYBEES.NS)
"""

from pathlib import Path
from common.credentials import PG_CONFIG, IBKR_PORT
from common.universe import ETF_UNIVERSE

# ── Safe imports for non-US universes (defined in universe.py) ──
try:
    from common.universe import HK_UNIVERSE
except ImportError:
    HK_UNIVERSE = []

try:
    from common.universe import INDIA_UNIVERSE
except ImportError:
    INDIA_UNIVERSE = []


# ═══════════════════════════════════════════════════════════════
# 0.  DEFAULT PIPELINE UNIVERSE
# ═══════════════════════════════════════════════════════════════
# The orchestrator imports this as the default ticker list.
# Override at runtime via Orchestrator(universe=[...]).
#
# The base ETF_UNIVERSE (~68 ETFs) is expanded with US single
# names from Tier 2 themes (~130 additional stocks) to give the
# scoring and rotation engines a richer universe to rank.
#
# Requires data: python ingest/ingest_cash.py --market us --period 2y
from common.universe import get_all_single_names as _get_singles
_us_singles = [s for s in _get_singles()
               if not s.endswith(".HK") and not s.endswith(".NS")]
UNIVERSE = sorted(set(list(ETF_UNIVERSE) + _us_singles))

# ═══════════════════════════════════════════════════════════════
# 1.  PROJECT PATHS
# ═══════════════════════════════════════════════════════════════
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR      = PROJECT_ROOT / "src"
DATA_DIR     = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
STAGING_FILE = DATA_DIR / "staging.json"
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# 2.  DATABASE
# ═══════════════════════════════════════════════════════════════
DB_URL = (
    f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
    f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
)

# ═══════════════════════════════════════════════════════════════
# 3.  IBKR TWS / GATEWAY
# ═══════════════════════════════════════════════════════════════
IBKR_CONFIG = {
    "host":       "127.0.0.1",
    "port":       IBKR_PORT,
    "client_id":  1,
    "timeout":    40,
    "readonly":   True,
}

# ═══════════════════════════════════════════════════════════════
# 4.  DATA FETCH PARAMETERS
# ═══════════════════════════════════════════════════════════════
FETCH_PARAMS = {
    "default_lookback":  "1 Y",
    "bar_size":          "1 day",
    "what_to_show":      "TRADES",
    "use_rth":           True,
    "pacing_delay":      1.5,
}

# ═══════════════════════════════════════════════════════════════
# 5.  BENCHMARKS
# ═══════════════════════════════════════════════════════════════
BENCHMARKS = ["SPY", "QQQ", "IWM"]
BENCHMARK_TICKER = "SPY"

# ═══════════════════════════════════════════════════════════════
# 6.  INDICATOR PARAMETERS
# ═══════════════════════════════════════════════════════════════
INDICATOR_PARAMS = {
    # ── Returns ────────────────────────────────────────────
    "return_windows":       [5, 10, 15],

    # ── Momentum oscillators ───────────────────────────────
    "rsi_period":           14,
    "macd_fast":            12,
    "macd_slow":            26,
    "macd_signal":          9,
    "adx_period":           14,

    # ── Moving averages ────────────────────────────────────
    "ema_period":           30,
    "ema_short":            9,
    "ema_mid":              21,
    "sma_short":            30,
    "sma_long":             50,
    "sma_50":               50,
    "sma_200":              200,

    # ── Bollinger Bands ────────────────────────────────────
    "bb_period":            20,
    "bb_std":               2.0,

    # ── Volatility ─────────────────────────────────────────
    "atr_period":           14,
    "realized_vol_window":  20,

    # ── Volume & accumulation ──────────────────────────────
    "obv":                  True,
    "ad_line":              True,
    "volume_avg_window":    20,
    "vol_sma_period":       20,
    "amihud_window":        20,
    "amihud_period":        20,

    # ── Stochastic ─────────────────────────────────────────
    "stoch_k":              14,
    "stoch_d":              3,
    "stoch_smooth":         3,

    # ── Rate of Change ─────────────────────────────────────
    "roc_period":           12,

    # ── Keltner Channel ────────────────────────────────────
    "kc_period":            20,
    "kc_atr_mult":          1.5,

    # ── VWAP lookback ──────────────────────────────────────
    "vwap_period":          20,

    # ── OBV smoothing ──────────────────────────────────────
    "obv_sma":              20,

    # ── MFI ────────────────────────────────────────────────
    "mfi_period":           14,

    # ── CCI ────────────────────────────────────────────────
    "cci_period":           20,

    # ── Donchian Channel ───────────────────────────────────
    "dc_period":            20,

    # ── Breadth ────────────────────────────────────────────
    "breadth_ma_windows":   [20, 50],

    # ── Normalization ──────────────────────────────────────
    "zscore_window":        60,

    # ── Correlation ────────────────────────────────────────
    "correlation_window":   60,

    # ── Relative strength params ───────────────────────────
    "rs_ema_span":                10,
    "rs_sma_span":                50,
    "rs_slope_window":            20,
    "rs_zscore_window":           60,
    "rs_momentum_short":          10,
    "rs_momentum_long":           30,
    "rs_vol_confirm_threshold":  1.3,
}

# ═══════════════════════════════════════════════════════════════
# 7.  RELATIVE STRENGTH PARAMETERS
# ═══════════════════════════════════════════════════════════════
RS_PARAMS = {
    # Legacy keys (retained for compatibility)
    "lookback_windows":     [10, 20],
    "primary_benchmark":    "SPY",

    # Keys used by compute/relative_strength.py
    "lookback":            63,
    "slope_window":        21,
    "zscore_window":       63,
    "ma_short":            10,
    "ma_long":             40,
    "strong_z":            1.0,
    "weak_z":             -1.0,
    "improving_slope":     0.0,
}

# ═══════════════════════════════════════════════════════════════
# 8.  OPTIONS PARAMETERS
# ═══════════════════════════════════════════════════════════════
OPTIONS_PARAMS = {
    "iv_percentile_window": 252,
    "iv_shift_window":      5,
    "oi_change_window":     30,
}

# ═══════════════════════════════════════════════════════════════
# 9.  SCORING — SIX PILLARS (original architecture)
# ═══════════════════════════════════════════════════════════════
PILLAR_WEIGHTS = {
    "relative_strength":    0.25,
    "trend_momentum":       0.25,
    "volume_accumulation":  0.20,
    "breadth":              0.15,
    "options_volatility":   0.10,
    "liquidity_penalty":    0.05,
}

PILLAR_METRIC_WEIGHTS = {
    "relative_strength":    None,
    "trend_momentum":       None,
    "volume_accumulation":  None,
    "breadth":              None,
    "options_volatility":   None,
    "liquidity_penalty":    None,
}

# ═══════════════════════════════════════════════════════════════
# 10. NORMALIZATION
# ═══════════════════════════════════════════════════════════════
NORMALIZATION = {
    "default_method":  "zscore",
    "zscore_cap":      3.0,
}

PILLAR_NORM_OVERRIDE = {}

# ═══════════════════════════════════════════════════════════════
# 11. THEME SCORING MODE  (HYBRID)
# ═══════════════════════════════════════════════════════════════
THEME_SCORING_SOURCE = {
    "relative_strength":    "etf",
    "trend_momentum":       "etf",
    "volume_accumulation":  "basket",
    "breadth":              "basket",
    "options_volatility":   "basket",
    "liquidity_penalty":    "basket",
}

# ═══════════════════════════════════════════════════════════════
# 12. ROTATION DETECTION THRESHOLDS
# ═══════════════════════════════════════════════════════════════
ROTATION_THRESHOLDS = {
    "candidate_zscore":         1.0,
    "emerging_cross_from":      0.0,
    "emerging_cross_to":        0.5,
    "emerging_lookback_days":   5,
    "min_dollar_volume":        5_000_000,
    "max_amihud":               0.5,
}

# ═══════════════════════════════════════════════════════════════
# 13. OUTPUT
# ═══════════════════════════════════════════════════════════════
OUTPUT = {
    "csv_dir":              DATA_DIR / "scores",
    "top_n_themes":         5,
    "top_n_names":          10,
    "save_to_postgres":     True,
}

# ═══════════════════════════════════════════════════════════════
# 14. SCORING WEIGHTS  (used by compute/scoring.py)
# ═══════════════════════════════════════════════════════════════
SCORING_WEIGHTS = {
    # Pillar weights (must sum to 1.0 when all five are active)
    "pillar_rotation":       0.30,
    "pillar_momentum":       0.25,
    "pillar_volatility":     0.15,
    "pillar_microstructure": 0.20,
    "pillar_breadth":        0.10,

    # Pillar 1 — Rotation sub-weights
    "rs_zscore_w":           0.35,
    "rs_regime_w":           0.30,
    "rs_momentum_w":         0.20,
    "rs_vol_confirm_w":      0.15,

    # Pillar 2 — Momentum sub-weights
    "rsi_w":                 0.35,
    "macd_w":                0.35,
    "adx_w":                 0.30,

    # Pillar 3 — Volatility sub-weights
    "realized_vol_w":        0.40,
    "atr_pct_w":             0.30,
    "amihud_w":              0.30,

    # Pillar 4 — Microstructure sub-weights
    "obv_slope_w":           0.35,
    "ad_slope_w":            0.30,
    "rel_volume_w":          0.35,
}

# ═══════════════════════════════════════════════════════════════
# 15. SCORING PARAMS  (used by compute/scoring.py)
# ═══════════════════════════════════════════════════════════════
SCORING_PARAMS = {
    "rank_window":          60,
    "micro_slope_window":   10,
    "rs_momentum_scale":   500,

    # Also used by the 4-pillar scoring variant
    "w_momentum":     0.30,
    "w_trend":        0.30,
    "w_volume":       0.20,
    "w_volatility":   0.20,
    "zscore_cap":     3.0,

    # Sector adjustment
    "sector_adj_leading":    0.04,
    "sector_adj_improving":  0.02,
    "sector_adj_weakening": -0.02,
    "sector_adj_lagging":   -0.04,
}

# ═══════════════════════════════════════════════════════════════
# 16. SECTOR CONFIGURATION
# ═══════════════════════════════════════════════════════════════
SECTOR_ETFS = {
    "Technology":       "XLK",
    "Healthcare":       "XLV",
    "Financials":       "XLF",
    "Consumer Disc":    "XLY",
    "Consumer Staples": "XLP",
    "Energy":           "XLE",
    "Industrials":      "XLI",
    "Materials":        "XLB",
    "Utilities":        "XLU",
    "Real Estate":      "XLRE",
    "Communication":    "XLC",
}

# Alias used by compute/sector_rs.py
SECTOR_ETF_MAP = SECTOR_ETFS

SECTOR_RS_PARAMS = {
    # Legacy keys
    "rs_lookback":      63,
    "momentum_window":  10,
    "rank_smoothing":   5,

    # Keys used by compute/sector_rs.py
    "lookback":            63,
    "slope_window":        21,
    "zscore_window":       63,
    "ma_short":            10,
    "ma_long":             40,
    "strong_z":            0.75,
    "weak_z":             -0.75,
    "improving_slope":     0.0,
    "top_n_sectors":       4,
    "tailwind_regimes":    ["leading", "improving"],
    "headwind_regimes":    ["lagging"],
}

SECTOR_SCORE_ADJUSTMENT = {
    "enabled":      True,
    "max_boost":    0.10,
    "max_penalty": -0.10,
}

# ═══════════════════════════════════════════════════════════════
# 17. TICKER → SECTOR MAPPING
# ═══════════════════════════════════════════════════════════════
TICKER_SECTOR_MAP = {
    # Technology
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology",
    "GOOGL": "Technology", "META": "Technology", "AMZN": "Technology",
    "AVGO": "Technology", "CRM": "Technology", "ADBE": "Technology",
    "AMD": "Technology", "INTC": "Technology", "ORCL": "Technology",
    "CSCO": "Technology", "QCOM": "Technology", "TXN": "Technology",
    "NOW": "Technology", "AMAT": "Technology", "MU": "Technology",

    # Financials
    "JPM": "Financials", "GS": "Financials", "BAC": "Financials",
    "MS": "Financials", "WFC": "Financials", "C": "Financials",
    "BLK": "Financials", "SCHW": "Financials", "AXP": "Financials",

    # Energy
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy",
    "SLB": "Energy", "EOG": "Energy", "MPC": "Energy",

    # Healthcare
    "JNJ": "Healthcare", "UNH": "Healthcare", "PFE": "Healthcare",
    "LLY": "Healthcare", "ABBV": "Healthcare", "MRK": "Healthcare",
    "TMO": "Healthcare", "ABT": "Healthcare", "BMY": "Healthcare",

    # Consumer Staples
    "PG": "Consumer Staples", "KO": "Consumer Staples",
    "PEP": "Consumer Staples", "WMT": "Consumer Staples",
    "COST": "Consumer Staples", "CL": "Consumer Staples",

    # Consumer Disc
    "HD": "Consumer Disc", "NKE": "Consumer Disc",
    "MCD": "Consumer Disc", "SBUX": "Consumer Disc",
    "TGT": "Consumer Disc", "LOW": "Consumer Disc",

    # Industrials
    "LIN": "Industrials", "CAT": "Industrials",
    "HON": "Industrials", "UPS": "Industrials",
    "RTX": "Industrials", "DE": "Industrials",
    "GE": "Industrials", "BA": "Industrials",

    # Materials
    "APD": "Materials", "ECL": "Materials",
    "NEM": "Materials", "FCX": "Materials",

    # Utilities
    "NEE": "Utilities", "DUK": "Utilities",
    "SO": "Utilities", "D": "Utilities",

    # Communication
    "DIS": "Communication", "NFLX": "Communication",
    "T": "Communication", "VZ": "Communication",
    "TMUS": "Communication",
}

# ═══════════════════════════════════════════════════════════════
# 18. SIGNAL PARAMETERS  (used by strategy/signals.py)
# ═══════════════════════════════════════════════════════════════

SIGNAL_PARAMS = {
    # ── Entry thresholds ─────────────────────────────────────
    "entry_score_min":        0.55,
    "entry_percentile_min":   0.70,
    "entry_momentum_confirm": True,

    # ── RSI hard gate (no BUY outside this range) ────────────
    "rsi_entry_min":          30,
    "rsi_entry_max":          70,

    
    # ── Exit thresholds ──────────────────────────────────────
    "exit_score_below":       0.35,
    "exit_percentile_below":  0.25,
    "exit_score_max":         0.35,

    # ── RS regime gate ───────────────────────────────────────
    "allowed_rs_regimes": ("leading", "improving", "neutral"),
    "allowed_sector_regimes": ["leading", "improving", "neutral"],

    # ── Legacy regime keys ───────────────────────────────────
    "stock_regime_allowed":   ["leading", "improving"],
    "sector_regime_blocked":  ["lagging"],

    # ── Momentum persistence ─────────────────────────────────
    "confirmation_streak":    2,
    "entry_confirm_days":     2,
    "exit_confirm_days":      2,

    # ── Anti-churn cooldown ──────────────────────────────────
    "cooldown_days":          15,

    # ── Position sizing ──────────────────────────────────────
    "max_position_pct":       0.15,
    "min_position_pct":       0.03,
    "base_position_pct":      0.08,
    "score_scale_factor":     1.5,
    "vol_penalty_threshold":  0.80,
    "vol_penalty_factor":     0.60,

    # ── Concentration limits ─────────────────────────────────
    "max_sector_exposure":    0.35,
    "max_positions":          10,
    "min_positions":          3,
}

# ═══════════════════════════════════════════════════════════════
# 19. PORTFOLIO CONSTRUCTION
# ═══════════════════════════════════════════════════════════════
# Add to config.py, section 19 (PORTFOLIO_PARAMS)

PORTFOLIO_PARAMS = {
    "total_capital":         100_000,
    "max_positions":         12,
    "min_positions":          4,
    "max_sector_pct":        0.35,
    "max_single_pct":        0.15,
    "min_single_pct":        0.02,
    "target_invested_pct":   0.90,
    "rebalance_threshold":   0.05,
    "incumbent_bonus":       0.05,

    # ── NEW: Volatility targeting ────────────────────────────
    "vol_target_enabled":    True,
    "portfolio_vol_target":  0.25,       # target 25% annualized portfolio vol
    "position_vol_target":   0.02,       # each position targets 2% daily risk contribution
    "vol_lookback":          20,         # realized vol calculation window
    "vol_floor":             0.10,       # minimum assumed vol (prevents over-leveraging low-vol names)
    "vol_cap":               0.80,       # maximum assumed vol (prevents dust positions)
}

# ═══════════════════════════════════════════════════════════════
# 20. MARKET BREADTH
# ═══════════════════════════════════════════════════════════════
BREADTH_PARAMS = {
    # Advance/Decline
    "min_stocks":           10,

    # McClellan
    "mcclellan_fast":       19,
    "mcclellan_slow":       39,

    # Percent above MA
    "ma_short":             50,
    "ma_long":              200,

    # New highs / lows
    "high_low_window":      252,

    # Breadth thrust
    "thrust_window":        10,
    "thrust_up_threshold":  0.615,
    "thrust_dn_threshold":  0.25,

    # Regime classification
    "regime_strong_pct":    0.65,
    "regime_weak_pct":      0.35,
}

# ═══════════════════════════════════════════════════════════════
# 21. BREADTH → PORTFOLIO INTEGRATION
# ═══════════════════════════════════════════════════════════════
BREADTH_PORTFOLIO = {
    "strong_exposure":     1.00,
    "neutral_exposure":    0.80,
    "weak_exposure":       0.50,
    "weak_block_new":      True,
    "weak_raise_entry":    0.05,
    "neutral_raise_entry": 0.02,
}

# ═══════════════════════════════════════════════════════════════
# 22. STOP-LOSS
# ═══════════════════════════════════════════════════════════════
ATR_STOP_MULTIPLIER = 2.0    # stop-loss at 2× ATR below entry


# ╔═══════════════════════════════════════════════════════════════╗
# ║                                                               ║
# ║         M U L T I - M A R K E T   C O N F I G U R A T I O N  ║
# ║                                                               ║
# ╚═══════════════════════════════════════════════════════════════╝

# ═══════════════════════════════════════════════════════════════
# 23. MULTI-MARKET CORE SETTINGS
# ═══════════════════════════════════════════════════════════════
# Which markets to run.  The orchestrator iterates this list.
# Remove a market here to skip it entirely.
ACTIVE_MARKETS = ["US", "HK", "IN"]

# Per-market benchmark for relative-strength calculation.
# Each ticker must be fetchable via the data layer (IBKR / yfinance).
MARKET_BENCHMARKS = {
    "US": "SPY",
    "HK": "2800.HK",       # Tracker Fund — tracks Hang Seng Index
    "IN": "NIFTYBEES.NS",  # Nippon India Nifty BeES — tracks Nifty 50
}

# Per-market IBKR contract hints (used by data fetcher).
# These override FETCH_PARAMS when fetching for a specific market.
MARKET_FETCH_OVERRIDES = {
    "US": {},                                             # use defaults
    "HK": {"exchange": "SEHK",  "currency": "HKD"},
    "IN": {"exchange": "NSE",   "currency": "INR"},
}


# ═══════════════════════════════════════════════════════════════
# 24. HK MARKET CONFIGURATION
# ═══════════════════════════════════════════════════════════════

HK_BENCHMARK = "2800.HK"

# ── Informal sector groupings (informational — NOT used for rotation) ──
# Verify tickers below match your common/universe.py → HK_UNIVERSE.
# Unmapped tickers default to "Other" in output reports.
HK_TICKER_GROUP_MAP = {
    # ── China Tech ─────────────────────────────────────────
    "0700.HK": "Tech",       # Tencent
    "9988.HK": "Tech",       # Alibaba
    "3690.HK": "Tech",       # Meituan
    "9618.HK": "Tech",       # JD.com
    "9888.HK": "Tech",       # Baidu
    "1810.HK": "Tech",       # Xiaomi
    "9999.HK": "Tech",       # NetEase
    "9626.HK": "Tech",       # Bilibili
    "1024.HK": "Tech",       # Kuaishou
    "0992.HK": "Tech",       # Lenovo

    # ── Financials / Insurance ─────────────────────────────
    "1299.HK": "Financials",  # AIA Group
    "0005.HK": "Financials",  # HSBC Holdings
    "0388.HK": "Financials",  # HK Exchanges & Clearing
    "2318.HK": "Financials",  # Ping An Insurance
    "0939.HK": "Financials",  # China Construction Bank
    "1398.HK": "Financials",  # ICBC
    "3988.HK": "Financials",  # Bank of China

    # ── Property / REITs ───────────────────────────────────
    "0001.HK": "Property",    # CK Hutchison
    "1113.HK": "Property",    # CK Asset Holdings
    "0016.HK": "Property",    # Sun Hung Kai Properties
    "0823.HK": "Property",    # Link REIT
    "1109.HK": "Property",    # China Resources Land

    # ── Energy / Utilities ─────────────────────────────────
    "0883.HK": "Energy",      # CNOOC
    "0857.HK": "Energy",      # PetroChina
    "0002.HK": "Energy",      # CLP Holdings
    "0003.HK": "Energy",      # HK & China Gas

    # ── Auto / EV ──────────────────────────────────────────
    "1211.HK": "Auto",        # BYD Company
    "2015.HK": "Auto",        # Li Auto
    "9868.HK": "Auto",        # XPeng
    "0175.HK": "Auto",        # Geely Automobile
    "9866.HK": "Auto",        # NIO Inc
    

    # ── Consumer / Telecom ─────────────────────────────────
    "0941.HK": "Telecom",     # China Mobile
    "0762.HK": "Telecom",     # China Unicom
    "9633.HK": "Consumer",    # Nongfu Spring
    "2020.HK": "Consumer",    # Anta Sports
    "9961.HK": "Consumer",    # Trip.com

    # ── Healthcare / Biotech ───────────────────────────────
    "1177.HK": "Healthcare",  # Sino Biopharmaceutical
    "2269.HK": "Healthcare",  # WuXi Biologics
    "3692.HK": "Healthcare",  # Hansoh Pharmaceutical
}

# ── HK scoring weights ──
# Inherits US defaults, then overrides.
# No breadth data; redistribute its weight to momentum & microstructure.
HK_SCORING_WEIGHTS = {
    **SCORING_WEIGHTS,

    # Adjusted pillar allocation (sums to 1.0)
    "pillar_rotation":       0.25,   # RS vs 2800.HK — still valuable
    "pillar_momentum":       0.30,   # ↑ from 0.25
    "pillar_volatility":     0.15,   # unchanged
    "pillar_microstructure": 0.30,   # ↑ from 0.20 — volume signals matter in HK
    "pillar_breadth":        0.00,   # no breadth for HK
}

# ── HK scoring params ──
# Inherits US defaults; zeroes out sector adjustment (no sector rotation).
HK_SCORING_PARAMS = {
    **SCORING_PARAMS,

    "sector_adj_leading":    0.0,
    "sector_adj_improving":  0.0,
    "sector_adj_weakening":  0.0,
    "sector_adj_lagging":    0.0,
}

# ── HK signal params ──
# More permissive regime gates (no sector rotation to gate on).
# Fewer positions (typically smaller HK universe).
HK_SIGNAL_PARAMS = {
    **SIGNAL_PARAMS,

    # Regime gates — stock RS gate kept, sector gate disabled
    "allowed_rs_regimes":     ["leading", "improving", "neutral"],
    "allowed_sector_regimes": ["leading", "improving", "neutral",
                               "weakening", "lagging"],
    "stock_regime_allowed":   ["leading", "improving", "neutral"],
    "sector_regime_blocked":  [],

    # Tighter portfolio for smaller universe
    "max_positions":          8,
    "min_positions":          2,
    "max_sector_exposure":    0.40,   # HK is sector-concentrated
    "max_position_pct":       0.20,   # allow larger single bets
    "base_position_pct":      0.10,
}

# ── HK portfolio params ──
HK_PORTFOLIO_PARAMS = {
    **PORTFOLIO_PARAMS,

    "max_positions":         8,
    "min_positions":         2,
    "max_sector_pct":        0.40,
    "max_single_pct":        0.20,
    "min_single_pct":        0.04,
    "target_invested_pct":   0.85,
    "rebalance_threshold":   0.06,
}

# ── HK relative-strength params ──
HK_RS_PARAMS = {
    **RS_PARAMS,
    "primary_benchmark":    "2800.HK",
}


# ═══════════════════════════════════════════════════════════════
# 25. INDIA MARKET CONFIGURATION
# ═══════════════════════════════════════════════════════════════

IN_BENCHMARK = "NIFTYBEES.NS"

# ── Informal sector groupings (informational — NOT used for rotation) ──
# Verify tickers below match your common/universe.py → INDIA_UNIVERSE.
INDIA_TICKER_GROUP_MAP = {
    # ── IT Services ────────────────────────────────────────
    "TCS.NS":        "IT",          # Tata Consultancy Services
    "INFY.NS":       "IT",          # Infosys
    "WIPRO.NS":      "IT",          # Wipro
    "HCLTECH.NS":    "IT",          # HCL Technologies
    "TECHM.NS":      "IT",          # Tech Mahindra
    "LTIM.NS":       "IT",          # LTIMindtree

    # ── Financials ─────────────────────────────────────────
    "HDFCBANK.NS":   "Financials",  # HDFC Bank
    "ICICIBANK.NS":  "Financials",  # ICICI Bank
    "SBIN.NS":       "Financials",  # State Bank of India
    "KOTAKBANK.NS":  "Financials",  # Kotak Mahindra Bank
    "AXISBANK.NS":   "Financials",  # Axis Bank
    "BAJFINANCE.NS": "Financials",  # Bajaj Finance
    "BAJFINSV.NS":   "Financials",  # Bajaj Finserv
    "INDUSINDBK.NS": "Financials",  # IndusInd Bank

    # ── Energy / Conglomerate ──────────────────────────────
    "RELIANCE.NS":   "Energy",      # Reliance Industries
    "ONGC.NS":       "Energy",      # Oil & Natural Gas Corp
    "NTPC.NS":       "Energy",      # NTPC Ltd
    "POWERGRID.NS":  "Energy",      # Power Grid Corp
    "ADANIGREEN.NS": "Energy",      # Adani Green Energy
    "COALINDIA.NS":  "Energy",      # Coal India

    # ── Consumer ───────────────────────────────────────────
    "HINDUNILVR.NS": "Consumer",    # Hindustan Unilever
    "ITC.NS":        "Consumer",    # ITC Ltd
    "ASIANPAINT.NS": "Consumer",    # Asian Paints
    "TITAN.NS":      "Consumer",    # Titan Company
    "NESTLEIND.NS":  "Consumer",    # Nestle India
    "BRITANNIA.NS":  "Consumer",    # Britannia Industries
    "MARUTI.NS":     "Consumer",    # Maruti Suzuki

    # ── Industrials ────────────────────────────────────────
    "LT.NS":         "Industrials", # Larsen & Toubro
    "ADANIENT.NS":   "Industrials", # Adani Enterprises
    "ADANIPORTS.NS": "Industrials", # Adani Ports
    "ULTRACEMCO.NS": "Industrials", # UltraTech Cement
    "GRASIM.NS":     "Industrials", # Grasim Industries
    "TATASTEEL.NS":  "Industrials", # Tata Steel
    "JSWSTEEL.NS":   "Industrials", # JSW Steel
    "HINDALCO.NS":   "Industrials", # Hindalco

    # ── Pharma / Healthcare ────────────────────────────────
    "SUNPHARMA.NS":  "Pharma",     # Sun Pharmaceutical
    "DRREDDY.NS":    "Pharma",     # Dr. Reddy's Laboratories
    "CIPLA.NS":      "Pharma",     # Cipla
    "APOLLOHOSP.NS": "Pharma",     # Apollo Hospitals
    "DIVISLAB.NS":   "Pharma",     # Divi's Laboratories

    # ── Telecom ────────────────────────────────────────────
    "BHARTIARTL.NS": "Telecom",    # Bharti Airtel

    # ── Auto ───────────────────────────────────────────────
    "TATAMOTORS.NS": "Auto",       # Tata Motors
    "M&M.NS":        "Auto",       # Mahindra & Mahindra
    "EICHERMOT.NS":  "Auto",       # Eicher Motors
    "BAJAJ-AUTO.NS": "Auto",       # Bajaj Auto
    "HEROMOTOCO.NS": "Auto",       # Hero MotoCorp
}

# ── India scoring weights ──
# Same logic as HK: no breadth, redistribute to momentum & microstructure.
IN_SCORING_WEIGHTS = {
    **SCORING_WEIGHTS,

    "pillar_rotation":       0.25,
    "pillar_momentum":       0.30,
    "pillar_volatility":     0.15,
    "pillar_microstructure": 0.30,
    "pillar_breadth":        0.00,
}

# ── India scoring params ──
IN_SCORING_PARAMS = {
    **SCORING_PARAMS,

    "sector_adj_leading":    0.0,
    "sector_adj_improving":  0.0,
    "sector_adj_weakening":  0.0,
    "sector_adj_lagging":    0.0,
}

# ── India signal params ──
IN_SIGNAL_PARAMS = {
    **SIGNAL_PARAMS,

    "allowed_rs_regimes":     ["leading", "improving", "neutral"],
    "allowed_sector_regimes": ["leading", "improving", "neutral",
                               "weakening", "lagging"],
    "stock_regime_allowed":   ["leading", "improving", "neutral"],
    "sector_regime_blocked":  [],

    "max_positions":          8,
    "min_positions":          2,
    "max_sector_exposure":    0.40,
    "max_position_pct":       0.20,
    "base_position_pct":      0.10,
}

# ── India portfolio params ──
IN_PORTFOLIO_PARAMS = {
    **PORTFOLIO_PARAMS,

    "max_positions":         8,
    "min_positions":         2,
    "max_sector_pct":        0.40,
    "max_single_pct":        0.20,
    "min_single_pct":        0.04,
    "target_invested_pct":   0.85,
    "rebalance_threshold":   0.06,
}

# ── India relative-strength params ──
IN_RS_PARAMS = {
    **RS_PARAMS,
    "primary_benchmark":    "NIFTYBEES.NS",
}


# ═══════════════════════════════════════════════════════════════
# 26. US CONVERGENCE  (dual-list merge settings)
# ═══════════════════════════════════════════════════════════════
# When both the rotation engine and the scoring engine produce
# signals for US, this section controls how they are merged.

US_CONVERGENCE = {
    # ── Label taxonomy ───────────────────────────────────────
    # Applied per ticker based on which lists it appears on.
    "labels": {
        "strong_buy":     "BUY on BOTH rotation + scoring",
        "buy_rotation":   "BUY on rotation only",
        "buy_scoring":    "BUY on scoring only",
        "conflict":       "BUY on one, SELL on the other — review",
        "strong_sell":    "SELL on BOTH rotation + scoring",
        "sell_rotation":  "SELL on rotation only",
        "sell_scoring":   "SELL on scoring only",
        "neutral":        "No signal from either engine",
    },

    # ── Conviction weighting ─────────────────────────────────
    # When building the final ranked list, how much does
    # convergence matter vs raw score?
    "convergence_boost":     0.10,    # added to score if both agree BUY
    "conflict_penalty":     -0.05,    # subtracted if engines disagree

    # ── Override rules ───────────────────────────────────────
    # If True, a strong_sell overrides any individual BUY —
    # i.e. if rotation says SELL and scoring says BUY, final = HOLD.
    "strong_sell_overrides": True,
    "strong_buy_overrides":  True,

    # ── Output control ───────────────────────────────────────
    "show_individual_lists": True,    # include per-engine lists in report
    "show_merged_list":      True,    # include convergence-merged list
}


# ═══════════════════════════════════════════════════════════════
# 27. MASTER MARKET CONFIG
# ═══════════════════════════════════════════════════════════════
# Single entry point for all per-market settings.
# Downstream code: `cfg = MARKET_CONFIG["HK"]` then access any key.
#
# Fields:
#   universe          – list of tickers to score
#   benchmark         – ticker for RS denominator
#   engines           – which engines to run ("rotation", "scoring")
#   scoring_weights   – pillar weights dict
#   scoring_params    – scoring params dict
#   signal_params     – signal thresholds dict
#   portfolio_params  – portfolio construction dict
#   rs_params         – relative-strength params dict
#   sector_rs_enabled – whether to run sector rotation engine
#   sector_etfs       – sector ETF map  (US only)
#   ticker_sector_map – ticker→sector   (US only)
#   ticker_group_map  – ticker→group    (HK/IN — informational)
#   convergence       – dual-list merge config (US only)
#   fetch_overrides   – IBKR per-market overrides

MARKET_CONFIG = {
    "US": {
        "universe":          UNIVERSE,
        "benchmark":         "SPY",
        "engines":           ["rotation", "scoring"],
        "scoring_weights":   SCORING_WEIGHTS,
        "scoring_params":    SCORING_PARAMS,
        "signal_params":     SIGNAL_PARAMS,
        "portfolio_params":  PORTFOLIO_PARAMS,
        "rs_params":         RS_PARAMS,
        "sector_rs_enabled": True,
        "sector_etfs":       SECTOR_ETFS,
        "sector_rs_params":  SECTOR_RS_PARAMS,
        "ticker_sector_map": TICKER_SECTOR_MAP,
        "ticker_group_map":  TICKER_SECTOR_MAP,   # same as sector for US
        "convergence":       US_CONVERGENCE,
        "fetch_overrides":   {},
    },
    "HK": {
        "universe":          list(HK_UNIVERSE),
        "benchmark":         HK_BENCHMARK,
        "engines":           ["scoring"],
        "scoring_weights":   HK_SCORING_WEIGHTS,
        "scoring_params":    HK_SCORING_PARAMS,
        "signal_params":     HK_SIGNAL_PARAMS,
        "portfolio_params":  HK_PORTFOLIO_PARAMS,
        "rs_params":         HK_RS_PARAMS,
        "sector_rs_enabled": False,
        "sector_etfs":       {},
        "sector_rs_params":  {},
        "ticker_sector_map": {},
        "ticker_group_map":  HK_TICKER_GROUP_MAP,
        "convergence":       None,
        "fetch_overrides":   {"exchange": "SEHK", "currency": "HKD"},
    },
    "IN": {
        "universe":          list(INDIA_UNIVERSE),
        "benchmark":         IN_BENCHMARK,
        "engines":           ["scoring"],
        "scoring_weights":   IN_SCORING_WEIGHTS,
        "scoring_params":    IN_SCORING_PARAMS,
        "signal_params":     IN_SIGNAL_PARAMS,
        "portfolio_params":  IN_PORTFOLIO_PARAMS,
        "rs_params":         IN_RS_PARAMS,
        "sector_rs_enabled": False,
        "sector_etfs":       {},
        "sector_rs_params":  {},
        "ticker_sector_map": {},
        "ticker_group_map":  INDIA_TICKER_GROUP_MAP,
        "convergence":       None,
        "fetch_overrides":   {"exchange": "NSE", "currency": "INR"},
    },
}

# ═══════════════════════════════════════════════════════════════
# 28. HELPER — look up group for any ticker across all markets
# ═══════════════════════════════════════════════════════════════

def get_ticker_group(ticker: str, market: str = None) -> str:
    """
    Return the sector/group label for *ticker*.

    If *market* is given, look up that market's group map directly.
    Otherwise, search all markets in order: US → HK → IN.
    Returns "Other" if the ticker is not mapped anywhere.
    """
    if market and market in MARKET_CONFIG:
        return MARKET_CONFIG[market]["ticker_group_map"].get(ticker, "Other")

    for mkt in MARKET_CONFIG.values():
        group = mkt["ticker_group_map"].get(ticker)
        if group:
            return group
    return "Other"
    
##################################################
"""
common/expiry.py
Monthly option expiry date utilities.

Markets supported:
  US    — 3rd Friday of expiry month
  HK    — Penultimate business day of expiry month (approx)
  India — Last Thursday of expiry month
"""

from datetime import date, timedelta
from typing import List, Tuple


def third_friday(year: int, month: int) -> date:
    """3rd Friday of month (US monthly expiry)."""
    first = date(year, month, 1)
    first_fri = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_fri + timedelta(days=14)


def last_thursday(year: int, month: int) -> date:
    """Last Thursday of month (India NSE monthly expiry)."""
    nxt = date(year + (month // 12), (month % 12) + 1, 1)
    last_day = nxt - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 3) % 7)


def hk_option_expiry(year: int, month: int) -> date:
    """
    Approximate HKEX stock option expiry: the business day
    immediately preceding the last business day of the expiry month.

    HKEX rule: Last trading day is the business day immediately
    before the last business day of the expiry month.  This is
    effectively the second-to-last business day.

    This is an approximation (ignores HKEX holidays).  For
    exact matching, use IBKR's available expiry list directly
    via ``select_expiries_from_chain()``.
    """
    # Last calendar day of the month
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)

    # Walk back to the last business day (skip weekends)
    while last_day.weekday() >= 5:  # Sat=5, Sun=6
        last_day -= timedelta(days=1)

    # Penultimate business day (one more step back, skip weekends)
    prev_bday = last_day - timedelta(days=1)
    while prev_bday.weekday() >= 5:
        prev_bday -= timedelta(days=1)

    return prev_bday


def next_monthly_expiries(
    ref_date: date | None = None,
    market: str = "us",
    n: int = 2,
) -> List[date]:
    """
    Return next *n* monthly expiry dates after ref_date.

    Supported markets: "us" (3rd Friday), "hk" (penultimate
    business day), "india" (last Thursday).
    """
    if ref_date is None:
        ref_date = date.today()

    market_lower = market.lower()

    if market_lower in ("hk", "hongkong", "sehk"):
        calc = hk_option_expiry
    elif market_lower in ("india", "in", "nse"):
        calc = last_thursday
    else:
        calc = third_friday

    expiries: List[date] = []
    y, m = ref_date.year, ref_date.month

    for _ in range(n + 6):  # generous lookahead
        exp = calc(y, m)
        if exp > ref_date:
            expiries.append(exp)
            if len(expiries) == n:
                break
        m += 1
        if m > 12:
            m, y = 1, y + 1

    return expiries


def match_expiry(
    targets: List[date],
    available: tuple | list,
    max_gap_days: int = 7,
) -> List[Tuple[date, str]]:
    """
    Match calculated target expiry dates to the closest available
    expiry strings.  Rejects matches > max_gap_days away.

    Parameters
    ----------
    targets : list[date]
        Pre-calculated target expiry dates.
    available : list/tuple of str
        ISO-format date strings from the exchange/broker.
    max_gap_days : int
        Maximum calendar-day gap for a valid match (default 7).

    Returns
    -------
    list[(date, str)]
        Matched (expiry_date, ISO string) pairs.
    """
    if not available:
        return []

    avail_dates = sorted(date.fromisoformat(s) for s in available)
    matched: List[Tuple[date, str]] = []

    for target in targets:
        if not avail_dates:
            break
        best = min(avail_dates, key=lambda d: abs((d - target).days))
        if abs((best - target).days) <= max_gap_days:
            matched.append((best, best.isoformat()))

    return matched


def select_expiries_from_chain(
    available_expiries: list[str],
    n: int = 2,
    ref_date: date | None = None,
) -> List[Tuple[date, str]]:
    """
    Pick the next N distinct-month expiries directly from an
    exchange/broker's available expiry list.

    This is the **robust** approach for any market: instead of
    pre-calculating target dates (which requires knowing the
    exact expiry convention and holiday calendar), it picks
    directly from what's actually tradeable.

    Works for US (3rd Friday), HK (penultimate business day),
    and any other market regardless of convention.

    Parameters
    ----------
    available_expiries : list[str]
        Expiry strings in YYYYMMDD or YYYY-MM-DD format.
    n : int
        Number of monthly expiries to return.
    ref_date : date
        Reference date (default: today).

    Returns
    -------
    list[(date, str)]
        List of (expiry_date, ISO string) pairs, one per month.

    Example
    -------
    >>> exps = ["20260529", "20260626", "20260731", "20260828"]
    >>> select_expiries_from_chain(exps, n=2)
    [(date(2026, 5, 29), '2026-05-29'), (date(2026, 6, 26), '2026-06-26')]
    """
    if ref_date is None:
        ref_date = date.today()

    # Parse all expiry strings to dates
    parsed: list[tuple[date, str]] = []
    for exp_str in available_expiries:
        # Handle both YYYYMMDD and YYYY-MM-DD
        clean = exp_str.strip()
        if len(clean) == 8 and clean.isdigit():
            iso = f"{clean[:4]}-{clean[4:6]}-{clean[6:]}"
        elif len(clean) == 10 and "-" in clean:
            iso = clean
        else:
            continue

        try:
            exp_date = date.fromisoformat(iso)
            if exp_date > ref_date:
                parsed.append((exp_date, iso))
        except ValueError:
            continue

    # Sort by date ascending
    parsed.sort(key=lambda x: x[0])

    # Pick the earliest expiry in each distinct month
    selected: list[tuple[date, str]] = []
    seen_months: set[tuple[int, int]] = set()

    for exp_date, iso_str in parsed:
        month_key = (exp_date.year, exp_date.month)
        if month_key not in seen_months:
            seen_months.add(month_key)
            selected.append((exp_date, iso_str))
            if len(selected) >= n:
                break

    return selected
    
###################################
""" common/market_config_v2.py"""
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
    
#######################################
from __future__ import annotations

"""
common/sector_map.py
--------------------
Maps every ticker in the universe to one of the 11 GICS sectors.

Used by the rotation engine to:
  1. Determine which tickers belong to each sector
  2. Rank tickers within leading sectors (top 3)
  3. Flag holdings in lagging/stagnant sectors for selling

Tickers that don't map to a GICS sector (international ETFs, fixed
income, commodities, broad-market) are excluded from sector rotation
but tracked under their asset class.

Design decisions & edge cases:
  - Crypto miners (MARA, RIOT, CLSK, HIVE, MSTR) -> Technology
    per GICS-style treatment in this system. COIN -> Financials.
  - Nuclear reactor builders (SMR, NNE) -> Industrials.
    Nuclear / power generators (CEG, VST, TLN, OKLO) -> Utilities.
    Uranium miners / fuel (CCJ, UEC, LEU) -> Energy.
  - Solar hardware (FSLR, ENPH, SEDG) -> Technology.
  - UBER -> Industrials.
  - Each ticker gets exactly one GICS sector here, even if it appears
    in multiple themes in universe.py.
  - Theme groupings such as AI Infrastructure / Neo-Cloud are handled
    separately in THEME_MAP so sector rotation stays clean.
"""

# ═══════════════════════════════════════════════════════════════
#  SECTOR ETFs  (the 11 sector benchmarks)
# ═══════════════════════════════════════════════════════════════

SECTOR_ETFS: dict[str, str] = {
    "Technology":             "XLK",
    "Financials":             "XLF",
    "Energy":                 "XLE",
    "Healthcare":             "XLV",
    "Industrials":            "XLI",
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples":       "XLP",
    "Utilities":              "XLU",
    "Real Estate":            "XLRE",
    "Materials":              "XLB",
}

# ═══════════════════════════════════════════════════════════════
#  GICS SECTOR MAPPING — organized by sector
# ═══════════════════════════════════════════════════════════════

_SECTOR_TICKERS: dict[str, list[str]] = {
    # ── Technology ─────────────────────────────────────────────
    "Technology": [
        # Mega / large cap
        "AAPL", "MSFT", "NVDA", "AVGO", "AMD", "QCOM", "INTC", "TSM", "ASML",
        "ORCL", "ADBE", "CRM", "ACN", "ADI",
        # Semis / equipment
        "AMAT", "LRCX", "KLAC", "MU", "MRVL", "ARM", "SMCI", "MBLY","SIMO", "SNDK",
        # Software / cloud / SaaS / infra
        "NOW", "SNOW", "NET", "PLTR", "PATH", "CRWD", "PANW", "TWLO",
        "CLS", "ANET", "TSSI", "TTD", "TOST", "PGY", "GLBE", "DDOG",
        "SNPS", "CDNS",
        # AI / robotics / infra-adjacent
        "AI", "NBIS", "SOUN", "PDYN", "CRWV", "APP",
        # Quantum computing
        "IONQ", "QBTS", "RGTI", "QUBT", "ARQQ",
        # Solar / clean-tech hardware
        "FSLR", "ENPH", "SEDG", "RUN", "OUST",
        # Crypto mining / infra
        "MSTR", "MARA", "RIOT", "CLSK", "HIVE",
        # Misc tech
        "GCT", "GENI",
        # India — electronics / IT / software
        "DIXON.NS", "KAYNES.NS", "SYRMA.NS", "CYIENTDLM.NS",
        "DATAPATTNS.NS", "CONTROLPR.NS", "FSL.NS", "INTELLECT.NS",
        "PAYTM.NS", "STLTECH.NS", "LTIM.NS",
        # HK — tech ETFs
        "3033.HK", "3067.HK",
    ],

    # ── Communication Services ─────────────────────────────────
    "Communication Services": [
        "GOOGL", "META", "BIDU",
        "SNAP", "ROKU", "NFLX", "DIS", "T", "VZ", "EA",
        "SE",
        "NAZARA.NS",
        "9888.HK",
    ],

    # ── Consumer Discretionary ─────────────────────────────────
    "Consumer Discretionary": [
        "AMZN", "TSLA", "DECK", "MELI", "JMIA",
        "HD", "LOW", "BKNG", "TJX", "ORLY", "CMG",
        "BABA", "JD", "PDD", "NIO", "XPEV", "LI", "TCOM",
        "RIVN", "LCID",
        "AMBER.NS", "EICHERMOT.NS", "FIEMIND.NS", "GABRIEL.NS",
        "METROBRAND.NS", "SAMHI.NS", "SJS.NS", "SKYGOLD.NS", "SONACOMS.NS",
        "1211.HK", "3690.HK", "9618.HK", "9866.HK", "9961.HK", "9988.HK",
    ],

    # ── Financials ─────────────────────────────────────────────
    "Financials": [
        "BRK.B", "COIN", "SOFI", "UPST", "HOOD", "CRCL",
        "JPM", "BAC", "WFC", "GS", "MS", "BLK", "SCHW", "C", "KKR",
        "AXISBANK.NS", "HDFCBANK.NS", "ICICIBANK.NS", "KOTAKBANK.NS",
    ],

    # ── Healthcare ─────────────────────────────────────────────
    "Healthcare": [
        "LLY", "AMGN", "GILD", "REGN", "VRTX", "MRNA",
        "UNH", "JNJ", "ABBV", "PFE", "BMY", "TMO", "ISRG", "MDT", "SYK", "BSX",
        "CRSP", "NTLA", "BEAM", "EDIT", "VKTX",
        "TEM", "PRME",
        "SHAILY.NS", "SYNGENE.NS",
    ],

    # ── Industrials ────────────────────────────────────────────
    "Industrials": [
        "LMT", "RTX", "NOC", "GD", "BA", "LHX", "HII", "KTOS",
        "CAT", "DE", "ETN", "PH", "EMR", "HON", "GE", "TT", "PCAR", "CMI",
        "GEV", "VRT", "NVT",
        "SMR", "NNE",
        "AXON", "UBER",
        "PLUG", "BE", "QS",
        "ARE&M.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS",
        "COCHINSHIP.NS", "CRAFTSMAN.NS", "GESHIP.NS", "GRSE.NS",
        "HGINFRA.NS", "IDEAFORGE.NS", "KEI.NS", "LT.NS",
        "MTARTECH.NS", "NCC.NS", "POLYCAB.NS", "TRITURBINE.NS", "WABAG.NS",
    ],

    # ── Energy ─────────────────────────────────────────────────
    "Energy": [
        "CCJ", "UEC", "LEU",
        "LNG",
        "XOM", "CVX", "COP", "SLB", "EOG",
        "WMB", "OKE", "KMI", "TRGP",
        "VLO", "MPC", "PSX", "OXY",
        "FANG", "DVN", "HAL", "BKR", "EQT",
        "RELIANCE.NS",
    ],

    # ── Utilities ──────────────────────────────────────────────
    "Utilities": [
        "CEG", "VST", "TLN", "OKLO",
        "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "PEG", "XEL", "ED", "D",
        "BORORENEW.NS", "SWSOLAR.NS", "TATAPOWER.NS", "WEBELSOLAR.NS",
        "2845.HK",
    ],

    # ── Materials ──────────────────────────────────────────────
    "Materials": [
        "MP", "UAMY",
        "LIN", "APD", "SHW", "NUE", "FCX", "DOW", "NEM", "ECL", "CF", "MOS",
        "DECCANCE.NS", "GALAXYSURF.NS", "NAVINFLUOR.NS",
        "PCBL.NS", "PIIND.NS", "SIRCA.NS",
    ],

    # ── Real Estate ────────────────────────────────────────────
    "Real Estate": [
        "EQIX", "DLR", "AMT",
        "PLD", "SPG", "O", "WELL", "AVB", "VICI", "PSA", "CBRE",
        "DBREALTY.NS", "PRESTIGE.NS",
    ],

    # ── Consumer Staples ───────────────────────────────────────
    "Consumer Staples": [
        "PG", "KO", "PEP", "COST", "WMT",
        "PM", "MO", "MDLZ", "CL", "KMB",
    ],
}

THEME_MAP: dict[str, str] = {
    "CRWV": "AI Infrastructure / Neo-Cloud",
    "NBIS": "AI Infrastructure / Neo-Cloud",
    "VRT": "AI Infrastructure / Neo-Cloud",
    "ANET": "AI Infrastructure / Neo-Cloud",
    "DLR": "AI Infrastructure / Neo-Cloud",
    "EQIX": "AI Infrastructure / Neo-Cloud",
    "AMT": "AI Infrastructure / Neo-Cloud",
    "CLS": "AI Infrastructure / Neo-Cloud",
    "NVT": "AI Infrastructure / Neo-Cloud",
    "SMCI": "AI Infrastructure / Neo-Cloud",
    "MSFT": "AI Platform / Software",
    "GOOGL": "AI Platform / Software",
    "META": "AI Platform / Software",
    "NOW": "AI Platform / Software",
    "SNOW": "AI Platform / Software",
    "DDOG": "AI Platform / Software",
    "PATH": "AI Platform / Software",
    "TWLO": "AI Platform / Software",
    "PLTR": "AI Platform / Software",
    "APP": "AI Platform / Software",
    "CRWD": "High Momentum Beta",
    "PANW": "High Momentum Beta",
    "CEG": "High Momentum Beta",
    "VST": "High Momentum Beta",
    "AXON": "High Momentum Beta",
    "DECK": "High Momentum Beta",
    "UBER": "High Momentum Beta",
    "ROKU": "High Momentum Beta",
    "TTD": "High Momentum Beta",
    "HOOD": "High Momentum Beta",
    "SOUN": "High Momentum Beta",
    "UPST": "High Momentum Beta",
    "SOFI": "High Momentum Beta",
    "TOST": "High Momentum Beta",
    "GLBE": "High Momentum Beta",
    "GENI": "High Momentum Beta",
    "IONQ": "Quantum",
    "QBTS": "Quantum",
    "RGTI": "Quantum",
    "QUBT": "Quantum",
    "ARQQ": "Quantum",
    "MSTR": "Bitcoin / Digital Assets",
    "COIN": "Bitcoin / Digital Assets",
    "MARA": "Bitcoin / Digital Assets",
    "RIOT": "Bitcoin / Digital Assets",
    "CLSK": "Bitcoin / Digital Assets",
    "HIVE": "Bitcoin / Digital Assets",
    "CRCL": "Bitcoin / Digital Assets",
    "BABA": "HK / China Tech",
    "JD": "HK / China Tech",
    "PDD": "HK / China Tech",
    "BIDU": "HK / China Tech",
    "NIO": "HK / China Tech",
    "XPEV": "HK / China Tech",
    "LI": "HK / China Tech",
    "TCOM": "HK / China Tech",
}

THEMATIC_ETF_SECTOR: dict[str, str] = {
    "SOXX": "Technology",
    "SMH": "Technology",
    "XBI": "Healthcare",
    "IBB": "Healthcare",
    "IGV": "Technology",
    "SKYY": "Technology",
    "HACK": "Technology",
    "CIBR": "Technology",
    "AIQ": "Technology",
    "QTUM": "Technology",
    "FINX": "Financials",
    "TAN": "Utilities",
    "ICLN": "Utilities",
    "LIT": "Materials",
    "DRIV": "Consumer Discretionary",
    "URA": "Energy",
    "URNM": "Energy",
    "NLR": "Utilities",
    "IBIT": "Financials",
    "BLOK": "Financials",
    "ITA": "Industrials",
    "ARKK": "Technology",
    "ARKG": "Healthcare",
    "KWEB": "Communication Services",
    "DTCR": "Real Estate",
}

NON_SECTOR_ASSETS: dict[str, str] = {
    "SPY": "Broad Market",
    "QQQ": "Broad Market",
    "IWM": "Broad Market",
    "DIA": "Broad Market",
    "MDY": "Broad Market",
    "MTUM": "Factor",
    "EEM": "International",
    "EFA": "International",
    "VWO": "International",
    "FXI": "International",
    "EWJ": "International",
    "EWZ": "International",
    "INDA": "International",
    "EWG": "International",
    "EWT": "International",
    "EWY": "International",
    "2800.HK": "International",
    "2828.HK": "International",
    "7226.HK": "International",
    "TLT": "Fixed Income",
    "IEF": "Fixed Income",
    "HYG": "Fixed Income",
    "LQD": "Fixed Income",
    "TIP": "Fixed Income",
    "AGG": "Fixed Income",
    "GLD": "Commodities",
    "SLV": "Commodities",
    "USO": "Commodities",
    "UNG": "Commodities",
    "DBA": "Commodities",
    "DBC": "Commodities",
}

INDIA_SECTOR_MAP = {
    "AARTIIND": "Materials",
    "ABB": "Industrials",
    "ADANIENT": "Energy",
    "ADANIGREEN": "Utilities",
    "ADANIPORTS": "Industrials",
    "ALLCARGO": "Industrials",
    "ANDHRSUGAR": "Materials",
    "APOLLOHOSP": "Healthcare",
    "ASAHISONG": "Materials",
    "ASHIANA": "Real Estate",
    "ASHOKLEY": "Industrials",
    "ASIANPAINT": "Materials",
    "BAJAJ-AUTO": "Consumer Discretionary",
    "BAJAJFINSV": "Financials",
    "BAJFINANCE": "Financials",
    "BHAGERIA": "Materials",
    "BHARTIARTL": "Communication Services",
    "BHEL": "Industrials",
    "BIOCON": "Healthcare",
    "BRITANNIA": "Consumer Staples",
    "CAPLIPOINT": "Healthcare",
    "CGPOWER": "Industrials",
    "CIPLA": "Healthcare",
    "COALINDIA": "Energy",
    "COFORGE": "Technology",
    "DABUR": "Consumer Staples",
    "DIVISLAB": "Healthcare",
    "DRREDDY": "Healthcare",
    "GRASIM": "Materials",
    "HCLTECH": "Technology",
    "HEIDELBERG": "Materials",
    "HEROMOTOCO": "Consumer Discretionary",
    "HIKAL": "Healthcare",
    "HINDALCO": "Materials",
    "HINDUNILVR": "Consumer Staples",
    "ICICIPRULI": "Financials",
    "INDUSINDBK": "Financials",
    "INFY": "Technology",
    "INSECTICID": "Materials",
    "ITC": "Consumer Staples",
    "JSWSTEEL": "Materials",
    "JUBLFOOD": "Consumer Discretionary",
    "JYOTHYLAB": "Consumer Staples",
    "KALYANIFRG": "Consumer Discretionary",
    "LAOPALA": "Consumer Discretionary",
    "LTF": "Financials",
    "MANAPPURAM": "Financials",
    "MARICO": "Consumer Staples",
    "MARUTI": "Consumer Discretionary",
    "MINDACORP": "Consumer Discretionary",
    "MPHASIS": "Technology",
    "MUTHOOTFIN": "Financials",
    "NESTLEIND": "Consumer Staples",
    "NMDC": "Materials",
    "NRBBEARING": "Consumer Discretionary",
    "NTPC": "Utilities",
    "ONGC": "Energy",
    "PERSISTENT": "Technology",
    "POWERGRID": "Utilities",
    "SBIN": "Financials",
    "SIEMENS": "Industrials",
    "SUNPHARMA": "Healthcare",
    "TATASTEEL": "Materials",
    "TCS": "Technology",
    "TECHM": "Technology",
    "TITAN": "Consumer Discretionary",
    "ULTRACEMCO": "Materials",
    "WIPRO": "Technology",
}

TICKER_SECTOR_MAP: dict[str, str] = {}

for _sector, _etf in SECTOR_ETFS.items():
    TICKER_SECTOR_MAP[_etf] = _sector

for _sector, _tickers in _SECTOR_TICKERS.items():
    for _t in _tickers:
        TICKER_SECTOR_MAP[_t] = _sector

TICKER_SECTOR_MAP.update(THEMATIC_ETF_SECTOR)

def get_sector(ticker: str) -> str | None:
    return TICKER_SECTOR_MAP.get(ticker)

def get_theme(ticker: str) -> str | None:
    return THEME_MAP.get(ticker)

def get_asset_class(ticker: str) -> str | None:
    return NON_SECTOR_ASSETS.get(ticker)

def get_sector_or_class(ticker: str) -> str:
    """
    Resolve sector for any ticker.

    Lookup order:
      1. Existing maps (TICKER_SECTOR_MAP, NON_SECTOR_ASSETS, THEME_MAP)
         — these use the full ticker string as-is.
      2. INDIA_SECTOR_MAP — keyed by bare symbol (strips .NS / .BO).
      3. Fallback: "Unknown"
    """
    # ── Existing maps first (full ticker, preserves current behaviour) ────
    hit = (
        TICKER_SECTOR_MAP.get(ticker)
        or NON_SECTOR_ASSETS.get(ticker)
        or THEME_MAP.get(ticker)
    )
    if hit:
        return hit

    # ── India bare-symbol lookup ──────────────────────────────────────────
    bare = ticker.replace(".NS", "").replace(".BO", "").upper()
    india_hit = INDIA_SECTOR_MAP.get(bare)
    if india_hit:
        return india_hit

    return "Unknown"

def get_tickers_for_sector(sector: str) -> list[str]:
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector and t not in SECTOR_ETFS.values()
    ]

def get_us_tickers_for_sector(sector: str) -> list[str]:
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector
        and t not in SECTOR_ETFS.values()
        and not t.endswith(".HK")
        and not t.endswith(".NS")
    ]

def get_india_tickers_for_sector(sector: str) -> list[str]:
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector and t.endswith(".NS")
    ]

def get_hk_tickers_for_sector(sector: str) -> list[str]:
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector and t.endswith(".HK")
    ]

def get_theme_tickers(theme: str) -> list[str]:
    return sorted([t for t, th in THEME_MAP.items() if th == theme])

def validate_universe_coverage():
    from common.universe import get_full_universe
    all_known = (
        set(TICKER_SECTOR_MAP.keys())
        | set(NON_SECTOR_ASSETS.keys())
        | set(THEME_MAP.keys())
    )
    full = set(get_full_universe())
    unmapped = full - all_known
    if unmapped:
        print(f"⚠️  {len(unmapped)} unmapped tickers: {sorted(unmapped)}")
    else:
        print(f"✅  All {len(full)} tickers mapped.")
    return unmapped

def print_sector_map():
    print(f"\n{'='*70}")
    print(f"  SECTOR MAP  ({len(TICKER_SECTOR_MAP)} tickers → 11 GICS sectors)")
    print(f"{'='*70}")
    for sector in sorted(SECTOR_ETFS.keys()):
        etf = SECTOR_ETFS[sector]
        tickers = get_tickers_for_sector(sector)
        us = sorted(t for t in tickers if not t.endswith(".HK") and not t.endswith(".NS"))
        hk = sorted(t for t in tickers if t.endswith(".HK"))
        india = sorted(t for t in tickers if t.endswith(".NS"))
        print(f"\n  {sector} [{etf}] — {len(tickers)} tickers")
        if us:
            print(f"    US:    {', '.join(us)}")
        if hk:
            print(f"    HK:    {', '.join(hk)}")
        if india:
            print(f"    India: {', '.join(india)}")
    print(f"\n{'='*70}")
    print(f"  NON-SECTOR ASSETS  ({len(NON_SECTOR_ASSETS)} tickers)")
    print(f"{'='*70}")
    by_class: dict[str, list[str]] = {}
    for t, c in NON_SECTOR_ASSETS.items():
        by_class.setdefault(c, []).append(t)
    for cls, tickers in sorted(by_class.items()):
        print(f"    {cls:16s}: {', '.join(sorted(tickers))}")
    print(f"\n{'='*70}")
    print(f"  THEMES  ({len(set(THEME_MAP.values()))} themes)")
    print(f"{'='*70}")
    by_theme: dict[str, list[str]] = {}
    for t, th in THEME_MAP.items():
        by_theme.setdefault(th, []).append(t)
    for th, tickers in sorted(by_theme.items()):
        print(f"    {th:26s}: {', '.join(sorted(tickers))}")
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    print_sector_map()
    validate_universe_coverage()


#####################################
""" common/universe_loader_v2.py """
from __future__ import annotations


def get_universe_for_market(market: str):
    from common.universe import get_universe_for_market as gufm
    return gufm(market)


##############################
"""
common/universe.py
Full investable universe for Smart Money Rotation.

Three tiers:
  1.  ETF Universe     — used by the core US rotation engine (scoring, signals, orders)
  1b. HK Universe      — scored by the bottom-up engine vs 2800.HK benchmark
  1c. India Universe   — scored by the bottom-up engine vs NIFTYBEES.NS benchmark
  2.  Single Names     — organized by theme, for future stock-picking layer

Both tiers get ingested into daily_prices so we always have data ready.

Ticker conventions:
  Hong Kong  — "XXXX.HK"   (exchange SEHK, currency HKD)
  India NSE  — "SYMBOL.NS"  (exchange NSE,  currency INR)
  India BSE  — "SYMBOL.BO"  (exchange BSE,  currency INR)
  US         — plain symbol  (exchange SMART, currency USD)

ingest.py must parse suffixes to set the correct IBKR contract params.
"""
from __future__ import annotations

"""
Full investable universe for Smart Money Rotation.

Three tiers:
  1.  ETF Universe     — used by the core US rotation engine (scoring, signals, orders)
  1b. HK Universe      — scored by the bottom-up engine vs 2800.HK benchmark
  1c. India Universe   — scored by the bottom-up engine vs NIFTYBEES.NS benchmark
  2.  Single Names     — organized by theme, for future stock-picking layer

Both tiers get ingested into daily_prices so we always have data ready.

Ticker conventions:
  Hong Kong  — "XXXX.HK"   (exchange SEHK, currency HKD)
  India NSE  — "SYMBOL.NS"  (exchange NSE,  currency INR)
  India BSE  — "SYMBOL.BO"  (exchange BSE,  currency INR)
  US         — plain symbol  (exchange SMART, currency USD)

ingest.py must parse suffixes to set the correct IBKR contract params.
"""

BROAD_MARKET = ["SPY", "QQQ", "IWM", "DIA", "MDY"]

SECTORS = [
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLC",
    "XLY", "XLP", "XLU", "XLRE", "XLB",
]

THEMATIC_ETFS = [
    "SOXX", "SMH",
    "XBI", "IBB",
    "IGV", "SKYY",
    "HACK", "CIBR",
    "AIQ",
    "QTUM",
    "FINX",
    "TAN", "ICLN",
    "LIT", "DRIV",
    "URA", "NLR", "URNM",
    "IBIT", "BLOK",
    "MTUM",
    "ITA",
    "ARKK", "ARKG",
    "KWEB",
    "DTCR",
]

INTERNATIONAL = [
    "EEM", "EFA", "VWO", "FXI", "EWJ",
    "EWZ", "INDA", "EWG", "EWT", "EWY",
]

HK_ETFS = ["2800.HK", "2828.HK", "3033.HK", "3067.HK"]

FIXED_INCOME = ["TLT", "AGG"]

COMMODITIES = ["GLD", "SLV", "USO", "UNG", "DBA", "DBC"]

ETF_UNIVERSE = (
    BROAD_MARKET
    + SECTORS
    + THEMATIC_ETFS
    + INTERNATIONAL
    + HK_ETFS
    + FIXED_INCOME
    + COMMODITIES
)

HK_SINGLE_NAMES = [
    "0285.HK", "0700.HK", "0881.HK", "0981.HK", "0992.HK", "1024.HK",
    "1157.HK", "1177.HK", "1211.HK", "1299.HK", "1317.HK", "1398.HK",
    "1428.HK", "1475.HK", "1585.HK", "1810.HK", "1833.HK", "1910.HK",
    "2015.HK", "2020.HK", "2269.HK", "2318.HK", "2333.HK",
    "2801.HK", "2823.HK", "2834.HK", "3074.HK",
    "3690.HK", "3692.HK", "3759.HK", "3988.HK", "6186.HK",
    "7226.HK", "9618.HK", "9626.HK", "9633.HK", "9866.HK", "9868.HK",
    "9888.HK", "9961.HK", "9988.HK", "0001.HK", "0002.HK", "0003.HK",
    "0005.HK", "0016.HK", "0175.HK", "0386.HK", "0388.HK", "0762.HK",
    "0823.HK", "0836.HK", "0857.HK", "0883.HK", "0939.HK", "0941.HK",
    "1109.HK", "1113.HK",
]

HK_UNIVERSE = sorted(set(HK_ETFS + HK_SINGLE_NAMES))

INDIA_LARGE_CAPS = [
    "ABB.NS", "AARTIIND.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS",
    "ALLCARGO.NS", "AMBER.NS", "ANDHRSUGAR.NS", "APOLLOHOSP.NS", "ARE&M.NS",
    "ASAHISONG.NS", "ASHIANA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "AXISBANK.NS",
    "BAJAJ-AUTO.NS", "BAJAJFINSV.NS", "BAJFINANCE.NS", "BDL.NS", "BEL.NS",
    "BHARATFORG.NS", "BHARTIARTL.NS", "BHAGERIA.NS", "BHEL.NS", "BIOCON.NS",
    "BORORENEW.NS", "BRITANNIA.NS", "CAPLIPOINT.NS", "CGPOWER.NS",
    "CIPLA.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "CONTROLPR.NS",
    "CRAFTSMAN.NS", "CYIENTDLM.NS", "DABUR.NS", "DATAPATTNS.NS", "DBREALTY.NS",
    "DECCANCE.NS", "DIXON.NS", "DIVISLAB.NS", "DRREDDY.NS", "EICHERMOT.NS",
    "FIEMIND.NS", "FSL.NS", "GABRIEL.NS", "GALAXYSURF.NS", "GESHIP.NS",
    "GRASIM.NS", "GRSE.NS", "HCLTECH.NS", "HDFCBANK.NS", "HEIDELBERG.NS",
    "HEROMOTOCO.NS", "HGINFRA.NS", "HIKAL.NS", "HINDALCO.NS", "HINDUNILVR.NS",
    "ICICIBANK.NS", "ICICIPRULI.NS", "IDEAFORGE.NS", "INDUSINDBK.NS", "INFY.NS",
    "INSECTICID.NS", "INTELLECT.NS", "ITC.NS", "JSWSTEEL.NS", "JUBLFOOD.NS",
    "JYOTHYLAB.NS", "KALYANIFRG.NS", "KAYNES.NS", "KEI.NS", "KOTAKBANK.NS",
    "LAOPALA.NS", "LTF.NS", "LT.NS", "LTIM.NS", "MANAPPURAM.NS", "MARICO.NS",
    "MARUTI.NS", "METROBRAND.NS", "MINDACORP.NS",
    "MPHASIS.NS", "MTARTECH.NS", "MUTHOOTFIN.NS", "NAZARA.NS", "NCC.NS",
    "NESTLEIND.NS", "NAVINFLUOR.NS", "NMDC.NS", "NRBBEARING.NS", "NTPC.NS",
    "ONGC.NS", "PAYTM.NS", "PCBL.NS", "PERSISTENT.NS", "PIIND.NS",
    "POLYCAB.NS", "POWERGRID.NS", "PRESTIGE.NS", "RELIANCE.NS", "SAMHI.NS",
    "SBIN.NS", "SHAILY.NS", "SIEMENS.NS", "SIRCA.NS", "SJS.NS",
    "SKYGOLD.NS", "SONACOMS.NS", "STLTECH.NS", "SUNPHARMA.NS", "SWSOLAR.NS",
    "SYNGENE.NS", "SYRMA.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TCS.NS",
    "TECHM.NS", "TITAN.NS", "TRITURBINE.NS", "ULTRACEMCO.NS", "WABAG.NS",
    "WEBELSOLAR.NS", "WIPRO.NS",
]

INDIA_BENCHMARKS = ["NIFTYBEES.NS"]
INDIA_UNIVERSE = sorted(set(INDIA_LARGE_CAPS + INDIA_BENCHMARKS))

SINGLE_NAMES = {
    "ai_infrastructure": {
        "name": "AI Infrastructure / Neo-Cloud",
        "etf_proxy": "DTCR",
        "tickers": [
            "CRWV", "NBIS", "VRT", "ANET", "DLR", "EQIX", "AMT", "CLS", "NVT", "SMCI",
            "MSFT", "GOOGL", "META", "SNPS", "CDNS", "DOCN", "APPS"
        ],
    },
    "ai_platform": {
        "name": "AI Platform / Software",
        "etf_proxy": "AIQ",
        "tickers": [
            "MSFT", "GOOGL", "META", "NOW", "SNOW", "DDOG", "TWLO", "PLTR", "APP",
        ],
    },
    "chips": {
        "name": "Semiconductors",
        "etf_proxy": "SOXX",
        "tickers": [
            "NVDA", "AMD", "AVGO", "MRVL", "QCOM", "INTC", "MU", "LRCX",
            "KLAC", "AMAT", "TSM", "ASML", "ARM", "SMCI", "MBLY", "SIMO", "SNDK", "SMTC"
        ],
    },
    "quantum": {
        "name": "Quantum Computing",
        "etf_proxy": "QTUM",
        "tickers": ["IONQ", "QBTS", "RGTI", "QUBT", "ARQQ"],
    },
    "nuclear": {
        "name": "Nuclear / Uranium / Power",
        "etf_proxy": "URA",
        "tickers": ["CCJ", "UEC", "NNE", "LEU", "SMR", "OKLO", "UAMY", "TLN", "GEV"],
    },
    "megacap": {
        "name": "Mega Cap Tech",
        "etf_proxy": "QQQ",
        "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AVGO", "LLY", "JPM", "WMT", "HD", "XOM", "ORCL"],
    },
    "data_centers": {
        "name": "Data Centers / Infrastructure",
        "etf_proxy": "DTCR",
        "tickers": ["EQIX", "DLR", "AMT", "VRT", "ANET", "CLS", "TSSI", "NVT", "NET", "CRWV"],
    },
    "bitcoin": {
        "name": "Bitcoin / Digital Assets",
        "etf_proxy": "IBIT",
        "tickers": ["MSTR", "COIN", "MARA", "RIOT", "CLSK", "HIVE", "CRCL"],
    },
    "hk_china": {
        "name": "Hong Kong / China Tech",
        "etf_proxy": "KWEB",
        "tickers": [
            "BABA", "JD", "PDD", "BIDU", "NIO", "XPEV", "LI", "TCOM",
            "1211.HK", "2845.HK", "3690.HK", "7226.HK", "9618.HK", "9866.HK", "9888.HK", "9961.HK", "9988.HK",
        ],
    },
    "momentum": {
        "name": "High Momentum Names",
        "etf_proxy": "MTUM",
        "tickers": [
            "APP", "CRWD", "PANW", "CEG", "VST", "AXON", "DECK", "ANET", "NOW", "UBER",
            "ROKU", "TTD", "HOOD", "SOUN", "UPST", "SOFI", "TOST", "GLBE", "GENI", "VRT","TEAM", "PGY", "AEHR",

        ],
    },
    "defense": {
        "name": "Defense & Aerospace",
        "etf_proxy": "ITA",
        "tickers": ["LMT", "RTX", "NOC", "GD", "BA", "LHX", "HII", "KTOS"],
    },
    "biotech": {
        "name": "Biotech / Genomics",
        "etf_proxy": "XBI",
        "tickers": ["MRNA", "REGN", "VRTX", "AMGN", "GILD", "CRSP", "NTLA", "BEAM", "EDIT", "VKTX", "TEM", "PRME"],
    },
    "clean_energy": {
        "name": "Clean Energy / EV",
        "etf_proxy": "ICLN",
        "tickers": ["TSLA", "RIVN", "LCID", "FSLR", "ENPH", "SEDG", "RUN", "PLUG", "BE", "QS", "MP", "MBLY", "OUST"],
    },
    "fintech": {
        "name": "Fintech / Payments",
        "etf_proxy": "FINX",
        "tickers": ["SOFI", "UPST", "HOOD", "TOST", "PGY", "GLBE", "MELI", "NU"],
    },
    "power_infra": {
        "name": "Power / Energy Infrastructure",
        "etf_proxy": "XLU",
        "tickers": ["CEG", "VST", "GEV", "TLN", "LNG", "NVT", "VRT", "CLS"],
    },
    "global_tech": {
        "name": "Global / Emerging Tech",
        "etf_proxy": "EEM",
        "tickers": ["SE", "MELI", "JMIA", "GCT"],
    },
    "energy": {
        "name": "Energy",
        "etf_proxy": "XLE",
        "tickers": ["XOM", "CVX", "COP", "SLB", "EOG", "WMB", "OKE", "KMI", "TRGP", "VLO", "MPC", "PSX", "OXY", "FANG", "DVN", "HAL", "BKR", "EQT"],
    },
    "consumer_staples": {
        "name": "Consumer Staples",
        "etf_proxy": "XLP",
        "tickers": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "MDLZ", "CL", "KMB"],
    },
    "utilities_defensive": {
        "name": "Utilities",
        "etf_proxy": "XLU",
        "tickers": ["NEE", "DUK", "SO", "AEP", "EXC", "SRE", "PEG", "XEL", "ED", "D"],
    },
    "materials": {
        "name": "Materials",
        "etf_proxy": "XLB",
        "tickers": ["LIN", "APD", "SHW", "NUE", "FCX", "DOW", "NEM", "ECL", "CF", "MOS"],
    },
    "software": {
        "name": "software",
        "etf_proxy": "IGV",
        "tickers": ["HUBS", "CRM", "ADBE", "NOW", "PATH", 'DUOL', "TTD"],
    },
    "healthcare_core": {
        "name": "Healthcare Core",
        "etf_proxy": "XLV",
        "tickers": ["UNH", "JNJ", "ABBV", "PFE", "BMY", "TMO", "ISRG", "MDT", "SYK", "BSX"],
    },
    "real_estate": {
        "name": "Real Estate",
        "etf_proxy": "XLRE",
        "tickers": ["PLD", "SPG", "O", "WELL", "AVB", "VICI", "PSA", "CBRE"],
    },
    "india": {
        "name": "India",
        "etf_proxy": "INDA",
        "tickers": [
            "AARTIIND.NS", "ALLCARGO.NS", "AMBER.NS", "ANDHRSUGAR.NS", "ARE&M.NS",
            "ASAHISONG.NS", "ASHIANA.NS", "ASHOKLEY.NS", "AXISBANK.NS", "BAJAJFINSV.NS",
            "BDL.NS", "BEL.NS", "BHAGERIA.NS", "BHARATFORG.NS", "BIOCON.NS",
            "BORORENEW.NS", "CAPLIPOINT.NS", "COALINDIA.NS", "COCHINSHIP.NS",
            "CONTROLPR.NS", "CRAFTSMAN.NS", "CYIENTDLM.NS", "DABUR.NS", "DATAPATTNS.NS",
            "DBREALTY.NS", "DECCANCE.NS", "DIXON.NS", "EICHERMOT.NS", "FIEMIND.NS",
            "FSL.NS", "GABRIEL.NS", "GALAXYSURF.NS", "GESHIP.NS", "GRSE.NS",
            "HDFCBANK.NS", "HEIDELBERG.NS", "HGINFRA.NS", "HIKAL.NS", "ICICIBANK.NS",
            "ICICIPRULI.NS", "IDEAFORGE.NS", "INDUSINDBK.NS", "INSECTICID.NS", "INTELLECT.NS",
            "ITC.NS", "JUBLFOOD.NS", "JYOTHYLAB.NS", "KALYANIFRG.NS", "KAYNES.NS",
            "KEI.NS", "KOTAKBANK.NS", "LAOPALA.NS", "LTF.NS", "LT.NS",
            "LTIM.NS", "MANAPPURAM.NS", "MARICO.NS", "METROBRAND.NS",
            "MINDACORP.NS", "MTARTECH.NS", "MUTHOOTFIN.NS", "NAVINFLUOR.NS",
            "NAZARA.NS", "NCC.NS", "PAYTM.NS", "PCBL.NS", "PIIND.NS",
            "POLYCAB.NS", "PRESTIGE.NS", "RELIANCE.NS", "SAMHI.NS", "SHAILY.NS",
            "SIRCA.NS", "SJS.NS", "SKYGOLD.NS", "SONACOMS.NS", "STLTECH.NS",
            "SWSOLAR.NS", "SYNGENE.NS", "SYRMA.NS", "TATAPOWER.NS", "TRITURBINE.NS",
            "WABAG.NS", "WEBELSOLAR.NS",
        ],
    },
}


def is_hk_ticker(symbol: str) -> bool:
    return symbol.upper().endswith('.HK')


def parse_hk_symbol(symbol: str) -> tuple[str, str]:
    code = symbol.replace('.HK', '')
    return code, 'SEHK'


def is_india_ticker(symbol: str) -> bool:
    s = symbol.upper()
    return s.endswith('.NS') or s.endswith('.BO')


def parse_india_symbol(symbol: str) -> tuple[str, str]:
    if symbol.endswith('.NS'):
        return symbol.replace('.NS', ''), 'NSE'
    elif symbol.endswith('.BO'):
        return symbol.replace('.BO', ''), 'BSE'
    raise ValueError(f'Not an India ticker: {symbol}')


def detect_market(symbol: str) -> str:
    if is_hk_ticker(symbol):
        return 'HK'
    if is_india_ticker(symbol):
        return 'IN'
    return 'US'


CATEGORY_MAP: dict[str, str] = {}
for _sym in BROAD_MARKET:
    CATEGORY_MAP[_sym] = 'Broad Market'
for _sym in SECTORS:
    CATEGORY_MAP[_sym] = 'Sector'
for _sym in THEMATIC_ETFS:
    CATEGORY_MAP[_sym] = 'Thematic'
for _sym in INTERNATIONAL:
    CATEGORY_MAP[_sym] = 'International'
for _sym in HK_ETFS:
    CATEGORY_MAP[_sym] = 'HK ETF'
for _sym in FIXED_INCOME:
    CATEGORY_MAP[_sym] = 'Fixed Income'
for _sym in COMMODITIES:
    CATEGORY_MAP[_sym] = 'Commodities'
for _sym in HK_SINGLE_NAMES:
    CATEGORY_MAP.setdefault(_sym, 'HK Single Name')
for _sym in INDIA_LARGE_CAPS:
    CATEGORY_MAP.setdefault(_sym, 'India Large Cap')
for _sym in INDIA_BENCHMARKS:
    CATEGORY_MAP.setdefault(_sym, 'India Benchmark')


def get_all_single_names() -> list[str]:
    all_t: set[str] = set()
    for theme in SINGLE_NAMES.values():
        all_t.update(theme['tickers'])
    return sorted(all_t)


def get_theme_etf_proxies() -> list[str]:
    return sorted({t['etf_proxy'] for t in SINGLE_NAMES.values()})


def get_themes_for_ticker(ticker: str) -> list[str]:
    return [key for key, theme in SINGLE_NAMES.items() if ticker in theme['tickers']]


def get_us_only_etfs() -> list[str]:
    return [s for s in ETF_UNIVERSE if not is_hk_ticker(s) and not is_india_ticker(s)]


def get_hk_only() -> list[str]:
    hk: set[str] = set(HK_UNIVERSE)
    for theme in SINGLE_NAMES.values():
        hk.update(s for s in theme['tickers'] if is_hk_ticker(s))
    return sorted(hk)


def get_india_only() -> list[str]:
    india: set[str] = set(INDIA_UNIVERSE)
    for theme in SINGLE_NAMES.values():
        india.update(s for s in theme['tickers'] if is_india_ticker(s))
    return sorted(india)


def get_full_universe() -> list[str]:
    all_syms: set[str] = set(ETF_UNIVERSE)
    all_syms.update(HK_UNIVERSE)
    all_syms.update(INDIA_UNIVERSE)
    all_syms.update(get_all_single_names())
    all_syms.update(get_theme_etf_proxies())
    return sorted(all_syms)


def get_universe_for_market(market: str) -> list[str]:
    if market == 'US':
        # Full universe: ETFs + US single names (matches MARKET_CONFIG)
        us_singles = [s for s in get_all_single_names()
                      if not s.endswith(".HK") and not s.endswith(".NS") and not s.endswith(".BO")]
        return sorted(set(list(ETF_UNIVERSE) + us_singles))
    elif market == 'HK':
        return list(HK_UNIVERSE)
    elif market == 'IN':
        return list(INDIA_UNIVERSE)
    else:
        raise ValueError(f"Unknown market: {market!r}  (expected 'US', 'HK', or 'IN')")


def print_universe():
    print(f"\n{'='*65}")
    print(f"  TIER 1 : ETF UNIVERSE  ({len(ETF_UNIVERSE)} symbols)")
    print(f"{'='*65}")
    etf_groups = [
        ('Broad Market', BROAD_MARKET),
        ('Sectors', SECTORS),
        ('Thematic ETFs', THEMATIC_ETFS),
        ('International', INTERNATIONAL),
        ('HK ETFs', HK_ETFS),
        ('Fixed Income', FIXED_INCOME),
        ('Commodities', COMMODITIES),
    ]
    for name, syms in etf_groups:
        print(f"  {name:16s} ({len(syms):2d}): {', '.join(syms)}")

    print(f"\n{'='*65}")
    print(f"  TIER 1b : HK SCORING UNIVERSE  ({len(HK_UNIVERSE)} symbols)")
    print(f"{'='*65}")
    print(f"  ETFs         ({len(HK_ETFS):2d}): {', '.join(HK_ETFS)}")
    print(f"  Single Names ({len(HK_SINGLE_NAMES):2d}): {', '.join(HK_SINGLE_NAMES[:10])}...")
    print('  Benchmark       : 2800.HK (Tracker Fund)')

    print(f"\n{'='*65}")
    print(f"  TIER 1c : INDIA SCORING UNIVERSE  ({len(INDIA_UNIVERSE)} symbols)")
    print(f"{'='*65}")
    print(f"  Large Caps   ({len(INDIA_LARGE_CAPS):2d}): {', '.join(INDIA_LARGE_CAPS[:8])}...")
    print('  Benchmark       : NIFTYBEES.NS (Nifty BeES)')

    singles = get_all_single_names()
    print(f"\n{'='*65}")
    print(f"  TIER 2 : SINGLE NAMES  ({len(singles)} unique across {len(SINGLE_NAMES)} themes)")
    print(f"{'='*65}")
    for key, theme in SINGLE_NAMES.items():
        print(
            f"  {theme['name']:30s} ({len(theme['tickers']):2d})"
            f"  proxy: {theme['etf_proxy']:5s}"
            f"  | {', '.join(theme['tickers'][:8])}"
            f"{'...' if len(theme['tickers']) > 8 else ''}"
        )

    hk_all = get_hk_only()
    print(f"\n{'='*65}")
    print(f"  ALL HK TICKERS  ({len(hk_all)} symbols — need SEHK/HKD)")
    print(f"{'='*65}")
    for i in range(0, len(hk_all), 8):
        print(f"  {', '.join(hk_all[i:i+8])}")

    india_all = get_india_only()
    print(f"\n{'='*65}")
    print(f"  ALL INDIA TICKERS  ({len(india_all)} symbols — need NSE/BSE)")
    print(f"{'='*65}")
    for i in range(0, len(india_all), 6):
        print(f"  {', '.join(india_all[i:i+6])}")

    full = get_full_universe()
    us_etfs = get_us_only_etfs()
    print(f"\n{'='*65}")
    print(f"  FULL UNIVERSE    : {len(full)} unique symbols")
    print(f"  US ETFs only     : {len(us_etfs)} symbols")
    print(f"  HK universe      : {len(HK_UNIVERSE)} symbols (scoring)")
    print(f"  HK all sources   : {len(hk_all)} symbols (incl. themes)")
    print(f"  India universe   : {len(INDIA_UNIVERSE)} symbols (scoring)")
    print(f"  India all sources: {len(india_all)} symbols (incl. themes)")
    print(f"{'='*65}\n")


if __name__ == '__main__':
    print_universe()
    
############################################