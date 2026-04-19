"""
config.py
---------
Central configuration for the smart-money rotation system.
All tunable parameters live here.  Every downstream module imports
from this file — nothing is hard-coded elsewhere.

To backtest alternative settings, duplicate this file as
config_v2.py and point your runner at it, or simply edit in place.
"""

from pathlib import Path
from common.credentials import PG_CONFIG, IBKR_PORT

# ═══════════════════════════════════════════════════════════════
# 1.  PROJECT PATHS
# ═══════════════════════════════════════════════════════════════
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
STAGING_FILE = DATA_DIR / "staging.json"

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
    "sma_short":            30,
    "sma_long":             50,

    # ── Volatility ─────────────────────────────────────────
    "atr_period":           14,
    "realized_vol_window":  20,

    # ── Volume & accumulation ──────────────────────────────
    "obv":                  True,
    "ad_line":              True,
    "volume_avg_window":    20,
    "amihud_window":        20,

    # ── Breadth ────────────────────────────────────────────
    "breadth_ma_windows":   [20, 50],

    # ── Normalization ──────────────────────────────────────
    "zscore_window":        60,

    # ── Correlation ────────────────────────────────────────
    "correlation_window":   60,
    
    # ── NEW: relative strength params ───────────────────────
    "rs_ema_span":                10,   # smoothing on RS ratio
    "rs_sma_span":                50,   # trend baseline for RS ratio
    "rs_slope_window":            20,   # rolling regression window
    "rs_zscore_window":           60,   # lookback for z-score normalization
    "rs_momentum_short":          10,   # short slope for acceleration
    "rs_momentum_long":           30,   # long slope for acceleration
    "rs_vol_confirm_threshold":  1.3,   # relative volume threshold
}

# ═══════════════════════════════════════════════════════════════
# 7.  RELATIVE STRENGTH PARAMETERS
# ═══════════════════════════════════════════════════════════════
RS_PARAMS = {
    "lookback_windows":     [10, 20],
    "slope_window":         10,
    "primary_benchmark":    "SPY",
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
# 9.  SCORING — SIX PILLARS
# ═══════════════════════════════════════════════════════════════
#
#   Pillar                      Weight   What it measures
#   ─────────────────────────   ──────   ────────────────────────
#   relative_strength           0.25     Cross-sectional rotation
#   trend_momentum              0.25     Absolute trend strength
#   volume_accumulation         0.20     Flow confirmation
#   breadth                     0.15     Participation within theme
#   options_volatility          0.10     Forward-looking positioning
#   liquidity_penalty           0.05     Quality gate (negative mod)
#
#   Weights MUST sum to 1.0.

PILLAR_WEIGHTS = {
    "relative_strength":    0.25,
    "trend_momentum":       0.25,
    "volume_accumulation":  0.20,
    "breadth":              0.15,
    "options_volatility":   0.10,
    "liquidity_penalty":    0.05,
}

# ── Per-pillar metric weights (equal-weight by default) ────────
# Set to None for equal-weight.  Override with a dict of
# {metric_name: weight} to fine-tune within a pillar.
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
#
#   method:
#     "zscore"     → winsorized z-score (default)
#     "percentile" → rank-based, output 0–1
#
#   zscore_cap: winsorize at ± this value (ignored for percentile)

NORMALIZATION = {
    "default_method":  "zscore",
    "zscore_cap":      3.0,
}

# Per-pillar override.  Any pillar not listed uses default_method.
PILLAR_NORM_OVERRIDE = {
    # "options_volatility": "percentile",
}

# ═══════════════════════════════════════════════════════════════
# 11. THEME SCORING MODE  (HYBRID)
# ═══════════════════════════════════════════════════════════════
#
#   "etf"    → compute on the theme ETF only
#   "basket" → compute on every name, then aggregate
#
#   Hybrid default:
#     - ETF for trend/momentum, relative strength
#     - Basket for breadth, volume/accumulation, options

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

# ── SCORING WEIGHTS ─────────────────────────────────────────
# Pillar weights must sum to 1.0
# Sub-component weights within each pillar must sum to 1.0
SCORING_WEIGHTS = {
    # Pillar weights
    "pillar_rotation":       0.35,
    "pillar_momentum":       0.25,
    "pillar_volatility":     0.20,
    "pillar_microstructure": 0.20,

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

# ── SCORING PARAMS ──────────────────────────────────────────
SCORING_PARAMS = {
    "rank_window":          60,    # rolling percentile-rank lookback
    "micro_slope_window":   10,    # slope lookback for OBV / A-D line
    "rs_momentum_scale":   500,    # multiplier before sigmoid (values ~±0.003)
}

# ── SECTOR CONFIGURATION ────────────────────────────────────
BENCHMARK_TICKER = "SPY"

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

SECTOR_RS_PARAMS = {
    "rs_lookback":      63,     # ~3 months
    "slope_window":     20,     # regression slope window
    "zscore_window":    63,     # z-score normalization lookback
    "momentum_window":  10,     # acceleration of RS slope
    "rank_smoothing":   5,      # smooth cross-sectional pct-ranks
}

# How much sector context adjusts the stock composite score
SECTOR_SCORE_ADJUSTMENT = {
    "enabled":      True,
    "max_boost":    0.10,       # best sector adds +0.10 to composite
    "max_penalty": -0.10,       # worst sector subtracts 0.10
}

# ── STRATEGY / SIGNAL PARAMETERS ────────────────────────────
SIGNAL_PARAMS = {
    # ── Entry thresholds ─────────────────────────────────────
    "entry_score_min":        0.60,   # adjusted score must exceed
    "entry_percentile_min":   0.70,   # score_percentile must exceed
    "entry_momentum_confirm": True,   # require momentum pillar > 0.55

    # ── Exit thresholds ──────────────────────────────────────
    "exit_score_below":       0.45,   # exit if adjusted drops below
    "exit_percentile_below":  0.30,   # exit if percentile drops below

    # ── Regime filters ───────────────────────────────────────
    "stock_regime_allowed":   ["leading", "improving"],
    "sector_regime_blocked":  ["lagging"],

    # ── Position sizing ──────────────────────────────────────
    "max_position_pct":       0.08,   # 8% max single position
    "min_position_pct":       0.02,   # 2% min (below this → skip)
    "base_position_pct":      0.05,   # 5% starting point
    "score_scale_factor":     1.5,    # how much score adjusts size
    "vol_penalty_threshold":  0.80,   # score_volatility below this
    "vol_penalty_factor":     0.60,   # → reduce size by 40%

    # ── Concentration limits ─────────────────────────────────
    "max_sector_exposure":    0.30,   # 30% max per sector
    "max_positions":          15,     # hard cap on open positions
    "min_positions":          5,      # diversification floor

    # ── Signal smoothing ─────────────────────────────────────
    "entry_confirm_days":     2,      # must hold above entry for N days
    "exit_confirm_days":      1,      # exit triggers after N days below
    "cooldown_days":          5,      # wait N days after exit before re-entry
}