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