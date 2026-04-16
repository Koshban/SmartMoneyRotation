# config.py
"""
Static universe, strategy parameters, and scoring weights.
Edit this file to add/remove tickers or tune the strategy.
"""

# ── Theme Buckets ───────────────────────────────────────────────────
UNIVERSE = {
    "AI_HIGH_BETA": [
        "NVDA", "AMD", "SMCI", "AVGO", "MRVL", "ARM", "TSM",
        "PLTR", "AI", "PATH", "SNPS", "CDNS",
    ],
    "QUANTUM": [
        "IONQ", "RGTI", "QBTS", "ARQQ",
    ],
    "NUCLEAR_URANIUM": [
        "CCJ", "URA", "UUUU", "NNE", "SMR", "LEU", "OKLO",
    ],
    "DATA_CENTER_INFRA": [
        "EQIX", "DLR", "VRT", "ANET", "CRWV", "PWR", "EME",
    ],
    "BITCOIN_DIGITAL": [
        "MSTR", "COIN", "MARA", "RIOT", "IBIT", "BITO", "CLSK",
    ],
    "SECTOR_ETFS": [
        "XLK", "XLE", "XLF", "XLI", "XLV", "XLU", "XLC",
        "XLRE", "XLB", "XLP", "XLY", "SMH", "SOXX", "IGV",
        "ARKK", "ARKW",
    ],
    "CHINA_HK_TECH": [
        "KWEB", "FXI", "BABA", "PDD", "JD", "BIDU", "TCOM",
    ],
}

BENCHMARKS = ["SPY", "QQQ", "IWM"]

# ── Reverse Lookup: ticker -> theme ─────────────────────────────────
TICKER_THEME = {}
for _theme, _tickers in UNIVERSE.items():
    for _t in _tickers:
        TICKER_THEME[_t] = _theme

# ── All Tickers (de-duped, sorted) ──────────────────────────────────
ALL_TICKERS = sorted(
    set(t for tl in UNIVERSE.values() for t in tl) | set(BENCHMARKS)
)

# ── Data Parameters ─────────────────────────────────────────────────
LOOKBACK_CALENDAR_DAYS = 365        # calendar days to fetch (covers ~252 trading days)
RETURN_WINDOWS = [5, 10, 15]        # trading days
RS_WINDOW = 10                      # ~2 weeks for relative-strength ratio

# ── Technical Parameters ────────────────────────────────────────────
RSI_PERIOD = 14
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
ADX_PERIOD = 14
EMA_PERIOD = 30
SMA_PERIODS = [30, 50]
ATR_PERIOD = 14
RVOL_WINDOW = 20                    # baseline for relative volume
OBV_SLOPE_WINDOW = 10               # slope lookback for OBV / A-D
AVWAP_ANCHOR = 20                   # rolling anchor for daily VWAP proxy
ZSCORE_WINDOW = 60                  # trailing window for time-series z-score
REALIZED_VOL_WINDOW = 20
AMIHUD_WINDOW = 20
INTRA_CORR_WINDOW = 20              # rolling pairwise correlation within theme

# ── Scoring Weights ─────────────────────────────────────────────────
CATEGORY_WEIGHTS = {
    "trend":                0.25,
    "momentum":             0.20,
    "relative_strength":    0.20,
    "volume_accumulation":  0.15,
    "breadth":              0.10,
    "volatility_quality":   0.05,
    "options_proxy":        0.05,
}

# ── Output ──────────────────────────────────────────────────────────
OUTPUT_DIR = "output"