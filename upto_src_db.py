"""
Let me provide the complete codebase again, and you check if CASH is complete or anything else needs rejigging.
If all completed now then We need to write a module for backtesting the entire project we have created for CASH smartmoneyrotattion strategies. 
We will need to load 20 years worth of Data , and then be able to backtest for any given period. We should also be able to backtest by default for the 20 years worth of data.

We can load data using the src/*/* components for the last 20 years.
We can create a separate backtest folder to then Test the strategies.
We will also need a method to calculate CAGR so that we can confirm which strategies ( if we need to tweak them ) gives best returns.
"""

"""
smartmoneyrotation/
└── backtest/
    ├── __init__.py
    ├── data_loader.py        # Download & cache 20 years of OHLCV
    ├── engine.py             # Run pipeline → backtest for any period
    ├── metrics.py            # CAGR, Sharpe, drawdown, annual returns
    ├── strategies.py         # Predefined strategy parameter variants
    ├── comparison.py         # Multi-strategy comparison framework
    └── runner.py             # CLI: python -m backtest.runner
└──common/
   └──  __init__.py
   └── config.py
   └── credentials.py
   └── universe.py
   └── expiry.py
└──compute/
     └──  __init__.py
     └── breadth.py
     └── indicators.py
     └── relative_strength.py
     └── scoring.py
     └── sector_rs.py
└──data/
     └──india_cash.parquet
     └──universe_cash.parquet
     └── staging.json
     └── options/
└──logs/
└──output/
    └──  __init__.py
    └── rankings.py
    └── reports.py
    └── signals.py
└──pipeline/
    └──  __init__.py
    └──  runner.py
    └──  orchestrator.py
└──portfolio/
    └──  __init__.py
    └── backtest.py
    └── rebalance.py
    └── risk.py
    └── sizing.py
└──reports/
    └──  __init__.py
    └──  portfolio_view.py
    └──  recommendations.py
    └──  weekly_report.py
    └──  html_report.py
└──results/
    └──  comparison_report.txt
    └──  run_logger.py
└──scripts/
    └──  run_market.py
    └──  run_strategy.py
└── src/
    └──db.py
	└── __init__.py
	└──  db.py
	└──  schema.py
	└──  load_db.py
	└──  loader.py
    └──  ingest_cash.py
    └──  ingest_options.py
    └──  __init__.py
└──strategy/
    ├── __init__.py
    ├── signals.py          ← per-ticker quality gates (unchanged)
    ├── portfolio.py        ← portfolio construction (unchanged)
    ├── rotation.py         ← top-down rotation engine (unchanged)
    ├── rotation_filters.py
    └── convergence.py      ← NEW: dual-list merge + market dispatcher
└── tests/
    └──  __init__.py
    └── test_connections.py ( and varios other tests )
 └──utils/
    └──  __init__.py
    └──  run_logger.py
 └── main.py  
 └── pyproject.toml         
 └── requirements.txt
 └── DataArchitecture.clj
 └── DataFlow.clj
 └── .gitignore
 
"""
#########################
"""
COMMON :
 ----------------------
 	credentials.py : Credentials, not to be committed to GH
"""	
######################### 

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
  - Crypto miners (MARA, RIOT, CLSK, HIVE, MSTR) → Technology
    per GICS (software / IT services).  COIN → Financials (exchange).
  - Nuclear reactor builders (SMR, NNE) → Industrials (pre-revenue
    equipment makers).  Nuclear utilities (CEG, VST, TLN, OKLO) → Utilities.
    Uranium miners (CCJ, UEC, LEU) → Energy.
  - Solar hardware (FSLR, ENPH, SEDG) → Technology per GICS
    (semiconductor equipment / electronic components).
  - UBER → Industrials (GICS reclassified to Ground Transportation 2023).
  - Each ticker gets exactly one sector, even if it appears in
    multiple themes in universe.py.
  - Consumer Staples has no single names currently.  Add PG, KO,
    PEP, COST, WMT etc. if you want stock picks when XLP leads.
"""

from __future__ import annotations


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
        # Semis / equipment
        "AMAT", "LRCX", "KLAC", "MU", "MRVL", "ARM", "SMCI", "MBLY",
        # Software / cloud / SaaS / infra
        "NOW", "SNOW", "NET", "PLTR", "PATH", "CRWD", "PANW", "TWLO",
        "CLS", "ANET", "TSSI", "TTD", "TOST", "PGY", "GLBE",
        # AI / robotics
        "AI", "NBIS", "SOUN", "PDYN", "CRWV", "APP",
        # Quantum computing
        "IONQ", "QBTS", "RGTI", "QUBT", "ARQQ",
        # Solar / clean-tech hardware (GICS: semis / electronic equipment)
        "FSLR", "ENPH", "SEDG", "RUN", "OUST",
        # Crypto mining / infra (GICS: IT services / software)
        "MSTR", "MARA", "RIOT", "CLSK", "HIVE",
        # Misc tech
        "GCT", "GENI",
        # India — electronics / IT / software
        "DIXON.NS", "KAYNES.NS", "SYRMA.NS", "CYIENTDLM.NS",
        "DATAPATTNS.NS", "CONTROLPR.NS", "FSL.NS", "INTELLECT.NS",
        "PAYTM.NS", "STLTECH.NS",
        # HK — tech ETFs
        "3033.HK", "3067.HK",
    ],

    # ── Communication Services ─────────────────────────────────
    "Communication Services": [
        "GOOGL", "META", "BIDU",
        "SNAP", "ROKU",
        "SE",       # Sea Limited (gaming / e-commerce)
        # India
        "NAZARA.NS",
        # HK
        "9888.HK",  # Baidu
    ],

    # ── Consumer Discretionary ─────────────────────────────────
    "Consumer Discretionary": [
        "AMZN", "TSLA", "DECK", "MELI", "JMIA",
        # China e-commerce / EV
        "BABA", "JD", "PDD", "NIO", "XPEV", "LI", "TCOM",
        # EV pure-play
        "RIVN", "LCID",
        # India
        "AMBER.NS", "EICHERMOT.NS", "FIEMIND.NS", "GABRIEL.NS",
        "METROBRAND.NS", "SAMHI.NS", "SJS.NS", "SKYGOLD.NS", "SONACOMS.NS",
        # HK
        "1211.HK",  # BYD
        "3690.HK",  # Meituan
        "9618.HK",  # JD.com
        "9866.HK",  # NIO
        "9961.HK",  # Trip.com
        "9988.HK",  # Alibaba
    ],

    # ── Financials ─────────────────────────────────────────────
    "Financials": [
        "BRK.B", "COIN", "SOFI", "UPST", "HOOD", "CRCL",
        # India
        "AXISBANK.NS", "HDFCBANK.NS", "ICICIBANK.NS", "KOTAKBANK.NS",
    ],

    # ── Healthcare ─────────────────────────────────────────────
    "Healthcare": [
        "LLY", "AMGN", "GILD", "REGN", "VRTX", "MRNA",
        # Genomics / biotech
        "CRSP", "NTLA", "BEAM", "EDIT", "VKTX",
        # Healthcare AI / misc
        "TEM", "PRME",
        # India
        "SHAILY.NS", "SYNGENE.NS",
    ],

    # ── Industrials ────────────────────────────────────────────
    "Industrials": [
        # Defense & aerospace
        "LMT", "RTX", "NOC", "GD", "BA", "LHX", "HII", "KTOS",
        # Power equipment / electrical
        "GEV", "VRT", "NVT",
        # Nuclear reactor builders (pre-revenue / equipment)
        "SMR", "NNE",
        # Transport / misc
        "AXON", "UBER",
        # Clean-energy industrials (fuel cells, batteries)
        "PLUG", "BE", "QS",
        # India
        "ARE&M.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS",
        "COCHINSHIP.NS", "CRAFTSMAN.NS", "GESHIP.NS", "GRSE.NS",
        "HGINFRA.NS", "IDEAFORGE.NS", "KEI.NS", "LT.NS",
        "MTARTECH.NS", "NCC.NS", "POLYCAB.NS", "TRITURBINE.NS", "WABAG.NS",
    ],

    # ── Energy ─────────────────────────────────────────────────
    "Energy": [
        # Uranium miners / fuel
        "CCJ", "UEC", "LEU",
        # LNG
        "LNG",
        # India
        "RELIANCE.NS",
    ],

    # ── Utilities ──────────────────────────────────────────────
    "Utilities": [
        # Nuclear / power generators
        "CEG", "VST", "TLN", "OKLO",
        # India
        "BORORENEW.NS", "SWSOLAR.NS", "TATAPOWER.NS", "WEBELSOLAR.NS",
        # HK
        "2845.HK",  # GX China Clean Energy
    ],

    # ── Materials ──────────────────────────────────────────────
    "Materials": [
        "MP",     # MP Materials (rare earth)
        "UAMY",   # International Consolidated Minerals (vanadium/uranium)
        # India
        "DECCANCE.NS", "GALAXYSURF.NS", "NAVINFLUOR.NS",
        "PCBL.NS", "PIIND.NS", "SIRCA.NS",
    ],

    # ── Real Estate ────────────────────────────────────────────
    "Real Estate": [
        "EQIX", "DLR", "AMT",
        # India
        "DBREALTY.NS", "PRESTIGE.NS",
    ],

    # ── Consumer Staples ───────────────────────────────────────
    "Consumer Staples": [
        # No single names in current universe.
        # Consider adding: PG, KO, PEP, COST, WMT, PM, MO, CL, KMB
    ],
}


# ═══════════════════════════════════════════════════════════════
#  THEMATIC ETF → PARENT GICS SECTOR
# ═══════════════════════════════════════════════════════════════
# Thematic ETFs can optionally be included as "tickers" inside
# their parent sector for RS ranking.

THEMATIC_ETF_SECTOR: dict[str, str] = {
    # Semiconductors
    "SOXX": "Technology",
    "SMH":  "Technology",
    # Biotech
    "XBI":  "Healthcare",
    "IBB":  "Healthcare",
    # Software / Cloud
    "IGV":  "Technology",
    "SKYY": "Technology",
    # Cybersecurity
    "HACK": "Technology",
    "CIBR": "Technology",
    # AI / Robotics
    "BOTZ": "Technology",
    "AIQ":  "Technology",
    # Quantum
    "QTUM": "Technology",
    # Fintech
    "FINX": "Financials",
    # Clean Energy / Solar
    "TAN":  "Utilities",
    "ICLN": "Utilities",
    # EV / Lithium
    "LIT":  "Materials",
    "DRIV": "Consumer Discretionary",
    # Nuclear / Uranium
    "URA":  "Energy",
    "URNM": "Energy",
    "NLR":  "Utilities",
    # Bitcoin / Blockchain
    "IBIT": "Financials",
    "BLOK": "Financials",
    # Defense
    "ITA":  "Industrials",
    # Innovation
    "ARKK": "Technology",
    "ARKG": "Healthcare",
    # China Internet
    "KWEB": "Communication Services",
}


# ═══════════════════════════════════════════════════════════════
#  NON-SECTOR ASSETS  (excluded from sector rotation)
# ═══════════════════════════════════════════════════════════════

NON_SECTOR_ASSETS: dict[str, str] = {
    # Broad Market (benchmark / reference)
    "SPY":  "Broad Market",
    "QQQ":  "Broad Market",
    "IWM":  "Broad Market",
    "DIA":  "Broad Market",
    "MDY":  "Broad Market",
    # Momentum factor
    "MTUM": "Factor",
    # International
    "EEM":     "International",
    "EFA":     "International",
    "VWO":     "International",
    "FXI":     "International",
    "EWJ":     "International",
    "EWZ":     "International",
    "INDA":    "International",
    "EWG":     "International",
    "EWT":     "International",
    "EWY":     "International",
    "2800.HK": "International",  # Tracker Fund HK
    "2828.HK": "International",  # Hang Seng H-Share ETF
    "7226.HK": "International",  # HSI Leveraged
    # Fixed Income
    "TLT": "Fixed Income",
    "IEF": "Fixed Income",
    "HYG": "Fixed Income",
    "LQD": "Fixed Income",
    "TIP": "Fixed Income",
    "AGG": "Fixed Income",
    # Commodities
    "GLD": "Commodities",
    "SLV": "Commodities",
    "USO": "Commodities",
    "UNG": "Commodities",
    "DBA": "Commodities",
    "DBC": "Commodities",
}


# ═══════════════════════════════════════════════════════════════
#  COMPILED FLAT MAP  (ticker → sector)
# ═══════════════════════════════════════════════════════════════

TICKER_SECTOR_MAP: dict[str, str] = {}

# 1. Sector ETFs themselves
for _sector, _etf in SECTOR_ETFS.items():
    TICKER_SECTOR_MAP[_etf] = _sector

# 2. All single-name tickers
for _sector, _tickers in _SECTOR_TICKERS.items():
    for _t in _tickers:
        TICKER_SECTOR_MAP[_t] = _sector

# 3. Thematic ETFs
TICKER_SECTOR_MAP.update(THEMATIC_ETF_SECTOR)


# ═══════════════════════════════════════════════════════════════
#  LOOKUP HELPERS
# ═══════════════════════════════════════════════════════════════

def get_sector(ticker: str) -> str | None:
    """
    Return GICS sector for a ticker, or None if it doesn't map
    to any of the 11 sectors (e.g. international, fixed income).
    """
    return TICKER_SECTOR_MAP.get(ticker)


def get_asset_class(ticker: str) -> str | None:
    """Return asset class for non-sector tickers."""
    return NON_SECTOR_ASSETS.get(ticker)


def get_sector_or_class(ticker: str) -> str:
    """Return sector if available, else asset class, else 'Unknown'."""
    return (
        TICKER_SECTOR_MAP.get(ticker)
        or NON_SECTOR_ASSETS.get(ticker)
        or "Unknown"
    )


def get_tickers_for_sector(sector: str) -> list[str]:
    """
    All tickers (single names + thematic ETFs) in a GICS sector.
    Excludes the sector ETF itself (that's the benchmark).
    """
    return [t for t, s in TICKER_SECTOR_MAP.items()
            if s == sector and t not in SECTOR_ETFS.values()]


def get_us_tickers_for_sector(sector: str) -> list[str]:
    """Same as above but US-listed only (no .HK / .NS)."""
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector
        and t not in SECTOR_ETFS.values()
        and not t.endswith(".HK")
        and not t.endswith(".NS")
    ]


def get_india_tickers_for_sector(sector: str) -> list[str]:
    """Return only .NS tickers for a given sector."""
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector and t.endswith(".NS")
    ]


def get_hk_tickers_for_sector(sector: str) -> list[str]:
    """Return only .HK tickers for a given sector."""
    return [
        t for t, s in TICKER_SECTOR_MAP.items()
        if s == sector and t.endswith(".HK")
    ]


def validate_universe_coverage():
    """
    Cross-check against universe.py to find any unmapped tickers.
    Call this during startup or testing.
    """
    from common.universe import get_full_universe

    all_known = set(TICKER_SECTOR_MAP.keys()) | set(NON_SECTOR_ASSETS.keys())
    full = set(get_full_universe())

    unmapped = full - all_known
    if unmapped:
        print(f"⚠️  {len(unmapped)} unmapped tickers: {sorted(unmapped)}")
    else:
        print(f"✅  All {len(full)} tickers mapped.")

    return unmapped


# ═══════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═══════════════════════════════════════════════════════════════

def print_sector_map():
    """Pretty-print sector assignments for verification."""
    print(f"\n{'='*70}")
    print(f"  SECTOR MAP  ({len(TICKER_SECTOR_MAP)} tickers → 11 GICS sectors)")
    print(f"{'='*70}")

    for sector in sorted(SECTOR_ETFS.keys()):
        etf = SECTOR_ETFS[sector]
        tickers = get_tickers_for_sector(sector)
        us = sorted(t for t in tickers
                     if not t.endswith('.HK') and not t.endswith('.NS'))
        hk = sorted(t for t in tickers if t.endswith('.HK'))
        india = sorted(t for t in tickers if t.endswith('.NS'))

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

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    print_sector_map()
    validate_universe_coverage()

############################################
"""
common/universe.py
-----------
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

# ═════════════════════════════════════════════════════════════════
#  TIER 1 :  ETF UNIVERSE  (core US rotation engine)
# ═════════════════════════════════════════════════════════════════

# ── Broad Market ───────────────────────────────────────────────
BROAD_MARKET = ["SPY", "QQQ", "IWM", "DIA", "MDY"]

# ── SPDR Sectors ───────────────────────────────────────────────
SECTORS = [
    "XLK",   # Technology
    "XLF",   # Financials
    "XLE",   # Energy
    "XLV",   # Healthcare
    "XLI",   # Industrials
    "XLC",   # Communication Services
    "XLY",   # Consumer Discretionary
    "XLP",   # Consumer Staples
    "XLU",   # Utilities
    "XLRE",  # Real Estate
    "XLB",   # Materials
]

# ── Thematic ETFs ──────────────────────────────────────────────
THEMATIC_ETFS = [
    # Semiconductors
    "SOXX", "SMH",
    # Biotech
    "XBI", "IBB",
    # Software / Cloud
    "IGV", "SKYY",
    # Cybersecurity
    "HACK", "CIBR",
    # AI / Robotics
    "BOTZ", "AIQ",
    # Quantum
    "QTUM",
    # Fintech
    "FINX",
    # Clean Energy / Solar
    "TAN", "ICLN",
    # Lithium / EV
    "LIT", "DRIV",
    # Nuclear / Uranium
    "URA", "NLR", "URNM",
    # Bitcoin / Blockchain
    "IBIT", "BLOK",
    # Momentum Factor
    "MTUM",
    # Defense
    "ITA",
    # Innovation
    "ARKK", "ARKG",
    # China Internet
    "KWEB",
]

# ── International ──────────────────────────────────────────────
INTERNATIONAL = [
    "EEM",   # Emerging Markets
    "EFA",   # EAFE (Developed ex-US)
    "VWO",   # Emerging Markets (Vanguard)
    "FXI",   # China Large Cap
    "EWJ",   # Japan
    "EWZ",   # Brazil
    "INDA",  # India
    "EWG",   # Germany
    "EWT",   # Taiwan
    "EWY",   # South Korea
]

# ── Hong Kong ETFs ─────────────────────────────────────────────
# Format: "XXXX.HK" — ingest.py parses suffix for SEHK / HKD
HK_ETFS = [
    "2800.HK",  # Tracker Fund of Hong Kong
    "2828.HK",  # Hang Seng H-Share ETF
    "3033.HK",  # CSOP Hang Seng TECH ETF
    "3067.HK",  # iShares Hang Seng TECH ETF
]

# ── Fixed Income ───────────────────────────────────────────────
FIXED_INCOME = [
    "TLT",   # 20+ Year Treasury
    "IEF",   # 7-10 Year Treasury
    "HYG",   # High Yield Corporate
    "LQD",   # Investment Grade Corporate
    "TIP",   # TIPS
    "AGG",   # US Aggregate Bond
]

# ── Commodities / Alternatives ─────────────────────────────────
COMMODITIES = [
    "GLD",   # Gold
    "SLV",   # Silver
    "USO",   # Oil
    "UNG",   # Natural Gas
    "DBA",   # Agriculture
    "DBC",   # Commodities Basket
]

# ── Combined ETF Universe ─────────────────────────────────────
ETF_UNIVERSE = (
    BROAD_MARKET
    + SECTORS
    + THEMATIC_ETFS
    + INTERNATIONAL
    + HK_ETFS
    + FIXED_INCOME
    + COMMODITIES
)


# ═════════════════════════════════════════════════════════════════
#  TIER 1b :  HK SCORING UNIVERSE
# ═════════════════════════════════════════════════════════════════
# Scored by the bottom-up engine against 2800.HK benchmark.
# No rotation engine, no sector RS — pure individual scoring.
# Roughly tracks the Hang Seng Composite + select H-shares.
#
# config.py imports HK_UNIVERSE for MARKET_CONFIG["HK"]["universe"].

HK_SINGLE_NAMES = [
    # ── China Tech ─────────────────────────────────────────
    "0700.HK",   # Tencent
    "9988.HK",   # Alibaba
    "3690.HK",   # Meituan
    "9618.HK",   # JD.com
    "9888.HK",   # Baidu
    "1810.HK",   # Xiaomi
    "9999.HK",   # NetEase
    "9626.HK",   # Bilibili
    "1024.HK",   # Kuaishou
    "0992.HK",   # Lenovo

    # ── Financials / Insurance ─────────────────────────────
    "1299.HK",   # AIA Group
    "0005.HK",   # HSBC Holdings
    "0388.HK",   # HK Exchanges & Clearing
    "2318.HK",   # Ping An Insurance
    "0939.HK",   # China Construction Bank
    "1398.HK",   # ICBC
    "3988.HK",   # Bank of China

    # ── Property / REITs ───────────────────────────────────
    "0001.HK",   # CK Hutchison
    "1113.HK",   # CK Asset Holdings
    "0016.HK",   # Sun Hung Kai Properties
    "0823.HK",   # Link REIT
    "1109.HK",   # China Resources Land

    # ── Energy / Utilities ─────────────────────────────────
    "0883.HK",   # CNOOC
    "0857.HK",   # PetroChina
    "0002.HK",   # CLP Holdings
    "0003.HK",   # HK & China Gas

    # ── Auto / EV ──────────────────────────────────────────
    "1211.HK",   # BYD Company
    "2015.HK",   # Li Auto
    "9868.HK",   # XPeng
    "0175.HK",   # Geely Automobile

    # ── Consumer ───────────────────────────────────────────
    "9633.HK",   # Nongfu Spring
    "2020.HK",   # Anta Sports

    # ── Telecom ────────────────────────────────────────────
    "0941.HK",   # China Mobile
    "0762.HK",   # China Unicom

    # ── Healthcare / Biotech ───────────────────────────────
    "1177.HK",   # Sino Biopharmaceutical
    "2269.HK",   # WuXi Biologics
    "3692.HK",   # Hansoh Pharmaceutical

    # ── Additional (from hk_china single-names theme) ──────
    "9866.HK",   # NIO Inc
    "9961.HK",   # Trip.com
]

# Combined: ETFs + single names (deduplicated, sorted)
HK_UNIVERSE = sorted(set(HK_ETFS + HK_SINGLE_NAMES))


# ═════════════════════════════════════════════════════════════════
#  TIER 1c :  INDIA SCORING UNIVERSE
# ═════════════════════════════════════════════════════════════════
# Scored by the bottom-up engine against NIFTYBEES.NS benchmark.
# Roughly tracks the Nifty 50 — large-cap, liquid names where
# momentum / volume indicators behave predictably.
#
# The smaller-cap India picks in Tier 2 SINGLE_NAMES["india"]
# stay in the stock-picking layer and are NOT scored here.
#
# config.py imports INDIA_UNIVERSE for MARKET_CONFIG["IN"]["universe"].

INDIA_LARGE_CAPS = [
    # ── IT Services ────────────────────────────────────────
    "TCS.NS",         # Tata Consultancy Services
    "INFY.NS",        # Infosys
    "WIPRO.NS",       # Wipro
    "HCLTECH.NS",     # HCL Technologies
    "TECHM.NS",       # Tech Mahindra
    "LTIM.NS",        # LTIMindtree

    # ── Financials ─────────────────────────────────────────
    "HDFCBANK.NS",    # HDFC Bank
    "ICICIBANK.NS",   # ICICI Bank
    "SBIN.NS",        # State Bank of India
    "KOTAKBANK.NS",   # Kotak Mahindra Bank
    "AXISBANK.NS",    # Axis Bank
    "BAJFINANCE.NS",  # Bajaj Finance
    "BAJFINSV.NS",    # Bajaj Finserv
    "INDUSINDBK.NS",  # IndusInd Bank

    # ── Energy / Conglomerate ──────────────────────────────
    "RELIANCE.NS",    # Reliance Industries
    "ONGC.NS",        # Oil & Natural Gas Corp
    "NTPC.NS",        # NTPC Ltd
    "POWERGRID.NS",   # Power Grid Corp
    "ADANIGREEN.NS",  # Adani Green Energy
    "COALINDIA.NS",   # Coal India

    # ── Consumer ───────────────────────────────────────────
    "HINDUNILVR.NS",  # Hindustan Unilever
    "ITC.NS",         # ITC Ltd
    "ASIANPAINT.NS",  # Asian Paints
    "TITAN.NS",       # Titan Company
    "NESTLEIND.NS",   # Nestle India
    "BRITANNIA.NS",   # Britannia Industries
    "MARUTI.NS",      # Maruti Suzuki

    # ── Industrials ────────────────────────────────────────
    "LT.NS",          # Larsen & Toubro
    "ADANIENT.NS",    # Adani Enterprises
    "ADANIPORTS.NS",  # Adani Ports
    "ULTRACEMCO.NS",  # UltraTech Cement
    "GRASIM.NS",      # Grasim Industries
    "TATASTEEL.NS",   # Tata Steel
    "JSWSTEEL.NS",    # JSW Steel
    "HINDALCO.NS",    # Hindalco

    # ── Pharma / Healthcare ────────────────────────────────
    "SUNPHARMA.NS",   # Sun Pharmaceutical
    "DRREDDY.NS",     # Dr. Reddy's Laboratories
    "CIPLA.NS",       # Cipla
    "APOLLOHOSP.NS",  # Apollo Hospitals
    "DIVISLAB.NS",    # Divi's Laboratories

    # ── Telecom ────────────────────────────────────────────
    "BHARTIARTL.NS",  # Bharti Airtel

    # ── Auto ───────────────────────────────────────────────
    "TATAMOTORS.NS",  # Tata Motors
    "M&M.NS",         # Mahindra & Mahindra
    "EICHERMOT.NS",   # Eicher Motors
    "BAJAJ-AUTO.NS",  # Bajaj Auto
    "HEROMOTOCO.NS",  # Hero MotoCorp
]

# Benchmark ETF is included so data is always fetched alongside the universe
INDIA_BENCHMARKS = [
    "NIFTYBEES.NS",   # Nippon India Nifty BeES — tracks Nifty 50
]

# Combined (deduplicated, sorted)
INDIA_UNIVERSE = sorted(set(INDIA_LARGE_CAPS + INDIA_BENCHMARKS))


# ═════════════════════════════════════════════════════════════════
#  TIER 2 :  SINGLE NAMES  (future stock-picking layer)
# ═════════════════════════════════════════════════════════════════

SINGLE_NAMES = {

    "ai": {
        "name": "Artificial Intelligence",
        "etf_proxy": "BOTZ",
        "tickers": [
            "NVDA", "AMD", "MSFT", "GOOGL", "META",
            "PLTR", "SMCI", "ARM", "AVGO", "MRVL",
            "AI", "PATH", "SNOW", "NBIS", "SOUN",
            "PDYN", "TEM", "CLS", "NET", "CRWV",
            "TTD", "TWLO",
        ],
    },

    "chips": {
        "name": "Semiconductors",
        "etf_proxy": "SOXX",
        "tickers": [
            "NVDA", "AMD", "AVGO", "MRVL", "QCOM",
            "INTC", "MU", "LRCX", "KLAC", "AMAT",
            "TSM", "ASML", "ARM", "SMCI", "MBLY",
        ],
    },

    "quantum": {
        "name": "Quantum Computing",
        "etf_proxy": "QTUM",
        "tickers": [
            "IONQ", "QBTS", "RGTI", "QUBT", "ARQQ",
        ],
    },

    "nuclear": {
        "name": "Nuclear / Uranium / Power",
        "etf_proxy": "URA",
        "tickers": [
            "CCJ", "UEC", "NNE", "LEU", "SMR",
            "OKLO", "UAMY", "TLN", "GEV",
        ],
    },

    "megacap": {
        "name": "Mega Cap Tech",
        "etf_proxy": "QQQ",
        "tickers": [
            "AAPL", "MSFT", "GOOGL", "AMZN", "META",
            "NVDA", "TSLA", "AVGO", "BRK.B", "LLY",
        ],
    },

    "data_centers": {
        "name": "Data Centers / Infrastructure",
        "etf_proxy": "SRVR",
        "tickers": [
            "EQIX", "DLR", "AMT", "VRT", "ANET",
            "CLS", "TSSI", "NVT", "NET",
        ],
    },

    "bitcoin": {
        "name": "Bitcoin / Digital Assets",
        "etf_proxy": "IBIT",
        "tickers": [
            "MSTR", "COIN", "MARA", "RIOT", "CLSK",
            "HIVE", "CRCL",
        ],
    },

    "hk_china": {
        "name": "Hong Kong / China Tech",
        "etf_proxy": "KWEB",
        "tickers": [
            # US-listed ADRs
            "BABA", "JD", "PDD", "BIDU",
            "NIO", "XPEV", "LI", "TCOM",
            # HK-listed shares
            "1211.HK",   # BYD
            "2845.HK",   # GX China Clean Energy
            "3690.HK",   # Meituan
            "7226.HK",   # HSI Leveraged
            "9618.HK",   # JD.com
            "9866.HK",   # NIO
            "9888.HK",   # Baidu
            "9961.HK",   # Trip.com
            "9988.HK",   # Alibaba
        ],
    },

    "momentum": {
        "name": "High Momentum Names",
        "etf_proxy": "MTUM",
        "tickers": [
            "APP", "CRWD", "PANW", "CEG", "VST",
            "AXON", "DECK", "ANET", "NOW", "UBER",
            "SNAP", "ROKU", "TTD", "HOOD", "SOUN",
            "UPST", "SOFI", "TOST", "GLBE", "GENI",
        ],
    },

    "defense": {
        "name": "Defense & Aerospace",
        "etf_proxy": "ITA",
        "tickers": [
            "LMT", "RTX", "NOC", "GD", "BA",
            "LHX", "HII", "KTOS",
        ],
    },

    "biotech": {
        "name": "Biotech / Genomics",
        "etf_proxy": "XBI",
        "tickers": [
            "MRNA", "REGN", "VRTX", "AMGN", "GILD",
            "CRSP", "NTLA", "BEAM", "EDIT", "VKTX",
            "TEM", "PRME",
        ],
    },

    "clean_energy": {
        "name": "Clean Energy / EV",
        "etf_proxy": "ICLN",
        "tickers": [
            "TSLA", "RIVN", "LCID", "FSLR", "ENPH",
            "SEDG", "RUN", "PLUG", "BE", "QS",
            "MP", "MBLY", "OUST",
        ],
    },

    "fintech": {
        "name": "Fintech / Payments",
        "etf_proxy": "FINX",
        "tickers": [
            "SOFI", "UPST", "HOOD", "TOST", "PGY",
            "GLBE", "MELI",
        ],
    },

    "power_infra": {
        "name": "Power / Energy Infrastructure",
        "etf_proxy": "XLU",
        "tickers": [
            "CEG", "VST", "GEV", "TLN", "LNG",
            "NVT", "VRT", "CLS",
        ],
    },

    "global_tech": {
        "name": "Global / Emerging Tech",
        "etf_proxy": "EEM",
        "tickers": [
            "SE", "MELI", "JMIA", "GCT",
        ],
    },

    "india": {
        "name": "India",
        "etf_proxy": "INDA",
        "tickers": [
            "AMBER.NS",
            "ARE&M.NS",
            "AXISBANK.NS",
            "BDL.NS",
            "BEL.NS",
            "BHARATFORG.NS",
            "BORORENEW.NS",
            "COCHINSHIP.NS",
            "CONTROLPR.NS",
            "CRAFTSMAN.NS",
            "CYIENTDLM.NS",
            "DATAPATTNS.NS",
            "DBREALTY.NS",
            "DECCANCE.NS",
            "DIXON.NS",
            "EICHERMOT.NS",
            "FIEMIND.NS",
            "FSL.NS",
            "GABRIEL.NS",
            "GALAXYSURF.NS",
            "GESHIP.NS",
            "GRSE.NS",
            "HDFCBANK.NS",
            "HGINFRA.NS",
            "ICICIBANK.NS",
            "IDEAFORGE.NS",
            "INTELLECT.NS",
            "KAYNES.NS",
            "KEI.NS",
            "KOTAKBANK.NS",
            "LT.NS",
            "METROBRAND.NS",
            "MTARTECH.NS",
            "NAVINFLUOR.NS",
            "NAZARA.NS",
            "NCC.NS",
            "PAYTM.NS",
            "PCBL.NS",
            "PIIND.NS",
            "POLYCAB.NS",
            "PRESTIGE.NS",
            "RELIANCE.NS",
            "SAMHI.NS",
            "SHAILY.NS",
            "SIRCA.NS",
            "SJS.NS",
            "SKYGOLD.NS",
            "SONACOMS.NS",
            "STLTECH.NS",
            "SWSOLAR.NS",
            "SYNGENE.NS",
            "SYRMA.NS",
            "TATAPOWER.NS",
            "TRITURBINE.NS",
            "WABAG.NS",
            "WEBELSOLAR.NS",
        ],
    },
}


# ═════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════

# ── HK Ticker Utilities ───────────────────────────────────────
def is_hk_ticker(symbol: str) -> bool:
    """Check if a symbol is a Hong Kong listed instrument."""
    return symbol.upper().endswith(".HK")


def parse_hk_symbol(symbol: str) -> tuple[str, str]:
    """Parse HK ticker: '2800.HK' → ('2800', 'SEHK')."""
    code = symbol.replace(".HK", "")
    return code, "SEHK"


# ── India Ticker Utilities ─────────────────────────────────────
def is_india_ticker(symbol: str) -> bool:
    """Check if a symbol is an Indian listed instrument (.NS or .BO)."""
    s = symbol.upper()
    return s.endswith(".NS") or s.endswith(".BO")


def parse_india_symbol(symbol: str) -> tuple[str, str]:
    """
    Parse Indian ticker into (tradingsymbol, exchange).
      'RELIANCE.NS' → ('RELIANCE', 'NSE')
      '543230.BO'   → ('543230',   'BSE')
    """
    if symbol.endswith(".NS"):
        return symbol.replace(".NS", ""), "NSE"
    elif symbol.endswith(".BO"):
        return symbol.replace(".BO", ""), "BSE"
    raise ValueError(f"Not an India ticker: {symbol}")


# ── Market Detection ──────────────────────────────────────────
def detect_market(symbol: str) -> str:
    """
    Return market code for a ticker: 'US', 'HK', or 'IN'.
    Used by the orchestrator to route tickers to the right engine.
    """
    if is_hk_ticker(symbol):
        return "HK"
    if is_india_ticker(symbol):
        return "IN"
    return "US"


# ── ETF Category Lookup ───────────────────────────────────────
CATEGORY_MAP: dict[str, str] = {}
for _sym in BROAD_MARKET:
    CATEGORY_MAP[_sym] = "Broad Market"
for _sym in SECTORS:
    CATEGORY_MAP[_sym] = "Sector"
for _sym in THEMATIC_ETFS:
    CATEGORY_MAP[_sym] = "Thematic"
for _sym in INTERNATIONAL:
    CATEGORY_MAP[_sym] = "International"
for _sym in HK_ETFS:
    CATEGORY_MAP[_sym] = "HK ETF"
for _sym in FIXED_INCOME:
    CATEGORY_MAP[_sym] = "Fixed Income"
for _sym in COMMODITIES:
    CATEGORY_MAP[_sym] = "Commodities"
# ── Tier 1b / 1c categories ───────────────────────────────────
for _sym in HK_SINGLE_NAMES:
    CATEGORY_MAP.setdefault(_sym, "HK Single Name")
for _sym in INDIA_LARGE_CAPS:
    CATEGORY_MAP.setdefault(_sym, "India Large Cap")
for _sym in INDIA_BENCHMARKS:
    CATEGORY_MAP.setdefault(_sym, "India Benchmark")


def get_all_single_names() -> list[str]:
    """Return deduplicated, sorted list of every single-name ticker."""
    all_t: set[str] = set()
    for theme in SINGLE_NAMES.values():
        all_t.update(theme["tickers"])
    return sorted(all_t)


def get_theme_etf_proxies() -> list[str]:
    """Return deduplicated list of ETF proxies from single-name themes."""
    return sorted({t["etf_proxy"] for t in SINGLE_NAMES.values()})


def get_themes_for_ticker(ticker: str) -> list[str]:
    """Return list of theme keys a ticker belongs to (can be multiple)."""
    return [
        key for key, theme in SINGLE_NAMES.items()
        if ticker in theme["tickers"]
    ]


def get_us_only_etfs() -> list[str]:
    """Return ETF universe excluding HK and India listed ETFs."""
    return [s for s in ETF_UNIVERSE if not is_hk_ticker(s) and not is_india_ticker(s)]


def get_hk_only() -> list[str]:
    """Return all HK-listed tickers (Tier 1b universe + any in Tier 2 themes)."""
    hk: set[str] = set(HK_UNIVERSE)
    for theme in SINGLE_NAMES.values():
        hk.update(s for s in theme["tickers"] if is_hk_ticker(s))
    return sorted(hk)


def get_india_only() -> list[str]:
    """Return all India-listed tickers (Tier 1c universe + any in Tier 2 themes)."""
    india: set[str] = set(INDIA_UNIVERSE)
    for theme in SINGLE_NAMES.values():
        india.update(s for s in theme["tickers"] if is_india_ticker(s))
    return sorted(india)


def get_full_universe() -> list[str]:
    """
    Everything — ETFs + HK universe + India universe + single names + theme proxies.
    Used by ingest.py for full backfill.
    """
    all_syms: set[str] = set(ETF_UNIVERSE)
    all_syms.update(HK_UNIVERSE)
    all_syms.update(INDIA_UNIVERSE)
    all_syms.update(get_all_single_names())
    all_syms.update(get_theme_etf_proxies())
    return sorted(all_syms)


def get_universe_for_market(market: str) -> list[str]:
    """
    Return the scoring universe for a specific market.
    This is what MARKET_CONFIG[market]["universe"] resolves to.
    Convenience function for modules that don't import config.py.
    """
    if market == "US":
        return list(ETF_UNIVERSE)
    elif market == "HK":
        return list(HK_UNIVERSE)
    elif market == "IN":
        return list(INDIA_UNIVERSE)
    else:
        raise ValueError(f"Unknown market: {market!r}  (expected 'US', 'HK', or 'IN')")


# ═════════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═════════════════════════════════════════════════════════════════

def print_universe():
    """Pretty-print all tiers for verification."""

    # ── Tier 1: ETFs ───────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  TIER 1 : ETF UNIVERSE  ({len(ETF_UNIVERSE)} symbols)")
    print(f"{'='*65}")
    etf_groups = [
        ("Broad Market", BROAD_MARKET),
        ("Sectors", SECTORS),
        ("Thematic ETFs", THEMATIC_ETFS),
        ("International", INTERNATIONAL),
        ("HK ETFs", HK_ETFS),
        ("Fixed Income", FIXED_INCOME),
        ("Commodities", COMMODITIES),
    ]
    for name, syms in etf_groups:
        print(f"  {name:16s} ({len(syms):2d}): {', '.join(syms)}")

    # ── Tier 1b: HK Universe ──────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  TIER 1b : HK SCORING UNIVERSE  ({len(HK_UNIVERSE)} symbols)")
    print(f"{'='*65}")
    print(f"  ETFs         ({len(HK_ETFS):2d}): {', '.join(HK_ETFS)}")
    print(f"  Single Names ({len(HK_SINGLE_NAMES):2d}): {', '.join(HK_SINGLE_NAMES[:10])}...")
    print(f"  Benchmark       : 2800.HK (Tracker Fund)")

    # ── Tier 1c: India Universe ────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  TIER 1c : INDIA SCORING UNIVERSE  ({len(INDIA_UNIVERSE)} symbols)")
    print(f"{'='*65}")
    print(f"  Large Caps   ({len(INDIA_LARGE_CAPS):2d}): {', '.join(INDIA_LARGE_CAPS[:8])}...")
    print(f"  Benchmark       : NIFTYBEES.NS (Nifty BeES)")

    # ── Tier 2: Single Names ───────────────────────────────────
    singles = get_all_single_names()
    print(f"\n{'='*65}")
    print(f"  TIER 2 : SINGLE NAMES  ({len(singles)} unique across "
          f"{len(SINGLE_NAMES)} themes)")
    print(f"{'='*65}")
    for key, theme in SINGLE_NAMES.items():
        print(f"  {theme['name']:30s} ({len(theme['tickers']):2d})"
              f"  proxy: {theme['etf_proxy']:5s}"
              f"  | {', '.join(theme['tickers'][:8])}"
              f"{'...' if len(theme['tickers']) > 8 else ''}")

    # ── HK Tickers (all sources) ──────────────────────────────
    hk_all = get_hk_only()
    print(f"\n{'='*65}")
    print(f"  ALL HK TICKERS  ({len(hk_all)} symbols — need SEHK/HKD)")
    print(f"{'='*65}")
    for i in range(0, len(hk_all), 8):
        print(f"  {', '.join(hk_all[i:i+8])}")

    # ── India Tickers (all sources) ───────────────────────────
    india_all = get_india_only()
    print(f"\n{'='*65}")
    print(f"  ALL INDIA TICKERS  ({len(india_all)} symbols — need NSE/BSE)")
    print(f"{'='*65}")
    for i in range(0, len(india_all), 6):
        print(f"  {', '.join(india_all[i:i+6])}")

    # ── Combined ───────────────────────────────────────────────
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


if __name__ == "__main__":
    print_universe()

#########################    
"""
common/expiry.py
Monthly option expiry date utilities."""

from datetime import date, timedelta
from typing import List, Tuple


def third_friday(year: int, month: int) -> date:
    """3rd Friday of month (US / HK monthly expiry)."""
    first = date(year, month, 1)
    first_fri = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_fri + timedelta(days=14)


def last_thursday(year: int, month: int) -> date:
    """Last Thursday of month (India NSE monthly expiry)."""
    nxt = date(year + (month // 12), (month % 12) + 1, 1)
    last_day = nxt - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 3) % 7)


def next_monthly_expiries(
    ref_date: date | None = None,
    market: str = "us",
    n: int = 2,
) -> List[date]:
    """Return next *n* monthly expiry dates after ref_date."""
    if ref_date is None:
        ref_date = date.today()

    calc = last_thursday if market == "india" else third_friday
    expiries: List[date] = []
    y, m = ref_date.year, ref_date.month

    for _ in range(n + 4):          # generous lookahead
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
) -> List[Tuple[date, str]]:
    """
    Match calculated target expiry dates to the closest available
    expiry strings from yfinance.  Rejects matches > 7 days away.
    """
    avail_dates = sorted(date.fromisoformat(s) for s in available)
    matched: List[Tuple[date, str]] = []

    for target in targets:
        best = min(avail_dates, key=lambda d: abs((d - target).days))
        if abs((best - target).days) <= 7:
            matched.append((best, best.isoformat()))

    return matched

#########################
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
# ETF_UNIVERSE is the core rotation engine's scoreable set.
# Single names and India/.HK tickers are additive — pass them
# explicitly when data sources are configured for those markets.
UNIVERSE = list(ETF_UNIVERSE)

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

    # ── Exit thresholds ──────────────────────────────────────
    "exit_score_below":       0.35,
    "exit_percentile_below":  0.25,
    "exit_score_max":         0.35,

    # ── RS regime gate ───────────────────────────────────────
    "allowed_rs_regimes":     ["leading", "improving"],
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
PORTFOLIO_PARAMS = {
    "total_capital":         100_000,
    "max_positions":         10,
    "min_positions":          3,
    "max_sector_pct":        0.35,
    "max_single_pct":        0.15,
    "min_single_pct":        0.03,
    "target_invested_pct":   0.90,
    "rebalance_threshold":   0.05,
    "incumbent_bonus":       0.05,
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

    # ── Consumer / Telecom ─────────────────────────────────
    "0941.HK": "Telecom",     # China Mobile
    "0762.HK": "Telecom",     # China Unicom
    "9633.HK": "Consumer",    # Nongfu Spring
    "2020.HK": "Consumer",    # Anta Sports

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

#########################
"""
COMPUTE :

"""
"""
compute/breadth.py
------------------
Market breadth analytics computed from an internal universe of
stocks.

Rather than relying on exchange-level advance/decline data (which
requires a paid feed), this module derives breadth from whatever
universe the caller provides.  With 15–50 stocks the readings are
noisy but still useful as a regime overlay; with 100+ they
converge toward traditional breadth measures.

Indicators
──────────
  advance_decline       daily advancing − declining count
  ad_line               cumulative A-D line
  adv_ratio             advancing / total (0‒1)
  mcclellan_osc         19/39 EMA of daily A-D (breadth momentum)
  mcclellan_sum         cumulative McClellan Oscillator
  pct_above_50          fraction of universe above 50-day SMA
  pct_above_200         fraction of universe above 200-day SMA
  new_highs             count making rolling 252-day high
  new_lows              count making rolling 252-day low
  hi_lo_diff            new_highs − new_lows
  hi_lo_ratio           new_highs / (new_highs + new_lows)
  thrust_ema            10-day EMA of adv_ratio
  breadth_thrust        1 when thrust_ema crosses above 61.5 %
  breadth_washout       1 when thrust_ema crosses below 25 %
  breadth_regime        strong / neutral / weak
  breadth_score         0–1 composite

Pipeline
────────
  {ticker: DataFrame}  universe of OHLCV DataFrames
       ↓
  align_universe()       — date-align closes into a panel
       ↓
  compute_advance_decline()
  compute_mcclellan()
  compute_pct_above_ma()
  compute_new_highs_lows()
  compute_breadth_thrust()
  compute_breadth_score()
  classify_breadth_regime()
       ↓
  compute_all_breadth()  — master orchestrator → single DataFrame
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import BREADTH_PARAMS


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _bp(key: str):
    """Fetch from BREADTH_PARAMS."""
    return BREADTH_PARAMS[key]


# ═══════════════════════════════════════════════════════════════
#  UNIVERSE ALIGNMENT
# ═══════════════════════════════════════════════════════════════

def align_universe(
    universe: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Build date-aligned panels from a universe of DataFrames.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} with at least a ``close`` column
        and a DatetimeIndex.

    Returns
    -------
    closes  : pd.DataFrame — columns = tickers, rows = dates
    volumes : pd.DataFrame — same shape, daily volume
    n       : int          — number of tickers in the panel
    """
    close_frames: dict[str, pd.Series]  = {}
    volume_frames: dict[str, pd.Series] = {}

    for ticker, df in universe.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        close_frames[ticker]  = df["close"].copy()
        volume_frames[ticker] = (
            df["volume"].copy() if "volume" in df.columns
            else pd.Series(np.nan, index=df.index)
        )

    if not close_frames:
        return pd.DataFrame(), pd.DataFrame(), 0

    closes  = pd.DataFrame(close_frames).sort_index()
    volumes = pd.DataFrame(volume_frames).sort_index()

    return closes, volumes, len(close_frames)


# ═══════════════════════════════════════════════════════════════
#  ADVANCE / DECLINE
# ═══════════════════════════════════════════════════════════════

def compute_advance_decline(
    closes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Daily advancing / declining / unchanged counts and the
    cumulative A-D line.
    """
    if closes.empty:
        return pd.DataFrame()

    daily_ret = closes.pct_change()

    advancing  = (daily_ret > 0).sum(axis=1)
    declining  = (daily_ret < 0).sum(axis=1)
    unchanged  = (daily_ret == 0).sum(axis=1)
    traded     = advancing + declining + unchanged

    result = pd.DataFrame(index=closes.index)
    result["advancing"]        = advancing.astype(int)
    result["declining"]        = declining.astype(int)
    result["unchanged"]        = unchanged.astype(int)
    result["total_traded"]     = traded.astype(int)
    result["advance_decline"]  = (advancing - declining).astype(int)
    result["ad_line"]          = result["advance_decline"].cumsum()

    # Advance ratio (0–1)
    result["adv_ratio"] = np.where(
        traded > 0,
        advancing / traded,
        np.nan,
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  McCLELLAN OSCILLATOR  +  SUMMATION INDEX
# ═══════════════════════════════════════════════════════════════

def compute_mcclellan(
    ad_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    McClellan Oscillator = EMA(fast) − EMA(slow) of daily A-D.
    McClellan Summation Index = cumulative sum of the oscillator.

    Positive oscillator → breadth momentum expanding.
    Rising summation    → sustained breadth improvement.
    """
    if ad_data.empty or "advance_decline" not in ad_data.columns:
        return ad_data

    ad_series = ad_data["advance_decline"].astype(float)

    fast = _bp("mcclellan_fast")
    slow = _bp("mcclellan_slow")

    ema_fast = ad_series.ewm(span=fast, adjust=False).mean()
    ema_slow = ad_series.ewm(span=slow, adjust=False).mean()

    result = ad_data.copy()
    result["mcclellan_osc"] = ema_fast - ema_slow
    result["mcclellan_sum"] = result["mcclellan_osc"].cumsum()

    # Normalised oscillator: divide by total traded to make it
    # comparable across different universe sizes
    total = result["total_traded"].replace(0, np.nan)
    result["mcclellan_osc_pct"] = result["mcclellan_osc"] / total

    return result


# ═══════════════════════════════════════════════════════════════
#  PERCENT ABOVE MOVING AVERAGES
# ═══════════════════════════════════════════════════════════════

def compute_pct_above_ma(
    closes: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each day, fraction of stocks whose close is above their
    own 50-day and 200-day SMA.

    Returns a DataFrame indexed by date with columns:
        pct_above_50, pct_above_200
    """
    if closes.empty:
        return pd.DataFrame()

    ma_short = _bp("ma_short")
    ma_long  = _bp("ma_long")

    sma50  = closes.rolling(ma_short, min_periods=ma_short).mean()
    sma200 = closes.rolling(ma_long, min_periods=ma_long).mean()

    above_50  = (closes > sma50).sum(axis=1)
    above_200 = (closes > sma200).sum(axis=1)

    count_50  = sma50.notna().sum(axis=1).replace(0, np.nan)
    count_200 = sma200.notna().sum(axis=1).replace(0, np.nan)

    result = pd.DataFrame(index=closes.index)
    result["pct_above_50"]  = above_50 / count_50
    result["pct_above_200"] = above_200 / count_200

    return result


# ═══════════════════════════════════════════════════════════════
#  NEW HIGHS / NEW LOWS
# ═══════════════════════════════════════════════════════════════

def compute_new_highs_lows(
    closes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Daily count of stocks making a rolling 252-day high or low.

    hi_lo_ratio = new_highs / (new_highs + new_lows), smoothed
    with a 10-day SMA to reduce noise.
    """
    if closes.empty:
        return pd.DataFrame()

    window = _bp("high_low_window")

    roll_high = closes.rolling(window, min_periods=window).max()
    roll_low  = closes.rolling(window, min_periods=window).min()

    is_new_high = (closes >= roll_high) & roll_high.notna()
    is_new_low  = (closes <= roll_low) & roll_low.notna()

    result = pd.DataFrame(index=closes.index)
    result["new_highs"]  = is_new_high.sum(axis=1).astype(int)
    result["new_lows"]   = is_new_low.sum(axis=1).astype(int)
    result["hi_lo_diff"] = result["new_highs"] - result["new_lows"]

    total_hl = result["new_highs"] + result["new_lows"]
    result["hi_lo_ratio"] = np.where(
        total_hl > 0,
        result["new_highs"] / total_hl,
        0.5,  # neutral when no new highs or lows
    )
    result["hi_lo_ratio_sma"] = (
        result["hi_lo_ratio"].rolling(10, min_periods=1).mean()
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  BREADTH THRUST
# ═══════════════════════════════════════════════════════════════

def compute_breadth_thrust(
    breadth: pd.DataFrame,
) -> pd.DataFrame:
    """
    Breadth thrust detection.

    A breadth thrust occurs when the 10-day EMA of the advance
    ratio surges above a threshold (historically 61.5 %).  This
    is one of the most reliable bullish signals in market
    history — when breadth expands that rapidly, the odds of a
    sustained rally are very high.

    A breadth washout occurs when the same EMA collapses below
    25 %, marking capitulatory selling — often a setup for a
    subsequent thrust.
    """
    if breadth.empty or "adv_ratio" not in breadth.columns:
        return breadth

    result = breadth.copy()
    window = _bp("thrust_window")

    result["thrust_ema"] = (
        result["adv_ratio"]
        .ewm(span=window, adjust=False)
        .mean()
    )

    up_thresh = _bp("thrust_up_threshold")
    dn_thresh = _bp("thrust_dn_threshold")

    ema      = result["thrust_ema"]
    prev_ema = ema.shift(1)

    result["breadth_thrust"] = (
        ((ema >= up_thresh) & (prev_ema < up_thresh)).astype(int)
    )
    result["breadth_washout"] = (
        ((ema <= dn_thresh) & (prev_ema > dn_thresh)).astype(int)
    )

    # Rolling flag: 1 for 20 days after a thrust fires
    result["thrust_active"] = (
        result["breadth_thrust"]
        .rolling(20, min_periods=1)
        .max()
        .fillna(0)
        .astype(int)
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  BREADTH SCORE  (composite 0–1)
# ═══════════════════════════════════════════════════════════════

def compute_breadth_score(breadth: pd.DataFrame) -> pd.DataFrame:
    """
    Composite breadth score (0–1) combining:

      0.30 × adv_ratio           (advancing breadth)
      0.25 × pct_above_50        (short-term health)
      0.20 × pct_above_200       (long-term health)
      0.15 × hi_lo_ratio_sma     (new-high leadership)
      0.10 × mcclellan_norm      (breadth momentum)

    Each component is already on [0, 1] (or clipped there).
    """
    if breadth.empty:
        return breadth

    result = breadth.copy()

    # Normalise McClellan oscillator pct to [0, 1] via sigmoid
    mc = result.get("mcclellan_osc_pct", pd.Series(0.5, index=result.index))
    mc_norm = 1.0 / (1.0 + np.exp(-10.0 * mc.fillna(0)))

    components = {
        "adv_ratio":       0.30,
        "pct_above_50":    0.25,
        "pct_above_200":   0.20,
        "hi_lo_ratio_sma": 0.15,
    }

    score = pd.Series(0.0, index=result.index)
    for col, weight in components.items():
        vals = result[col].fillna(0.5) if col in result.columns else 0.5
        score += weight * vals

    score += 0.10 * mc_norm

    result["breadth_score"] = score.clip(0, 1)

    return result


# ═══════════════════════════════════════════════════════════════
#  REGIME CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

def classify_breadth_regime(
    breadth: pd.DataFrame,
) -> pd.DataFrame:
    """
    Classify each day's breadth as strong / neutral / weak.

    Uses a smoothed version of ``breadth_score`` (5-day SMA) so
    the regime doesn't flip on single noisy days.
    """
    if breadth.empty or "breadth_score" not in breadth.columns:
        return breadth

    result = breadth.copy()

    smoothed = (
        result["breadth_score"]
        .rolling(5, min_periods=1)
        .mean()
    )

    strong = _bp("regime_strong_pct")
    weak   = _bp("regime_weak_pct")

    conditions = [
        smoothed >= strong,
        smoothed <= weak,
    ]
    choices = ["strong", "weak"]

    result["breadth_regime"] = np.select(
        conditions, choices, default="neutral"
    )
    result["breadth_score_smooth"] = smoothed

    return result


# ═══════════════════════════════════════════════════════════════
#  UP-VOLUME RATIO
# ═══════════════════════════════════════════════════════════════

def compute_up_volume_ratio(
    closes: pd.DataFrame,
    volumes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Fraction of total volume that belongs to advancing stocks.

    up_volume_ratio > 0.9 on a single day is a 90 % up-volume
    day — historically one of the strongest breadth signals.
    """
    if closes.empty or volumes.empty:
        return pd.DataFrame()

    daily_ret = closes.pct_change()
    up_mask   = (daily_ret > 0)

    up_vol    = (volumes * up_mask.astype(float)).sum(axis=1)
    total_vol = volumes.sum(axis=1).replace(0, np.nan)

    result = pd.DataFrame(index=closes.index)
    result["up_volume"]       = up_vol
    result["total_volume"]    = total_vol.fillna(0)
    result["up_volume_ratio"] = (up_vol / total_vol).clip(0, 1)
    result["up_vol_sma10"]    = (
        result["up_volume_ratio"]
        .rolling(10, min_periods=1)
        .mean()
    )

    # Flag 90 % up-volume days
    result["ninety_pct_up_day"] = (
        (result["up_volume_ratio"] >= 0.90).astype(int)
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_breadth(
    universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Full breadth pipeline.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} with at least ``close`` and
        optionally ``volume`` columns.

    Returns
    -------
    pd.DataFrame
        One row per trading day with all breadth indicators,
        breadth_score, and breadth_regime.
    """
    closes, volumes, n = align_universe(universe)

    if n < _bp("min_stocks"):
        return pd.DataFrame()

    # ── Advance / Decline ─────────────────────────────────────
    breadth = compute_advance_decline(closes)

    # ── McClellan ─────────────────────────────────────────────
    breadth = compute_mcclellan(breadth)

    # ── Percent above MAs ─────────────────────────────────────
    pct_ma = compute_pct_above_ma(closes)
    breadth = breadth.join(pct_ma, how="left")

    # ── New highs / lows ──────────────────────────────────────
    hi_lo = compute_new_highs_lows(closes)
    breadth = breadth.join(hi_lo, how="left")

    # ── Up-volume ratio ───────────────────────────────────────
    up_vol = compute_up_volume_ratio(closes, volumes)
    if not up_vol.empty:
        breadth = breadth.join(up_vol, how="left")

    # ── Thrust ────────────────────────────────────────────────
    breadth = compute_breadth_thrust(breadth)

    # ── Composite score ───────────────────────────────────────
    breadth = compute_breadth_score(breadth)

    # ── Regime ────────────────────────────────────────────────
    breadth = classify_breadth_regime(breadth)

    # ── Metadata ──────────────────────────────────────────────
    breadth["breadth_n_stocks"] = n

    return breadth


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

def breadth_report(breadth: pd.DataFrame, lookback: int = 5) -> str:
    """
    Format a human-readable breadth summary for the latest
    *lookback* days.
    """
    if breadth.empty:
        return "No breadth data available."

    ln: list[str] = []
    div = "=" * 60
    sub = "-" * 60

    tail = breadth.tail(lookback)
    last = breadth.iloc[-1]

    ln.append(div)
    ln.append("MARKET BREADTH REPORT")
    ln.append(div)
    ln.append(
        f"  Date:              {breadth.index[-1].strftime('%Y-%m-%d')}"
    )
    ln.append(
        f"  Universe size:     "
        f"{int(last.get('breadth_n_stocks', 0))} stocks"
    )
    ln.append(
        f"  Breadth regime:    {last.get('breadth_regime', '?')}"
    )
    ln.append(
        f"  Breadth score:     "
        f"{last.get('breadth_score', 0):.3f}  "
        f"(smooth: {last.get('breadth_score_smooth', 0):.3f})"
    )

    ln.append("")
    ln.append(sub)
    ln.append("CURRENT READINGS")
    ln.append(sub)
    ln.append(
        f"  Advancing:         "
        f"{int(last.get('advancing', 0))} / "
        f"{int(last.get('total_traded', 0))}  "
        f"({last.get('adv_ratio', 0):.1%})"
    )
    ln.append(
        f"  A-D line:          {int(last.get('ad_line', 0))}"
    )
    ln.append(
        f"  McClellan Osc:     {last.get('mcclellan_osc', 0):.2f}"
    )
    ln.append(
        f"  McClellan Sum:     {last.get('mcclellan_sum', 0):.1f}"
    )
    ln.append(
        f"  % above 50d SMA:   "
        f"{last.get('pct_above_50', 0):.1%}"
    )
    ln.append(
        f"  % above 200d SMA:  "
        f"{last.get('pct_above_200', 0):.1%}"
    )
    ln.append(
        f"  New highs:         {int(last.get('new_highs', 0))}"
    )
    ln.append(
        f"  New lows:          {int(last.get('new_lows', 0))}"
    )
    ln.append(
        f"  Hi-Lo ratio:       "
        f"{last.get('hi_lo_ratio_sma', 0):.3f}"
    )

    if "up_volume_ratio" in last.index:
        ln.append(
            f"  Up-volume ratio:   "
            f"{last.get('up_volume_ratio', 0):.1%}"
        )

    if last.get("thrust_active", 0) == 1:
        ln.append(f"  ⚡ Breadth thrust ACTIVE")
    if last.get("breadth_washout", 0) == 1:
        ln.append(f"  ⚠ Breadth washout detected")

    # ── Recent trend ──────────────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append(f"LAST {lookback} DAYS")
    ln.append(sub)
    header = (
        f"  {'Date':<12} {'Adv':>4} {'Dec':>4} "
        f"{'Ratio':>6} {'McCl':>6} {'%>50d':>6} "
        f"{'Score':>6} {'Regime':<8}"
    )
    ln.append(header)
    ln.append(
        f"  {'──────────':<12} {'───':>4} {'───':>4} "
        f"{'─────':>6} {'─────':>6} {'─────':>6} "
        f"{'─────':>6} {'──────':<8}"
    )
    for dt, row in tail.iterrows():
        date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)
        ln.append(
            f"  {date_str:<12} "
            f"{int(row.get('advancing', 0)):>4} "
            f"{int(row.get('declining', 0)):>4} "
            f"{row.get('adv_ratio', 0):>5.1%} "
            f"{row.get('mcclellan_osc', 0):>6.1f} "
            f"{row.get('pct_above_50', 0):>5.1%} "
            f"{row.get('breadth_score', 0):>6.3f} "
            f"{str(row.get('breadth_regime', '?')):<8}"
        )

    return "\n".join(ln)

# ═══════════════════════════════════════════════════════════════
#  SCORING PIPELINE BRIDGE
# ═══════════════════════════════════════════════════════════════

def breadth_to_pillar_scores(
    breadth: pd.DataFrame,
    symbols: list[str],
    scale: float = 100.0,
) -> pd.DataFrame:
    """
    Convert universe-level breadth_score into a per-symbol
    DataFrame shaped for the composite scoring pipeline.

    Option A (broadcast): every symbol receives the same
    daily breadth score.  This is appropriate when breadth
    is used as a market-regime overlay.

    Parameters
    ----------
    breadth : pd.DataFrame
        Output of ``compute_all_breadth()``, must contain
        ``breadth_score`` column (values 0–1).
    symbols : list[str]
        Column names for the output (the ETF/ticker universe).
    scale : float
        Multiply by this to match pillar score range.
        Default 100 converts 0–1 → 0–100.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows × symbol columns, values 0–100.
    """
    if breadth.empty or "breadth_score" not in breadth.columns:
        return pd.DataFrame(index=breadth.index, columns=symbols, dtype=float)

    score = breadth["breadth_score"] * scale

    return pd.DataFrame(
        {symbol: score for symbol in symbols},
        index=breadth.index,
    )


def breadth_to_pillar_scores_grouped(
    group_breadth: dict[str, pd.DataFrame],
    group_map: dict[str, list[str]],
    fallback_breadth: pd.DataFrame | None = None,
    scale: float = 100.0,
) -> pd.DataFrame:
    """
    Option B: per-group breadth scores mapped to individual symbols.

    Parameters
    ----------
    group_breadth : dict
        {group_name: breadth_df} — output of ``compute_all_breadth()``
        run on each group's constituent universe.
    group_map : dict
        {group_name: [symbol, ...]} — maps groups to ETF symbols.
    fallback_breadth : pd.DataFrame or None
        Universe-level breadth for symbols not in any group.
    scale : float
        Score multiplier (default 100).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows × symbol columns, values 0–100.
    """
    all_symbols = [s for syms in group_map.values() for s in syms]

    # Collect all date indices
    all_dates = set()
    for bdf in group_breadth.values():
        if not bdf.empty:
            all_dates.update(bdf.index)
    if fallback_breadth is not None and not fallback_breadth.empty:
        all_dates.update(fallback_breadth.index)

    if not all_dates:
        return pd.DataFrame(columns=all_symbols, dtype=float)

    idx = pd.DatetimeIndex(sorted(all_dates))
    result = pd.DataFrame(index=idx, columns=all_symbols, dtype=float)

    for group_name, symbols in group_map.items():
        bdf = group_breadth.get(group_name)
        if bdf is None or bdf.empty or "breadth_score" not in bdf.columns:
            continue
        score = bdf["breadth_score"].reindex(idx) * scale
        for sym in symbols:
            if sym in result.columns:
                result[sym] = score

    # Fill unmapped symbols with fallback
    if fallback_breadth is not None and "breadth_score" in fallback_breadth.columns:
        fb_score = fallback_breadth["breadth_score"].reindex(idx) * scale
        for sym in result.columns:
            if result[sym].isna().all():
                result[sym] = fb_score

    return result

#################################    

"""
compute/indicators.py
---------------------
Pure functions that compute technical indicators on OHLCV DataFrames.

No database knowledge, no scoring opinions — just math.

Every function:
    • takes a DataFrame with columns: open, high, low, close, volume
    • returns the same DataFrame with new indicator columns appended
    • pulls default parameters from common.config.INDICATOR_PARAMS

Master entry point:
    df = compute_all_indicators(df)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import INDICATOR_PARAMS

# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

_REQUIRED_COLS = {"open", "high", "low", "close", "volume"}


def _validate_ohlcv(df: pd.DataFrame) -> None:
    """Raise if the DataFrame is missing any required OHLCV columns."""
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    if df.empty:
        raise ValueError("DataFrame is empty")


def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window, min_periods=window).mean()


def _wilder_smooth(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing method (used by RSI, ADX, ATR)."""
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """
    Slope of a least-squares linear fit over a rolling window.
    Positive slope = series trending upward.
    """
    def _slope(arr):
        if len(arr) < window or np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]

    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True
    )


# ═══════════════════════════════════════════════════════════════
#  1. RETURNS
# ═══════════════════════════════════════════════════════════════

def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """N-day percentage returns for each window in config."""
    for w in INDICATOR_PARAMS["return_windows"]:
        df[f"ret_{w}d"] = df["close"].pct_change(w)
    return df


# ═══════════════════════════════════════════════════════════════
#  2. RSI
# ═══════════════════════════════════════════════════════════════

def add_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Relative Strength Index (Wilder's smoothing).
    Output: rsi_{period}  (0–100 scale)
    """
    period = INDICATOR_PARAMS["rsi_period"]
    delta = df["close"].diff()

    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = _wilder_smooth(gain, period)
    avg_loss = _wilder_smooth(loss, period)

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    df[f"rsi_{period}"] = 100.0 - (100.0 / (1.0 + rs))
    return df


# ═══════════════════════════════════════════════════════════════
#  3. MACD
# ═══════════════════════════════════════════════════════════════

def add_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    MACD line, signal line, histogram.
    Outputs: macd_line, macd_signal, macd_hist
    """
    fast   = INDICATOR_PARAMS["macd_fast"]
    slow   = INDICATOR_PARAMS["macd_slow"]
    signal = INDICATOR_PARAMS["macd_signal"]

    ema_fast = _ema(df["close"], fast)
    ema_slow = _ema(df["close"], slow)

    df["macd_line"]   = ema_fast - ema_slow
    df["macd_signal"] = _ema(df["macd_line"], signal)
    df["macd_hist"]   = df["macd_line"] - df["macd_signal"]
    return df


# ═══════════════════════════════════════════════════════════════
#  4. ADX  (Average Directional Index)
# ═══════════════════════════════════════════════════════════════

def add_adx(df: pd.DataFrame) -> pd.DataFrame:
    """
    ADX with +DI / -DI.
    Outputs: adx_{period}, plus_di, minus_di
    """
    period = INDICATOR_PARAMS["adx_period"]
    high   = df["high"]
    low    = df["low"]
    close  = df["close"]

    # ── True Range ──────────────────────────────────────────
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = _wilder_smooth(tr, period)

    # ── Directional Movement ────────────────────────────────
    up_move   = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=df.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=df.index,
    )

    plus_di  = 100.0 * _wilder_smooth(plus_dm,  period) / atr.replace(0, np.nan)
    minus_di = 100.0 * _wilder_smooth(minus_dm, period) / atr.replace(0, np.nan)

    di_sum  = plus_di + minus_di
    di_diff = (plus_di - minus_di).abs()
    dx      = 100.0 * di_diff / di_sum.replace(0, np.nan)

    df[f"adx_{period}"] = _wilder_smooth(dx, period)
    df["plus_di"]       = plus_di
    df["minus_di"]      = minus_di
    return df


# ═══════════════════════════════════════════════════════════════
#  5. MOVING AVERAGES
# ═══════════════════════════════════════════════════════════════

def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    EMA and SMA lines, plus price-to-MA distance (%).
    Outputs: ema_{p}, sma_{s}, sma_{l},
                close_vs_ema_{p}_pct, close_vs_sma_{l}_pct
    """
    ema_p = INDICATOR_PARAMS["ema_period"]
    sma_s = INDICATOR_PARAMS["sma_short"]
    sma_l = INDICATOR_PARAMS["sma_long"]

    df[f"ema_{ema_p}"] = _ema(df["close"], ema_p)
    df[f"sma_{sma_s}"] = _sma(df["close"], sma_s)
    df[f"sma_{sma_l}"] = _sma(df["close"], sma_l)

    # Distance from MA (positive = price above MA)
    df[f"close_vs_ema_{ema_p}_pct"] = (
        (df["close"] / df[f"ema_{ema_p}"] - 1.0) * 100.0
    )
    df[f"close_vs_sma_{sma_l}_pct"] = (
        (df["close"] / df[f"sma_{sma_l}"] - 1.0) * 100.0
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  6. ATR  (Average True Range)
# ═══════════════════════════════════════════════════════════════

def add_atr(df: pd.DataFrame) -> pd.DataFrame:
    """
    ATR in absolute and percentage-of-price terms.
    Percentage ATR makes cross-asset comparison possible.
    Outputs: atr_{period}, atr_{period}_pct
    """
    period     = INDICATOR_PARAMS["atr_period"]
    prev_close = df["close"].shift(1)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)

    df[f"atr_{period}"]     = _wilder_smooth(tr, period)
    df[f"atr_{period}_pct"] = df[f"atr_{period}"] / df["close"] * 100.0
    return df


# ═══════════════════════════════════════════════════════════════
#  7. REALIZED VOLATILITY
# ═══════════════════════════════════════════════════════════════

def add_realized_vol(df: pd.DataFrame) -> pd.DataFrame:
    """
    Annualised realized volatility from log returns.
    Also computes 5-day change to detect vol expansion / contraction.
    Outputs: realized_vol_{w}d, realized_vol_{w}d_chg5
    """
    window  = INDICATOR_PARAMS["realized_vol_window"]
    log_ret = np.log(df["close"] / df["close"].shift(1))

    col = f"realized_vol_{window}d"
    df[col]           = log_ret.rolling(window).std() * np.sqrt(252) * 100.0
    df[f"{col}_chg5"] = df[col].diff(5)
    return df


# ═══════════════════════════════════════════════════════════════
#  8. OBV  (On-Balance Volume)
# ═══════════════════════════════════════════════════════════════

def add_obv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cumulative OBV plus its 10-day slope.
    Rising OBV slope on an up-trend = accumulation confirmation.
    Outputs: obv, obv_slope_10d
    """
    if not INDICATOR_PARAMS.get("obv", True):
        return df

    sign = np.sign(df["close"].diff()).fillna(0.0)
    df["obv"] = (sign * df["volume"]).cumsum()
    df["obv_slope_10d"] = _rolling_slope(df["obv"], 10)
    return df


# ═══════════════════════════════════════════════════════════════
#  9. ACCUMULATION / DISTRIBUTION LINE
# ═══════════════════════════════════════════════════════════════

def add_ad_line(df: pd.DataFrame) -> pd.DataFrame:
    """
    A/D line using the Close Location Value (CLV) multiplier.
    CLV = [(close-low) - (high-close)] / (high-low)
    Outputs: ad_line, ad_line_slope_10d
    """
    if not INDICATOR_PARAMS.get("ad_line", True):
        return df

    hl_range = df["high"] - df["low"]
    clv = (
        (df["close"] - df["low"]) - (df["high"] - df["close"])
    ) / hl_range.replace(0.0, np.nan)
    clv = clv.fillna(0.0)

    df["ad_line"]            = (clv * df["volume"]).cumsum()
    df["ad_line_slope_10d"]  = _rolling_slope(df["ad_line"], 10)
    return df


# ═══════════════════════════════════════════════════════════════
# 10. VOLUME METRICS
# ═══════════════════════════════════════════════════════════════

def add_volume_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average volume, relative volume, dollar volume.
    Relative volume > 1.5 often indicates institutional activity.
    Outputs: volume_avg_{w}, relative_volume,
                dollar_volume, dollar_volume_avg_{w}
    """
    w = INDICATOR_PARAMS["volume_avg_window"]

    df[f"volume_avg_{w}"]        = _sma(df["volume"], w)
    df["relative_volume"]        = (
        df["volume"] / df[f"volume_avg_{w}"].replace(0.0, np.nan)
    )
    df["dollar_volume"]          = df["close"] * df["volume"]
    df[f"dollar_volume_avg_{w}"] = _sma(df["dollar_volume"], w)
    return df


# ═══════════════════════════════════════════════════════════════
# 11. AMIHUD ILLIQUIDITY
# ═══════════════════════════════════════════════════════════════

def add_amihud(df: pd.DataFrame) -> pd.DataFrame:
    """
    Amihud (2002) illiquidity ratio: |return| / dollar_volume.
    Higher = more illiquid.  Scaled by 1e6 for readability.
    Requires dollar_volume column (call add_volume_metrics first).
    Outputs: amihud_{w}d
    """
    w = INDICATOR_PARAMS["amihud_window"]

    if "dollar_volume" not in df.columns:
        df["dollar_volume"] = df["close"] * df["volume"]

    daily_illiq = (
        df["close"].pct_change().abs()
        / df["dollar_volume"].replace(0.0, np.nan)
    )
    df[f"amihud_{w}d"] = daily_illiq.rolling(w).mean() * 1e6
    return df


# ═══════════════════════════════════════════════════════════════
# 12. VWAP DISTANCE  (daily-bar proxy)
# ═══════════════════════════════════════════════════════════════

def add_vwap_distance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling VWAP proxy for daily data.

    True VWAP needs intraday bars.  With daily data we approximate
    using a N-day volume-weighted average of the typical price
    (H+L+C)/3.  Distance > 0 means close is above VWAP — a sign
    of sustained buying pressure (accumulation).

    Outputs: vwap_{w}d, vwap_{w}d_dist_pct
    """
    w       = INDICATOR_PARAMS["volume_avg_window"]     # reuse 20D
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    tp_vol  = typical * df["volume"]

    rolling_tp_vol = tp_vol.rolling(w).sum()
    rolling_vol    = df["volume"].rolling(w).sum()

    vwap_col = f"vwap_{w}d"
    df[vwap_col]              = rolling_tp_vol / rolling_vol.replace(0.0, np.nan)
    df[f"{vwap_col}_dist_pct"] = (df["close"] / df[vwap_col] - 1.0) * 100.0
    return df


# ═══════════════════════════════════════════════════════════════
# MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute every technical indicator on an OHLCV DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: open, high, low, close, volume.
        Must be sorted by date ascending.

    Returns
    -------
    pd.DataFrame
        Copy of the input with ~30 indicator columns appended.
        Early rows will contain NaN where lookback is insufficient.
    """
    _validate_ohlcv(df)
    df = df.copy()

    # ── Order matters: volume_metrics before amihud ─────────
    df = add_returns(df)
    df = add_rsi(df)
    df = add_macd(df)
    df = add_adx(df)
    df = add_moving_averages(df)
    df = add_atr(df)
    df = add_realized_vol(df)
    df = add_obv(df)
    df = add_ad_line(df)
    df = add_volume_metrics(df)      # creates dollar_volume
    df = add_amihud(df)              # needs dollar_volume
    df = add_vwap_distance(df)

    return df

"""
compute/relative_strength.py
-----------------------------
Relative strength vs benchmark — the core rotation signal (Pillar 1).

Pure functions.  Takes a stock OHLCV DataFrame and a benchmark OHLCV
DataFrame, returns the stock DataFrame with RS columns appended.

No database knowledge.  No scoring opinions.  Just math.

Key concepts
------------
RS ratio   : stock_close / bench_close — rising means outperforming.
RS slope   : linear regression slope of the smoothed RS ratio.
             Positive  = money rotating IN.
             Negative  = money rotating OUT.
RS z-score : standardised slope for cross-ticker comparison.
RS momentum: short-term slope minus long-term slope (acceleration).
RS regime  : categorical label — leading / weakening / lagging / improving.
             "improving" is the sweet spot: early rotation before the crowd.

Why EMA / SMA / Volume here?
-----------------------------
indicators.py smooths the stock's own price.  This module smooths the
RS *ratio* (stock ÷ benchmark) — a completely different series.  Volume
is used to confirm whether RS improvement has institutional participation
or is just low-liquidity drift.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import INDICATOR_PARAMS

# ═══════════════════════════════════════════════════════════════
#  DEFAULTS  (fallback if config keys are missing)
# ═══════════════════════════════════════════════════════════════

_DEFAULTS = {
    "rs_ema_span":               10,
    "rs_sma_span":               50,
    "rs_slope_window":           20,
    "rs_zscore_window":          60,
    "rs_momentum_short":         10,
    "rs_momentum_long":          30,
    "rs_vol_confirm_threshold": 1.3,
    "volume_avg_window":         20,
}


def _p(key: str):
    """Fetch parameter from config, fall back to module default."""
    return INDICATOR_PARAMS.get(key, _DEFAULTS[key])


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window, min_periods=window).mean()


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """Slope of least-squares linear fit over a rolling window."""
    def _slope(arr):
        if len(arr) < window or np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]

    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True
    )


# ═══════════════════════════════════════════════════════════════
#  1. RS RATIO  (raw + smoothed)
# ═══════════════════════════════════════════════════════════════

def add_rs_ratio(
    df: pd.DataFrame,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS ratio = stock close / benchmark close.
    Normalised to 1.0 at the first valid data point so the
    absolute level is interpretable (>1 = outperforming since start).

    EMA smoothing removes daily noise.
    SMA provides a longer-term trend baseline.

    Outputs: rs_raw, rs_ema, rs_sma
    """
    raw = df["close"] / bench_close

    # Normalise to 1.0 at first valid observation
    first_valid = raw.first_valid_index()
    if first_valid is not None:
        raw = raw / raw.loc[first_valid]

    df["rs_raw"] = raw
    df["rs_ema"] = _ema(raw, _p("rs_ema_span"))
    df["rs_sma"] = _sma(raw, _p("rs_sma_span"))
    return df


# ═══════════════════════════════════════════════════════════════
#  2. RS SLOPE — the rotation signal
# ═══════════════════════════════════════════════════════════════

def add_rs_slope(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling linear-regression slope of the smoothed RS ratio.

    This IS the rotation signal:
      slope > 0 → outperforming benchmark → money rotating in
      slope < 0 → underperforming → money rotating out
      slope flips neg→pos → early rotation detected

    Outputs: rs_slope
    """
    if "rs_ema" not in df.columns:
        raise ValueError("Run add_rs_ratio first — rs_ema column missing")

    df["rs_slope"] = _rolling_slope(df["rs_ema"], _p("rs_slope_window"))
    return df


# ═══════════════════════════════════════════════════════════════
#  3. RS Z-SCORE — cross-ticker comparison
# ═══════════════════════════════════════════════════════════════

def add_rs_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Z-score of RS slope over a longer lookback.

    Allows apples-to-apples comparison across tickers:
      z > +1.5  →  strong relative outperformance
      z < -1.5  →  strong relative underperformance
      z flips from -1 to +1  →  meaningful regime change

    Outputs: rs_zscore
    """
    if "rs_slope" not in df.columns:
        raise ValueError("Run add_rs_slope first — rs_slope column missing")

    w         = _p("rs_zscore_window")
    roll_mean = df["rs_slope"].rolling(w).mean()
    roll_std  = df["rs_slope"].rolling(w).std()

    df["rs_zscore"] = (
        (df["rs_slope"] - roll_mean) / roll_std.replace(0.0, np.nan)
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  4. RS MOMENTUM — acceleration of rotation
# ═══════════════════════════════════════════════════════════════

def add_rs_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Short-term RS slope minus long-term RS slope.

    Positive momentum = RS improvement is accelerating.
    This catches early rotation before the slope itself turns
    positive — like a MACD for relative strength.

    Outputs: rs_momentum
    """
    if "rs_ema" not in df.columns:
        raise ValueError("Run add_rs_ratio first — rs_ema column missing")

    slope_short = _rolling_slope(df["rs_ema"], _p("rs_momentum_short"))
    slope_long  = _rolling_slope(df["rs_ema"], _p("rs_momentum_long"))

    df["rs_momentum"] = slope_short - slope_long
    return df


# ═══════════════════════════════════════════════════════════════
#  5. VOLUME-CONFIRMED RS
# ═══════════════════════════════════════════════════════════════

def add_rs_volume_confirmation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks whether improving RS is backed by above-average volume.

    Smart-money rotation shows up as RS improvement on elevated
    volume.  Without volume confirmation, RS improvement could be
    low-liquidity drift — a trap.

    If indicators.py has already run, reuses relative_volume.
    Otherwise computes it here from raw volume.

    Outputs: rs_rel_volume, rs_vol_confirmed
    """
    if "rs_slope" not in df.columns:
        raise ValueError("Run add_rs_slope first — rs_slope column missing")

    threshold = _p("rs_vol_confirm_threshold")

    # Reuse relative_volume from indicators.py if available
    if "relative_volume" in df.columns:
        df["rs_rel_volume"] = df["relative_volume"]
    else:
        vol_avg = _sma(df["volume"], _p("volume_avg_window"))
        df["rs_rel_volume"] = df["volume"] / vol_avg.replace(0.0, np.nan)

    df["rs_vol_confirmed"] = (
        (df["rs_slope"] > 0) & (df["rs_rel_volume"] > threshold)
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  6. RS REGIME — categorical label
# ═══════════════════════════════════════════════════════════════

def add_rs_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorical label based on two dimensions:
      • RS trend  : rs_ema vs rs_sma  (above = uptrend)
      • RS direction: rs_slope sign   (positive = improving)

    Four regimes:
      ┌────────────┬──────────────────┬──────────────────┐
      │            │  slope > 0       │  slope ≤ 0       │
      ├────────────┼──────────────────┼──────────────────┤
      │ EMA > SMA  │  LEADING         │  WEAKENING       │
      │ EMA ≤ SMA  │  IMPROVING  ★    │  LAGGING         │
      └────────────┴──────────────────┴──────────────────┘

      ★ "improving" is the sweet spot for entry — smart money
        is rotating in before the RS line crosses above its
        trend.  This is where the edge lives.

    Outputs: rs_regime
    """
    for col in ("rs_ema", "rs_sma", "rs_slope"):
        if col not in df.columns:
            raise ValueError(
                f"Run prerequisite functions first — {col} missing"
            )

    above_sma = df["rs_ema"] > df["rs_sma"]
    slope_pos = df["rs_slope"] > 0

    conditions = [
        above_sma & slope_pos,     # leading
        above_sma & ~slope_pos,    # weakening
        ~above_sma & ~slope_pos,   # lagging
        ~above_sma & slope_pos,    # improving
    ]
    labels = ["leading", "weakening", "lagging", "improving"]

    df["rs_regime"] = np.select(conditions, labels, default="unknown")
    return df


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_rs(
    stock_df: pd.DataFrame,
    bench_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute all relative-strength metrics for a stock vs its benchmark.

    Parameters
    ----------
    stock_df : pd.DataFrame
        Stock OHLCV, date-indexed, sorted ascending.
        Must have: open, high, low, close, volume.
        May already have indicator columns from indicators.py.

    bench_df : pd.DataFrame
        Benchmark OHLCV (SPY / QQQ / IWM), same format.
        Which benchmark to use per ticker is a pipeline decision,
        not a concern of this module.

    Returns
    -------
    pd.DataFrame
        stock_df (date-aligned) with columns appended:
        rs_raw, rs_ema, rs_sma, rs_slope, rs_zscore,
        rs_momentum, rs_rel_volume, rs_vol_confirmed, rs_regime

    Raises
    ------
    ValueError
        If inputs are empty, missing 'close', or have fewer
        than 30 overlapping dates.
    """
    # ── Validate ────────────────────────────────────────────
    for name, d in [("stock", stock_df), ("bench", bench_df)]:
        if "close" not in d.columns:
            raise ValueError(f"{name}_df missing 'close' column")
        if d.empty:
            raise ValueError(f"{name}_df is empty")

    # ── Align on common dates ───────────────────────────────
    common = stock_df.index.intersection(bench_df.index)
    if len(common) < 30:
        raise ValueError(
            f"Only {len(common)} overlapping dates — need at least 30"
        )

    df          = stock_df.loc[common].copy()
    bench_close = bench_df.loc[common, "close"]

    # ── Compute in dependency order ─────────────────────────
    df = add_rs_ratio(df, bench_close)       # rs_raw, rs_ema, rs_sma
    df = add_rs_slope(df)                    # rs_slope
    df = add_rs_zscore(df)                   # rs_zscore
    df = add_rs_momentum(df)                 # rs_momentum
    df = add_rs_volume_confirmation(df)      # rs_rel_volume, rs_vol_confirmed
    df = add_rs_regime(df)                   # rs_regime

    return df

#############################################

"""
compute/scoring.py
-------------------
Five-pillar composite scoring engine.

Takes a DataFrame that already has indicator columns (from indicators.py)
and relative-strength columns (from relative_strength.py), plus an
optional breadth score series (from breadth.py), and produces sub-scores
plus a weighted composite score per row (ticker-day).

Pillar 1 — Rotation       : Is smart money rotating into this name?
Pillar 2 — Momentum       : Is price action confirming the rotation?
Pillar 3 — Volatility     : Is risk / reward favorable for entry?
Pillar 4 — Microstructure : Is institutional volume backing the move?
Pillar 5 — Breadth        : Is the broad market confirming the move?

Each pillar returns a Series in [0, 1].
Weighted average → composite in [0, 1].
All weights live in common/config.py → SCORING_WEIGHTS.

When breadth data is unavailable the engine falls back to a four-pillar
mode, renormalising the remaining weights so they still sum to 1.0.

This module does NOT make trade decisions — it ranks.
The strategy layer downstream decides what score threshold triggers action.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SCORING_WEIGHTS, SCORING_PARAMS


# ═══════════════════════════════════════════════════════════════
#  COLUMN NAME MAP
#  Indicators.py outputs specific suffixed names.
#  Map logical names → actual column names so the scoring
#  engine stays decoupled from naming conventions.
# ═══════════════════════════════════════════════════════════════

COL = {
    # Pillar 1 — Rotation (from relative_strength.py)
    "rs_zscore":        "rs_zscore",
    "rs_regime":        "rs_regime",
    "rs_momentum":      "rs_momentum",
    "rs_vol_confirmed": "rs_vol_confirmed",

    # Pillar 2 — Momentum
    "rsi":              "rsi_14",
    "macd_hist":        "macd_hist",
    "adx":              "adx_14",

    # Pillar 3 — Volatility / Risk
    "realized_vol":     "realized_vol_20d",
    "atr":              "atr_14",
    "close":            "close",
    "amihud":           "amihud_20d",

    # Pillar 4 — Microstructure
    "obv":              "obv",
    "obv_slope":        "obv_slope_10d",
    "ad_line":          "ad_line",
    "ad_slope":         "ad_line_slope_10d",
    "relative_volume":  "relative_volume",
}


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSORS
# ═══════════════════════════════════════════════════════════════

def _w(key: str) -> float:
    """Fetch weight from SCORING_WEIGHTS."""
    return SCORING_WEIGHTS[key]


def _sp(key: str):
    """Fetch parameter from SCORING_PARAMS."""
    return SCORING_PARAMS[key]


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _sigmoid(x):
    """Map real values → (0, 1).  Vectorised, clipped to avoid overflow."""
    return 1.0 / (1.0 + np.exp(-np.clip(x, -10, 10)))


def _rolling_pctrank(series: pd.Series, window: int) -> pd.Series:
    """
    Rolling percentile rank in [0, 1].

    0.80 means today's value exceeds 80 % of values in the lookback.
    Self-normalising — no assumptions about scale or distribution.
    """
    def _pct(arr):
        if np.isnan(arr[-1]):
            return np.nan
        return np.sum(arr <= arr[-1]) / len(arr)

    return series.rolling(window, min_periods=window // 2).apply(
        _pct, raw=True
    )


def _col(key: str) -> str:
    """Resolve logical column name → actual DataFrame column name."""
    return COL[key]


# ═══════════════════════════════════════════════════════════════
#  PILLAR 1 — ROTATION
#  "Is smart money rotating into this stock vs benchmark?"
# ═══════════════════════════════════════════════════════════════

def _pillar_rotation(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    rs_zscore        Standardised RS slope → sigmoid.
                     z = 0 → 0.50,  z = +2 → 0.88,  z = -2 → 0.12.

    rs_regime        Categorical phase map:
                     leading 1.0 | improving 0.75 | weakening 0.25
                     lagging 0.0 | unknown   0.50

    rs_momentum      Acceleration of rotation → sigmoid.
                     Positive = RS improvement is speeding up.

    rs_vol_confirmed Binary: is above-average volume backing the move?
    """
    # ── rs_zscore ────────────────────────────────────────────
    zs = pd.Series(
        _sigmoid(df[_col("rs_zscore")].fillna(0).values),
        index=df.index,
    )

    # ── rs_regime ────────────────────────────────────────────
    regime_map = {
        "leading":   1.00,
        "improving": 0.75,
        "weakening": 0.25,
        "lagging":   0.00,
        "unknown":   0.50,
    }
    regime = df[_col("rs_regime")].map(regime_map).fillna(0.5)

    # ── rs_momentum ──────────────────────────────────────────
    mom = pd.Series(
        _sigmoid(
            df[_col("rs_momentum")].fillna(0).values
            * _sp("rs_momentum_scale")
        ),
        index=df.index,
    )

    # ── rs_vol_confirmed ─────────────────────────────────────
    vol_conf = df[_col("rs_vol_confirmed")].astype(float).fillna(0.0)

    return (
        _w("rs_zscore_w")       * zs
        + _w("rs_regime_w")     * regime
        + _w("rs_momentum_w")   * mom
        + _w("rs_vol_confirm_w") * vol_conf
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 2 — MOMENTUM
#  "Is price action confirming the rotation?"
# ═══════════════════════════════════════════════════════════════

def _pillar_momentum(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    RSI   Piecewise linear.  Sweet spot 40-70 (trending, not overbought).
          25-40 ramps up, 70-80 ramps down, extremes score 0.10.

    MACD  Histogram rolling percentile rank.
          Top of range = strong bullish momentum.

    ADX   Piecewise linear.  >25 = trending = good for momentum.
          <15 = choppy = low score.
    """
    # ── RSI ──────────────────────────────────────────────────
    rsi_score = pd.Series(
        np.interp(
            df[_col("rsi")].fillna(50).values,
            [0, 25, 40, 55, 70, 80, 100],
            [0.10, 0.10, 0.60, 1.00, 1.00, 0.50, 0.10],
        ),
        index=df.index,
    )

    # ── MACD histogram ───────────────────────────────────────
    macd_score = _rolling_pctrank(
        df[_col("macd_hist")].fillna(0.0), _sp("rank_window")
    ).fillna(0.5)

    # ── ADX ──────────────────────────────────────────────────
    adx_score = pd.Series(
        np.interp(
            df[_col("adx")].fillna(15).values,
            [0, 10, 15, 25, 40, 60],
            [0.10, 0.20, 0.40, 0.85, 1.00, 1.00],
        ),
        index=df.index,
    )

    return (
        _w("rsi_w")   * rsi_score
        + _w("macd_w") * macd_score
        + _w("adx_w")  * adx_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 3 — VOLATILITY / RISK
#  "Is risk / reward favorable for entry?"
# ═══════════════════════════════════════════════════════════════

def _pillar_volatility(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    realized_vol  Rolling percentile → tent function.
                  Moderate vol (near median) scores highest.
                  Extremes (dead money OR panic) score low.

    atr_pct       ATR ÷ close, same tent logic.
                  Captures intraday range risk vs close-to-close risk.

    amihud        Illiquidity.  Inverted rank — lower is better.
                  Liquid names get capital first.
    """
    rw = _sp("rank_window")

    # ── Realized vol → tent ──────────────────────────────────
    vol_rank  = _rolling_pctrank(
        df[_col("realized_vol")].ffill(), rw
    ).fillna(0.5)
    vol_score = 1.0 - 2.0 * (vol_rank - 0.5).abs()

    # ── ATR percent → tent ───────────────────────────────────
    atr_pct   = df[_col("atr")] / df[_col("close")].replace(0, np.nan)
    atr_rank  = _rolling_pctrank(atr_pct.ffill(), rw).fillna(0.5)
    atr_score = 1.0 - 2.0 * (atr_rank - 0.5).abs()

    # ── Amihud → inverted rank ───────────────────────────────
    amihud_rank  = _rolling_pctrank(
        df[_col("amihud")].ffill(), rw
    ).fillna(0.5)
    amihud_score = 1.0 - amihud_rank

    return (
        _w("realized_vol_w") * vol_score
        + _w("atr_pct_w")   * atr_score
        + _w("amihud_w")    * amihud_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 4 — MICROSTRUCTURE
#  "Is institutional volume backing the move?"
# ═══════════════════════════════════════════════════════════════

def _pillar_microstructure(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    OBV slope    Pre-computed rolling slope → percentile rank.
                 Rising OBV = accumulation (buying pressure).

    A/D slope    Pre-computed rolling slope → percentile rank.
                 Weighted by intra-bar position — more granular than OBV.

    Rel volume   Piecewise linear.  1.5-2.5× average = institutional
                 interest.  >4× could be panic or event-driven.
    """
    rw = _sp("rank_window")

    # ── OBV slope (pre-computed by indicators.py) ────────────
    obv_score = _rolling_pctrank(
        df[_col("obv_slope")].ffill(), rw
    ).fillna(0.5)

    # ── A/D line slope (pre-computed by indicators.py) ───────
    ad_score = _rolling_pctrank(
        df[_col("ad_slope")].ffill(), rw
    ).fillna(0.5)

    # ── Relative volume ──────────────────────────────────────
    rvol_score = pd.Series(
        np.interp(
            df[_col("relative_volume")].fillna(1.0).values,
            [0.0, 0.5, 1.0, 1.5, 2.5, 4.0, 8.0],
            [0.10, 0.20, 0.50, 0.85, 1.00, 0.80, 0.50],
        ),
        index=df.index,
    )

    return (
        _w("obv_slope_w")   * obv_score
        + _w("ad_slope_w")  * ad_score
        + _w("rel_volume_w") * rvol_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 5 — BREADTH
#  "Is the broad market confirming the move?"
# ═══════════════════════════════════════════════════════════════

def _pillar_breadth(
    df: pd.DataFrame,
    breadth_scores: pd.Series,
) -> pd.Series:
    """
    Market breadth overlay.

    The breadth pillar is unique: it is a universe-level signal
    (the same value for every symbol on a given day), not a
    per-symbol indicator.  It acts as a tide gauge — when broad
    participation is strong, all boats get a lift; when breadth
    deteriorates, conviction is dampened across the board.

    Parameters
    ----------
    df : pd.DataFrame
        The per-symbol indicator DataFrame (used only for its index).
    breadth_scores : pd.Series
        Daily breadth scores on a 0–100 scale, as produced by
        ``breadth_to_pillar_scores()`` for this symbol's column.

    Returns
    -------
    pd.Series
        Values in [0, 1] aligned to df's index.
    """
    # Align to the symbol's date index, forward-fill gaps
    # (breadth may have fewer rows if the constituent universe
    # started trading later than this symbol)
    aligned = breadth_scores.reindex(df.index).ffill().fillna(50.0)

    # Rescale 0–100 → 0–1
    return (aligned / 100.0).clip(0, 1)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_composite_score(
    df: pd.DataFrame,
    breadth_scores: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute all pillar sub-scores and the weighted composite.

    Parameters
    ----------
    df : pd.DataFrame
        Date-indexed OHLCV with indicator columns (from
        compute_all_indicators) **and** RS columns (from
        compute_all_rs) already present.
    breadth_scores : pd.Series or None
        Daily breadth score for this symbol (0–100 scale),
        typically one column from the output of
        ``breadth_to_pillar_scores()``.  If None, the engine
        falls back to four-pillar mode with renormalised weights.

    Returns
    -------
    pd.DataFrame
        Input frame with columns appended:
          score_rotation       [0-1]   Pillar 1
          score_momentum       [0-1]   Pillar 2
          score_volatility     [0-1]   Pillar 3
          score_microstructure [0-1]   Pillar 4
          score_breadth        [0-1]   Pillar 5 (if breadth provided)
          score_composite      [0-1]   Weighted average
          score_percentile     [0-1]   Time-series pct rank of composite
          breadth_available    bool    Whether breadth was used

    Raises
    ------
    ValueError
        Missing columns needed by pillars 1–4.
    """
    # ── Validate required columns (pillars 1–4) ──────────────
    required = {
        "pillar_rotation":       [
            _col("rs_zscore"), _col("rs_regime"),
            _col("rs_momentum"), _col("rs_vol_confirmed"),
        ],
        "pillar_momentum":       [
            _col("rsi"), _col("macd_hist"), _col("adx"),
        ],
        "pillar_volatility":     [
            _col("realized_vol"), _col("atr"),
            _col("close"), _col("amihud"),
        ],
        "pillar_microstructure": [
            _col("obv_slope"), _col("ad_slope"),
            _col("relative_volume"),
        ],
    }
    missing = []
    for pillar, cols in required.items():
        for c in cols:
            if c not in df.columns:
                missing.append(f"{c} (needed by {pillar})")
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # ── Determine mode: 5-pillar or 4-pillar fallback ────────
    use_breadth = (
        breadth_scores is not None
        and not breadth_scores.empty
        and "pillar_breadth" in SCORING_WEIGHTS
    )

    pillar_keys = [
        "pillar_rotation", "pillar_momentum",
        "pillar_volatility", "pillar_microstructure",
    ]
    if use_breadth:
        pillar_keys.append("pillar_breadth")

    raw_weights = {k: _w(k) for k in pillar_keys}
    total_w = sum(raw_weights.values())

    # Renormalise so active pillars sum to exactly 1.0
    if total_w <= 0:
        raise ValueError("Pillar weights sum to zero.")
    weights = {k: v / total_w for k, v in raw_weights.items()}

    # ── Compute pillar scores ─────────────────────────────────
    out = df.copy()

    out["score_rotation"]       = _pillar_rotation(out).clip(0, 1)
    out["score_momentum"]       = _pillar_momentum(out).clip(0, 1)
    out["score_volatility"]     = _pillar_volatility(out).clip(0, 1)
    out["score_microstructure"] = _pillar_microstructure(out).clip(0, 1)

    if use_breadth:
        out["score_breadth"] = _pillar_breadth(out, breadth_scores).clip(0, 1)

    # ── Composite ─────────────────────────────────────────────
    composite = (
        weights["pillar_rotation"]       * out["score_rotation"]
        + weights["pillar_momentum"]     * out["score_momentum"]
        + weights["pillar_volatility"]   * out["score_volatility"]
        + weights["pillar_microstructure"] * out["score_microstructure"]
    )

    if use_breadth:
        composite += weights["pillar_breadth"] * out["score_breadth"]

    out["score_composite"] = composite.clip(0, 1)

    # ── Time-series percentile ────────────────────────────────
    out["score_percentile"] = _rolling_pctrank(
        out["score_composite"], 252
    )

    # ── Metadata ──────────────────────────────────────────────
    out["breadth_available"] = use_breadth

    return out

#########################################################

"""
compute/sector_rs.py
--------------------
Sector-level relative-strength analysis.

Answers: "Which sectors are leading the market rotation?"

Pipeline
────────
  1. Fetch OHLCV for 11 GICS sector ETFs + benchmark (SPY).
  2. Compute RS ratio / slope / z-score / regime per sector
     (same math as stock-level RS in relative_strength.py).
  3. Cross-sectionally rank sectors each day — who's strongest?
  4. Derive a tailwind / headwind value per sector for the
     composite-score adjustment.
  5. Merge sector context into individual stock DataFrames.

All tuneable parameters live in common/config.py.
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from common.config import (
    BENCHMARK_TICKER,
    SECTOR_ETFS,
    SECTOR_RS_PARAMS,
    SECTOR_SCORE_ADJUSTMENT,
)


# ═══════════════════════════════════════════════════════════════
#  yfinance sector label → our SECTOR_ETFS key
# ═══════════════════════════════════════════════════════════════

_YF_SECTOR_MAP: dict[str, str] = {
    "Technology":             "Technology",
    "Healthcare":             "Healthcare",
    "Financial Services":     "Financials",
    "Consumer Cyclical":      "Consumer Disc",
    "Consumer Defensive":     "Consumer Staples",
    "Energy":                 "Energy",
    "Industrials":            "Industrials",
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Communication Services": "Communication",
}

# In-memory cache so we only hit yfinance once per ticker
_sector_cache: dict[str, str | None] = {}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _clean_download(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise a yfinance download to lower-case flat columns."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """OLS slope over a rolling window."""
    def _slope(arr):
        if np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]
    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True,
    )


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_sector_data(
    period: str = "2y",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Download OHLCV for every sector ETF **and** the benchmark.

    Parameters
    ----------
    period : str
        yfinance period string ("2y", "5y", "max" …).

    Returns
    -------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV DataFrame} for each sector.
    benchmark_df : pd.DataFrame
        OHLCV for the benchmark (SPY by default).
    """
    import yfinance as yf

    sector_data: dict[str, pd.DataFrame] = {}
    for name, etf in SECTOR_ETFS.items():
        raw = yf.download(etf, period=period, progress=False)
        if raw.empty:
            warnings.warn(f"No data for {etf} ({name}), skipping.")
            continue
        sector_data[name] = _clean_download(raw)

    bench_raw = yf.download(BENCHMARK_TICKER, period=period, progress=False)
    if bench_raw.empty:
        raise ValueError(f"Benchmark {BENCHMARK_TICKER} returned no data.")
    benchmark_df = _clean_download(bench_raw)

    return sector_data, benchmark_df


# ═══════════════════════════════════════════════════════════════
#  SINGLE-SECTOR RS COMPUTATION
# ═══════════════════════════════════════════════════════════════

def _compute_single_sector_rs(
    sector_close: pd.Series,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS metrics for **one** sector ETF vs the benchmark.

    Same mathematics as relative_strength.py:
      ratio → slope → z-score → regime.

    Returns DataFrame with columns prefixed ``sect_rs_``.
    """
    p = SECTOR_RS_PARAMS

    # ── RS ratio ─────────────────────────────────────────────
    ratio = sector_close / bench_close

    # ── RS slope (direction of relative performance) ─────────
    slope = _rolling_slope(ratio, p["slope_window"])

    # ── Z-score of slope ─────────────────────────────────────
    s_mean = slope.rolling(p["zscore_window"], min_periods=20).mean()
    s_std  = slope.rolling(p["zscore_window"], min_periods=20).std()
    zscore = (slope - s_mean) / s_std.replace(0, np.nan)

    # ── Momentum (acceleration of rotation) ──────────────────
    momentum = slope.diff(p["momentum_window"])

    # ── Regime ───────────────────────────────────────────────
    conditions = [
        (zscore > 0) & (momentum > 0),
        (zscore <= 0) & (momentum > 0),
        (zscore > 0)  & (momentum <= 0),
        (zscore <= 0) & (momentum <= 0),
    ]
    choices = ["leading", "improving", "weakening", "lagging"]
    regime = pd.Series(
        np.select(conditions, choices, default="unknown"),
        index=ratio.index,
    )

    return pd.DataFrame({
        "sect_rs_ratio":    ratio,
        "sect_rs_slope":    slope,
        "sect_rs_zscore":   zscore,
        "sect_rs_momentum": momentum,
        "sect_rs_regime":   regime,
    }, index=ratio.index)


# ═══════════════════════════════════════════════════════════════
#  MASTER COMPUTATION — ALL SECTORS + CROSS-SECTIONAL RANKS
# ═══════════════════════════════════════════════════════════════

def compute_all_sector_rs(
    sector_data: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute RS for every sector and add cross-sectional rankings.

    Parameters
    ----------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV} from :func:`fetch_sector_data`.
    benchmark_df : pd.DataFrame
        Benchmark OHLCV.

    Returns
    -------
    pd.DataFrame
        **MultiIndex (date, sector)** with columns:

        ============== ===============================================
        sect_rs_ratio  raw price ratio (sector ETF / benchmark)
        sect_rs_slope  regression slope of the ratio
        sect_rs_zscore standardised slope
        sect_rs_momentum acceleration of the slope
        sect_rs_regime leading / improving / weakening / lagging
        sect_rs_rank   integer rank, 1 = strongest (NaN during warmup)
        sect_rs_pctrank percentile rank 0–1, 1.0 = strongest (smoothed)
        sector_tailwind score adjustment value
        etf            sector ETF ticker symbol
        ============== ===============================================
    """
    frames: list[pd.DataFrame] = []

    for sector_name, sector_df in sector_data.items():
        common = sector_df.index.intersection(benchmark_df.index)
        if len(common) < 60:
            warnings.warn(
                f"{sector_name}: only {len(common)} overlapping dates, "
                f"need ≥60.  Skipping."
            )
            continue

        metrics = _compute_single_sector_rs(
            sector_df.loc[common, "close"],
            benchmark_df.loc[common, "close"],
        )
        metrics["sector"] = sector_name
        metrics["etf"]    = SECTOR_ETFS[sector_name]
        metrics.index.name = "date"
        frames.append(metrics)

    if not frames:
        raise ValueError("No sectors produced valid RS data.")

    combined = pd.concat(frames)

    # ── Cross-sectional rank per date ─────────────────────────
    #    rank 1 = highest z-score = strongest sector
    #    NaN zscore rows → NaN rank (warmup period)
    combined["sect_rs_rank"] = (
        combined
        .groupby(level=0)["sect_rs_zscore"]
        .rank(ascending=False, method="min", na_option="keep")
    )

    # ── Percentile rank (0–1, 1.0 = strongest) ───────────────
    #    Count only non-NaN ranks per date
    n_ranked = (
        combined
        .groupby(level=0)["sect_rs_rank"]
        .transform(lambda s: s.notna().sum())
    )
    combined["sect_rs_pctrank"] = np.where(
        combined["sect_rs_rank"].isna() | (n_ranked <= 1),
        np.nan,
        1.0 - (combined["sect_rs_rank"] - 1) / (n_ranked - 1),
    )

    # ── Smooth the percentile rank over time ──────────────────
    smooth_w = SECTOR_RS_PARAMS.get("rank_smoothing", 5)
    if smooth_w > 1:
        combined["sect_rs_pctrank"] = (
            combined
            .groupby("sector")["sect_rs_pctrank"]
            .transform(lambda s: s.rolling(smooth_w, min_periods=1).mean())
        )

    # ── Sector tailwind / headwind ────────────────────────────
    #    pctrank 1.0 → max_boost,  0.0 → max_penalty,  0.5 → 0
    adj = SECTOR_SCORE_ADJUSTMENT
    if adj["enabled"]:
        combined["sector_tailwind"] = np.where(
            combined["sect_rs_pctrank"].isna(),
            0.0,
            adj["max_penalty"]
            + (adj["max_boost"] - adj["max_penalty"])
            * combined["sect_rs_pctrank"],
        )
    else:
        combined["sector_tailwind"] = 0.0

    # ── Set MultiIndex (date, sector) ─────────────────────────
    combined = (
        combined
        .reset_index()
        .set_index(["date", "sector"])
        .sort_index()
    )

    return combined


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT — one-day cross-sectional view (for dashboards)
# ═══════════════════════════════════════════════════════════════

def sector_snapshot(
    sector_rs_df: pd.DataFrame,
    date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Sector rankings for a single date, sorted strongest → weakest.

    Parameters
    ----------
    sector_rs_df : pd.DataFrame
        Output of :func:`compute_all_sector_rs` (MultiIndex).
    date : str or Timestamp, optional
        Target date.  Defaults to the latest available date.

    Returns
    -------
    pd.DataFrame  – one row per sector, indexed by sector name.
    """
    dates = sector_rs_df.index.get_level_values("date")

    if date is None:
        target = dates.max()
    else:
        target = pd.Timestamp(date)
        if target not in dates:
            available = dates.unique().sort_values()
            mask = available <= target
            if not mask.any():
                raise ValueError(f"No data on or before {date}")
            target = available[mask][-1]

    snap = sector_rs_df.loc[target].copy()
    return snap.sort_values("sect_rs_rank")


# ═══════════════════════════════════════════════════════════════
#  SECTOR LOOKUP — ticker → sector name
# ═══════════════════════════════════════════════════════════════

def lookup_sector(ticker: str) -> str | None:
    """
    Look up the GICS sector for a stock ticker via yfinance.

    Returns the sector name matching a SECTOR_ETFS key,
    or ``None`` if the lookup fails.

    Results are cached in memory for the session.
    """
    import yfinance as yf

    ticker = ticker.upper()

    if ticker in _sector_cache:
        return _sector_cache[ticker]

    try:
        info = yf.Ticker(ticker).info
        yf_sector = info.get("sector", None)
        if yf_sector and yf_sector in _YF_SECTOR_MAP:
            result = _YF_SECTOR_MAP[yf_sector]
        else:
            warnings.warn(
                f"Could not map yfinance sector '{yf_sector}' "
                f"for {ticker}."
            )
            result = None
    except Exception as e:
        warnings.warn(f"Sector lookup failed for {ticker}: {e}")
        result = None

    _sector_cache[ticker] = result
    return result


# ═══════════════════════════════════════════════════════════════
#  MERGE — add sector context to an individual stock DataFrame
# ═══════════════════════════════════════════════════════════════

def merge_sector_context(
    stock_df: pd.DataFrame,
    sector_rs_df: pd.DataFrame,
    sector_name: str,
) -> pd.DataFrame:
    """
    Add sector-level columns to an individual stock's DataFrame.

    Columns added
    ─────────────
    sect_rs_zscore     Sector z-score vs benchmark
    sect_rs_regime     Sector regime
    sect_rs_rank       Sector rank (1 = best)
    sect_rs_pctrank    Sector percentile (1.0 = best)
    sector_tailwind    Score adjustment value
    sector_name        Sector label

    If ``score_composite`` already exists, also creates:

    score_adjusted     score_composite + sector_tailwind, clipped [0, 1]

    Parameters
    ----------
    stock_df : pd.DataFrame
        Date-indexed stock data (with or without score columns).
    sector_rs_df : pd.DataFrame
        MultiIndex (date, sector) from :func:`compute_all_sector_rs`.
    sector_name : str
        Must match a key in SECTOR_ETFS.

    Returns
    -------
    pd.DataFrame  – stock_df with sector columns appended.
    """
    available_sectors = (
        sector_rs_df.index
        .get_level_values("sector")
        .unique()
        .tolist()
    )
    if sector_name not in available_sectors:
        raise ValueError(
            f"Sector '{sector_name}' not found.  "
            f"Available: {available_sectors}"
        )

    # Extract this sector, drop the sector level → date-indexed
    sect = sector_rs_df.xs(sector_name, level="sector")

    merge_cols = [
        "sect_rs_zscore", "sect_rs_regime", "sect_rs_rank",
        "sect_rs_pctrank", "sector_tailwind",
    ]
    merge_cols = [c for c in merge_cols if c in sect.columns]

    out = stock_df.copy()
    out = out.join(sect[merge_cols], how="left")
    out["sector_name"] = sector_name

    # ── Adjusted composite score ─────────────────────────────
    if "score_composite" in out.columns and "sector_tailwind" in out.columns:
        out["score_adjusted"] = (
            out["score_composite"]
            + out["sector_tailwind"].fillna(0)
        ).clip(0, 1)

    return out

#####################################
"""
OUTPUT :
------------
"""
	"""
compute/sector_rs.py
--------------------
Sector-level relative-strength analysis.

Answers: "Which sectors are leading the market rotation?"

Pipeline
────────
  1. Fetch OHLCV for 11 GICS sector ETFs + benchmark (SPY).
  2. Compute RS ratio / slope / z-score / regime per sector
     (same math as stock-level RS in relative_strength.py).
  3. Cross-sectionally rank sectors each day — who's strongest?
  4. Derive a tailwind / headwind value per sector for the
     composite-score adjustment.
  5. Merge sector context into individual stock DataFrames.

All tuneable parameters live in common/config.py.
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from common.config import (
    BENCHMARK_TICKER,
    SECTOR_ETFS,
    SECTOR_RS_PARAMS,
    SECTOR_SCORE_ADJUSTMENT,
)


# ═══════════════════════════════════════════════════════════════
#  yfinance sector label → our SECTOR_ETFS key
# ═══════════════════════════════════════════════════════════════

_YF_SECTOR_MAP: dict[str, str] = {
    "Technology":             "Technology",
    "Healthcare":             "Healthcare",
    "Financial Services":     "Financials",
    "Consumer Cyclical":      "Consumer Disc",
    "Consumer Defensive":     "Consumer Staples",
    "Energy":                 "Energy",
    "Industrials":            "Industrials",
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Communication Services": "Communication",
}

# In-memory cache so we only hit yfinance once per ticker
_sector_cache: dict[str, str | None] = {}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _clean_download(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise a yfinance download to lower-case flat columns."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """OLS slope over a rolling window."""
    def _slope(arr):
        if np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]
    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True,
    )


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_sector_data(
    period: str = "2y",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Download OHLCV for every sector ETF **and** the benchmark.

    Parameters
    ----------
    period : str
        yfinance period string ("2y", "5y", "max" …).

    Returns
    -------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV DataFrame} for each sector.
    benchmark_df : pd.DataFrame
        OHLCV for the benchmark (SPY by default).
    """
    import yfinance as yf

    sector_data: dict[str, pd.DataFrame] = {}
    for name, etf in SECTOR_ETFS.items():
        raw = yf.download(etf, period=period, progress=False)
        if raw.empty:
            warnings.warn(f"No data for {etf} ({name}), skipping.")
            continue
        sector_data[name] = _clean_download(raw)

    bench_raw = yf.download(BENCHMARK_TICKER, period=period, progress=False)
    if bench_raw.empty:
        raise ValueError(f"Benchmark {BENCHMARK_TICKER} returned no data.")
    benchmark_df = _clean_download(bench_raw)

    return sector_data, benchmark_df


# ═══════════════════════════════════════════════════════════════
#  SINGLE-SECTOR RS COMPUTATION
# ═══════════════════════════════════════════════════════════════

def _compute_single_sector_rs(
    sector_close: pd.Series,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS metrics for **one** sector ETF vs the benchmark.

    Same mathematics as relative_strength.py:
      ratio → slope → z-score → regime.

    Returns DataFrame with columns prefixed ``sect_rs_``.
    """
    p = SECTOR_RS_PARAMS

    # ── RS ratio ─────────────────────────────────────────────
    ratio = sector_close / bench_close

    # ── RS slope (direction of relative performance) ─────────
    slope = _rolling_slope(ratio, p["slope_window"])

    # ── Z-score of slope ─────────────────────────────────────
    s_mean = slope.rolling(p["zscore_window"], min_periods=20).mean()
    s_std  = slope.rolling(p["zscore_window"], min_periods=20).std()
    zscore = (slope - s_mean) / s_std.replace(0, np.nan)

    # ── Momentum (acceleration of rotation) ──────────────────
    momentum = slope.diff(p["momentum_window"])

    # ── Regime ───────────────────────────────────────────────
    conditions = [
        (zscore > 0) & (momentum > 0),
        (zscore <= 0) & (momentum > 0),
        (zscore > 0)  & (momentum <= 0),
        (zscore <= 0) & (momentum <= 0),
    ]
    choices = ["leading", "improving", "weakening", "lagging"]
    regime = pd.Series(
        np.select(conditions, choices, default="unknown"),
        index=ratio.index,
    )

    return pd.DataFrame({
        "sect_rs_ratio":    ratio,
        "sect_rs_slope":    slope,
        "sect_rs_zscore":   zscore,
        "sect_rs_momentum": momentum,
        "sect_rs_regime":   regime,
    }, index=ratio.index)


# ═══════════════════════════════════════════════════════════════
#  MASTER COMPUTATION — ALL SECTORS + CROSS-SECTIONAL RANKS
# ═══════════════════════════════════════════════════════════════

def compute_all_sector_rs(
    sector_data: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute RS for every sector and add cross-sectional rankings.

    Parameters
    ----------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV} from :func:`fetch_sector_data`.
    benchmark_df : pd.DataFrame
        Benchmark OHLCV.

    Returns
    -------
    pd.DataFrame
        **MultiIndex (date, sector)** with columns:

        ============== ===============================================
        sect_rs_ratio  raw price ratio (sector ETF / benchmark)
        sect_rs_slope  regression slope of the ratio
        sect_rs_zscore standardised slope
        sect_rs_momentum acceleration of the slope
        sect_rs_regime leading / improving / weakening / lagging
        sect_rs_rank   integer rank, 1 = strongest (NaN during warmup)
        sect_rs_pctrank percentile rank 0–1, 1.0 = strongest (smoothed)
        sector_tailwind score adjustment value
        etf            sector ETF ticker symbol
        ============== ===============================================
    """
    frames: list[pd.DataFrame] = []

    for sector_name, sector_df in sector_data.items():
        common = sector_df.index.intersection(benchmark_df.index)
        if len(common) < 60:
            warnings.warn(
                f"{sector_name}: only {len(common)} overlapping dates, "
                f"need ≥60.  Skipping."
            )
            continue

        metrics = _compute_single_sector_rs(
            sector_df.loc[common, "close"],
            benchmark_df.loc[common, "close"],
        )
        metrics["sector"] = sector_name
        metrics["etf"]    = SECTOR_ETFS[sector_name]
        metrics.index.name = "date"
        frames.append(metrics)

    if not frames:
        raise ValueError("No sectors produced valid RS data.")

    combined = pd.concat(frames)

    # ── Cross-sectional rank per date ─────────────────────────
    #    rank 1 = highest z-score = strongest sector
    #    NaN zscore rows → NaN rank (warmup period)
    combined["sect_rs_rank"] = (
        combined
        .groupby(level=0)["sect_rs_zscore"]
        .rank(ascending=False, method="min", na_option="keep")
    )

    # ── Percentile rank (0–1, 1.0 = strongest) ───────────────
    #    Count only non-NaN ranks per date
    n_ranked = (
        combined
        .groupby(level=0)["sect_rs_rank"]
        .transform(lambda s: s.notna().sum())
    )
    combined["sect_rs_pctrank"] = np.where(
        combined["sect_rs_rank"].isna() | (n_ranked <= 1),
        np.nan,
        1.0 - (combined["sect_rs_rank"] - 1) / (n_ranked - 1),
    )

    # ── Smooth the percentile rank over time ──────────────────
    smooth_w = SECTOR_RS_PARAMS.get("rank_smoothing", 5)
    if smooth_w > 1:
        combined["sect_rs_pctrank"] = (
            combined
            .groupby("sector")["sect_rs_pctrank"]
            .transform(lambda s: s.rolling(smooth_w, min_periods=1).mean())
        )

    # ── Sector tailwind / headwind ────────────────────────────
    #    pctrank 1.0 → max_boost,  0.0 → max_penalty,  0.5 → 0
    adj = SECTOR_SCORE_ADJUSTMENT
    if adj["enabled"]:
        combined["sector_tailwind"] = np.where(
            combined["sect_rs_pctrank"].isna(),
            0.0,
            adj["max_penalty"]
            + (adj["max_boost"] - adj["max_penalty"])
            * combined["sect_rs_pctrank"],
        )
    else:
        combined["sector_tailwind"] = 0.0

    # ── Set MultiIndex (date, sector) ─────────────────────────
    combined = (
        combined
        .reset_index()
        .set_index(["date", "sector"])
        .sort_index()
    )

    return combined


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT — one-day cross-sectional view (for dashboards)
# ═══════════════════════════════════════════════════════════════

def sector_snapshot(
    sector_rs_df: pd.DataFrame,
    date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Sector rankings for a single date, sorted strongest → weakest.

    Parameters
    ----------
    sector_rs_df : pd.DataFrame
        Output of :func:`compute_all_sector_rs` (MultiIndex).
    date : str or Timestamp, optional
        Target date.  Defaults to the latest available date.

    Returns
    -------
    pd.DataFrame  – one row per sector, indexed by sector name.
    """
    dates = sector_rs_df.index.get_level_values("date")

    if date is None:
        target = dates.max()
    else:
        target = pd.Timestamp(date)
        if target not in dates:
            available = dates.unique().sort_values()
            mask = available <= target
            if not mask.any():
                raise ValueError(f"No data on or before {date}")
            target = available[mask][-1]

    snap = sector_rs_df.loc[target].copy()
    return snap.sort_values("sect_rs_rank")


# ═══════════════════════════════════════════════════════════════
#  SECTOR LOOKUP — ticker → sector name
# ═══════════════════════════════════════════════════════════════

def lookup_sector(ticker: str) -> str | None:
    """
    Look up the GICS sector for a stock ticker via yfinance.

    Returns the sector name matching a SECTOR_ETFS key,
    or ``None`` if the lookup fails.

    Results are cached in memory for the session.
    """
    import yfinance as yf

    ticker = ticker.upper()

    if ticker in _sector_cache:
        return _sector_cache[ticker]

    try:
        info = yf.Ticker(ticker).info
        yf_sector = info.get("sector", None)
        if yf_sector and yf_sector in _YF_SECTOR_MAP:
            result = _YF_SECTOR_MAP[yf_sector]
        else:
            warnings.warn(
                f"Could not map yfinance sector '{yf_sector}' "
                f"for {ticker}."
            )
            result = None
    except Exception as e:
        warnings.warn(f"Sector lookup failed for {ticker}: {e}")
        result = None

    _sector_cache[ticker] = result
    return result


# ═══════════════════════════════════════════════════════════════
#  MERGE — add sector context to an individual stock DataFrame
# ═══════════════════════════════════════════════════════════════

def merge_sector_context(
    stock_df: pd.DataFrame,
    sector_rs_df: pd.DataFrame,
    sector_name: str,
) -> pd.DataFrame:
    """
    Add sector-level columns to an individual stock's DataFrame.

    Columns added
    ─────────────
    sect_rs_zscore     Sector z-score vs benchmark
    sect_rs_regime     Sector regime
    sect_rs_rank       Sector rank (1 = best)
    sect_rs_pctrank    Sector percentile (1.0 = best)
    sector_tailwind    Score adjustment value
    sector_name        Sector label

    If ``score_composite`` already exists, also creates:

    score_adjusted     score_composite + sector_tailwind, clipped [0, 1]

    Parameters
    ----------
    stock_df : pd.DataFrame
        Date-indexed stock data (with or without score columns).
    sector_rs_df : pd.DataFrame
        MultiIndex (date, sector) from :func:`compute_all_sector_rs`.
    sector_name : str
        Must match a key in SECTOR_ETFS.

    Returns
    -------
    pd.DataFrame  – stock_df with sector columns appended.
    """
    available_sectors = (
        sector_rs_df.index
        .get_level_values("sector")
        .unique()
        .tolist()
    )
    if sector_name not in available_sectors:
        raise ValueError(
            f"Sector '{sector_name}' not found.  "
            f"Available: {available_sectors}"
        )

    # Extract this sector, drop the sector level → date-indexed
    sect = sector_rs_df.xs(sector_name, level="sector")

    merge_cols = [
        "sect_rs_zscore", "sect_rs_regime", "sect_rs_rank",
        "sect_rs_pctrank", "sector_tailwind",
    ]
    merge_cols = [c for c in merge_cols if c in sect.columns]

    out = stock_df.copy()
    out = out.join(sect[merge_cols], how="left")
    out["sector_name"] = sector_name

    # ── Adjusted composite score ─────────────────────────────
    if "score_composite" in out.columns and "sector_tailwind" in out.columns:
        out["score_adjusted"] = (
            out["score_composite"]
            + out["sector_tailwind"].fillna(0)
        ).clip(0, 1)

    return out


########################################################
"""
output/rankings.py
------------------
Daily cross-sectional rankings across the scored ETF / stock universe.

Takes the output of the scoring pipeline — one scored DataFrame per
symbol — and produces ranked tables showing which names have the
strongest composite scores on any given trading day.

This is the bridge between scoring and portfolio: the strategy layer
consumes these rankings to decide what to buy, hold, and sell.

Key Columns Added
─────────────────
  rank              1 = best (highest composite score)
  pct_rank          0–1 percentile within universe
  universe_size     how many symbols are ranked that day
  rank_change       +N = improved N places vs prior day
  pillars_bullish   count of pillar scores > 0.50
  pillar_agreement  fraction of pillars > 0.50  (0–1)
  ret_1d / 5d / 20d recent returns for context

Pipeline
────────
  {ticker: scored_df}
       ↓
  build_rankings_panel()     — stack into MultiIndex panel
       ↓
  rank_universe()            — cross-sectional rank per date
       ↓
  compute_rank_changes()     — day-over-day rank movement
       ↓
  compute_pillar_agreement() — signal agreement across pillars
       ↓
  compute_all_rankings()     — master orchestrator → ranked panel
       ↓
  latest_rankings()          — snapshot for a single date
  filter_top_n()             — top N symbols
  filter_by_regime()         — filter by RS regime
  rank_history()             — single ticker over time
  rankings_summary()         — summary statistics dict
  rankings_report()          — formatted text report
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  COLUMN LISTS
# ═══════════════════════════════════════════════════════════════

_SCORE_COLS = [
    "score_composite",
    "score_adjusted",
    "score_rotation",
    "score_momentum",
    "score_volatility",
    "score_microstructure",
    "score_breadth",
    "score_percentile",
]

_META_COLS = [
    "rs_regime",
    "rs_zscore",
    "rs_momentum",
    "sect_rs_regime",
    "close",
    "breadth_available",
]

_PILLAR_COLS = [
    "score_rotation",
    "score_momentum",
    "score_volatility",
    "score_microstructure",
    "score_breadth",
]


# ═══════════════════════════════════════════════════════════════
#  PANEL BUILDER
# ═══════════════════════════════════════════════════════════════

def build_rankings_panel(
    scored_universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Stack per-symbol scored DataFrames into a single panel.

    Parameters
    ----------
    scored_universe : dict
        {ticker: DataFrame} where each DataFrame has been through
        compute_all_indicators → compute_all_rs →
        compute_composite_score, and optionally
        strategy.signals.generate_signals().

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) with score, metadata, signal
        gate, and return columns.  Symbols missing
        ``score_composite`` are silently skipped.

    Notes
    -----
    Any column starting with ``sig_`` is automatically carried
    forward so that per-ticker gates from ``strategy/signals.py``
    are available to ``output/signals.py`` for entry qualification.
    """
    frames: list[pd.DataFrame] = []

    for ticker, df in scored_universe.items():
        if df is None or df.empty:
            continue
        if "score_composite" not in df.columns:
            continue

        # ── Core score + metadata columns ─────────────────
        available = [c for c in _SCORE_COLS + _META_COLS
                     if c in df.columns]

        # ── Per-ticker signal gate columns (strategy/) ────
        sig_cols = [c for c in df.columns if c.startswith("sig_")]
        available += sig_cols

        subset = df[available].copy()
        subset["ticker"] = ticker

        # Recent returns for context
        if "close" in df.columns:
            c = df["close"]
            subset["ret_1d"]  = c.pct_change(1)
            subset["ret_5d"]  = c.pct_change(5)
            subset["ret_20d"] = c.pct_change(20)

        frames.append(subset)

    if not frames:
        return pd.DataFrame()

    panel = pd.concat(frames)
    panel = panel.set_index("ticker", append=True)
    panel.index.names = ["date", "ticker"]

    return panel.sort_index()


# ═══════════════════════════════════════════════════════════════
#  CROSS-SECTIONAL RANKING
# ═══════════════════════════════════════════════════════════════

def rank_universe(
    panel: pd.DataFrame,
    rank_col: str = "score_composite",
) -> pd.DataFrame:
    """
    Cross-sectional rank on each trading day.

    Higher score = better = rank 1.

    Parameters
    ----------
    panel : pd.DataFrame
        Output of ``build_rankings_panel()``.
    rank_col : str
        Column to rank by (default ``score_composite``).

    Returns
    -------
    pd.DataFrame
        Same frame with ``rank``, ``pct_rank``, ``universe_size``
        added.
    """
    if panel.empty or rank_col not in panel.columns:
        return panel

    result = panel.copy()
    grouped = result.groupby(level="date")[rank_col]

    result["rank"] = grouped.rank(
        ascending=False, method="min",
    ).astype(int)
    result["pct_rank"] = grouped.rank(ascending=False, pct=True)
    result["universe_size"] = grouped.transform("count").astype(int)

    return result


# ═══════════════════════════════════════════════════════════════
#  RANK CHANGES
# ═══════════════════════════════════════════════════════════════

def compute_rank_changes(
    ranked: pd.DataFrame,
) -> pd.DataFrame:
    """
    Day-over-day rank movement for each ticker.

    rank_change > 0  → symbol moved UP in ranking (improved)
    rank_change < 0  → symbol dropped
    rank_change = 0  → unchanged
    """
    if ranked.empty or "rank" not in ranked.columns:
        return ranked

    result = ranked.copy()

    rank_wide = result["rank"].unstack(level="ticker")
    # previous_rank − current_rank → positive = improved
    change_wide = -rank_wide.diff()

    change_long = change_wide.stack()
    change_long.name = "rank_change"
    change_long.index.names = ["date", "ticker"]

    result = result.join(change_long)
    result["rank_change"] = result["rank_change"].fillna(0).astype(int)

    return result


# ═══════════════════════════════════════════════════════════════
#  PILLAR AGREEMENT
# ═══════════════════════════════════════════════════════════════

def compute_pillar_agreement(
    ranked: pd.DataFrame,
    threshold: float = 0.50,
) -> pd.DataFrame:
    """
    Count how many pillar scores exceed a threshold.

    High agreement (4/5 or 5/5 pillars bullish) signals broad
    confirmation — the composite isn't being carried by a single
    strong pillar masking weakness elsewhere.

    Parameters
    ----------
    ranked : pd.DataFrame
        Ranked panel with pillar score columns.
    threshold : float
        Score above which a pillar counts as bullish (default 0.50).

    Returns
    -------
    pd.DataFrame
        Same frame with ``pillars_bullish`` (int count) and
        ``pillar_agreement`` (0–1 fraction) appended.
    """
    if ranked.empty:
        return ranked

    result = ranked.copy()
    available = [c for c in _PILLAR_COLS if c in result.columns]

    if not available:
        result["pillars_bullish"]  = 0
        result["pillar_agreement"] = 0.0
        return result

    above = (result[available] > threshold).sum(axis=1)
    result["pillars_bullish"]  = above.astype(int)
    result["pillar_agreement"] = above / len(available)

    return result


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def latest_rankings(
    ranked: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Extract one day's rankings as a flat ticker-indexed DataFrame.

    Parameters
    ----------
    ranked : pd.DataFrame
        Fully ranked panel (MultiIndex: date, ticker).
    date : pd.Timestamp or None
        Date to extract.  If None, uses the most recent date.

    Returns
    -------
    pd.DataFrame
        Ticker-indexed, sorted by rank (1 = best first).
    """
    if ranked.empty:
        return pd.DataFrame()

    dates = ranked.index.get_level_values("date").unique()

    if date is not None:
        if date not in dates:
            prior = dates[dates <= date]
            if prior.empty:
                return pd.DataFrame()
            date = prior[-1]
    else:
        date = dates[-1]

    snapshot = ranked.xs(date, level="date").copy()
    snapshot["date"] = date

    return snapshot.sort_values("rank")


def filter_top_n(
    snapshot: pd.DataFrame,
    n: int = 5,
) -> pd.DataFrame:
    """Filter a snapshot to the top N ranked symbols."""
    if snapshot.empty or "rank" not in snapshot.columns:
        return snapshot
    return snapshot[snapshot["rank"] <= n].copy()


def filter_by_regime(
    snapshot: pd.DataFrame,
    regimes: list[str],
) -> pd.DataFrame:
    """Filter a snapshot to symbols in the given RS regimes."""
    if snapshot.empty or "rs_regime" not in snapshot.columns:
        return snapshot
    return snapshot[snapshot["rs_regime"].isin(regimes)].copy()


# ═══════════════════════════════════════════════════════════════
#  RANK HISTORY  (single ticker over time)
# ═══════════════════════════════════════════════════════════════

def rank_history(
    ranked: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """
    Extract one ticker's ranking history.

    Returns a date-indexed DataFrame showing rank, score, and
    change columns over time.
    """
    if ranked.empty:
        return pd.DataFrame()

    tickers = ranked.index.get_level_values("ticker").unique()
    if ticker not in tickers:
        return pd.DataFrame()

    return ranked.xs(ticker, level="ticker").copy()


# ═══════════════════════════════════════════════════════════════
#  SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════

def rankings_summary(ranked: pd.DataFrame) -> dict:
    """
    Compute summary statistics for the latest day's rankings.

    Returns a dict with keys: date, universe_size, mean_composite,
    median_composite, std_composite, spread, top_ticker, top_score,
    bottom_ticker, bottom_score, regime_distribution.
    """
    snap = latest_rankings(ranked)
    if snap.empty:
        return {}

    comp = snap.get("score_composite")

    summary: dict = {
        "date":             snap["date"].iloc[0] if "date" in snap.columns else None,
        "universe_size":    len(snap),
        "mean_composite":   comp.mean()   if comp is not None else None,
        "median_composite": comp.median() if comp is not None else None,
        "std_composite":    comp.std()    if comp is not None else None,
        "spread":           (comp.max() - comp.min()) if comp is not None else None,
        "top_ticker":       snap.index[0],
        "top_score":        comp.iloc[0]  if comp is not None else None,
        "bottom_ticker":    snap.index[-1],
        "bottom_score":     comp.iloc[-1] if comp is not None else None,
    }

    if "rs_regime" in snap.columns:
        summary["regime_distribution"] = (
            snap["rs_regime"].value_counts().to_dict()
        )

    if "pillar_agreement" in snap.columns:
        summary["mean_agreement"] = snap["pillar_agreement"].mean()

    return summary


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_rankings(
    scored_universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Full rankings pipeline.

    Parameters
    ----------
    scored_universe : dict
        {ticker: scored_df} — output of the scoring pipeline
        for each symbol in the universe.  May optionally include
        ``sig_*`` columns from ``strategy/signals.py``; these
        are carried forward into the ranked panel.

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) panel with all scores,
        ranks, rank changes, pillar agreement metrics, and
        any per-ticker signal gate columns.
    """
    panel = build_rankings_panel(scored_universe)
    if panel.empty:
        return pd.DataFrame()

    ranked = rank_universe(panel)
    ranked = compute_rank_changes(ranked)
    ranked = compute_pillar_agreement(ranked)

    return ranked


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def rankings_report(
    ranked: pd.DataFrame,
    top_n: int | None = None,
    breadth_regime: str = "unknown",
    breadth_score: float = 0.0,
) -> str:
    """
    Formatted text report of the latest rankings.

    Parameters
    ----------
    ranked : pd.DataFrame
        Output of ``compute_all_rankings()``.
    top_n : int or None
        If set, only show the top N symbols.  None = show all.
    breadth_regime : str
        Current breadth regime label (for the header).
    breadth_score : float
        Current breadth score (0–1) for the header.

    Returns
    -------
    str
        Human-readable rankings report.
    """
    if ranked.empty:
        return "No rankings data available."

    snap = latest_rankings(ranked)
    if snap.empty:
        return "No rankings data available."

    summary = rankings_summary(ranked)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )

    ln: list[str] = []
    div = "=" * 72
    sub = "-" * 72

    # ── Header ────────────────────────────────────────────────
    ln.append(div)
    ln.append(f"UNIVERSE RANKINGS — {date_str}")
    ln.append(div)
    ln.append(
        f"  Universe:      {summary.get('universe_size', 0)} symbols"
    )
    ln.append(
        f"  Breadth:       {breadth_regime} ({breadth_score:.3f})"
    )
    ln.append(
        f"  Mean score:    {summary.get('mean_composite', 0):.3f}"
    )
    ln.append(
        f"  Median score:  {summary.get('median_composite', 0):.3f}"
    )
    ln.append(
        f"  Spread:        {summary.get('spread', 0):.3f}  "
        f"(top {summary.get('top_score', 0):.3f} → "
        f"bottom {summary.get('bottom_score', 0):.3f})"
    )
    if summary.get("mean_agreement") is not None:
        ln.append(
            f"  Mean agree:    {summary['mean_agreement']:.0%}"
        )

    # ── Rankings table ────────────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("RANKINGS")
    ln.append(sub)

    display = filter_top_n(snap, top_n) if top_n else snap

    pillar_present = [c for c in _PILLAR_COLS if c in display.columns]
    short_names = {
        "score_rotation":       "Rot",
        "score_momentum":       "Mom",
        "score_volatility":     "Vol",
        "score_microstructure": "Micro",
        "score_breadth":        "Brdth",
    }

    # Header line
    hdr = f"  {'#':>3}  {'Ticker':<7} {'Comp':>6}"
    for pc in pillar_present:
        hdr += f" {short_names.get(pc, pc[-5:]):>6}"
    hdr += f"  {'Regime':<12} {'1d':>7} {'5d':>7} {'Agree':>5} {'Δ':>3}"
    ln.append(hdr)

    sep = f"  {'───':>3}  {'───────':<7} {'──────':>6}"
    for _ in pillar_present:
        sep += f" {'──────':>6}"
    sep += (
        f"  {'────────────':<12}"
        f" {'───────':>7} {'───────':>7} {'─────':>5} {'───':>3}"
    )
    ln.append(sep)

    for ticker, row in display.iterrows():
        rank_val = int(row.get("rank", 0))
        comp_val = row.get("score_composite", 0)
        regime   = str(row.get("rs_regime", "?"))
        ret_1d   = row.get("ret_1d", np.nan)
        ret_5d   = row.get("ret_5d", np.nan)
        agree    = row.get("pillar_agreement", 0)
        delta    = int(row.get("rank_change", 0))

        line = f"  {rank_val:>3}  {ticker:<7} {comp_val:>6.3f}"
        for pc in pillar_present:
            v = row.get(pc, 0)
            line += f" {v:>6.3f}" if pd.notna(v) else f" {'—':>6}"

        ret_1d_str = f"{ret_1d:>+7.1%}" if pd.notna(ret_1d) else f"{'—':>7}"
        ret_5d_str = f"{ret_5d:>+7.1%}" if pd.notna(ret_5d) else f"{'—':>7}"
        delta_str  = f"{delta:+d}" if delta != 0 else "0"

        line += (
            f"  {regime:<12}"
            f" {ret_1d_str}"
            f" {ret_5d_str}"
            f" {agree:>5.0%}"
            f" {delta_str:>3}"
        )
        ln.append(line)

    # ── Top movers ────────────────────────────────────────────
    if "rank_change" in snap.columns:
        ln.append("")
        ln.append(sub)
        ln.append("TOP MOVERS")
        ln.append(sub)

        risers = snap[snap["rank_change"] > 0].sort_values(
            "rank_change", ascending=False
        )
        fallers = snap[snap["rank_change"] < 0].sort_values(
            "rank_change", ascending=True
        )

        if not risers.empty:
            parts = [
                f"{t} ({int(r):+d})"
                for t, r in risers["rank_change"].head(5).items()
            ]
            ln.append(f"  Risers:   {', '.join(parts)}")
        else:
            ln.append(f"  Risers:   (none)")

        if not fallers.empty:
            parts = [
                f"{t} ({int(r):+d})"
                for t, r in fallers["rank_change"].head(5).items()
            ]
            ln.append(f"  Fallers:  {', '.join(parts)}")
        else:
            ln.append(f"  Fallers:  (none)")

    # ── Regime distribution ───────────────────────────────────
    if "rs_regime" in snap.columns:
        ln.append("")
        ln.append(sub)
        ln.append("REGIME DISTRIBUTION")
        ln.append(sub)

        n_total = len(snap)
        regime_counts = snap["rs_regime"].value_counts()
        for regime in ["leading", "improving", "weakening", "lagging"]:
            cnt = regime_counts.get(regime, 0)
            frac = cnt / n_total if n_total > 0 else 0
            bar = "█" * int(frac * 30)
            ln.append(
                f"  {regime:<12} {cnt:>2} / {n_total}"
                f"  ({frac:>4.0%})  {bar}"
            )

    # ── Pillar agreement ──────────────────────────────────────
    if "pillars_bullish" in snap.columns and pillar_present:
        ln.append("")
        ln.append(sub)
        ln.append("PILLAR AGREEMENT")
        ln.append(sub)

        n_pillars = len(pillar_present)
        for i in range(n_pillars, -1, -1):
            cnt = (snap["pillars_bullish"] == i).sum()
            if cnt > 0:
                matched = list(snap[snap["pillars_bullish"] == i].index)
                ticker_str = ", ".join(matched[:6])
                if len(matched) > 6:
                    ticker_str += f" (+{len(matched) - 6} more)"
                ln.append(
                    f"  {i}/{n_pillars} bullish:"
                    f"  {cnt} symbol{'s' if cnt != 1 else ''}"
                    f"  ({ticker_str})"
                )

    return "\n".join(ln)


##############################################################    
"""
output/reports.py
-----------------
Comprehensive strategy reports that combine rankings, signals,
breadth, gate diagnostics, and (optional) backtest performance
into a single unified report.

Layers
──────
  daily_report()          One-day strategy snapshot
  transition_report()     Recent signal changes
  breadth_section()       Market breadth analysis section
  strategy_overview()     Static strategy rules reference
  performance_report()    Backtest results (when available)
  generate_full_report()  Master — combines all sections

Each function returns a plain-text string.  The master function
concatenates whichever sections are available, so it works
both during live signal generation (no backtest) and after a
historical simulation.

NOTE: compute/breadth.py already exports ``breadth_report()``
      which provides a historical breadth dump with lookback.
      This module's ``breadth_section()`` is a shorter summary
      intended as one section of the unified strategy report.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from output.signals import (
    SignalConfig,
    BUY, HOLD, SELL, NEUTRAL,
    latest_signals,
    signal_changes,
    active_positions,
    signals_summary,
    compute_turnover,
    _count_gates,
)
from output.rankings import (
    latest_rankings,
    rankings_summary,
)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

_DIV = "=" * 76
_SUB = "-" * 76
_THIN = "·" * 76

_REGIME_ICON = {
    "leading":   "🟢",
    "improving": "🔵",
    "weakening": "🟡",
    "lagging":   "🔴",
}

_SIG_ICON = {
    BUY:     "🟢 BUY ",
    HOLD:    "🔵 HOLD",
    SELL:    "🔴 SELL",
    NEUTRAL: "⚪ —   ",
}


# ═══════════════════════════════════════════════════════════════
#  DAILY REPORT
# ═══════════════════════════════════════════════════════════════

def daily_report(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> str:
    """
    Comprehensive single-day strategy report.

    Includes: market context, active positions with gate
    diagnostics, eligible watchlist, exit candidates, recent
    transitions, and turnover metrics.
    """
    if config is None:
        config = SignalConfig()
    if signals_df.empty:
        return "No data available for daily report."

    snap = latest_signals(signals_df)
    if snap.empty:
        return "No data available for daily report."

    summary = signals_summary(signals_df)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )
    has_gates = "sig_confirmed" in snap.columns

    ln: list[str] = []

    # ── Header ────────────────────────────────────────────
    ln.append(_DIV)
    ln.append(f"  DAILY STRATEGY REPORT — {date_str}")
    ln.append(_DIV)

    # Market context
    br_regime = "unknown"
    br_score = 0.0
    if breadth is not None and not breadth.empty:
        br_regime = breadth["breadth_regime"].iloc[-1]
        if "breadth_score" in breadth.columns:
            br_score = breadth["breadth_score"].iloc[-1]

    ln.append("")
    ln.append("  MARKET CONTEXT")
    ln.append(f"  Breadth regime:  {br_regime} ({br_score:.3f})")
    ln.append(
        f"  Positions:       {summary.get('n_active', 0)}"
        f" / {config.max_positions} max"
    )
    ln.append(
        f"  Entry mode:      "
        f"{'sig_confirmed (6 gates)' if has_gates else 'score threshold (fallback)'}"
    )
    ln.append(
        f"  Signal mix:      "
        f"BUY {summary.get('n_buy', 0)}  "
        f"HOLD {summary.get('n_hold', 0)}  "
        f"SELL {summary.get('n_sell', 0)}  "
        f"NEUTRAL {summary.get('n_neutral', 0)}"
    )
    if summary.get("mean_strength") is not None:
        ln.append(
            f"  Mean conviction: {summary['mean_strength']:.3f}"
        )

    # ── Active positions ──────────────────────────────────
    ln.append("")
    ln.append(_SUB)
    ln.append("  ACTIVE POSITIONS")
    ln.append(_SUB)

    active = snap[snap["signal"].isin([BUY, HOLD])]
    if active.empty:
        ln.append("  (no active positions)")
    else:
        for ticker, row in active.iterrows():
            sig = _SIG_ICON.get(row["signal"], row["signal"])
            rank = int(row.get("rank", 0))
            comp = row.get("score_composite", 0)
            strength = row.get("signal_strength", 0)
            regime = str(row.get("rs_regime", "?"))
            r_icon = _REGIME_ICON.get(regime, "")
            ret_1d = row.get("ret_1d", np.nan)
            ret_5d = row.get("ret_5d", np.nan)

            r1 = f"{ret_1d:+.1%}" if pd.notna(ret_1d) else "—"
            r5 = f"{ret_5d:+.1%}" if pd.notna(ret_5d) else "—"

            ln.append(
                f"  {sig}  {ticker:<6}  "
                f"#{rank}  score={comp:.3f}  "
                f"str={strength:.3f}  "
                f"{r_icon} {regime:<11}  "
                f"1d={r1}  5d={r5}"
            )

            # Gate detail for active positions
            if has_gates:
                gates = _format_gate_line(row)
                ln.append(f"           {gates}")

    # ── Gate diagnostics for full universe ────────────────
    if has_gates:
        ln.append("")
        ln.append(_SUB)
        ln.append("  UNIVERSE GATE DIAGNOSTICS")
        ln.append(_SUB)

        for ticker in snap.sort_values("rank").index:
            row = snap.loc[ticker]
            rank = int(row.get("rank", 0))
            conf = "CONF" if row.get("sig_confirmed") == 1 else "—"
            reason = str(row.get("sig_reason", ""))
            gates = _format_gate_line(row)
            ln.append(
                f"  #{rank:<3} {ticker:<6} [{conf:<4}] "
                f"{gates}  {reason}"
            )

    # ── Watchlist ─────────────────────────────────────────
    if "entry_eligible" in snap.columns:
        watchlist = snap[
            (snap["signal"] == NEUTRAL)
            & (snap["entry_eligible"])
        ]
        if not watchlist.empty:
            ln.append("")
            ln.append(_SUB)
            ln.append("  WATCHLIST (eligible, no slot)")
            ln.append(_SUB)
            for ticker, row in watchlist.iterrows():
                ln.append(
                    f"  ○ {ticker:<6}  "
                    f"#{int(row.get('rank', 0))}  "
                    f"score={row.get('score_composite', 0):.3f}  "
                    f"{row.get('rs_regime', '?')}"
                )

    # ── Exit candidates ───────────────────────────────────
    if "exit_triggered" in snap.columns:
        exits = snap[
            (snap["signal"].isin([BUY, HOLD]))
            & (snap["exit_triggered"])
        ]
        if not exits.empty:
            ln.append("")
            ln.append(_SUB)
            ln.append("  ⚠ EXIT CANDIDATES (threshold breached)")
            ln.append(_SUB)
            for ticker, row in exits.iterrows():
                ln.append(
                    f"  ✕ {ticker:<6}  "
                    f"#{int(row.get('rank', 0))}  "
                    f"score={row.get('score_composite', 0):.3f}"
                )

    # ── Recent transitions ────────────────────────────────
    changes = signal_changes(signals_df)
    if not changes.empty:
        recent = changes.tail(10)
        ln.append("")
        ln.append(_SUB)
        ln.append("  RECENT TRANSITIONS (last 10)")
        ln.append(_SUB)
        for (dt, tkr), row in recent.iterrows():
            dt_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            ln.append(
                f"  {dt_str}  {tkr:<6}  "
                f"{row.get('transition', '?')}"
            )

    # ── Turnover ──────────────────────────────────────────
    turnover = compute_turnover(signals_df, lookback=20)
    if not turnover.empty:
        ln.append("")
        ln.append(_SUB)
        ln.append("  TURNOVER")
        ln.append(_SUB)
        total_buys = int(turnover["buys"].sum())
        total_sells = int(turnover["sells"].sum())
        avg_active = turnover["active"].mean()
        roll_turn = turnover["rolling_turnover"].iloc[-1]
        ln.append(f"  Total entries:    {total_buys}")
        ln.append(f"  Total exits:      {total_sells}")
        ln.append(f"  Avg active:       {avg_active:.1f}")
        ln.append(f"  Rolling turnover: {roll_turn:.3f} (20d)")

    ln.append("")
    ln.append(_DIV)
    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  BREADTH SECTION
# ═══════════════════════════════════════════════════════════════

def breadth_section(
    breadth: pd.DataFrame | None,
) -> str:
    """
    Market breadth analysis section for the unified report.

    This is a *report section formatter* — a short summary of
    the current breadth state suitable for embedding in the
    strategy report.  For a detailed historical breadth dump
    with configurable lookback, use ``compute.breadth.breadth_report()``.
    """
    if breadth is None or breadth.empty:
        return "No breadth data available."

    ln: list[str] = []
    ln.append(_SUB)
    ln.append("  BREADTH ANALYSIS")
    ln.append(_SUB)

    regime = breadth["breadth_regime"].iloc[-1]
    score = (
        breadth["breadth_score"].iloc[-1]
        if "breadth_score" in breadth.columns else 0
    )
    smooth = (
        breadth["breadth_score_smooth"].iloc[-1]
        if "breadth_score_smooth" in breadth.columns else score
    )

    ln.append(f"  Current regime:  {regime}")
    ln.append(f"  Raw score:       {score:.3f}")
    ln.append(f"  Smoothed score:  {smooth:.3f}")

    # Regime history (last 5 unique)
    if "breadth_regime" in breadth.columns:
        regimes = breadth["breadth_regime"].dropna()
        if len(regimes) > 0:
            shifted = regimes != regimes.shift(1)
            transitions = regimes[shifted].tail(5)
            if len(transitions) > 0:
                parts = []
                for dt, r in transitions.items():
                    d = (
                        dt.strftime("%m-%d")
                        if hasattr(dt, "strftime") else str(dt)
                    )
                    parts.append(f"{d}: {r}")
                ln.append(f"  Recent shifts:   {' → '.join(parts)}")

    # Score distribution
    if "breadth_score" in breadth.columns:
        bs = breadth["breadth_score"].dropna()
        if len(bs) > 20:
            ln.append(f"  Score range:     "
                      f"[{bs.min():.3f}, {bs.max():.3f}]")
            ln.append(f"  20d mean:        "
                      f"{bs.tail(20).mean():.3f}")

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  STRATEGY OVERVIEW
# ═══════════════════════════════════════════════════════════════

def strategy_overview(
    config: SignalConfig | None = None,
) -> str:
    """
    Static description of the strategy rules and parameters.
    Useful as a reference appendix in any report.
    """
    if config is None:
        config = SignalConfig()

    has_gates = True  # describe full system

    ln: list[str] = []
    ln.append(_DIV)
    ln.append("  STRATEGY OVERVIEW")
    ln.append(_DIV)

    ln.append("")
    ln.append("  Per-Ticker Quality Gates (strategy/signals.py)")
    ln.append(_THIN)
    ln.append("  1. Score threshold  — score_adjusted ≥ entry_min")
    ln.append("  2. RS regime        — stock in leading/improving")
    ln.append("  3. Sector regime    — sector tide favourable")
    ln.append("  4. Breadth regime   — market not weak")
    ln.append("  5. Momentum streak  — N consecutive days > 0.5")
    ln.append("  6. Cooldown         — not recently exited")
    ln.append("  All six must pass → sig_confirmed = 1")

    ln.append("")
    ln.append("  Portfolio-Level Signals (output/signals.py)")
    ln.append(_THIN)
    ln.append(
        f"  Entry:   sig_confirmed AND rank ≤ "
        f"{config.entry_rank_max}"
    )
    ln.append(
        f"  Exit:    rank > {config.exit_rank_max} OR "
        f"score < {config.exit_score_min}"
    )
    ln.append(
        f"  Max positions:   {config.max_positions}"
    )
    ln.append(
        f"  Rank hysteresis: enter ≤ {config.entry_rank_max}, "
        f"exit > {config.exit_rank_max}"
    )
    ln.append(
        f"  Breadth breaker: "
        f"{config.breadth_bearish_action} when "
        f"regime ∈ {config.breadth_bearish}"
    )

    ln.append("")
    ln.append("  Signal Strength Weights")
    ln.append(_THIN)
    ln.append(f"  Composite score:  {config.w_score:.0%}")
    ln.append(f"  Rank percentile:  {config.w_rank:.0%}")
    ln.append(f"  Pillar agreement: {config.w_agreement:.0%}")
    ln.append(f"  Regime quality:   {config.w_regime:.0%}")
    ln.append(f"  Breadth quality:  {config.w_breadth:.0%}")

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE REPORT
# ═══════════════════════════════════════════════════════════════

def performance_report(
    backtest_result,
) -> str:
    """
    Format backtest results into a text report section.

    Parameters
    ----------
    backtest_result
        A ``BacktestResult`` from ``portfolio.backtest``.
        If None, returns placeholder text.
    """
    if backtest_result is None:
        return "No backtest results available."

    m = backtest_result.metrics
    if not m:
        return "No performance metrics computed."

    ln: list[str] = []
    ln.append(_DIV)
    ln.append("  BACKTEST PERFORMANCE")
    ln.append(_DIV)

    ln.append("")
    ln.append("  Returns")
    ln.append(_THIN)
    ln.append(
        f"  Total return:     "
        f"{m.get('total_return', 0):+.2%}"
    )
    ln.append(
        f"  CAGR:             "
        f"{m.get('cagr', 0):+.2%}"
    )
    ln.append(
        f"  Volatility (ann): "
        f"{m.get('annual_volatility', 0):.2%}"
    )

    ln.append("")
    ln.append("  Risk-Adjusted")
    ln.append(_THIN)
    ln.append(
        f"  Sharpe ratio:     "
        f"{m.get('sharpe_ratio', 0):.3f}"
    )
    ln.append(
        f"  Sortino ratio:    "
        f"{m.get('sortino_ratio', 0):.3f}"
    )
    ln.append(
        f"  Calmar ratio:     "
        f"{m.get('calmar_ratio', 0):.3f}"
    )

    ln.append("")
    ln.append("  Drawdown")
    ln.append(_THIN)
    ln.append(
        f"  Max drawdown:     "
        f"{m.get('max_drawdown', 0):.2%}"
    )
    ln.append(
        f"  Max DD duration:  "
        f"{m.get('max_dd_duration', 0)} days"
    )
    ln.append(
        f"  Current DD:       "
        f"{m.get('current_drawdown', 0):.2%}"
    )

    ln.append("")
    ln.append("  Trading")
    ln.append(_THIN)
    ln.append(
        f"  Total trades:     "
        f"{m.get('total_trades', 0)}"
    )
    ln.append(
        f"  Win rate:         "
        f"{m.get('win_rate', 0):.1%}"
    )
    ln.append(
        f"  Profit factor:    "
        f"{m.get('profit_factor', 0):.2f}"
    )
    ln.append(
        f"  Avg win / loss:   "
        f"{m.get('avg_win', 0):+.2%} / "
        f"{m.get('avg_loss', 0):+.2%}"
    )
    ln.append(
        f"  Total commission: "
        f"${m.get('total_commission', 0):,.2f}"
    )

    ln.append("")
    ln.append("  Capital")
    ln.append(_THIN)
    ln.append(
        f"  Initial:          "
        f"${m.get('initial_capital', 0):,.2f}"
    )
    ln.append(
        f"  Final:            "
        f"${m.get('final_capital', 0):,.2f}"
    )
    ln.append(
        f"  Peak:             "
        f"${m.get('peak_capital', 0):,.2f}"
    )

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_full_report(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
    backtest_result=None,
    include_strategy: bool = True,
) -> str:
    """
    Combine all report sections into one comprehensive document.

    Includes whichever sections have data: daily signals are
    always included, breadth if provided, backtest performance
    if a result object is passed, and the strategy overview
    if requested.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``compute_all_signals()``.
    breadth : pd.DataFrame or None
        Breadth data.
    config : SignalConfig or None
        Portfolio-level config.
    backtest_result : BacktestResult or None
        Output of ``run_backtest()``.
    include_strategy : bool
        Whether to append the strategy rules reference.

    Returns
    -------
    str
        Full text report.
    """
    if config is None:
        config = SignalConfig()

    sections: list[str] = []

    # Daily snapshot
    sections.append(daily_report(signals_df, breadth, config))

    # Breadth detail
    if breadth is not None and not breadth.empty:
        sections.append(breadth_section(breadth))

    # Backtest performance
    if backtest_result is not None:
        sections.append(performance_report(backtest_result))

    # Strategy reference
    if include_strategy:
        sections.append(strategy_overview(config))

    return "\n\n".join(sections)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _format_gate_line(row: pd.Series) -> str:
    """Format per-ticker gate status as a compact string."""
    gate_cols = [
        ("sig_regime_ok",   "Reg"),
        ("sig_sector_ok",   "Sec"),
        ("sig_breadth_ok",  "Brd"),
        ("sig_momentum_ok", "Mom"),
        ("sig_in_cooldown", "CD"),
    ]

    parts: list[str] = []
    for col, label in gate_cols:
        if col not in row.index:
            continue
        val = row[col]
        if col == "sig_in_cooldown":
            icon = "✕" if val else "✓"
        else:
            icon = "✓" if val else "✕"
        parts.append(f"{icon}{label}")

    return "  ".join(parts) if parts else "—"


"""
output/signals.py
-----------------
Portfolio-level trade signal generation.

Layers on top of ``strategy/signals.py`` (per-ticker quality
gates) and ``output/rankings.py`` (cross-sectional rankings)
to produce final portfolio signals: BUY, HOLD, SELL, NEUTRAL.

Architecture
────────────
  strategy/signals.py answers "Is this ticker trade-worthy?"
    · Six per-ticker gates: regime, sector, breadth, momentum,
      cooldown, score threshold
    · Produces: sig_confirmed (0/1), sig_exit (0/1),
      sig_position_pct, gate diagnostics

  output/rankings.py answers "Where does this ticker rank?"
    · Cross-sectional ranking by composite score
    · Produces: rank, pillar_agreement, universe_size

  output/signals.py answers "Which tickers do we hold?"     ← this file
    · Uses sig_confirmed for entry qualification
    · Adds cross-sectional rank filter with hysteresis
    · Enforces position limits
    · Portfolio-level breadth circuit breaker
    · Conviction scoring for downstream sizing

  When strategy/signals.py has NOT been run (sig_confirmed
  absent from the panel), entry falls back to:
    score_composite ≥ entry_score_min

Signal Types
────────────
  BUY      Enter new position
  HOLD     Maintain existing position
  SELL     Exit position
  NEUTRAL  No position

Entry (AND — all required)
──────────────────────────
  sig_confirmed == 1   (or score ≥ entry_score_min as fallback)
  rank ≤ entry_rank_max
  slots available      (< max_positions)
  breadth not bearish

Exit (OR — any one fires)
─────────────────────────
  rank > exit_rank_max
  score_composite < exit_score_min
  breadth bearish + exit_all mode

Hysteresis
──────────
  Rank band: enter at rank ≤ 5, exit only at rank > 8.
  A symbol entering at rank 3 stays through rank 7 without
  churning.

  Per-ticker hysteresis (cooldown, momentum streak) is
  handled by strategy/signals.py through sig_confirmed.
  The two layers stack: a ticker must survive both the
  per-ticker quality bar AND the cross-sectional rank bar.

Signal Strength
───────────────
  A 0–1 conviction score blending:
    composite score    30%   — raw quality
    rank percentile    20%   — position in universe
    pillar agreement   20%   — breadth of confirmation
    regime quality     15%   — RS regime desirability
    breadth quality    15%   — market health

Pipeline
────────
  ranked_panel (with optional sig_* columns from strategy/)
       ↓
  check_entry_eligible()     — boolean per row
       ↓
  check_exit_triggered()     — boolean per row
       ↓
  generate_signals()         — stateful BUY/HOLD/SELL/NEUTRAL
       ↓
  compute_signal_strength()  — 0–1 conviction score
       ↓
  compute_all_signals()      — master orchestrator
       ↓
  latest_signals()           — single-day snapshot
  signal_changes()           — entries / exits / transitions
  signal_history()           — single ticker over time
  active_positions()         — currently held symbols
  compute_turnover()         — entry/exit frequency
  signals_summary()          — summary statistics dict
  signals_report()           — formatted text report
"""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

BUY     = "BUY"
HOLD    = "HOLD"
SELL    = "SELL"
NEUTRAL = "NEUTRAL"

_REGIME_QUALITY: dict[str, float] = {
    "leading":    1.00,
    "improving":  0.75,
    "weakening":  0.25,
    "lagging":    0.00,
}

_BREADTH_QUALITY: dict[str, float] = {
    "strong":   1.00,
    "healthy":  0.80,
    "neutral":  0.50,
    "caution":  0.30,
    "weak":     0.10,
    "critical": 0.00,
}


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class SignalConfig:
    """
    Portfolio-level signal thresholds.

    Per-ticker quality thresholds (regime, sector, momentum,
    cooldown, score) live in ``SIGNAL_PARAMS`` and are enforced
    by ``strategy/signals.py``.  This config controls only the
    cross-sectional and portfolio layers.
    the portfolio-level allocator. It takes the full cross-sectional panel (after every ticker has been scored and gated) and 
    answers "which of the trade-worthy tickers do we actually hold?" through rank filtering with hysteresis, position limits, and 
    a portfolio-level breadth circuit breaker.
    """

    # ── Rank thresholds (hysteresis band) ─────────────────
    entry_rank_max: int = 8          # was 5 — enter if ranked in top 8
    exit_rank_max:  int = 20         # was 8 — only exit if falls below 20

    # ── Score thresholds ──────────────────────────────────
    #    exit_score_min always applies as an OR exit trigger.
    #    entry_score_min is used only when sig_confirmed is
    #    absent (strategy/signals.py was not run).
    entry_score_min: float = 0.40
    exit_score_min:  float = 0.30

    # ── Breadth circuit breaker ───────────────────────────
    breadth_bearish:        tuple[str, ...] = ("weak", "critical")
    breadth_bearish_action: str = "reduce"    # "reduce" or "exit_all"

    # ── Position limits ───────────────────────────────────
    max_positions: int = 8

    # ── Signal strength weights ───────────────────────────
    w_score:     float = 0.30
    w_rank:      float = 0.20
    w_agreement: float = 0.20
    w_regime:    float = 0.15
    w_breadth:   float = 0.15


# ═══════════════════════════════════════════════════════════════
#  ENTRY / EXIT ELIGIBILITY
# ═══════════════════════════════════════════════════════════════

def check_entry_eligible(
    ranked: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.Series:
    """
    Boolean mask: True where entry criteria are met.

    When ``sig_confirmed`` is present (strategy/signals.py
    was run on each ticker before ranking):

        sig_confirmed == 1  AND  rank ≤ entry_rank_max

    Fallback (``sig_confirmed`` absent):

        score_composite ≥ entry_score_min  AND  rank ≤ entry_rank_max
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.Series(dtype=bool)

    rank_ok = ranked["rank"] <= config.entry_rank_max

    if "sig_confirmed" in ranked.columns:
        ticker_ok = ranked["sig_confirmed"] == 1
    else:
        ticker_ok = ranked["score_composite"] >= config.entry_score_min

    return rank_ok & ticker_ok


def check_exit_triggered(
    ranked: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.Series:
    """
    Boolean mask: True where any exit threshold is breached.

    Triggers (OR):
      rank              > exit_rank_max
      score_composite   < exit_score_min
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.Series(dtype=bool)

    breach = ranked["rank"] > config.exit_rank_max
    breach = breach | (
        ranked["score_composite"] < config.exit_score_min
    )

    return breach


# ═══════════════════════════════════════════════════════════════
#  STATEFUL SIGNAL GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_signals(
    ranked: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Generate BUY / HOLD / SELL / NEUTRAL signals with hysteresis.

    Processes the ranked panel date-by-date, maintaining a set
    of held positions.

    For held positions each day:
      · Force exit if breadth bearish + exit_all mode
      · Exit if any exit trigger fires (rank or score)
      · Otherwise HOLD

    For non-held tickers each day:
      · Skip if breadth bearish (both modes block new entries)
      · BUY if entry eligible and slots available
      · Otherwise NEUTRAL

    New entries are prioritised by composite score (highest
    first) when more candidates than available slots.

    Parameters
    ----------
    ranked : pd.DataFrame
        MultiIndex (date, ticker) panel from
        ``compute_all_rankings()``, optionally containing
        ``sig_confirmed`` / ``sig_exit`` columns from
        ``strategy/signals.py``.
    breadth : pd.DataFrame or None
        Breadth data with ``breadth_regime`` column, indexed
        by date.
    config : SignalConfig or None
        Portfolio-level thresholds.

    Returns
    -------
    pd.DataFrame
        Input panel with ``signal``, ``entry_eligible``,
        ``exit_triggered``, and ``in_position`` columns added.
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.DataFrame()

    # Pre-compute masks over the entire panel
    entry_mask = check_entry_eligible(ranked, config)
    exit_mask  = check_exit_triggered(ranked, config)

    dates = ranked.index.get_level_values("date").unique().sort_values()

    held: set[str]                = set()
    signals: dict[tuple, str]     = {}

    for date in dates:

        # ── Day slices ────────────────────────────────────
        try:
            day_data  = ranked.xs(date, level="date")
            day_entry = entry_mask.xs(date, level="date")
            day_exit  = exit_mask.xs(date, level="date")
        except KeyError:
            continue

        day_tickers = set(day_data.index.tolist())

        # ── Breadth circuit breaker ───────────────────────
        breadth_is_bearish = False
        if (
            breadth is not None
            and "breadth_regime" in breadth.columns
            and date in breadth.index
        ):
            br = breadth.loc[date, "breadth_regime"]
            if isinstance(br, pd.Series):
                br = br.iloc[0]
            breadth_is_bearish = br in config.breadth_bearish

        day_signals: dict[str, str] = {}
        sells: set[str] = set()

        # ── 1. Process held positions ─────────────────────
        for ticker in list(held):
            if ticker not in day_tickers:
                # Ticker dropped from panel (no data today)
                held.discard(ticker)
                continue

            force_exit = (
                breadth_is_bearish
                and config.breadth_bearish_action == "exit_all"
            )

            if force_exit or day_exit.loc[ticker]:
                day_signals[ticker] = SELL
                sells.add(ticker)
            else:
                day_signals[ticker] = HOLD

        held -= sells

        # ── 2. New entries ────────────────────────────────
        #    Blocked when breadth is bearish (both modes).
        if not breadth_is_bearish:
            slots = config.max_positions - len(held)
            if slots > 0:
                candidates: list[tuple[str, float]] = []
                for ticker in day_tickers:
                    if ticker in day_signals:
                        continue          # already HOLD or SELL
                    if day_entry.loc[ticker]:
                        score = day_data.loc[
                            ticker, "score_composite"
                        ]
                        candidates.append((ticker, score))

                # Best composite score gets priority
                candidates.sort(key=lambda x: x[1], reverse=True)

                for ticker, _ in candidates[: max(0, slots)]:
                    day_signals[ticker] = BUY
                    held.add(ticker)

        # ── 3. Remainder → NEUTRAL ───────────────────────
        for ticker in day_tickers:
            if ticker not in day_signals:
                day_signals[ticker] = NEUTRAL

        # ── Store ─────────────────────────────────────────
        for ticker, sig in day_signals.items():
            signals[(date, ticker)] = sig

    # ── Assemble result ───────────────────────────────────
    sig_series = pd.Series(signals, name="signal")
    sig_series.index = pd.MultiIndex.from_tuples(
        sig_series.index, names=["date", "ticker"],
    )

    result = ranked.copy()
    result["signal"]         = sig_series
    result["signal"]         = result["signal"].fillna(NEUTRAL)
    result["entry_eligible"] = entry_mask
    result["exit_triggered"] = exit_mask
    result["in_position"]    = result["signal"].isin([BUY, HOLD])

    return result


# ═══════════════════════════════════════════════════════════════
#  SIGNAL STRENGTH  (CONVICTION)
# ═══════════════════════════════════════════════════════════════

def compute_signal_strength(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Compute 0–1 conviction score for each row.

    Components and default weights:
      score_composite    30%  — raw composite quality
      rank_factor        20%  — position in universe
      pillar_agreement   20%  — breadth of confirmation
      regime_quality     15%  — RS regime desirability
      breadth_quality    15%  — market breadth health

    Strength is zeroed for SELL and NEUTRAL signals.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``generate_signals()``.
    breadth : pd.DataFrame or None
        Breadth data for the breadth quality component.
    config : SignalConfig or None
        Weights.

    Returns
    -------
    pd.DataFrame
        Same frame with ``signal_strength`` column added.
    """
    if config is None:
        config = SignalConfig()
    if signals_df.empty:
        return signals_df

    result = signals_df.copy()
    active = result["signal"].isin([BUY, HOLD])

    # ── 1. Score factor (already 0–1) ─────────────────────
    score_f = result["score_composite"].clip(0, 1)

    # ── 2. Rank factor ────────────────────────────────────
    usize  = result["universe_size"].clip(lower=2)
    rank_f = (1.0 - (result["rank"] - 1) / (usize - 1)).clip(0, 1)

    # ── 3. Agreement factor ───────────────────────────────
    if "pillar_agreement" in result.columns:
        agree_f = result["pillar_agreement"].fillna(0.5)
    else:
        agree_f = pd.Series(0.5, index=result.index)

    # ── 4. Regime factor ──────────────────────────────────
    if "rs_regime" in result.columns:
        regime_f = (
            result["rs_regime"].map(_REGIME_QUALITY).fillna(0.5)
        )
    else:
        regime_f = pd.Series(0.5, index=result.index)

    # ── 5. Breadth factor ─────────────────────────────────
    breadth_f = pd.Series(0.5, index=result.index)
    if breadth is not None and "breadth_regime" in breadth.columns:
        b_quality = breadth["breadth_regime"].map(_BREADTH_QUALITY)
        dates     = result.index.get_level_values("date")
        breadth_f = b_quality.reindex(dates).fillna(0.5)
        breadth_f.index = result.index

    # ── Weighted blend ────────────────────────────────────
    strength = (
        config.w_score     * score_f
        + config.w_rank    * rank_f
        + config.w_agreement * agree_f
        + config.w_regime  * regime_f
        + config.w_breadth * breadth_f
    ).clip(0, 1)

    # Zero out non-active signals
    result["signal_strength"] = strength.where(active, 0.0)

    return result


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT / HISTORY EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def latest_signals(
    signals_df: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Extract one day's signals as a flat ticker-indexed DataFrame.

    Sorted: BUY first, then HOLD, then NEUTRAL, then SELL,
    each group sorted by rank.
    """
    if signals_df.empty:
        return pd.DataFrame()

    dates = signals_df.index.get_level_values("date").unique()

    if date is not None:
        if date not in dates:
            prior = dates[dates <= date]
            if prior.empty:
                return pd.DataFrame()
            date = prior[-1]
    else:
        date = dates[-1]

    snap = signals_df.xs(date, level="date").copy()
    snap["date"] = date

    _priority = {BUY: 0, HOLD: 1, NEUTRAL: 2, SELL: 3}
    snap["_sort"] = snap["signal"].map(_priority).fillna(4)
    snap = snap.sort_values(["_sort", "rank"]).drop(columns="_sort")

    return snap


def signal_changes(
    signals_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract rows where the signal changed from the prior day.

    Adds ``prev_signal`` and ``transition`` columns
    (e.g. ``NEUTRAL → BUY``, ``HOLD → SELL``).
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return pd.DataFrame()

    sig_wide  = signals_df["signal"].unstack(level="ticker")
    prev_wide = sig_wide.shift(1)

    changed_wide = sig_wide != prev_wide
    # First date always "changed" — exclude
    changed_wide.iloc[0] = False

    changed_long = changed_wide.stack()
    changed_long.name = "changed"
    changed_long.index.names = ["date", "ticker"]

    mask = changed_long[changed_long]
    if mask.empty:
        return pd.DataFrame()

    changes = signals_df.loc[mask.index].copy()

    prev_long = prev_wide.stack()
    prev_long.name = "prev_signal"
    prev_long.index.names = ["date", "ticker"]

    changes = changes.join(prev_long)
    changes["transition"] = (
        changes["prev_signal"].astype(str) + " → "
        + changes["signal"].astype(str)
    )

    return changes


def signal_history(
    signals_df: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Extract one ticker's signal history over time."""
    if signals_df.empty:
        return pd.DataFrame()

    tickers = signals_df.index.get_level_values("ticker").unique()
    if ticker not in tickers:
        return pd.DataFrame()

    return signals_df.xs(ticker, level="ticker").copy()


def active_positions(
    signals_df: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> list[str]:
    """
    Return tickers with BUY or HOLD on a given date.

    Sorted by rank (best first).
    """
    snap = latest_signals(signals_df, date)
    if snap.empty:
        return []

    active = snap[snap["signal"].isin([BUY, HOLD])]
    return active.index.tolist()


# ═══════════════════════════════════════════════════════════════
#  TURNOVER ANALYSIS
# ═══════════════════════════════════════════════════════════════

def compute_turnover(
    signals_df: pd.DataFrame,
    lookback: int = 20,
) -> pd.DataFrame:
    """
    Compute daily position turnover.

    turnover = (entries + exits) / universe_size per day,
    smoothed over a rolling window.

    Returns a date-indexed DataFrame with buys, sells, active
    positions count, daily turnover, and rolling turnover.
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return pd.DataFrame()

    dates = (
        signals_df.index.get_level_values("date")
        .unique().sort_values()
    )

    buys = signals_df[signals_df["signal"] == BUY].groupby(
        level="date",
    ).size()
    sells = signals_df[signals_df["signal"] == SELL].groupby(
        level="date",
    ).size()
    active = signals_df[
        signals_df["signal"].isin([BUY, HOLD])
    ].groupby(level="date").size()
    universe = signals_df.groupby(level="date").size()

    buys     = buys.reindex(dates, fill_value=0)
    sells    = sells.reindex(dates, fill_value=0)
    active   = active.reindex(dates, fill_value=0)
    universe = universe.reindex(dates, fill_value=1)

    turnover = pd.DataFrame({
        "buys":            buys,
        "sells":           sells,
        "active":          active,
        "daily_turnover":  (buys + sells) / universe.clip(lower=1),
    }, index=dates)

    turnover["rolling_turnover"] = (
        turnover["daily_turnover"]
        .rolling(lookback, min_periods=1)
        .mean()
    )

    return turnover


# ═══════════════════════════════════════════════════════════════
#  SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════

def signals_summary(signals_df: pd.DataFrame) -> dict:
    """
    Summary statistics for the latest day's signals.

    Returns dict with: date, n_buy, n_hold, n_sell, n_neutral,
    n_active, positions, mean_strength, total_strength,
    strongest, weakest, regime_mix.
    """
    snap = latest_signals(signals_df)
    if snap.empty:
        return {}

    active_snap = snap[snap["signal"].isin([BUY, HOLD])]

    summary: dict = {
        "date":      (
            snap["date"].iloc[0] if "date" in snap.columns
            else None
        ),
        "n_buy":     int((snap["signal"] == BUY).sum()),
        "n_hold":    int((snap["signal"] == HOLD).sum()),
        "n_sell":    int((snap["signal"] == SELL).sum()),
        "n_neutral": int((snap["signal"] == NEUTRAL).sum()),
        "n_active":  len(active_snap),
        "positions": active_snap.index.tolist(),
    }

    if "signal_strength" in snap.columns and not active_snap.empty:
        summary["mean_strength"]  = float(
            active_snap["signal_strength"].mean()
        )
        summary["total_strength"] = float(
            active_snap["signal_strength"].sum()
        )
        summary["strongest"] = active_snap[
            "signal_strength"
        ].idxmax()
        summary["weakest"] = active_snap[
            "signal_strength"
        ].idxmin()

    if "rs_regime" in active_snap.columns and not active_snap.empty:
        summary["regime_mix"] = (
            active_snap["rs_regime"].value_counts().to_dict()
        )

    return summary


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_signals(
    ranked: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Full portfolio signal pipeline.

    Parameters
    ----------
    ranked : pd.DataFrame
        Output of ``compute_all_rankings()``, optionally with
        ``sig_confirmed`` columns from ``strategy/signals.py``.
    breadth : pd.DataFrame or None
        Breadth data for circuit breaker and strength scoring.
    config : SignalConfig or None
        Portfolio-level thresholds.

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) panel with signal, strength,
        eligibility, and position columns appended.
    """
    if config is None:
        config = SignalConfig()

    signals = generate_signals(ranked, breadth, config)
    if signals.empty:
        return pd.DataFrame()

    signals = compute_signal_strength(signals, breadth, config)

    return signals


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def signals_report(
    signals_df: pd.DataFrame,
    breadth_regime: str = "unknown",
    breadth_score: float = 0.0,
    config: SignalConfig | None = None,
) -> str:
    """
    Formatted text report of the latest signals.

    Includes: header summary, active positions table, new
    entries, exits with reasons, watchlist, per-ticker gate
    diagnostics (when available), and config reference.
    """
    if config is None:
        config = SignalConfig()

    if signals_df.empty:
        return "No signal data available."

    snap = latest_signals(signals_df)
    if snap.empty:
        return "No signal data available."

    summary = signals_summary(signals_df)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )

    # Track whether per-ticker gates are present
    has_gates = "sig_confirmed" in snap.columns

    ln: list[str] = []
    div = "=" * 72
    sub = "-" * 72

    # ── Header ────────────────────────────────────────────
    ln.append(div)
    ln.append(f"TRADE SIGNALS — {date_str}")
    ln.append(div)
    ln.append(
        f"  Breadth:       {breadth_regime} ({breadth_score:.3f})"
    )
    ln.append(
        f"  Positions:     {summary.get('n_active', 0)} / "
        f"{config.max_positions} max"
    )
    ln.append(
        f"  BUY: {summary.get('n_buy', 0)}  "
        f"HOLD: {summary.get('n_hold', 0)}  "
        f"SELL: {summary.get('n_sell', 0)}  "
        f"NEUTRAL: {summary.get('n_neutral', 0)}"
    )
    if summary.get("mean_strength") is not None:
        ln.append(
            f"  Mean strength: {summary['mean_strength']:.3f}"
        )
    ln.append(
        f"  Ticker gates:  "
        f"{'active (strategy/signals.py)' if has_gates else 'fallback (score only)'}"
    )

    # ── Active positions ──────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("ACTIVE POSITIONS")
    ln.append(sub)

    active = snap[snap["signal"].isin([BUY, HOLD])]
    if active.empty:
        ln.append("  (no active positions)")
    else:
        hdr = (
            f"  {'Signal':<8} {'Ticker':<7} {'#':>3} "
            f"{'Comp':>6} {'Str':>5} {'Regime':<12} "
            f"{'1d':>7} {'5d':>7}"
        )
        if has_gates:
            hdr += f" {'Gates':>5}"
        ln.append(hdr)
        sep_line = (
            f"  {'────────':<8} {'───────':<7} {'───':>3} "
            f"{'──────':>6} {'─────':>5} {'────────────':<12} "
            f"{'───────':>7} {'───────':>7}"
        )
        if has_gates:
            sep_line += f" {'─────':>5}"
        ln.append(sep_line)

        for ticker, row in active.iterrows():
            sig      = row["signal"]
            rank_val = int(row.get("rank", 0))
            comp     = row.get("score_composite", 0)
            strength = row.get("signal_strength", 0)
            regime   = str(row.get("rs_regime", "?"))
            ret_1d   = row.get("ret_1d", np.nan)
            ret_5d   = row.get("ret_5d", np.nan)

            r1 = (
                f"{ret_1d:>+7.1%}" if pd.notna(ret_1d)
                else f"{'—':>7}"
            )
            r5 = (
                f"{ret_5d:>+7.1%}" if pd.notna(ret_5d)
                else f"{'—':>7}"
            )

            line = (
                f"  {sig:<8} {ticker:<7} {rank_val:>3} "
                f"{comp:>6.3f} {strength:>5.3f} {regime:<12} "
                f"{r1} {r5}"
            )
            if has_gates:
                gates_passed = _count_gates(row)
                line += f" {gates_passed:>5}"
            ln.append(line)

    # ── New entries ───────────────────────────────────────
    new_buys = snap[snap["signal"] == BUY]
    if not new_buys.empty:
        ln.append("")
        ln.append(sub)
        ln.append("NEW ENTRIES")
        ln.append(sub)
        for ticker, row in new_buys.iterrows():
            reason = row.get("sig_reason", "")
            ln.append(
                f"  → BUY  {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"strength={row.get('signal_strength', 0):.3f}  "
                f"{row.get('rs_regime', '?')}"
                f"{f'  ({reason})' if reason else ''}"
            )

    # ── Exits ─────────────────────────────────────────────
    exits = snap[snap["signal"] == SELL]
    if not exits.empty:
        ln.append("")
        ln.append(sub)
        ln.append("EXITS")
        ln.append(sub)
        for ticker, row in exits.iterrows():
            reasons: list[str] = []
            if row.get("rank", 0) > config.exit_rank_max:
                reasons.append(
                    f"rank {int(row['rank'])} > "
                    f"{config.exit_rank_max}"
                )
            if (
                row.get("score_composite", 1)
                < config.exit_score_min
            ):
                reasons.append(
                    f"score {row['score_composite']:.3f} < "
                    f"{config.exit_score_min:.3f}"
                )
            reason_str = (
                ", ".join(reasons) if reasons else "forced exit"
            )

            ln.append(
                f"  ✕ SELL {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"({reason_str})"
            )

    # ── Gate diagnostics (when strategy/signals.py active) ─
    if has_gates:
        ln.append("")
        ln.append(sub)
        ln.append("PER-TICKER GATE DIAGNOSTICS")
        ln.append(sub)
        gate_cols = [
            ("sig_regime_ok",   "Regime"),
            ("sig_sector_ok",   "Sector"),
            ("sig_breadth_ok",  "Breadth"),
            ("sig_momentum_ok", "Momentum"),
            ("sig_in_cooldown", "Cooldown"),
        ]
        available_gates = [
            (col, label) for col, label in gate_cols
            if col in snap.columns
        ]

        for ticker, row in snap.sort_values("rank").iterrows():
            flags: list[str] = []
            for col, label in available_gates:
                val = row.get(col, None)
                if val is None:
                    continue
                if col == "sig_in_cooldown":
                    flags.append(
                        f"{'✕' if val else '✓'} {label}"
                    )
                else:
                    flags.append(
                        f"{'✓' if val else '✕'} {label}"
                    )

            conf = (
                "CONF" if row.get("sig_confirmed") == 1
                else "—"
            )
            ln.append(
                f"  #{int(row.get('rank', 0)):<3} "
                f"{ticker:<7} [{conf:<4}] "
                f"{'  '.join(flags)}"
            )

    # ── Watchlist ─────────────────────────────────────────
    if "entry_eligible" in snap.columns:
        neutral_elig = snap[
            (snap["signal"] == NEUTRAL) & (snap["entry_eligible"])
        ]
    else:
        neutral_elig = pd.DataFrame()

    if not neutral_elig.empty:
        ln.append("")
        ln.append(sub)
        ln.append("WATCHLIST (eligible but no slot)")
        ln.append(sub)
        for ticker, row in neutral_elig.iterrows():
            ln.append(
                f"  ○ {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"{row.get('rs_regime', '?')}"
            )

    # ── Config reference ──────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("SIGNAL CONFIG (portfolio level)")
    ln.append(sub)
    ln.append(
        f"  Entry:  rank ≤ {config.entry_rank_max}"
        + (
            f", sig_confirmed == 1"
            if has_gates
            else f", score ≥ {config.entry_score_min:.2f}"
        )
    )
    ln.append(
        f"  Exit:   rank > {config.exit_rank_max}, "
        f"score < {config.exit_score_min:.2f}"
    )
    ln.append(
        f"  Max positions: {config.max_positions}  "
        f"Breadth bearish: {config.breadth_bearish_action}"
    )

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _count_gates(row: pd.Series) -> str:
    """Count passed / total per-ticker gates for display."""
    gate_cols = [
        "sig_regime_ok", "sig_sector_ok", "sig_breadth_ok",
        "sig_momentum_ok",
    ]
    cooldown_col = "sig_in_cooldown"

    total = 0
    passed = 0

    for col in gate_cols:
        if col in row.index:
            total += 1
            if row[col]:
                passed += 1

    if cooldown_col in row.index:
        total += 1
        if not row[cooldown_col]:
            passed += 1

    return f"{passed}/{total}" if total > 0 else "—"


####################################################
    
"""    
PIPELINE :
----------------------------
"""
"""
pipeline/orchestrator.py
------------------------
Top-level coordinator for the CASH system.

Ties together every phase of analysis into a single call or
a phase-by-phase interactive workflow:

  Phase 0 — Data Loading
      Load OHLCV for all tickers + benchmark from the
      configured data source.

  Phase 1 — Universe-Level Computations
      Breadth indicators, sector relative strength, and
      breadth-to-pillar-score mapping.  These feed into
      the per-ticker pipeline as contextual inputs.

  Phase 2 — Per-Ticker Pipeline
      Run ``runner.run_batch()`` which chains indicators →
      RS → scoring → sector merge → signals for each ticker.

  Phase 3 — Cross-Sectional Analysis
      Rankings across the scored universe, portfolio
      construction with position sizing, and portfolio-level
      signal reconciliation.

  Phase 4 — Reporting
      Generate recommendation report and optional backtest.

The orchestrator can be run end-to-end via
``run_full_pipeline()`` or phase-by-phase for interactive /
notebook use via the ``Orchestrator`` class.

Typical Usage
─────────────
  # One-shot (CLI / cron)
  result = run_full_pipeline(lookback_days=365)

  # Interactive (notebook)
  orch = Orchestrator(lookback_days=180)
  orch.load_data()
  orch.compute_universe_context()
  orch.run_tickers()
  orch.cross_sectional_analysis()
  result = orch.generate_reports()

Dependencies
────────────
  pipeline/runner.py              — single-ticker pipeline
  compute/breadth.py              — universe breadth
  compute/sector_rs.py            — sector RS panel
  output/rankings.py              — cross-sectional rankings
  output/signals.py               — portfolio-level signals
  strategy/portfolio.py           — position sizing & allocation
  portfolio/backtest.py           — historical backtest
  reports/recommendations.py      — ticker recommendations
  src/db/loader.py                — OHLCV data loading
  common/config.py                — all parameters
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional
import numpy as np
import pandas as pd

# ── Convergence ───────────────────────────────────────────────
from strategy.convergence import (
    run_convergence,
    build_price_matrix,
    enrich_snapshots,
    convergence_report,
    MarketSignalResult,
)
from strategy.rotation import (
    run_rotation,
    RotationConfig,
    RotationResult,
)

# ── Config ────────────────────────────────────────────────────
from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SECTOR_ETFS,
    TICKER_SECTOR_MAP,
    UNIVERSE,
    MARKET_CONFIG,
    ACTIVE_MARKETS,
)

# ── Compute ───────────────────────────────────────────────────
from compute.breadth import (
    breadth_to_pillar_scores,
    compute_all_breadth,
)
from compute.sector_rs import compute_all_sector_rs

# ── Data loading ──────────────────────────────────────────────
from src.db.loader import load_ohlcv, load_universe_ohlcv

# ── Pipeline ──────────────────────────────────────────────────
from pipeline.runner import (
    TickerResult,
    results_errors,
    results_to_scored_universe,
    results_to_snapshots,
    run_batch,
    run_ticker,
)

# ── Output ────────────────────────────────────────────────────
from output.rankings import compute_all_rankings
from output.signals import compute_all_signals

# ── Strategy ──────────────────────────────────────────────────
from strategy.portfolio import build_portfolio

# ── Portfolio ─────────────────────────────────────────────────
from portfolio.backtest import run_backtest, BacktestConfig

# ── Reports ───────────────────────────────────────────────────
from reports.recommendations import build_report


logger = logging.getLogger(__name__)

# Extra calendar days fetched before the requested window so        # ← NEW
# long-period indicators (200-day MA, etc.) can warm up.            # ← NEW
_DEFAULT_WARMUP_DAYS = 220                                          # ← NEW


# ═══════════════════════════════════════════════════════════════
#  PIPELINE RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class PipelineResult:
    """
    Complete output of a full pipeline run.

    Every downstream consumer (CLI, web dashboard, notebook,
    report generator) reads from this single object.
    """

    ticker_results: dict[str, TickerResult] = field(default_factory=dict)
    scored_universe: dict[str, pd.DataFrame] = field(default_factory=dict)
    snapshots: list[dict] = field(default_factory=list)
    rankings: pd.DataFrame = field(default_factory=pd.DataFrame)
    portfolio: dict = field(default_factory=dict)
    signals: pd.DataFrame = field(default_factory=pd.DataFrame)

    breadth: Optional[pd.DataFrame] = None
    breadth_scores: Optional[pd.DataFrame] = None
    sector_rs: Optional[pd.DataFrame] = None
    bench_df: Optional[pd.DataFrame] = None

    rotation_result: Optional[Any] = None            # RotationResult
    convergence: Optional[Any] = None                # MarketSignalResult
    market: str = "US"
    lookback_days: int = 365                                        # ← NEW

    recommendation_report: Optional[dict] = None
    backtest: Any = None                          # BacktestResult or None

    errors: list[str] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    run_date: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    as_of: Optional[pd.Timestamp] = None

    @property
    def n_tickers(self) -> int:
        return len(self.scored_universe)

    @property
    def n_errors(self) -> int:
        return len(self.errors)

    @property
    def total_time(self) -> float:
        return sum(self.timings.values())

    def top_n(self, n: int = 10) -> list[dict]:
        return self.snapshots[:n]

    def summary(self) -> str:
        top = self.snapshots[0]["ticker"] if self.snapshots else "N/A"
        return (
            f"CASH Pipeline — {self.run_date.strftime('%Y-%m-%d')} — "
            f"{self.n_tickers} tickers scored, "
            f"{self.n_errors} errors, "
            f"top={top}, "
            f"lookback={self.lookback_days}d, "                     # ← NEW
            f"{self.total_time:.1f}s"
        )


# ═══════════════════════════════════════════════════════════════
#  ORCHESTRATOR CLASS
# ═══════════════════════════════════════════════════════════════

class Orchestrator:
    """
    Stateful pipeline coordinator.

    Use for fine-grained phase-by-phase control (notebooks,
    debugging).  For one-shot usage, prefer ``run_full_pipeline()``.
    """

    def __init__(                                      # ← FIXED: was nested class
        self,
        *,
        market: str = "US",
        universe: list[str] | None = None,
        benchmark: str | None = None,
        capital: float | None = None,
        lookback_days: int | None = None,              # ← NEW
        as_of: pd.Timestamp | None = None,
        enable_breadth: bool = True,
        enable_sectors: bool = True,
        enable_signals: bool = True,
        enable_backtest: bool = False,
    ):
        # ── Market-aware defaults ─────────────────────────────
        self.market: str = market
        mcfg = MARKET_CONFIG.get(market, {})

        self.tickers: list[str] = universe or list(
            mcfg.get("universe", UNIVERSE)
        )
        self.benchmark: str = benchmark or mcfg.get(
            "benchmark", BENCHMARK_TICKER
        )
        self.capital: float = capital or PORTFOLIO_PARAMS["total_capital"]
        self.as_of: pd.Timestamp | None = as_of

        # ── Lookback days ─────────────────────────────────────  # ← NEW
        # None means "load all available data" (no filter).       # ← NEW
        # When set, load_data() fetches lookback_days +           # ← NEW
        # _DEFAULT_WARMUP_DAYS to allow indicator warm-up.        # ← NEW
        self.lookback_days: int | None = lookback_days             # ← NEW

        # Respect market config for feature flags
        self.enable_breadth: bool = (
            enable_breadth
            and mcfg.get("scoring_weights", {}).get(
                "pillar_breadth", 0.10
            ) > 0
        )
        self.enable_sectors: bool = (
            enable_sectors
            and mcfg.get("sector_rs_enabled", True)
        )
        self.enable_signals: bool = enable_signals
        self.enable_backtest: bool = enable_backtest

        # ── Mutable state (populated phase by phase) ──────────
        self._ohlcv: dict[str, pd.DataFrame] = {}
        self._bench_df: pd.DataFrame = pd.DataFrame()
        self._breadth: pd.DataFrame | None = None
        self._breadth_scores: pd.DataFrame | None = None
        self._sector_rs: pd.DataFrame | None = None
        self._ticker_results: dict[str, TickerResult] = {}
        self._scored_universe: dict[str, pd.DataFrame] = {}
        self._snapshots: list[dict] = []
        self._rankings: pd.DataFrame = pd.DataFrame()
        self._portfolio: dict = {}
        self._signals: pd.DataFrame = pd.DataFrame()
        self._recommendation_report: dict | None = None
        self._backtest: Any = None

        self._rotation_result: Any = None           # RotationResult
        self._convergence_result: Any = None        # MarketSignalResult

        self._timings: dict[str, float] = {}
        self._phases_completed: list[str] = []

    # ───────────────────────────────────────────────────────
    #  Phase 0 — Data Loading
    # ───────────────────────────────────────────────────────

    def load_data(
        self,
        preloaded: dict[str, pd.DataFrame] | None = None,
        bench_df: pd.DataFrame | None = None,
    ) -> None:
        """
        Load OHLCV data for all tickers and the benchmark.

        When ``self.lookback_days`` is set, requests
        ``lookback_days + _DEFAULT_WARMUP_DAYS`` calendar days
        from the data source so that long-period indicators
        (200-day SMA, etc.) can initialise before the analysis
        window begins.

        Parameters
        ----------
        preloaded : dict, optional
            ``{ticker: OHLCV DataFrame}`` to skip data loading.
        bench_df : pd.DataFrame, optional
            Pre-loaded benchmark OHLCV.
        """
        t0 = time.perf_counter()

        # ── Compute fetch window ──────────────────────────────  # ← NEW
        fetch_days: int | None = None                              # ← NEW
        if self.lookback_days is not None:                         # ← NEW
            fetch_days = self.lookback_days + _DEFAULT_WARMUP_DAYS # ← NEW
            logger.info(                                           # ← NEW
                f"Phase 0: lookback={self.lookback_days}d "        # ← NEW
                f"+ warmup={_DEFAULT_WARMUP_DAYS}d "               # ← NEW
                f"→ fetching {fetch_days}d from source"            # ← NEW
            )                                                      # ← NEW

        if preloaded is not None:
            self._ohlcv = preloaded
            logger.info(
                f"Phase 0: Using {len(preloaded)} pre-loaded "
                f"ticker DataFrames"
            )
        else:
            all_symbols = list(set(self.tickers + [self.benchmark]))
            self._ohlcv = load_universe_ohlcv(                    # ← CHANGED
                all_symbols,
                days=fetch_days,                                   # ← NEW
            )
            logger.info(
                f"Phase 0: Loaded {len(self._ohlcv)} tickers "
                f"from data source"
                + (f" (last {fetch_days} days)"                    # ← NEW
                   if fetch_days else "")                          # ← NEW
            )

        # ── Extract or load benchmark ─────────────────────────
        if bench_df is not None:
            self._bench_df = bench_df
        elif self.benchmark in self._ohlcv:
            self._bench_df = self._ohlcv[self.benchmark]
        else:
            self._bench_df = load_ohlcv(                           # ← CHANGED
                self.benchmark,
                days=fetch_days,                                   # ← NEW
            )

        if self._bench_df.empty:
            raise ValueError(
                f"Benchmark {self.benchmark} has no data. "
                f"Cannot proceed."
            )

        elapsed = time.perf_counter() - t0
        self._timings["load_data"] = elapsed
        self._phases_completed.append("load_data")
        logger.info(
            f"Phase 0 complete: {len(self._ohlcv)} tickers, "
            f"benchmark={self.benchmark} "
            f"({len(self._bench_df)} bars), "
            f"{elapsed:.1f}s"
        )

    # ───────────────────────────────────────────────────────
    #  Phase 1 — Universe-Level Context
    # ───────────────────────────────────────────────────────

    def compute_universe_context(self) -> None:
        """
        Compute universe-level breadth and sector RS.

        Breadth feeds Pillar 5 (scoring) and Gate 3 (signals).
        Sector RS feeds sector tailwind adjustments and Gate 2.
        Both are optional — the per-ticker pipeline degrades
        gracefully without them.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        # ── Breadth ───────────────────────────────────────────
        if self.enable_breadth:
            try:
                self._breadth = compute_all_breadth(self._ohlcv)

                if not self._breadth.empty:
                    symbols = list(self._ohlcv.keys())
                    self._breadth_scores = breadth_to_pillar_scores(
                        self._breadth, symbols
                    )
                    logger.info(
                        f"Phase 1: Breadth computed — "
                        f"{len(self._breadth)} bars, "
                        f"regime="
                        f"{self._breadth['breadth_regime'].iloc[-1]}"
                    )
                else:
                    logger.warning(
                        "Phase 1: Breadth returned empty — "
                        "universe may be too small"
                    )
            except Exception as e:
                logger.warning(
                    f"Phase 1: Breadth computation failed — {e}.  "
                    f"Proceeding without breadth context."
                )
                self._breadth = None
                self._breadth_scores = None
        else:
            logger.info("Phase 1: Breadth disabled — skipping")

        # ── Sector RS ─────────────────────────────────────────
        if self.enable_sectors:
            try:
                sector_ohlcv = _extract_sector_ohlcv(self._ohlcv)
                if sector_ohlcv:
                    self._sector_rs = compute_all_sector_rs(
                        sector_ohlcv, self._bench_df
                    )
                    logger.info(
                        f"Phase 1: Sector RS computed — "
                        f"{len(sector_ohlcv)} sectors"
                    )
                else:
                    logger.info(
                        "Phase 1: No sector ETFs found in "
                        "universe — skipping sector RS"
                    )
            except Exception as e:
                logger.warning(
                    f"Phase 1: Sector RS computation failed — "
                    f"{e}.  Proceeding without sector context."
                )
                self._sector_rs = None
        else:
            logger.info("Phase 1: Sectors disabled — skipping")

        elapsed = time.perf_counter() - t0
        self._timings["universe_context"] = elapsed
        self._phases_completed.append("universe_context")
        logger.info(f"Phase 1 complete: {elapsed:.1f}s")

    # ───────────────────────────────────────────────────────
    #  Phase 2 — Per-Ticker Pipeline
    # ───────────────────────────────────────────────────────

    def run_tickers(self) -> None:
        """
        Run the single-ticker pipeline for every ticker.

        Calls ``runner.run_batch()`` which chains:
          indicators → RS → scoring → sector → signals
        for each ticker.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        self._ticker_results = run_batch(
            universe=self._ohlcv,
            bench_df=self._bench_df,
            breadth=self._breadth,
            breadth_scores_panel=self._breadth_scores,
            sector_rs=self._sector_rs,
            as_of=self.as_of,
            skip_benchmark=True,
            benchmark_ticker=self.benchmark,
        )

        self._scored_universe = results_to_scored_universe(
            self._ticker_results
        )
        self._snapshots = results_to_snapshots(self._ticker_results)

        elapsed = time.perf_counter() - t0
        self._timings["run_tickers"] = elapsed
        self._phases_completed.append("run_tickers")

        n_ok = len(self._scored_universe)
        n_err = len(results_errors(self._ticker_results))
        logger.info(
            f"Phase 2 complete: {n_ok} scored, "
            f"{n_err} errors, {elapsed:.1f}s"
        )

    # ───────────────────────────────────────────────────────
    #  Phase 2.5 — Rotation Engine  (US only)
    # ───────────────────────────────────────────────────────

    def run_rotation_engine(
        self,
        current_holdings: list[str] | None = None,
        config: RotationConfig | None = None,
    ) -> None:
        """
        Run the top-down sector rotation engine.

        Only meaningful for US — skipped silently for HK/IN.
        Requires Phase 2 (run_tickers) to have completed so
        that OHLCV data is available.

        When Phase 2 produced scored DataFrames, the indicator
        data is passed to the rotation engine's quality filter.
        This gates and scores candidates within leading sectors
        on six technical dimensions (MA structure, RSI, MACD,
        ADX, volume, volatility) and blends the quality score
        with relative strength for final stock ranking.

        When Phase 2 results are empty (or quality is disabled
        in RotationConfig), the rotation engine falls back to
        RS-only ranking — fully backward compatible.
        """
        self._require_phase("run_tickers")

        mcfg = MARKET_CONFIG.get(self.market, {})
        engines = mcfg.get("engines", ["scoring"])

        if "rotation" not in engines:
            logger.info(
                f"Phase 2.5: Rotation not configured for "
                f"{self.market} — skipping"
            )
            return

        t0 = time.perf_counter()

        # Build wide price matrix from loaded OHLCV
        prices = build_price_matrix(self._ohlcv)

        if prices.empty or self.benchmark not in prices.columns:
            logger.warning(
                "Phase 2.5: Cannot build price matrix for "
                "rotation — skipping"
            )
            return

        # ── Build indicator_data from Phase 2 results ─────
        #
        # self._scored_universe is {ticker: DataFrame} where
        # each DataFrame has all indicator columns produced by
        # compute_all_indicators() in the per-ticker pipeline
        # (ema_30, sma_50, rsi_14, adx_14, macd_hist,
        # obv_slope_10d, relative_volume, atr_14_pct, etc.).
        #
        # The rotation engine's quality filter reads these
        # columns to gate and score each candidate stock.
        #
        # Tickers not in this dict (missing data, ETFs not in
        # the scoring universe, failed tickers) receive neutral
        # quality (0.5) and pass the gate by default — so they
        # participate in ranking but don't get the quality bonus.
        indicator_data: dict[str, pd.DataFrame] | None = None

        if self._scored_universe:
            indicator_data = {
                ticker: df
                for ticker, df in self._scored_universe.items()
                if df is not None and not df.empty
            }
            if not indicator_data:
                indicator_data = None

        try:
            r_cfg = config or RotationConfig(
                benchmark=self.benchmark,
            )

            # Log quality filter status
            quality_enabled = r_cfg.quality.enabled
            n_indicator = len(indicator_data) if indicator_data else 0

            if quality_enabled and n_indicator > 0:
                logger.info(
                    f"Phase 2.5: Quality filter ON — "
                    f"{n_indicator} tickers have indicator data"
                )
            elif quality_enabled and n_indicator == 0:
                logger.info(
                    f"Phase 2.5: Quality filter enabled but "
                    f"no indicator data available — "
                    f"falling back to RS-only"
                )
            else:
                logger.info(
                    f"Phase 2.5: Quality filter OFF "
                    f"(disabled in config)"
                )

            self._rotation_result = run_rotation(
                prices=prices,
                current_holdings=current_holdings or [],
                config=r_cfg,
                indicator_data=indicator_data,
            )

            rr = self._rotation_result
            logger.info(
                f"Phase 2.5: Rotation complete — "
                f"{len(rr.buys)} BUY, "
                f"{len(rr.sells)} SELL, "
                f"{len(rr.reduces)} REDUCE, "
                f"{len(rr.holds)} HOLD  |  "
                f"leading={rr.leading_sectors}"
            )

            # Log quality impact on BUY picks
            if quality_enabled and n_indicator > 0:
                self._log_quality_summary(rr)

        except Exception as e:
            logger.warning(
                f"Phase 2.5: Rotation failed — {e}.  "
                f"Proceeding with scoring only."
            )
            self._rotation_result = None

        elapsed = time.perf_counter() - t0
        self._timings["rotation"] = elapsed
        self._phases_completed.append("rotation")


    def _log_quality_summary(self, rr: RotationResult) -> None:
        """
        Log how the quality filter affected BUY recommendations.

        Called from run_rotation_engine() when quality is active.
        """
        buys = rr.buys
        if not buys:
            return

        q_scores = [
            r.quality_score for r in buys
            if r.quality_score > 0
        ]
        gate_fails = [
            r.ticker for r in buys
            if not r.quality_gate_passed
        ]

        if q_scores:
            avg_q = sum(q_scores) / len(q_scores)
            logger.info(
                f"Phase 2.5: Quality summary — "
                f"{len(buys)} BUYs, "
                f"avg quality {avg_q:.2f}, "
                f"range [{min(q_scores):.2f}, "
                f"{max(q_scores):.2f}]"
            )

        if gate_fails:
            logger.info(
                f"Phase 2.5: {len(gate_fails)} BUY(s) had "
                f"gate failures (included via fallback): "
                f"{gate_fails[:5]}"
            )

    # ───────────────────────────────────────────────────────
    #  Phase 2.75 — Convergence Merge
    # ───────────────────────────────────────────────────────

    def apply_convergence(self) -> None:
        """
        Merge scoring + rotation signals via the convergence layer.

        For US:  dual-list merge (scoring + rotation)
        For HK/IN: scoring passthrough

        Updates ``self._snapshots`` with convergence labels and
        adjusted scores so downstream phases (rankings, portfolio,
        reports) benefit from the convergence intelligence.
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        self._convergence_result = run_convergence(
            market=self.market,
            scoring_snapshots=self._snapshots,
            rotation_result=self._rotation_result,
        )

        # Enrich snapshots in-place with convergence data
        enrich_snapshots(self._snapshots, self._convergence_result)

        n_strong = len(self._convergence_result.strong_buys)
        n_conflict = len(self._convergence_result.conflicts)
        logger.info(
            f"Phase 2.75: Convergence applied — "
            f"{self._convergence_result.n_tickers} tickers, "
            f"{n_strong} STRONG_BUY, "
            f"{n_conflict} CONFLICT"
        )

        elapsed = time.perf_counter() - t0
        self._timings["convergence"] = elapsed
        self._phases_completed.append("convergence")

    # ───────────────────────────────────────────────────────
    #  Phase 3 — Cross-Sectional Analysis
    # ───────────────────────────────────────────────────────

    def cross_sectional_analysis(self) -> None:
        """
        Rank the scored universe, build the portfolio, and
        generate portfolio-level signals.

        Sub-phases
        ──────────
        3a. Rankings — cross-sectional rank per date
        3b. Portfolio — position selection + weight allocation
        3c. Signals — BUY/HOLD/SELL with hysteresis
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        if not self._scored_universe:
            logger.warning(
                "Phase 3: No scored tickers — skipping "
                "cross-sectional analysis"
            )
            self._phases_completed.append("cross_sectional")
            return

        # ── 3a. Rankings ──────────────────────────────────────
        try:
            self._rankings = compute_all_rankings(
                self._scored_universe
            )
            n_rows = len(self._rankings)
            logger.info(
                f"Phase 3a: Rankings computed — "
                f"{n_rows} rows"
            )
        except Exception as e:
            logger.warning(f"Phase 3a: Rankings failed — {e}")
            self._rankings = pd.DataFrame()

        # ── 3b. Portfolio Construction ────────────────────────
        try:
            self._portfolio = build_portfolio(
                universe=self._scored_universe,
                breadth=self._breadth,
            )

            n_pos = self._portfolio.get(
                "metadata", {}
            ).get("num_holdings", 0)
            logger.info(
                f"Phase 3b: Portfolio built — "
                f"{n_pos} positions"
            )

            # Enrich orchestrator snapshots with allocation info
            _enrich_snapshots_with_allocations(
                self._snapshots,
                self._portfolio,
                self.capital,
            )
        except Exception as e:
            logger.warning(
                f"Phase 3b: Portfolio build failed — {e}"
            )
            self._portfolio = {}

        # ── 3c. Portfolio-Level Signal Generation ─────────────
        if self.enable_signals and not self._rankings.empty:
            try:
                self._signals = compute_all_signals(
                    ranked=self._rankings,
                    breadth=self._breadth,
                )
                logger.info(
                    f"Phase 3c: Signals generated — "
                    f"{len(self._signals)} rows"
                )

                # Update snapshots with reconciled signals
                _enrich_snapshots_with_signals(
                    self._snapshots, self._signals
                )
            except Exception as e:
                logger.warning(
                    f"Phase 3c: Signal generation failed — {e}"
                )
                self._signals = pd.DataFrame()
        else:
            if not self.enable_signals:
                logger.info(
                    "Phase 3c: Signals disabled — skipping"
                )
            else:
                logger.warning(
                    "Phase 3c: No rankings — cannot generate "
                    "signals"
                )

        elapsed = time.perf_counter() - t0
        self._timings["cross_sectional"] = elapsed
        self._phases_completed.append("cross_sectional")
        logger.info(f"Phase 3 complete: {elapsed:.1f}s")

    # ───────────────────────────────────────────────────────
    #  Phase 4 — Reports & Optional Backtest
    # ───────────────────────────────────────────────────────

    def generate_reports(self) -> PipelineResult:
        """
        Generate reports and assemble the final PipelineResult.

        Returns
        -------
        PipelineResult
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        # ── Recommendation Report ─────────────────────────────
        try:
            report_input = self._build_report_input()
            self._recommendation_report = build_report(
                report_input
            )
            logger.info("Phase 4: Recommendation report built")
        except Exception as e:
            logger.warning(
                f"Phase 4: Recommendation report failed — {e}"
            )
            self._recommendation_report = None

        # ── Backtest (optional) ───────────────────────────────
        if self.enable_backtest:
            if not self._signals.empty:
                try:
                    bt_config = BacktestConfig(
                        initial_capital=self.capital,
                    )
                    self._backtest = run_backtest(
                        signals_df=self._signals,
                        config=bt_config,
                    )
                    metrics = (
                        self._backtest.metrics
                        if self._backtest else {}
                    )
                    logger.info(
                        f"Phase 4: Backtest complete — "
                        f"CAGR="
                        f"{metrics.get('cagr', 0):.1%}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Phase 4: Backtest failed — {e}"
                    )
            else:
                logger.warning(
                    "Phase 4: Cannot run backtest — "
                    "no signals generated"
                )

        elapsed = time.perf_counter() - t0
        self._timings["reports"] = elapsed
        self._phases_completed.append("reports")

        # ── Assemble PipelineResult ───────────────────────────
        errors = results_errors(self._ticker_results)

        result = PipelineResult(
            ticker_results=self._ticker_results,
            scored_universe=self._scored_universe,
            snapshots=self._snapshots,
            rankings=self._rankings,
            portfolio=self._portfolio,
            signals=self._signals,
            breadth=self._breadth,
            breadth_scores=self._breadth_scores,
            sector_rs=self._sector_rs,
            bench_df=self._bench_df,
            rotation_result=self._rotation_result,
            convergence=self._convergence_result,
            market=self.market,
            lookback_days=self.lookback_days or 0,                 # ← NEW
            recommendation_report=self._recommendation_report,
            backtest=self._backtest,
            errors=errors,
            timings=self._timings,
            run_date=pd.Timestamp.now(),
            as_of=self.as_of,
        )

        logger.info(result.summary())
        return result

    # ───────────────────────────────────────────────────────
    #  Convenience: Run All Phases
    # ───────────────────────────────────────────────────────

    def run_all(
        self,
        preloaded: dict[str, pd.DataFrame] | None = None,
        bench_df: pd.DataFrame | None = None,
        current_holdings: list[str] | None = None,
    ) -> PipelineResult:
        """Execute all phases in sequence."""
        self.load_data(preloaded=preloaded, bench_df=bench_df)
        self.compute_universe_context()
        self.run_tickers()

        self.run_rotation_engine(
            current_holdings=current_holdings,
        )
        self.apply_convergence()

        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Re-run Portfolio with Different Parameters
    # ───────────────────────────────────────────────────────

    def rebuild_portfolio(
        self,
        capital: float | None = None,
    ) -> PipelineResult:
        """
        Re-run Phase 3 + 4 without re-computing indicators.
        """
        self._require_phase("run_tickers")

        if capital is not None:
            self.capital = capital

        self._snapshots = results_to_snapshots(
            self._ticker_results
        )
        self._rankings = pd.DataFrame()
        self._portfolio = {}
        self._signals = pd.DataFrame()
        self._recommendation_report = None

        self._phases_completed = [
            p for p in self._phases_completed
            if p in (
                "load_data", "universe_context", "run_tickers"
            )
        ]

        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Single-Ticker Re-run
    # ───────────────────────────────────────────────────────

    def rerun_ticker(self, ticker: str) -> TickerResult:
        """
        Re-run the pipeline for a single ticker.
        """
        self._require_phase("load_data")

        if ticker not in self._ohlcv:
            return TickerResult(
                ticker=ticker,
                error=f"No OHLCV data for {ticker}",
            )

        b_scores = None
        if (
            self._breadth_scores is not None
            and ticker in self._breadth_scores.columns
        ):
            b_scores = self._breadth_scores[ticker]

        result = run_ticker(
            ticker=ticker,
            ohlcv=self._ohlcv[ticker],
            bench_df=self._bench_df,
            breadth=self._breadth,
            breadth_scores=b_scores,
            sector_rs=self._sector_rs,
            as_of=self.as_of,
        )

        self._ticker_results[ticker] = result
        logger.info(
            f"Re-ran {ticker}: "
            f"{'OK' if result.ok else result.error}"
        )
        return result

    # ───────────────────────────────────────────────────────
    #  Report Input Bridge
    # ───────────────────────────────────────────────────────

    def _build_report_input(self) -> dict:
        """
        Construct the dict format that
        ``reports.recommendations.build_report()`` expects.

        Bridges from the orchestrator's internal state
        (PipelineResult-style) to the legacy dict format with
        keys: summary, regime, risk_flags, portfolio_actions,
        ranked_buys, sells, holds, bucket_weights.
        """
        # ── Regime detection ──────────────────────────────
        regime_label, regime_desc = _detect_regime(
            self._bench_df, self._breadth
        )

        spy_close = 0.0
        spy_sma200 = None
        if not self._bench_df.empty:
            spy_close = float(
                self._bench_df["close"].iloc[-1]
            )
            if len(self._bench_df) >= 200:
                sma = self._bench_df["close"].rolling(200).mean()
                spy_sma200 = float(sma.iloc[-1])

        breadth_label = "unknown"
        if (
            self._breadth is not None
            and not self._breadth.empty
            and "breadth_regime" in self._breadth.columns
        ):
            breadth_label = str(
                self._breadth["breadth_regime"].iloc[-1]
            )

        # ── Split snapshots by signal ─────────────────────
        buys = [
            s for s in self._snapshots
            if s.get("signal") == "BUY"
        ]
        sells = [
            s for s in self._snapshots
            if s.get("signal") == "SELL"
        ]
        holds = [
            s for s in self._snapshots
            if s.get("signal") not in ("BUY", "SELL")
        ]

        # Ensure allocation fields default to 0 (not None)
        for s in buys + sells + holds:
            s.setdefault("shares", 0)
            s.setdefault("dollar_alloc", 0)
            s.setdefault("weight_pct", 0)
            s.setdefault("stop_price", None)
            s.setdefault("risk_per_share", None)
            s.setdefault("themes", [])
            s.setdefault("category", "")
            s.setdefault("bucket", "")
            if s["shares"] is None:
                s["shares"] = 0
            if s["dollar_alloc"] is None:
                s["dollar_alloc"] = 0
            if s["weight_pct"] is None:
                s["weight_pct"] = 0

        # ── Summary values ────────────────────────────────
        total_buy = sum(
            s.get("dollar_alloc", 0) or 0 for s in buys
        )
        cash_rem = self.capital - total_buy
        cash_pct = (
            (cash_rem / self.capital * 100)
            if self.capital > 0 else 100
        )

        date = (
            self._snapshots[0]["date"]
            if self._snapshots
            else pd.Timestamp.now()
        )

        # ── Risk flags ────────────────────────────────────
        risk_flags: list[str] = []
        if breadth_label == "weak":
            risk_flags.append(
                "BREADTH_WEAK: Market breadth is weak — "
                "reduced exposure recommended"
            )
        if regime_label in ("bear_mild", "bear_severe"):
            risk_flags.append(
                f"REGIME: {regime_label} — defensive "
                f"positioning recommended"
            )
        if regime_label == "bear_severe":
            risk_flags.append(
                "CIRCUIT_BREAKER: Severe bear — "
                "consider halting new buys"
            )

        # ── Bucket weights from sector exposure ───────────
        bucket_weights: dict[str, float] = {}
        if self._portfolio:
            se = self._portfolio.get("sector_exposure", {})
            meta = self._portfolio.get("metadata", {})
            for sector, weight in se.items():
                bucket_weights[sector] = weight
            bucket_weights["cash"] = meta.get("cash_pct", 0.05)
        else:
            bucket_weights = {
                "core_equity": 0.70,
                "thematic": 0.20,
                "cash": 0.10,
            }

        return {
            "summary": {
                "date":             date,
                "portfolio_value":  self.capital,
                "regime":           regime_label,
                "regime_desc":      regime_desc,
                "spy_close":        spy_close,
                "bucket_breakdown": {},
                "cash_pct":         cash_pct,
                "tickers_analysed": len(self._snapshots),
                "buy_count":        len(buys),
                "sell_count":       len(sells),
                "hold_count":       len(holds),
                "error_count":      len(
                    results_errors(self._ticker_results)
                ),
                "total_buy_dollar": total_buy,
                "cash_remaining":   cash_rem,
            },
            "regime": {
                "label":       regime_label,
                "description": regime_desc,
                "spy_close":   spy_close,
                "spy_sma200":  spy_sma200,
                "breadth":     breadth_label,
            },
            "risk_flags":        risk_flags,
            "portfolio_actions": [],
            "ranked_buys":       buys,
            "sells":             sells,
            "holds":             holds,
            "bucket_weights":    bucket_weights,
        }

    # ───────────────────────────────────────────────────────
    #  Internal Helpers
    # ───────────────────────────────────────────────────────

    def _require_phase(self, phase: str) -> None:
        if phase not in self._phases_completed:
            raise RuntimeError(
                f"Phase '{phase}' has not been run yet.  "
                f"Completed phases: {self._phases_completed}"
            )


# ═══════════════════════════════════════════════════════════════
#  ONE-SHOT ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_full_pipeline(
    *,
    market: str = "US",
    universe: list[str] | None = None,
    benchmark: str | None = None,
    capital: float | None = None,
    lookback_days: int | None = None,                              # ← NEW
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, pd.DataFrame] | None = None,
    bench_df: pd.DataFrame | None = None,
    current_holdings: list[str] | None = None,
    enable_breadth: bool = True,
    enable_sectors: bool = True,
    enable_signals: bool = True,
    enable_backtest: bool = False,
) -> PipelineResult:
    """
    Run the full CASH pipeline end-to-end for one market.

    Parameters
    ----------
    lookback_days : int, optional
        Calendar days of analysis history.  When set, data
        loading fetches ``lookback_days + 220`` days so that
        long-period indicators can warm up.  When ``None``,
        all available data is loaded (original behaviour).

    This is the main entry point for CLI usage and scheduled
    jobs.  For multi-market, use ``run_multi_market_pipeline()``.
    For interactive control, use ``Orchestrator`` directly.
    """
    orch = Orchestrator(
        market=market,
        universe=universe,
        benchmark=benchmark,
        capital=capital,
        lookback_days=lookback_days,                               # ← NEW
        as_of=as_of,
        enable_breadth=enable_breadth,
        enable_sectors=enable_sectors,
        enable_signals=enable_signals,
        enable_backtest=enable_backtest,
    )

    return orch.run_all(
        preloaded=preloaded,
        bench_df=bench_df,
        current_holdings=current_holdings,
    )


# ═══════════════════════════════════════════════════════════════
#  MULTI-MARKET PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_multi_market_pipeline(
    *,
    active_markets: list[str] | None = None,
    capital: float | None = None,
    lookback_days: int | None = None,                              # ← NEW
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, dict[str, pd.DataFrame]] | None = None,
    current_holdings: dict[str, list[str]] | None = None,
    enable_backtest: bool = False,
) -> dict[str, PipelineResult]:
    """
    Run the full CASH pipeline for every active market.

    Creates a separate Orchestrator per market, each with the
    correct benchmark, universe, and feature flags.

    Parameters
    ----------
    active_markets : list[str], optional
        Markets to run.  Defaults to ``ACTIVE_MARKETS`` from config
        (typically ``["US", "HK", "IN"]``).
    capital : float, optional
        Portfolio value per market.
    lookback_days : int, optional                                  # ← NEW
        Calendar days of analysis history, applied to every       # ← NEW
        market.  When ``None``, all available data is loaded.     # ← NEW
    as_of : pd.Timestamp, optional
        Cut-off date for backtesting.
    preloaded : dict, optional
        ``{market: {ticker: OHLCV DataFrame}}``.  If provided,
        skips data loading for that market.
    current_holdings : dict, optional
        ``{market: [ticker, ...]}``.  Holdings are passed to
        the rotation engine (US) for sell evaluation.
    enable_backtest : bool
        Run historical backtest for each market.

    Returns
    -------
    dict[str, PipelineResult]
        ``{market_code: PipelineResult}`` for each market that
        ran successfully.

    Example
    -------
    ::

        results = run_multi_market_pipeline(lookback_days=365)
        us = results["US"]
        hk = results["HK"]

        for s in us.convergence.strong_buys:
            print(f"{s.ticker}: STRONG BUY, adj={s.adjusted_score:.3f}")
    """
    markets = active_markets or ACTIVE_MARKETS
    results: dict[str, PipelineResult] = {}

    for market in markets:
        mcfg = MARKET_CONFIG.get(market)
        if mcfg is None:
            logger.warning(
                f"Market '{market}' not in MARKET_CONFIG — skipping"
            )
            continue

        logger.info(f"\n{'=' * 60}")
        logger.info(f"  MARKET: {market}")
        logger.info(f"  Benchmark: {mcfg['benchmark']}")
        logger.info(f"  Universe: {len(mcfg['universe'])} tickers")
        logger.info(f"  Engines: {mcfg['engines']}")
        if lookback_days is not None:                              # ← NEW
            logger.info(f"  Lookback: {lookback_days} days")       # ← NEW
        logger.info(f"{'=' * 60}")

        # Pre-loaded data for this market
        pre = (
            preloaded.get(market) if preloaded else None
        )
        holdings = (
            current_holdings.get(market, [])
            if current_holdings else None
        )

        try:
            orch = Orchestrator(
                market=market,
                capital=capital,
                lookback_days=lookback_days,                       # ← NEW
                as_of=as_of,
                enable_backtest=enable_backtest,
            )

            result = orch.run_all(
                preloaded=pre,
                current_holdings=holdings,
            )
            results[market] = result

            logger.info(
                f"[{market}] Pipeline complete: "
                f"{result.n_tickers} tickers, "
                f"{result.n_errors} errors, "
                f"{result.total_time:.1f}s"
            )

            # Log convergence summary
            if result.convergence:
                logger.info(
                    f"[{market}] {result.convergence.summary()}"
                )

        except Exception as e:
            logger.error(
                f"[{market}] Pipeline failed: {e}",
                exc_info=True,
            )

    logger.info(f"\nMulti-market complete: {list(results.keys())}")
    return results


# ═══════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═══════════════════════════════════════════════════════════════

def _detect_regime(
    bench_df: pd.DataFrame,
    breadth: pd.DataFrame | None,
) -> tuple[str, str]:
    """
    Simple market regime detection from benchmark price action
    and breadth state.

    Returns (label, description) where label is one of:
    bull_confirmed, bull_cautious, bear_mild, bear_severe.
    """
    if bench_df is None or bench_df.empty:
        return "bull_cautious", "Insufficient data for regime"

    close = float(bench_df["close"].iloc[-1])

    # Check SPY vs 200-day SMA
    above_sma200 = True
    if len(bench_df) >= 200:
        sma200 = float(
            bench_df["close"].rolling(200).mean().iloc[-1]
        )
        above_sma200 = close > sma200

    # Check breadth regime
    b_regime = "unknown"
    if (
        breadth is not None
        and not breadth.empty
        and "breadth_regime" in breadth.columns
    ):
        b_regime = str(breadth["breadth_regime"].iloc[-1])

    if above_sma200 and b_regime == "strong":
        return (
            "bull_confirmed",
            "SPY above 200d SMA, breadth strong",
        )
    elif above_sma200:
        return (
            "bull_cautious",
            f"SPY above 200d SMA, breadth {b_regime}",
        )
    elif b_regime == "weak":
        return (
            "bear_severe",
            "SPY below 200d SMA, breadth weak",
        )
    else:
        return (
            "bear_mild",
            f"SPY below 200d SMA, breadth {b_regime}",
        )


def _extract_sector_ohlcv(
    ohlcv: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Extract OHLCV for sector ETFs present in the loaded data.

    Returns ``{sector_name: OHLCV DataFrame}`` for sectors
    whose ETF ticker exists in ``ohlcv``.
    """
    sector_data: dict[str, pd.DataFrame] = {}
    for sector_name, etf_ticker in SECTOR_ETFS.items():
        if etf_ticker in ohlcv:
            sector_data[sector_name] = ohlcv[etf_ticker]
    return sector_data


def _enrich_snapshots_with_allocations(
    snapshots: list[dict],
    portfolio: dict,
    capital: float,
) -> None:
    """
    Merge portfolio allocation fields into ticker snapshots.

    ``build_portfolio()`` returns ``target_weights`` as
    ``{ticker: weight_fraction}``.  This function converts
    those weights to dollar allocations and share counts,
    then writes them into the snapshot dicts in-place.

    Tickers not in the portfolio get zero allocations.
    """
    target_weights = portfolio.get("target_weights", {})

    for snap in snapshots:
        ticker = snap["ticker"]
        weight = target_weights.get(ticker, 0.0)

        if weight > 0:
            dollar_alloc = weight * capital
            close = snap.get("close", 0) or 0
            shares = int(dollar_alloc / close) if close > 0 else 0

            snap["weight_pct"] = round(weight * 100, 2)
            snap["dollar_alloc"] = round(dollar_alloc, 2)
            snap["shares"] = shares
            snap["category"] = "selected"
        else:
            snap["weight_pct"] = 0.0
            snap["dollar_alloc"] = 0.0
            snap["shares"] = 0
            snap["category"] = "not_selected"


def _enrich_snapshots_with_signals(
    snapshots: list[dict],
    signals_df: pd.DataFrame,
) -> None:
    """
    Update snapshot ``signal`` field from the portfolio-level
    signals DataFrame.

    ``compute_all_signals()`` returns a MultiIndex (date, ticker)
    panel.  We extract the latest date's signals and overwrite
    the per-ticker signal (from ``strategy/signals.py``) with
    the portfolio-level signal (which incorporates rank
    hysteresis, position limits, and breadth gating).
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return

    dates = (
        signals_df.index.get_level_values("date")
        .unique()
        .sort_values()
    )
    if len(dates) == 0:
        return

    latest_date = dates[-1]

    try:
        latest = signals_df.xs(latest_date, level="date")
        sig_map = latest["signal"].to_dict()
    except (KeyError, TypeError):
        return

    for snap in snapshots:
        ticker = snap["ticker"]
        if ticker in sig_map:
            snap["signal"] = sig_map[ticker]


################################################################################
"""
pipeline/runner.py
------------------
Single-ticker pipeline.

Accepts raw OHLCV for one ticker + benchmark, runs every CASH
compute module in sequence, returns the enriched DataFrame and
a summary snapshot of the latest row.

This is the atomic unit of work — orchestrator.py calls this
once per ticker, then layers on cross-sectional logic
(rankings, portfolio signals, backtesting).

Pipeline Order
──────────────
  raw OHLCV → date slice (optional as_of cut)
       ↓
  1. compute_all_indicators()     ~30 technical indicator columns
       ↓
  2. compute_all_rs()             RS ratio/slope/zscore/regime
       ↓
  3. compute_composite_score()    5-pillar composite (breadth opt.)
       ↓
  4. merge_sector_context()       sector tailwind (optional)
       ↓
  5. generate_signals()           6-gate entry/exit filter
       ↓
  TickerResult { df, snapshot, error }

Each stage validates its inputs and fails fast with a clear
error message.  Optional stages (sector, breadth) degrade
gracefully — the ticker is still scored and ranked without
sector adjustments or breadth gating.

Dependencies
────────────
  compute/indicators.py          — technical indicators
  compute/relative_strength.py   — RS vs benchmark
  compute/scoring.py             — 5-pillar composite
  compute/sectors.py             — sector merge (optional)
  strategy/signals.py            — 6-gate filter (optional)
  common/config.py               — all parameters
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from common.config import (
    INDICATOR_PARAMS,
    TICKER_SECTOR_MAP,
)
from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score
from compute.sector_rs import merge_sector_context
from strategy.signals import generate_signals

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Minimum bars needed for meaningful output.
#
# Breakdown of the slowest lookback chains:
#   indicators.py:   sma_long (50)
#   relative_strength.py:
#     rs_sma_span (50) → rs_slope_window (20) → rs_zscore_window (60)
#     Total: ~130 bars for RS z-score to produce values
#   scoring.py:      rank_window (60) for rolling percentile ranks
#   breadth.py:      high_low_window (252) but that's universe-level
#
# With 200 bars the latest ~60 rows have fully warmed-up
# values across all pillars.
_MIN_BARS = 200

# ATR multiplier for initial stop-loss calculation.
# stop_price = close − ATR_STOP_MULT × ATR
ATR_STOP_MULT = 2.0


# ═══════════════════════════════════════════════════════════════
#  RESULT OBJECT
# ═══════════════════════════════════════════════════════════════

@dataclass
class TickerResult:
    """
    Output of the single-ticker pipeline.

    Attributes
    ----------
    ticker : str
        Symbol, e.g. "AAPL".
    df : pd.DataFrame
        Full enriched DataFrame with indicator, RS, scoring,
        sector, and signal columns appended.  Empty if error.
    snapshot : dict
        Latest-row summary values for quick access by the
        orchestrator and report generators.
    error : str or None
        Error message if the pipeline failed for this ticker.
        None on success.
    stages_completed : list[str]
        Which pipeline stages ran successfully.  Useful for
        diagnosing partial failures (e.g. scoring succeeded
        but sector merge failed).
    """

    ticker: str
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    snapshot: dict = field(default_factory=dict)
    error: Optional[str] = None
    stages_completed: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True if the core pipeline (indicators + RS + scoring) succeeded."""
        return self.error is None and not self.df.empty

    @property
    def has_signals(self) -> bool:
        """True if signal generation ran successfully."""
        return "signals" in self.stages_completed

    @property
    def has_sector(self) -> bool:
        """True if sector context was merged."""
        return "sector" in self.stages_completed


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_ticker(
    ticker: str,
    ohlcv: pd.DataFrame,
    bench_df: pd.DataFrame,
    *,
    breadth: pd.DataFrame | None = None,
    breadth_scores: pd.Series | None = None,
    sector_rs: pd.DataFrame | None = None,
    sector_name: str | None = None,
    as_of: pd.Timestamp | None = None,
) -> TickerResult:
    """
    Full CASH pipeline for a single ticker.

    Parameters
    ----------
    ticker : str
        Symbol, e.g. "AAPL".
    ohlcv : pd.DataFrame
        Columns: open, high, low, close, volume.
        DatetimeIndex sorted ascending.
    bench_df : pd.DataFrame
        Benchmark OHLCV (SPY), same format.  Must have at
        least a ``close`` column and DatetimeIndex.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()`` — universe-level
        breadth with ``breadth_regime`` column.  Used by the
        signal gate (Gate 3) and position-size scaling.
    breadth_scores : pd.Series, optional
        This ticker's column from ``breadth_to_pillar_scores()``
        — daily breadth score on 0–100 scale for Pillar 5 of
        the composite score.
    sector_rs : pd.DataFrame, optional
        Output of ``compute_all_sector_rs()`` — MultiIndex
        (date, sector).  Used for sector tailwind/headwind
        adjustments to the composite score.
    sector_name : str, optional
        Sector label matching a key in ``SECTOR_ETFS`` (e.g.
        ``"Technology"``).  If None, auto-looked up from
        ``TICKER_SECTOR_MAP`` in config.
    as_of : pd.Timestamp, optional
        Cut-off date — everything after is excluded.  Used by
        the backtester; None means use all available data.

    Returns
    -------
    TickerResult
        .df        — enriched DataFrame (empty on error)
        .snapshot  — latest-row summary dict
        .error     — error message or None
        .stages_completed — list of completed stages

    Notes
    -----
    Stages 1–3 (indicators, RS, scoring) are **required** —
    failure in any of these produces an error result.

    Stages 4–5 (sector merge, signal generation) are
    **optional** — failure degrades gracefully.  The ticker
    will still appear in rankings and backtests, just without
    sector adjustments or per-ticker signal gates.

    The ``compute_all_rs()`` call aligns the stock and
    benchmark on common trading dates, so the returned
    DataFrame may have fewer rows than the input.
    """
    stages: list[str] = []

    # ── 0. Date slice ─────────────────────────────────────────
    df = ohlcv.copy()
    bench = bench_df.copy()

    if as_of is not None:
        df = df.loc[:as_of]
        bench = bench.loc[:as_of]

    if len(df) < _MIN_BARS:
        return TickerResult(
            ticker=ticker,
            error=(
                f"Insufficient data: need ≥ {_MIN_BARS} bars, "
                f"got {len(df)}"
            ),
        )

    # ── 1. Technical indicators (REQUIRED) ────────────────────
    #
    #    Adds ~30 columns: returns, RSI, MACD, ADX, moving
    #    averages, ATR, realized vol, OBV, A/D line, volume
    #    metrics, Amihud illiquidity, VWAP distance.
    #
    try:
        df = compute_all_indicators(df)
        stages.append("indicators")
    except (ValueError, KeyError) as e:
        return TickerResult(
            ticker=ticker,
            error=f"Stage 1 (indicators) failed: {e}",
            stages_completed=stages,
        )

    # ── 2. Relative strength vs benchmark (REQUIRED) ──────────
    #
    #    Adds: rs_raw, rs_ema, rs_sma, rs_slope, rs_zscore,
    #    rs_momentum, rs_rel_volume, rs_vol_confirmed, rs_regime.
    #
    #    Also aligns stock and benchmark on common dates — the
    #    returned DataFrame may be shorter than the input.
    #
    try:
        df = compute_all_rs(df, bench)
        stages.append("relative_strength")
    except ValueError as e:
        return TickerResult(
            ticker=ticker,
            error=f"Stage 2 (RS) failed: {e}",
            stages_completed=stages,
        )

    # ── 3. Composite scoring (REQUIRED) ───────────────────────
    #
    #    Adds: score_rotation, score_momentum, score_volatility,
    #    score_microstructure, score_composite, score_percentile.
    #    Optionally score_breadth if breadth_scores provided.
    #
    try:
        df = compute_composite_score(df, breadth_scores)
        stages.append("scoring")
    except (ValueError, KeyError) as e:
        return TickerResult(
            ticker=ticker,
            error=f"Stage 3 (scoring) failed: {e}",
            stages_completed=stages,
        )

    # ── 4. Sector context (OPTIONAL) ──────────────────────────
    #
    #    Adds: sect_rs_zscore, sect_rs_regime, sect_rs_rank,
    #    sect_rs_pctrank, sector_tailwind, sector_name.
    #    If score_composite exists, also creates score_adjusted.
    #
    #    Degrades gracefully — ticker is still scored without
    #    sector adjustments.
    #
    resolved_sector = sector_name or TICKER_SECTOR_MAP.get(ticker)

    if sector_rs is not None and resolved_sector is not None:
        try:
            df = merge_sector_context(df, sector_rs, resolved_sector)
            stages.append("sector")
            logger.debug(
                f"{ticker}: sector context merged "
                f"({resolved_sector})"
            )
        except (ValueError, KeyError) as e:
            # Sector data missing or sector name not in panel —
            # continue without sector adjustments
            logger.debug(
                f"{ticker}: sector merge skipped — {e}"
            )
            # Still tag the sector name for downstream grouping
            df["sector_name"] = resolved_sector
    else:
        # No sector RS data provided or ticker not in map —
        # tag what we know
        if resolved_sector:
            df["sector_name"] = resolved_sector

    # ── 5. Entry / exit signals (OPTIONAL) ────────────────────
    #
    #    Adds: sig_regime_ok, sig_sector_ok, sig_breadth_ok,
    #    sig_momentum_ok, sig_in_cooldown, sig_confirmed,
    #    sig_exit, sig_position_pct, sig_reason.
    #
    #    Requires RS and sector columns to be present.
    #    Degrades gracefully — ticker appears in rankings
    #    without per-ticker quality gates.
    #
    try:
        df = generate_signals(df, breadth)
        stages.append("signals")
    except Exception as e:
        logger.warning(
            f"{ticker}: signal generation failed — {e}.  "
            f"Ticker will still be scored and ranked."
        )

    # ── 6. Build snapshot ─────────────────────────────────────
    snapshot = _build_snapshot(ticker, df)

    logger.debug(
        f"{ticker}: pipeline complete — "
        f"{len(df)} bars, "
        f"stages={stages}, "
        f"composite={snapshot.get('composite', 0):.3f}"
    )

    return TickerResult(
        ticker=ticker,
        df=df,
        snapshot=snapshot,
        stages_completed=stages,
    )


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT BUILDER
# ═══════════════════════════════════════════════════════════════

def _build_snapshot(ticker: str, df: pd.DataFrame) -> dict:
    """
    Extract latest-row values into a flat dict.

    Used by the orchestrator for quick-access summaries and by
    the report generators for display.  Downstream modules may
    enrich this with allocation fields (shares, dollar_alloc,
    weight_pct, category, bucket, themes).

    The snapshot includes nested dicts ``sub_scores``,
    ``indicators``, and ``rs`` for compatibility with the
    recommendation report format.
    """
    if df.empty:
        return {"ticker": ticker, "error": "empty DataFrame"}

    last = df.iloc[-1]
    close = _f(last, "close", 0.0)
    date = df.index[-1]

    # ── ATR-based stop ────────────────────────────────────────
    atr_period = INDICATOR_PARAMS["atr_period"]
    atr_col = f"atr_{atr_period}"
    atr_pct_col = f"{atr_col}_pct"
    atr_val = _f(last, atr_col, 0.0)

    stop_price = None
    risk_per_share = None
    if close > 0 and atr_val > 0:
        stop_price = round(close - ATR_STOP_MULT * atr_val, 4)
        risk_per_share = round(ATR_STOP_MULT * atr_val, 4)

    # ── Best available composite score ────────────────────────
    # score_adjusted includes sector tailwind; falls back to
    # score_composite if sector merge didn't run.
    composite = _f(last, "score_adjusted", None)
    if composite is None:
        composite = _f(last, "score_composite", 0.0)

    # ── Simplified per-ticker signal ──────────────────────────
    # This is NOT the final portfolio signal (that comes from
    # output/signals.py cross-sectional logic).  This is the
    # per-ticker quality assessment from strategy/signals.py.
    sig_confirmed = _i(last, "sig_confirmed", 0)
    sig_exit = _i(last, "sig_exit", 0)

    if sig_exit:
        action = "SELL"
    elif sig_confirmed:
        action = "BUY"
    else:
        action = "HOLD"

    # ── Confidence proxy ──────────────────────────────────────
    # Map composite [0.5, 1.0] → confidence [0.0, 1.0].
    # Below 0.5 → 0 confidence.
    confidence = max(0.0, min(1.0, (composite - 0.5) * 2.0))

    # ── Indicator column names from config ────────────────────
    rsi_col = f"rsi_{INDICATOR_PARAMS['rsi_period']}"
    adx_col = f"adx_{INDICATOR_PARAMS['adx_period']}"
    vol_col = f"realized_vol_{INDICATOR_PARAMS['realized_vol_window']}d"

    return {
        # ── Identity ──────────────────────────────────────
        "ticker":       ticker,
        "date":         date,
        "close":        round(close, 4),

        # ── Composite score (best available) ──────────────
        "composite":    round(composite, 4),
        "confidence":   round(confidence, 4),
        "signal":       action,

        # ── Pillar scores ─────────────────────────────────
        "score_composite":      round(_f(last, "score_composite", 0), 4),
        "score_adjusted":       round(_f(last, "score_adjusted", 0), 4) if "score_adjusted" in df.columns else None,
        "score_rotation":       round(_f(last, "score_rotation", 0), 4),
        "score_momentum":       round(_f(last, "score_momentum", 0), 4),
        "score_volatility":     round(_f(last, "score_volatility", 0), 4),
        "score_microstructure": round(_f(last, "score_microstructure", 0), 4),
        "score_breadth":        round(_f(last, "score_breadth", 0), 4) if "score_breadth" in df.columns else None,
        "score_percentile":     round(_f(last, "score_percentile", 0), 4),

        # ── Sub-scores (recommendations.py compatibility) ─
        "sub_scores": {
            "trend":        round(_f(last, "score_rotation", 0), 4),
            "momentum":     round(_f(last, "score_momentum", 0), 4),
            "volatility":   round(_f(last, "score_volatility", 0), 4),
            "rel_strength": round(_f(last, "score_rotation", 0), 4),
        },

        # ── Relative strength ─────────────────────────────
        "rs": {
            "rs_ratio":      round(_f(last, "rs_raw", 1.0), 6),
            "rs_percentile": round(_f(last, "score_percentile", 0), 4),
            "rs_regime":     _s(last, "rs_regime", "unknown"),
            "rs_zscore":     round(_f(last, "rs_zscore", 0), 4),
            "rs_momentum":   round(_f(last, "rs_momentum", 0), 6),
        },
        "rs_regime":        _s(last, "rs_regime", "unknown"),
        "rs_zscore":        round(_f(last, "rs_zscore", 0), 4),

        # ── Key indicators ────────────────────────────────
        "indicators": {
            "rsi":             round(_f(last, rsi_col, 50), 2),
            "adx":             round(_f(last, adx_col, 0), 2),
            "macd_line":       round(_f(last, "macd_line", 0), 4),
            "macd_signal":     round(_f(last, "macd_signal", 0), 4),
            "macd_hist":       round(_f(last, "macd_hist", 0), 4),
            "atr":             round(atr_val, 4),
            "atr_pct":         round(_f(last, atr_pct_col, 0), 4),
            "realized_vol":    round(_f(last, vol_col, 0), 4),
            "relative_volume": round(_f(last, "relative_volume", 1), 4),
            "obv_slope":       round(_f(last, "obv_slope_10d", 0), 4),
        },

        # ── Sector context ────────────────────────────────
        "sector_name":     _s(last, "sector_name", None),
        "sect_rs_regime":  _s(last, "sect_rs_regime", None),
        "sect_rs_rank":    _f(last, "sect_rs_rank", None),
        "sector_tailwind": round(_f(last, "sector_tailwind", 0), 4) if "sector_tailwind" in df.columns else 0.0,

        # ── Per-ticker signal gates ───────────────────────
        "sig_confirmed":    sig_confirmed,
        "sig_exit":         sig_exit,
        "sig_reason":       _s(last, "sig_reason", "no_signal"),
        "sig_position_pct": round(_f(last, "sig_position_pct", 0), 4) if "sig_position_pct" in df.columns else 0.0,

        # ── Risk ──────────────────────────────────────────
        "stop_price":      stop_price,
        "risk_per_share":  risk_per_share,

        # ── Metadata (enriched by orchestrator) ───────────
        "bars_used":           len(df),
        "breadth_available":   bool(_f(last, "breadth_available", False)) if "breadth_available" in df.columns else False,
        "category":            None,     # set by orchestrator
        "bucket":              None,     # set by orchestrator
        "themes":              [],       # set by orchestrator
        "shares":              None,     # set by orchestrator
        "dollar_alloc":        None,     # set by orchestrator
        "weight_pct":          None,     # set by orchestrator
    }


# ═══════════════════════════════════════════════════════════════
#  VALUE EXTRACTION HELPERS
# ═══════════════════════════════════════════════════════════════

def _f(row: pd.Series, col: str, default: float | None = 0.0) -> float | None:
    """
    Extract a float value from a pandas Series row.

    Handles NaN, None, and missing columns gracefully.
    Returns ``default`` when the value is missing or not numeric.
    """
    val = row.get(col)
    if val is None:
        return default
    try:
        fval = float(val)
        if np.isnan(fval):
            return default
        return fval
    except (TypeError, ValueError):
        return default


def _s(row: pd.Series, col: str, default: str | None = "unknown") -> str | None:
    """
    Extract a string value from a pandas Series row.

    Handles NaN, None, and missing columns gracefully.
    Returns ``default`` when the value is missing.
    """
    val = row.get(col)
    if val is None:
        return default
    if isinstance(val, float) and np.isnan(val):
        return default
    return str(val)


def _i(row: pd.Series, col: str, default: int = 0) -> int:
    """Extract an int value from a pandas Series row."""
    val = row.get(col)
    if val is None:
        return default
    try:
        fval = float(val)
        if np.isnan(fval):
            return default
        return int(fval)
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════
#  BATCH RUNNER
# ═══════════════════════════════════════════════════════════════

def run_batch(
    universe: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    *,
    breadth: pd.DataFrame | None = None,
    breadth_scores_panel: pd.DataFrame | None = None,
    sector_rs: pd.DataFrame | None = None,
    as_of: pd.Timestamp | None = None,
    skip_benchmark: bool = True,
    benchmark_ticker: str = "SPY",
    progress: bool = True,
) -> dict[str, TickerResult]:
    """
    Run the single-ticker pipeline for every ticker in the
    universe.

    This is a convenience wrapper around ``run_ticker()`` that
    handles breadth-score extraction and progress logging.  The
    orchestrator may call this directly or implement its own
    loop with additional enrichment.

    Parameters
    ----------
    universe : dict
        {ticker: OHLCV DataFrame} for all symbols.
    bench_df : pd.DataFrame
        Benchmark OHLCV (e.g. SPY).
    breadth : pd.DataFrame, optional
        Universe-level breadth from ``compute_all_breadth()``.
    breadth_scores_panel : pd.DataFrame, optional
        Output of ``breadth_to_pillar_scores()`` — DataFrame
        with columns = tickers, values = 0–100 daily scores.
        Each ticker gets its own column as ``breadth_scores``
        argument to ``run_ticker()``.
    sector_rs : pd.DataFrame, optional
        Output of ``compute_all_sector_rs()`` — MultiIndex
        (date, sector) panel.
    as_of : pd.Timestamp, optional
        Cut-off date for backtesting.
    skip_benchmark : bool
        If True, skip the benchmark ticker (e.g. SPY) to avoid
        computing RS of SPY vs itself.  Default True.
    benchmark_ticker : str
        Benchmark symbol to skip.  Default ``"SPY"``.
    progress : bool
        Log progress every 10 tickers.  Default True.

    Returns
    -------
    dict[str, TickerResult]
        {ticker: TickerResult} for every processed ticker.
    """
    results: dict[str, TickerResult] = {}
    total = len(universe)
    ok = 0
    errors = 0
    skipped = 0

    for i, (ticker, ohlcv) in enumerate(universe.items(), 1):

        # Skip benchmark (RS of SPY vs SPY is meaningless)
        if skip_benchmark and ticker == benchmark_ticker:
            skipped += 1
            continue

        if progress and (i % 10 == 0 or i == 1 or i == total):
            logger.info(
                f"  [{i}/{total}] {ticker}..."
            )

        # Extract per-ticker breadth score Series if available
        b_scores = None
        if (
            breadth_scores_panel is not None
            and ticker in breadth_scores_panel.columns
        ):
            b_scores = breadth_scores_panel[ticker]

        result = run_ticker(
            ticker=ticker,
            ohlcv=ohlcv,
            bench_df=bench_df,
            breadth=breadth,
            breadth_scores=b_scores,
            sector_rs=sector_rs,
            as_of=as_of,
        )

        results[ticker] = result

        if result.ok:
            ok += 1
        else:
            errors += 1
            logger.debug(f"  {ticker}: {result.error}")

    logger.info(
        f"Batch complete: {ok} succeeded, "
        f"{errors} errors, {skipped} skipped "
        f"(of {total} total)"
    )

    return results


# ═══════════════════════════════════════════════════════════════
#  RESULT EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def results_to_scored_universe(
    results: dict[str, TickerResult],
) -> dict[str, pd.DataFrame]:
    """
    Extract ``{ticker: enriched_df}`` from results, keeping
    only successful tickers.

    The returned dict is ready to pass directly to:
      - ``output.rankings.compute_all_rankings()``
      - ``strategy.portfolio.build_portfolio()``
    """
    return {
        ticker: r.df
        for ticker, r in results.items()
        if r.ok
    }


def results_to_snapshots(
    results: dict[str, TickerResult],
) -> list[dict]:
    """
    Extract snapshot dicts from results, keeping only
    successful tickers.  Sorted by composite score descending.

    Useful for feeding into the recommendation report.
    """
    snapshots = [
        r.snapshot
        for r in results.values()
        if r.ok
    ]
    snapshots.sort(
        key=lambda s: s.get("composite", 0),
        reverse=True,
    )
    return snapshots


def results_errors(
    results: dict[str, TickerResult],
) -> list[str]:
    """
    Collect error messages from failed tickers.

    Returns list of ``"TICKER: error message"`` strings.
    """
    return [
        f"{ticker}: {r.error}"
        for ticker, r in results.items()
        if r.error is not None
    ]
    
##################################################
"""
PORTFOLIO:
----------------------------------

"""
"""
portfolio/backtest.py
---------------------
Historical simulation engine.

Takes the output of ``compute_all_signals()`` — the full
MultiIndex (date, ticker) panel with BUY / HOLD / SELL /
NEUTRAL signals and signal strengths — and simulates
portfolio performance day by day.

Architecture
────────────
  signals_df (date × ticker panel)
       ↓
  for each date:
       mark_to_market()          update portfolio value
       process_exits()           sell SELL signals
       compute_target_weights()  sizing for active set
       process_entries()         buy new BUY signals
       check_rebalance()         drift-based rebalance
       record_state()            equity, positions, trades
       ↓
  BacktestResult
       ↓
  compute_performance_metrics()  Sharpe, CAGR, drawdown, etc.

Execution Model
───────────────
  Signals on day T are executed at day T's close price.
  The ``execution_delay`` parameter shifts execution to T+N.
  Commission and slippage are applied to each trade.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np
import pandas as pd

from portfolio.sizing import (
    SizingConfig,
    compute_target_weights,
)
from portfolio.rebalance import (
    RebalanceConfig,
    Trade,
    compute_drift,
    needs_rebalance,
    generate_trades,
)
from portfolio.risk import (
    compute_portfolio_risk,
    drawdown_stats,
)
from output.signals import BUY, HOLD, SELL, NEUTRAL


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestConfig:
    """Full backtest configuration."""

    initial_capital: float = 100_000.0

    sizing: SizingConfig = field(
        default_factory=SizingConfig,
    )
    rebalance: RebalanceConfig = field(
        default_factory=RebalanceConfig,
    )

    # Execution delay: 0 = same-day close, 1 = next-day close
    execution_delay: int = 0

    # Rebalance HOLD positions periodically
    rebalance_holds: bool = True


# ═══════════════════════════════════════════════════════════════
#  RESULT OBJECT
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestResult:
    """Complete backtest output."""

    equity_curve: pd.Series         # date → portfolio value
    daily_returns: pd.Series        # date → daily return
    positions: pd.DataFrame         # date × ticker → shares
    weights: pd.DataFrame           # date × ticker → weight
    trades: list[Trade]             # all trades executed
    metrics: dict                   # performance metrics
    config: BacktestConfig = field(
        default_factory=BacktestConfig,
    )


# ═══════════════════════════════════════════════════════════════
#  BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════

def run_backtest(
    signals_df: pd.DataFrame,
    config: BacktestConfig | None = None,
) -> BacktestResult:
    """
    Run a historical simulation over the signal panel.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``compute_all_signals()``.  Must contain
        columns: signal, signal_strength, close.
    config : BacktestConfig or None
        Simulation parameters.

    Returns
    -------
    BacktestResult
        Equity curve, positions, trades, and metrics.
    """
    if config is None:
        config = BacktestConfig()

    if signals_df.empty or "close" not in signals_df.columns:
        return _empty_result(config)

    # ── Extract price matrix ──────────────────────────────
    prices = signals_df["close"].unstack(level="ticker")
    dates = prices.index.sort_values()

    # ── State ─────────────────────────────────────────────
    cash: float = config.initial_capital
    holdings: dict[str, float] = {}   # ticker → shares

    equity_records: dict[pd.Timestamp, float] = {}
    position_records: list[dict] = []
    weight_records: list[dict] = []
    all_trades: list[Trade] = []

    # ── Signal matrix for delayed execution ───────────────
    signal_wide = signals_df["signal"].unstack(level="ticker")
    strength_wide = signals_df["signal_strength"].unstack(
        level="ticker",
    )

    # Shift for execution delay
    delay = config.execution_delay
    if delay > 0:
        signal_wide = signal_wide.shift(delay)
        strength_wide = strength_wide.shift(delay)

    # ── Day-by-day simulation ─────────────────────────────
    for date in dates:

        if date not in signal_wide.index:
            continue

        today_prices = prices.loc[date].dropna().to_dict()
        today_signals = signal_wide.loc[date].dropna().to_dict()
        today_strengths = (
            strength_wide.loc[date].dropna().to_dict()
        )

        if not today_prices:
            continue

        # ── 1. Mark to market ─────────────────────────────
        portfolio_value = cash
        for ticker, shares in holdings.items():
            price = today_prices.get(ticker)
            if price is not None:
                portfolio_value += shares * price

        # ── 2. Process SELL signals ───────────────────────
        sells = [
            t for t, s in today_signals.items()
            if s == SELL and t in holdings
        ]

        for ticker in sells:
            price = today_prices.get(ticker)
            if price is None or price <= 0:
                continue

            shares = holdings.pop(ticker)
            trade_value = shares * price
            commission = (
                trade_value * config.rebalance.commission_pct
            )
            slippage = (
                trade_value * config.rebalance.slippage_pct
            )
            cash += trade_value - commission - slippage

            all_trades.append(Trade(
                date=date,
                ticker=ticker,
                action="SELL",
                shares=shares,
                price=price,
                value=trade_value,
                commission=commission,
                slippage=slippage,
            ))

        # ── 3. Determine active set and target weights ────
        active = [
            t for t, s in today_signals.items()
            if s in (BUY, HOLD)
        ]

        if active:
            # Compute volatilities for vol-based sizing
            vols = _compute_volatilities(
                prices, date, active, config.sizing.vol_lookback,
            )

            target_weights = compute_target_weights(
                tickers=active,
                config=config.sizing,
                strengths={
                    t: today_strengths.get(t, 0.5)
                    for t in active
                },
                volatilities=vols,
            )
        else:
            target_weights = {}

        # ── 4. Recalculate portfolio value after sells ────
        portfolio_value = cash
        for ticker, shares in holdings.items():
            price = today_prices.get(ticker)
            if price is not None:
                portfolio_value += shares * price

        # ── 5. Check rebalance need ──────────────────────
        current_weights = {}
        if portfolio_value > 0:
            for ticker, shares in holdings.items():
                price = today_prices.get(ticker, 0)
                current_weights[ticker] = (
                    shares * price / portfolio_value
                )

        drift = compute_drift(current_weights, target_weights)

        # New BUY signals always trigger trades
        new_buys = [
            t for t, s in today_signals.items()
            if s == BUY and t not in holdings
        ]

        do_rebalance = (
            bool(new_buys)
            or (
                config.rebalance_holds
                and needs_rebalance(drift, config.rebalance)
            )
        )

        # ── 6. Generate and execute trades ────────────────
        if do_rebalance and target_weights:
            trades = generate_trades(
                current_positions=dict(holdings),
                target_weights=target_weights,
                prices=today_prices,
                portfolio_value=portfolio_value,
                date=date,
                config=config.rebalance,
            )

            for trade in trades:
                if trade.action == "SELL":
                    sold_shares = min(
                        trade.shares,
                        holdings.get(trade.ticker, 0),
                    )
                    if sold_shares > 0:
                        cost = (
                            trade.commission + trade.slippage
                        )
                        cash += sold_shares * trade.price - cost
                        holdings[trade.ticker] = (
                            holdings.get(trade.ticker, 0)
                            - sold_shares
                        )
                        if holdings[trade.ticker] <= 0.001:
                            holdings.pop(trade.ticker, None)

                        all_trades.append(Trade(
                            date=trade.date,
                            ticker=trade.ticker,
                            action="SELL",
                            shares=sold_shares,
                            price=trade.price,
                            value=sold_shares * trade.price,
                            commission=trade.commission,
                            slippage=trade.slippage,
                        ))

                elif trade.action == "BUY":
                    cost = (
                        trade.value
                        + trade.commission
                        + trade.slippage
                    )
                    if cost <= cash:
                        cash -= cost
                        holdings[trade.ticker] = (
                            holdings.get(trade.ticker, 0)
                            + trade.shares
                        )
                        all_trades.append(trade)

        # ── 7. End-of-day portfolio value ─────────────────
        # First pass: compute total portfolio value
        eod_value = cash
        pos_record = {"date": date, "_cash": cash}

        for ticker, shares in holdings.items():
            price = today_prices.get(ticker, 0)
            eod_value += shares * price
            pos_record[ticker] = shares

        # Second pass: compute weights using correct total
        wt_record = {"date": date}
        if eod_value > 0:
            for ticker, shares in holdings.items():
                price = today_prices.get(ticker, 0)
                wt_record[ticker] = (
                    (shares * price) / eod_value
                )

        equity_records[date] = eod_value
        position_records.append(pos_record)
        weight_records.append(wt_record)

    # ── Build result ──────────────────────────────────────
    equity = pd.Series(equity_records, name="equity")
    equity.index.name = "date"

    daily_ret = equity.pct_change().fillna(0)
    daily_ret.name = "return"

    positions_df = pd.DataFrame(position_records).set_index(
        "date",
    ).fillna(0)
    weights_df = pd.DataFrame(weight_records).set_index(
        "date",
    ).fillna(0)

    # Compute metrics
    metrics = compute_performance_metrics(
        equity, daily_ret, all_trades, config,
    )

    return BacktestResult(
        equity_curve=equity,
        daily_returns=daily_ret,
        positions=positions_df,
        weights=weights_df,
        trades=all_trades,
        metrics=metrics,
        config=config,
    )


# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE METRICS
# ═══════════════════════════════════════════════════════════════

def compute_performance_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    trades: list[Trade],
    config: BacktestConfig,
) -> dict:
    """
    Compute comprehensive performance metrics.

    Returns dict with: total_return, cagr, annual_volatility,
    sharpe_ratio, sortino_ratio, calmar_ratio, max_drawdown,
    max_dd_duration, current_drawdown, total_trades, win_rate,
    profit_factor, avg_win, avg_loss, total_commission,
    initial_capital, final_capital, peak_capital.
    """
    if equity.empty:
        return {}

    initial = config.initial_capital
    final = equity.iloc[-1]
    peak = equity.max()

    # Returns
    total_return = (final / initial) - 1
    n_days = len(equity)
    n_years = max(n_days / 252, 0.01)
    cagr = (final / initial) ** (1 / n_years) - 1

    # Volatility
    ann_vol = float(daily_returns.std() * np.sqrt(252))

    # Sharpe (assume rf = 0)
    mean_daily = daily_returns.mean()
    sharpe = (
        (mean_daily / daily_returns.std() * np.sqrt(252))
        if daily_returns.std() > 0 else 0.0
    )

    # Sortino (downside vol)
    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 0 else 0.001
    sortino = (
        mean_daily / down_std * np.sqrt(252)
        if down_std > 0 else 0.0
    )

    # Drawdown
    dd = drawdown_stats(equity)
    max_dd = dd.get("max_drawdown", 0)
    calmar = abs(cagr / max_dd) if max_dd < 0 else 0.0

    # Trade statistics
    trade_pnls = _compute_trade_pnls(trades)
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    n_trades = len(trade_pnls)
    win_rate = len(wins) / max(n_trades, 1)

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    profit_factor = gross_profit / gross_loss

    avg_win = np.mean(wins) if wins else 0.0
    avg_loss = np.mean(losses) if losses else 0.0

    total_commission = sum(
        t.commission + t.slippage for t in trades
    )

    return {
        "total_return": total_return,
        "cagr": cagr,
        "annual_volatility": ann_vol,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "max_drawdown": dd.get("max_drawdown", 0),
        "max_dd_duration": dd.get("max_dd_duration", 0),
        "current_drawdown": dd.get("current_drawdown", 0),
        "total_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "total_commission": total_commission,
        "initial_capital": initial,
        "final_capital": final,
        "peak_capital": peak,
        "n_days": n_days,
        "n_years": n_years,
    }


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _compute_volatilities(
    prices: pd.DataFrame,
    current_date: pd.Timestamp,
    tickers: list[str],
    lookback: int,
) -> dict[str, float]:
    """Compute annualised volatility for each ticker."""
    vols = {}
    for ticker in tickers:
        if ticker not in prices.columns:
            continue
        loc = prices.index.get_loc(current_date)
        start = max(0, loc - lookback)
        window = prices.iloc[start: loc + 1][ticker].dropna()
        if len(window) > 2:
            ret = window.pct_change().dropna()
            vols[ticker] = float(ret.std() * np.sqrt(252))
        else:
            vols[ticker] = 0.20  # default 20% vol
    return vols


def _compute_trade_pnls(trades: list[Trade]) -> list[float]:
    """
    Compute P&L for each round-trip trade pair.

    Matches BUY → SELL pairs per ticker in FIFO order and
    computes the percentage return for each closed trade.
    """
    open_trades: dict[str, list[Trade]] = {}
    pnls: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                entry_cost = (
                    entry.price
                    + entry.price * 0.0015  # approx costs
                )
                exit_net = (
                    trade.price
                    - trade.price * 0.0015
                )
                pnl = (exit_net / entry_cost) - 1
                pnls.append(pnl)

    return pnls


def _empty_result(config: BacktestConfig) -> BacktestResult:
    """Return an empty BacktestResult."""
    return BacktestResult(
        equity_curve=pd.Series(dtype=float),
        daily_returns=pd.Series(dtype=float),
        positions=pd.DataFrame(),
        weights=pd.DataFrame(),
        trades=[],
        metrics={},
        config=config,
    )
    

"""
portfolio/rebalance.py
----------------------
Rebalancing logic and trade generation.

Compares current portfolio weights to target weights and
generates a list of trades needed to align them, subject to
drift thresholds, minimum trade sizes, and transaction costs.

Pipeline
────────
  current_weights + target_weights
       ↓
  compute_drift()          — per-position drift
       ↓
  needs_rebalance()        — does max drift exceed threshold?
       ↓
  generate_trades()        — list of Trade objects
       ↓
  estimate_costs()         — commission + slippage
"""

from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class RebalanceConfig:
    """Rebalancing parameters."""

    # Drift threshold: rebalance when any position drifts
    # more than this from its target weight
    # Rebalance only when drift exceeds 15% (was 5%)
    drift_threshold: float = 0.15

    # Minimum trade as fraction of portfolio value
    min_trade_pct: float = 0.01

    # Transaction costs
    commission_pct: float = 0.001    # 10 bps
    slippage_pct: float = 0.0005     # 5 bps


# ═══════════════════════════════════════════════════════════════
#  TRADE OBJECT
# ═══════════════════════════════════════════════════════════════

@dataclass
class Trade:
    """A single trade to execute."""

    date: pd.Timestamp
    ticker: str
    action: str          # "BUY" or "SELL"
    shares: float
    price: float
    value: float         # shares × price (unsigned)
    commission: float
    slippage: float

    @property
    def total_cost(self) -> float:
        return self.commission + self.slippage

    @property
    def net_value(self) -> float:
        if self.action == "BUY":
            return -(self.value + self.total_cost)
        return self.value - self.total_cost


# ═══════════════════════════════════════════════════════════════
#  DRIFT COMPUTATION
# ═══════════════════════════════════════════════════════════════

def compute_drift(
    current_weights: dict[str, float],
    target_weights: dict[str, float],
) -> dict[str, float]:
    """
    Compute per-position drift from target.

    Positive drift = overweight, negative = underweight.
    """
    all_tickers = set(current_weights) | set(target_weights)
    drift = {}
    for t in all_tickers:
        cur = current_weights.get(t, 0.0)
        tgt = target_weights.get(t, 0.0)
        drift[t] = cur - tgt
    return drift


def needs_rebalance(
    drift: dict[str, float],
    config: RebalanceConfig | None = None,
) -> bool:
    """Check if any position's drift exceeds the threshold."""
    if config is None:
        config = RebalanceConfig()
    if not drift:
        return False
    max_drift = max(abs(d) for d in drift.values())
    return max_drift > config.drift_threshold


# ═══════════════════════════════════════════════════════════════
#  TRADE GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_trades(
    current_positions: dict[str, float],
    target_weights: dict[str, float],
    prices: dict[str, float],
    portfolio_value: float,
    date: pd.Timestamp,
    config: RebalanceConfig | None = None,
) -> list[Trade]:
    """
    Generate trades to move from current positions to targets.

    Parameters
    ----------
    current_positions : dict
        {ticker: n_shares} currently held.
    target_weights : dict
        {ticker: weight} target allocation (0–1).
    prices : dict
        {ticker: price} current prices.
    portfolio_value : float
        Total portfolio value (cash + positions).
    date : pd.Timestamp
        Trade date.
    config : RebalanceConfig or None
        Rebalancing parameters.

    Returns
    -------
    list[Trade]
        Sells first, then buys, each with cost estimates.
    """
    if config is None:
        config = RebalanceConfig()

    trades: list[Trade] = []
    min_trade_val = portfolio_value * config.min_trade_pct

    all_tickers = set(current_positions) | set(target_weights)

    sells: list[Trade] = []
    buys: list[Trade] = []

    for ticker in all_tickers:
        price = prices.get(ticker)
        if price is None or price <= 0:
            continue

        current_shares = current_positions.get(ticker, 0.0)
        current_value = current_shares * price

        target_value = portfolio_value * target_weights.get(
            ticker, 0.0
        )
        trade_value = target_value - current_value

        if abs(trade_value) < min_trade_val:
            continue

        trade_shares = abs(trade_value) / price
        abs_value = abs(trade_value)
        commission = abs_value * config.commission_pct
        slippage = abs_value * config.slippage_pct

        trade = Trade(
            date=date,
            ticker=ticker,
            action="BUY" if trade_value > 0 else "SELL",
            shares=trade_shares,
            price=price,
            value=abs_value,
            commission=commission,
            slippage=slippage,
        )

        if trade.action == "SELL":
            sells.append(trade)
        else:
            buys.append(trade)

    # Sells first to free up cash
    return sells + buys


def estimate_costs(
    trades: list[Trade],
) -> dict[str, float]:
    """Summarise transaction costs for a list of trades."""
    total_commission = sum(t.commission for t in trades)
    total_slippage = sum(t.slippage for t in trades)
    total_value = sum(t.value for t in trades)
    return {
        "n_trades": len(trades),
        "total_value": total_value,
        "total_commission": total_commission,
        "total_slippage": total_slippage,
        "total_cost": total_commission + total_slippage,
    }
    
    

"""
portfolio/risk.py
-----------------
Portfolio risk metrics and analysis.

Pure functions — no state.  Take an equity curve, daily
returns, or position data and return risk metrics.

Functions
─────────
  compute_drawdown()          drawdown series + stats
  compute_var()               historical Value at Risk
  compute_cvar()              Conditional VaR (exp. shortfall)
  concentration_risk()        HHI and top-weight metrics
  rolling_volatility()        rolling annualised vol
  compute_portfolio_risk()    master function → full risk dict
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  DRAWDOWN
# ═══════════════════════════════════════════════════════════════

def compute_drawdown(
    equity_curve: pd.Series,
) -> pd.DataFrame:
    """
    Compute drawdown series from an equity curve.

    Returns DataFrame with columns:
      equity         the input equity curve
      peak           running peak
      drawdown       fractional drawdown (negative)
      drawdown_pct   drawdown as percentage
    """
    if equity_curve.empty:
        return pd.DataFrame()

    peak = equity_curve.cummax()
    dd = (equity_curve - peak) / peak

    return pd.DataFrame({
        "equity": equity_curve,
        "peak": peak,
        "drawdown": dd,
        "drawdown_pct": dd * 100,
    })


def drawdown_stats(
    equity_curve: pd.Series,
) -> dict:
    """
    Summary drawdown statistics.

    Returns dict with: max_drawdown, max_dd_start, max_dd_end,
    max_dd_duration, current_drawdown, recovery_days.
    """
    if equity_curve.empty:
        return {}

    dd_df = compute_drawdown(equity_curve)
    dd = dd_df["drawdown"]

    max_dd = dd.min()
    max_dd_idx = dd.idxmin()

    # Peak before max drawdown
    peak_before = dd_df["peak"].loc[:max_dd_idx]
    if not peak_before.empty:
        peak_idx = equity_curve.loc[
            :max_dd_idx
        ].idxmax()
    else:
        peak_idx = dd.index[0]

    # Duration: count trading days from peak to trough
    if hasattr(peak_idx, "strftime"):
        duration = len(dd.loc[peak_idx:max_dd_idx])
    else:
        duration = 0

    # Current drawdown
    current_dd = dd.iloc[-1] if len(dd) > 0 else 0.0

    return {
        "max_drawdown": max_dd,
        "max_dd_start": peak_idx,
        "max_dd_end": max_dd_idx,
        "max_dd_duration": duration,
        "current_drawdown": current_dd,
    }


# ═══════════════════════════════════════════════════════════════
#  VALUE AT RISK
# ═══════════════════════════════════════════════════════════════

def compute_var(
    returns: pd.Series,
    confidence: float = 0.95,
) -> float:
    """
    Historical Value at Risk.

    Returns the loss threshold at the given confidence level
    (negative number — e.g. -0.02 means 2% daily loss).
    """
    if returns.empty:
        return 0.0
    return float(returns.quantile(1 - confidence))


def compute_cvar(
    returns: pd.Series,
    confidence: float = 0.95,
) -> float:
    """
    Conditional VaR (Expected Shortfall).

    Average loss in the worst (1 - confidence) tail.
    """
    if returns.empty:
        return 0.0
    var = compute_var(returns, confidence)
    tail = returns[returns <= var]
    return float(tail.mean()) if len(tail) > 0 else var


# ═══════════════════════════════════════════════════════════════
#  CONCENTRATION RISK
# ═══════════════════════════════════════════════════════════════

def concentration_risk(
    weights: dict[str, float],
) -> dict:
    """
    Concentration metrics for current portfolio weights.

    Returns dict with: hhi (Herfindahl-Hirschman Index),
    effective_n (equivalent number of equal positions),
    max_weight, max_ticker.
    """
    if not weights:
        return {
            "hhi": 0.0,
            "effective_n": 0,
            "max_weight": 0.0,
            "max_ticker": None,
        }

    vals = np.array(list(weights.values()))
    total = vals.sum()
    if total <= 0:
        return {
            "hhi": 0.0,
            "effective_n": 0,
            "max_weight": 0.0,
            "max_ticker": None,
        }

    w = vals / total
    hhi = float((w ** 2).sum())
    effective_n = 1.0 / hhi if hhi > 0 else 0
    max_w = float(w.max())
    max_ticker = list(weights.keys())[int(w.argmax())]

    return {
        "hhi": hhi,
        "effective_n": effective_n,
        "max_weight": max_w,
        "max_ticker": max_ticker,
    }


# ═══════════════════════════════════════════════════════════════
#  ROLLING VOLATILITY
# ═══════════════════════════════════════════════════════════════

def rolling_volatility(
    returns: pd.Series,
    window: int = 20,
    annualise: bool = True,
) -> pd.Series:
    """Rolling annualised volatility."""
    vol = returns.rolling(window, min_periods=max(window // 2, 2)).std()
    if annualise:
        vol = vol * np.sqrt(252)
    return vol


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_portfolio_risk(
    equity_curve: pd.Series,
    daily_returns: pd.Series | None = None,
    current_weights: dict[str, float] | None = None,
) -> dict:
    """
    Comprehensive portfolio risk metrics.

    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio value.
    daily_returns : pd.Series or None
        Daily returns (computed from equity if not provided).
    current_weights : dict or None
        Current position weights for concentration analysis.

    Returns
    -------
    dict
        Full risk metrics including drawdown, VaR, CVaR,
        volatility, and concentration.
    """
    if equity_curve.empty:
        return {}

    if daily_returns is None:
        daily_returns = equity_curve.pct_change().dropna()

    result: dict = {}

    # Drawdown
    result.update(drawdown_stats(equity_curve))

    # VaR / CVaR
    result["var_95"] = compute_var(daily_returns, 0.95)
    result["var_99"] = compute_var(daily_returns, 0.99)
    result["cvar_95"] = compute_cvar(daily_returns, 0.95)

    # Volatility
    if len(daily_returns) > 0:
        result["daily_volatility"] = float(daily_returns.std())
        result["annual_volatility"] = float(
            daily_returns.std() * np.sqrt(252)
        )
    else:
        result["daily_volatility"] = 0.0
        result["annual_volatility"] = 0.0

    # Skew / kurtosis
    if len(daily_returns) > 5:
        result["skewness"] = float(daily_returns.skew())
        result["kurtosis"] = float(daily_returns.kurtosis())

    # Concentration
    if current_weights:
        result["concentration"] = concentration_risk(
            current_weights
        )

    return result
    

"""
portfolio/sizing.py
-------------------
Position sizing algorithms.

Given a set of active tickers and their signal strengths,
compute target portfolio weights.  All methods respect
max / min position limits and normalise to a target exposure.

Methods
───────
  equal_weight        1/N for each active position
  score_weighted      proportional to signal_strength
  inverse_volatility  inversely proportional to recent vol
  risk_parity         equal risk contribution (approx)

Pipeline
────────
  active tickers + metadata
       ↓
  _raw_weights()     — method-specific raw weights
       ↓
  _apply_limits()    — clip to [min, max] per position
       ↓
  _normalise()       — scale so weights sum to target_exposure
       ↓
  compute_target_weights()  ← master function
"""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class SizingConfig:
    """Position sizing parameters."""

    method: str = "score_weighted"

    # Per-position limits (fraction of portfolio)
    max_position_pct: float = 0.25
    min_position_pct: float = 0.02

    # Total target equity exposure (rest stays cash)
    target_exposure: float = 1.00

    # Volatility lookback for vol-based methods
    vol_lookback: int = 20


# ═══════════════════════════════════════════════════════════════
#  RAW WEIGHT METHODS
# ═══════════════════════════════════════════════════════════════

def equal_weight(
    tickers: list[str],
    **kwargs,
) -> dict[str, float]:
    """1/N equal weight."""
    if not tickers:
        return {}
    w = 1.0 / len(tickers)
    return {t: w for t in tickers}


def score_weighted(
    tickers: list[str],
    strengths: dict[str, float] | None = None,
    **kwargs,
) -> dict[str, float]:
    """Weight proportional to signal strength."""
    if not tickers:
        return {}
    if strengths is None:
        return equal_weight(tickers)

    raw = {t: max(strengths.get(t, 0.0), 0.001) for t in tickers}
    total = sum(raw.values())
    if total <= 0:
        return equal_weight(tickers)
    return {t: v / total for t, v in raw.items()}


def inverse_volatility(
    tickers: list[str],
    volatilities: dict[str, float] | None = None,
    **kwargs,
) -> dict[str, float]:
    """Weight inversely proportional to volatility."""
    if not tickers:
        return {}
    if volatilities is None:
        return equal_weight(tickers)

    inv = {}
    for t in tickers:
        vol = volatilities.get(t, 0.0)
        inv[t] = 1.0 / max(vol, 0.001)

    total = sum(inv.values())
    if total <= 0:
        return equal_weight(tickers)
    return {t: v / total for t, v in inv.items()}


def risk_parity(
    tickers: list[str],
    volatilities: dict[str, float] | None = None,
    **kwargs,
) -> dict[str, float]:
    """
    Approximate risk parity: equal risk contribution.

    Uses inverse-variance weighting as a simple approximation
    (exact risk parity requires a covariance matrix and
    iterative optimisation).
    """
    if not tickers:
        return {}
    if volatilities is None:
        return equal_weight(tickers)

    inv_var = {}
    for t in tickers:
        vol = volatilities.get(t, 0.0)
        inv_var[t] = 1.0 / max(vol ** 2, 1e-8)

    total = sum(inv_var.values())
    if total <= 0:
        return equal_weight(tickers)
    return {t: v / total for t, v in inv_var.items()}


_METHODS = {
    "equal":              equal_weight,
    "equal_weight":       equal_weight,
    "score_weighted":     score_weighted,
    "inverse_volatility": inverse_volatility,
    "risk_parity":        risk_parity,
}


# ═══════════════════════════════════════════════════════════════
#  LIMIT AND NORMALISE
# ═══════════════════════════════════════════════════════════════

def _apply_limits(
    weights: dict[str, float],
    config: SizingConfig,
) -> dict[str, float]:
    """Clip weights to [min, max] and drop sub-minimum."""
    result = {}
    for t, w in weights.items():
        if w < config.min_position_pct:
            continue
        result[t] = min(w, config.max_position_pct)
    return result


def _normalise(
    weights: dict[str, float],
    target: float,
) -> dict[str, float]:
    """Scale weights to sum to target exposure."""
    total = sum(weights.values())
    if total <= 0 or not weights:
        return weights
    scale = target / total
    return {t: w * scale for t, w in weights.items()}


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_target_weights(
    tickers: list[str],
    config: SizingConfig | None = None,
    strengths: dict[str, float] | None = None,
    volatilities: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Compute target portfolio weights for active tickers.

    Parameters
    ----------
    tickers : list[str]
        Active tickers (BUY or HOLD signals).
    config : SizingConfig or None
        Sizing parameters.
    strengths : dict or None
        {ticker: signal_strength} for score-weighted method.
    volatilities : dict or None
        {ticker: annualised_vol} for vol-based methods.

    Returns
    -------
    dict[str, float]
        {ticker: target_weight} where weights sum to
        ``config.target_exposure`` (or less if positions
        are dropped for being below minimum).
    """
    if config is None:
        config = SizingConfig()
    if not tickers:
        return {}

    method_fn = _METHODS.get(config.method, score_weighted)

    raw = method_fn(
        tickers,
        strengths=strengths,
        volatilities=volatilities,
    )

    # First normalise to target, then clip, then re-normalise
    raw = _normalise(raw, config.target_exposure)
    clipped = _apply_limits(raw, config)

    if not clipped:
        return {}

    return _normalise(clipped, config.target_exposure)
    
#############################################################

""" 
SCRIPTS
"""
#!/usr/bin/env python3
"""
scripts/run_strategy.py
-----------------------
Unified CLI for the CASH strategy system.

Three execution modes across three markets (US, HK, India):

  top-down   — Sector rotation with RS-based stock selection (US)
               or composite relative-strength ranking (HK, India).
               Answers: "Where is the smart money flowing?"

  bottom-up  — Per-ticker technical scoring pipeline via the
               orchestrator.  Scores every ticker on momentum,
               trend, volume, breadth, and relative strength.
               Answers: "Which individual stocks look strongest?"

  full       — Combined pipeline: bottom-up feeds indicator data
               into top-down rotation for quality-filtered stock
               selection, then convergence merges both signal
               lists for maximum conviction.
               Answers: "What should I buy, sell, or hold?"

Usage
=====

  # ── Top-Down (sector rotation / RS ranking) ────────
  python -m scripts.run_strategy top-down --market US
  python -m scripts.run_strategy top-down --market US --quality --holdings NVDA,CRWD
  python -m scripts.run_strategy top-down --market HK
  python -m scripts.run_strategy top-down --market IN

  # ── Bottom-Up (per-ticker scoring) ─────────────────
  python -m scripts.run_strategy bottom-up --market US
  python -m scripts.run_strategy bottom-up --market HK
  python -m scripts.run_strategy bottom-up --market IN

  # ── Full Pipeline (combined) ────────────────────────
  python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG
  python -m scripts.run_strategy full --market ALL
  python -m scripts.run_strategy full --market ALL -o results/report.json

Architecture
============

  top-down
    └─ Load OHLCV → build price matrix
       ├─ US:    run_rotation()  → sector rankings + BUY/SELL/HOLD
       └─ HK/IN: composite_rs_all() → tiered RS ranking vs benchmark

  bottom-up
    └─ Orchestrator phases 0 → 1 → 2 → 3 → 4
       (data → breadth/context → per-ticker pipeline → rankings → reports)
       Rotation and convergence are skipped.

  full
    └─ Orchestrator.run_all()
       Phases 0 → 1 → 2 → 2.5 (rotation) → 2.75 (convergence) → 3 → 4
       Bottom-up indicator data feeds into rotation quality filter.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# ── Ensure project root is importable ─────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pipeline.orchestrator import (
    Orchestrator,
    PipelineResult,
    run_full_pipeline,
    run_multi_market_pipeline,
)
from strategy.rotation import (
    RotationConfig,
    RotationResult,
    composite_rs_all,
    run_rotation,
    print_result as print_rotation_result,
)
from strategy.rotation_filters import QualityConfig
from common.config import MARKET_CONFIG, ACTIVE_MARKETS

# Optional: convergence module for price matrix building
try:
    from strategy.convergence import build_price_matrix as _conv_build_prices
except ImportError:
    _conv_build_prices = None

log = logging.getLogger("run_strategy")

W = 80  # print width


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _resolve_markets(market_arg: str) -> list[str]:
    """Parse --market flag into a list of market codes."""
    if market_arg.upper() == "ALL":
        return list(ACTIVE_MARKETS)
    code = market_arg.upper()
    if code not in MARKET_CONFIG:
        available = ", ".join(MARKET_CONFIG.keys())
        raise SystemExit(
            f"Unknown market '{code}'.  Available: {available}, ALL"
        )
    return [code]


def _parse_holdings(holdings_str: str) -> list[str]:
    """Parse comma-separated holdings string."""
    if not holdings_str:
        return []
    return [t.strip().upper() for t in holdings_str.split(",") if t.strip()]


def _build_price_matrix(ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a wide close-price matrix from {ticker: OHLCV DataFrame}.

    Delegates to ``strategy.convergence.build_price_matrix()`` when
    available; otherwise uses a simple fallback.
    """
    if _conv_build_prices is not None:
        return _conv_build_prices(ohlcv)

    series: dict[str, pd.Series] = {}
    for ticker, df in ohlcv.items():
        if df is not None and not df.empty and "close" in df.columns:
            series[ticker] = df["close"]
    if not series:
        return pd.DataFrame()
    return pd.DataFrame(series).sort_index().ffill()


def _compute_indicators_inline(
    ohlcv: dict[str, pd.DataFrame],
    tickers: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Compute technical indicators for quality filtering in
    top-down standalone mode.

    When the bottom-up pipeline is not run (top-down only),
    this bridges the gap by computing indicators inline from
    raw OHLCV data.  The quality filter in run_rotation() reads
    columns like ema_30, sma_50, rsi_14, adx_14, macd_hist,
    obv_slope_10d, relative_volume, and atr_14_pct.
    """
    try:
        from compute.indicators import compute_all_indicators
    except ImportError:
        log.warning(
            "compute.indicators not importable — "
            "quality filter will be disabled"
        )
        return {}

    if tickers is None:
        tickers = list(ohlcv.keys())

    result: dict[str, pd.DataFrame] = {}
    n_fail = 0

    for ticker in tickers:
        df = ohlcv.get(ticker)
        if df is None or df.empty or len(df) < 60:
            continue
        try:
            enriched = compute_all_indicators(df)
            if enriched is not None and not enriched.empty:
                result[ticker] = enriched
        except Exception as exc:
            log.debug("Indicators failed for %s: %s", ticker, exc)
            n_fail += 1

    log.info(
        "Inline indicators: %d OK, %d failed out of %d",
        len(result), n_fail, len(tickers),
    )
    return result


def _market_suffix(market: str) -> str:
    """Return ticker suffix for filtering by market."""
    return {
        "HK": ".HK",
        "IN": ".NS",
    }.get(market, "")


def _is_market_ticker(ticker: str, market: str) -> bool:
    """Check if a ticker belongs to the given market."""
    if market == "US":
        return not ticker.endswith(".HK") and not ticker.endswith(".NS")
    suffix = _market_suffix(market)
    return ticker.endswith(suffix) if suffix else True


# ═══════════════════════════════════════════════════════════════
#  TOP-DOWN MODE
# ═══════════════════════════════════════════════════════════════

def _run_top_down(args) -> dict[str, Any]:
    """
    Top-down analysis for requested markets.

    US: Full sector rotation via run_rotation().
        Optionally with quality filter (--quality) which
        computes indicators inline and gates/scores candidates.

    HK/IN: Composite relative-strength ranking of all tickers
           vs the local benchmark (2800.HK / NIFTYBEES.NS).
           Tickers are split into top / middle / bottom tiers.
    """
    markets = _resolve_markets(args.market)
    holdings = _parse_holdings(args.holdings)
    results: dict[str, Any] = {}

    for market in markets:
        mcfg = MARKET_CONFIG[market]
        benchmark = mcfg.get("benchmark", "SPY")
        engines = mcfg.get("engines", ["scoring"])
        t0 = time.perf_counter()

        _print_header(f"TOP-DOWN ANALYSIS  —  {market}", benchmark=benchmark)

        # ── Load data via orchestrator ────────────────────
        orch = Orchestrator(market=market, lookback_days=args.lookback)
        orch.load_data()

        prices = _build_price_matrix(orch._ohlcv)
        if prices.empty:
            log.error("[%s] Empty price matrix — skipping", market)
            continue
        if benchmark not in prices.columns:
            log.error(
                "[%s] Benchmark %s not in price data — skipping",
                market, benchmark,
            )
            continue

        n_days = len(prices)
        n_tickers = prices.shape[1]
        date_range = (
            f"{prices.index[0].strftime('%Y-%m-%d')} to "
            f"{prices.index[-1].strftime('%Y-%m-%d')}"
        )
        print(f"  Data: {n_days} trading days × {n_tickers} tickers")
        print(f"  Range: {date_range}")

        if "rotation" in engines:
            # ── US: Full sector rotation ──────────────────
            indicator_data: dict[str, pd.DataFrame] | None = None

            if args.quality:
                print(f"\n  Computing indicators for quality filter...")
                indicator_data = _compute_indicators_inline(orch._ohlcv)
                if not indicator_data:
                    indicator_data = None
                    print(f"  ⚠  No indicator data — falling back to RS-only")
                else:
                    print(f"  ✓  Indicators for {len(indicator_data)} tickers")

            qcfg = QualityConfig(
                enabled=bool(args.quality and indicator_data),
            )
            if args.quality_weight is not None:
                qcfg.w_quality = args.quality_weight
                qcfg.w_rs = 1.0 - args.quality_weight

            rcfg_kw: dict[str, Any] = {
                "benchmark": benchmark,
                "quality": qcfg,
            }
            if args.stocks_per_sector is not None:
                rcfg_kw["stocks_per_sector"] = args.stocks_per_sector
            if args.max_positions is not None:
                rcfg_kw["max_total_positions"] = args.max_positions

            rcfg = RotationConfig(**rcfg_kw)

            rotation_result = run_rotation(
                prices=prices,
                current_holdings=holdings,
                config=rcfg,
                indicator_data=indicator_data,
            )
            print_rotation_result(rotation_result)
            results[market] = rotation_result

        else:
            # ── HK / IN: RS ranking vs benchmark ─────────
            _print_rs_ranking(prices, benchmark, market)
            config = RotationConfig(benchmark=benchmark)
            rs_all, raw = composite_rs_all(prices, config)
            results[market] = {"rs_ranking": rs_all, "raw_returns": raw}

        elapsed = time.perf_counter() - t0
        print(f"\n  ⏱  {market} top-down: {elapsed:.1f}s")

    return results


def _print_rs_ranking(
    prices: pd.DataFrame,
    benchmark: str,
    market: str,
) -> None:
    """
    Print tiered RS ranking for non-rotation markets (HK, India).

    Tickers are ranked by composite relative strength vs the local
    benchmark, then split into top, middle, and bottom thirds.
    """
    config = RotationConfig(benchmark=benchmark)
    rs_all, raw = composite_rs_all(prices, config)

    # Filter to this market's tickers (exclude benchmark)
    tickers = [
        t for t in rs_all.index
        if _is_market_ticker(t, market) and t != benchmark
    ]

    if not tickers:
        print(f"\n  No tickers found for market {market}")
        return

    filtered = rs_all.loc[
        rs_all.index.isin(tickers)
    ].sort_values(ascending=False)

    n_total = len(filtered)
    n_top = max(1, n_total // 3)
    n_bot = max(1, n_total // 3)

    print(f"\n  RELATIVE STRENGTH vs {benchmark}  ({n_total} tickers)")
    print(f"  {'─' * (W - 4)}")

    # ── Top Tier ──────────────────────────────────────────
    top = filtered.head(n_top)
    print(f"\n  🟢 TOP TIER  ({len(top)} tickers — strongest RS)")
    print(f"  {'─' * (W - 4)}")
    for i, (ticker, rs) in enumerate(top.items()):
        rets = raw.get(ticker, {})
        rets_str = "  ".join(
            f"{p}d:{r:+.1%}" for p, r in sorted(rets.items())
        )
        print(f"   {i + 1:3d}. {ticker:16s}  RS {rs:+.4f}   {rets_str}")

    # ── Middle Tier ───────────────────────────────────────
    mid_start = n_top
    mid_end = n_total - n_bot
    mid = filtered.iloc[mid_start:mid_end]
    if not mid.empty:
        print(f"\n  ⚪ MIDDLE TIER  ({len(mid)} tickers)")
        print(f"  {'─' * (W - 4)}")
        for i, (ticker, rs) in enumerate(mid.items()):
            rets = raw.get(ticker, {})
            rets_str = "  ".join(
                f"{p}d:{r:+.1%}" for p, r in sorted(rets.items())
            )
            print(
                f"   {mid_start + i + 1:3d}. {ticker:16s}  "
                f"RS {rs:+.4f}   {rets_str}"
            )

    # ── Bottom Tier ───────────────────────────────────────
    bot = filtered.tail(n_bot)
    if not bot.empty:
        print(f"\n  🔴 BOTTOM TIER  ({len(bot)} tickers — weakest RS)")
        print(f"  {'─' * (W - 4)}")
        for i, (ticker, rs) in enumerate(bot.items()):
            rets = raw.get(ticker, {})
            rets_str = "  ".join(
                f"{p}d:{r:+.1%}" for p, r in sorted(rets.items())
            )
            idx = n_total - n_bot + i + 1
            print(
                f"   {idx:3d}. {ticker:16s}  "
                f"RS {rs:+.4f}   {rets_str}"
            )

    # ── Benchmark reference ───────────────────────────────
    print(f"\n  {'─' * (W - 4)}")
    bench_ret = raw.get(benchmark, {})
    bench_str = "  ".join(
        f"{p}d:{r:+.1%}" for p, r in sorted(bench_ret.items())
    )
    print(f"  Benchmark {benchmark}: {bench_str}")
    print(f"{'═' * W}")


# ═══════════════════════════════════════════════════════════════
#  BOTTOM-UP MODE
# ═══════════════════════════════════════════════════════════════

def _run_bottom_up(args) -> dict[str, PipelineResult]:
    """
    Per-ticker scoring pipeline for requested markets.

    Runs orchestrator phases 0 → 1 → 2 → 3 → 4.
    Rotation (phase 2.5) and convergence (phase 2.75) are
    skipped — use 'full' mode for those.
    """
    markets = _resolve_markets(args.market)
    results: dict[str, PipelineResult] = {}

    for market in markets:
        mcfg = MARKET_CONFIG[market]
        t0 = time.perf_counter()

        _print_header(
            f"BOTTOM-UP SCORING  —  {market}",
            benchmark=mcfg.get("benchmark", "?"),
            extra=f"Universe: {len(mcfg.get('universe', []))} tickers",
        )

        orch = Orchestrator(
            market=market,
            lookback_days=args.lookback,
            enable_backtest=False,
        )

        # Run phases 0 → 1 → 2 → 3 → 4 (no rotation, no convergence)
        orch.load_data()
        orch.compute_universe_context()
        orch.run_tickers()
        orch.cross_sectional_analysis()
        result = orch.generate_reports()

        _print_bottom_up_result(result, market)
        results[market] = result

        elapsed = time.perf_counter() - t0
        print(f"\n  ⏱  {market} bottom-up: {elapsed:.1f}s total")

    return results


def _print_bottom_up_result(
    result: PipelineResult,
    market: str,
    top_n: int = 15,
) -> None:
    """Pretty-print bottom-up scoring results."""
    snaps = result.snapshots
    if not snaps:
        print("\n  No scored tickers.")
        return

    # ── Pipeline stats ────────────────────────────────────
    print(f"\n  Scored: {result.n_tickers}  │  "
          f"Errors: {result.n_errors}  │  "
          f"Compute time: {result.total_time:.1f}s")

    # ── Breadth regime ────────────────────────────────────
    if result.breadth is not None and not result.breadth.empty:
        if "breadth_regime" in result.breadth.columns:
            regime = result.breadth["breadth_regime"].iloc[-1]
            print(f"  Breadth regime: {regime}")

    # ── Top N tickers ─────────────────────────────────────
    show_n = min(top_n, len(snaps))
    print(f"\n  TOP {show_n} SCORED TICKERS")
    print(f"  {'─' * (W - 4)}")
    print(
        f"  {'#':>3s}  {'Ticker':8s}  {'Score':>6s}  "
        f"{'Signal':>7s}  {'RS':>7s}  {'RSI':>5s}  {'ADX':>5s}"
    )
    print(f"  {'─' * (W - 4)}")

    for i, snap in enumerate(snaps[:show_n]):
        ticker = snap.get("ticker", "?")
        score = snap.get("composite_score", 0.0) or 0.0
        signal = snap.get("signal", "?")
        rs = snap.get("rs_score", 0.0) or 0.0
        rsi = snap.get("rsi_14", 0.0) or 0.0
        adx = snap.get("adx_14", 0.0) or 0.0

        sig_icon = {
            "BUY": "🟢", "SELL": "🔴", "HOLD": "⚪",
        }.get(str(signal), "⚪")

        print(
            f"  {i + 1:3d}  {ticker:8s}  {score:6.3f}  "
            f"{sig_icon}{str(signal):>5s}  "
            f"{rs:+7.4f}  {rsi:5.1f}  {adx:5.1f}"
        )

    # ── Signal summary ────────────────────────────────────
    buys = [s for s in snaps if s.get("signal") == "BUY"]
    sells = [s for s in snaps if s.get("signal") == "SELL"]
    holds = len(snaps) - len(buys) - len(sells)

    print(f"\n  {'─' * (W - 4)}")
    print(
        f"  Signals: {len(buys)} BUY  │  {holds} HOLD  │  "
        f"{len(sells)} SELL  │  {len(snaps)} total"
    )

    # ── Portfolio summary ─────────────────────────────────
    if result.portfolio:
        meta = result.portfolio.get("metadata", {})
        n_pos = meta.get("num_holdings", 0)
        cash_pct = meta.get("cash_pct", 0)
        print(
            f"  Portfolio: {n_pos} positions  │  "
            f"Cash: {cash_pct:.1%}"
        )

    print(f"{'═' * W}")


# ═══════════════════════════════════════════════════════════════
#  FULL MODE
# ═══════════════════════════════════════════════════════════════

def _run_full(args) -> dict[str, PipelineResult]:
    """
    Full combined pipeline for requested markets.

    Delegates to the orchestrator's run_all() which executes:
      Phase 0:    Data loading
      Phase 1:    Universe breadth + sector RS
      Phase 2:    Per-ticker scoring pipeline
      Phase 2.5:  Sector rotation (US — with quality filter)
      Phase 2.75: Convergence merge (scoring + rotation)
      Phase 3:    Cross-sectional rankings + portfolio
      Phase 4:    Reports

    Bottom-up indicator data from Phase 2 feeds into the rotation
    engine's quality filter in Phase 2.5, giving quality-gated,
    RS+quality blended stock selection within leading sectors.
    """
    markets = _resolve_markets(args.market)
    holdings = _parse_holdings(args.holdings)
    results: dict[str, PipelineResult] = {}

    if len(markets) > 1:
        # ── Multi-market ──────────────────────────────────
        holdings_map: dict[str, list[str]] | None = None
        if holdings:
            # Route holdings to US (rotation only applies there)
            holdings_map = {m: [] for m in markets}
            holdings_map["US"] = holdings

        raw = run_multi_market_pipeline(
            active_markets=markets,
            lookback_days=args.lookback,
            current_holdings=holdings_map,
        )
        for market, result in raw.items():
            _print_full_result(result, market)
            results[market] = result
    else:
        # ── Single market ─────────────────────────────────
        market = markets[0]
        result = run_full_pipeline(
            market=market,
            lookback_days=args.lookback,
            current_holdings=holdings,
        )
        _print_full_result(result, market)
        results[market] = result

    return results


def _print_full_result(result: PipelineResult, market: str) -> None:
    """Pretty-print full pipeline results."""
    _print_header(
        f"FULL PIPELINE  —  {market}",
        extra=result.summary(),
    )

    # ── Bottom-up scoring summary ─────────────────────────
    _print_bottom_up_result(result, market, top_n=10)

    # ── Rotation result (US, if available) ────────────────
    if result.rotation_result is not None:
        print(f"\n{'─' * W}")
        print(f"  SECTOR ROTATION OVERLAY")
        print(f"{'─' * W}")
        print_rotation_result(result.rotation_result)

    # ── Convergence summary ───────────────────────────────
    if result.convergence is not None:
        _print_convergence(result.convergence)

    print(f"\n{'═' * W}")


def _print_convergence(conv: Any) -> None:
    """Print convergence merge results (defensive to unknown structure)."""
    print(f"\n{'─' * W}")
    print(f"  CONVERGENCE MERGE")
    print(f"{'─' * W}")

    # Strong buys
    strong = getattr(conv, "strong_buys", [])
    if strong:
        print(f"\n  🟢 STRONG BUYS  ({len(strong)})")
        for sb in strong[:10]:
            ticker = getattr(sb, "ticker", str(sb))
            adj = getattr(sb, "adjusted_score", None)
            score_str = f"  adj={adj:.3f}" if adj is not None else ""
            print(f"     {ticker}{score_str}")
        if len(strong) > 10:
            print(f"     ... and {len(strong) - 10} more")

    # Conflicts
    conflicts = getattr(conv, "conflicts", [])
    if conflicts:
        print(f"\n  ⚠  CONFLICTS  ({len(conflicts)})")
        for c in conflicts[:5]:
            ticker = getattr(c, "ticker", str(c))
            reason = getattr(c, "reason", "")
            reason_str = f"  — {reason}" if reason else ""
            print(f"     {ticker}{reason_str}")

    # Summary line
    summary_fn = getattr(conv, "summary", None)
    if callable(summary_fn):
        print(f"\n  {summary_fn()}")


# ═══════════════════════════════════════════════════════════════
#  PRINTING HELPERS
# ═══════════════════════════════════════════════════════════════

def _print_header(
    title: str,
    benchmark: str | None = None,
    extra: str | None = None,
) -> None:
    """Print a section header."""
    print(f"\n{'═' * W}")
    print(f"  {title}  —  {date.today()}")
    if benchmark:
        print(f"  Benchmark: {benchmark}")
    if extra:
        print(f"  {extra}")
    print(f"{'═' * W}")


# ═══════════════════════════════════════════════════════════════
#  JSON EXPORT
# ═══════════════════════════════════════════════════════════════

def _export_json(
    results: dict[str, Any],
    output_path: str,
    mode: str,
) -> None:
    """
    Serialise results to JSON for downstream consumption.

    Handles three result types:
      - RotationResult   (top-down US)
      - PipelineResult   (bottom-up / full)
      - dict with rs_ranking  (top-down HK/IN)
    """
    data: dict[str, Any] = {
        "mode": mode,
        "run_date": str(date.today()),
        "markets": {},
    }

    for market, result in results.items():
        if isinstance(result, RotationResult):
            data["markets"][market] = _rotation_to_dict(result)
        elif isinstance(result, PipelineResult):
            data["markets"][market] = _pipeline_to_dict(result)
        elif isinstance(result, dict) and "rs_ranking" in result:
            data["markets"][market] = _rs_ranking_to_dict(result)

    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n  📄 Exported to {p}")


def _rotation_to_dict(rr: RotationResult) -> dict:
    """Serialise a RotationResult for JSON."""
    return {
        "type": "rotation",
        "as_of": str(rr.as_of_date),
        "leading_sectors": rr.leading_sectors,
        "lagging_sectors": rr.lagging_sectors,
        "quality_stats": rr.quality_stats,
        "sector_rankings": [
            {
                "rank": s.rank,
                "sector": s.sector,
                "etf": s.etf,
                "tier": s.tier,
                "composite_rs": round(s.composite_rs, 6),
                "period_returns": {
                    str(k): round(v, 6)
                    for k, v in s.period_returns.items()
                },
            }
            for s in rr.sector_rankings
        ],
        "recommendations": [
            {
                "ticker": r.ticker,
                "action": r.action.value,
                "sector": r.sector,
                "sector_rank": r.sector_rank,
                "sector_tier": r.sector_tier,
                "rs_composite": round(r.rs_composite, 6),
                "rs_vs_sector_etf": round(r.rs_vs_sector_etf, 6),
                "quality_score": round(r.quality_score, 4),
                "quality_gate_passed": r.quality_gate_passed,
                "blended_score": round(r.blended_score, 4),
                "reason": r.reason,
            }
            for r in rr.recommendations
        ],
        "summary": {
            "buys": len(rr.buys),
            "sells": len(rr.sells),
            "reduces": len(rr.reduces),
            "holds": len(rr.holds),
        },
    }


def _pipeline_to_dict(result: PipelineResult) -> dict:
    """Serialise a PipelineResult for JSON."""
    out: dict[str, Any] = {
        "type": "pipeline",
        "n_tickers": result.n_tickers,
        "n_errors": result.n_errors,
        "total_time": round(result.total_time, 2),
        "top_30": [
            {
                "ticker": s.get("ticker"),
                "composite_score": round(
                    (s.get("composite_score") or 0), 4
                ),
                "signal": s.get("signal"),
                "rs_score": round((s.get("rs_score") or 0), 4),
            }
            for s in result.snapshots[:30]
        ],
    }

    if result.rotation_result is not None:
        rr = result.rotation_result
        out["rotation"] = {
            "leading_sectors": rr.leading_sectors,
            "lagging_sectors": rr.lagging_sectors,
            "buys": [r.ticker for r in rr.buys],
            "sells": [r.ticker for r in rr.sells],
        }

    if result.convergence is not None:
        conv = result.convergence
        strong = getattr(conv, "strong_buys", [])
        out["convergence"] = {
            "strong_buys": [
                getattr(sb, "ticker", str(sb)) for sb in strong
            ],
            "n_conflicts": len(getattr(conv, "conflicts", [])),
        }

    return out


def _rs_ranking_to_dict(result: dict) -> dict:
    """Serialise an RS ranking (HK/IN top-down) for JSON."""
    rs = result.get("rs_ranking")
    if rs is None:
        return {"type": "rs_ranking", "tickers": []}

    return {
        "type": "rs_ranking",
        "tickers": [
            {"ticker": t, "rs": round(float(v), 6)}
            for t, v in rs.items()
        ],
    }


# ═══════════════════════════════════════════════════════════════
#  ARGUMENT PARSER
# ═══════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_strategy",
        description="CASH Strategy System — unified CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Top-down
  %(prog)s top-down --market US
  %(prog)s top-down --market US --quality --holdings NVDA,CRWD
  %(prog)s top-down --market HK
  %(prog)s top-down --market IN

  # Bottom-up
  %(prog)s bottom-up --market US
  %(prog)s bottom-up --market IN

  # Full combined pipeline
  %(prog)s full --market US --holdings NVDA,CRWD,CEG
  %(prog)s full --market ALL --lookback 365
  %(prog)s full --market ALL -o results/report.json -v
""",
    )

    sub = p.add_subparsers(dest="mode", required=True)

    # ── Shared options builder ────────────────────────────
    def _add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--market", default="US",
            help="Market code: US, HK, IN, or ALL (default: US)",
        )
        sp.add_argument(
            "--lookback", type=int, default=None, metavar="DAYS",
            help=(
                "Calendar days of history to load.  "
                "Default: all available data."
            ),
        )
        sp.add_argument(
            "--holdings", default="",
            help=(
                "Comma-separated current holdings for sell "
                "evaluation (e.g. NVDA,CRWD,CEG)"
            ),
        )
        sp.add_argument(
            "--output", "-o", metavar="PATH",
            help="Export results to JSON file",
        )
        sp.add_argument(
            "--verbose", "-v", action="store_true",
            help="Debug-level logging",
        )
        sp.add_argument(
            "--quiet", "-q", action="store_true",
            help="Suppress log output (results only)",
        )

    # ── top-down ──────────────────────────────────────────
    td = sub.add_parser(
        "top-down",
        help="Sector rotation (US) / RS ranking (HK, IN)",
        description=(
            "Run top-down sector rotation for US, or relative-\n"
            "strength ranking for HK/IN.  Fast — does not run\n"
            "the full per-ticker scoring pipeline."
        ),
    )
    _add_common(td)
    td.add_argument(
        "--quality", action="store_true",
        help=(
            "Enable quality filter for US rotation.  "
            "Computes indicators inline (~30-60s) and gates "
            "candidates on SMA/EMA/RSI/ADX."
        ),
    )
    td.add_argument(
        "--quality-weight", type=float, metavar="W",
        help="Quality weight in RS/quality blend (default: 0.40)",
    )
    td.add_argument(
        "--stocks-per-sector", type=int, metavar="N",
        help="Stock picks per leading sector (default: 3)",
    )
    td.add_argument(
        "--max-positions", type=int, metavar="N",
        help="Maximum total portfolio positions (default: 12)",
    )

    # ── bottom-up ─────────────────────────────────────────
    bu = sub.add_parser(
        "bottom-up",
        help="Per-ticker scoring pipeline",
        description=(
            "Run the full per-ticker technical scoring pipeline\n"
            "via the orchestrator.  Computes indicators, RS,\n"
            "composite scores, rankings, and portfolio allocation.\n"
            "Does not run sector rotation or convergence."
        ),
    )
    _add_common(bu)

    # ── full ──────────────────────────────────────────────
    fu = sub.add_parser(
        "full",
        help="Combined bottom-up + top-down pipeline",
        description=(
            "Run the complete CASH pipeline.  Bottom-up scoring\n"
            "feeds indicator data into top-down rotation for\n"
            "quality-filtered stock selection, then convergence\n"
            "merges both signal lists for maximum conviction."
        ),
    )
    _add_common(fu)

    return p


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> dict[str, Any]:
    """
    Parse CLI arguments, run the requested strategy mode, and
    print results.

    Can also be called programmatically for testing::

        from scripts.run_strategy import main
        results = main(["top-down", "--market", "US"])
        results = main(["bottom-up", "--market", "HK", "-v"])
        results = main(["full", "--market", "ALL"])
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # ── Logging ───────────────────────────────────────────
    level = (
        logging.DEBUG if args.verbose
        else logging.WARNING if args.quiet
        else logging.INFO
    )
    logging.basicConfig(
        level=level,
        format="%(asctime)s │ %(name)-22s │ %(message)s",
        datefmt="%H:%M:%S",
    )

    t0 = time.perf_counter()

    # ── Dispatch ──────────────────────────────────────────
    if args.mode == "top-down":
        results = _run_top_down(args)
    elif args.mode == "bottom-up":
        results = _run_bottom_up(args)
    elif args.mode == "full":
        results = _run_full(args)
    else:
        parser.print_help()
        return {}

    # ── Export ────────────────────────────────────────────
    if args.output:
        _export_json(results, args.output, args.mode)

    elapsed = time.perf_counter() - t0
    n_markets = len(results)
    print(
        f"\n  ✓  Done — {n_markets} market(s) in {elapsed:.1f}s\n"
    )

    return results


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()

#########################################################################################
#!/usr/bin/env python3
"""
scripts/run_market.py
─────────────────────
Run the CASH pipeline for one market and generate an HTML
recommendations report.

Usage
─────
  # US, default 365 days
  python -m scripts.run_market

  # Hong Kong, 180 days lookback
  python -m scripts.run_market -m HK --days 180

  # India, 90 days, open browser
  python -m scripts.run_market -m IN --days 90 --open

  # US with current holdings
  python -m scripts.run_market -m US --days 365 --holdings NVDA,CRWD,PANW --open
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import webbrowser
from datetime import datetime
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────
_SCRIPTS = Path(__file__).resolve().parent
_ROOT    = _SCRIPTS.parent
_SRC     = _ROOT / "src"
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from pipeline.orchestrator import run_full_pipeline
from strategy.convergence import convergence_report
from reports.html_report import generate_html_report
from common.config import MARKET_CONFIG


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="run_market",
        description=(
            "Run the CASH pipeline: load N days of data from "
            "existing parquet / DB / yfinance, analyse, and "
            "generate an HTML recommendations report."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m scripts.run_market                                  # US, 365d
  python -m scripts.run_market -m HK --days 180                 # HK, 180d
  python -m scripts.run_market -m IN --days 90 --open           # IN, 90d, open
  python -m scripts.run_market -m US --days 365 --holdings NVDA,CRWD --open

NOTE: Data must already exist in parquet/DB.  To download first:
  python src/ingest_cash.py --market us --days 365
        """,
    )

    p.add_argument(
        "-m", "--market",
        type=str,
        default="US",
        choices=sorted(MARKET_CONFIG.keys()),
        help="Market / region to analyse (default: US)",
    )
    p.add_argument(
        "-n", "--days",
        type=int,
        default=365,
        help=(
            "Calendar days of data to analyse.  An extra warm-up "
            "buffer (~220 days) is added automatically for "
            "indicator initialisation. (default: 365)"
        ),
    )
    p.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output HTML path (default: cash_{market}_{date}.html)",
    )
    p.add_argument(
        "--holdings",
        type=str,
        default="",
        help=(
            "Comma-separated current holdings for rotation "
            "sell evaluation (e.g. NVDA,CRWD,PANW)"
        ),
    )
    p.add_argument(
        "--capital",
        type=float,
        default=None,
        help="Portfolio capital (default: from config)",
    )
    p.add_argument(
        "--open",
        action="store_true",
        help="Open the report in default browser after generation",
    )
    p.add_argument(
        "--text",
        action="store_true",
        help="Also print text convergence report to stdout",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--backtest",
        action="store_true",
        default=False,
        help="Run backtest phase (off by default for quick runs)",
    )

    return p.parse_args()


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()
    _setup_logging(args.verbose)
    logger = logging.getLogger("run_market")

    market = args.market.upper()
    mcfg = MARKET_CONFIG.get(market)
    if mcfg is None:
        logger.error(f"Unknown market: {market}")
        sys.exit(1)

    # Parse holdings
    holdings: list[str] = []
    if args.holdings:
        holdings = [
            t.strip().upper()
            for t in args.holdings.split(",")
            if t.strip()
        ]

    # ── Banner ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  CASH Pipeline — {market}")
    logger.info(f"  Benchmark   : {mcfg['benchmark']}")
    logger.info(f"  Universe    : {len(mcfg['universe'])} tickers")
    logger.info(f"  Engines     : {mcfg.get('engines', ['scoring'])}")
    logger.info(f"  Lookback    : {args.days} days")
    if holdings:
        logger.info(f"  Holdings    : {holdings}")
    logger.info("=" * 60)

    # ── Run pipeline ──────────────────────────────────────────
    wall_t0 = time.perf_counter()

    try:
        result = run_full_pipeline(
            market=market,
            lookback_days=args.days,
            capital=args.capital,
            current_holdings=holdings or None,
            enable_backtest=args.backtest,
        )
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    wall_elapsed = time.perf_counter() - wall_t0

    # ── Pipeline errors? ──────────────────────────────────────
    if result.n_errors > 0:
        logger.warning(
            f"{result.n_errors} error(s) during pipeline:"
        )
        for e in result.errors:
            logger.warning(f"  • {e}")

    if result.convergence is None:
        logger.error("No convergence result — cannot generate report")
        sys.exit(1)

    # ── Text report (optional) ────────────────────────────────
    if args.text:
        print()
        print(convergence_report(result.convergence))
        print()

    # ── HTML report ───────────────────────────────────────────
    logger.info("Generating HTML report…")
    html = generate_html_report(result)

    if args.output:
        out_path = Path(args.output)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        out_path = Path(f"cash_{market.lower()}_{ts}.html")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    logger.info(f"Report written: {out_path}  ({size_kb:.0f} KB)")

    # ── Summary to stdout ─────────────────────────────────────
    conv = result.convergence
    print()
    print(f"  {'─' * 44}")
    print(f"  {market}  │  {conv.n_tickers} tickers  │  "
          f"{args.days}d lookback  │  {wall_elapsed:.1f}s")
    print(f"  {'─' * 44}")
    print(f"  STRONG BUY : {len(conv.strong_buys):>3}")
    print(f"  BUY        : {len(conv.buys):>3}")
    print(f"  CONFLICT   : {len(conv.conflicts):>3}")
    print(f"  SELL       : {len(conv.sells):>3}")
    print(f"  HOLD       : {len(conv.holds):>3}")

    if conv.strong_buys:
        print()
        print("  Top Strong-Buy picks:")
        for s in conv.strong_buys[:5]:
            print(
                f"    #{s.rank:<3}  {s.ticker:<8s}  "
                f"adj={s.adjusted_score:.3f}  "
                f"{s.scoring_regime}"
            )

    print()
    print(f"  📄  {out_path.resolve()}")
    print()

    # ── Open in browser ───────────────────────────────────────
    if args.open:
        url = f"file://{out_path.resolve()}"
        logger.info(f"Opening {url}")
        webbrowser.open(url)


if __name__ == "__main__":
    main()


#########################################################################
"""
REPORTS:
--------------------------------------
"""

"""
reports/html_report.py
──────────────────────
Self-contained HTML recommendation report for CASH pipeline.

Produces a dark-themed responsive dashboard:
  • Summary cards with signal counts
  • Strong-buy highlight cards
  • Per-category signal tables with scores, reasons, sector data
  • Interactive sort, collapse, and ticker search
  • No external JS/CSS dependencies (Google Fonts optional)

Usage
─────
    from reports.html_report import generate_html_report
    html = generate_html_report(pipeline_result)
    Path("report.html").write_text(html)
"""

from __future__ import annotations

import html as _html
from datetime import datetime
from typing import Any

from strategy.convergence import (
    MarketSignalResult,
    ConvergedSignal,
    STRONG_BUY,
    BUY_SCORING,
    BUY_ROTATION,
    CONFLICT,
    HOLD,
    NEUTRAL,
    SELL_SCORING,
    SELL_ROTATION,
    STRONG_SELL,
)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _esc(s: Any) -> str:
    if s is None:
        return "—"
    return _html.escape(str(s))


def _pct(v: float) -> str:
    return f"{max(0, min(100, v * 100)):.0f}"


_LABEL_COLORS = {
    STRONG_BUY:    ("#238636", "#3fb950"),
    BUY_SCORING:   ("#1a7f37", "#56d364"),
    BUY_ROTATION:  ("#0d5524", "#7ee787"),
    CONFLICT:      ("#7a4e05", "#d29922"),
    HOLD:          ("#30363d", "#8b949e"),
    NEUTRAL:       ("#30363d", "#8b949e"),
    SELL_SCORING:  ("#8b2c22", "#f85149"),
    SELL_ROTATION: ("#6e1d18", "#ff7b72"),
    STRONG_SELL:   ("#b62324", "#ff7b72"),
}

_LABEL_DISPLAY = {
    STRONG_BUY:    "Strong Buy",
    BUY_SCORING:   "Buy (Scoring)",
    BUY_ROTATION:  "Buy (Rotation)",
    CONFLICT:      "Conflict",
    HOLD:          "Hold",
    NEUTRAL:       "Neutral",
    SELL_SCORING:  "Sell (Scoring)",
    SELL_ROTATION: "Sell (Rotation)",
    STRONG_SELL:   "Strong Sell",
}

_SECTION_ORDER = [
    (STRONG_BUY,    "🟢🟢", "Strong Buy",       "Both engines agree — highest conviction"),
    (BUY_SCORING,   "🟢",   "Buy — Scoring",    "Strong individual profile; sector not leading"),
    (BUY_ROTATION,  "🟢",   "Buy — Rotation",   "In a leading sector; individual metrics not yet confirmed"),
    (CONFLICT,      "⚠️",    "Conflict",          "Engines disagree — review manually"),
    (SELL_SCORING,  "🔴",   "Sell — Scoring",   "Weak individual profile"),
    (SELL_ROTATION, "🔴",   "Sell — Rotation",  "Sector lagging; consider exit"),
    (STRONG_SELL,   "🔴🔴", "Strong Sell",       "Both engines agree — highest conviction exit"),
    (HOLD,          "⚪",   "Hold",              "No actionable signal"),
    (NEUTRAL,       "⚪",   "Neutral",           "Insufficient data or edge"),
]


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY
# ═══════════════════════════════════════════════════════════════

def generate_html_report(
    pipeline_result: Any,
    title: str | None = None,
) -> str:
    """
    Generate a complete self-contained HTML report.

    Parameters
    ----------
    pipeline_result : PipelineResult
        Must have ``.convergence`` (MarketSignalResult) set.
    title : str, optional
        Page title override.

    Returns
    -------
    str
        Complete HTML document.
    """
    conv: MarketSignalResult | None = getattr(
        pipeline_result, "convergence", None
    )
    market   = getattr(pipeline_result, "market", "US")
    timings  = getattr(pipeline_result, "timings", {})
    run_date = getattr(pipeline_result, "run_date", None)
    n_errors = getattr(pipeline_result, "n_errors", 0)

    if conv is None:
        return _error_page("No convergence data — pipeline may have failed.")

    date_str = (
        run_date.strftime("%B %d, %Y at %H:%M")
        if run_date
        else datetime.now().strftime("%B %d, %Y at %H:%M")
    )
    has_rotation = "rotation" in timings
    total_time   = sum(timings.values()) if timings else 0.0
    page_title   = title or f"CASH — {market} Recommendations"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(page_title)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
{_CSS}
</style>
</head>
<body>

{_header_html(market, date_str, conv, n_errors, total_time, has_rotation)}

<main class="container">

  <div class="toolbar">
    <input type="text" id="search-input" class="search-box"
           placeholder="Search ticker…" oninput="filterAll(this.value)">
    <button class="btn" onclick="expandAll()">Expand All</button>
    <button class="btn" onclick="collapseAll()">Collapse All</button>
  </div>

  {_summary_cards_html(conv)}
  {_all_sections_html(conv, has_rotation)}

  <footer class="footer">
    <p>Generated {_esc(date_str)} &nbsp;·&nbsp; CASH Pipeline v2.0</p>
    {_timings_html(timings)}
    <p class="muted">For informational purposes only. Not financial advice.</p>
  </footer>
</main>

<script>
{_JS}
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════
#  HEADER
# ═══════════════════════════════════════════════════════════════

def _header_html(
    market: str,
    date_str: str,
    conv: MarketSignalResult,
    n_errors: int,
    total_time: float,
    has_rotation: bool,
) -> str:
    engines = "Scoring + Rotation" if has_rotation else "Scoring Only"
    err_badge = (
        f'<span class="header-badge badge-err">{n_errors} errors</span>'
        if n_errors > 0 else ""
    )

    return f"""
<header class="header">
  <div class="container header-inner">
    <div>
      <h1 class="logo">CASH
        <span class="market-badge">{_esc(market)}</span>
      </h1>
      <p class="subtitle">Convergence Analysis &amp; Signal Hierarchy</p>
    </div>
    <div class="header-meta">
      <span class="header-badge">{_esc(engines)}</span>
      <span class="header-badge">{conv.n_tickers} tickers</span>
      <span class="header-badge">{total_time:.1f}s</span>
      {err_badge}
      <div class="header-date">{_esc(date_str)}</div>
    </div>
  </div>
</header>"""


# ═══════════════════════════════════════════════════════════════
#  SUMMARY CARDS
# ═══════════════════════════════════════════════════════════════

def _summary_cards_html(conv: MarketSignalResult) -> str:
    counts: dict[str, int] = {}
    for s in conv.signals:
        counts[s.convergence_label] = counts.get(s.convergence_label, 0) + 1

    cards_data = [
        ("Strong Buy",  STRONG_BUY,   "card-green"),
        ("Buy",         "_ALL_BUYS",  "card-green-dim"),
        ("Conflict",    CONFLICT,     "card-amber"),
        ("Sell",        "_ALL_SELLS", "card-red"),
        ("Hold",        HOLD,         "card-gray"),
    ]

    html_parts = ['<div class="cards-row">']
    for label, key, css in cards_data:
        if key == "_ALL_BUYS":
            v = counts.get(BUY_SCORING, 0) + counts.get(BUY_ROTATION, 0)
        elif key == "_ALL_SELLS":
            v = (counts.get(SELL_SCORING, 0) + counts.get(SELL_ROTATION, 0)
                 + counts.get(STRONG_SELL, 0))
        else:
            v = counts.get(key, 0)
        html_parts.append(f"""
    <div class="card {css}">
      <div class="card-value">{v}</div>
      <div class="card-label">{_esc(label)}</div>
    </div>""")
    html_parts.append("</div>")
    return "\n".join(html_parts)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL SECTIONS
# ═══════════════════════════════════════════════════════════════

def _all_sections_html(conv: MarketSignalResult, has_rotation: bool) -> str:
    # Group signals by label
    groups: dict[str, list[ConvergedSignal]] = {}
    for s in conv.signals:
        groups.setdefault(s.convergence_label, []).append(s)

    parts: list[str] = []
    for label, emoji, heading, desc in _SECTION_ORDER:
        sigs = groups.get(label, [])
        if not sigs:
            continue

        collapsed = label in (HOLD, NEUTRAL)
        section_id = label.lower().replace("_", "-")
        bg, fg = _LABEL_COLORS.get(label, ("#30363d", "#8b949e"))

        if label == STRONG_BUY:
            body = _strong_buy_cards_html(sigs, has_rotation)
        else:
            body = _signal_table_html(sigs, has_rotation, section_id)

        display = "none" if collapsed else "block"
        chevron = "▸" if collapsed else "▾"

        parts.append(f"""
<section class="signal-section" id="section-{section_id}">
  <div class="section-header" onclick="toggleSection('{section_id}')"
       style="border-left: 4px solid {fg}">
    <div>
      <h2>{emoji} {_esc(heading)}
        <span class="count-badge" style="background:{bg};color:{fg}">{len(sigs)}</span>
      </h2>
      <p class="section-desc">{_esc(desc)}</p>
    </div>
    <span class="chevron" id="chev-{section_id}">{chevron}</span>
  </div>
  <div class="section-body" id="body-{section_id}" style="display:{display}">
    {body}
  </div>
</section>""")

    return "\n".join(parts)


# ── Strong Buy Cards ──────────────────────────────────────────

def _strong_buy_cards_html(
    signals: list[ConvergedSignal],
    has_rotation: bool,
) -> str:
    parts = ['<div class="sb-cards">']
    for s in signals:
        confirmed_icon = "✓" if s.scoring_confirmed else "✗"
        score_w = _pct(s.adjusted_score)

        rot_html = ""
        if has_rotation and s.rotation_signal:
            rot_html = f"""
      <div class="sb-row">
        <span class="sb-dim">Rotation</span>
        <span>{_esc(s.rotation_signal)}&nbsp; RS {s.rotation_rs:+.3f}</span>
      </div>
      <div class="sb-row">
        <span class="sb-dim">Sector</span>
        <span>{_esc(s.rotation_sector)} (#{s.rotation_sector_rank} {_esc(s.rotation_sector_tier)})</span>
      </div>"""

        reason = _build_reason(s)

        parts.append(f"""
    <div class="sb-card" data-ticker="{_esc(s.ticker)}">
      <div class="sb-top">
        <span class="sb-rank">#{s.rank}</span>
        <span class="sb-ticker">{_esc(s.ticker)}</span>
        <span class="sb-score">{s.adjusted_score:.3f}</span>
      </div>
      <div class="score-bar"><div class="score-fill score-fill-green" style="width:{score_w}%"></div></div>
      <div class="sb-details">
        <div class="sb-row">
          <span class="sb-dim">Scoring</span>
          <span>{_esc(s.scoring_signal)} [{confirmed_icon}] &nbsp;{_esc(s.scoring_regime)}</span>
        </div>
        {rot_html}
      </div>
      <div class="sb-reason">{_esc(reason)}</div>
    </div>""")

    parts.append("</div>")
    return "\n".join(parts)


# ── Signal Table ──────────────────────────────────────────────

def _signal_table_html(
    signals: list[ConvergedSignal],
    has_rotation: bool,
    table_id: str,
) -> str:
    rot_cols = ""
    if has_rotation:
        rot_cols = """
        <th onclick="sortTable('{tid}',5)" class="sortable">Rot Signal</th>
        <th onclick="sortTable('{tid}',6)" class="sortable">Sector</th>
        <th onclick="sortTable('{tid}',7)" class="sortable">Sect Rank</th>
        <th onclick="sortTable('{tid}',8)" class="sortable">RS</th>""".replace(
            "{tid}", table_id
        )

    reason_col = 9 if has_rotation else 5

    header = f"""
    <table class="signal-table" id="table-{table_id}">
      <thead><tr>
        <th onclick="sortTable('{table_id}',0)" class="sortable">#</th>
        <th onclick="sortTable('{table_id}',1)" class="sortable">Ticker</th>
        <th onclick="sortTable('{table_id}',2)" class="sortable">Score</th>
        <th onclick="sortTable('{table_id}',3)" class="sortable">Adj</th>
        <th onclick="sortTable('{table_id}',4)" class="sortable">Scoring</th>
        {rot_cols}
        <th>Reason</th>
      </tr></thead>
      <tbody>"""

    rows: list[str] = []
    for s in signals:
        confirmed_icon = "✓" if s.scoring_confirmed else "✗"
        score_w = _pct(s.composite_score)
        adj_w   = _pct(s.adjusted_score)
        reason  = _build_reason(s)

        rot_cells = ""
        if has_rotation:
            rot_cells = f"""
        <td>{_esc(s.rotation_signal)}</td>
        <td>{_esc(s.rotation_sector)}</td>
        <td class="center">{s.rotation_sector_rank if s.rotation_sector_rank < 99 else '—'}</td>
        <td class="mono">{s.rotation_rs:+.3f}</td>"""

        rows.append(f"""
      <tr data-ticker="{_esc(s.ticker)}">
        <td class="center">{s.rank}</td>
        <td class="ticker-cell">{_esc(s.ticker)}</td>
        <td>
          <div class="score-bar-sm"><div class="score-fill score-fill-auto" style="width:{score_w}%"></div></div>
          <span class="mono">{s.composite_score:.3f}</span>
        </td>
        <td class="mono">{s.adjusted_score:.3f}</td>
        <td>{_esc(s.scoring_signal)}&nbsp;<span class="dim">[{confirmed_icon}]</span></td>
        {rot_cells}
        <td class="reason-cell">{_esc(reason)}</td>
      </tr>""")

    return header + "\n".join(rows) + "\n</tbody></table>"


# ── Reason Builder ────────────────────────────────────────────

def _build_reason(s: ConvergedSignal) -> str:
    parts: list[str] = []

    if s.scoring_confirmed:
        parts.append(f"Scoring confirmed ({s.scoring_regime})")
    elif s.scoring_signal and s.scoring_signal not in ("HOLD", "NEUTRAL"):
        parts.append(f"Scoring {s.scoring_signal} ({s.scoring_regime}, unconfirmed)")

    if s.rotation_reason:
        parts.append(s.rotation_reason)
    elif s.rotation_signal and s.rotation_sector:
        parts.append(
            f"Rotation {s.rotation_signal}: "
            f"{s.rotation_sector} "
            f"(#{s.rotation_sector_rank} {s.rotation_sector_tier})"
        )

    return " · ".join(parts) if parts else "—"


# ── Timings ───────────────────────────────────────────────────

def _timings_html(timings: dict[str, float]) -> str:
    if not timings:
        return ""
    items = " &nbsp;·&nbsp; ".join(
        f"{k}: {v:.1f}s" for k, v in timings.items()
    )
    return f'<p class="muted">Timings: {items}</p>'


# ── Error page ────────────────────────────────────────────────

def _error_page(message: str) -> str:
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>CASH — Error</title>
<style>body{{font-family:sans-serif;background:#0d1117;color:#f85149;
display:flex;align-items:center;justify-content:center;height:100vh;}}
.box{{background:#161b22;padding:40px;border-radius:12px;text-align:center;}}
</style></head><body><div class="box"><h1>Pipeline Error</h1>
<p>{_esc(message)}</p></div></body></html>"""


# ═══════════════════════════════════════════════════════════════
#  CSS
# ═══════════════════════════════════════════════════════════════

_CSS = """
/* ── Reset & Base ──────────────────────────────────────── */
*, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }

html { font-size: 15px; }

body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #0d1117;
  color: #e6edf3;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
}

.mono { font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: 0.85rem; }
.dim  { color: #8b949e; }
.muted { color: #484f58; font-size: 0.82rem; margin-top: 6px; }
.center { text-align: center; }

.container { max-width: 1280px; margin: 0 auto; padding: 0 24px; }

/* ── Header ────────────────────────────────────────────── */
.header {
  background: #161b22;
  border-bottom: 1px solid #30363d;
  padding: 20px 0;
}
.header-inner {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
}
.logo {
  font-size: 1.6rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  color: #e6edf3;
}
.market-badge {
  display: inline-block;
  background: #238636;
  color: #fff;
  font-size: 0.75rem;
  font-weight: 600;
  padding: 2px 10px;
  border-radius: 20px;
  vertical-align: middle;
  margin-left: 8px;
}
.subtitle { color: #8b949e; font-size: 0.9rem; margin-top: 2px; }
.header-meta { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.header-badge {
  display: inline-block;
  background: #21262d;
  color: #8b949e;
  font-size: 0.78rem;
  padding: 3px 10px;
  border-radius: 20px;
}
.badge-err { background: #3d1a1a; color: #f85149; }
.header-date { color: #8b949e; font-size: 0.82rem; }

/* ── Toolbar ───────────────────────────────────────────── */
.toolbar {
  display: flex; gap: 10px; align-items: center;
  margin: 24px 0 16px 0; flex-wrap: wrap;
}
.search-box {
  flex: 1; min-width: 200px; max-width: 360px;
  background: #161b22; border: 1px solid #30363d;
  color: #e6edf3; padding: 8px 14px;
  border-radius: 8px; font-size: 0.9rem;
  outline: none;
}
.search-box:focus { border-color: #58a6ff; }
.search-box::placeholder { color: #484f58; }
.btn {
  background: #21262d; color: #8b949e;
  border: 1px solid #30363d; padding: 7px 14px;
  border-radius: 8px; font-size: 0.82rem;
  cursor: pointer;
}
.btn:hover { background: #30363d; color: #e6edf3; }

/* ── Summary Cards ─────────────────────────────────────── */
.cards-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 14px;
  margin-bottom: 28px;
}
.card {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 12px;
  padding: 18px 16px;
  text-align: center;
}
.card-value {
  font-size: 2rem; font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
}
.card-label { font-size: 0.82rem; color: #8b949e; margin-top: 4px; }
.card-green     .card-value { color: #3fb950; }
.card-green-dim .card-value { color: #56d364; }
.card-amber     .card-value { color: #d29922; }
.card-red       .card-value { color: #f85149; }
.card-gray      .card-value { color: #8b949e; }

/* ── Signal Section ────────────────────────────────────── */
.signal-section {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 12px;
  margin-bottom: 18px;
  overflow: hidden;
}
.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  cursor: pointer;
  user-select: none;
  transition: background 0.15s;
}
.section-header:hover { background: #1c2128; }
.section-header h2 {
  font-size: 1.05rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 8px;
}
.count-badge {
  display: inline-block;
  font-size: 0.72rem;
  font-weight: 600;
  padding: 2px 9px;
  border-radius: 20px;
  vertical-align: middle;
}
.section-desc { color: #8b949e; font-size: 0.82rem; margin-top: 2px; }
.chevron { font-size: 1.2rem; color: #484f58; }
.section-body { padding: 0 20px 18px 20px; }

/* ── Strong-Buy Cards ──────────────────────────────────── */
.sb-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 14px;
}
.sb-card {
  background: #0d1117;
  border: 1px solid #238636;
  border-radius: 10px;
  padding: 16px;
  transition: border-color 0.2s;
}
.sb-card:hover { border-color: #3fb950; }
.sb-top {
  display: flex;
  align-items: baseline;
  gap: 10px;
  margin-bottom: 8px;
}
.sb-rank {
  color: #8b949e;
  font-size: 0.82rem;
  font-family: 'JetBrains Mono', monospace;
}
.sb-ticker {
  font-size: 1.2rem;
  font-weight: 700;
  color: #3fb950;
  letter-spacing: 0.03em;
}
.sb-score {
  margin-left: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.1rem;
  color: #3fb950;
}
.sb-details { margin-top: 10px; }
.sb-row {
  display: flex;
  justify-content: space-between;
  font-size: 0.85rem;
  padding: 3px 0;
  border-bottom: 1px solid #21262d;
}
.sb-dim { color: #8b949e; }
.sb-reason {
  margin-top: 10px;
  font-size: 0.82rem;
  color: #8b949e;
  font-style: italic;
  line-height: 1.5;
}

/* ── Score Bar ─────────────────────────────────────────── */
.score-bar {
  height: 6px;
  background: #21262d;
  border-radius: 3px;
  overflow: hidden;
}
.score-bar-sm {
  display: inline-block;
  width: 50px;
  height: 4px;
  background: #21262d;
  border-radius: 2px;
  overflow: hidden;
  vertical-align: middle;
  margin-right: 6px;
}
.score-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.4s;
}
.score-fill-green {
  background: linear-gradient(90deg, #238636, #3fb950);
}
.score-fill-auto {
  background: linear-gradient(90deg, #1f6feb, #58a6ff);
}

/* ── Table ─────────────────────────────────────────────── */
.signal-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}
.signal-table th {
  text-align: left;
  padding: 10px 10px;
  color: #8b949e;
  font-weight: 500;
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  border-bottom: 1px solid #30363d;
  white-space: nowrap;
}
.signal-table th.sortable { cursor: pointer; }
.signal-table th.sortable:hover { color: #e6edf3; }
.signal-table td {
  padding: 9px 10px;
  border-bottom: 1px solid #21262d;
  vertical-align: middle;
}
.signal-table tbody tr:hover { background: #1c2128; }
.ticker-cell {
  font-weight: 600;
  font-family: 'JetBrains Mono', monospace;
  letter-spacing: 0.02em;
  color: #e6edf3;
}
.reason-cell {
  color: #8b949e;
  font-size: 0.82rem;
  max-width: 340px;
  line-height: 1.45;
}

/* ── Footer ────────────────────────────────────────────── */
.footer {
  margin-top: 36px;
  padding: 20px 0 40px 0;
  text-align: center;
  color: #484f58;
  font-size: 0.82rem;
  border-top: 1px solid #21262d;
}

/* ── Responsive ────────────────────────────────────────── */
@media (max-width: 768px) {
  html { font-size: 14px; }
  .container { padding: 0 12px; }
  .header-inner { flex-direction: column; align-items: flex-start; }
  .sb-cards { grid-template-columns: 1fr; }
  .signal-table { display: block; overflow-x: auto; }
  .section-body { padding: 0 12px 14px 12px; }
}
"""


# ═══════════════════════════════════════════════════════════════
#  JAVASCRIPT
# ═══════════════════════════════════════════════════════════════

_JS = """
/* ── Section toggle ────────────────────────────────────── */
function toggleSection(id) {
  var body = document.getElementById('body-' + id);
  var chev = document.getElementById('chev-' + id);
  if (!body) return;
  if (body.style.display === 'none') {
    body.style.display = 'block';
    if (chev) chev.textContent = '▾';
  } else {
    body.style.display = 'none';
    if (chev) chev.textContent = '▸';
  }
}

function expandAll() {
  document.querySelectorAll('.section-body').forEach(function(el) {
    el.style.display = 'block';
  });
  document.querySelectorAll('.chevron').forEach(function(el) {
    el.textContent = '▾';
  });
}

function collapseAll() {
  document.querySelectorAll('.section-body').forEach(function(el) {
    el.style.display = 'none';
  });
  document.querySelectorAll('.chevron').forEach(function(el) {
    el.textContent = '▸';
  });
}

/* ── Table sort ────────────────────────────────────────── */
var sortState = {};

function sortTable(tableId, colIdx) {
  var table = document.getElementById('table-' + tableId);
  if (!table) return;
  var tbody = table.querySelector('tbody');
  if (!tbody) return;
  var rows = Array.from(tbody.querySelectorAll('tr'));

  var key = tableId + '-' + colIdx;
  var asc = sortState[key] === 'asc' ? false : true;
  sortState[key] = asc ? 'asc' : 'desc';

  rows.sort(function(a, b) {
    var aText = a.cells[colIdx] ? a.cells[colIdx].textContent.trim() : '';
    var bText = b.cells[colIdx] ? b.cells[colIdx].textContent.trim() : '';
    var aNum = parseFloat(aText.replace(/[^\\d.\\-+]/g, ''));
    var bNum = parseFloat(bText.replace(/[^\\d.\\-+]/g, ''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return asc ? aNum - bNum : bNum - aNum;
    }
    return asc ? aText.localeCompare(bText) : bText.localeCompare(aText);
  });

  rows.forEach(function(row) { tbody.appendChild(row); });
}

/* ── Ticker search / filter ────────────────────────────── */
function filterAll(query) {
  var q = query.toUpperCase().trim();

  /* Filter table rows */
  document.querySelectorAll('.signal-table tbody tr').forEach(function(row) {
    var ticker = row.getAttribute('data-ticker') || '';
    row.style.display = (!q || ticker.toUpperCase().indexOf(q) !== -1)
      ? '' : 'none';
  });

  /* Filter strong-buy cards */
  document.querySelectorAll('.sb-card').forEach(function(card) {
    var ticker = card.getAttribute('data-ticker') || '';
    card.style.display = (!q || ticker.toUpperCase().indexOf(q) !== -1)
      ? '' : 'none';
  });
}
"""

###############################################################
"""
reports/portfolio_view.py
Portfolio view and rebalance-delta generator.

Compares the CASH recommendation report against your actual current
holdings and produces:
  1. A rebalance plan (what to buy, sell, trim, add)
  2. A current-portfolio health check (per-position diagnostics)
  3. Drift analysis (target vs actual allocation by bucket)

This is the "what do I actually need to DO" layer.
"""

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
from datetime import datetime


# ═════════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═════════════════════════════════════════════════════════════════

@dataclass
class Position:
    """A single current holding."""
    ticker: str
    shares: int
    avg_cost: float
    current_price: float
    category: str = ""
    bucket: str = ""

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def cost_basis(self) -> float:
        return self.shares * self.avg_cost

    @property
    def unrealised_pnl(self) -> float:
        return self.market_value - self.cost_basis

    @property
    def unrealised_pct(self) -> float:
        if self.cost_basis == 0:
            return 0.0
        return self.unrealised_pnl / self.cost_basis

    def to_dict(self) -> dict:
        return {
            "ticker":         self.ticker,
            "shares":         self.shares,
            "avg_cost":       self.avg_cost,
            "current_price":  self.current_price,
            "market_value":   self.market_value,
            "cost_basis":     self.cost_basis,
            "unrealised_pnl": self.unrealised_pnl,
            "unrealised_pct": self.unrealised_pct,
            "category":       self.category,
            "bucket":         self.bucket,
        }


@dataclass
class TradeAction:
    """A single trade needed to reach the target portfolio."""
    ticker: str
    action: str          # "BUY_NEW", "ADD", "TRIM", "CLOSE", "NO_CHANGE"
    current_shares: int
    target_shares: int
    delta_shares: int    # positive = buy, negative = sell
    current_price: float
    delta_dollars: float
    reason: str
    priority: int = 0    # lower = more urgent (1 = highest)
    stop_price: Optional[float] = None
    composite: Optional[float] = None
    confidence: Optional[float] = None

    @property
    def abs_delta_dollars(self) -> float:
        return abs(self.delta_dollars)

    def to_dict(self) -> dict:
        return {
            "ticker":          self.ticker,
            "action":          self.action,
            "current_shares":  self.current_shares,
            "target_shares":   self.target_shares,
            "delta_shares":    self.delta_shares,
            "current_price":   self.current_price,
            "delta_dollars":   self.delta_dollars,
            "reason":          self.reason,
            "priority":        self.priority,
            "stop_price":      self.stop_price,
            "composite":       self.composite,
            "confidence":      self.confidence,
        }


@dataclass
class RebalancePlan:
    """Complete rebalance output."""
    date: str
    portfolio_value: float
    cash_balance: float
    positions: list          # list of Position
    trades: list             # list of TradeAction
    bucket_drift: dict       # bucket -> {target_pct, actual_pct, drift_pct, drift_dollars}
    health_checks: list      # list of per-position health dicts
    warnings: list = field(default_factory=list)

    @property
    def buy_trades(self) -> list:
        return [t for t in self.trades if t.delta_shares > 0]

    @property
    def sell_trades(self) -> list:
        return [t for t in self.trades if t.delta_shares < 0]

    @property
    def net_cash_impact(self) -> float:
        """Negative means net outflow (buying), positive means net inflow (selling)."""
        return sum(t.delta_dollars for t in self.trades)

    @property
    def trade_count(self) -> int:
        return len([t for t in self.trades if t.action != "NO_CHANGE"])

    def to_dict(self) -> dict:
        return {
            "date":            self.date,
            "portfolio_value": self.portfolio_value,
            "cash_balance":    self.cash_balance,
            "trade_count":     self.trade_count,
            "net_cash_impact": self.net_cash_impact,
            "trades":          [t.to_dict() for t in self.trades],
            "bucket_drift":    self.bucket_drift,
            "health_checks":   self.health_checks,
            "warnings":        self.warnings,
        }


# ═════════════════════════════════════════════════════════════════
#  1.  REBALANCE PLAN BUILDER
# ═════════════════════════════════════════════════════════════════

def build_rebalance_plan(
    report: dict,
    current_positions: list[dict],
    cash_balance: float,
    portfolio_value: float,
    *,
    min_trade_dollars: float = 100.0,
    trim_threshold_pct: float = 0.02,
) -> RebalancePlan:
    """
    Compare CASH recommendation report against current holdings
    and produce a complete rebalance plan.

    Parameters
    ----------
    report : dict
        Output of recommendations.build_report().
    current_positions : list of dict
        Each dict needs: ticker, shares, avg_cost, current_price.
        Optional: category, bucket.
    cash_balance : float
        Current cash available.
    portfolio_value : float
        Total portfolio value (positions + cash).
    min_trade_dollars : float
        Trades smaller than this are filtered out as noise.
    trim_threshold_pct : float
        Minimum position weight drift to trigger a trim (fraction of portfolio).

    Returns
    -------
    RebalancePlan
    """
    # ── parse current positions ─────────────────────────────────
    positions = []
    current_map = {}  # ticker -> Position
    for p in current_positions:
        pos = Position(
            ticker=p["ticker"].upper(),
            shares=int(p["shares"]),
            avg_cost=float(p["avg_cost"]),
            current_price=float(p["current_price"]),
            category=p.get("category", ""),
            bucket=p.get("bucket", ""),
        )
        positions.append(pos)
        current_map[pos.ticker] = pos

    # ── parse recommendation targets ───────────────────────────
    buy_targets = {}   # ticker -> buy dict
    for b in report["buy_list"]:
        buy_targets[b["ticker"].upper()] = b

    sell_tickers = set()
    for s in report["sell_list"]:
        sell_tickers.add(s["ticker"].upper())

    hold_tickers = set()
    for h in report["hold_list"]:
        hold_tickers.add(h["ticker"].upper())

    # ── generate trade actions ──────────────────────────────────
    trades = []
    warnings = []
    processed = set()

    # --- SELLS: close positions that CASH says to sell ----------
    for ticker in sell_tickers:
        processed.add(ticker)
        if ticker in current_map:
            pos = current_map[ticker]
            delta = -pos.shares
            sell_record = _find_in_list(report["sell_list"], ticker)
            trades.append(TradeAction(
                ticker=ticker,
                action="CLOSE",
                current_shares=pos.shares,
                target_shares=0,
                delta_shares=delta,
                current_price=pos.current_price,
                delta_dollars=delta * pos.current_price,
                reason=f"SELL signal — composite {sell_record['composite']:.2f}, "
                       f"confidence {sell_record['confidence']:.0%}",
                priority=1,
                composite=sell_record["composite"],
                confidence=sell_record["confidence"],
            ))
        # if we don't hold it, no action needed

    # --- BUYS: new positions or add to existing ----------------
    for ticker, target in buy_targets.items():
        processed.add(ticker)
        target_shares = target["shares"]
        target_price = target["close"]

        if ticker in current_map:
            # already hold — compute delta
            pos = current_map[ticker]
            current_shares = pos.shares
            delta = target_shares - current_shares

            if delta > 0:
                # need to add
                delta_dollars = delta * target_price
                if abs(delta_dollars) >= min_trade_dollars:
                    trades.append(TradeAction(
                        ticker=ticker,
                        action="ADD",
                        current_shares=current_shares,
                        target_shares=target_shares,
                        delta_shares=delta,
                        current_price=target_price,
                        delta_dollars=delta_dollars,
                        reason=f"BUY signal — add {delta} shares to reach "
                               f"target {target_shares} "
                               f"(composite {target['composite']:.2f})",
                        priority=2,
                        stop_price=target.get("stop_price"),
                        composite=target["composite"],
                        confidence=target["confidence"],
                    ))
            elif delta < 0:
                # overweight vs target — suggest trim
                delta_dollars = delta * target_price
                weight_drift = abs(delta_dollars) / portfolio_value
                if weight_drift >= trim_threshold_pct:
                    trades.append(TradeAction(
                        ticker=ticker,
                        action="TRIM",
                        current_shares=current_shares,
                        target_shares=target_shares,
                        delta_shares=delta,
                        current_price=target_price,
                        delta_dollars=delta_dollars,
                        reason=f"Overweight by {abs(delta)} shares — trim to "
                               f"target {target_shares}",
                        priority=3,
                        stop_price=target.get("stop_price"),
                        composite=target["composite"],
                        confidence=target["confidence"],
                    ))
            # else: exactly on target, no trade needed
        else:
            # new position
            delta_dollars = target_shares * target_price
            if delta_dollars >= min_trade_dollars:
                trades.append(TradeAction(
                    ticker=ticker,
                    action="BUY_NEW",
                    current_shares=0,
                    target_shares=target_shares,
                    delta_shares=target_shares,
                    current_price=target_price,
                    delta_dollars=delta_dollars,
                    reason=f"New BUY — rank #{target['rank']}, "
                           f"composite {target['composite']:.2f}, "
                           f"confidence {target['confidence']:.0%}",
                    priority=2,
                    stop_price=target.get("stop_price"),
                    composite=target["composite"],
                    confidence=target["confidence"],
                ))

    # --- ORPHANS: positions we hold that CASH didn't mention ---
    for ticker, pos in current_map.items():
        if ticker not in processed:
            if ticker in hold_tickers:
                # CASH says hold — no action
                trades.append(TradeAction(
                    ticker=ticker,
                    action="NO_CHANGE",
                    current_shares=pos.shares,
                    target_shares=pos.shares,
                    delta_shares=0,
                    current_price=pos.current_price,
                    delta_dollars=0,
                    reason="HOLD signal — maintain current position",
                    priority=9,
                ))
            else:
                # not in buy, sell, or hold — CASH has no opinion
                # flag as orphan for manual review
                warnings.append(
                    f"ORPHAN: {ticker} ({pos.shares} shares, "
                    f"${pos.market_value:,.0f}) — not in CASH universe. "
                    f"Review manually."
                )
                trades.append(TradeAction(
                    ticker=ticker,
                    action="NO_CHANGE",
                    current_shares=pos.shares,
                    target_shares=pos.shares,
                    delta_shares=0,
                    current_price=pos.current_price,
                    delta_dollars=0,
                    reason="ORPHAN — not in CASH universe, review manually",
                    priority=5,
                ))

    # ── sort trades by priority then abs dollar size ────────────
    trades.sort(key=lambda t: (t.priority, -t.abs_delta_dollars))

    # ── cash feasibility check ──────────────────────────────────
    total_buy_cost = sum(t.delta_dollars for t in trades if t.delta_shares > 0)
    total_sell_proceeds = abs(sum(t.delta_dollars for t in trades if t.delta_shares < 0))
    net_outflow = total_buy_cost - total_sell_proceeds

    if net_outflow > cash_balance:
        warnings.append(
            f"CASH SHORTFALL: Net buy cost ${net_outflow:,.0f} exceeds "
            f"cash ${cash_balance:,.0f} by ${net_outflow - cash_balance:,.0f}. "
            f"Sells should execute before buys, or reduce buy sizes."
        )

    # ── bucket drift analysis ───────────────────────────────────
    bucket_drift = _compute_bucket_drift(
        report=report,
        current_map=current_map,
        buy_targets=buy_targets,
        portfolio_value=portfolio_value,
    )

    # ── per-position health checks ──────────────────────────────
    health_checks = _build_health_checks(
        positions=positions,
        report=report,
        portfolio_value=portfolio_value,
    )

    return RebalancePlan(
        date=report["header"]["date"],
        portfolio_value=portfolio_value,
        cash_balance=cash_balance,
        positions=positions,
        trades=trades,
        bucket_drift=bucket_drift,
        health_checks=health_checks,
        warnings=warnings,
    )


# ═════════════════════════════════════════════════════════════════
#  2.  DRIFT ANALYSIS
# ═════════════════════════════════════════════════════════════════

def _compute_bucket_drift(
    report: dict,
    current_map: dict,
    buy_targets: dict,
    portfolio_value: float,
) -> dict:
    """
    Compare target bucket allocation against current actual allocation.

    Returns dict of bucket -> {target_pct, actual_pct, drift_pct, drift_dollars,
                                actual_dollars, target_dollars}
    """
    bucket_weights = report["allocation"]["bucket_weights"]
    drift = {}

    # actual dollars per bucket from current holdings
    actual_by_bucket = {}
    for ticker, pos in current_map.items():
        bkt = pos.bucket or _infer_bucket(ticker, buy_targets, report)
        actual_by_bucket[bkt] = actual_by_bucket.get(bkt, 0) + pos.market_value

    # also count tickers in buy_targets that we don't hold yet
    # (they represent target allocation, not actual)

    all_buckets = set(bucket_weights.keys()) | set(actual_by_bucket.keys())

    for bucket in sorted(all_buckets):
        target_pct = bucket_weights.get(bucket, 0.0)
        target_dollars = target_pct * portfolio_value
        actual_dollars = actual_by_bucket.get(bucket, 0.0)
        actual_pct = actual_dollars / portfolio_value if portfolio_value > 0 else 0.0
        drift_pct = actual_pct - target_pct
        drift_dollars = actual_dollars - target_dollars

        drift[bucket] = {
            "target_pct":     target_pct,
            "actual_pct":     actual_pct,
            "drift_pct":      drift_pct,
            "drift_dollars":  drift_dollars,
            "actual_dollars":  actual_dollars,
            "target_dollars":  target_dollars,
        }

    return drift


def _infer_bucket(ticker: str, buy_targets: dict, report: dict) -> str:
    """Try to find the bucket for a ticker from buy targets or hold list."""
    if ticker in buy_targets:
        return buy_targets[ticker].get("bucket", "unknown")
    for h in report["hold_list"]:
        if h["ticker"].upper() == ticker:
            return h.get("bucket", "unknown")
    return "unknown"


# ═════════════════════════════════════════════════════════════════
#  3.  PER-POSITION HEALTH CHECKS
# ═════════════════════════════════════════════════════════════════

def _build_health_checks(
    positions: list,
    report: dict,
    portfolio_value: float,
) -> list:
    """
    Generate a health diagnostic for each current position.

    Flags: concentration risk, underwater positions, stop proximity,
    signal disagreement.
    """
    buy_map = {b["ticker"].upper(): b for b in report["buy_list"]}
    sell_set = {s["ticker"].upper() for s in report["sell_list"]}
    checks = []

    max_position_pct = 0.08  # flag if any single position > 8% of portfolio

    for pos in positions:
        ticker = pos.ticker
        weight = pos.market_value / portfolio_value if portfolio_value > 0 else 0
        flags = []

        # concentration
        if weight > max_position_pct:
            flags.append(
                f"CONCENTRATION: {weight:.1%} of portfolio "
                f"(threshold {max_position_pct:.0%})"
            )

        # underwater
        if pos.unrealised_pct < -0.10:
            flags.append(
                f"UNDERWATER: {pos.unrealised_pct:.1%} unrealised loss "
                f"(${pos.unrealised_pnl:,.0f})"
            )

        # deep underwater
        if pos.unrealised_pct < -0.25:
            flags.append(
                f"DEEP LOSS: {pos.unrealised_pct:.1%} — consider tax-loss "
                f"harvest or forced exit"
            )

        # stop proximity
        if ticker in buy_map:
            rec = buy_map[ticker]
            stop = rec.get("stop_price")
            if stop and pos.current_price > 0:
                stop_distance = (pos.current_price - stop) / pos.current_price
                if stop_distance < 0.02:
                    flags.append(
                        f"STOP PROXIMITY: price ${pos.current_price:.2f} is "
                        f"only {stop_distance:.1%} above stop ${stop:.2f}"
                    )
                if stop_distance < 0:
                    flags.append(
                        f"STOP BREACHED: price ${pos.current_price:.2f} is "
                        f"BELOW stop ${stop:.2f} — exit immediately"
                    )

        # signal disagreement: we hold it but CASH says SELL
        if ticker in sell_set:
            flags.append("SIGNAL CONFLICT: CASH recommends SELL but position is held")

        # signal check: we hold it and it's not in buy or hold
        if (ticker not in buy_map and
            ticker not in sell_set and
            ticker not in {h["ticker"].upper() for h in report["hold_list"]}):
            flags.append("ORPHAN: ticker not in CASH universe — no signal available")

        checks.append({
            "ticker":          ticker,
            "shares":          pos.shares,
            "current_price":   pos.current_price,
            "market_value":    pos.market_value,
            "weight_pct":      weight,
            "avg_cost":        pos.avg_cost,
            "unrealised_pnl":  pos.unrealised_pnl,
            "unrealised_pct":  pos.unrealised_pct,
            "flags":           flags,
            "flag_count":      len(flags),
            "healthy":         len(flags) == 0,
        })

    # sort: most-flagged first
    checks.sort(key=lambda c: (-c["flag_count"], -c["market_value"]))
    return checks


# ═════════════════════════════════════════════════════════════════
#  4.  PLAIN-TEXT REBALANCE REPORT
# ═════════════════════════════════════════════════════════════════

def rebalance_to_text(plan: RebalancePlan) -> str:
    """Render the rebalance plan as a plain-text report."""
    lines = []

    lines.append("=" * 72)
    lines.append("  CASH — REBALANCE PLAN")
    lines.append(f"  Date: {plan.date}    Portfolio: ${plan.portfolio_value:,.0f}    "
                 f"Cash: ${plan.cash_balance:,.0f}")
    lines.append("=" * 72)

    # ── warnings ────────────────────────────────────────────────
    if plan.warnings:
        lines.append("")
        lines.append("─── ⚠  WARNINGS ────────────────────────────────────────────────")
        for w in plan.warnings:
            lines.append(f"  ▸ {w}")

    # ── trade actions ───────────────────────────────────────────
    active_trades = [t for t in plan.trades if t.action != "NO_CHANGE"]

    lines.append("")
    lines.append(f"─── TRADES REQUIRED ({len(active_trades)}) "
                 "─────────────────────────────────────")

    if active_trades:
        lines.append(
            f"  {'Action':<10s} {'Ticker':<8s} {'Current':>8s} {'Target':>8s} "
            f"{'Delta':>8s} {'$Delta':>10s} {'Reason'}"
        )
        lines.append("  " + "-" * 68)

        for t in active_trades:
            sign = "+" if t.delta_shares > 0 else ""
            lines.append(
                f"  {t.action:<10s} {t.ticker:<8s} "
                f"{t.current_shares:>8d} {t.target_shares:>8d} "
                f"{sign}{t.delta_shares:>7d} "
                f"${t.delta_dollars:>+9,.0f}  "
                f"{t.reason}"
            )
            if t.stop_price:
                lines.append(f"{'':>51s} stop: ${t.stop_price:.2f}")
    else:
        lines.append("  No trades required — portfolio is aligned with recommendations.")

    # ── execution summary ───────────────────────────────────────
    lines.append("")
    lines.append("─── EXECUTION SUMMARY ──────────────────────────────────────────")
    buy_trades = plan.buy_trades
    sell_trades = plan.sell_trades
    total_buy = sum(t.delta_dollars for t in buy_trades)
    total_sell = sum(t.delta_dollars for t in sell_trades)  # negative
    lines.append(f"  Total to BUY:    ${total_buy:>12,.0f}  ({len(buy_trades)} trades)")
    lines.append(f"  Total to SELL:   ${abs(total_sell):>12,.0f}  ({len(sell_trades)} trades)")
    lines.append(f"  Net cash impact: ${plan.net_cash_impact:>+12,.0f}")
    lines.append(f"  Cash after:      ${plan.cash_balance + plan.net_cash_impact:>12,.0f}")

    # ── suggested execution order ───────────────────────────────
    if sell_trades and buy_trades:
        lines.append("")
        lines.append("─── SUGGESTED EXECUTION ORDER ──────────────────────────────────")
        lines.append("  1. Execute SELLS first to free cash:")
        for t in sell_trades:
            lines.append(f"     • {t.action} {t.ticker}  ({abs(t.delta_shares)} shares, "
                         f"~${abs(t.delta_dollars):,.0f})")
        lines.append("  2. Then execute BUYS:")
        for t in buy_trades:
            lines.append(f"     • {t.action} {t.ticker}  ({t.delta_shares} shares, "
                         f"~${t.delta_dollars:,.0f})")

    # ── bucket drift ────────────────────────────────────────────
    lines.append("")
    lines.append("─── BUCKET DRIFT ANALYSIS ─────────────────────────────────────")
    lines.append(
        f"  {'Bucket':<22s} {'Target':>7s} {'Actual':>7s} "
        f"{'Drift':>7s} {'$Drift':>12s}"
    )
    lines.append("  " + "-" * 58)
    for bucket, d in sorted(plan.bucket_drift.items()):
        lines.append(
            f"  {bucket:<22s} {d['target_pct']:>6.1%} {d['actual_pct']:>6.1%} "
            f"{d['drift_pct']:>+6.1%} ${d['drift_dollars']:>+11,.0f}"
        )

    # ── health checks ──────────────────────────────────────────
    flagged = [c for c in plan.health_checks if not c["healthy"]]
    if flagged:
        lines.append("")
        lines.append(f"─── POSITION HEALTH FLAGS ({len(flagged)} positions) "
                     "──────────────────────")
        for c in flagged:
            lines.append(
                f"  {c['ticker']:<8s}  ${c['market_value']:>10,.0f}  "
                f"({c['weight_pct']:.1%})  P&L {c['unrealised_pct']:>+6.1%}"
            )
            for flag in c["flags"]:
                lines.append(f"    ▸ {flag}")
            lines.append("")
    else:
        lines.append("")
        lines.append("─── POSITION HEALTH: All positions healthy ─────────────────────")

    # ── no-change positions ─────────────────────────────────────
    no_change = [t for t in plan.trades if t.action == "NO_CHANGE"]
    if no_change:
        lines.append("")
        lines.append(f"─── NO CHANGE ({len(no_change)} positions) "
                     "─────────────────────────────────")
        for t in no_change:
            lines.append(
                f"  {t.ticker:<8s}  {t.current_shares:>6d} shares  "
                f"${t.current_shares * t.current_price:>10,.0f}  "
                f"— {t.reason}"
            )

    lines.append("")
    lines.append("=" * 72)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  5.  HTML REBALANCE REPORT
# ═════════════════════════════════════════════════════════════════

def rebalance_to_html(plan: RebalancePlan) -> str:
    """Render the rebalance plan as a self-contained HTML page."""

    active_trades = [t for t in plan.trades if t.action != "NO_CHANGE"]

    # ── trade rows ──────────────────────────────────────────────
    trade_rows = ""
    for t in active_trades:
        action_class = {
            "BUY_NEW": "buy", "ADD": "buy", "TRIM": "sell", "CLOSE": "sell",
        }.get(t.action, "neutral")
        sign = "+" if t.delta_shares > 0 else ""
        stop_str = f"${t.stop_price:.2f}" if t.stop_price else "—"
        trade_rows += f"""
        <tr class="{action_class}">
            <td class="action-badge {action_class}">{t.action}</td>
            <td class="ticker">{t.ticker}</td>
            <td class="num">{t.current_shares}</td>
            <td class="num">{t.target_shares}</td>
            <td class="num delta">{sign}{t.delta_shares}</td>
            <td class="num">${t.delta_dollars:+,.0f}</td>
            <td class="num">{stop_str}</td>
            <td class="reason">{t.reason}</td>
        </tr>"""

    # ── drift rows ──────────────────────────────────────────────
    drift_rows = ""
    for bucket, d in sorted(plan.bucket_drift.items()):
        drift_class = "over" if d["drift_pct"] > 0.01 else "under" if d["drift_pct"] < -0.01 else ""
        drift_rows += f"""
        <tr class="{drift_class}">
            <td>{bucket.replace('_', ' ').title()}</td>
            <td class="num">{d['target_pct']:.1%}</td>
            <td class="num">{d['actual_pct']:.1%}</td>
            <td class="num drift">{d['drift_pct']:+.1%}</td>
            <td class="num">${d['drift_dollars']:+,.0f}</td>
        </tr>"""

    # ── health rows ─────────────────────────────────────────────
    flagged = [c for c in plan.health_checks if not c["healthy"]]
    health_rows = ""
    for c in flagged:
        flag_html = "<br>".join(f"⚠ {f}" for f in c["flags"])
        health_rows += f"""
        <tr>
            <td class="ticker">{c['ticker']}</td>
            <td class="num">${c['market_value']:,.0f}</td>
            <td class="num">{c['weight_pct']:.1%}</td>
            <td class="num pnl">{c['unrealised_pct']:+.1%}</td>
            <td class="flags">{flag_html}</td>
        </tr>"""

    # ── warnings ────────────────────────────────────────────────
    warning_html = ""
    if plan.warnings:
        items = "".join(f"<li>{w}</li>" for w in plan.warnings)
        warning_html = f"""
        <div class="warning-box">
            <h2>⚠ Warnings</h2>
            <ul>{items}</ul>
        </div>"""

    # ── execution summary ───────────────────────────────────────
    total_buy = sum(t.delta_dollars for t in plan.buy_trades)
    total_sell_abs = abs(sum(t.delta_dollars for t in plan.sell_trades))
    cash_after = plan.cash_balance + plan.net_cash_impact

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CASH Rebalance — {plan.date}</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, Helvetica, Arial, sans-serif;
        background: #0d1117; color: #c9d1d9;
        padding: 24px; max-width: 1100px; margin: 0 auto;
        font-size: 14px; line-height: 1.5;
    }}
    h1 {{ color: #58a6ff; font-size: 22px; }}
    h2 {{ color: #8b949e; font-size: 16px; margin: 24px 0 12px;
          border-bottom: 1px solid #30363d; padding-bottom: 6px; }}
    .subtitle {{ color: #8b949e; font-size: 13px; margin-bottom: 16px; }}
    .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
                   gap: 10px; margin: 16px 0; }}
    .stat-card {{ background: #161b22; padding: 12px; border-radius: 8px;
                   border: 1px solid #30363d; }}
    .stat-card .label {{ font-size: 11px; color: #8b949e; text-transform: uppercase; }}
    .stat-card .value {{ font-size: 18px; font-weight: 700; color: #e6edf3; }}
    table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 13px; }}
    th {{ text-align: left; font-size: 11px; color: #8b949e; text-transform: uppercase;
          padding: 8px 6px; border-bottom: 2px solid #30363d; }}
    td {{ padding: 7px 6px; border-bottom: 1px solid #21262d; }}
    tr:hover {{ background: #161b22; }}
    .ticker {{ font-weight: 700; color: #58a6ff; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .action-badge {{
        display: inline-block; padding: 2px 8px; border-radius: 4px;
        font-size: 11px; font-weight: 700; text-transform: uppercase;
    }}
    .action-badge.buy {{ background: #23863622; color: #3fb950; }}
    .action-badge.sell {{ background: #f8514922; color: #f85149; }}
    .delta {{ font-weight: 700; }}
    tr.buy .delta {{ color: #3fb950; }}
    tr.sell .delta {{ color: #f85149; }}
    .reason {{ font-size: 11px; color: #8b949e; max-width: 260px; }}
    .drift {{ font-weight: 700; }}
    tr.over .drift {{ color: #d29922; }}
    tr.under .drift {{ color: #58a6ff; }}
    .pnl {{ font-weight: 700; }}
    .flags {{ font-size: 11px; color: #d29922; line-height: 1.4; }}
    .warning-box {{ background: #2d1b1b; border: 1px solid #f85149;
                     border-radius: 8px; padding: 16px; margin: 16px 0; }}
    .warning-box h2 {{ color: #f85149; border: none; margin-top: 0; }}
    .warning-box li {{ margin: 4px 0 4px 20px; font-size: 13px; }}
    .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid #30363d;
               font-size: 11px; color: #484f58; text-align: center; }}
    @media (max-width: 700px) {{
        body {{ padding: 12px; }}
        table {{ font-size: 11px; }}
        .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
    }}
</style>
</head>
<body>

<h1>CASH — Rebalance Plan</h1>
<div class="subtitle">{plan.date} &nbsp;|&nbsp; Portfolio ${plan.portfolio_value:,.0f}</div>

{warning_html}

<div class="stat-grid">
    <div class="stat-card">
        <div class="label">Trades</div>
        <div class="value">{len(active_trades)}</div>
    </div>
    <div class="stat-card">
        <div class="label">Total Buy</div>
        <div class="value">${total_buy:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Total Sell</div>
        <div class="value">${total_sell_abs:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Net Impact</div>
        <div class="value">${plan.net_cash_impact:+,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Cash Before</div>
        <div class="value">${plan.cash_balance:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Cash After</div>
        <div class="value">${cash_after:,.0f}</div>
    </div>
</div>

<h2>Trades Required</h2>
<table>
    <thead>
        <tr><th>Action</th><th>Ticker</th><th>Current</th><th>Target</th>
            <th>Delta</th><th>$ Impact</th><th>Stop</th><th>Reason</th></tr>
    </thead>
    <tbody>
        {trade_rows if trade_rows else '<tr><td colspan="8">No trades required.</td></tr>'}
    </tbody>
</table>

<h2>Bucket Drift</h2>
<table>
    <thead>
        <tr><th>Bucket</th><th>Target</th><th>Actual</th><th>Drift</th><th>$ Drift</th></tr>
    </thead>
    <tbody>{drift_rows}</tbody>
</table>

{"<h2>Position Health Flags</h2>" if flagged else ""}
{"<table><thead><tr><th>Ticker</th><th>Value</th><th>Weight</th><th>P&L</th><th>Flags</th></tr></thead><tbody>" + health_rows + "</tbody></table>" if flagged else '<p style="color:#3fb950;margin-top:16px;">✓ All positions healthy — no flags.</p>'}

<div class="footer">
    CASH Rebalance Plan &nbsp;|&nbsp; {plan.date} &nbsp;|&nbsp;
    {len(active_trades)} trades &nbsp;|&nbsp;
    {len(flagged)} health flags
</div>

</body>
</html>"""

    return html


# ═════════════════════════════════════════════════════════════════
#  6.  FILE OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def save_rebalance_text(plan: RebalancePlan, filepath: str) -> str:
    text = rebalance_to_text(plan)
    with open(filepath, "w") as f:
        f.write(text)
    return filepath


def save_rebalance_html(plan: RebalancePlan, filepath: str) -> str:
    html = rebalance_to_html(plan)
    with open(filepath, "w") as f:
        f.write(html)
    return filepath


def print_rebalance(plan: RebalancePlan):
    print(rebalance_to_text(plan))


# ═════════════════════════════════════════════════════════════════
#  7.  CONVENIENCE: QUICK DIFF
# ═════════════════════════════════════════════════════════════════

def quick_diff(report: dict, current_positions: list[dict]) -> dict:
    """
    Fast summary of what changes are needed, without building a
    full RebalancePlan. Good for dashboards or quick checks.

    Returns dict with: new_buys, additions, trims, closes, holds, orphans.
    Each is a list of ticker strings.
    """
    current_tickers = {p["ticker"].upper() for p in current_positions}
    buy_tickers = {b["ticker"].upper() for b in report["buy_list"]}
    sell_tickers = {s["ticker"].upper() for s in report["sell_list"]}
    hold_tickers = {h["ticker"].upper() for h in report["hold_list"]}

    new_buys = buy_tickers - current_tickers
    additions = buy_tickers & current_tickers
    closes = sell_tickers & current_tickers
    holds = hold_tickers & current_tickers
    orphans = current_tickers - buy_tickers - sell_tickers - hold_tickers

    return {
        "new_buys":   sorted(new_buys),
        "additions":  sorted(additions),
        "trims":      [],  # would need share counts to determine
        "closes":     sorted(closes),
        "holds":      sorted(holds),
        "orphans":    sorted(orphans),
    }


# ═════════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═════════════════════════════════════════════════════════════════

def _find_in_list(lst: list, ticker: str) -> dict:
    """Find a ticker dict in a report list."""
    ticker = ticker.upper()
    for item in lst:
        if item["ticker"].upper() == ticker:
            return item
    return {"composite": 0, "confidence": 0}
    

#############################################

"""
reports/recommendations.py
Recommendation report generator for the CASH pipeline.

Accepts either a ``PipelineResult`` object (from
``pipeline.orchestrator``) **or** a legacy dict, and produces:

  1. A structured report dict (for programmatic use / JSON)
  2. A plain-text report  (for terminal / logging)
  3. An HTML report        (for browser / email)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = [
    "build_report",
    "to_text",
    "to_html",
    "save_text",
    "save_html",
    "print_report",
]


# ═════════════════════════════════════════════════════════════════
#  0.  PUBLIC ENTRY POINT
# ═════════════════════════════════════════════════════════════════

def build_report(source: Any) -> dict:
    """
    Build a structured recommendation report.

    Parameters
    ----------
    source : PipelineResult  **or**  dict
        • ``PipelineResult`` returned by
          ``orchestrator.run_full_pipeline()``
        • Legacy dict with keys *summary, regime, risk_flags,
          ranked_buys, sells, holds, bucket_weights*

    Returns
    -------
    dict
        Sections: *header, regime, allocation, buy_list,
        sell_list, hold_list, risk, portfolio_snapshot*
    """
    # Duck-type for PipelineResult (avoids circular import)
    if hasattr(source, "snapshots") and hasattr(source, "run_date"):
        raw = _from_pipeline_result(source)
    elif isinstance(source, dict):
        raw = source
    else:
        raise TypeError(
            f"build_report expects PipelineResult or dict, "
            f"got {type(source).__name__}"
        )
    return _assemble_report(raw)


# ═════════════════════════════════════════════════════════════════
#  1.  PipelineResult → RAW DICT TRANSFORMER  (FIXED)
# ═════════════════════════════════════════════════════════════════

def _from_pipeline_result(result: Any) -> dict:
    """
    Convert a ``PipelineResult`` into the flat dict format that
    ``_assemble_report()`` expects.

    Reads from the correct PipelineResult fields:
      result.snapshots       — per-ticker snapshot dicts
      result.portfolio       — output of build_portfolio():
                               metadata, target_weights,
                               sector_exposure, holdings (DataFrame),
                               candidates, rejected, excluded, trades
      result.breadth         — universe-level breadth DataFrame
      result.scored_universe — {ticker: DataFrame} for SPY lookup
      result.errors          — list of error strings
      result.run_date        — pd.Timestamp
    """
    snapshots = result.snapshots or []
    portfolio = result.portfolio or {}
    errors    = result.errors or []

    # ── Read from build_portfolio() output structure ─────────────
    #
    # build_portfolio() returns:
    #   metadata        dict  (num_holdings, total_invested,
    #                          cash_pct, breadth_regime, etc.)
    #   target_weights  {ticker: weight_fraction}
    #   sector_exposure {sector: total_weight}
    #   holdings        DataFrame of selected positions
    #   candidates      DataFrame of confirmed tickers
    #   rejected        DataFrame of rejected tickers
    #   excluded        DataFrame of candidates that didn't fit
    #   trades          DataFrame or None
    #
    meta             = portfolio.get("metadata", {})
    target_weights   = portfolio.get("target_weights", {})
    sector_exposure  = portfolio.get("sector_exposure", {})

    # ── Normalise every snapshot ────────────────────────────────
    normalised = [_normalise_snapshot(s) for s in snapshots]

    # ── Infer capital ───────────────────────────────────────────
    capital = _infer_capital_from_snapshots(normalised, meta)

    # ── Back-fill allocations from target_weights ───────────────
    if target_weights:
        _backfill_allocations(normalised, target_weights, capital)

    # ── Classify by signal ──────────────────────────────────────
    _buys, _sells, _holds = [], [], []
    for s in normalised:
        sig = s["signal"].upper()
        if sig in ("BUY", "STRONG_BUY"):
            _buys.append(s)
        elif sig in ("SELL", "STRONG_SELL"):
            _sells.append(s)
        else:
            _holds.append(s)
    _buys.sort(key=lambda x: x["composite"], reverse=True)

    # ── Breadth from universe-level DataFrame ───────────────────
    breadth_df = getattr(result, "breadth", None)

    breadth_regime = "unknown"
    breadth_score  = None
    if (
        breadth_df is not None
        and hasattr(breadth_df, "empty")
        and not breadth_df.empty
    ):
        if "breadth_regime" in breadth_df.columns:
            breadth_regime = str(
                breadth_df["breadth_regime"].iloc[-1]
            )
        if "breadth_score" in breadth_df.columns:
            breadth_score = float(
                breadth_df["breadth_score"].iloc[-1]
            )

    # ── Regime detection ────────────────────────────────────────
    spy_close, spy_sma200, above_sma200 = _spy_from_scored_universe(
        result,
    )
    regime_label, regime_desc = _detect_regime_fallback(
        above_sma200, breadth_regime,
    )

    # ── Capital figures ─────────────────────────────────────────
    total_buy = sum(
        _num(b.get("dollar_alloc"), 0) for b in _buys
    )
    cash_remaining = capital - total_buy
    cash_pct = (
        (cash_remaining / capital * 100)
        if capital > 0 else 100.0
    )

    # ── Bucket weights from sector exposure ─────────────────────
    bucket_weights: dict[str, float] = {}
    if sector_exposure:
        for sector, weight in sector_exposure.items():
            bucket_weights[sector] = weight
        bucket_weights["cash"] = _num(meta.get("cash_pct"), 0.05)
    else:
        bucket_weights = _default_bucket_weights(regime_label)

    # ── Risk flags ──────────────────────────────────────────────
    risk_flags: list[str] = []
    if breadth_regime == "weak":
        risk_flags.append(
            "BREADTH_WEAK: Market breadth is weak — "
            "reduced exposure recommended"
        )
    if regime_label in ("bear_mild", "bear_severe"):
        risk_flags.append(
            f"REGIME: {regime_label} — defensive "
            f"positioning recommended"
        )
    if regime_label == "bear_severe":
        risk_flags.append(
            "CIRCUIT_BREAKER: Severe bear — "
            "consider halting new buys"
        )

    return {
        "summary": {
            "date":             result.run_date,
            "portfolio_value":  capital,
            "regime":           regime_label,
            "regime_desc":      regime_desc,
            "spy_close":        spy_close,
            "tickers_analysed": len(snapshots),
            "buy_count":        len(_buys),
            "sell_count":       len(_sells),
            "hold_count":       len(_holds),
            "error_count":      len(errors),
            "total_buy_dollar": total_buy,
            "cash_remaining":   cash_remaining,
            "cash_pct":         cash_pct,
            "bucket_breakdown": {},
        },
        "regime": {
            "label":       regime_label,
            "description": regime_desc,
            "spy_close":   spy_close,
            "spy_sma200":  spy_sma200,
            "breadth":     breadth_score,
        },
        "risk_flags":        risk_flags,
        "portfolio_actions": [],
        "ranked_buys":       _buys,
        "sells":             _sells,
        "holds":             _holds,
        "bucket_weights":    bucket_weights,
    }


# ═════════════════════════════════════════════════════════════════
#  2.  ASSEMBLE STRUCTURED REPORT  (raw dict → report dict)
# ═════════════════════════════════════════════════════════════════

def _assemble_report(raw: dict) -> dict:
    """
    Transform the raw pipeline dict into the final report
    structure consumed by ``to_text()`` and ``to_html()``.
    """
    summary = raw.get("summary", {})
    regime  = raw.get("regime", {})
    flags   = raw.get("risk_flags", [])
    buys    = raw.get("ranked_buys", [])
    sells   = raw.get("sells", [])
    holds   = raw.get("holds", [])
    buckets = raw.get("bucket_weights", {})

    # ── header ──────────────────────────────────────────────────
    regime_label = regime.get(
        "label", summary.get("regime", "bull_cautious"),
    )
    header = {
        "title":           "CASH — Composite Adaptive Signal Hierarchy",
        "subtitle":        "Recommendation Report",
        "date":            _fmt_date(summary.get("date", datetime.now())),
        "portfolio_value": _num(summary.get("portfolio_value"), 0),
        "regime":          regime_label,
        "regime_desc":     summary.get("regime_desc", ""),
        "spy_close":       _num(summary.get("spy_close"), 0),
    }

    # ── regime section ──────────────────────────────────────────
    regime_section = {
        "label":       regime_label,
        "description": regime.get(
            "description", summary.get("regime_desc", ""),
        ),
        "spy_close":   _num(
            regime.get("spy_close", summary.get("spy_close")), 0,
        ),
        "spy_sma200":  regime.get("spy_sma200"),
        "breadth":     regime.get("breadth"),
        "guidance":    _regime_guidance(regime_label),
    }

    # ── allocation ──────────────────────────────────────────────
    allocation_section = {
        "bucket_weights": buckets,
        "actual_fill":    summary.get("bucket_breakdown", {}),
        "cash_pct":       _num(summary.get("cash_pct"), 0),
    }

    # ── buy list ────────────────────────────────────────────────
    buy_list = []
    for b in buys:
        sub = b.get("sub_scores", {})
        ind = b.get("indicators", {})
        rs  = b.get("rs", {})
        buy_list.append({
            "rank":           len(buy_list) + 1,
            "ticker":         b.get("ticker", "???"),
            "category":       b.get("category", ""),
            "bucket":         b.get("bucket", ""),
            "themes":         b.get("themes", []),
            "close":          _num(b.get("close"), 0),
            "composite":      _num(b.get("composite"), 0),
            "confidence":     _num(b.get("confidence"), 0),
            "signal":         "BUY",
            "shares":         int(_num(b.get("shares"), 0)),
            "dollar_alloc":   _num(b.get("dollar_alloc"), 0),
            "weight_pct":     _num(b.get("weight_pct"), 0),
            "stop_price":     b.get("stop_price"),
            "risk_per_share": b.get("risk_per_share"),
            "sub_scores": {
                "trend":        _num(sub.get("trend"), 0),
                "momentum":     _num(sub.get("momentum"), 0),
                "volatility":   _num(sub.get("volatility"), 0),
                "rel_strength": _num(sub.get("rel_strength"), 0),
            },
            "key_indicators": {
                "rsi":           _num(ind.get("rsi"), 50),
                "adx":           _num(ind.get("adx"), 20),
                "macd_hist":     _num(ind.get("macd_hist"), 0),
                "rs_percentile": _num(rs.get("rs_percentile"), 0.5),
                "rs_regime":     rs.get("rs_regime", "neutral"),
            },
        })

    # ── sell list ───────────────────────────────────────────────
    sell_list = []
    for s in sells:
        sub = s.get("sub_scores", {})
        ind = s.get("indicators", {})
        rs  = s.get("rs", {})
        sell_list.append({
            "ticker":     s.get("ticker", "???"),
            "category":   s.get("category", ""),
            "close":      _num(s.get("close"), 0),
            "composite":  _num(s.get("composite"), 0),
            "confidence": _num(s.get("confidence"), 0),
            "signal":     "SELL",
            "sub_scores": {
                "trend":        _num(sub.get("trend"), 0),
                "momentum":     _num(sub.get("momentum"), 0),
                "volatility":   _num(sub.get("volatility"), 0),
                "rel_strength": _num(sub.get("rel_strength"), 0),
            },
            "key_indicators": {
                "rsi":           _num(ind.get("rsi"), 50),
                "adx":           _num(ind.get("adx"), 20),
                "macd_hist":     _num(ind.get("macd_hist"), 0),
                "rs_percentile": _num(rs.get("rs_percentile"), 0.5),
                "rs_regime":     rs.get("rs_regime", "neutral"),
            },
        })

    # ── hold list (condensed) ───────────────────────────────────
    hold_list = []
    for h in holds:
        hold_list.append({
            "ticker":    h.get("ticker", "???"),
            "category":  h.get("category", ""),
            "close":     _num(h.get("close"), 0),
            "composite": _num(h.get("composite"), 0),
            "signal":    "HOLD",
        })
    hold_list.sort(key=lambda x: x["composite"], reverse=True)

    # ── risk flags ──────────────────────────────────────────────
    risk_section = {
        "flags":           flags,
        "circuit_breaker": any("CIRCUIT_BREAKER" in str(f) for f in flags),
        "exposure_warn":   any("EXPOSURE" in str(f) for f in flags),
    }

    # ── portfolio snapshot ──────────────────────────────────────
    snapshot = {
        "tickers_analysed": int(_num(summary.get("tickers_analysed"), 0)),
        "buy_count":        int(_num(summary.get("buy_count"), len(buy_list))),
        "sell_count":       int(_num(summary.get("sell_count"), len(sell_list))),
        "hold_count":       int(_num(summary.get("hold_count"), len(hold_list))),
        "error_count":      int(_num(summary.get("error_count"), 0)),
        "total_buy_dollar": _num(summary.get("total_buy_dollar"), 0),
        "cash_remaining":   _num(summary.get("cash_remaining"), 0),
        "cash_pct":         _num(summary.get("cash_pct"), 0),
    }

    return {
        "header":             header,
        "regime":             regime_section,
        "allocation":         allocation_section,
        "buy_list":           buy_list,
        "sell_list":          sell_list,
        "hold_list":          hold_list,
        "risk":               risk_section,
        "portfolio_snapshot": snapshot,
    }


# ═════════════════════════════════════════════════════════════════
#  3.  PLAIN-TEXT REPORT
# ═════════════════════════════════════════════════════════════════

def to_text(report: dict) -> str:
    """
    Render the structured report as a plain-text string.
    Suitable for terminal output or log files.
    """
    lines: list[str] = []
    h    = report["header"]
    r    = report["regime"]
    a    = report["allocation"]
    snap = report["portfolio_snapshot"]
    risk = report["risk"]

    # ── title block ─────────────────────────────────────────────
    lines.append("=" * 72)
    lines.append(f"  {h['title']}")
    lines.append(f"  {h['subtitle']}")
    lines.append(
        f"  Date: {h['date']}    "
        f"Portfolio: ${h['portfolio_value']:,.0f}"
    )
    lines.append("=" * 72)

    # ── regime ──────────────────────────────────────────────────
    lines.append("")
    lines.append(
        "─── MARKET REGIME "
        "───────────────────────────────────────────────────"
    )
    lines.append(
        f"  Regime:      {r['label'].upper()}  —  {r['description']}"
    )
    lines.append(f"  SPY Close:   ${r['spy_close']:,.2f}")
    if r.get("spy_sma200"):
        lines.append(f"  SPY SMA200:  ${r['spy_sma200']:,.2f}")
    lines.append(f"  Guidance:    {r['guidance']}")

    # ── risk flags ──────────────────────────────────────────────
    if risk["flags"]:
        lines.append("")
        lines.append(
            "─── ⚠  RISK FLAGS "
            "─────────────────────────────────────────────────"
        )
        for f in risk["flags"]:
            lines.append(f"  ▸ {f}")
        if risk["circuit_breaker"]:
            lines.append(
                "  *** CIRCUIT BREAKER ACTIVE — "
                "ALL BUYS DOWNGRADED TO HOLD ***"
            )

    # ── allocation targets ──────────────────────────────────────
    lines.append("")
    lines.append(
        "─── ALLOCATION TARGETS (this regime) "
        "────────────────────────────────"
    )
    for bucket, weight in sorted(a["bucket_weights"].items()):
        actual = a["actual_fill"].get(bucket, 0)
        lines.append(
            f"  {bucket:22s}  target {weight:5.0%}    "
            f"filled ${actual:>10,.0f}"
        )
    lines.append(f"  {'Cash':22s}          {a['cash_pct']:5.1f}%")

    # ── buy recommendations ─────────────────────────────────────
    lines.append("")
    lines.append(
        "─── BUY RECOMMENDATIONS "
        "────────────────────────────────────────────"
    )
    if report["buy_list"]:
        lines.append(
            f"  {'#':>3s}  {'Ticker':<8s} {'Cat':<18s} "
            f"{'Comp':>5s} {'Conf':>5s} {'Shares':>6s} "
            f"{'Dollar':>10s} {'Wt%':>5s} {'Stop':>8s}"
        )
        lines.append("  " + "-" * 68)
        for b in report["buy_list"]:
            stop_str = (
                f"${b['stop_price']:>7.2f}"
                if b["stop_price"] else "    n/a"
            )
            lines.append(
                f"  {b['rank']:3d}  {b['ticker']:<8s} "
                f"{str(b['category'])[:18]:<18s} "
                f"{b['composite']:5.2f} {b['confidence']:5.0%} "
                f"{b['shares']:6d} "
                f"${b['dollar_alloc']:>9,.0f} "
                f"{b['weight_pct']:5.1f} {stop_str}"
            )

        # detail block for top 10
        lines.append("")
        for b in report["buy_list"][:10]:
            sc = b["sub_scores"]
            ki = b["key_indicators"]
            lines.append(f"  {b['ticker']}:")
            lines.append(
                f"    Scores  → trend {sc['trend']:+.2f}  "
                f"mom {sc['momentum']:+.2f}  "
                f"vol {sc['volatility']:+.2f}  "
                f"RS {sc['rel_strength']:+.2f}"
            )
            lines.append(
                f"    Indicators → RSI {ki['rsi']:.0f}  "
                f"ADX {ki['adx']:.0f}  "
                f"MACD-H {ki['macd_hist']:+.3f}  "
                f"RS%ile {ki['rs_percentile']:.0%}  "
                f"RS-regime {ki['rs_regime']}"
            )
            themes = b.get("themes", [])
            if themes:
                lines.append(
                    f"    Themes  → {', '.join(themes)}"
                )
            lines.append("")
    else:
        lines.append("  No BUY signals this run.")

    # ── sell recommendations ────────────────────────────────────
    lines.append(
        "─── SELL RECOMMENDATIONS "
        "───────────────────────────────────────────"
    )
    if report["sell_list"]:
        lines.append(
            f"  {'Ticker':<8s} {'Cat':<18s} {'Comp':>5s} "
            f"{'Conf':>5s} {'RSI':>4s} {'RS%':>5s}"
        )
        lines.append("  " + "-" * 48)
        for s in report["sell_list"]:
            ki = s["key_indicators"]
            lines.append(
                f"  {s['ticker']:<8s} "
                f"{str(s['category'])[:18]:<18s} "
                f"{s['composite']:5.2f} {s['confidence']:5.0%} "
                f"{ki['rsi']:4.0f} {ki['rs_percentile']:5.0%}"
            )
    else:
        lines.append("  No SELL signals this run.")

    # ── holds (near-buy watchlist) ──────────────────────────────
    lines.append("")
    lines.append(
        "─── HOLD (top 15 by composite — watchlist) "
        "────────────────────────────"
    )
    for ho in report["hold_list"][:15]:
        lines.append(
            f"  {ho['ticker']:<8s} "
            f"{str(ho['category'])[:18]:<18s} "
            f"composite {ho['composite']:5.2f}"
        )

    # ── snapshot ────────────────────────────────────────────────
    lines.append("")
    lines.append(
        "─── PORTFOLIO SNAPSHOT "
        "─────────────────────────────────────────────"
    )
    lines.append(
        f"  Tickers analysed:  {snap['tickers_analysed']}"
    )
    lines.append(
        f"  BUY:  {snap['buy_count']}    "
        f"SELL:  {snap['sell_count']}    "
        f"HOLD:  {snap['hold_count']}    "
        f"Errors:  {snap['error_count']}"
    )
    lines.append(
        f"  Total buy allocation:  "
        f"${snap['total_buy_dollar']:,.0f}"
    )
    lines.append(
        f"  Cash remaining:        "
        f"${snap['cash_remaining']:,.0f}  "
        f"({snap['cash_pct']:.1f}%)"
    )
    lines.append("")
    lines.append("=" * 72)

    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  4.  HTML REPORT
# ═════════════════════════════════════════════════════════════════

def to_html(report: dict) -> str:
    """
    Render the structured report as self-contained HTML.
    Can be saved to file, opened in a browser, or emailed.
    """
    h    = report["header"]
    r    = report["regime"]
    a    = report["allocation"]
    snap = report["portfolio_snapshot"]
    risk = report["risk"]

    regime_color = _regime_color(r["label"])

    # ── buy rows ────────────────────────────────────────────────
    buy_rows = ""
    for b in report["buy_list"]:
        stop_str = (
            f"${b['stop_price']:.2f}" if b["stop_price"] else "—"
        )
        themes_str = (
            ", ".join(b["themes"]) if b.get("themes") else "—"
        )
        sc = b["sub_scores"]
        ki = b["key_indicators"]
        buy_rows += f"""
        <tr>
            <td class="rank">{b['rank']}</td>
            <td class="ticker">{b['ticker']}</td>
            <td class="cat">{b['category']}</td>
            <td class="num">{b['composite']:.2f}</td>
            <td class="num">{b['confidence']:.0%}</td>
            <td class="num">{b['shares']}</td>
            <td class="num">${b['dollar_alloc']:,.0f}</td>
            <td class="num">{b['weight_pct']:.1f}%</td>
            <td class="num">{stop_str}</td>
            <td class="detail">
                T&nbsp;{sc['trend']:+.2f} M&nbsp;{sc['momentum']:+.2f}
                V&nbsp;{sc['volatility']:+.2f} RS&nbsp;{sc['rel_strength']:+.2f}<br>
                RSI&nbsp;{ki['rsi']:.0f} ADX&nbsp;{ki['adx']:.0f}
                RS%ile&nbsp;{ki['rs_percentile']:.0%}
                ({ki['rs_regime']})<br>
                <span class="themes">{themes_str}</span>
            </td>
        </tr>"""

    # ── sell rows ───────────────────────────────────────────────
    sell_rows = ""
    for s in report["sell_list"]:
        ki = s["key_indicators"]
        sell_rows += f"""
        <tr>
            <td class="ticker">{s['ticker']}</td>
            <td class="cat">{s['category']}</td>
            <td class="num">{s['composite']:.2f}</td>
            <td class="num">{s['confidence']:.0%}</td>
            <td class="num">{ki['rsi']:.0f}</td>
            <td class="num">{ki['rs_percentile']:.0%}</td>
        </tr>"""

    # ── hold rows ───────────────────────────────────────────────
    hold_rows = ""
    for ho in report["hold_list"][:20]:
        hold_rows += f"""
        <tr>
            <td class="ticker">{ho['ticker']}</td>
            <td class="cat">{ho['category']}</td>
            <td class="num">{ho['composite']:.2f}</td>
        </tr>"""

    # ── allocation rows ─────────────────────────────────────────
    alloc_rows = ""
    for bucket, weight in sorted(a["bucket_weights"].items()):
        actual = a["actual_fill"].get(bucket, 0)
        alloc_rows += f"""
        <tr>
            <td>{bucket.replace('_', ' ').title()}</td>
            <td class="num">{weight:.0%}</td>
            <td class="num">${actual:,.0f}</td>
        </tr>"""

    # ── risk flags ──────────────────────────────────────────────
    risk_html = ""
    if risk["flags"]:
        flag_items = "".join(
            f"<li>{f}</li>" for f in risk["flags"]
        )
        cb_banner = ""
        if risk["circuit_breaker"]:
            cb_banner = (
                '<div class="circuit-breaker">'
                "⚠ CIRCUIT BREAKER ACTIVE — "
                "ALL BUYS DOWNGRADED TO HOLD"
                "</div>"
            )
        risk_html = f"""
        <div class="risk-section">
            <h2>⚠ Risk Flags</h2>
            {cb_banner}
            <ul>{flag_items}</ul>
        </div>"""

    # ── assemble page ───────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CASH Report — {h['date']}</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, Helvetica, Arial, sans-serif;
        background: #0d1117; color: #c9d1d9;
        padding: 24px; max-width: 1200px; margin: 0 auto;
        font-size: 14px; line-height: 1.5;
    }}
    h1 {{ color: #58a6ff; font-size: 22px; margin-bottom: 4px; }}
    h2 {{
        color: #8b949e; font-size: 16px; margin: 24px 0 12px;
        border-bottom: 1px solid #30363d; padding-bottom: 6px;
    }}
    .subtitle {{ color: #8b949e; font-size: 14px; }}
    .header-row {{
        display: flex; justify-content: space-between;
        align-items: center; flex-wrap: wrap; gap: 12px;
        margin-bottom: 16px;
    }}
    .regime-badge {{
        display: inline-block; padding: 6px 16px;
        border-radius: 6px; font-weight: 700; font-size: 14px;
        background: {regime_color}22; color: {regime_color};
        border: 1px solid {regime_color};
    }}
    .stat-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 12px; margin: 16px 0;
    }}
    .stat-card {{
        background: #161b22; padding: 12px; border-radius: 8px;
        border: 1px solid #30363d;
    }}
    .stat-card .label {{
        font-size: 11px; color: #8b949e; text-transform: uppercase;
    }}
    .stat-card .value {{
        font-size: 20px; font-weight: 700; color: #e6edf3;
    }}
    table {{ width: 100%; border-collapse: collapse; margin: 8px 0; }}
    th {{
        text-align: left; font-size: 11px; color: #8b949e;
        text-transform: uppercase; padding: 8px 6px;
        border-bottom: 2px solid #30363d;
    }}
    td {{
        padding: 7px 6px; border-bottom: 1px solid #21262d;
        font-size: 13px;
    }}
    tr:hover {{ background: #161b22; }}
    .rank {{ text-align: center; color: #8b949e; }}
    .ticker {{ font-weight: 700; color: #58a6ff; }}
    .cat {{ color: #8b949e; font-size: 12px; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .detail {{ font-size: 11px; color: #8b949e; line-height: 1.4; }}
    .themes {{ color: #a371f7; }}
    .risk-section {{
        background: #2d1b1b; border: 1px solid #f85149;
        border-radius: 8px; padding: 16px; margin: 16px 0;
    }}
    .risk-section h2 {{ color: #f85149; border: none; margin-top: 0; }}
    .risk-section li {{ margin: 4px 0 4px 20px; }}
    .circuit-breaker {{
        background: #f8514922; color: #f85149; padding: 10px;
        border-radius: 6px; font-weight: 700; text-align: center;
        margin-bottom: 12px;
    }}
    .sell-table .ticker {{ color: #f85149; }}
    .guidance {{
        background: #161b22; padding: 12px 16px; border-radius: 8px;
        border-left: 4px solid {regime_color}; margin: 12px 0;
        color: #e6edf3;
    }}
    .footer {{
        margin-top: 32px; padding-top: 16px;
        border-top: 1px solid #30363d;
        font-size: 11px; color: #484f58; text-align: center;
    }}
    @media (max-width: 700px) {{
        body {{ padding: 12px; font-size: 13px; }}
        .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
        table {{ font-size: 11px; }}
        td, th {{ padding: 5px 3px; }}
    }}
</style>
</head>
<body>

<!-- HEADER -->
<div class="header-row">
    <div>
        <h1>{h['title']}</h1>
        <div class="subtitle">{h['subtitle']}  —  {h['date']}</div>
    </div>
    <div class="regime-badge">{r['label'].upper()}</div>
</div>

<!-- STAT CARDS -->
<div class="stat-grid">
    <div class="stat-card">
        <div class="label">Portfolio</div>
        <div class="value">${h['portfolio_value']:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">SPY Close</div>
        <div class="value">${r['spy_close']:,.2f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Buys</div>
        <div class="value">{snap['buy_count']}</div>
    </div>
    <div class="stat-card">
        <div class="label">Sells</div>
        <div class="value">{snap['sell_count']}</div>
    </div>
    <div class="stat-card">
        <div class="label">Allocated</div>
        <div class="value">${snap['total_buy_dollar']:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Cash</div>
        <div class="value">{snap['cash_pct']:.1f}%</div>
    </div>
</div>

<!-- REGIME -->
<h2>Market Regime</h2>
<div class="guidance">{r['guidance']}</div>

<!-- RISK FLAGS -->
{risk_html}

<!-- ALLOCATION -->
<h2>Allocation Targets</h2>
<table>
    <thead><tr><th>Bucket</th><th>Target</th><th>Filled</th></tr></thead>
    <tbody>{alloc_rows}</tbody>
</table>

<!-- BUY RECOMMENDATIONS -->
<h2>Buy Recommendations ({snap['buy_count']})</h2>
<table>
    <thead>
        <tr>
            <th>#</th><th>Ticker</th><th>Category</th>
            <th>Comp</th><th>Conf</th><th>Shares</th>
            <th>Dollar</th><th>Wt%</th><th>Stop</th><th>Detail</th>
        </tr>
    </thead>
    <tbody>{buy_rows if buy_rows else '<tr><td colspan="10">No BUY signals this run.</td></tr>'}</tbody>
</table>

<!-- SELL RECOMMENDATIONS -->
<h2>Sell Recommendations ({snap['sell_count']})</h2>
<table class="sell-table">
    <thead>
        <tr>
            <th>Ticker</th><th>Category</th><th>Comp</th>
            <th>Conf</th><th>RSI</th><th>RS%</th>
        </tr>
    </thead>
    <tbody>{sell_rows if sell_rows else '<tr><td colspan="6">No SELL signals this run.</td></tr>'}</tbody>
</table>

<!-- HOLD / WATCHLIST -->
<h2>Hold / Watchlist (top 20)</h2>
<table>
    <thead>
        <tr><th>Ticker</th><th>Category</th><th>Composite</th></tr>
    </thead>
    <tbody>{hold_rows}</tbody>
</table>

<!-- FOOTER -->
<div class="footer">
    Generated by CASH v1.0 &nbsp;|&nbsp; {h['date']} &nbsp;|&nbsp;
    {snap['tickers_analysed']} tickers analysed &nbsp;|&nbsp;
    {snap['error_count']} errors
</div>

</body>
</html>"""

    return html


# ═════════════════════════════════════════════════════════════════
#  5.  FILE OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def save_text(report: dict, filepath: str) -> str:
    """Save plain-text report to *filepath*.  Returns filepath."""
    text = to_text(report)
    with open(filepath, "w") as f:
        f.write(text)
    return filepath


def save_html(report: dict, filepath: str) -> str:
    """Save HTML report to *filepath*.  Returns filepath."""
    html = to_html(report)
    with open(filepath, "w") as f:
        f.write(html)
    return filepath


def print_report(report: dict) -> None:
    """Print the plain-text report to stdout."""
    print(to_text(report))


# ═════════════════════════════════════════════════════════════════
#  6.  SNAPSHOT NORMALISER
# ═════════════════════════════════════════════════════════════════

def _normalise_snapshot(snap: dict) -> dict:
    """
    Normalise a per-ticker snapshot dict to a consistent schema.

    Handles divergent key names that arise from different
    pipeline phases (scoring vs portfolio construction vs
    signal generation).
    """

    def _get(*keys, default=None):
        for k in keys:
            if k in snap and snap[k] is not None:
                return snap[k]
        return default

    # Sub-scores — may be nested under "sub_scores" or flat
    raw_sub = snap.get("sub_scores", {})
    sub_scores = {
        "trend": _num(
            raw_sub.get("trend", _get("trend_score")), 0.0,
        ),
        "momentum": _num(
            raw_sub.get("momentum", _get("momentum_score")), 0.0,
        ),
        "volatility": _num(
            raw_sub.get("volatility", _get("volatility_score")), 0.0,
        ),
        "rel_strength": _num(
            raw_sub.get(
                "rel_strength",
                _get("rs_score", "relative_strength_score"),
            ),
            0.0,
        ),
    }

    # Indicators — may be nested or flat
    raw_ind = snap.get("indicators", {})
    indicators = {
        "rsi": _num(
            raw_ind.get("rsi", _get("rsi")), 50.0,
        ),
        "adx": _num(
            raw_ind.get("adx", _get("adx")), 20.0,
        ),
        "macd_hist": _num(
            raw_ind.get(
                "macd_hist",
                _get("macd_hist", "macd_histogram"),
            ),
            0.0,
        ),
    }

    # Relative strength — may be nested or flat
    raw_rs = snap.get("rs", {})
    rs = {
        "rs_percentile": _num(
            raw_rs.get("rs_percentile", _get("rs_percentile")),
            0.5,
        ),
        "rs_regime": (
            raw_rs.get("rs_regime")
            or _get("rs_regime")
            or "neutral"
        ),
    }

    return {
        "ticker":         _get("ticker", default="???"),
        "category":       _get("category", default=""),
        "bucket":         _get("bucket", default="core_equity"),
        "themes":         _get("themes", default=[]),
        "close":          float(_num(
            _get("close", "last_close", "price"), 0.0,
        )),
        "composite":      float(_num(
            _get("composite", "composite_score"), 0.0,
        )),
        "confidence":     float(_num(
            _get("confidence"), 0.5,
        )),
        "signal":         _get("signal", default="HOLD"),
        "shares":         int(_num(_get("shares"), 0)),
        "dollar_alloc":   float(_num(
            _get("dollar_alloc", "allocation", "dollar_allocation"),
            0.0,
        )),
        "weight_pct":     float(_num(
            _get("weight_pct", "weight", "portfolio_weight"),
            0.0,
        )),
        "stop_price":     _get("stop_price", "stop", default=None),
        "risk_per_share": _get("risk_per_share", default=None),
        "sub_scores":     sub_scores,
        "indicators":     indicators,
        "rs":             rs,
    }


# ═════════════════════════════════════════════════════════════════
#  7.  PRIVATE HELPERS
# ═════════════════════════════════════════════════════════════════

def _num(val: Any, default: float = 0) -> float:
    """Return *val* if it is a usable number, else *default*."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _fmt_date(dt: Any) -> str:
    """Format a date for display."""
    if isinstance(dt, pd.Timestamp):
        return dt.strftime("%Y-%m-%d")
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    return str(dt)


def _regime_color(label: str) -> str:
    """Hex colour for regime badge."""
    return {
        "bull_confirmed": "#3fb950",
        "bull_cautious":  "#d29922",
        "bear_mild":      "#db6d28",
        "bear_severe":    "#f85149",
    }.get(label, "#8b949e")


def _regime_guidance(label: str) -> str:
    """Human-readable guidance per regime."""
    return {
        "bull_confirmed": (
            "Strong uptrend confirmed.  Favour equity-heavy allocation.  "
            "Lean into momentum, growth sectors, and thematic plays.  "
            "Tighten stops only on extended names."
        ),
        "bull_cautious": (
            "Uptrend intact but showing signs of fatigue.  "
            "Maintain equity exposure but shift toward quality; "
            "begin adding fixed income / alternatives.  "
            "Widen watchlist for rotation opportunities."
        ),
        "bear_mild": (
            "Trend is deteriorating.  Reduce equity exposure significantly.  "
            "Favour defensive sectors (Staples, Utilities, Healthcare), "
            "increase fixed income and gold.  "
            "Only take high-conviction new positions."
        ),
        "bear_severe": (
            "Market in severe drawdown.  Capital preservation is priority.  "
            "Minimal equity exposure — only inverse or ultra-defensive.  "
            "Heavy fixed income and alternatives.  "
            "Circuit breaker may be active."
        ),
    }.get(label, "Regime not recognised — proceed with caution.")


def _regime_description(label: str) -> str:
    """Short description for a regime label."""
    return {
        "bull_confirmed": "Strong uptrend confirmed across breadth indicators",
        "bull_cautious":  "Uptrend intact but showing fatigue",
        "bear_mild":      "Trend deteriorating — defensive posture recommended",
        "bear_severe":    "Severe drawdown — capital preservation priority",
    }.get(label, "Market regime unclear")


def _default_bucket_weights(regime: str) -> dict:
    """Sensible fallback bucket weights when config is unavailable."""
    defaults = {
        "bull_confirmed": {
            "core_equity": 0.55, "tactical": 0.20,
            "fixed_income": 0.10, "alternatives": 0.10,
            "cash": 0.05,
        },
        "bull_cautious": {
            "core_equity": 0.45, "tactical": 0.15,
            "fixed_income": 0.20, "alternatives": 0.10,
            "cash": 0.10,
        },
        "bear_mild": {
            "core_equity": 0.25, "tactical": 0.10,
            "fixed_income": 0.30, "alternatives": 0.15,
            "cash": 0.20,
        },
        "bear_severe": {
            "core_equity": 0.10, "tactical": 0.05,
            "fixed_income": 0.35, "alternatives": 0.20,
            "cash": 0.30,
        },
    }
    return defaults.get(regime, defaults["bull_cautious"])


# ═════════════════════════════════════════════════════════════════
#  8.  PipelineResult BRIDGE HELPERS
# ═════════════════════════════════════════════════════════════════

def _infer_capital_from_snapshots(
    normalised: list[dict],
    meta: dict,
) -> float:
    """
    Infer total portfolio capital from snapshot allocation data.

    The orchestrator writes dollar_alloc and weight_pct into
    snapshots via ``_enrich_snapshots_with_allocations()``, but
    PipelineResult doesn't store capital directly.

    Strategies (in priority order):
      1. Derive from any position's dollar_alloc / weight_pct
      2. Sum all allocations and scale by metadata cash_pct
      3. Default to 100,000
    """
    # Strategy 1: single-position ratio
    for s in normalised:
        da = _num(s.get("dollar_alloc"), 0)
        wp = _num(s.get("weight_pct"), 0)
        if da > 0 and wp > 0.1:
            return da / (wp / 100.0)

    # Strategy 2: aggregate allocations + cash fraction
    total_alloc = sum(
        _num(s.get("dollar_alloc"), 0)
        for s in normalised
        if _num(s.get("dollar_alloc"), 0) > 0
    )
    cash_frac = _num(meta.get("cash_pct"), 0.05)
    if total_alloc > 0 and 0 < cash_frac < 1.0:
        return total_alloc / (1.0 - cash_frac)

    return 100_000


def _backfill_allocations(
    normalised: list[dict],
    target_weights: dict[str, float],
    capital: float,
) -> None:
    """
    Enrich normalised snapshots with allocation data from
    ``target_weights`` when not already present.

    Modifies *normalised* in place.
    """
    for s in normalised:
        ticker = s.get("ticker", "")
        weight = target_weights.get(ticker, 0.0)

        if weight > 0 and _num(s.get("dollar_alloc"), 0) == 0:
            close = _num(s.get("close"), 0)
            dollar = weight * capital
            shares = int(dollar / close) if close > 0 else 0

            s["weight_pct"]   = round(weight * 100, 2)
            s["dollar_alloc"] = round(dollar, 2)
            s["shares"]       = shares


def _spy_from_scored_universe(
    result: Any,
) -> tuple:
    """
    Extract SPY close and 200-day SMA from the best available source.

    Priority order:
      1. result.bench_df  — always present when orchestrator ran
      2. result.scored_universe["SPY"]  — only if SPY wasn't skipped
      3. result.snapshots  — last resort

    Returns ``(spy_close, spy_sma200, above_sma200)``.
    """
    spy_close    = 0.0
    spy_sma200   = None
    above_sma200 = True

    # ── 1. Benchmark DataFrame (primary — always populated) ───
    bench_df = getattr(result, "bench_df", None)
    if (
        bench_df is not None
        and hasattr(bench_df, "empty")
        and not bench_df.empty
        and "close" in bench_df.columns
    ):
        spy_close = float(bench_df["close"].iloc[-1])
        if len(bench_df) >= 200:
            sma = float(
                bench_df["close"].rolling(200).mean().iloc[-1]
            )
            spy_sma200   = sma
            above_sma200 = spy_close > sma
        return spy_close, spy_sma200, above_sma200

    # ── 2. Scored universe (SPY present if skip_benchmark=False) ─
    scored = getattr(result, "scored_universe", None) or {}
    if "SPY" in scored:
        spy_df = scored["SPY"]
        if (
            spy_df is not None
            and not spy_df.empty
            and "close" in spy_df.columns
        ):
            spy_close = float(spy_df["close"].iloc[-1])
            if len(spy_df) >= 200:
                sma = float(
                    spy_df["close"].rolling(200).mean().iloc[-1]
                )
                spy_sma200   = sma
                above_sma200 = spy_close > sma
            return spy_close, spy_sma200, above_sma200

    # ── 3. Snapshots (last resort) ────────────────────────────
    for s in (getattr(result, "snapshots", None) or []):
        if s.get("ticker") == "SPY":
            spy_close = _num(s.get("close"), 0.0)
            break

    return spy_close, spy_sma200, above_sma200


def _detect_regime_fallback(
    above_sma200: bool,
    breadth_regime: str,
) -> tuple:
    """
    Simple market regime detection from SPY trend and breadth.

    Mirrors ``orchestrator._detect_regime()`` logic so that
    reports generated via the fallback path produce consistent
    regime labels.

    Returns ``(regime_label, regime_description)``.
    """
    if above_sma200 and breadth_regime == "strong":
        return (
            "bull_confirmed",
            "SPY above 200d SMA, breadth strong",
        )
    elif above_sma200:
        return (
            "bull_cautious",
            f"SPY above 200d SMA, breadth {breadth_regime}",
        )
    elif breadth_regime == "weak":
        return (
            "bear_severe",
            "SPY below 200d SMA, breadth weak",
        )
    else:
        return (
            "bear_mild",
            f"SPY below 200d SMA, breadth {breadth_regime}",
        )


#################################################################
"""
reports/weekly_report.py
Weekly report wrapper for the CASH pipeline.

Runs the standard pipeline, saves output with ISO-week filenames,
and optionally compares against the previous week's JSON to
surface new / removed positions and regime changes.

Usage — programmatic::

    from reports.weekly_report import generate_weekly_report
    report = generate_weekly_report()

Usage — command-line::

    python -m reports.weekly_report
    python -m reports.weekly_report --capital 200000
    python -m reports.weekly_report --output-dir output/weekly
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from common.config import LOGS_DIR, UNIVERSE, PORTFOLIO_PARAMS
from pipeline.orchestrator import run_full_pipeline
from reports.recommendations import (
    build_report,
    save_text,
    save_html,
    to_text,
)

logger = logging.getLogger(__name__)

__all__ = [
    "generate_weekly_report",
    "load_previous_week",
    "compare_weeks",
    "weekly_diff_text",
]


# ═════════════════════════════════════════════════════════════════
#  GENERATE
# ═════════════════════════════════════════════════════════════════

def generate_weekly_report(
    universe: list[str] | None = None,
    capital: float | None = None,
    output_dir: str = "output/weekly",
    save: bool = True,
    include_diff: bool = True,
) -> dict:
    """
    Run the full pipeline and save a weekly report.

    Parameters
    ----------
    universe : list[str], optional
        Ticker list.  Defaults to ``UNIVERSE`` from config.
    capital : float, optional
        Portfolio value.  Defaults to config.
    output_dir : str
        Where to write the weekly files.
    save : bool
        Write files to disk.
    include_diff : bool
        Append a week-over-week diff section if a previous
        weekly JSON is found.

    Returns
    -------
    dict
        The structured report (same shape as ``build_report``
        output), with an extra ``"weekly_diff"`` key when
        *include_diff* is True and a previous week exists.
    """
    uni = universe or list(UNIVERSE)
    cap = capital or PORTFOLIO_PARAMS.get("total_capital", 100_000)

    logger.info(
        f"Weekly report: {len(uni)} tickers, ${cap:,.0f}"
    )

    # ── pipeline ────────────────────────────────────────────────
    result = run_full_pipeline(
        universe=uni,
        capital=cap,
        enable_breadth=True,
        enable_sectors=True,
        enable_signals=True,
        enable_backtest=False,
    )

    report = result.recommendation_report
    if report is None:
        report = build_report(result)

    # ── week-over-week diff ─────────────────────────────────────
    if include_diff:
        prev = load_previous_week(output_dir)
        if prev is not None:
            diff = compare_weeks(report, prev)
            report["weekly_diff"] = diff
            logger.info(
                f"Week diff: {len(diff['new_buys'])} new buys, "
                f"{len(diff['removed_buys'])} removed, "
                f"regime_changed={diff['regime_change']}"
            )
        else:
            report["weekly_diff"] = None
            logger.info("No previous weekly report found for diff")

    # ── save ────────────────────────────────────────────────────
    if save:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        week_str = datetime.now().strftime("%Y_W%V")
        date_str = datetime.now().strftime("%Y%m%d")

        txt_path  = out / f"weekly_{week_str}_{date_str}.txt"
        html_path = out / f"weekly_{week_str}_{date_str}.html"
        json_path = out / f"weekly_{week_str}_{date_str}.json"

        # Text — append diff section if available
        text = to_text(report)
        if report.get("weekly_diff"):
            text += "\n" + weekly_diff_text(report["weekly_diff"])
        with open(txt_path, "w") as f:
            f.write(text)
        logger.info(f"Weekly text  → {txt_path}")

        save_html(report, str(html_path))
        logger.info(f"Weekly HTML  → {html_path}")

        _save_json(report, str(json_path))
        logger.info(f"Weekly JSON  → {json_path}")

    return report


# ═════════════════════════════════════════════════════════════════
#  WEEK-OVER-WEEK COMPARISON
# ═════════════════════════════════════════════════════════════════

def load_previous_week(
    output_dir: str = "output/weekly",
) -> dict | None:
    """
    Load the most recent *previous* weekly JSON.

    Looks in *output_dir* for ``weekly_*.json`` files,
    sorts descending, and returns the second-newest
    (the newest is assumed to be the current run or an
    in-progress write).
    """
    out = Path(output_dir)
    if not out.exists():
        return None

    files = sorted(out.glob("weekly_*.json"), reverse=True)
    # Need at least one completed previous file
    target = files[1] if len(files) >= 2 else (
        files[0] if len(files) == 1 else None
    )
    if target is None:
        return None

    try:
        with open(target, "r") as f:
            data = json.load(f)
        logger.info(f"Previous week loaded: {target.name}")
        return data
    except (json.JSONDecodeError, IOError) as exc:
        logger.warning(f"Could not load previous week: {exc}")
        return None


def compare_weeks(
    current: dict, previous: dict,
) -> dict:
    """
    Compare two weekly report dicts and summarise changes.

    Returns
    -------
    dict with keys:
        new_buys, removed_buys, new_sells, removed_sells,
        regime_change, prev_regime, curr_regime,
        buy_count_delta, sell_count_delta
    """
    curr_buys = {
        b["ticker"] for b in current.get("buy_list", [])
    }
    prev_buys = {
        b["ticker"] for b in previous.get("buy_list", [])
    }
    curr_sells = {
        s["ticker"] for s in current.get("sell_list", [])
    }
    prev_sells = {
        s["ticker"] for s in previous.get("sell_list", [])
    }

    curr_regime = (
        current.get("header", {}).get("regime", "unknown")
    )
    prev_regime = (
        previous.get("header", {}).get("regime", "unknown")
    )

    return {
        "new_buys":         sorted(curr_buys - prev_buys),
        "removed_buys":     sorted(prev_buys - curr_buys),
        "retained_buys":    sorted(curr_buys & prev_buys),
        "new_sells":        sorted(curr_sells - prev_sells),
        "removed_sells":    sorted(prev_sells - curr_sells),
        "regime_change":    curr_regime != prev_regime,
        "prev_regime":      prev_regime,
        "curr_regime":      curr_regime,
        "buy_count_delta":  len(curr_buys) - len(prev_buys),
        "sell_count_delta": len(curr_sells) - len(prev_sells),
    }


def weekly_diff_text(diff: dict) -> str:
    """Render the week-over-week diff as a plain-text section."""
    lines = [
        "",
        "─── WEEK-OVER-WEEK CHANGES "
        "──────────────────────────────────────────",
    ]

    if diff["regime_change"]:
        lines.append(
            f"  ⚠ REGIME CHANGED: "
            f"{diff['prev_regime'].upper()} → "
            f"{diff['curr_regime'].upper()}"
        )
    else:
        lines.append(
            f"  Regime unchanged: {diff['curr_regime'].upper()}"
        )

    lines.append("")
    if diff["new_buys"]:
        lines.append(
            f"  NEW buys ({len(diff['new_buys'])}):     "
            + ", ".join(diff["new_buys"])
        )
    if diff["removed_buys"]:
        lines.append(
            f"  REMOVED buys ({len(diff['removed_buys'])}): "
            + ", ".join(diff["removed_buys"])
        )
    if diff["retained_buys"]:
        lines.append(
            f"  Retained buys ({len(diff['retained_buys'])}): "
            + ", ".join(diff["retained_buys"])
        )

    if diff["new_sells"]:
        lines.append(
            f"  NEW sells ({len(diff['new_sells'])}):    "
            + ", ".join(diff["new_sells"])
        )
    if diff["removed_sells"]:
        lines.append(
            f"  REMOVED sells ({len(diff['removed_sells'])}): "
            + ", ".join(diff["removed_sells"])
        )

    lines.append(
        f"  Buy count delta:  {diff['buy_count_delta']:+d}    "
        f"Sell count delta: {diff['sell_count_delta']:+d}"
    )
    lines.append("─" * 72)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  JSON SERIALISATION
# ═════════════════════════════════════════════════════════════════

def _save_json(data: dict, filepath: str) -> None:
    """Save report dict to JSON with numpy/pandas fallback."""
    import numpy as np

    def _ser(obj: Any) -> Any:
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=_ser)


# ═════════════════════════════════════════════════════════════════
#  CLI
# ═════════════════════════════════════════════════════════════════

def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CASH — Weekly report generator",
    )
    p.add_argument(
        "--capital", type=float, default=None,
        help="Portfolio value (overrides config)",
    )
    p.add_argument(
        "--output-dir", type=str, default="output/weekly",
        help="Directory for weekly reports",
    )
    p.add_argument(
        "--no-diff", action="store_true",
        help="Skip week-over-week comparison",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Debug logging",
    )
    return p


def _cli_main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)-24s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    report = generate_weekly_report(
        capital=args.capital,
        output_dir=args.output_dir,
        include_diff=not args.no_diff,
    )

    snap = report.get("portfolio_snapshot", {})
    print(
        f"\nWeekly report complete: "
        f"{snap.get('buy_count', 0)} buys, "
        f"{snap.get('sell_count', 0)} sells"
    )

    diff = report.get("weekly_diff")
    if diff:
        print(weekly_diff_text(diff))


if __name__ == "__main__":
    _cli_main()

