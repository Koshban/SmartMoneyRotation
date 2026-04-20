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
  result = run_full_pipeline()

  # Interactive (notebook)
  orch = Orchestrator()
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
    ACTIVE_MARKETS
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
    bench_df: Optional[pd.DataFrame] = None       # ← NEW

    # ── NEW: convergence + rotation ───────────────────────────
    rotation_result: Optional[Any] = None            # RotationResult
    convergence: Optional[Any] = None                # MarketSignalResult
    market: str = "US"

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

    class Orchestrator:
        def __init__(
            self,
            *,
            market: str = "US",                       # ← NEW
            universe: list[str] | None = None,
            benchmark: str | None = None,
            capital: float | None = None,
            as_of: pd.Timestamp | None = None,
            enable_breadth: bool = True,
            enable_sectors: bool = True,
            enable_signals: bool = True,
            enable_backtest: bool = False,
        ):
            # ── Market-aware defaults ─────────────────────────────
            self.market: str = market                  # ← NEW
            mcfg = MARKET_CONFIG.get(market, {})

            self.tickers: list[str] = universe or list(
                mcfg.get("universe", UNIVERSE)
            )
            self.benchmark: str = benchmark or mcfg.get(
                "benchmark", BENCHMARK_TICKER
            )
            self.capital: float = capital or PORTFOLIO_PARAMS["total_capital"]
            self.as_of: pd.Timestamp | None = as_of

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

            # ── NEW: rotation + convergence state ─────────────────
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

        Parameters
        ----------
        preloaded : dict, optional
            ``{ticker: OHLCV DataFrame}`` to skip data loading.
        bench_df : pd.DataFrame, optional
            Pre-loaded benchmark OHLCV.
        """
        t0 = time.perf_counter()

        if preloaded is not None:
            self._ohlcv = preloaded
            logger.info(
                f"Phase 0: Using {len(preloaded)} pre-loaded "
                f"ticker DataFrames"
            )
        else:
            all_symbols = list(set(self.tickers + [self.benchmark]))
            self._ohlcv = load_universe_ohlcv(all_symbols)
            logger.info(
                f"Phase 0: Loaded {len(self._ohlcv)} tickers "
                f"from data source"
            )

        # ── Extract or load benchmark ─────────────────────────
        if bench_df is not None:
            self._bench_df = bench_df
        elif self.benchmark in self._ohlcv:
            self._bench_df = self._ohlcv[self.benchmark]
        else:
            self._bench_df = load_ohlcv(self.benchmark)

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
        #
        # compute_all_breadth() expects {ticker: OHLCV DataFrame}
        # with at least 'close' and optionally 'volume' columns.
        # It internally aligns them into a panel.
        #
        # breadth_to_pillar_scores() takes the breadth DataFrame
        # plus a list of symbols and broadcasts the breadth score
        # to every ticker (same market-level score per day).
        #
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
        #
        # compute_all_sector_rs() expects:
        #   sector_data: {sector_name: OHLCV DataFrame}
        #   benchmark_df: OHLCV DataFrame
        # Returns MultiIndex (date, sector) panel.
        #
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

        try:
            r_cfg = config or RotationConfig(
                benchmark=self.benchmark,
            )
            self._rotation_result = run_rotation(
                prices=prices,
                current_holdings=current_holdings or [],
                config=r_cfg,
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
        except Exception as e:
            logger.warning(
                f"Phase 2.5: Rotation failed — {e}.  "
                f"Proceeding with scoring only."
            )
            self._rotation_result = None

        elapsed = time.perf_counter() - t0
        self._timings["rotation"] = elapsed
        self._phases_completed.append("rotation")
    

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
        #
        # compute_all_rankings() takes {ticker: scored_df} and
        # returns MultiIndex (date, ticker) panel with rank,
        # pct_rank, pillar_agreement, rank_change columns.
        #
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
        #
        # build_portfolio() takes {ticker: scored_df} and
        # internally does snapshot extraction, candidate
        # filtering, ranking, selection, and weight
        # normalization.  Returns a dict with target_weights,
        # holdings DataFrame, sector_exposure, metadata, etc.
        #
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
        #
        # compute_all_signals() takes the ranked panel from 3a
        # and applies cross-sectional rank filters with
        # hysteresis, position limits, and a breadth circuit
        # breaker to produce BUY/HOLD/SELL/NEUTRAL signals.
        #
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
        #
        # build_report() expects a specific dict format with
        # keys: summary, regime, risk_flags, portfolio_actions,
        # ranked_buys, sells, holds, bucket_weights.
        #
        # _build_report_input() bridges from the orchestrator's
        # internal state to that format.
        #
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
        #
        # run_backtest() takes the signals DataFrame from
        # Phase 3c (MultiIndex: date × ticker with signal,
        # signal_strength, close columns).
        #
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
            rotation_result=self._rotation_result,       # ← NEW
            convergence=self._convergence_result,        # ← NEW
            market=self.market,                          # ← NEW
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

        # ── NEW: rotation + convergence ───────────────────
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

        The snapshot dicts produced by ``runner._build_snapshot``
        already contain the nested structure (sub_scores,
        indicators, rs) that ``build_report`` reads, so this
        is primarily a partitioning + metadata assembly step.
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
    market: str = "US",                                # ← NEW
    universe: list[str] | None = None,
    benchmark: str | None = None,
    capital: float | None = None,
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, pd.DataFrame] | None = None,
    bench_df: pd.DataFrame | None = None,
    current_holdings: list[str] | None = None,         # ← NEW
    enable_breadth: bool = True,
    enable_sectors: bool = True,
    enable_signals: bool = True,
    enable_backtest: bool = False,
) -> PipelineResult:
    """
    Run the full CASH pipeline end-to-end for one market.

    This is the main entry point for CLI usage and scheduled
    jobs.  For multi-market, use ``run_multi_market_pipeline()``.
    For interactive control, use ``Orchestrator`` directly.
    """
    orch = Orchestrator(
        market=market,                                 # ← NEW
        universe=universe,
        benchmark=benchmark,
        capital=capital,
        as_of=as_of,
        enable_breadth=enable_breadth,
        enable_sectors=enable_sectors,
        enable_signals=enable_signals,
        enable_backtest=enable_backtest,
    )

    return orch.run_all(
        preloaded=preloaded,
        bench_df=bench_df,
        current_holdings=current_holdings,             # ← NEW
    )


# ═══════════════════════════════════════════════════════════════
#  MULTI-MARKET PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_multi_market_pipeline(
    *,
    active_markets: list[str] | None = None,
    capital: float | None = None,
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

        results = run_multi_market_pipeline()
        us = results["US"]
        hk = results["HK"]

        # US has convergence data
        for s in us.convergence.strong_buys:
            print(f"{s.ticker}: STRONG BUY, adj={s.adjusted_score:.3f}")

        # HK has scoring-only signals
        for s in hk.convergence.buys:
            print(f"{s.ticker}: BUY, score={s.composite_score:.3f}")
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
scripts/run_market.py
─────────────────────
Run the CASH pipeline for one market and generate an HTML
recommendations report.

Usage
─────
  # US (default) — scoring + rotation convergence
  python -m scripts.run_market

  # Hong Kong — scoring only, 180 days data
  python -m scripts.run_market -m HK -n 180

  # India — open report in browser automatically
  python -m scripts.run_market -m IN --open

  # US with current holdings for rotation sell evaluation
  python -m scripts.run_market -m US --holdings NVDA,CRWD,PANW

  # Custom output path
  python -m scripts.run_market -m US -o ~/reports/us_today.html
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import webbrowser
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Pipeline imports ──────────────────────────────────────────
from pipeline.orchestrator import Orchestrator, run_full_pipeline
from strategy.convergence import convergence_report
from reports.html_report import generate_html_report
from common.config import MARKET_CONFIG, ACTIVE_MARKETS


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="run_market",
        description="Run CASH pipeline and generate HTML report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m scripts.run_market
  python -m scripts.run_market -m HK -n 180
  python -m scripts.run_market -m US --holdings NVDA,CRWD --open
        """,
    )

    p.add_argument(
        "-m", "--market",
        type=str,
        default="US",
        choices=list(MARKET_CONFIG.keys()),
        help="Market to analyse (default: US)",
    )
    p.add_argument(
        "-n", "--days",
        type=int,
        default=365,
        help="Lookback days for data loading (default: 365)",
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
        help="Comma-separated current holdings for rotation "
             "sell evaluation (e.g. NVDA,CRWD,PANW)",
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
        "--no-backtest",
        action="store_true",
        default=True,
        help="Skip backtest phase (default: skip)",
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

    # ── Info banner ───────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  CASH Pipeline — {market}")
    logger.info(f"  Benchmark  : {mcfg['benchmark']}")
    logger.info(f"  Universe   : {len(mcfg['universe'])} tickers")
    logger.info(f"  Engines    : {mcfg['engines']}")
    logger.info(f"  Lookback   : {args.days} days")
    if holdings:
        logger.info(f"  Holdings   : {holdings}")
    logger.info("=" * 60)

    # ── Run pipeline ──────────────────────────────────────────
    t0 = time.perf_counter()

    try:
        result = run_full_pipeline(
            market=market,
            capital=args.capital,
            current_holdings=holdings if holdings else None,
            enable_backtest=not args.no_backtest,
        )
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.perf_counter() - t0
    logger.info(f"Pipeline completed in {elapsed:.1f}s")

    # ── Text report (optional) ────────────────────────────────
    if args.text and result.convergence:
        print()
        print(convergence_report(result.convergence))
        print()

    # ── HTML report ───────────────────────────────────────────
    logger.info("Generating HTML report…")

    html = generate_html_report(result)

    # Determine output path
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
    if result.convergence:
        conv = result.convergence
        print()
        print(f"  {market}  |  {conv.n_tickers} tickers")
        print(f"  STRONG_BUY : {len(conv.strong_buys)}")
        print(f"  BUY        : {len(conv.buys)}")
        print(f"  CONFLICT   : {len(conv.conflicts)}")
        print(f"  SELL        : {len(conv.sells)}")
        print(f"  HOLD       : {len(conv.holds)}")

        if conv.strong_buys:
            print()
            print("  Top STRONG_BUY:")
            for s in conv.strong_buys[:5]:
                print(f"    #{s.rank}  {s.ticker:<8s}  "
                      f"adj={s.adjusted_score:.3f}")

    print()
    print(f"  Report → {out_path.resolve()}")
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

#####################################################################
"""
SRC : 
-----------
"""
"""
src/db/loader.py
--------------
Unified OHLCV data loader for the CASH compute pipeline.

Reads from:
  1. Local parquet files (data/universe_ohlcv.parquet, data/india_ohlcv.parquet)
  2. PostgreSQL regional cash tables (if parquet unavailable)
  3. yfinance (fallback for missing tickers)

Returns DataFrames in the standard format expected by compute/:
  - Columns: open, high, low, close, volume
  - DatetimeIndex named "date", sorted ascending
  - No NaN/zero closes
"""
from __future__ import annotations
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd
from common.config import DATA_DIR
import logging
logger = logging.getLogger(__name__)

# ── Parquet paths ─────────────────────────────────────────────
_UNIVERSE_PARQUET = DATA_DIR / "universe_ohlcv.parquet"
_INDIA_PARQUET    = DATA_DIR / "india_ohlcv.parquet"

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]

# ── Module-level cache ────────────────────────────────────────
# Loaded once per session to avoid re-reading parquet on every
# single-ticker call.  Keyed by parquet path.
_parquet_cache: dict[Path, pd.DataFrame] = {}


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def load_ohlcv(
    ticker: str,
    source: str = "auto",
) -> pd.DataFrame:
    """
    Load OHLCV for a single ticker.

    Parameters
    ----------
    ticker : str
        Symbol, e.g. "AAPL", "XLK", "RELIANCE.NS", "2800.HK".
    source : str
        "parquet" — local parquet files only
        "db"      — PostgreSQL only
        "yfinance"— yfinance download
        "auto"    — try parquet → db → yfinance

    Returns
    -------
    pd.DataFrame
        Columns: open, high, low, close, volume.
        DatetimeIndex sorted ascending.
        Empty DataFrame if loading fails.
    """
    if source == "auto":
        # Try parquet first (fast, no network)
        df = _load_from_parquet(ticker)
        if not df.empty:
            return df

        # Try DB
        df = _load_from_db(ticker)
        if not df.empty:
            return df

        # Fallback to yfinance
        df = _load_from_yfinance(ticker)
        return df

    if source == "parquet":
        return _load_from_parquet(ticker)
    elif source == "db":
        return _load_from_db(ticker)
    elif source == "yfinance":
        return _load_from_yfinance(ticker)
    else:
        logger.warning(f"Unknown source '{source}' for {ticker}")
        return pd.DataFrame()


def load_universe_ohlcv(
    tickers: list[str],
    source: str = "auto",
) -> dict[str, pd.DataFrame]:
    """
    Load OHLCV for multiple tickers.

    For parquet sources, this is efficient: reads the file once
    and extracts all tickers from the cached DataFrame.

    Returns {ticker: DataFrame} for successfully loaded symbols.
    Failed tickers are logged and skipped.
    """
    # Pre-warm the parquet cache if using auto/parquet
    if source in ("auto", "parquet"):
        _ensure_parquet_cached()

    result: dict[str, pd.DataFrame] = {}
    missing: list[str] = []

    for ticker in tickers:
        try:
            df = load_ohlcv(ticker, source=source)
            if not df.empty:
                result[ticker] = df
            else:
                missing.append(ticker)
        except Exception as e:
            logger.warning(f"Failed to load {ticker}: {e}")
            missing.append(ticker)

    logger.info(
        f"Loaded {len(result)}/{len(tickers)} tickers"
        + (f" (missing: {len(missing)})" if missing else "")
    )

    if missing and len(missing) <= 20:
        logger.debug(f"Missing tickers: {missing}")

    return result


def get_available_tickers(source: str = "parquet") -> list[str]:
    """
    Return list of tickers available in the data source.

    Useful for verifying universe coverage before running
    the pipeline.
    """
    if source == "parquet":
        _ensure_parquet_cached()
        tickers = set()
        for path, df in _parquet_cache.items():
            if "_sym_col" in df.attrs:
                sym_col = df.attrs["_sym_col"]
                tickers.update(df[sym_col].unique().tolist())
        return sorted(tickers)

    return []


def data_summary() -> dict:
    """
    Quick summary of available data files and coverage.

    Returns dict with file paths, sizes, ticker counts,
    date ranges.
    """
    info = {}

    for label, path in [
        ("universe", _UNIVERSE_PARQUET),
        ("india", _INDIA_PARQUET),
    ]:
        if path.exists():
            df = _read_parquet_raw(path)
            sym_col = _find_symbol_col(df)
            date_col = _find_date_col(df)

            entry = {
                "path": str(path),
                "size_mb": round(path.stat().st_size / (1024 * 1024), 1),
                "rows": len(df),
            }
            if sym_col:
                entry["tickers"] = int(df[sym_col].nunique())
                entry["ticker_list"] = sorted(df[sym_col].unique().tolist())
            if date_col:
                dates = pd.to_datetime(df[date_col])
                entry["date_min"] = str(dates.min().date())
                entry["date_max"] = str(dates.max().date())

            info[label] = entry
        else:
            info[label] = {"path": str(path), "exists": False}

    return info


# ═══════════════════════════════════════════════════════════════
#  PARQUET LOADING
# ═══════════════════════════════════════════════════════════════

def _ensure_parquet_cached() -> None:
    """Load parquet files into module-level cache if not already."""
    for path in [_UNIVERSE_PARQUET, _INDIA_PARQUET]:
        if path.exists() and path not in _parquet_cache:
            try:
                df = _read_parquet_raw(path)
                sym_col = _find_symbol_col(df)
                if sym_col:
                    df.attrs["_sym_col"] = sym_col
                    _parquet_cache[path] = df
                    n_syms = df[sym_col].nunique()
                    logger.info(
                        f"Cached {path.name}: {len(df):,} rows, "
                        f"{n_syms} symbols"
                    )
                else:
                    logger.warning(
                        f"No symbol column in {path.name}, "
                        f"columns: {list(df.columns)}"
                    )
            except Exception as e:
                logger.warning(f"Failed to cache {path.name}: {e}")


def _load_from_parquet(ticker: str) -> pd.DataFrame:
    """Extract a single ticker's OHLCV from cached parquet data."""
    _ensure_parquet_cached()

    for path, df in _parquet_cache.items():
        sym_col = df.attrs.get("_sym_col")
        if sym_col is None:
            continue

        mask = df[sym_col] == ticker
        if not mask.any():
            # Try case-insensitive
            mask = df[sym_col].str.upper() == ticker.upper()

        if mask.any():
            subset = df[mask].copy()
            return _normalise(subset)

    return pd.DataFrame()


def _read_parquet_raw(path: Path) -> pd.DataFrame:
    """Read a parquet file, resetting any index."""
    df = pd.read_parquet(path)
    # If the index looks like a date, reset it to a column
    if isinstance(df.index, pd.DatetimeIndex) or df.index.name in (
        "Date", "date", "trade_date",
    ):
        df = df.reset_index()
    return df


# ═══════════════════════════════════════════════════════════════
#  DATABASE LOADING
# ═══════════════════════════════════════════════════════════════

def _load_from_db(ticker: str) -> pd.DataFrame:
    """
    Load from PostgreSQL regional cash tables.

    Determines the correct table (us_cash, hk_cash, india_cash,
    others_cash) from the ticker suffix.
    """
    try:
        import psycopg2
        from common.credentials import PG_CONFIG
    except ImportError:
        return pd.DataFrame()

    # Determine region/table
    table = _ticker_to_cash_table(ticker)

    try:
        conn = psycopg2.connect(**PG_CONFIG)
        query = f"""
            SELECT date, open, high, low, close, volume
            FROM {table}
            WHERE symbol = %s
            ORDER BY date ASC
        """
        df = pd.read_sql(query, conn, params=(ticker,))
        conn.close()

        if df.empty:
            return pd.DataFrame()

        return _normalise(df)

    except Exception as e:
        logger.debug(f"DB load failed for {ticker} from {table}: {e}")
        return pd.DataFrame()


def _ticker_to_cash_table(ticker: str) -> str:
    """Map ticker to the correct regional cash table name."""
    t = ticker.upper()
    if t.endswith(".HK"):
        return "hk_cash"
    elif t.endswith(".NS") or t.endswith(".BO"):
        return "india_cash"
    elif "." not in t:
        return "us_cash"
    else:
        return "others_cash"


# ═══════════════════════════════════════════════════════════════
#  YFINANCE LOADING
# ═══════════════════════════════════════════════════════════════

def _load_from_yfinance(
    ticker: str,
    period: str = "2y",
) -> pd.DataFrame:
    """Load from yfinance as last-resort fallback."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — cannot fallback")
        return pd.DataFrame()

    try:
        raw = yf.download(
            ticker, period=period, progress=False, auto_adjust=False,
        )
        if raw.empty:
            return pd.DataFrame()
        return _normalise(raw)
    except Exception as e:
        logger.debug(f"yfinance failed for {ticker}: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
#  NORMALISATION
# ═══════════════════════════════════════════════════════════════

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise any OHLCV DataFrame to the standard format
    expected by compute/:

      - Columns: open, high, low, close, volume (lowercase)
      - DatetimeIndex named "date", sorted ascending
      - No duplicate dates
      - No rows where close is NaN or zero
    """
    df = df.copy()

    # ── Flatten MultiIndex columns (yfinance multi-ticker) ────
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            c[0] if isinstance(c, tuple) else c
            for c in df.columns
        ]

    # ── Lowercase column names ────────────────────────────────
    df.columns = [str(c).lower().strip() for c in df.columns]

    # ── Rename known variants to canonical names ──────────────
    # Applied once — downstream code only references canonical names.
    _COLUMN_RENAMES = {
        "adj close":  "adj_close",
        "trade_date": "date",
    }
    df.rename(
        columns={k: v for k, v in _COLUMN_RENAMES.items() if k in df.columns},
        inplace=True,
    )

    # ── Set date index ────────────────────────────────────────
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            logger.debug("Cannot convert index to datetime")
            return pd.DataFrame()

    df.index.name = "date"

    # ── Drop non-OHLCV columns ────────────────────────────────
    # After renaming, only canonical names exist.  We keep
    # exactly _REQUIRED_COLS and discard everything else.
    # This is safer than maintaining an ever-growing deny-list:
    # new metadata columns from DB or yfinance are automatically
    # excluded without needing code changes.
    keep = [c for c in df.columns if c in _REQUIRED_COLS]
    missing = [c for c in _REQUIRED_COLS if c not in df.columns]

    if missing:
        logger.debug(
            f"Missing required columns after normalisation: {missing}. "
            f"Available: {list(df.columns)}"
        )
        return pd.DataFrame()

    df = df[keep]

    # ── Clean ─────────────────────────────────────────────────
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df["close"].notna() & (df["close"] > 0)]

    # Ensure numeric types
    for col in _REQUIRED_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where any OHLC is NaN (volume NaN is OK, fill with 0)
    df = df.dropna(subset=["open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype(np.int64)

    return df


# ═══════════════════════════════════════════════════════════════
#  COLUMN FINDERS
# ═══════════════════════════════════════════════════════════════

def _find_symbol_col(df: pd.DataFrame) -> str | None:
    """Find the symbol/ticker column in a DataFrame."""
    for candidate in ["symbol", "Symbol", "ticker", "Ticker",
                      "SYMBOL", "TICKER"]:
        if candidate in df.columns:
            return candidate
    if df.index.name in ("symbol", "ticker"):
        name = df.index.name
        df.reset_index(inplace=True)
        return name
    return None


def _find_date_col(df: pd.DataFrame) -> str | None:
    """Find the date column in a DataFrame."""
    for candidate in ["Date", "date", "trade_date", "Trade_Date",
                      "DATE", "timestamp"]:
        if candidate in df.columns:
            return candidate
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index.name or "index"
    return None


# ═══════════════════════════════════════════════════════════════
#  CLI — Quick test / diagnostics
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    print("\n" + "=" * 60)
    print("  DATA LOADER — Diagnostics")
    print("=" * 60)

    # Summary
    summary = data_summary()
    for label, info in summary.items():
        print(f"\n  {label.upper()}:")
        if info.get("exists") is False:
            print(f"    File: {info['path']}  — NOT FOUND")
        else:
            print(f"    File:    {info['path']}")
            print(f"    Size:    {info.get('size_mb', '?')} MB")
            print(f"    Rows:    {info.get('rows', '?'):,}")
            print(f"    Tickers: {info.get('tickers', '?')}")
            print(f"    Range:   {info.get('date_min', '?')} → "
                  f"{info.get('date_max', '?')}")

    # Test loading a few tickers
    test_tickers = ["SPY", "QQQ", "XLK"]
    print(f"\n  Test loading: {test_tickers}")
    for t in test_tickers:
        df = load_ohlcv(t)
        if df.empty:
            print(f"    {t}: NO DATA")
        else:
            print(f"    {t}: {len(df)} bars, "
                  f"{df.index[0].date()} → {df.index[-1].date()}, "
                  f"close={df['close'].iloc[-1]:.2f}")

    print("\n" + "=" * 60)

##################################################################################
"""
src/db/schema.py

Single source of truth for all DB table definitions.

Usage:
    python src/db/schema.py create          # Create all tables
    python src/db/schema.py drop --yes      # Drop all tables (confirm required)
    python src/db/schema.py recreate --yes  # Drop + Create
    python src/db/schema.py status          # Show which tables exist
    python src/db/schema.py drop-options --yes  # Drop only options tables
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg2

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONNECTION
# ═══════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


# ═══════════════════════════════════════════════════════════════
#  CASH TABLE DDL  (unchanged from your current schema)
# ═══════════════════════════════════════════════════════════════

CASH_REGIONS = ["us", "hk", "india", "others"]

def _cash_ddl(region: str) -> str:
    """
    Cash (equity/ETF) OHLCV table.

    Columns:  date, symbol, open, high, low, close, volume
    Unique:   (date, symbol)
    Index:    symbol, date
    """
    table = f"{region}_cash"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id          SERIAL PRIMARY KEY,
        date        DATE           NOT NULL,
        symbol      VARCHAR(20)    NOT NULL,
        open        NUMERIC(14,4),
        high        NUMERIC(14,4),
        low         NUMERIC(14,4),
        close       NUMERIC(14,4)  NOT NULL,
        volume      BIGINT,
        created_at  TIMESTAMP DEFAULT NOW(),

        CONSTRAINT uq_{table}_date_symbol
            UNIQUE (date, symbol)
    );

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol
        ON {table} (symbol);

    CREATE INDEX IF NOT EXISTS ix_{table}_date
        ON {table} (date);

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);
    """


# ═══════════════════════════════════════════════════════════════
#  OPTIONS TABLE DDL  (comprehensive — greeks, bid/ask, source)
# ═══════════════════════════════════════════════════════════════

OPTIONS_REGIONS = ["us", "hk"]  # Add "india" when ready

def _options_ddl(region: str) -> str:
    """
    Options snapshot table — one row per (date, symbol, expiry, strike, opt_type).

    Designed to hold data from both yfinance (no greeks) and IBKR (full greeks).
    Columns that a source doesn't provide are simply NULL.

    Columns:
        Identification:  date, symbol, expiry, strike, opt_type
        Market data:     bid, ask, last, volume, oi
        Volatility:      iv
        Greeks:          delta, gamma, theta, vega, rho
        Context:         underlying_price, dte
        Metadata:        source, created_at

    Unique:  (date, symbol, expiry, strike, opt_type)
    """
    table = f"{region}_options"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id                SERIAL PRIMARY KEY,

        -- ── Identification ────────────────────────────────────
        date              DATE           NOT NULL,
        symbol            VARCHAR(20)    NOT NULL,
        expiry            DATE           NOT NULL,
        strike            NUMERIC(14,4)  NOT NULL,
        opt_type          CHAR(1)        NOT NULL CHECK (opt_type IN ('C', 'P')),

        -- ── Market Data ───────────────────────────────────────
        bid               NUMERIC(14,4),
        ask               NUMERIC(14,4),
        last              NUMERIC(14,4),
        volume            INTEGER,
        oi                INTEGER,

        -- ── Implied Volatility ────────────────────────────────
        iv                NUMERIC(10,6),

        -- ── Greeks (NULL when source is yfinance) ─────────────
        delta             NUMERIC(10,6),
        gamma             NUMERIC(10,6),
        theta             NUMERIC(10,6),
        vega              NUMERIC(10,6),
        rho               NUMERIC(10,6),

        -- ── Context ──────────────────────────────────────────
        underlying_price  NUMERIC(14,4),
        dte               INTEGER,

        -- ── Metadata ─────────────────────────────────────────
        source            VARCHAR(20)    DEFAULT 'yfinance',
        created_at        TIMESTAMP      DEFAULT NOW(),

        -- ── Constraints ──────────────────────────────────────
        CONSTRAINT uq_{table}_snapshot
            UNIQUE (date, symbol, expiry, strike, opt_type)
    );

    -- Fast lookups by symbol
    CREATE INDEX IF NOT EXISTS ix_{table}_symbol
        ON {table} (symbol);

    -- Fast lookups by date (for daily snapshots)
    CREATE INDEX IF NOT EXISTS ix_{table}_date
        ON {table} (date);

    -- Composite: symbol + date (most common query pattern)
    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);

    -- Composite: symbol + expiry (for chain lookups)
    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_expiry
        ON {table} (symbol, expiry);

    -- Filtered: high-IV contracts
    CREATE INDEX IF NOT EXISTS ix_{table}_iv
        ON {table} (iv DESC)
        WHERE iv IS NOT NULL;

    -- Filtered: IBKR data with greeks
    CREATE INDEX IF NOT EXISTS ix_{table}_greeks
        ON {table} (symbol, expiry, strike)
        WHERE delta IS NOT NULL;
    """


# ═══════════════════════════════════════════════════════════════
#  AGGREGATE / DERIVED TABLE  (optional — for pipeline output)
# ═══════════════════════════════════════════════════════════════

def _signals_ddl() -> str:
    """
    Pipeline output: daily signals / scores per ticker.

    This table is OPTIONAL. The pipeline can write to parquet instead.
    Kept here so the DB can serve as a single reporting layer.
    """
    return """
    CREATE TABLE IF NOT EXISTS signals (
        id              SERIAL PRIMARY KEY,
        date            DATE           NOT NULL,
        symbol          VARCHAR(20)    NOT NULL,
        market          VARCHAR(10)    NOT NULL,

        -- ── Cash Metrics ──────────────────────────────────────
        close           NUMERIC(14,4),
        rsi_14          NUMERIC(8,4),
        macd            NUMERIC(14,6),
        macd_signal     NUMERIC(14,6),
        bb_pct          NUMERIC(8,4),
        atr_14          NUMERIC(14,4),
        adx_14          NUMERIC(8,4),
        vol_z_20        NUMERIC(8,4),

        -- ── Options Metrics ───────────────────────────────────
        iv_avg          NUMERIC(10,6),
        iv_skew         NUMERIC(10,6),
        put_call_ratio  NUMERIC(8,4),
        max_oi_strike   NUMERIC(14,4),
        total_oi        INTEGER,
        total_volume    INTEGER,

        -- ── Scores ────────────────────────────────────────────
        cash_score      NUMERIC(8,4),
        options_score   NUMERIC(8,4),
        combined_score  NUMERIC(8,4),
        regime          VARCHAR(20),
        recommendation  VARCHAR(50),

        -- ── Metadata ─────────────────────────────────────────
        created_at      TIMESTAMP DEFAULT NOW(),

        CONSTRAINT uq_signals_date_symbol
            UNIQUE (date, symbol)
    );

    CREATE INDEX IF NOT EXISTS ix_signals_date
        ON signals (date DESC);

    CREATE INDEX IF NOT EXISTS ix_signals_symbol
        ON signals (symbol);

    CREATE INDEX IF NOT EXISTS ix_signals_score
        ON signals (combined_score DESC)
        WHERE combined_score IS NOT NULL;
    """


# ═══════════════════════════════════════════════════════════════
#  REGISTRY — all tables managed by this schema
# ═══════════════════════════════════════════════════════════════

def all_table_names() -> list[str]:
    """Every table this schema manages, in creation order."""
    tables = [f"{r}_cash" for r in CASH_REGIONS]
    tables += [f"{r}_options" for r in OPTIONS_REGIONS]
    tables.append("signals")
    return tables


def all_ddl() -> list[str]:
    """All DDL statements in creation order."""
    stmts = [_cash_ddl(r) for r in CASH_REGIONS]
    stmts += [_options_ddl(r) for r in OPTIONS_REGIONS]
    stmts.append(_signals_ddl())
    return stmts


def options_table_names() -> list[str]:
    return [f"{r}_options" for r in OPTIONS_REGIONS]


# ═══════════════════════════════════════════════════════════════
#  OPERATIONS
# ═══════════════════════════════════════════════════════════════

def create_all():
    """Create all tables (idempotent — IF NOT EXISTS)."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for ddl in all_ddl():
            cur.execute(ddl)
        conn.commit()
        LOG.info(f"Created tables: {', '.join(all_table_names())}")
    except Exception as e:
        conn.rollback()
        LOG.error(f"Create failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def drop_all():
    """Drop ALL managed tables. Destructive!"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for table in reversed(all_table_names()):
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            LOG.info(f"  Dropped: {table}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        LOG.error(f"Drop failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def drop_options():
    """Drop only options tables. Preserves cash data."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for table in options_table_names():
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            LOG.info(f"  Dropped: {table}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        LOG.error(f"Drop failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def table_status() -> dict[str, dict]:
    """Check which tables exist and their row counts."""
    conn = get_conn()
    cur = conn.cursor()
    status = {}
    try:
        for table in all_table_names():
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                );
            """, (table,))
            exists = cur.fetchone()[0]

            rows = 0
            if exists:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                rows = cur.fetchone()[0]

            status[table] = {"exists": exists, "rows": rows}
    finally:
        cur.close()
        conn.close()

    return status


def print_status():
    """Pretty-print table status."""
    status = table_status()
    LOG.info("=" * 50)
    LOG.info(f"{'Table':<20s} {'Exists':<10s} {'Rows':>10s}")
    LOG.info("-" * 50)
    for table, info in status.items():
        marker = "✓" if info["exists"] else "✗"
        rows = f"{info['rows']:,}" if info["exists"] else "—"
        LOG.info(f"{table:<20s} {marker:<10s} {rows:>10s}")
    LOG.info("=" * 50)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Manage DB schema for options pipeline",
    )
    parser.add_argument(
        "action",
        choices=["create", "drop", "recreate", "status", "drop-options"],
        help="Action to perform",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for destructive operations (drop, recreate)",
    )
    args = parser.parse_args()

    if args.action == "create":
        create_all()

    elif args.action == "drop":
        if not args.yes:
            LOG.error("Pass --yes to confirm dropping ALL tables")
            return
        drop_all()

    elif args.action == "recreate":
        if not args.yes:
            LOG.error("Pass --yes to confirm drop + recreate ALL tables")
            return
        drop_all()
        create_all()

    elif args.action == "drop-options":
        if not args.yes:
            LOG.error("Pass --yes to confirm dropping options tables")
            return
        drop_options()
        LOG.info("Now run: python src/db/schema.py create")

    elif args.action == "status":
        print_status()


if __name__ == "__main__":
    main()
    


#####################################################################################
"""
src/db/db.py

Database connection utilities.
All table definitions live in schema.py — this file only provides
the connection engine and health check.
"""

import logging
from pathlib import Path
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

LOG = logging.getLogger(__name__)

# ── Connection string ─────────────────────────────────────────
DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Singleton engine (reused across the process)
_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Return a SQLAlchemy engine (singleton per process).

    Uses connection pooling — safe to call repeatedly.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False,
        )
    return _engine


def test_connection() -> bool:
    """Verify DB is reachable and responsive."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                LOG.info(
                    f"DB connection OK → "
                    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
                )
                return True
    except Exception as e:
        LOG.error(f"DB connection FAILED: {e}")
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_connection()
    
######################################################################   

"""
src/ingest_options.py
Fetch option chains and save to parquet + CSV.

Sources:
  yfinance  — US options (IV, bid/ask, volume, OI — no greeks)
  IBKR TWS  — US/HK options (full greeks, real-time)

Auto-selects:
  --period ≤ 5d AND market in (us, hk)  →  IBKR
  Otherwise                              →  yfinance

Usage:
    python src/ingest_options.py --market us                    # yfinance (backfill)
    python src/ingest_options.py --market us --source ibkr      # IBKR (recent + greeks)
    python src/ingest_options.py --market hk                    # IBKR only (auto)
    python src/ingest_options.py --market us --rungs 7
    python src/ingest_options.py --market us --consolidate      # CSVs → parquet
"""
import sys
from pathlib import Path
_SRC  = Path(__file__).resolve().parent        # .../src
_ROOT = _SRC.parent                            # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import argparse
import logging
import time
from datetime import date


import pandas as pd
import yfinance as yf

# ── Path setup ─────────────────────────────────────────────────
SRC  = Path(__file__).resolve().parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.universe import (
    get_us_only_etfs,
    get_all_single_names,
    get_hk_only,
    is_hk_ticker,
    is_india_ticker,
)
from common.expiry import next_monthly_expiries, match_expiry

try:
    from common.credentials import IBKR_PORT, IBKR_HOST
except ImportError:
    IBKR_PORT = 7497
    IBKR_HOST = "127.0.0.1"

try:
    from common.credentials import IBKR_CLIENT_ID_INGEST
except ImportError:
    IBKR_CLIENT_ID_INGEST = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)

DATA_DIR = ROOT / "data"
RUNGS    = 5
DELAY_YF = 1.5
DELAY_IBKR = 0.6   # IBKR pacing: ~60 requests per 10 min


# ═══════════════════════════════════════════════════════════════
#  SYMBOL LISTS
# ═══════════════════════════════════════════════════════════════

def get_symbols(market: str) -> list[str]:
    """Build symbol list for a market."""
    if market == "us":
        etfs = get_us_only_etfs()
        singles = [
            s for s in get_all_single_names()
            if not is_hk_ticker(s) and not is_india_ticker(s)
        ]
        return sorted(set(etfs + singles))
    elif market == "hk":
        return get_hk_only()
    else:
        LOG.warning(f"Options not supported for market: {market}")
        return []


def choose_source(market: str, force_source: str | None = None) -> str:
    """Auto-select data source."""
    if force_source:
        return force_source
    if market == "hk":
        return "ibkr"       # yfinance has no HK options
    return "yfinance"        # default for US


# ═══════════════════════════════════════════════════════════════
#  YFINANCE FETCH  (existing logic, cleaned up)
# ═══════════════════════════════════════════════════════════════

def current_price_yf(ticker: yf.Ticker) -> float | None:
    """Latest price from yfinance fast_info."""
    try:
        fi = ticker.fast_info
        return float(fi.get("lastPrice") or fi.get("regularMarketPrice"))
    except Exception:
        return None


def select_strikes(
    puts_df:  pd.DataFrame,
    calls_df: pd.DataFrame,
    price:    float,
    n:        int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pick n nearest OTM strikes on each side of price."""
    otm_p = (
        puts_df[puts_df["strike"] <= price]
        .sort_values("strike", ascending=False)
        .head(n)
    )
    otm_c = (
        calls_df[calls_df["strike"] >= price]
        .sort_values("strike", ascending=True)
        .head(n)
    )
    return otm_p, otm_c


def fetch_symbol_yfinance(
    symbol: str,
    n_rungs: int = 5,
) -> pd.DataFrame | None:
    """Fetch options chain for one symbol via yfinance."""
    ticker = yf.Ticker(symbol)
    price = current_price_yf(ticker)
    if price is None:
        LOG.warning(f"    {symbol}: no price, skipping")
        return None

    try:
        available = ticker.options
    except Exception as e:
        LOG.warning(f"    {symbol}: cannot read expiries – {e}")
        return None

    if not available:
        LOG.warning(f"    {symbol}: no options listed")
        return None

    targets = next_monthly_expiries(market="us", n=2)
    matched = match_expiry(targets, available)

    if not matched:
        LOG.warning(f"    {symbol}: no monthly expiry matched")
        return None

    LOG.info(
        f"    {symbol:<10s}  price={price:>10.2f}   "
        f"expiries={[str(d) for d, _ in matched]}"
    )

    today = date.today()
    rows = []

    for exp_date, exp_str in matched:
        try:
            chain = ticker.option_chain(exp_str)
        except Exception as e:
            LOG.warning(f"    {symbol} {exp_str}: chain error – {e}")
            continue

        otm_p, otm_c = select_strikes(
            chain.puts, chain.calls, price, n_rungs,
        )

        for _, r in otm_p.iterrows():
            rows.append({
                "date":             today.isoformat(),
                "symbol":           symbol,
                "expiry":           exp_date.isoformat(),
                "strike":           r["strike"],
                "opt_type":         "P",
                "bid":              r.get("bid"),
                "ask":              r.get("ask"),
                "last":             r.get("lastPrice"),
                "volume":           r.get("volume"),
                "oi":               r.get("openInterest"),
                "iv":               r.get("impliedVolatility"),
                # yfinance doesn't provide greeks
                "delta":            None,
                "gamma":            None,
                "theta":            None,
                "vega":             None,
                "rho":              None,
                "underlying_price": price,
                "source":           "yfinance",
            })

        for _, r in otm_c.iterrows():
            rows.append({
                "date":             today.isoformat(),
                "symbol":           symbol,
                "expiry":           exp_date.isoformat(),
                "strike":           r["strike"],
                "opt_type":         "C",
                "bid":              r.get("bid"),
                "ask":              r.get("ask"),
                "last":             r.get("lastPrice"),
                "volume":           r.get("volume"),
                "oi":               r.get("openInterest"),
                "iv":               r.get("impliedVolatility"),
                "delta":            None,
                "gamma":            None,
                "theta":            None,
                "vega":             None,
                "rho":              None,
                "underlying_price": price,
                "source":           "yfinance",
            })

        time.sleep(DELAY_YF)

    return pd.DataFrame(rows) if rows else None


# ═══════════════════════════════════════════════════════════════
#  IBKR FETCH
# ═══════════════════════════════════════════════════════════════

def _make_stock_contract(symbol: str, market: str):
    """Build an ib_insync Stock contract."""
    from ib_insync import Stock

    if market == "hk":
        ibkr_sym = symbol.replace(".HK", "").lstrip("0") or "0"
        return Stock(ibkr_sym, "SEHK", "HKD")
    else:
        ibkr_sym = symbol.split(".")[0]
        return Stock(ibkr_sym, "SMART", "USD")


def _make_option_contract(
    symbol: str,
    expiry_str: str,
    strike: float,
    right: str,
    market: str,
):
    """Build an ib_insync Option contract."""
    from ib_insync import Option

    if market == "hk":
        ibkr_sym = symbol.replace(".HK", "").lstrip("0") or "0"
        return Option(
            ibkr_sym, expiry_str, strike, right, "SEHK", currency="HKD",
        )
    else:
        ibkr_sym = symbol.split(".")[0]
        return Option(
            ibkr_sym, expiry_str, strike, right, "SMART", currency="USD",
        )


def fetch_symbol_ibkr(
    ib,
    symbol: str,
    market: str,
    n_rungs: int = 5,
) -> pd.DataFrame | None:
    """
    Fetch options chain for one symbol via IBKR TWS.

    Returns DataFrame with full greeks.
    """
    from ib_insync import util

    # ── Get underlying price ──────────────────────────────────
    stock = _make_stock_contract(symbol, market)
    qualified = ib.qualifyContracts(stock)
    if not qualified:
        LOG.warning(f"    {symbol}: could not qualify stock contract")
        return None

    stock = qualified[0]

    # Request market data snapshot for current price
    ib.reqMarketDataType(3)  # delayed-frozen as fallback
    ticker_data = ib.reqMktData(stock, "", False, False)
    ib.sleep(2)

    price = ticker_data.marketPrice()
    if price is None or price != price:  # NaN check
        price = ticker_data.close
    if price is None or price != price or price <= 0:
        LOG.warning(f"    {symbol}: no price from IBKR")
        ib.cancelMktData(stock)
        return None

    ib.cancelMktData(stock)

    # ── Get available expiries via security definitions ───────
    try:
        chains = ib.reqSecDefOptParams(
            stock.symbol, "", stock.secType, stock.conId,
        )
    except Exception as e:
        LOG.warning(f"    {symbol}: reqSecDefOptParams failed – {e}")
        return None

    if not chains:
        LOG.warning(f"    {symbol}: no option chains available")
        return None

    # Pick the chain with the most strikes (usually SMART for US)
    chain = max(chains, key=lambda c: len(c.strikes))
    available_expiries = sorted(chain.expirations)
    available_strikes = sorted(chain.strikes)

    if not available_expiries:
        LOG.warning(f"    {symbol}: no expiries in chain")
        return None

    # ── Match to next 2 monthly expiries ──────────────────────
    expiry_market = "us" if market != "hk" else "us"  # both use 3rd Friday
    targets = next_monthly_expiries(market=expiry_market, n=2)
    matched = match_expiry(
        targets,
        [_ibkr_expiry_to_iso(e) for e in available_expiries],
    )

    if not matched:
        LOG.warning(f"    {symbol}: no monthly expiry matched from IBKR")
        return None

    LOG.info(
        f"    {symbol:<10s}  price={price:>10.2f}   "
        f"expiries={[str(d) for d, _ in matched]}  "
        f"({len(available_strikes)} strikes available)"
    )

    today = date.today()
    rows = []

    for exp_date, exp_str in matched:
        # Convert back to IBKR format (YYYYMMDD)
        ibkr_expiry = exp_str.replace("-", "")

        # Select nearest OTM strikes
        puts_strikes = sorted(
            [s for s in available_strikes if s <= price],
            reverse=True,
        )[:n_rungs]

        calls_strikes = sorted(
            [s for s in available_strikes if s >= price],
        )[:n_rungs]

        for strike in puts_strikes:
            row = _fetch_single_option(
                ib, symbol, market, ibkr_expiry, strike,
                "P", price, today, exp_date,
            )
            if row:
                rows.append(row)
            ib.sleep(DELAY_IBKR)

        for strike in calls_strikes:
            row = _fetch_single_option(
                ib, symbol, market, ibkr_expiry, strike,
                "C", price, today, exp_date,
            )
            if row:
                rows.append(row)
            ib.sleep(DELAY_IBKR)

    return pd.DataFrame(rows) if rows else None


def _fetch_single_option(
    ib, symbol, market, ibkr_expiry, strike, right,
    underlying_price, today, exp_date,
) -> dict | None:
    """Fetch data for a single option contract from IBKR."""
    try:
        contract = _make_option_contract(
            symbol, ibkr_expiry, strike, right, market,
        )
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            return None

        contract = qualified[0]

        # Request market data
        ticker = ib.reqMktData(contract, "106", False, False)
        ib.sleep(1.5)

        row = {
            "date":             today.isoformat(),
            "symbol":           symbol,
            "expiry":           exp_date.isoformat(),
            "strike":           strike,
            "opt_type":         right,
            "bid":              _safe_float(ticker.bid),
            "ask":              _safe_float(ticker.ask),
            "last":             _safe_float(ticker.last),
            "volume":           _safe_int(ticker.volume),
            "oi":               None,  # populated from
            "iv":               _safe_float(ticker.modelGreeks.impliedVol
                                            if ticker.modelGreeks else None),
            "delta":            _safe_float(ticker.modelGreeks.delta
                                            if ticker.modelGreeks else None),
            "gamma":            _safe_float(ticker.modelGreeks.gamma
                                            if ticker.modelGreeks else None),
            "theta":            _safe_float(ticker.modelGreeks.theta
                                            if ticker.modelGreeks else None),
            "vega":             _safe_float(ticker.modelGreeks.vega
                                            if ticker.modelGreeks else None),
            "rho":              _safe_float(ticker.modelGreeks.rho
                                            if ticker.modelGreeks else None),
            "underlying_price": underlying_price,
            "source":           "ibkr",
        }

        ib.cancelMktData(contract)
        return row

    except Exception as e:
        LOG.debug(
            f"    {symbol} {right}{strike} {ibkr_expiry}: {e}"
        )
        return None


def _ibkr_expiry_to_iso(expiry: str) -> str:
    """Convert IBKR '20250620' format to '2025-06-20'."""
    if len(expiry) == 8 and expiry.isdigit():
        return f"{expiry[:4]}-{expiry[4:6]}-{expiry[6:]}"
    return expiry


def _safe_float(val) -> float | None:
    """Safely convert to float, handling NaN and None."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    """Safely convert to int."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:
            return None
        return int(f)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════
#  IBKR SESSION MANAGER
# ═══════════════════════════════════════════════════════════════

def run_ibkr(symbols: list[str], market: str, n_rungs: int):
    """Fetch all symbols via IBKR with connection management."""
    try:
        from ib_insync import IB
    except ImportError:
        LOG.error("ib_insync not installed. Run: pip install ib_insync")
        return

    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID_INGEST)
        LOG.info(f"[IBKR] Connected to TWS at {IBKR_HOST}:{IBKR_PORT}")
    except Exception as e:
        LOG.error(f"[IBKR] Cannot connect to TWS: {e}")
        LOG.warning("[IBKR] Is TWS/Gateway running? Falling back to yfinance.")
        if market == "hk":
            LOG.error("HK options require IBKR — no fallback available.")
            return
        LOG.info("Falling back to yfinance for US options (no greeks)")
        run_yfinance(symbols, n_rungs)
        return

    out_dir = DATA_DIR / "options" / market
    out_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    errors = 0
    skips = 0

    try:
        for i, sym in enumerate(symbols, 1):
            LOG.info(f"[{i}/{len(symbols)}]  {sym}")

            try:
                df = fetch_symbol_ibkr(ib, sym, market, n_rungs)
            except Exception as e:
                LOG.error(f"    {sym}: unexpected error – {e}")
                errors += 1
                continue

            if df is None or df.empty:
                skips += 1
                continue

            # Save per-ticker CSV (append-safe)
            fname = sym.replace(".", "_").replace("/", "_") + ".csv"
            path = out_dir / fname
            _append_save(df, path)

            day_ct = int((df["date"] == date.today().isoformat()).sum())
            total += day_ct
            LOG.info(f"         → {day_ct} contracts ({path.name})")

    finally:
        ib.disconnect()
        LOG.info("[IBKR] Disconnected")

    LOG.info(
        f"DONE {market.upper()} (IBKR): "
        f"{total} contracts | {skips} skipped | {errors} errors"
    )


# ═══════════════════════════════════════════════════════════════
#  YFINANCE SESSION RUNNER
# ═══════════════════════════════════════════════════════════════

def run_yfinance(symbols: list[str], n_rungs: int):
    """Fetch all symbols via yfinance."""
    out_dir = DATA_DIR / "options" / "us"
    out_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    errors = 0
    skips = 0

    for i, sym in enumerate(symbols, 1):
        LOG.info(f"[{i}/{len(symbols)}]  {sym}")

        try:
            df = fetch_symbol_yfinance(sym, n_rungs)
        except Exception as e:
            LOG.error(f"    {sym}: unexpected error – {e}")
            errors += 1
            continue

        if df is None or df.empty:
            skips += 1
            continue

        fname = sym.replace(".", "_").replace("/", "_") + ".csv"
        path = out_dir / fname
        _append_save(df, path)

        day_ct = int((df["date"] == date.today().isoformat()).sum())
        total += day_ct
        LOG.info(f"         → {day_ct} contracts ({path.name})")

        time.sleep(DELAY_YF)

    LOG.info(
        f"DONE US (yfinance): "
        f"{total} contracts | {skips} skipped | {errors} errors"
    )


# ═══════════════════════════════════════════════════════════════
#  CONSOLIDATION — CSVs → Parquet
# ═══════════════════════════════════════════════════════════════

def consolidate(market: str = "us") -> pd.DataFrame:
    """
    Read all per-ticker option CSVs and combine into one parquet.

    This creates the file that load_db.py expects for DB loading.
    """
    csv_dir = DATA_DIR / "options" / market
    if not csv_dir.exists():
        LOG.warning(f"No options directory: {csv_dir}")
        return pd.DataFrame()

    frames = []
    for csv_file in sorted(csv_dir.glob("*.csv")):
        try:
            df = pd.read_csv(csv_file)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            LOG.warning(f"Failed to read {csv_file.name}: {e}")

    if not frames:
        LOG.warning(f"No CSV files found in {csv_dir}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    out_path = DATA_DIR / f"{market}_options.parquet"
    combined.to_parquet(out_path, index=False)

    LOG.info(
        f"Consolidated {len(frames)} CSVs → {out_path.name}  "
        f"({len(combined):,} rows, "
        f"{combined['symbol'].nunique()} symbols)"
    )
    return combined


# ═══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════

def _append_save(df: pd.DataFrame, path: Path):
    """
    Save DataFrame to CSV, appending to existing data.
    Replaces today's rows if re-running.
    """
    if path.exists():
        prev = pd.read_csv(path, dtype={"date": str})
        prev = prev[prev["date"] != date.today().isoformat()]
        df = pd.concat([prev, df], ignore_index=True)
    df.to_csv(path, index=False)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Fetch options chains (yfinance + IBKR)",
    )
    parser.add_argument(
        "--market",
        choices=["us", "hk"],
        default="us",
        help="Market to fetch (default: us)",
    )
    parser.add_argument(
        "--source",
        choices=["yfinance", "ibkr"],
        default=None,
        help="Force data source (default: auto)",
    )
    parser.add_argument(
        "--rungs",
        type=int,
        default=RUNGS,
        help=f"OTM strikes per side (default: {RUNGS})",
    )
    parser.add_argument(
        "--consolidate",
        action="store_true",
        help="Consolidate per-ticker CSVs into one parquet",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=None,
        help="Fetch specific tickers only",
    )
    args = parser.parse_args()

    # ── Consolidate mode ──────────────────────────────────────
    if args.consolidate:
        consolidate(args.market)
        return

    # ── Fetch mode ────────────────────────────────────────────
    if args.tickers:
        symbols = [t.upper() for t in args.tickers]
    else:
        symbols = get_symbols(args.market)

    if not symbols:
        LOG.error(f"No symbols for market: {args.market}")
        return

    source = choose_source(args.market, args.source)

    LOG.info("=" * 60)
    LOG.info(
        f"OPTIONS — {args.market.upper()} | "
        f"{len(symbols)} symbols | "
        f"{args.rungs} rungs/side | "
        f"source: {source}"
    )
    LOG.info("=" * 60)

    if source == "ibkr":
        run_ibkr(symbols, args.market, args.rungs)
    else:
        run_yfinance(symbols, args.rungs)

    LOG.info("=" * 60)
    LOG.info(f"Output → {DATA_DIR / 'options' / args.market}")
    LOG.info("=" * 60)


if __name__ == "__main__":
    main()


#############################################################

"""
src/ingest_cash.py – Download OHLCV universe data (yfinance + IBKR).

Auto-selects data source:
  Period ≤ 5 days   → IBKR TWS  (must be running)
  Period > 5 days   → yfinance  (bulk backfill)

Override with --source yfinance | ibkr

Outputs:
  data/{market}_cash.parquet   — per-market files (for load_db.py)
  data/universe_ohlcv.parquet  — combined file   (for loader.py)

Usage:
    python src/ingest_cash.py --market all --period 2y
    python src/ingest_cash.py --market all --period 3d
    python src/ingest_cash.py --market us --period 5d --source ibkr
    python src/ingest_cash.py --full --backfill
"""

import sys
from pathlib import Path
_SRC  = Path(__file__).resolve().parent        # .../src
_ROOT = _SRC.parent                            # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import math
import logging
import argparse
import re
from datetime import datetime, date, timedelta

# ── Ensure src/ is on the Python path ─────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
import yfinance as yf

from common.credentials import IBKR_PORT
from common.universe import (
    get_us_only_etfs,
    get_all_single_names,
    get_hk_only,
    get_india_only,
    is_hk_ticker,
    is_india_ticker,
)

try:
    from common.credentials import IBKR_HOST
except ImportError:
    IBKR_HOST = "127.0.0.1"

try:
    from common.credentials import IBKR_CLIENT_ID_INGEST
except ImportError:
    IBKR_CLIENT_ID_INGEST = 10

# ── Paths ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)

# ── Period → approximate calendar days ─────────────────────────
PERIOD_DAYS_MAP = {
    "1d": 1, "2d": 2, "3d": 3, "4d": 4, "5d": 5,
    "1w": 7, "2w": 14, "1mo": 30, "3mo": 90, "6mo": 180,
    "1y": 365, "2y": 730, "5y": 1825, "10y": 3650, "max": 9999,
}

IBKR_THRESHOLD_DAYS = 5
IBKR_SUPPORTED_MARKETS = {"us", "hk"}


# ====================================================================
#  Symbol lists from universe.py
# ====================================================================

def get_symbols_for_market(market: str) -> list[str]:
    """Build symbol list for a market using universe.py helpers."""
    if market == "us":
        # US ETFs + US-listed single names (exclude HK and India tickers)
        etfs = get_us_only_etfs()
        singles = [
            s for s in get_all_single_names()
            if not is_hk_ticker(s) and not is_india_ticker(s)
        ]
        combined = list(dict.fromkeys(etfs + singles))  # dedup, preserve order
        return combined

    elif market == "hk":
        return get_hk_only()

    elif market == "india":
        return get_india_only()

    else:
        logger.warning(f"Unknown market: {market}")
        return []


# ====================================================================
#  Helpers
# ====================================================================

def period_to_days(period: str) -> int:
    """Convert a period string like '2y', '5d', '3mo' to approx calendar days."""
    period = period.lower().strip()
    if period in PERIOD_DAYS_MAP:
        return PERIOD_DAYS_MAP[period]

    m = re.match(r"^(\d+)\s*(d|w|mo|m|y)$", period)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "d":
            return n
        if unit == "w":
            return n * 7
        if unit in ("mo", "m"):
            return n * 30
        if unit == "y":
            return n * 365

    logger.warning(f"Cannot parse period '{period}', defaulting to 9999 days (yfinance)")
    return 9999


def period_to_ibkr_duration(period: str) -> str:
    """Convert period string to IBKR durationStr format like '5 D'."""
    days = period_to_days(period)
    if days <= 7:
        return f"{days} D"
    elif days <= 60:
        weeks = max(1, days // 7)
        return f"{weeks} W"
    elif days <= 365:
        months = max(1, days // 30)
        return f"{months} M"
    else:
        years = max(1, days // 365)
        return f"{years} Y"


def choose_source(period: str, market: str, force_source: str = None) -> str:
    """
    Decide whether to use 'yfinance' or 'ibkr'.
      1. If force_source is set, use that.
      2. If period ≤ 5 days AND market is IBKR-supported → ibkr
      3. Otherwise → yfinance
    """
    if force_source:
        return force_source

    days = period_to_days(period)
    if days <= IBKR_THRESHOLD_DAYS and market in IBKR_SUPPORTED_MARKETS:
        return "ibkr"

    return "yfinance"


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise column names to lowercase for load_db.py compatibility.

    yfinance returns Title Case (Date, Open, High, ...),
    IBKR fetch already renames to Title Case.
    load_db.py expects lowercase (date, symbol, open, ...).
    """
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Standardise common variations
    renames = {
        "adj close": "adj_close",
    }
    df.rename(columns={k: v for k, v in renames.items() if k in df.columns},
              inplace=True)

    return df


# ====================================================================
#  IBKR contract helpers
# ====================================================================

def clean_hk_symbol(symbol: str) -> str:
    """'0005.HK' → '5', '0700.HK' → '700'"""
    sym = symbol.replace(".HK", "").lstrip("0")
    return sym if sym else "0"


def make_ibkr_contract(symbol: str, market: str):
    """Build an ib_insync Stock contract from a yfinance-style symbol."""
    from ib_insync import Stock

    if market == "hk":
        ibkr_sym = clean_hk_symbol(symbol)
        return Stock(ibkr_sym, "SEHK", "HKD")
    elif market == "india":
        ibkr_sym = symbol.replace(".NS", "").replace(".BO", "")
        return Stock(ibkr_sym, "NSE", "INR")
    else:
        ibkr_sym = symbol.split(".")[0]
        return Stock(ibkr_sym, "SMART", "USD")


# ====================================================================
#  yfinance fetch
# ====================================================================

def fetch_yfinance(symbols: list[str], period: str) -> pd.DataFrame:
    """Bulk download OHLCV via yfinance."""
    logger.info(f"[yfinance] Downloading {len(symbols)} symbols, period={period}")

    if not symbols:
        return pd.DataFrame()

    df = yf.download(
        tickers=symbols,
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
    )

    if df.empty:
        logger.warning("[yfinance] Empty result")
        return pd.DataFrame()

    # ── Reshape multi-ticker result ────────────────────────────
    records = []

    if len(symbols) == 1:
        sym = symbols[0]
        tmp = df.copy()
        tmp = tmp.reset_index()
        tmp["symbol"] = sym
        records.append(tmp)
    else:
        for sym in symbols:
            try:
                tmp = df[sym].copy()
                tmp = tmp.dropna(how="all")
                if tmp.empty:
                    continue
                tmp = tmp.reset_index()
                tmp["symbol"] = sym
                records.append(tmp)
            except KeyError:
                logger.warning(f"[yfinance] No data for {sym}")
                continue

    if not records:
        return pd.DataFrame()

    result = pd.concat(records, ignore_index=True)

    # Standardise date column name (yfinance uses "Date" or "date")
    col_map = {}
    for c in result.columns:
        if c.lower() == "date":
            col_map[c] = "Date"
    result.rename(columns=col_map, inplace=True)

    logger.info(f"[yfinance] Got {len(result):,} rows for {result['symbol'].nunique()} symbols")
    return result


# ====================================================================
#  IBKR fetch
# ====================================================================

def fetch_ibkr(symbols: list[str], period: str, market: str) -> pd.DataFrame:
    """Fetch historical daily bars from IBKR TWS."""
    try:
        from ib_insync import IB, util
    except ImportError:
        logger.error("ib_insync not installed. Run: pip install ib_insync")
        return pd.DataFrame()

    duration = period_to_ibkr_duration(period)
    logger.info(
        f"[IBKR] Fetching {len(symbols)} symbols, "
        f"duration={duration}, market={market}"
    )

    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID_INGEST)
        ib.reqMarketDataType(1)
        logger.info(f"[IBKR] Connected to TWS at {IBKR_HOST}:{IBKR_PORT}")
    except Exception as e:
        logger.error(f"[IBKR] Cannot connect to TWS: {e}")
        logger.warning("[IBKR] Falling back to yfinance")
        return fetch_yfinance(symbols, period)

    all_records = []

    try:
        for idx, sym in enumerate(symbols, 1):
            logger.info(f"[IBKR] [{idx}/{len(symbols)}] {sym}")

            contract = make_ibkr_contract(sym, market)
            qualified = ib.qualifyContracts(contract)

            if not qualified:
                logger.warning(f"[IBKR]   Could not qualify {sym}, skipping")
                continue

            contract = qualified[0]

            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime="",
                    durationStr=duration,
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1,
                )
            except Exception as e:
                logger.warning(f"[IBKR]   Error fetching {sym}: {e}")
                continue

            if not bars:
                logger.warning(f"[IBKR]   No bars for {sym}")
                continue

            df = util.df(bars)
            df["symbol"] = sym

            df.rename(columns={
                "date":     "Date",
                "open":     "Open",
                "high":     "High",
                "low":      "Low",
                "close":    "Close",
                "volume":   "Volume",
                "average":  "VWAP",
                "barCount": "Trades",
            }, inplace=True)

            df["Adj Close"] = df["Close"]

            all_records.append(df)
            logger.info(f"[IBKR]   {sym}: {len(df)} bars")

            # Pacing: IBKR allows ~60 hist data requests per 10 min
            ib.sleep(0.5)

    finally:
        ib.disconnect()
        logger.info("[IBKR] Disconnected")

    if not all_records:
        return pd.DataFrame()

    result = pd.concat(all_records, ignore_index=True)
    logger.info(f"[IBKR] Got {len(result):,} rows for {result['symbol'].nunique()} symbols")
    return result


# ====================================================================
#  Orchestration
# ====================================================================

def fetch_full_universe(markets: list[str], period: str, force_source: str = None):
    """
    Download OHLCV for all symbols across requested markets.
    Auto-selects yfinance vs IBKR based on period length.

    Saves:
      data/{market}_cash.parquet   — per-market (for load_db.py)
      data/universe_ohlcv.parquet  — combined   (for loader.py)
    """
    DATA_DIR.mkdir(exist_ok=True)
    all_dfs = []

    for market in markets:
        symbols = get_symbols_for_market(market)
        if not symbols:
            logger.warning(f"No symbols for market: {market}")
            continue

        source = choose_source(period, market, force_source)
        logger.info(
            f"Market: {market.upper()} | "
            f"{len(symbols)} symbols | "
            f"period: {period} | "
            f"source: {source}"
        )

        if source == "ibkr":
            df = fetch_ibkr(symbols, period, market)
        else:
            df = fetch_yfinance(symbols, period)

        if df is None or df.empty:
            logger.warning(f"No data returned for market: {market}")
            continue

        # ── Normalise columns to lowercase ─────────────────────
        df = normalise_columns(df)

        # ── Save per-market parquet (what load_db.py expects) ──
        market_path = DATA_DIR / f"{market}_cash.parquet"
        df.to_parquet(market_path, index=False)
        size_mb = market_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Saved → {market_path}  "
            f"({size_mb:.1f} MB, {len(df):,} rows, "
            f"{df['symbol'].nunique()} symbols)"
        )

        all_dfs.append(df)

    if not all_dfs:
        logger.warning("No data collected across any market")
        return

    # ── Save combined file (for loader.py parquet reads) ───────
    combined = pd.concat(all_dfs, ignore_index=True)
    combined_path = DATA_DIR / "universe_ohlcv.parquet"
    combined.to_parquet(combined_path, index=False)
    size_mb = combined_path.stat().st_size / (1024 * 1024)
    logger.info(
        f"Saved → {combined_path}  "
        f"({size_mb:.1f} MB, {len(combined):,} rows, "
        f"{combined['symbol'].nunique()} symbols — combined)"
    )


# ====================================================================
#  CLI
# ====================================================================

def main():
    parser = argparse.ArgumentParser(description="Ingest OHLCV data (yfinance + IBKR)")
    parser.add_argument(
        "--market",
        choices=["us", "hk", "india", "all"],
        default="all",
        help="Which market(s) to download (default: all)",
    )
    parser.add_argument(
        "--period",
        default="2y",
        help="Period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, max (default: 2y)",
    )
    parser.add_argument(
        "--source",
        choices=["yfinance", "ibkr"],
        default=None,
        help="Force data source (default: auto based on period)",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Alias for --market all",
    )
    parser.add_argument(
        "--backfill", action="store_true",
        help="Alias for --period max",
    )
    args = parser.parse_args()

    if args.full:
        args.market = "all"
    if args.backfill:
        args.period = "max"

    if args.market == "all":
        markets = ["us", "hk", "india"]
    else:
        markets = [args.market]

    # Show auto-selection logic
    days = period_to_days(args.period)
    logger.info(
        f"Period={args.period} ({days} days) → "
        f"auto threshold: ≤{IBKR_THRESHOLD_DAYS}d uses IBKR"
        + (f" [OVERRIDDEN → {args.source}]" if args.source else "")
    )

    # Show symbol counts
    for mkt in markets:
        syms = get_symbols_for_market(mkt)
        src = choose_source(args.period, mkt, args.source)
        logger.info(f"  {mkt.upper():6s}: {len(syms):>4d} symbols → {src}")

    fetch_full_universe(
        markets=markets,
        period=args.period,
        force_source=args.source,
    )

    logger.info("Done")


if __name__ == "__main__":
    main()
    



"""
src/db/load_db.py

Load parquet / CSV files into PostgreSQL tables defined in schema.py.

Usage:
    python src/db/load_db.py --market all --type all
    python src/db/load_db.py --market us  --type cash
    python src/db/load_db.py --market us  --type options
    python src/db/load_db.py --market hk  --type options
    python src/db/load_db.py --status
"""
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import argparse
import logging
import pandas as pd
from sqlalchemy import text

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from db.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)

DATA_DIR = ROOT / "data"


# ═══════════════════════════════════════════════════════════════
#  COLUMN MAPS  — parquet/CSV column → DB column
# ═══════════════════════════════════════════════════════════════

CASH_COLUMNS = [
    "date", "symbol", "open", "high", "low", "close", "volume",
]

OPTIONS_COLUMNS = [
    "date", "symbol", "expiry", "strike", "opt_type",
    "bid", "ask", "last", "volume", "oi",
    "iv",
    "delta", "gamma", "theta", "vega", "rho",
    "underlying_price", "dte",
    "source",
]


# ═══════════════════════════════════════════════════════════════
#  CASH LOADING
# ═══════════════════════════════════════════════════════════════

def load_cash(market: str) -> int:
    """
    Load cash OHLCV data into {market}_cash table.

    Reads from: data/{market}_cash.parquet  (or .csv fallback)
    Upsert:     ON CONFLICT (date, symbol) DO UPDATE
    """
    table = f"{market}_cash"
    df = _read_data_file(market, "cash")

    if df is None or df.empty:
        LOG.warning(f"No data found for {table}")
        return 0

    # Ensure required columns exist
    for col in ["date", "symbol", "close"]:
        if col not in df.columns:
            LOG.error(f"{table}: missing required column '{col}'")
            return 0

    # Keep only known columns, fill missing optional ones with None
    for col in CASH_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[CASH_COLUMNS].copy()

    # Clean
    df = df.dropna(subset=["date", "symbol", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.drop_duplicates(subset=["date", "symbol"], keep="last")

    if df.empty:
        LOG.warning(f"{table}: no valid rows after cleaning")
        return 0

    # Upsert
    engine = get_engine()
    upsert_sql = f"""
        INSERT INTO {table} (date, symbol, open, high, low, close, volume)
        VALUES (:date, :symbol, :open, :high, :low, :close, :volume)
        ON CONFLICT (date, symbol) DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """

    rows_loaded = _batch_upsert(engine, upsert_sql, df, table)
    return rows_loaded


# ═══════════════════════════════════════════════════════════════
#  OPTIONS LOADING
# ═══════════════════════════════════════════════════════════════

def load_options(market: str) -> int:
    """
    Load options snapshot data into {market}_options table.

    Reads from: data/{market}_options.parquet  (or .csv fallback)
    Upsert:     ON CONFLICT (date, symbol, expiry, strike, opt_type) DO UPDATE

    Handles both yfinance data (greeks are NULL) and IBKR data (full greeks).
    When IBKR data overwrites yfinance data for the same contract, greeks
    get populated.
    """
    table = f"{market}_options"
    df = _read_data_file(market, "options")

    if df is None or df.empty:
        LOG.warning(f"No data found for {table}")
        return 0

    # Ensure required columns exist
    for col in ["date", "symbol", "expiry", "strike", "opt_type"]:
        if col not in df.columns:
            LOG.error(f"{table}: missing required column '{col}'")
            return 0

    # Add missing optional columns as None
    for col in OPTIONS_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Compute DTE if not present
    if df["dte"].isna().all():
        try:
            df["dte"] = (
                pd.to_datetime(df["expiry"]) - pd.to_datetime(df["date"])
            ).dt.days
        except Exception:
            pass

    df = df[OPTIONS_COLUMNS].copy()

    # Clean
    df = df.dropna(subset=["date", "symbol", "expiry", "strike", "opt_type"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    df["opt_type"] = df["opt_type"].str.upper().str.strip()
    df = df[df["opt_type"].isin(["C", "P"])]
    df = df.drop_duplicates(
        subset=["date", "symbol", "expiry", "strike", "opt_type"],
        keep="last",
    )

    if df.empty:
        LOG.warning(f"{table}: no valid rows after cleaning")
        return 0

    # Upsert — IBKR data (with greeks) overwrites yfinance data (without)
    engine = get_engine()
    upsert_sql = f"""
        INSERT INTO {table} (
            date, symbol, expiry, strike, opt_type,
            bid, ask, last, volume, oi,
            iv,
            delta, gamma, theta, vega, rho,
            underlying_price, dte,
            source
        )
        VALUES (
            :date, :symbol, :expiry, :strike, :opt_type,
            :bid, :ask, :last, :volume, :oi,
            :iv,
            :delta, :gamma, :theta, :vega, :rho,
            :underlying_price, :dte,
            :source
        )
        ON CONFLICT (date, symbol, expiry, strike, opt_type) DO UPDATE SET
            bid              = COALESCE(EXCLUDED.bid,              {table}.bid),
            ask              = COALESCE(EXCLUDED.ask,              {table}.ask),
            last             = COALESCE(EXCLUDED.last,             {table}.last),
            volume           = COALESCE(EXCLUDED.volume,           {table}.volume),
            oi               = COALESCE(EXCLUDED.oi,               {table}.oi),
            iv               = COALESCE(EXCLUDED.iv,               {table}.iv),
            delta            = COALESCE(EXCLUDED.delta,            {table}.delta),
            gamma            = COALESCE(EXCLUDED.gamma,            {table}.gamma),
            theta            = COALESCE(EXCLUDED.theta,            {table}.theta),
            vega             = COALESCE(EXCLUDED.vega,             {table}.vega),
            rho              = COALESCE(EXCLUDED.rho,              {table}.rho),
            underlying_price = COALESCE(EXCLUDED.underlying_price, {table}.underlying_price),
            dte              = COALESCE(EXCLUDED.dte,              {table}.dte),
            source           = COALESCE(EXCLUDED.source,           {table}.source);
    """

    rows_loaded = _batch_upsert(engine, upsert_sql, df, table)
    return rows_loaded


# ═══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════

def _read_data_file(market: str, dtype: str) -> pd.DataFrame | None:
    """
    Read data from parquet (preferred) or CSV fallback.

    Looks for:  data/{market}_{dtype}.parquet
                data/{market}_{dtype}.csv
    """
    parquet_path = DATA_DIR / f"{market}_{dtype}.parquet"
    csv_path = DATA_DIR / f"{market}_{dtype}.csv"

    if parquet_path.exists():
        LOG.info(f"Reading {parquet_path.name}")
        return pd.read_parquet(parquet_path)
    elif csv_path.exists():
        LOG.info(f"Reading {csv_path.name} (parquet not found)")
        return pd.read_csv(csv_path)
    else:
        LOG.warning(
            f"No data file found: {parquet_path.name} or {csv_path.name}"
        )
        return None


def _batch_upsert(
    engine,
    sql: str,
    df: pd.DataFrame,
    table: str,
    batch_size: int = 1000,
) -> int:
    """
    Execute upsert in batches for memory efficiency.

    Converts NaN/NaT to None for proper NULL handling in SQL.
    """
    # Replace NaN with None (psycopg2 sends NULL)
    records = df.where(df.notna(), None).to_dict("records")

    total = 0
    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(text(sql), batch)
            total += len(batch)

            if total % 5000 == 0 or total == len(records):
                LOG.info(f"  {table}: {total:,} / {len(records):,} rows")

    LOG.info(f"  {table}: loaded {total:,} rows total")
    return total


def load_status():
    """Show row counts for all tables."""
    engine = get_engine()
    from db.schema import all_table_names

    LOG.info("=" * 50)
    LOG.info(f"{'Table':<20s} {'Rows':>10s}")
    LOG.info("-" * 50)

    with engine.connect() as conn:
        for table in all_table_names():
            try:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).scalar()
                LOG.info(f"{table:<20s} {result:>10,}")
            except Exception:
                LOG.info(f"{table:<20s} {'(missing)':>10s}")

    LOG.info("=" * 50)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

MARKET_CHOICES = ["us", "hk", "india", "others", "all"]
TYPE_CHOICES = ["cash", "options", "all"]

def main():
    parser = argparse.ArgumentParser(
        description="Load parquet/CSV data into PostgreSQL",
    )
    parser.add_argument(
        "--market",
        choices=MARKET_CHOICES,
        default="all",
    )
    parser.add_argument(
        "--type",
        choices=TYPE_CHOICES,
        default="all",
        dest="dtype",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show table row counts",
    )
    args = parser.parse_args()

    if args.status:
        load_status()
        return

    # ── Determine what to load ────────────────────────────────
    from db.schema import CASH_REGIONS, OPTIONS_REGIONS

    if args.market == "all":
        cash_markets = CASH_REGIONS
        opt_markets = OPTIONS_REGIONS
    else:
        cash_markets = [args.market] if args.market in CASH_REGIONS else []
        opt_markets = [args.market] if args.market in OPTIONS_REGIONS else []

    total = 0

    # ── Cash ──────────────────────────────────────────────────
    if args.dtype in ("cash", "all"):
        for m in cash_markets:
            try:
                n = load_cash(m)
                total += n
            except Exception as e:
                LOG.error(f"Failed loading {m}_cash: {e}")

    # ── Options ───────────────────────────────────────────────
    if args.dtype in ("options", "all"):
        for m in opt_markets:
            try:
                n = load_options(m)
                total += n
            except Exception as e:
                LOG.error(f"Failed loading {m}_options: {e}")

    LOG.info(f"DONE — {total:,} total rows loaded")


if __name__ == "__main__":
    main()
    


########################################################################################
"""
src/db/db.py

Database connection utilities.
All table definitions live in schema.py — this file only provides
the connection engine and health check.
"""
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

LOG = logging.getLogger(__name__)

# ── Connection string ─────────────────────────────────────────
DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Singleton engine (reused across the process)
_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Return a SQLAlchemy engine (singleton per process).

    Uses connection pooling — safe to call repeatedly.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False,
        )
    return _engine


def test_connection() -> bool:
    """Verify DB is reachable and responsive."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                LOG.info(
                    f"DB connection OK → "
                    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
                )
                return True
    except Exception as e:
        LOG.error(f"DB connection FAILED: {e}")
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_connection()
    
    
################################################################################
"""
STRATEGY:
-----------------------------
"""
"""
strategy/portfolio.py
---------------------
Multi-stock portfolio construction.

Takes signal DataFrames from multiple tickers (output of
``generate_signals``), ranks candidates cross-sectionally,
enforces concentration limits, and outputs a target portfolio.

Pipeline
────────
  {ticker: signaled_df}  for N stocks
       ↓
  extract_snapshots()     — latest-day data per ticker
       ↓
  filter_candidates()     — keep sig_confirmed == 1
       ↓
  rank_candidates()       — sort by score + incumbent bonus
       ↓
  select_positions()      — top N, enforce sector caps
       ↓
  compute_weights()       — normalise to target allocation
       ↓
  build_portfolio()       — master orchestrator
       ↓
  compute_rebalance()     — diff vs current → trade list

Breadth Integration
───────────────────
  When breadth data is passed to ``build_portfolio()``, the
  target invested percentage is scaled by the breadth regime:
    strong  → 100 % of target_invested_pct
    neutral →  80 %
    weak    →  50 %

  This is a portfolio-level risk dial that sits on top of the
  per-stock breadth gate already applied in signals.py.

Output
──────
  dict with target_weights, holdings DataFrame, sector exposure,
  rebalance trades, and metadata.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import (
    PORTFOLIO_PARAMS,
    SIGNAL_PARAMS,
    BREADTH_PORTFOLIO,
)


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _pp(key: str):
    """Fetch from PORTFOLIO_PARAMS."""
    return PORTFOLIO_PARAMS[key]


def _bpp(key: str):
    """Fetch from BREADTH_PORTFOLIO."""
    return BREADTH_PORTFOLIO[key]


def _get_score(row) -> float:
    """Pick best available score from a snapshot row."""
    for col in ("score_adjusted", "score_composite"):
        val = row.get(col, np.nan)
        if pd.notna(val):
            return float(val)
    return 0.0


# ═══════════════════════════════════════════════════════════════
#  BREADTH EXPOSURE ADJUSTMENT
# ═══════════════════════════════════════════════════════════════

def _breadth_exposure_multiplier(
    breadth: pd.DataFrame | None,
) -> tuple[float, str]:
    """
    Determine portfolio exposure multiplier from latest breadth.

    Returns
    -------
    multiplier : float — scale factor for target_invested_pct
    regime     : str   — breadth regime label
    """
    if breadth is None or breadth.empty:
        return 1.0, "unknown"

    if "breadth_regime" not in breadth.columns:
        return 1.0, "unknown"

    regime = str(breadth["breadth_regime"].iloc[-1])

    mapping = {
        "strong":  _bpp("strong_exposure"),
        "neutral": _bpp("neutral_exposure"),
        "weak":    _bpp("weak_exposure"),
    }

    multiplier = mapping.get(regime, 1.0)
    return multiplier, regime


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT EXTRACTION
# ═══════════════════════════════════════════════════════════════

_SNAPSHOT_COLS = [
    "score_adjusted", "score_composite",
    "score_momentum", "score_trend", "score_volume", "score_volatility",
    "sig_confirmed", "sig_position_pct", "sig_reason",
    "sig_regime_ok", "sig_sector_ok", "sig_breadth_ok", "sig_momentum_ok",
    "sig_in_cooldown", "sig_effective_entry_min",
    "rs_regime", "rs_zscore",
    "sect_rs_regime", "sect_rs_rank", "sect_rs_pctrank",
    "sector_name", "sector_tailwind",
    "breadth_regime", "breadth_score",
]


def extract_snapshots(
    universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Extract latest-day signal data for every ticker.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} where each DataFrame is the output
        of ``generate_signals()``.

    Returns
    -------
    pd.DataFrame
        One row per ticker with key signal / score columns and a
        unified ``score`` column (adjusted → composite fallback).
    """
    rows: list[dict] = []
    for ticker, df in universe.items():
        if df is None or df.empty:
            continue
        last = df.iloc[-1]
        row: dict = {"ticker": ticker, "date": df.index[-1]}
        for col in _SNAPSHOT_COLS:
            row[col] = last.get(col, np.nan)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result["score"] = result.apply(_get_score, axis=1)
    return result


# ═══════════════════════════════════════════════════════════════
#  CANDIDATE FILTERING
# ═══════════════════════════════════════════════════════════════

def filter_candidates(
    snapshots: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split snapshots into candidates (sig_confirmed == 1) and
    rejected.

    Returns
    -------
    candidates : pd.DataFrame — stocks eligible for inclusion
    rejected   : pd.DataFrame — stocks without signal + reason
    """
    if snapshots.empty:
        empty = pd.DataFrame()
        return empty, empty

    is_long = snapshots["sig_confirmed"].fillna(0).astype(int) == 1

    candidates = snapshots[is_long].copy().reset_index(drop=True)
    rejected   = snapshots[~is_long].copy().reset_index(drop=True)

    # Annotate rejection reasons
    reasons: list[str] = []
    for _, row in rejected.iterrows():
        if row.get("sig_in_cooldown", False):
            reasons.append("cooldown")
        elif not row.get("sig_regime_ok", True):
            reasons.append(
                f"stock regime: {row.get('rs_regime', '?')}"
            )
        elif not row.get("sig_sector_ok", True):
            reasons.append(
                f"sector blocked: {row.get('sect_rs_regime', '?')}"
            )
        elif not row.get("sig_breadth_ok", True):
            reasons.append(
                f"breadth weak: {row.get('breadth_regime', '?')}"
            )
        elif not row.get("sig_momentum_ok", True):
            reasons.append("momentum unconfirmed")
        elif row.get("score", 0) < SIGNAL_PARAMS.get(
            "entry_score_min", 0.60
        ):
            reasons.append(
                f"score {row.get('score', 0):.2f} below entry"
            )
        else:
            reasons.append(str(row.get("sig_reason", "no signal")))
    rejected["rejection_reason"] = reasons

    return candidates, rejected


# ═══════════════════════════════════════════════════════════════
#  RANKING
# ═══════════════════════════════════════════════════════════════

def rank_candidates(
    candidates: pd.DataFrame,
    current_holdings: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Rank candidates by score.

    Incumbent positions receive a small bonus
    (``incumbent_bonus``) so the system doesn't churn in and
    out of names that hover near the threshold.
    """
    if candidates.empty:
        return candidates

    ranked = candidates.copy()
    bonus  = _pp("incumbent_bonus")

    if current_holdings and bonus > 0:
        ranked["incumbent"] = ranked["ticker"].isin(current_holdings)
        ranked["ranking_score"] = (
            ranked["score"]
            + ranked["incumbent"].astype(float) * bonus
        )
    else:
        ranked["incumbent"]     = False
        ranked["ranking_score"] = ranked["score"]

    ranked = (
        ranked
        .sort_values("ranking_score", ascending=False)
        .reset_index(drop=True)
    )
    ranked["rank"] = range(1, len(ranked) + 1)
    return ranked


# ═══════════════════════════════════════════════════════════════
#  SELECTION  +  SECTOR CAPS
# ═══════════════════════════════════════════════════════════════

def select_positions(
    ranked: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Walk the ranked list top→bottom and accept positions subject
    to **max_positions** and **max_sector_pct** constraints.

    Returns
    -------
    selected : pd.DataFrame — accepted (with ``raw_weight`` col)
    excluded : pd.DataFrame — dropped candidates + reason
    """
    if ranked.empty:
        return pd.DataFrame(), pd.DataFrame()

    max_pos    = _pp("max_positions")
    max_sector = _pp("max_sector_pct")
    min_w      = _pp("min_single_pct")

    selected_rows: list[pd.Series] = []
    excluded_rows: list[pd.Series] = []
    sector_weight: dict[str, float] = {}

    for _, row in ranked.iterrows():
        # ── Position limit ────────────────────────────────────
        if len(selected_rows) >= max_pos:
            r = row.copy()
            r["exclusion_reason"] = f"position limit ({max_pos})"
            excluded_rows.append(r)
            continue

        sector = row.get("sector_name", "Unknown")
        if pd.isna(sector):
            sector = "Unknown"

        raw_w = row.get("sig_position_pct", 0.05)
        if raw_w <= 0:
            raw_w = 0.05

        current_total = sector_weight.get(sector, 0.0)
        remaining     = max_sector - current_total

        # ── Sector cap ────────────────────────────────────────
        if remaining < min_w:
            r = row.copy()
            r["exclusion_reason"] = (
                f"sector cap ({sector} at {current_total:.0%})"
            )
            excluded_rows.append(r)
            continue

        actual_w = min(raw_w, remaining)

        r = row.copy()
        r["raw_weight"] = actual_w
        selected_rows.append(r)
        sector_weight[sector] = current_total + actual_w

    selected = (
        pd.DataFrame(selected_rows) if selected_rows
        else pd.DataFrame()
    )
    excluded = (
        pd.DataFrame(excluded_rows) if excluded_rows
        else pd.DataFrame()
    )
    return selected, excluded


# ═══════════════════════════════════════════════════════════════
#  WEIGHT NORMALISATION  (water-fill)
# ═══════════════════════════════════════════════════════════════

def _waterfill(
    tickers: list[str],
    raw: list[float],
    max_w: float,
    min_w: float,
    target: float,
) -> dict[str, float]:
    """
    Iterative weight normalisation with clamping.

    1. Scale so weights sum to *target*.
    2. Cap any weight above *max_w*.
    3. Remove any weight below *min_w*.
    4. Redistribute freed capital to unclamped names.
    5. Repeat until stable (usually 2–3 rounds).
    """
    weights = dict(zip(tickers, raw))

    for _ in range(10):
        weights = {k: v for k, v in weights.items() if v >= min_w * 0.5}
        if not weights:
            break

        total = sum(weights.values())
        if total <= 0:
            break

        scale   = target / total
        weights = {k: v * scale for k, v in weights.items()}

        changed = False
        for k in list(weights):
            if weights[k] > max_w:
                weights[k] = max_w
                changed = True
            elif weights[k] < min_w:
                del weights[k]
                changed = True

        if not changed:
            break

    return weights


def _enforce_sector_caps(
    weights: dict[str, float],
    sectors: dict[str, str],
    max_sector: float,
) -> dict[str, float]:
    """
    Post-normalisation sector-cap enforcement.

    If scaling pushed any sector above the limit, proportionally
    reduce its positions.  Freed capital becomes cash (not
    redistributed) to avoid infinite loops.
    """
    for _ in range(5):
        totals: dict[str, float] = {}
        for tk, w in weights.items():
            s = sectors.get(tk, "Unknown")
            totals[s] = totals.get(s, 0.0) + w

        any_over = False
        for s, t in totals.items():
            if t > max_sector + 0.001:
                scale = max_sector / t
                for tk in weights:
                    if sectors.get(tk, "Unknown") == s:
                        weights[tk] *= scale
                any_over = True

        if not any_over:
            break
    return weights


def compute_weights(
    selected: pd.DataFrame,
    target_override: float | None = None,
) -> pd.DataFrame:
    """
    Normalise ``raw_weight`` into final portfolio weights via
    water-fill, then enforce sector caps a second time (scaling
    can push totals above the cap set during selection).

    Parameters
    ----------
    selected : pd.DataFrame
        Rows from ``select_positions`` with ``raw_weight`` column.
    target_override : float, optional
        When provided, overrides ``target_invested_pct`` from
        config.  Used by breadth integration to scale exposure.
    """
    if selected.empty or "raw_weight" not in selected.columns:
        return selected

    target = (
        target_override
        if target_override is not None
        else _pp("target_invested_pct")
    )
    max_w = _pp("max_single_pct")
    min_w = _pp("min_single_pct")

    tickers = selected["ticker"].tolist()
    raw     = selected["raw_weight"].tolist()

    normalised = _waterfill(tickers, raw, max_w, min_w, target)

    # Build sector map for post-norm cap check
    sectors: dict[str, str] = {}
    for _, row in selected.iterrows():
        s = row.get("sector_name", "Unknown")
        sectors[row["ticker"]] = s if pd.notna(s) else "Unknown"

    normalised = _enforce_sector_caps(
        normalised, sectors, _pp("max_sector_pct")
    )

    kept = selected[selected["ticker"].isin(normalised)].copy()
    kept["weight"] = kept["ticker"].map(normalised)
    kept = (
        kept
        .sort_values("weight", ascending=False)
        .reset_index(drop=True)
    )
    return kept


# ═══════════════════════════════════════════════════════════════
#  REBALANCE
# ═══════════════════════════════════════════════════════════════

def compute_rebalance(
    target_weights: dict[str, float],
    current_weights: dict[str, float],
    threshold: float | None = None,
) -> pd.DataFrame:
    """
    Compare target vs current portfolio and produce a trade list.

    Actions: BUY (new), SELL (close), ADD (increase),
    TRIM (decrease), HOLD (change below threshold).
    """
    if threshold is None:
        threshold = _pp("rebalance_threshold")

    all_tickers = sorted(
        set(list(target_weights) + list(current_weights))
    )

    rows: list[dict] = []
    for tk in all_tickers:
        curr = current_weights.get(tk, 0.0)
        tgt  = target_weights.get(tk, 0.0)
        diff = tgt - curr

        if abs(diff) < threshold:
            action = "HOLD"
        elif diff > 0 and curr == 0:
            action = "BUY"
        elif diff < 0 and tgt == 0:
            action = "SELL"
        elif diff > 0:
            action = "ADD"
        else:
            action = "TRIM"

        rows.append({
            "ticker":         tk,
            "current_weight": round(curr, 4),
            "target_weight":  round(tgt, 4),
            "delta":          round(diff, 4),
            "action":         action,
        })

    result = pd.DataFrame(rows)
    order  = {"SELL": 0, "TRIM": 1, "HOLD": 2, "ADD": 3, "BUY": 4}
    result["_s"] = result["action"].map(order)
    result = (
        result
        .sort_values(["_s", "delta"], ascending=[True, True])
        .drop(columns="_s")
        .reset_index(drop=True)
    )
    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def build_portfolio(
    universe: dict[str, pd.DataFrame],
    current_holdings: dict[str, float] | None = None,
    breadth: pd.DataFrame | None = None,
) -> dict:
    """
    Full portfolio construction pipeline.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} — each DataFrame is the output
        of ``generate_signals()``.
    current_holdings : dict, optional
        {ticker: weight} of the current portfolio.  When
        provided, incumbents get a ranking bonus and a rebalance
        trade list is generated.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()``.  When provided,
        the target invested percentage is scaled by the breadth
        regime (strong / neutral / weak).

    Returns
    -------
    dict
        ===============  ==========================================
        snapshots        latest-day row for every ticker
        candidates       tickers with active long signal
        rejected         tickers without signal + reason
        holdings         final portfolio with weights
        excluded         candidates that didn't make the cut
        target_weights   {ticker: weight}
        sector_exposure  {sector: total_weight}
        trades           rebalance trades (None if no current)
        metadata         summary statistics
        ===============  ==========================================
    """
    snapshots = extract_snapshots(universe)

    if snapshots.empty:
        return _empty_result(current_holdings)

    # ── Breadth exposure adjustment ───────────────────────────
    breadth_mult, breadth_regime = _breadth_exposure_multiplier(breadth)
    base_target    = _pp("target_invested_pct")
    adjusted_target = base_target * breadth_mult

    # ── Filter ────────────────────────────────────────────────
    candidates, rejected = filter_candidates(snapshots)

    if candidates.empty:
        return _no_candidate_result(
            snapshots, rejected, current_holdings,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        )

    # ── Rank ──────────────────────────────────────────────────
    ranked = rank_candidates(candidates, current_holdings)

    # ── Select ────────────────────────────────────────────────
    selected, excluded = select_positions(ranked)

    if selected.empty:
        return _no_candidate_result(
            snapshots, rejected, current_holdings, excluded,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        )

    # ── Weight (breadth-adjusted target) ──────────────────────
    holdings = compute_weights(selected, target_override=adjusted_target)

    target_weights = (
        dict(zip(holdings["ticker"], holdings["weight"]))
        if not holdings.empty else {}
    )

    # ── Sector exposure ───────────────────────────────────────
    sector_exposure = _calc_sector_exposure(holdings)

    # ── Rebalance ─────────────────────────────────────────────
    trades = None
    if current_holdings is not None:
        trades = compute_rebalance(target_weights, current_holdings)

    return {
        "snapshots":       snapshots,
        "candidates":      candidates,
        "rejected":        rejected,
        "holdings":        holdings,
        "excluded":        excluded,
        "target_weights":  target_weights,
        "sector_exposure": sector_exposure,
        "trades":          trades,
        "metadata":        _build_metadata(
            holdings, snapshots,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        ),
    }


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _calc_sector_exposure(holdings: pd.DataFrame) -> dict[str, float]:
    if holdings.empty or "sector_name" not in holdings.columns:
        return {}
    exp: dict[str, float] = {}
    for sec in holdings["sector_name"].dropna().unique():
        mask = holdings["sector_name"] == sec
        exp[str(sec)] = round(
            float(holdings.loc[mask, "weight"].sum()), 4
        )
    return dict(sorted(exp.items(), key=lambda x: -x[1]))


def _build_metadata(
    holdings: pd.DataFrame,
    snapshots: pd.DataFrame,
    breadth_regime: str = "unknown",
    breadth_mult: float = 1.0,
) -> dict:
    invested = (
        float(holdings["weight"].sum())
        if "weight" in holdings.columns else 0.0
    )
    n_sect = (
        int(holdings["sector_name"].nunique())
        if "sector_name" in holdings.columns and not holdings.empty
        else 0
    )
    n_cand = (
        int(snapshots["sig_confirmed"].fillna(0).eq(1).sum())
        if not snapshots.empty else 0
    )
    return {
        "num_universe":     len(snapshots),
        "num_candidates":   n_cand,
        "num_holdings":     len(holdings),
        "total_invested":   round(invested, 4),
        "cash_pct":         round(1.0 - invested, 4),
        "num_sectors":      n_sect,
        "breadth_regime":   breadth_regime,
        "breadth_exposure": round(breadth_mult, 2),
    }


def _empty_result(current_holdings=None) -> dict:
    trades = None
    if current_holdings:
        trades = compute_rebalance({}, current_holdings)
    return {
        "snapshots":       pd.DataFrame(),
        "candidates":      pd.DataFrame(),
        "rejected":        pd.DataFrame(),
        "holdings":        pd.DataFrame(),
        "excluded":        pd.DataFrame(),
        "target_weights":  {},
        "sector_exposure": {},
        "trades":          trades,
        "metadata": {
            "num_universe": 0, "num_candidates": 0,
            "num_holdings": 0, "total_invested": 0.0,
            "cash_pct": 1.0, "num_sectors": 0,
            "breadth_regime": "unknown", "breadth_exposure": 1.0,
        },
    }


def _no_candidate_result(
    snapshots, rejected,
    current_holdings=None, excluded=None,
    breadth_regime: str = "unknown",
    breadth_mult: float = 1.0,
) -> dict:
    trades = None
    if current_holdings:
        trades = compute_rebalance({}, current_holdings)
    return {
        "snapshots":       snapshots,
        "candidates":      pd.DataFrame(),
        "rejected":        rejected,
        "holdings":        pd.DataFrame(),
        "excluded":        excluded if excluded is not None
                           else pd.DataFrame(),
        "target_weights":  {},
        "sector_exposure": {},
        "trades":          trades,
        "metadata":        _build_metadata(
            pd.DataFrame(), snapshots,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        ),
    }


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

def portfolio_report(result: dict) -> str:
    """
    Format a ``build_portfolio`` result as a human-readable
    report string.
    """
    ln: list[str] = []
    meta = result.get("metadata", {})
    div  = "=" * 60
    sub  = "-" * 60

    # ── Header ────────────────────────────────────────────────
    ln.append(div)
    ln.append("PORTFOLIO SUMMARY")
    ln.append(div)
    ln.append(f"  Universe:       {meta.get('num_universe', 0)} stocks")
    ln.append(
        f"  Candidates:     "
        f"{meta.get('num_candidates', 0)} with active signal"
    )
    ln.append(
        f"  Holdings:       {meta.get('num_holdings', 0)} positions"
    )
    ln.append(
        f"  Invested:       {meta.get('total_invested', 0):.1%}"
    )
    ln.append(f"  Cash:           {meta.get('cash_pct', 1):.1%}")
    ln.append(f"  Sectors:        {meta.get('num_sectors', 0)}")

    # ── Breadth context ───────────────────────────────────────
    b_regime = meta.get("breadth_regime", "unknown")
    b_mult   = meta.get("breadth_exposure", 1.0)
    ln.append(
        f"  Breadth:        {b_regime}  "
        f"(exposure scale: {b_mult:.0%})"
    )

    # ── Sector exposure ───────────────────────────────────────
    se = result.get("sector_exposure", {})
    if se:
        ln.append("")
        ln.append(sub)
        ln.append("SECTOR EXPOSURE")
        ln.append(sub)
        for sec, w in se.items():
            bar = "█" * int(w * 50)
            ln.append(f"  {sec:<20} {w:>6.1%}  {bar}")

    # ── Holdings ──────────────────────────────────────────────
    h = result.get("holdings", pd.DataFrame())
    if not h.empty:
        ln.append("")
        ln.append(sub)
        ln.append("HOLDINGS")
        ln.append(sub)
        header = (
            f"  {'Ticker':<7} {'Weight':>7} {'Score':>6} "
            f"{'Sector':<16} {'Regime':<12} {'Sect Rgm':<10}"
        )
        ln.append(header)
        ln.append(
            f"  {'──────':<7} {'──────':>7} {'─────':>6} "
            f"{'───────────────':<16} {'──────────':<12} "
            f"{'────────':<10}"
        )
        for _, row in h.iterrows():
            ln.append(
                f"  {str(row.get('ticker','?')):<7} "
                f"{row.get('weight', 0):>6.1%} "
                f"{row.get('score', 0):>6.2f} "
                f"{str(row.get('sector_name','?')):<16} "
                f"{str(row.get('rs_regime','?')):<12} "
                f"{str(row.get('sect_rs_regime','?')):<10}"
            )

    # ── Excluded candidates ───────────────────────────────────
    ex = result.get("excluded", pd.DataFrame())
    if not ex.empty:
        ln.append("")
        ln.append(sub)
        ln.append(f"EXCLUDED CANDIDATES ({len(ex)})")
        ln.append(sub)
        for _, row in ex.iterrows():
            ln.append(
                f"  {str(row.get('ticker','?')):<7} "
                f"score={row.get('score', 0):.2f}  "
                f"→ {row.get('exclusion_reason', '?')}"
            )

    # ── Rejected ──────────────────────────────────────────────
    rej = result.get("rejected", pd.DataFrame())
    if not rej.empty:
        ln.append("")
        ln.append(sub)
        ln.append(f"REJECTED ({len(rej)} stocks)")
        ln.append(sub)
        show = rej.sort_values("score", ascending=False).head(10)
        for _, row in show.iterrows():
            ln.append(
                f"  {str(row.get('ticker','?')):<7} "
                f"score={row.get('score', 0):.2f}  "
                f"→ {row.get('rejection_reason', '?')}"
            )
        if len(rej) > 10:
            ln.append(f"  ... and {len(rej) - 10} more")

    # ── Trades ────────────────────────────────────────────────
    trades = result.get("trades")
    if trades is not None and not trades.empty:
        ln.append("")
        ln.append(sub)
        ln.append("REBALANCE TRADES")
        ln.append(sub)
        for _, row in trades.iterrows():
            act  = row["action"]
            tick = row["ticker"]
            tgt  = row["target_weight"]
            d    = row["delta"]
            if act == "HOLD":
                ln.append(
                    f"  {act:<5} {tick:<7} @ {tgt:>5.1%}"
                )
            else:
                sign = "+" if d > 0 else ""
                ln.append(
                    f"  {act:<5} {tick:<7} → {tgt:>5.1%} "
                    f"({sign}{d:.1%})"
                )

    return "\n".join(ln)

    
####################################################################################   

"""
strategy/convergence.py
-----------------------
Dual-list signal convergence for US; scoring-only passthrough
for HK and India.

Architecture
────────────
  US:  scoring engine (bottom-up) + rotation engine (top-down)
       → merge_convergence() → ConvergedSignal list

  HK:  scoring engine only (vs 2800.HK benchmark)
       → scoring_passthrough() → ConvergedSignal list

  IN:  scoring engine only (vs NIFTYBEES.NS benchmark)
       → scoring_passthrough() → ConvergedSignal list

The value of the dual list isn't either list in isolation — it's
the convergence.

  STRONG_BUY:  BUY on BOTH rotation + scoring.  The stock is in
               a leading sector AND scores well on its own merits.
               This is the highest conviction signal.

  BUY_SCORING: BUY on scoring only.  Good individual profile but
               sector isn't leading.  Still tradeable — just lower
               conviction than STRONG_BUY.

  BUY_ROTATION: BUY on rotation only.  In a leading sector but
                individual metrics don't confirm yet.  Watch for
                scoring improvement.

  CONFLICT:    One says BUY, the other says SELL.  A strong name
               swimming against the sector tide, or a weak name
               dragged up by its sector.  Flag for manual review.

  STRONG_SELL: SELL on BOTH.  Highest conviction exit.

Convergence scoring adjustment:
  STRONG_BUY  → composite + convergence_boost  (default +0.10)
  CONFLICT    → composite + conflict_penalty   (default −0.05)

Parameters live in common/config.py → US_CONVERGENCE.

Pipeline
────────
  Scoring snapshots         Rotation result
  (pipeline/runner.py)      (strategy/rotation.py)
        │                         │
        └──────────┬──────────────┘
                   ↓
         merge_convergence()        ← US only
                   ↓
         list[ConvergedSignal]
                   ↓
         MarketSignalResult
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from common.config import MARKET_CONFIG

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONVERGENCE LABELS
# ═══════════════════════════════════════════════════════════════

STRONG_BUY    = "STRONG_BUY"
BUY_SCORING   = "BUY_SCORING"
BUY_ROTATION  = "BUY_ROTATION"
CONFLICT      = "CONFLICT"
STRONG_SELL   = "STRONG_SELL"
SELL_SCORING  = "SELL_SCORING"
SELL_ROTATION = "SELL_ROTATION"
HOLD          = "HOLD"
NEUTRAL       = "NEUTRAL"

_BUY_LABELS  = {STRONG_BUY, BUY_SCORING, BUY_ROTATION}
_SELL_LABELS = {STRONG_SELL, SELL_SCORING, SELL_ROTATION}


# ═══════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class ConvergedSignal:
    """One ticker's merged signal from both engines."""

    ticker: str
    convergence_label: str

    # ── Scoring engine data ────────────────────────────────────
    scoring_signal: str | None       # BUY / HOLD / SELL / None
    composite_score: float           # raw composite from scoring
    adjusted_score: float            # after convergence boost/penalty
    scoring_regime: str              # rs_regime from scoring engine
    scoring_confirmed: bool          # sig_confirmed == 1

    # ── Rotation engine data ───────────────────────────────────
    rotation_signal: str | None      # BUY / SELL / HOLD / REDUCE / None
    rotation_rs: float               # composite RS from rotation
    rotation_sector: str | None      # sector name
    rotation_sector_rank: int        # 1 = strongest, 99 = unknown
    rotation_sector_tier: str        # Leading / Neutral / Lagging / n/a
    rotation_reason: str             # human-readable reason

    # ── Assigned after sorting ─────────────────────────────────
    rank: int = 0

    @property
    def is_buy(self) -> bool:
        return self.convergence_label in _BUY_LABELS

    @property
    def is_sell(self) -> bool:
        return self.convergence_label in _SELL_LABELS

    @property
    def is_conflict(self) -> bool:
        return self.convergence_label == CONFLICT

    def to_dict(self) -> dict:
        return {
            "ticker":               self.ticker,
            "convergence_label":    self.convergence_label,
            "scoring_signal":       self.scoring_signal,
            "composite_score":      round(self.composite_score, 4),
            "adjusted_score":       round(self.adjusted_score, 4),
            "scoring_regime":       self.scoring_regime,
            "scoring_confirmed":    self.scoring_confirmed,
            "rotation_signal":      self.rotation_signal,
            "rotation_rs":          round(self.rotation_rs, 4),
            "rotation_sector":      self.rotation_sector,
            "rotation_sector_rank": self.rotation_sector_rank,
            "rotation_sector_tier": self.rotation_sector_tier,
            "rotation_reason":      self.rotation_reason,
            "rank":                 self.rank,
        }


@dataclass
class MarketSignalResult:
    """Complete convergence output for one market."""

    market: str
    signals: list[ConvergedSignal] = field(default_factory=list)

    @property
    def buys(self) -> list[ConvergedSignal]:
        return [s for s in self.signals if s.is_buy]

    @property
    def sells(self) -> list[ConvergedSignal]:
        return [s for s in self.signals if s.is_sell]

    @property
    def conflicts(self) -> list[ConvergedSignal]:
        return [s for s in self.signals if s.is_conflict]

    @property
    def holds(self) -> list[ConvergedSignal]:
        return [s for s in self.signals
                if s.convergence_label == HOLD]

    @property
    def strong_buys(self) -> list[ConvergedSignal]:
        return [s for s in self.signals
                if s.convergence_label == STRONG_BUY]

    @property
    def strong_sells(self) -> list[ConvergedSignal]:
        return [s for s in self.signals
                if s.convergence_label == STRONG_SELL]

    @property
    def n_tickers(self) -> int:
        return len(self.signals)

    def get_signal(self, ticker: str) -> ConvergedSignal | None:
        for s in self.signals:
            if s.ticker == ticker:
                return s
        return None

    def to_dataframe(self) -> pd.DataFrame:
        if not self.signals:
            return pd.DataFrame()
        return pd.DataFrame([s.to_dict() for s in self.signals])

    def summary(self) -> str:
        parts = [f"[{self.market}] {self.n_tickers} tickers"]
        label_counts: dict[str, int] = {}
        for s in self.signals:
            label_counts[s.convergence_label] = (
                label_counts.get(s.convergence_label, 0) + 1
            )
        for label in [STRONG_BUY, BUY_SCORING, BUY_ROTATION,
                      HOLD, NEUTRAL, CONFLICT,
                      SELL_SCORING, SELL_ROTATION, STRONG_SELL]:
            cnt = label_counts.get(label, 0)
            if cnt > 0:
                parts.append(f"{label}={cnt}")
        return "  ".join(parts)


# ═══════════════════════════════════════════════════════════════
#  MERGE CONVERGENCE  (US dual-list)
# ═══════════════════════════════════════════════════════════════

def merge_convergence(
    scoring_snapshots: list[dict],
    rotation_result,
    convergence_config: dict | None = None,
) -> list[ConvergedSignal]:
    """
    Merge scoring engine snapshots with rotation engine result
    into a unified signal set with convergence labels.

    Parameters
    ----------
    scoring_snapshots : list[dict]
        Output of ``results_to_snapshots()`` — per-ticker dicts
        with keys: ticker, composite, signal, sig_confirmed,
        rs_regime, sig_exit, etc.
    rotation_result : RotationResult
        Output of ``strategy.rotation.run_rotation()``.
    convergence_config : dict, optional
        Convergence parameters.  Defaults to
        ``common.config.US_CONVERGENCE``.

    Returns
    -------
    list[ConvergedSignal]
        One per ticker, sorted by adjusted_score descending,
        with rank assigned.
    """
    if convergence_config is None:
        convergence_config = MARKET_CONFIG["US"].get("convergence", {})
    if convergence_config is None:
        convergence_config = {}

    boost   = convergence_config.get("convergence_boost", 0.10)
    penalty = convergence_config.get("conflict_penalty", -0.05)

    # ── Index rotation recommendations by ticker ──────────────
    rot_map: dict[str, dict] = {}
    if rotation_result is not None:
        for rec in rotation_result.recommendations:
            rot_map[rec.ticker] = {
                "action":       rec.action.value,
                "rs":           rec.rs_composite,
                "rs_vs_sector": rec.rs_vs_sector_etf,
                "sector":       rec.sector,
                "sector_rank":  rec.sector_rank,
                "sector_tier":  rec.sector_tier,
                "reason":       rec.reason,
            }

    # ── Walk every scored ticker ──────────────────────────────
    signals: list[ConvergedSignal] = []
    processed: set[str] = set()

    for snap in scoring_snapshots:
        ticker = snap.get("ticker", "???")
        processed.add(ticker)

        score     = snap.get("composite", 0.0)
        scr_sig   = snap.get("signal", "HOLD")
        confirmed = snap.get("sig_confirmed", 0) == 1
        regime    = snap.get("rs_regime", "unknown")
        sig_exit  = snap.get("sig_exit", 0) == 1

        # Normalise scoring engine's opinion
        s_buy  = scr_sig in ("BUY", "STRONG_BUY") or confirmed
        s_sell = scr_sig in ("SELL", "STRONG_SELL") or sig_exit

        # Look up rotation engine's opinion
        rot = rot_map.get(ticker, {})
        r_action = rot.get("action")   # BUY / SELL / HOLD / REDUCE / None

        r_buy  = r_action == "BUY"
        r_sell = r_action in ("SELL", "REDUCE")

        # ── Convergence logic ─────────────────────────────────
        label, adj = _classify(
            s_buy, s_sell, r_buy, r_sell,
            r_action, scr_sig, score, boost, penalty,
        )

        signals.append(ConvergedSignal(
            ticker=ticker,
            convergence_label=label,
            scoring_signal=scr_sig,
            composite_score=score,
            adjusted_score=adj,
            scoring_regime=regime,
            scoring_confirmed=confirmed,
            rotation_signal=r_action,
            rotation_rs=rot.get("rs", 0.0),
            rotation_sector=rot.get("sector"),
            rotation_sector_rank=rot.get("sector_rank", 99),
            rotation_sector_tier=rot.get("sector_tier", "n/a"),
            rotation_reason=rot.get("reason", ""),
        ))

    # ── Rotation-only tickers (not in scoring universe) ───────
    for ticker, rot in rot_map.items():
        if ticker in processed:
            continue

        r_action = rot["action"]
        if r_action == "BUY":
            label = BUY_ROTATION
        elif r_action in ("SELL", "REDUCE"):
            label = SELL_ROTATION
        else:
            label = HOLD

        signals.append(ConvergedSignal(
            ticker=ticker,
            convergence_label=label,
            scoring_signal=None,
            composite_score=0.0,
            adjusted_score=0.0,
            scoring_regime="unknown",
            scoring_confirmed=False,
            rotation_signal=r_action,
            rotation_rs=rot.get("rs", 0.0),
            rotation_sector=rot.get("sector"),
            rotation_sector_rank=rot.get("sector_rank", 99),
            rotation_sector_tier=rot.get("sector_tier", "n/a"),
            rotation_reason=rot.get("reason", ""),
        ))

    # ── Rank by adjusted score ────────────────────────────────
    signals.sort(key=lambda s: s.adjusted_score, reverse=True)
    for i, sig in enumerate(signals, 1):
        sig.rank = i

    return signals


def _classify(
    s_buy: bool, s_sell: bool,
    r_buy: bool, r_sell: bool,
    r_action: str | None,
    scr_sig: str,
    score: float,
    boost: float,
    penalty: float,
) -> tuple[str, float]:
    """
    Apply convergence classification logic.

    Returns (label, adjusted_score).
    """
    if s_buy and r_buy:
        return STRONG_BUY, min(1.0, score + boost)

    if s_buy and r_sell:
        return CONFLICT, max(0.0, score + penalty)

    if s_sell and r_buy:
        return CONFLICT, max(0.0, score + penalty)

    if s_sell and r_sell:
        return STRONG_SELL, max(0.0, score + penalty)

    if s_buy and not r_sell:
        return BUY_SCORING, score

    if r_buy and not s_sell:
        return BUY_ROTATION, score

    if s_sell and not r_buy:
        return SELL_SCORING, score

    if r_sell and not s_buy:
        return SELL_ROTATION, score

    if r_action == "HOLD" or scr_sig == "HOLD":
        return HOLD, score

    return NEUTRAL, score


# ═══════════════════════════════════════════════════════════════
#  SCORING-ONLY PASSTHROUGH  (HK, IN)
# ═══════════════════════════════════════════════════════════════

def scoring_passthrough(
    scoring_snapshots: list[dict],
    market: str = "HK",
) -> list[ConvergedSignal]:
    """
    Convert scoring-only snapshots into ConvergedSignal objects.

    No rotation engine → no dual-list merge.  The convergence
    label simply reflects the scoring engine's assessment.

    Parameters
    ----------
    scoring_snapshots : list[dict]
        Same format as ``merge_convergence`` input.
    market : str
        Market label for logging.

    Returns
    -------
    list[ConvergedSignal]
        Sorted by composite_score descending, ranked.
    """
    signals: list[ConvergedSignal] = []

    for snap in scoring_snapshots:
        ticker    = snap.get("ticker", "???")
        score     = snap.get("composite", 0.0)
        scr_sig   = snap.get("signal", "HOLD")
        confirmed = snap.get("sig_confirmed", 0) == 1
        regime    = snap.get("rs_regime", "unknown")
        sig_exit  = snap.get("sig_exit", 0) == 1

        # Map scoring signal → convergence label
        if scr_sig in ("BUY", "STRONG_BUY") or confirmed:
            label = BUY_SCORING
        elif scr_sig in ("SELL", "STRONG_SELL") or sig_exit:
            label = SELL_SCORING
        elif scr_sig == "HOLD":
            label = HOLD
        else:
            label = NEUTRAL

        signals.append(ConvergedSignal(
            ticker=ticker,
            convergence_label=label,
            scoring_signal=scr_sig,
            composite_score=score,
            adjusted_score=score,
            scoring_regime=regime,
            scoring_confirmed=confirmed,
            rotation_signal=None,
            rotation_rs=0.0,
            rotation_sector=None,
            rotation_sector_rank=99,
            rotation_sector_tier="n/a",
            rotation_reason="",
        ))

    signals.sort(key=lambda s: s.adjusted_score, reverse=True)
    for i, sig in enumerate(signals, 1):
        sig.rank = i

    return signals


# ═══════════════════════════════════════════════════════════════
#  MARKET DISPATCHER
# ═══════════════════════════════════════════════════════════════

def run_convergence(
    market: str,
    scoring_snapshots: list[dict],
    rotation_result=None,
) -> MarketSignalResult:
    """
    Route to the correct merge strategy based on market.

    Parameters
    ----------
    market : str
        "US", "HK", or "IN".
    scoring_snapshots : list[dict]
        Per-ticker snapshot dicts from the scoring pipeline.
    rotation_result : RotationResult or None
        Output of ``strategy.rotation.run_rotation()``.
        Required for US, ignored for HK/IN.

    Returns
    -------
    MarketSignalResult
    """
    cfg = MARKET_CONFIG.get(market, {})
    engines = cfg.get("engines", ["scoring"])

    if "rotation" in engines and rotation_result is not None:
        convergence_cfg = cfg.get("convergence", {})
        signals = merge_convergence(
            scoring_snapshots, rotation_result, convergence_cfg
        )
        logger.info(
            f"[{market}] Convergence merge: "
            f"{sum(1 for s in signals if s.convergence_label == STRONG_BUY)} "
            f"STRONG_BUY, "
            f"{sum(1 for s in signals if s.is_conflict)} CONFLICT, "
            f"{len(signals)} total"
        )
    else:
        signals = scoring_passthrough(scoring_snapshots, market)
        logger.info(
            f"[{market}] Scoring only: "
            f"{sum(1 for s in signals if s.is_buy)} BUY, "
            f"{sum(1 for s in signals if s.is_sell)} SELL, "
            f"{len(signals)} total"
        )

    result = MarketSignalResult(market=market, signals=signals)
    logger.info(result.summary())
    return result


# ═══════════════════════════════════════════════════════════════
#  HELPERS — enrich snapshots with convergence data
# ═══════════════════════════════════════════════════════════════

def enrich_snapshots(
    snapshots: list[dict],
    convergence: MarketSignalResult,
) -> list[dict]:
    """
    Write convergence labels and adjusted scores back into
    the snapshot dicts.

    Modifies snapshots in-place.  Downstream report generators
    can then pick up ``convergence_label`` and ``adjusted_score``
    from the snapshot without knowing about the convergence
    module directly.
    """
    sig_map = {s.ticker: s for s in convergence.signals}

    for snap in snapshots:
        ticker = snap.get("ticker")
        cs = sig_map.get(ticker)
        if cs is None:
            snap["convergence_label"] = NEUTRAL
            snap["convergence_rank"]  = 999
            continue

        snap["convergence_label"]    = cs.convergence_label
        snap["convergence_rank"]     = cs.rank
        snap["adjusted_score"]       = cs.adjusted_score
        snap["rotation_signal"]      = cs.rotation_signal
        snap["rotation_rs"]          = cs.rotation_rs
        snap["rotation_sector"]      = cs.rotation_sector
        snap["rotation_sector_rank"] = cs.rotation_sector_rank
        snap["rotation_sector_tier"] = cs.rotation_sector_tier
        snap["rotation_reason"]      = cs.rotation_reason

    return snapshots


# ═══════════════════════════════════════════════════════════════
#  PRICE MATRIX BUILDER
# ═══════════════════════════════════════════════════════════════

def build_price_matrix(
    ohlcv: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Build a wide-format adjusted-close matrix from per-ticker
    OHLCV DataFrames.

    The rotation engine expects:
      DatetimeIndex × ticker columns → close prices.

    Forward-fills gaps so the matrix is dense.

    Parameters
    ----------
    ohlcv : dict[str, pd.DataFrame]
        {ticker: OHLCV DataFrame with 'close' column}

    Returns
    -------
    pd.DataFrame
        Wide matrix, or empty DataFrame if no valid data.
    """
    frames: dict[str, pd.Series] = {}
    for ticker, df in ohlcv.items():
        if df is None or df.empty:
            continue
        if "close" in df.columns:
            frames[ticker] = df["close"]

    if not frames:
        return pd.DataFrame()

    matrix = pd.DataFrame(frames)
    matrix = matrix.sort_index().ffill()
    return matrix


# ═══════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═══════════════════════════════════════════════════════════════

def convergence_report(result: MarketSignalResult) -> str:
    """
    Format a MarketSignalResult as a human-readable text report.
    """
    ln: list[str] = []
    w = 76
    div = "=" * w
    sub = "-" * w
    market = result.market

    ln.append(div)
    ln.append(f"  {market} CONVERGENCE REPORT")
    ln.append(div)
    ln.append(f"  {result.summary()}")

    # ── Strong buys ───────────────────────────────────────────
    if result.strong_buys:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🟢🟢  STRONG BUY  ({len(result.strong_buys)})")
        ln.append(f"  Both scoring + rotation agree — highest conviction")
        ln.append(sub)
        for s in result.strong_buys:
            ln.append(_fmt_signal(s))

    # ── Buy (scoring only) ────────────────────────────────────
    scr_buys = [s for s in result.signals
                if s.convergence_label == BUY_SCORING]
    if scr_buys:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🟢  BUY — Scoring Only  ({len(scr_buys)})")
        ln.append(f"  Strong individual profile, sector not leading")
        ln.append(sub)
        for s in scr_buys:
            ln.append(_fmt_signal(s))

    # ── Buy (rotation only) ───────────────────────────────────
    rot_buys = [s for s in result.signals
                if s.convergence_label == BUY_ROTATION]
    if rot_buys:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🟢  BUY — Rotation Only  ({len(rot_buys)})")
        ln.append(f"  In leading sector, individual metrics not confirmed")
        ln.append(sub)
        for s in rot_buys:
            ln.append(_fmt_signal(s))

    # ── Conflicts ─────────────────────────────────────────────
    if result.conflicts:
        ln.append("")
        ln.append(sub)
        ln.append(f"  ⚠️  CONFLICT  ({len(result.conflicts)})")
        ln.append(f"  Engines disagree — review manually")
        ln.append(sub)
        for s in result.conflicts:
            ln.append(_fmt_signal(s))

    # ── Sells ─────────────────────────────────────────────────
    if result.sells:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🔴  SELL  ({len(result.sells)})")
        ln.append(sub)
        for s in result.sells:
            ln.append(_fmt_signal(s))

    # ── Holds (abbreviated) ───────────────────────────────────
    holds = result.holds
    if holds:
        ln.append("")
        ln.append(sub)
        ln.append(f"  ⚪  HOLD  ({len(holds)})")
        ln.append(sub)
        for s in holds[:10]:
            ln.append(
                f"    #{s.rank:<3d}  {s.ticker:<8s}  "
                f"score={s.composite_score:.3f}"
            )
        if len(holds) > 10:
            ln.append(f"    ... and {len(holds) - 10} more")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)


def _fmt_signal(s: ConvergedSignal) -> str:
    """Format one ConvergedSignal for text display."""
    parts = [
        f"    #{s.rank:<3d}  {s.ticker:<8s}  "
        f"score={s.composite_score:.3f}  "
        f"adj={s.adjusted_score:.3f}"
    ]

    if s.scoring_signal:
        confirmed = "✓" if s.scoring_confirmed else "✗"
        parts.append(
            f"  scr={s.scoring_signal}[{confirmed}] "
            f"regime={s.scoring_regime}"
        )

    if s.rotation_signal:
        parts.append(
            f"  rot={s.rotation_signal} "
            f"RS={s.rotation_rs:+.3f}"
        )
        if s.rotation_sector:
            parts.append(
                f"  sector={s.rotation_sector} "
                f"(#{s.rotation_sector_rank} {s.rotation_sector_tier})"
            )

    return "".join(parts)

#################################################################
"""
strategy/rotation.py
------------------
Core Smart Money Rotation engine.

Flow
====
  1. Compute composite relative strength (RS) for each sector ETF vs SPY
  2. Rank the 11 GICS sectors → Leading (top 3), Neutral, Lagging (bottom 3)
  3. Within each Leading sector, rank US single names by composite RS
  4. Select the top N stocks per Leading sector → BUY candidates
  5. Evaluate every current holding against sell rules → SELL / REDUCE / HOLD
  6. Enforce max-position cap, combine, and return RotationResult

Relative Strength
=================
  RS_composite(ticker) = Σ  weight_i × (ret_i(ticker) − ret_i(benchmark))

  Default weights:
      40 %  ×  21-day return   (≈ 1 month)   — recent momentum
      35 %  ×  63-day return   (≈ 3 months)
      25 %  × 126-day return   (≈ 6 months)  — persistence filter

Sell Rules (in priority order)
==============================
  1. Sector moved to Lagging (bottom 3)       → SELL
  2. Sector moved to Neutral (middle 5)        → REDUCE  (half position)
  3. Individual RS below threshold              → SELL
  4. None of the above                          → HOLD

All tunable knobs live in the RotationConfig dataclass.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum

import pandas as pd

from common.sector_map import (
    SECTOR_ETFS,
    get_sector,
    get_sector_or_class,
    get_us_tickers_for_sector,
)

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class RotationConfig:
    """Every tunable knob for the rotation engine."""

    # ── Benchmark ──────────────────────────────────────────────
    benchmark: str = "SPY"

    # ── Sector tier sizing ─────────────────────────────────────
    n_leading_sectors: int = 3
    n_lagging_sectors: int = 3      # bottom N → full SELL

    # ── RS lookback periods (trading days) and weights ─────────
    # Must sum to 1.0.
    rs_periods: dict[int, float] = field(default_factory=lambda: {
        21:  0.40,    # ≈ 1 month
        63:  0.35,    # ≈ 3 months
        126: 0.25,    # ≈ 6 months
    })

    # ── Stock selection within leading sectors ─────────────────
    stocks_per_sector: int = 3
    min_rs_score: float = 0.0       # floor RS to be BUY-eligible
    prefer_positive_rs_vs_sector: bool = True   # must also beat sector ETF

    # ── Sell thresholds ────────────────────────────────────────
    sell_if_sector_not_leading: bool = True
    sell_individual_rs_below: float = -0.05     # hard floor on individual RS

    # ── Portfolio constraints ──────────────────────────────────
    max_total_positions: int = 12
    max_per_sector: int = 4         # including sector ETF fallback

    # ── Data requirements ──────────────────────────────────────
    min_history_days: int = 130     # must have ≥ this many rows


# ═══════════════════════════════════════════════════════════════
#  ENUMS & DATA CLASSES
# ═══════════════════════════════════════════════════════════════

class Action(str, Enum):
    BUY    = "BUY"
    SELL   = "SELL"
    HOLD   = "HOLD"
    REDUCE = "REDUCE"          # sector drifted to Neutral → trim


@dataclass
class SectorScore:
    """One row in the sector ranking table."""
    sector: str
    etf: str
    composite_rs: float
    rank: int                  # 1 = strongest
    tier: str                  # Leading / Neutral / Lagging
    period_returns: dict[int, float] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (f"  {self.rank:2d}. {self.sector:28s} [{self.etf:4s}]  "
                f"RS {self.composite_rs:+.4f}  ({self.tier})")


@dataclass
class Recommendation:
    """A single actionable signal."""
    ticker: str
    action: Action
    sector: str               # GICS sector or asset-class label
    sector_rank: int           # 1–11 (99 if not a sector ticker)
    sector_tier: str           # Leading / Neutral / Lagging / n/a
    rs_composite: float        # ticker RS vs SPY
    rs_vs_sector_etf: float    # ticker RS vs its own sector ETF
    reason: str

    def __repr__(self) -> str:
        return (f"  {self.action.value:6s}  {self.ticker:8s}  │  "
                f"{self.sector:24s} (rank {self.sector_rank:2d}, {self.sector_tier:8s})  │  "
                f"RS {self.rs_composite:+.4f}  vs-sector {self.rs_vs_sector_etf:+.4f}  │  "
                f"{self.reason}")


@dataclass
class RotationResult:
    """Complete output of one rotation run."""
    as_of_date: date
    config: RotationConfig
    sector_rankings: list[SectorScore]
    recommendations: list[Recommendation]

    # ── convenience filters ────────────────────────────────────
    @property
    def leading_sectors(self) -> list[str]:
        return [s.sector for s in self.sector_rankings if s.tier == "Leading"]

    @property
    def lagging_sectors(self) -> list[str]:
        return [s.sector for s in self.sector_rankings if s.tier == "Lagging"]

    @property
    def buys(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.BUY]

    @property
    def sells(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.SELL]

    @property
    def reduces(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.REDUCE]

    @property
    def holds(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.HOLD]


# ═══════════════════════════════════════════════════════════════
#  RELATIVE-STRENGTH MATH
# ═══════════════════════════════════════════════════════════════

def _period_returns(prices: pd.DataFrame, period: int) -> pd.Series:
    """
    Percentage return over the last `period` trading days
    for every column in the price matrix.
    """
    if len(prices) < period:
        return pd.Series(dtype=float)
    current = prices.iloc[-1]
    past = prices.iloc[-period]
    safe = past.replace(0, float("nan"))
    return (current - past) / safe


def composite_rs_all(
    prices: pd.DataFrame,
    config: RotationConfig,
) -> tuple[pd.Series, dict[str, dict[int, float]]]:
    """
    Compute composite RS (vs benchmark) for every ticker in the matrix.

    Returns
    -------
    rs_series : Series[ticker → float], sorted descending.
    raw_returns : dict[ticker → {period: return}] for drill-down.
    """
    bench = config.benchmark
    if bench not in prices.columns:
        raise ValueError(f"Benchmark '{bench}' missing from price data.")

    tickers = [c for c in prices.columns if c != bench]
    composite = pd.Series(0.0, index=tickers)
    raw_returns: dict[str, dict[int, float]] = {t: {} for t in tickers}

    for period, weight in config.rs_periods.items():
        rets = _period_returns(prices, period)
        if rets.empty:
            log.warning("Not enough data for %d-day lookback, skipping.", period)
            continue
        bench_ret = rets.get(bench, 0.0)
        for t in tickers:
            t_ret = rets.get(t, float("nan"))
            raw_returns[t][period] = t_ret if pd.notna(t_ret) else 0.0
            excess = (t_ret - bench_ret) if pd.notna(t_ret) else 0.0
            composite[t] += weight * excess

    return composite.sort_values(ascending=False), raw_returns


def _rs_vs(
    prices: pd.DataFrame,
    ticker: str,
    versus: str,
    config: RotationConfig,
) -> float:
    """Single ticker's composite RS vs an arbitrary benchmark."""
    if ticker not in prices.columns or versus not in prices.columns:
        return 0.0
    score = 0.0
    for period, weight in config.rs_periods.items():
        rets = _period_returns(prices, period)
        if rets.empty:
            continue
        t_ret = rets.get(ticker, 0.0)
        b_ret = rets.get(versus, 0.0)
        t_ret = t_ret if pd.notna(t_ret) else 0.0
        b_ret = b_ret if pd.notna(b_ret) else 0.0
        score += weight * (t_ret - b_ret)
    return score


# ═══════════════════════════════════════════════════════════════
#  STEP 1 — RANK SECTORS
# ═══════════════════════════════════════════════════════════════

def _rank_sectors(
    prices: pd.DataFrame,
    config: RotationConfig,
) -> list[SectorScore]:
    """
    Score each sector ETF by composite RS vs SPY, assign rank & tier.
    """
    rs_all, raw = composite_rs_all(prices, config)

    scored: list[SectorScore] = []
    for sector, etf in SECTOR_ETFS.items():
        scored.append(SectorScore(
            sector=sector,
            etf=etf,
            composite_rs=rs_all.get(etf, 0.0),
            rank=0,
            tier="",
            period_returns=raw.get(etf, {}),
        ))

    scored.sort(key=lambda s: s.composite_rs, reverse=True)

    n_lead = config.n_leading_sectors
    n_lag = config.n_lagging_sectors
    for i, s in enumerate(scored):
        s.rank = i + 1
        if i < n_lead:
            s.tier = "Leading"
        elif i >= len(scored) - n_lag:
            s.tier = "Lagging"
        else:
            s.tier = "Neutral"

    return scored


# ═══════════════════════════════════════════════════════════════
#  STEP 2 — PICK STOCKS IN LEADING SECTORS
# ═══════════════════════════════════════════════════════════════

def _pick_stocks(
    leading: list[SectorScore],
    prices: pd.DataFrame,
    config: RotationConfig,
    rs_all: pd.Series,
) -> list[Recommendation]:
    """
    For each leading sector, rank US single names by RS and return
    the top N as BUY recommendations.

    Falls back to the sector ETF itself when no single names have data.
    """
    buys: list[Recommendation] = []

    for ss in leading:
        sector = ss.sector
        etf = ss.etf
        candidates = get_us_tickers_for_sector(sector)

        # keep only those present in the price matrix
        available = [t for t in candidates if t in prices.columns]

        if not available:
            # fallback → buy the sector ETF
            log.info("No single-name data for %s — falling back to %s", sector, etf)
            buys.append(Recommendation(
                ticker=etf,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs_all.get(etf, 0.0),
                rs_vs_sector_etf=0.0,
                reason=f"Sector ETF fallback (no single-name data for {sector})",
            ))
            continue

        # score each candidate
        scored: list[tuple[str, float, float]] = []
        for t in available:
            rs = rs_all.get(t, 0.0)
            rs_vs_etf = _rs_vs(prices, t, etf, config)

            # eligibility gates
            if rs < config.min_rs_score:
                continue
            if config.prefer_positive_rs_vs_sector and rs_vs_etf < 0:
                continue

            scored.append((t, rs, rs_vs_etf))

        # sort by composite RS descending
        scored.sort(key=lambda x: x[1], reverse=True)

        picks = scored[: config.stocks_per_sector]

        if not picks:
            # all candidates below threshold → fall back to ETF
            log.info("All candidates in %s below RS threshold — using %s", sector, etf)
            buys.append(Recommendation(
                ticker=etf,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs_all.get(etf, 0.0),
                rs_vs_sector_etf=0.0,
                reason=f"Sector ETF fallback (no candidates above RS threshold)",
            ))
            continue

        for ticker, rs, rs_vs_etf in picks:
            buys.append(Recommendation(
                ticker=ticker,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs,
                rs_vs_sector_etf=rs_vs_etf,
                reason=f"Top RS in leading sector {sector} (rank {ss.rank})",
            ))

    return buys


# ═══════════════════════════════════════════════════════════════
#  STEP 3 — EVALUATE CURRENT HOLDINGS
# ═══════════════════════════════════════════════════════════════

def _evaluate_holdings(
    holdings: list[str],
    sector_scores: list[SectorScore],
    prices: pd.DataFrame,
    config: RotationConfig,
    rs_all: pd.Series,
) -> list[Recommendation]:
    """
    Walk every current holding through the sell-rule waterfall.

    Priority
    --------
    1.  Sector → Lagging         → SELL
    2.  Sector → Neutral         → REDUCE
    3.  Individual RS too weak   → SELL
    4.  Otherwise                → HOLD
    """
    tier_map = {s.sector: s.tier for s in sector_scores}
    rank_map = {s.sector: s.rank for s in sector_scores}
    leading_set = {s.sector for s in sector_scores if s.tier == "Leading"}

    recs: list[Recommendation] = []

    for ticker in holdings:
        sector = get_sector(ticker)
        label = get_sector_or_class(ticker)

        sector_etf = SECTOR_ETFS.get(sector, config.benchmark) if sector else config.benchmark
        rs = rs_all.get(ticker, 0.0)
        rs_vs_etf = _rs_vs(prices, ticker, sector_etf, config)
        s_rank = rank_map.get(sector, 99) if sector else 99
        s_tier = tier_map.get(sector, "n/a") if sector else "n/a"

        # ── Rule 1 & 2: sector drift ──────────────────────────
        if config.sell_if_sector_not_leading and sector and sector not in leading_set:
            if s_tier == "Lagging":
                recs.append(Recommendation(
                    ticker=ticker, action=Action.SELL,
                    sector=label, sector_rank=s_rank, sector_tier=s_tier,
                    rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                    reason=f"Sector {sector} is Lagging (rank {s_rank}/11)",
                ))
            else:
                recs.append(Recommendation(
                    ticker=ticker, action=Action.REDUCE,
                    sector=label, sector_rank=s_rank, sector_tier=s_tier,
                    rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                    reason=f"Sector {sector} drifted to Neutral (rank {s_rank}/11)",
                ))
            continue

        # ── Rule 3: individual RS collapse ─────────────────────
        if rs < config.sell_individual_rs_below:
            recs.append(Recommendation(
                ticker=ticker, action=Action.SELL,
                sector=label, sector_rank=s_rank, sector_tier=s_tier,
                rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                reason=(f"Individual RS {rs:+.3f} below floor "
                        f"({config.sell_individual_rs_below:+.3f})"),
            ))
            continue

        # ── Rule 4: everything fine ────────────────────────────
        recs.append(Recommendation(
            ticker=ticker, action=Action.HOLD,
            sector=label, sector_rank=s_rank, sector_tier=s_tier,
            rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
            reason=f"Sector {sector} still Leading (rank {s_rank}), RS OK",
        ))

    return recs


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_rotation(
    prices: pd.DataFrame,
    current_holdings: list[str] | None = None,
    config: RotationConfig | None = None,
) -> RotationResult:
    """
    Run the full Smart Money Rotation.

    Parameters
    ----------
    prices : DataFrame
        DatetimeIndex, columns = tickers, values = adjusted close.
        Must include the benchmark, the 11 sector ETFs, and any
        single names you want considered.  Missing tickers are
        silently skipped.

    current_holdings : list[str] or None
        Tickers currently in the portfolio.  Pass [] or None for
        a fresh start (only BUY signals emitted).

    config : RotationConfig or None
        Override any defaults.  None → use RotationConfig().

    Returns
    -------
    RotationResult
        .sector_rankings  — ordered list of SectorScore
        .recommendations  — ordered list of Recommendation
        .buys / .sells / .reduces / .holds — convenience accessors
    """
    if config is None:
        config = RotationConfig()
    if current_holdings is None:
        current_holdings = []

    # ── Validate data depth ────────────────────────────────────
    if len(prices) < config.min_history_days:
        raise ValueError(
            f"Need ≥ {config.min_history_days} rows of price data, "
            f"got {len(prices)}."
        )

    as_of = (prices.index[-1].date()
             if hasattr(prices.index[-1], "date")
             else prices.index[-1])

    log.info("Rotation as-of %s  |  %d days × %d tickers",
             as_of, prices.shape[0], prices.shape[1])

    # ── Step 1: rank sectors ───────────────────────────────────
    sector_rankings = _rank_sectors(prices, config)
    leading = [s for s in sector_rankings if s.tier == "Leading"]

    log.info("Leading : %s", [s.sector for s in leading])
    log.info("Lagging : %s", [s.sector for s in sector_rankings if s.tier == "Lagging"])

    # Pre-compute RS for all tickers once (reused everywhere)
    rs_all, _ = composite_rs_all(prices, config)

    # ── Step 2: pick stocks in leading sectors ─────────────────
    raw_buys = _pick_stocks(leading, prices, config, rs_all)

    # Remove tickers already held (they'll appear as HOLD instead)
    held_set = set(current_holdings)
    new_buys = [r for r in raw_buys if r.ticker not in held_set]

    # ── Step 3: evaluate current holdings ──────────────────────
    hold_sell = _evaluate_holdings(
        holdings=current_holdings,
        sector_scores=sector_rankings,
        prices=prices,
        config=config,
        rs_all=rs_all,
    )

    # ── Step 4: enforce max-position cap ───────────────────────
    n_keeping = sum(1 for r in hold_sell if r.action in (Action.HOLD, Action.REDUCE))
    open_slots = max(0, config.max_total_positions - n_keeping)
    new_buys = new_buys[:open_slots]

    # ── Step 5: combine & sort ─────────────────────────────────
    # Order: SELL first (liquidate), then REDUCE, then BUY (deploy),
    # then HOLD.  Within each group, sort by RS descending.
    _action_order = {Action.SELL: 0, Action.REDUCE: 1, Action.BUY: 2, Action.HOLD: 3}

    all_recs = hold_sell + new_buys
    all_recs.sort(key=lambda r: (_action_order[r.action], -r.rs_composite))

    result = RotationResult(
        as_of_date=as_of,
        config=config,
        sector_rankings=sector_rankings,
        recommendations=all_recs,
    )

    log.info("Result  : %d SELL  %d REDUCE  %d BUY  %d HOLD",
             len(result.sells), len(result.reduces),
             len(result.buys), len(result.holds))

    return result


# ═══════════════════════════════════════════════════════════════
#  DATABASE CONVENIENCE  (adapt to your data layer)
# ═══════════════════════════════════════════════════════════════

def load_price_matrix(
    engine,
    tickers: list[str] | None = None,
    lookback_days: int = 200,
) -> pd.DataFrame:
    """
    Pull adjusted-close price matrix from the daily_prices table.

    Parameters
    ----------
    engine : sqlalchemy Engine (or connection).
    tickers : explicit list, or None → full ETF universe + single names.
    lookback_days : calendar days to fetch (not trading days).

    Returns
    -------
    DataFrame[DatetimeIndex, ticker columns], NaN-forward-filled.

    Adapt the SQL to match your schema.  This assumes a table with
    columns: date, symbol, adj_close.
    """
    from datetime import timedelta

    from common.universe import ETF_UNIVERSE, get_all_single_names

    if tickers is None:
        tickers = sorted(set(ETF_UNIVERSE + get_all_single_names()))

    placeholders = ", ".join([f"'{t}'" for t in tickers])
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = f"""
        SELECT date, symbol, adj_close
        FROM   daily_prices
        WHERE  symbol IN ({placeholders})
          AND  date >= '{cutoff}'
        ORDER  BY date
    """

    df = pd.read_sql(query, engine, parse_dates=["date"])
    matrix = df.pivot(index="date", columns="symbol", values="adj_close")
    matrix = matrix.sort_index().ffill()

    return matrix


# ═══════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═══════════════════════════════════════════════════════════════

def print_result(result: RotationResult) -> None:
    """Pretty-print the full rotation output to stdout."""

    w = 72
    print(f"\n{'═' * w}")
    print(f"  SMART MONEY ROTATION  —  {result.as_of_date}")
    print(f"{'═' * w}")

    # ── Sector Rankings ────────────────────────────────────────
    print(f"\n  SECTOR RANKINGS")
    print(f"  {'─' * (w - 4)}")
    for s in result.sector_rankings:
        icon = "🟢" if s.tier == "Leading" else ("🔴" if s.tier == "Lagging" else "⚪")
        rets_str = "  ".join(
            f"{p}d: {r:+.1%}" for p, r in sorted(s.period_returns.items())
        )
        print(f"  {icon} {s.rank:2d}. {s.sector:28s} [{s.etf:4s}]  "
              f"RS {s.composite_rs:+.4f}   {rets_str}")

    # ── Sell ───────────────────────────────────────────────────
    if result.sells:
        print(f"\n  🔴 SELL  ({len(result.sells)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.sells:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  │  {r.reason}")

    # ── Reduce ─────────────────────────────────────────────────
    if result.reduces:
        print(f"\n  🟡 REDUCE  ({len(result.reduces)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.reduces:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  │  {r.reason}")

    # ── Buy ────────────────────────────────────────────────────
    if result.buys:
        print(f"\n  🟢 BUY  ({len(result.buys)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.buys:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  "
                  f"vs-sector {r.rs_vs_sector_etf:+.4f}  │  {r.reason}")

    # ── Hold ───────────────────────────────────────────────────
    if result.holds:
        print(f"\n  ⚪ HOLD  ({len(result.holds)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.holds:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  │  {r.reason}")

    # ── Summary ────────────────────────────────────────────────
    print(f"\n  {'─' * (w - 4)}")
    total = len(result.recommendations)
    print(f"  Summary: {len(result.sells)} sell  {len(result.reduces)} reduce  "
          f"{len(result.buys)} buy  {len(result.holds)} hold  "
          f"({total} total)")
    print(f"  Leading sectors : {', '.join(result.leading_sectors)}")
    print(f"  Lagging sectors : {', '.join(result.lagging_sectors)}")
    print(f"{'═' * w}\n")


# ═══════════════════════════════════════════════════════════════
#  CLI / EXAMPLE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Example usage with a live database:

        from sqlalchemy import create_engine
        engine = create_engine("sqlite:///data/market.db")

        prices = load_price_matrix(engine, lookback_days=200)
        result = run_rotation(
            prices=prices,
            current_holdings=["NVDA", "CRWD", "CEG", "LMT", "AVGO"],
            config=RotationConfig(
                stocks_per_sector=3,
                max_total_positions=12,
            ),
        )
        print_result(result)
    """

    # ── Quick smoke test with synthetic data ───────────────────
    import numpy as np

    np.random.seed(42)
    n_days = 150

    all_tickers = (
        ["SPY"]
        + list(SECTOR_ETFS.values())
        + get_us_tickers_for_sector("Technology")[:5]
        + get_us_tickers_for_sector("Energy")[:3]
        + get_us_tickers_for_sector("Industrials")[:3]
    )
    all_tickers = sorted(set(all_tickers))

    dates = pd.bdate_range(end=date.today(), periods=n_days)
    fake_prices = pd.DataFrame(
        index=dates,
        columns=all_tickers,
        data=100 * np.cumprod(
            1 + np.random.normal(0.0003, 0.015, (n_days, len(all_tickers))),
            axis=0,
        ),
    )

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    result = run_rotation(
        prices=fake_prices,
        current_holdings=["NVDA", "CCJ", "LMT"],
    )
    print_result(result)


##############################################################
"""
strategy/signals.py
-------------------
Entry / exit signal generation.

Takes a scored, RS-enriched DataFrame for a single ticker and
produces a column set that tells the portfolio builder what to do.

Signals are *gated*: every candidate must pass a checklist of
conditions before it earns a ``sig_confirmed == 1`` flag.

Gates
─────
  1.  score_adjusted  ≥  entry_score_min          (quality bar)
  2.  rs_regime       ∈  allowed_rs_regimes       (stock trend)
  3.  sect_rs_regime  ∈  allowed_sector_regimes   (sector tide)
  4.  momentum_streak ≥  N consecutive days > 0.5 (persistence)
  5.  NOT in cooldown after recent exit            (anti-churn)
  6.  breadth_regime  ∈  allowed_breadth_regimes  (market gate)

Pipeline
────────
  scored_df  →  _gate_regime()
             →  _gate_sector()
             →  _gate_breadth()
             →  _gate_momentum()
             →  _gate_cooldown()
             →  _gate_entry()
             →  _compute_exits()
             →  _position_sizing()
             →  generate_signals()  ← master function

Each gate adds its own boolean column so downstream analytics
can diagnose *why* a signal was or wasn't generated.
 is the per-ticker quality filter. It runs on a single ticker's time series and 
 answers "is this ticker trade-worthy today?" through six gates (regime, sector, breadth, momentum, cooldown, score). 
 Its output is sig_confirmed, sig_exit, and all the sig_* diagnostic columns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SIGNAL_PARAMS, BREADTH_PORTFOLIO


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _sp(key: str):
    return SIGNAL_PARAMS[key]


def _bpp(key: str):
    return BREADTH_PORTFOLIO[key]


# ═══════════════════════════════════════════════════════════════
#  GATE 1 — STOCK RS REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_regime(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    allowed = _sp("allowed_rs_regimes")

    if "rs_regime" in result.columns:
        result["sig_regime_ok"] = result["rs_regime"].isin(allowed)
    else:
        result["sig_regime_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 2 — SECTOR REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_sector(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    allowed = _sp("allowed_sector_regimes")

    if "sect_rs_regime" in result.columns:
        result["sig_sector_ok"] = (
            result["sect_rs_regime"].isin(allowed)
        )
    else:
        result["sig_sector_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 3 — BREADTH REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_breadth(
    df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Market-level breadth gate.

    When breadth data is provided, this gate:
      - Merges breadth_regime and breadth_score onto the
        ticker's DataFrame by date.
      - Sets ``sig_breadth_ok`` = True when breadth_regime
        is NOT weak (or when ``weak_block_new`` is False).
      - Adjusts the effective entry threshold upward in
        neutral / weak regimes.

    Without breadth data the gate passes unconditionally.
    """
    result = df.copy()

    if breadth is None or breadth.empty:
        result["sig_breadth_ok"]       = True
        result["breadth_regime"]       = "unknown"
        result["breadth_score"]        = np.nan
        result["entry_score_adj"]      = 0.0
        return result

    # ── Merge breadth onto ticker dates ───────────────────────
    breadth_cols = ["breadth_regime", "breadth_score", "breadth_score_smooth"]
    available    = [c for c in breadth_cols if c in breadth.columns]

    if not available:
        result["sig_breadth_ok"]  = True
        result["breadth_regime"]  = "unknown"
        result["breadth_score"]   = np.nan
        result["entry_score_adj"] = 0.0
        return result

    bdata = breadth[available].copy()

    # Align on date index
    result = result.join(bdata, how="left")

    # Forward-fill breadth regime for any gaps
    if "breadth_regime" in result.columns:
        result["breadth_regime"] = (
            result["breadth_regime"].ffill().fillna("unknown")
        )
    else:
        result["breadth_regime"] = "unknown"

    if "breadth_score" in result.columns:
        result["breadth_score"] = result["breadth_score"].ffill()

    # ── Set gate ──────────────────────────────────────────────
    block_new = _bpp("weak_block_new")

    if block_new:
        result["sig_breadth_ok"] = (
            result["breadth_regime"] != "weak"
        )
    else:
        result["sig_breadth_ok"] = True

    # ── Entry threshold adjustment ────────────────────────────
    weak_raise    = _bpp("weak_raise_entry")
    neutral_raise = _bpp("neutral_raise_entry")

    conditions = [
        result["breadth_regime"] == "weak",
        result["breadth_regime"] == "neutral",
    ]
    choices = [weak_raise, neutral_raise]

    result["entry_score_adj"] = np.select(
        conditions, choices, default=0.0
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 4 — MOMENTUM PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def _gate_momentum(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    streak = _sp("confirmation_streak")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_momentum_ok"] = False
        return result

    above = result[score_col] >= 0.50
    cumsum = above.cumsum()
    reset  = cumsum - cumsum.where(~above).ffill().fillna(0)

    result["sig_momentum_streak"] = reset.astype(int)
    result["sig_momentum_ok"]     = reset >= streak

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 5 — COOLDOWN
# ═══════════════════════════════════════════════════════════════

def _gate_cooldown(df: pd.DataFrame) -> pd.DataFrame:
    result   = df.copy()
    cooldown = _sp("cooldown_days")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_in_cooldown"] = False
        return result

    exit_thresh = _sp("exit_score_max")

    was_above = (result[score_col].shift(1) >= exit_thresh)
    now_below = (result[score_col] < exit_thresh)
    exit_event = was_above & now_below

    cooldown_remaining = pd.Series(0, index=result.index, dtype=int)
    counter = 0
    for i in range(len(result)):
        if exit_event.iloc[i]:
            counter = cooldown
        elif counter > 0:
            counter -= 1
        cooldown_remaining.iloc[i] = counter

    result["sig_in_cooldown"]       = cooldown_remaining > 0
    result["sig_cooldown_remaining"] = cooldown_remaining

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 6 — ENTRY CONFIRMATION
# ═══════════════════════════════════════════════════════════════

def _gate_entry(df: pd.DataFrame) -> pd.DataFrame:
    result    = df.copy()
    base_min  = _sp("entry_score_min")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_confirmed"] = 0
        result["sig_reason"]    = "no score"
        return result

    # ── Effective entry threshold (breadth-adjusted) ──────────
    entry_adj = result.get("entry_score_adj", 0.0)
    if isinstance(entry_adj, (int, float)):
        entry_adj = pd.Series(entry_adj, index=result.index)
    effective_min = base_min + entry_adj.fillna(0.0)

    result["sig_effective_entry_min"] = effective_min

    scores = result[score_col]

    # All gates must pass
    regime_ok   = result.get("sig_regime_ok", True)
    sector_ok   = result.get("sig_sector_ok", True)
    breadth_ok  = result.get("sig_breadth_ok", True)
    momentum_ok = result.get("sig_momentum_ok", False)
    cooldown    = result.get("sig_in_cooldown", False)

    confirmed = (
        (scores >= effective_min)
        & regime_ok
        & sector_ok
        & breadth_ok
        & momentum_ok
        & (~cooldown)
    )

    result["sig_confirmed"] = confirmed.astype(int)

    # ── Reason annotation ─────────────────────────────────────
    reasons = pd.Series("", index=result.index)

    reasons = reasons.where(
        confirmed,
        np.where(
            ~regime_ok.astype(bool) if not isinstance(
                regime_ok, bool
            ) else ~regime_ok,
            "regime_blocked",
            np.where(
                ~sector_ok.astype(bool) if not isinstance(
                    sector_ok, bool
                ) else ~sector_ok,
                "sector_blocked",
                np.where(
                    ~breadth_ok.astype(bool) if not isinstance(
                        breadth_ok, bool
                    ) else ~breadth_ok,
                    "breadth_weak",
                    np.where(
                        cooldown.astype(bool) if not isinstance(
                            cooldown, bool
                        ) else cooldown,
                        "cooldown",
                        np.where(
                            ~momentum_ok.astype(bool) if not isinstance(
                                momentum_ok, bool
                            ) else ~momentum_ok,
                            "momentum_unconfirmed",
                            np.where(
                                scores < effective_min,
                                "score_below_entry",
                                "no_signal",
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )

    reasons = reasons.where(~confirmed, "LONG")
    result["sig_reason"] = reasons

    return result


# ═══════════════════════════════════════════════════════════════
#  EXIT SIGNALS
# ═══════════════════════════════════════════════════════════════

def _compute_exits(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    exit_max = _sp("exit_score_max")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_exit"] = 0
        return result

    was_confirmed = result["sig_confirmed"].shift(1).fillna(0) == 1
    now_weak      = result[score_col] < exit_max

    result["sig_exit"] = (was_confirmed & now_weak).astype(int)

    # Also flag if breadth turned weak on an existing position
    if "breadth_regime" in result.columns:
        breadth_was_ok  = result["sig_breadth_ok"].shift(1, fill_value=True)
        breadth_now_bad = result["breadth_regime"] == "weak"
        result["sig_exit_breadth"] = (
            was_confirmed & breadth_now_bad & breadth_was_ok
        ).astype(int)
    else:
        result["sig_exit_breadth"] = 0

    return result


# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING
# ═══════════════════════════════════════════════════════════════

def _position_sizing(df: pd.DataFrame) -> pd.DataFrame:
    result   = df.copy()
    base     = _sp("base_position_pct")
    max_pos  = _sp("max_position_pct")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_position_pct"] = 0.0
        return result

    scores = result[score_col].fillna(0)

    # Scale linearly from base → max as score goes 0.6 → 1.0
    low, high = 0.60, 1.0
    frac = ((scores - low) / (high - low)).clip(0, 1)
    raw  = base + frac * (max_pos - base)

    # ── Breadth-based scaling ─────────────────────────────────
    # In neutral/weak breadth, scale down position sizes
    if "breadth_regime" in result.columns:
        breadth_scale = result["breadth_regime"].map({
            "strong":  1.0,
            "neutral": _bpp("neutral_exposure"),
            "weak":    _bpp("weak_exposure"),
        }).fillna(1.0)
    else:
        breadth_scale = 1.0

    raw = raw * breadth_scale

    result["sig_position_pct"] = np.where(
        result["sig_confirmed"] == 1, raw, 0.0,
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_signals(
    df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Run the full signal pipeline on a single ticker's scored
    DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Output of the scoring pipeline, with RS and sector RS
        columns already merged.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()``.  When provided,
        breadth regime gates and position-size scaling are active.

    Returns
    -------
    pd.DataFrame
        Original columns plus all ``sig_*`` columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    result = _gate_regime(df)
    result = _gate_sector(result)
    result = _gate_breadth(result, breadth)
    result = _gate_momentum(result)
    result = _gate_cooldown(result)
    result = _gate_entry(result)
    result = _compute_exits(result)
    result = _position_sizing(result)

    return result

#######################################################################################
"""
UTILS:
-----------
"""
"""
utils/run_logger.py
═══════════════════
Rich-console logger that mirrors output to terminal AND saves a
styled HTML log file to ``logs/``.

Usage
─────
    from utils.run_logger import RunLogger

    log = RunLogger("test_portfolio")   # creates timestamped HTML log
    log.h1("STEP 1: Fetching data")
    log.print("Benchmark (SPY): 501 rows")
    log.success("All tickers loaded")
    log.warning("Sector XLE has < 100 rows")
    log.error("Something went wrong", exc_info=True)
    log.table(title="Portfolio", columns=[...], rows=[...])
    log.kv("Breadth regime", "strong")
    log.divider()
    log.save()                          # writes HTML to logs/
"""

from __future__ import annotations

import datetime as dt
import re
import traceback as tb
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
import io

from common.config import LOGS_DIR


class RunLogger:
    """Dual-output logger: live terminal + recorded HTML file."""

    def __init__(self, run_name: str = "run", width: int = 120):
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filename = f"{run_name}_{ts}.html"
        self.filepath = LOGS_DIR / self.filename
        self.run_name = run_name
        self.started = dt.datetime.now()

        # Terminal console (live output)
        self._term = Console(width=width, force_terminal=True)
        # Recording console (captures everything for HTML export)
        self._rec = Console(
            width=width, record=True, force_terminal=True, file=io.StringIO(),
        )

    # ── Primitives ────────────────────────────────────────────

    def _echo(self, *args, **kwargs):
        """Print to both terminal and recording console."""
        self._term.print(*args, **kwargs)
        self._rec.print(*args, **kwargs)

    def print(self, *args, **kwargs):
        """Plain print (supports rich markup like [bold], [green], etc.)."""
        self._echo(*args, **kwargs)

    def log(self, *args, **kwargs):
        """Print with automatic timestamp prefix."""
        self._term.log(*args, **kwargs)
        self._rec.log(*args, **kwargs)

    # ── Headings ──────────────────────────────────────────────

    def h1(self, title: str):
        """Major section heading."""
        self._echo()
        self._echo(Panel(
            Text(title, style="bold white"),
            border_style="bright_cyan",
            box=box.DOUBLE_EDGE,
            expand=True,
        ))

    def h2(self, title: str):
        """Sub-section heading."""
        self._echo()
        self._echo(f"[bold bright_yellow]── {title} ──[/]")

    def divider(self, style: str = "dim"):
        """Horizontal rule."""
        self._echo(f"[{style}]{'─' * 80}[/]")

    # ── Semantic helpers ──────────────────────────────────────

    def success(self, msg: str):
        self._echo(f"  [bold green]✓[/] {msg}")

    def warning(self, msg: str):
        self._echo(f"  [bold yellow]⚠[/] {msg}")

    def error(self, msg: str, exc_info: bool = False):
        self._echo(f"  [bold red]✗[/] {msg}")
        if exc_info:
            txt = tb.format_exc()
            self._echo(f"[dim red]{txt}[/]")

    def info(self, msg: str):
        self._echo(f"  [dim cyan]ℹ[/] {msg}")

    def kv(self, key: str, value, key_width: int = 22):
        """Key-value pair, neatly aligned."""
        self._echo(
            f"  [bold]{key + ':':<{key_width}}[/] {value}"
        )

    def bullet(self, msg: str, style: str = "green"):
        """Bullet point."""
        self._echo(f"  [{style}]•[/] {msg}")

    # ── Sector / regime badges ────────────────────────────────

    def regime_badge(self, regime: str) -> str:
        """Return a rich-markup badge for a regime string."""
        badges = {
            "leading":   "[bold green]🟢 leading[/]",
            "weakening": "[bold yellow]🟡 weakening[/]",
            "improving": "[bold blue]🔵 improving[/]",
            "lagging":   "[bold red]🔴 lagging[/]",
            "strong":    "[bold green]● strong[/]",
            "neutral":   "[bold yellow]● neutral[/]",
            "weak":      "[bold red]● weak[/]",
            "crisis":    "[bold magenta]● crisis[/]",
        }
        return badges.get(regime, f"[dim]{regime}[/]")

    # ── Tables ────────────────────────────────────────────────

    def table(
        self,
        title: str,
        columns: list[dict],
        rows: list[list],
        box_style=box.SIMPLE_HEAVY,
    ):
        """
        Render a rich table.

        Parameters
        ----------
        columns : list of dict
            Each dict has keys: ``header``, and optionally
            ``justify`` ("left"/"right"/"center"), ``style``.
        rows : list of list
            Each inner list is one row of cell values (str).
        """
        tbl = Table(title=title, box=box_style, show_lines=False)
        for col in columns:
            tbl.add_column(
                col["header"],
                justify=col.get("justify", "left"),
                style=col.get("style", ""),
            )
        for row in rows:
            tbl.add_row(*[str(c) for c in row])
        self._echo(tbl)

    def sector_rankings(self, sector_rs_latest: list[dict]):
        """
        Pretty-print sector rankings.

        Expects list of dicts with keys:
        sector, rank, pctrank, regime
        """
        cols = [
            {"header": "Sector", "style": "bold"},
            {"header": "PctRank", "justify": "right"},
            {"header": "Regime", "justify": "center"},
        ]
        rows = []
        for s in sector_rs_latest:
            regime = s.get("regime", "")
            badge = self.regime_badge(regime)
            rows.append([
                s["sector"],
                f"{s.get('pctrank', 0):.2f}",
                badge,
            ])
        self.table("Sector Relative Strength", cols, rows)

    def portfolio_table(self, holdings: list[dict]):
        """
        Pretty-print portfolio holdings.

        Expects list of dicts with keys:
        ticker, weight, sector, score, signal
        """
        if not holdings:
            self.warning("No holdings")
            return

        cols = [
            {"header": "Ticker", "style": "bold cyan"},
            {"header": "Weight", "justify": "right"},
            {"header": "Sector"},
            {"header": "Score", "justify": "right"},
            {"header": "Signal", "justify": "center"},
        ]
        rows = []
        for h in holdings:
            signal = h.get("signal", "")
            if signal == "BUY":
                sig_styled = "[bold green]BUY[/]"
            elif signal == "SELL":
                sig_styled = "[bold red]SELL[/]"
            else:
                sig_styled = signal
            rows.append([
                h["ticker"],
                f"{h.get('weight', 0):.1%}",
                h.get("sector", ""),
                f"{h.get('score', 0):.3f}",
                sig_styled,
            ])
        self.table("Portfolio Holdings", cols, rows)

    def rebalance_table(self, actions: list[dict]):
        """
        Pretty-print rebalance actions.

        Expects list of dicts with keys:
        ticker, current_weight, target_weight, delta, action
        """
        cols = [
            {"header": "Ticker", "style": "bold"},
            {"header": "Current", "justify": "right"},
            {"header": "Target", "justify": "right"},
            {"header": "Delta", "justify": "right"},
            {"header": "Action", "justify": "center"},
        ]
        rows = []
        for a in actions:
            action = a.get("action", "")
            if action == "BUY":
                act_styled = "[bold green]BUY[/]"
            elif action == "SELL":
                act_styled = "[bold red]SELL[/]"
            elif action == "REDUCE":
                act_styled = "[bold yellow]REDUCE[/]"
            elif action == "TRIM":
                act_styled = "[bold yellow]TRIM[/]"
            elif action == "ADD":
                act_styled = "[bold cyan]ADD[/]"
            else:
                act_styled = action
            rows.append([
                a["ticker"],
                f"{a.get('current_weight', 0):.1%}",
                f"{a.get('target_weight', 0):.1%}",
                f"{a.get('delta', 0):+.1%}",
                act_styled,
            ])
        self.table("Rebalance Orders", cols, rows)

    # ── Breadth summary ───────────────────────────────────────

    def breadth_summary(self, breadth: dict):
        """
        Pretty-print breadth data from a breadth dict/row.
        """
        self.h2("Market Breadth")
        regime = breadth.get("regime", "unknown")
        self.kv("Regime", self.regime_badge(regime))
        self.kv("Breadth score", f"{breadth.get('score', 0):.3f}")
        self.kv("Smooth score", f"{breadth.get('score_smooth', 0):.3f}")
        self.kv("A-D line", breadth.get("ad_line", "N/A"))
        self.kv("% above 50d", f"{breadth.get('pct_above_50', 0):.1%}")
        self.kv("% above 200d", f"{breadth.get('pct_above_200', 0):.1%}")
        thrust = breadth.get("thrust_active", False)
        if thrust:
            self.kv("Thrust", "[bold magenta]⚡ ACTIVE[/]")
        else:
            self.kv("Thrust", "[dim]inactive[/]")

    # ── Save / export ─────────────────────────────────────────

    def save(self) -> Path:
        """
        Write the recorded output to an HTML file and return the path.
        """
        elapsed = dt.datetime.now() - self.started
        self._echo()
        self.divider()
        self._echo(
            f"[dim]Run completed in {elapsed.total_seconds():.1f}s  •  "
            f"Log saved to {self.filepath.name}[/]"
        )

        raw_html = self._rec.export_html(
            inline_styles=True,
            theme=_DARK_THEME,
        )

        # ── Extract just the <pre>...</pre> block ─────────────
        # Rich's export_html() returns a full HTML document.
        # We only want the <pre><code>...</code></pre> fragment
        # to embed inside our own styled wrapper.
        match = re.search(
            r"(<pre\b.*?</pre>)", raw_html, re.DOTALL
        )
        if match:
            body_fragment = match.group(1)
        else:
            # Fallback: strip outer html/head/body tags manually
            body_fragment = raw_html
            for tag in (
                "<!DOCTYPE html>", "<html>", "</html>",
                "<head>", "</head>", "<body>", "</body>",
            ):
                body_fragment = body_fragment.replace(tag, "")
            # Also strip any <meta> and <style> blocks
            body_fragment = re.sub(
                r"<meta[^>]*>", "", body_fragment
            )
            body_fragment = re.sub(
                r"<style>.*?</style>", "", body_fragment,
                flags=re.DOTALL,
            )

        full_html = _HTML_WRAPPER.format(
            title=f"SMR – {self.run_name}",
            timestamp=self.started.strftime("%Y-%m-%d %H:%M:%S"),
            elapsed=f"{elapsed.total_seconds():.1f}s",
            body=body_fragment,
        )

        self.filepath.write_text(full_html, encoding="utf-8")
        return self.filepath


# ── HTML template ─────────────────────────────────────────────

_HTML_WRAPPER = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  body {{
    background: #1a1b26;
    color: #c0caf5;
    font-family: 'Cascadia Code', 'Fira Code', 'JetBrains Mono',
                 'Consolas', monospace;
    font-size: 14px;
    line-height: 1.5;
    padding: 24px 32px;
    max-width: 1200px;
    margin: 0 auto;
  }}
  .header {{
    border-bottom: 2px solid #3b4261;
    padding-bottom: 12px;
    margin-bottom: 24px;
    color: #7aa2f7;
  }}
  .header h1 {{ margin: 0; font-size: 20px; }}
  .header .meta {{ color: #565f89; font-size: 12px; margin-top: 4px; }}
  pre {{
    background: transparent !important;
    color: inherit !important;
    margin: 0;
    padding: 0;
    white-space: pre-wrap;
    word-wrap: break-word;
  }}
  code {{
    font-family: inherit;
    background: transparent !important;
  }}
  ::selection {{
    background: #33467c;
    color: #c0caf5;
  }}
</style>
</head>
<body>
<div class="header">
  <h1>🔄 Smart Money Rotation</h1>
  <div class="meta">{timestamp}  •  elapsed {elapsed}</div>
</div>
{body}
</body>
</html>
"""

# Rich's built-in MONOKAI theme works well; override if desired
from rich.terminal_theme import MONOKAI as _DARK_THEME

####################################################################


"""
main.py
CASH — Composite Adaptive Signal Hierarchy
===========================================

Entry point.  One command → full pipeline → reports on disk.

Usage:
    python main.py                                # default run
    python main.py --portfolio 150000             # custom capital
    python main.py --positions positions.json     # with holdings
    python main.py --output-dir reports/          # custom output
    python main.py --text-only                    # skip HTML
    python main.py --tickers AAPL MSFT NVDA       # specific tickers
    python main.py --universe universes/core.json # custom universe
    python main.py --dry-run                      # score only
    python main.py --backtest                     # include backtest
    python main.py --verbose                      # debug logging
"""

import argparse
import json
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ── CASH modules ────────────────────────────────────────────────
from common.config import (
    UNIVERSE,
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    LOGS_DIR,
)
from pipeline.orchestrator import (
    Orchestrator,
    PipelineResult,
    run_full_pipeline,
)
from pipeline.runner import results_errors
from reports.recommendations import (
    build_report,
    to_text,
    to_html,
    save_text,
    save_html,
    print_report,
)

# Optional: portfolio rebalance view
try:
    from reports.portfolio_view import (
        build_rebalance_plan,
        save_rebalance_text,
        save_rebalance_html,
        print_rebalance,
    )
    _HAS_REBALANCE = True
except ImportError:
    _HAS_REBALANCE = False


# ═════════════════════════════════════════════════════════════════
#  LOGGING
# ═════════════════════════════════════════════════════════════════

def setup_logging(
    verbose: bool = False,
    log_file: str | None = None,
) -> str:
    """
    Configure root logger.

    If *log_file* is ``None`` the log is written to ``LOGS_DIR``
    (from ``common.config``) with a timestamped filename.

    Returns the resolved log-file path.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s"
    datefmt = "%H:%M:%S"

    # Ensure LOGS_DIR exists
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)

    # Default log location: LOGS_DIR/cash_<timestamp>.log
    if log_file is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(logs_path / f"cash_{ts}.log")
    else:
        # Relative paths resolve inside LOGS_DIR
        lf = Path(log_file)
        if not lf.is_absolute():
            log_file = str(logs_path / lf)

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="w"),
    ]

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
        force=True,
    )
    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    return log_file


logger = logging.getLogger("cash.main")


# ═════════════════════════════════════════════════════════════════
#  CLI ARGUMENT PARSER
# ═════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cash",
        description="CASH — Composite Adaptive Signal Hierarchy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --portfolio 200000 --positions holdings.json
  python main.py --dry-run --verbose
  python main.py --tickers AAPL MSFT NVDA GOOG
  python main.py --output-dir ~/reports --text-only
        """,
    )

    # ── portfolio ───────────────────────────────────────────────
    p.add_argument(
        "--portfolio", "-p", type=float, default=None,
        help="Total portfolio value in dollars (overrides config)",
    )
    p.add_argument(
        "--positions", type=str, default=None,
        help="Path to JSON file with current holdings "
             "(enables rebalance report)",
    )

    # ── universe ────────────────────────────────────────────────
    p.add_argument(
        "--universe", "-u", type=str, default=None,
        help="Path to universe JSON file (list of tickers)",
    )
    p.add_argument(
        "--tickers", "-t", type=str, nargs="+", default=None,
        help="Run on specific tickers only (space-separated)",
    )

    # ── output ──────────────────────────────────────────────────
    p.add_argument(
        "--output-dir", "-o", type=str, default="output",
        help="Directory for report files (default: output/)",
    )
    p.add_argument(
        "--text-only", action="store_true",
        help="Generate text reports only, skip HTML",
    )
    p.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress terminal output (files still saved)",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Also save structured report as JSON",
    )

    # ── run mode ────────────────────────────────────────────────
    p.add_argument(
        "--dry-run", action="store_true",
        help="Score and rank only — no portfolio or signals",
    )
    p.add_argument(
        "--backtest", action="store_true",
        help="Run historical backtest after pipeline",
    )
    p.add_argument(
        "--top-n", type=int, default=None,
        help="Only show top N buy candidates in output",
    )

    # ── debug ───────────────────────────────────────────────────
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--log-file", type=str, default=None,
        help="Custom log filename (written inside LOGS_DIR)",
    )

    return p


# ═════════════════════════════════════════════════════════════════
#  POSITION LOADING
# ═════════════════════════════════════════════════════════════════

def load_positions(
    filepath: str,
) -> tuple[list[dict], float | None]:
    """
    Load current holdings from a JSON file.

    Supports two formats:

    Plain list::

        [
            {"ticker": "AAPL", "shares": 50,
             "avg_cost": 142.30, "current_price": 178.50},
            ...
        ]

    Wrapper with cash::

        {
            "positions": [ ... ],
            "cash": 25000.0
        }

    Returns ``(positions_list, cash_or_None)``.
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Positions file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        positions = data.get("positions", [])
        cash = data.get("cash", None)
    elif isinstance(data, list):
        positions = data
        cash = None
    else:
        logger.error(
            f"Unexpected positions format in {filepath}"
        )
        sys.exit(1)

    required = {"ticker", "shares", "avg_cost", "current_price"}
    for i, pos in enumerate(positions):
        missing = required - set(pos.keys())
        if missing:
            logger.error(
                f"Position {i} ({pos.get('ticker', '?')}) "
                f"missing fields: {missing}"
            )
            sys.exit(1)

    logger.info(
        f"Loaded {len(positions)} positions from {filepath}"
    )
    if cash is not None:
        logger.info(f"Cash from positions file: ${cash:,.0f}")

    return positions, cash


def load_universe_file(filepath: str) -> list[str]:
    """
    Load a universe JSON file and return ticker strings.

    Accepts a plain list of strings::

        ["AAPL", "MSFT", "NVDA"]

    or a list of objects with a ``"ticker"`` key::

        [{"ticker": "AAPL", "category": "Tech"}, ...]
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Universe file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    if not isinstance(data, list) or len(data) == 0:
        logger.error("Universe file must be a non-empty list")
        sys.exit(1)

    if isinstance(data[0], str):
        return [t.upper() for t in data]
    elif isinstance(data[0], dict) and "ticker" in data[0]:
        return [d["ticker"].upper() for d in data]
    else:
        logger.error(
            "Universe entries must be strings or dicts "
            "with a 'ticker' key"
        )
        sys.exit(1)


# ═════════════════════════════════════════════════════════════════
#  OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def ensure_output_dir(output_dir: str) -> Path:
    """Create the output directory if it doesn't exist."""
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_filenames(
    output_dir: Path, date_str: str,
) -> dict:
    """Generate timestamped filenames for all output files."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "report_txt":     output_dir / f"cash_report_{date_str}_{ts}.txt",
        "report_html":    output_dir / f"cash_report_{date_str}_{ts}.html",
        "report_json":    output_dir / f"cash_report_{date_str}_{ts}.json",
        "rebalance_txt":  output_dir / f"cash_rebalance_{date_str}_{ts}.txt",
        "rebalance_html": output_dir / f"cash_rebalance_{date_str}_{ts}.html",
        "rebalance_json": output_dir / f"cash_rebalance_{date_str}_{ts}.json",
        "pipeline_json":  output_dir / f"cash_pipeline_{date_str}_{ts}.json",
    }


# ═════════════════════════════════════════════════════════════════
#  DRY-RUN HELPERS
# ═════════════════════════════════════════════════════════════════

def _run_dry(
    tickers: list[str],
    capital: float,
) -> PipelineResult:
    """
    Execute Phases 0–2 only (load → breadth/sector → score).

    Skips portfolio construction, signal generation, and
    report building.  Useful for inspecting raw scores.
    """
    orch = Orchestrator(
        universe=tickers,
        capital=capital,
        enable_breadth=True,
        enable_sectors=True,
        enable_signals=False,
        enable_backtest=False,
    )
    orch.load_data()
    orch.compute_universe_context()
    orch.run_tickers()

    errors = results_errors(orch._ticker_results)

    return PipelineResult(
        ticker_results=orch._ticker_results,
        scored_universe=orch._scored_universe,
        snapshots=orch._snapshots,
        breadth=orch._breadth,
        breadth_scores=orch._breadth_scores,
        sector_rs=orch._sector_rs,
        bench_df=orch._bench_df,                    # ← NEW
        errors=errors,
        timings=orch._timings,
        run_date=pd.Timestamp.now(),
    )


def _print_dry_run_summary(
    result: PipelineResult,
    top_n: int = 20,
) -> None:
    """Print a compact scoring table for dry-run mode."""
    snaps = result.snapshots[:top_n]
    if not snaps:
        print("  No scored tickers.")
        return

    print()
    print("  DRY RUN — Top Scored Tickers")
    print("  " + "─" * 58)
    print(
        f"  {'Rank':<5} {'Ticker':<8} {'Composite':>10} "
        f"{'Signal':<10} {'Close':>10}"
    )
    print("  " + "─" * 58)

    for i, s in enumerate(snaps, 1):
        ticker = s.get("ticker", "???")
        score = s.get("composite", 0)
        signal = s.get("signal", "—")
        close = s.get("close", 0)
        print(
            f"  {i:<5} {ticker:<8} {score:>10.1f} "
            f"{signal:<10} {close:>10.2f}"
        )

    print("  " + "─" * 58)
    print(
        f"  {result.n_tickers} scored, "
        f"{result.n_errors} errors"
    )
    print()


# ═════════════════════════════════════════════════════════════════
#  REBALANCE HANDLER
# ═════════════════════════════════════════════════════════════════

def _handle_rebalance(
    args: argparse.Namespace,
    report: dict,
    current_positions: list[dict],
    positions_cash: float | None,
    capital: float,
    filenames: dict,
) -> None:
    """Build and save the rebalance plan."""
    logger.info("")
    logger.info(
        "─── BUILDING REBALANCE PLAN ───────────────────────"
    )

    cash_for_rebalance = (
        positions_cash
        if positions_cash is not None
        else capital * 0.10
    )

    try:
        plan = build_rebalance_plan(
            report=report,
            current_positions=current_positions,
            cash_balance=cash_for_rebalance,
            portfolio_value=capital,
        )
    except Exception as e:
        logger.warning(f"build_rebalance_plan failed: {e}")
        return

    # Terminal
    if not args.quiet:
        try:
            print()
            print_rebalance(plan)
        except Exception as e:
            logger.warning(f"print_rebalance failed: {e}")

    # Text
    try:
        save_rebalance_text(
            plan, str(filenames["rebalance_txt"])
        )
        logger.info(
            f"Rebalance text → {filenames['rebalance_txt']}"
        )
    except Exception as e:
        logger.warning(f"save_rebalance_text failed: {e}")

    # HTML
    if not args.text_only:
        try:
            save_rebalance_html(
                plan, str(filenames["rebalance_html"])
            )
            logger.info(
                f"Rebalance HTML → {filenames['rebalance_html']}"
            )
        except Exception as e:
            logger.warning(
                f"save_rebalance_html failed: {e}"
            )

    # JSON
    if args.json and hasattr(plan, "to_dict"):
        _save_json(
            plan.to_dict(),
            str(filenames["rebalance_json"]),
        )
        logger.info(
            f"Rebalance JSON → {filenames['rebalance_json']}"
        )

    # Summary log
    if hasattr(plan, "trade_count"):
        logger.info(f"Trades required: {plan.trade_count}")
    if hasattr(plan, "net_cash_impact"):
        logger.info(
            f"Net cash impact: ${plan.net_cash_impact:+,.0f}"
        )
    if hasattr(plan, "warnings") and plan.warnings:
        for w in plan.warnings:
            logger.warning(w)


# ═════════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ═════════════════════════════════════════════════════════════════

def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── setup ───────────────────────────────────────────────────
    output_dir = ensure_output_dir(args.output_dir)
    date_str = datetime.now().strftime("%Y%m%d")
    filenames = generate_filenames(output_dir, date_str)

    log_file = setup_logging(
        verbose=args.verbose,
        log_file=args.log_file,
    )

    logger.info("=" * 60)
    logger.info("  CASH — Composite Adaptive Signal Hierarchy")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)

    t_start = time.time()

    # ── capital ─────────────────────────────────────────────────
    capital = args.portfolio or PORTFOLIO_PARAMS.get(
        "total_capital", 100_000
    )
    logger.info(f"Portfolio value: ${capital:,.0f}")

    # ── universe ────────────────────────────────────────────────
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
        logger.info(f"CLI tickers: {', '.join(tickers)}")
    elif args.universe:
        tickers = load_universe_file(args.universe)
        logger.info(
            f"Universe from {args.universe}: "
            f"{len(tickers)} tickers"
        )
    else:
        tickers = list(UNIVERSE)
        logger.info(
            f"Default universe: {len(tickers)} tickers"
        )

    # ── load positions (optional) ───────────────────────────────
    current_positions = None
    positions_cash = None
    if args.positions:
        current_positions, positions_cash = load_positions(
            args.positions
        )

    # ── run pipeline ────────────────────────────────────────────
    logger.info("")
    logger.info(
        "─── RUNNING PIPELINE ──────────────────────────────"
    )

    if args.dry_run:
        logger.info(
            "DRY RUN — scoring only, no portfolio/signals"
        )
        result = _run_dry(tickers, capital)
    else:
        result = run_full_pipeline(
            universe=tickers,
            capital=capital,
            enable_breadth=True,
            enable_sectors=True,
            enable_signals=True,
            enable_backtest=args.backtest,
        )

    t_pipeline = time.time()
    logger.info(
        f"Pipeline completed in {t_pipeline - t_start:.1f}s"
    )
    logger.info(result.summary())

    # ── terminal summary ────────────────────────────────────────
    if not args.quiet:
        print()
        print(result.summary())

    # ── dry-run: print scores and exit ──────────────────────────
    if args.dry_run:
        if not args.quiet:
            _print_dry_run_summary(
                result, top_n=args.top_n or 20
            )
        _finish(t_start, output_dir, log_file)
        return result

    # ── reports ─────────────────────────────────────────────────
    logger.info("")
    logger.info(
        "─── SAVING REPORTS ────────────────────────────────"
    )

    # The orchestrator already calls build_report() internally
    # and stores the result in PipelineResult.recommendation_report
    report = result.recommendation_report

    if report is None:
        logger.warning(
            "No recommendation report was generated — "
            "check pipeline logs for errors"
        )
    else:
        # Apply --top-n filter to the buy list
        if args.top_n and args.top_n > 0:
            for key in ("ranked_buys", "buy_list"):
                if key in report and isinstance(report[key], list):
                    report[key] = report[key][: args.top_n]
            logger.info(f"Filtered to top {args.top_n} buys")

        # Terminal output
        if not args.quiet:
            try:
                print()
                print_report(report)
            except Exception as e:
                logger.warning(f"print_report failed: {e}")

        # Save text report
        try:
            save_text(report, str(filenames["report_txt"]))
            logger.info(
                f"Text report  → {filenames['report_txt']}"
            )
        except Exception as e:
            logger.warning(f"save_text failed: {e}")

        # Save HTML report
        if not args.text_only:
            try:
                save_html(
                    report, str(filenames["report_html"])
                )
                logger.info(
                    f"HTML report  → {filenames['report_html']}"
                )
            except Exception as e:
                logger.warning(f"save_html failed: {e}")

        # Save JSON report
        if args.json:
            _save_json(report, str(filenames["report_json"]))
            logger.info(
                f"JSON report  → {filenames['report_json']}"
            )

    # Pipeline JSON (verbose only — for debugging)
    if args.verbose:
        _save_pipeline_result(
            result, str(filenames["pipeline_json"])
        )
        logger.info(
            f"Pipeline JSON → {filenames['pipeline_json']}"
        )

    # ── rebalance plan (if positions provided) ──────────────────
    if current_positions is not None:
        if _HAS_REBALANCE and report is not None:
            _handle_rebalance(
                args, report, current_positions,
                positions_cash, capital, filenames,
            )
        elif not _HAS_REBALANCE:
            logger.warning(
                "reports.portfolio_view not available — "
                "skipping rebalance plan"
            )
        else:
            logger.warning(
                "No report available — "
                "skipping rebalance plan"
            )
    else:
        logger.info(
            "No --positions file — "
            "skipping rebalance plan. "
            "Use --positions <file.json> to generate one."
        )

    # ── done ────────────────────────────────────────────────────
    _finish(t_start, output_dir, log_file)
    return result


def _finish(
    t_start: float, output_dir: Path, log_file: str,
) -> None:
    """Log the closing banner."""
    elapsed = time.time() - t_start
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  CASH run complete in {elapsed:.1f}s")
    logger.info(f"  Reports: {output_dir}/")
    logger.info(f"  Log:     {log_file}")
    logger.info("=" * 60)


# ═════════════════════════════════════════════════════════════════
#  PROGRAMMATIC ENTRY POINT
# ═════════════════════════════════════════════════════════════════

def run(
    portfolio_value: float | None = None,
    tickers: list[str] | None = None,
    universe_path: str | None = None,
    positions: list[dict] | None = None,
    cash_balance: float | None = None,
    output_dir: str = "output",
    save_files: bool = True,
    verbose: bool = False,
    enable_backtest: bool = False,
) -> dict:
    """
    Programmatic entry point for notebooks and scripts.

    Returns a dict with keys: ``result``, ``report``,
    ``plan``, ``filenames``.
    """
    setup_logging(verbose=verbose)

    capital = portfolio_value or PORTFOLIO_PARAMS.get(
        "total_capital", 100_000
    )

    # Resolve universe
    if tickers:
        uni = [t.upper() for t in tickers]
    elif universe_path:
        uni = load_universe_file(universe_path)
    else:
        uni = list(UNIVERSE)

    # Run
    result = run_full_pipeline(
        universe=uni,
        capital=capital,
        enable_backtest=enable_backtest,
    )

    output = {
        "result":   result,
        "report":   result.recommendation_report,
        "plan":     None,
        "filenames": {},
    }

    # Rebalance
    if (
        positions is not None
        and _HAS_REBALANCE
        and result.recommendation_report
    ):
        cb = (
            cash_balance
            if cash_balance is not None
            else capital * 0.10
        )
        try:
            plan = build_rebalance_plan(
                report=result.recommendation_report,
                current_positions=positions,
                cash_balance=cb,
                portfolio_value=capital,
            )
            output["plan"] = plan
        except Exception as e:
            logger.warning(f"Rebalance plan failed: {e}")

    # Save files
    if save_files:
        out = ensure_output_dir(output_dir)
        date_str = datetime.now().strftime("%Y%m%d")
        fnames = generate_filenames(out, date_str)

        if result.recommendation_report:
            try:
                save_text(
                    result.recommendation_report,
                    str(fnames["report_txt"]),
                )
                output["filenames"]["report_txt"] = str(
                    fnames["report_txt"]
                )
            except Exception as e:
                logger.warning(f"save_text failed: {e}")

            try:
                save_html(
                    result.recommendation_report,
                    str(fnames["report_html"]),
                )
                output["filenames"]["report_html"] = str(
                    fnames["report_html"]
                )
            except Exception as e:
                logger.warning(f"save_html failed: {e}")

        if output["plan"] and _HAS_REBALANCE:
            try:
                save_rebalance_text(
                    output["plan"],
                    str(fnames["rebalance_txt"]),
                )
                save_rebalance_html(
                    output["plan"],
                    str(fnames["rebalance_html"]),
                )
                output["filenames"]["rebalance_txt"] = str(
                    fnames["rebalance_txt"]
                )
                output["filenames"]["rebalance_html"] = str(
                    fnames["rebalance_html"]
                )
            except Exception as e:
                logger.warning(f"Rebalance save failed: {e}")

    return output


# ═════════════════════════════════════════════════════════════════
#  SERIALISATION HELPERS
# ═════════════════════════════════════════════════════════════════

def _save_json(data: dict, filepath: str) -> None:
    """Save a dict to JSON with numpy/pandas fallback."""
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=_json_serialiser)


def _save_pipeline_result(
    result: PipelineResult, filepath: str,
) -> None:
    """
    Serialise a ``PipelineResult`` to JSON for debugging.

    Large DataFrames are summarised (shape + columns) to keep
    the file size manageable.
    """
    out: dict = {}

    out["run_date"] = result.run_date.isoformat()
    out["as_of"] = (
        result.as_of.isoformat() if result.as_of else None
    )
    out["n_tickers"] = result.n_tickers
    out["n_errors"] = result.n_errors
    out["total_time"] = result.total_time
    out["timings"] = result.timings
    out["errors"] = result.errors
    out["snapshots"] = result.snapshots
    out["portfolio"] = result.portfolio

    # Summarise DataFrames instead of dumping full contents
    for name, df in [
        ("rankings", result.rankings),
        ("signals", result.signals),
        ("breadth", result.breadth),
    ]:
        if df is not None and hasattr(df, "shape"):
            out[name] = {
                "shape": list(df.shape),
                "columns": list(df.columns),
            }
        else:
            out[name] = None

    with open(filepath, "w") as f:
        json.dump(out, f, indent=2, default=_json_serialiser)


def _json_serialiser(obj):
    """Fallback JSON serialiser for numpy / pandas types."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


# ═════════════════════════════════════════════════════════════════
#  ENTRY
# ═════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()

################### BACKTESTING ###################
"""
backtest/__init__.py
=========
Historical backtesting harness for the CASH Smart Money Rotation system.

Loads 20 years of OHLCV, runs the full pipeline over any date range,
computes performance metrics (including CAGR), and compares strategy
variants side-by-side.

Quick start
-----------
    from backtest.engine import run_backtest_period
    from backtest.data_loader import ensure_history

    data = ensure_history()
    result = run_backtest_period(data)
    print(f"CAGR: {result.metrics['cagr']:.2%}")

CLI
---
    python -m backtest.runner                          # 20-year default
    python -m backtest.runner --start 2015 --end 2024  # custom period
    python -m backtest.runner --compare                # all strategies
    python -m backtest.runner --strategy momentum_heavy
"""

from backtest.engine import run_backtest_period, BacktestRun
from backtest.metrics import compute_cagr, compute_full_metrics
from backtest.comparison import compare_strategies
from backtest.data_loader import ensure_history, load_cached_history

__all__ = [
    "run_backtest_period",
    "BacktestRun",
    "compute_cagr",
    "compute_full_metrics",
    "compare_strategies",
    "ensure_history",
    "load_cached_history",
]

##############################################################
"""
backtest/metrics.py
-------------------
Comprehensive performance analytics for backtesting.

Standalone functions — no state.  Each takes an equity curve,
daily returns, or trade list and returns metrics.

The ``compute_cagr()`` function is the primary tool for
evaluating strategy quality across different time periods.

All functions are independent of the CASH pipeline and can
be used with any equity curve or return series.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio.rebalance import Trade


# ═══════════════════════════════════════════════════════════════
#  CAGR  (Compound Annual Growth Rate)
# ═══════════════════════════════════════════════════════════════

def compute_cagr(
    initial_value: float,
    final_value: float,
    n_years: float,
) -> float:
    """
    Compound Annual Growth Rate.

    Parameters
    ----------
    initial_value : float
        Starting portfolio value.
    final_value : float
        Ending portfolio value.
    n_years : float
        Number of years (can be fractional).

    Returns
    -------
    float
        CAGR as a decimal (e.g. 0.12 = 12% per year).

    Examples
    --------
    >>> compute_cagr(100_000, 250_000, 10)
    0.09596...  # ~9.6% CAGR
    """
    if initial_value <= 0 or n_years <= 0:
        return 0.0
    if final_value <= 0:
        return -1.0
    return (final_value / initial_value) ** (1.0 / n_years) - 1.0


def cagr_from_equity(equity: pd.Series) -> float:
    """Compute CAGR directly from an equity curve."""
    if equity.empty or len(equity) < 2:
        return 0.0
    initial = equity.iloc[0]
    final = equity.iloc[-1]
    n_days = (equity.index[-1] - equity.index[0]).days
    n_years = max(n_days / 365.25, 0.01)
    return compute_cagr(initial, final, n_years)


def cagr_from_returns(daily_returns: pd.Series) -> float:
    """Compute CAGR from a daily return series."""
    if daily_returns.empty:
        return 0.0
    equity = (1 + daily_returns).cumprod()
    return cagr_from_equity(equity)


# ═══════════════════════════════════════════════════════════════
#  ROLLING CAGR
# ═══════════════════════════════════════════════════════════════

def rolling_cagr(
    equity: pd.Series,
    window_years: int = 3,
) -> pd.Series:
    """
    Rolling CAGR over a trailing window.

    Useful for seeing how the strategy's annualised return
    varies over different market regimes.
    """
    if equity.empty:
        return pd.Series(dtype=float)

    window_days = int(window_years * 252)
    if len(equity) < window_days:
        return pd.Series(dtype=float, index=equity.index)

    result = pd.Series(np.nan, index=equity.index)

    for i in range(window_days, len(equity)):
        initial = equity.iloc[i - window_days]
        final = equity.iloc[i]
        if initial > 0:
            result.iloc[i] = (final / initial) ** (1.0 / window_years) - 1

    result.name = f"rolling_{window_years}y_cagr"
    return result


# ═══════════════════════════════════════════════════════════════
#  COMPREHENSIVE METRICS
# ═══════════════════════════════════════════════════════════════

def compute_full_metrics(
    equity_curve: pd.Series,
    daily_returns: pd.Series,
    trades: list[Trade],
    initial_capital: float,
    benchmark_equity: pd.Series | None = None,
) -> dict:
    """
    Compute every performance metric needed for strategy evaluation.

    Returns
    -------
    dict with keys:

    Returns
        total_return, cagr, best_year, worst_year

    Risk
        annual_volatility, sharpe_ratio, sortino_ratio,
        calmar_ratio, max_drawdown, max_dd_duration,
        current_drawdown, var_95, cvar_95, skewness, kurtosis

    Trading
        total_trades, win_rate, profit_factor, avg_win,
        avg_loss, avg_holding_days, total_commission

    Capital
        initial_capital, final_capital, peak_capital

    Benchmark (if provided)
        bench_cagr, bench_sharpe, bench_max_dd,
        excess_cagr, information_ratio, tracking_error,
        up_capture, down_capture

    Periods
        n_days, n_years, start_date, end_date
    """
    if equity_curve.empty:
        return {}

    final = equity_curve.iloc[-1]
    peak = equity_curve.max()
    n_days = len(equity_curve)
    calendar_days = (equity_curve.index[-1] - equity_curve.index[0]).days
    n_years = max(calendar_days / 365.25, 0.01)

    # ── Returns ───────────────────────────────────────────────
    total_return = (final / initial_capital) - 1
    cagr = compute_cagr(initial_capital, final, n_years)

    ann_vol = float(daily_returns.std() * np.sqrt(252)) if len(daily_returns) > 1 else 0.0
    mean_daily = daily_returns.mean() if len(daily_returns) > 0 else 0.0

    # ── Risk-adjusted ─────────────────────────────────────────
    sharpe = (
        (mean_daily / daily_returns.std() * np.sqrt(252))
        if daily_returns.std() > 0 else 0.0
    )

    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 0 else 0.001
    sortino = (
        mean_daily / down_std * np.sqrt(252)
        if down_std > 0 else 0.0
    )

    # ── Drawdown ──────────────────────────────────────────────
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    max_dd = drawdown.min()
    current_dd = drawdown.iloc[-1]

    # Max drawdown duration
    is_dd = drawdown < 0
    dd_groups = (~is_dd).cumsum()
    dd_lengths = is_dd.groupby(dd_groups).sum()
    max_dd_duration = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0

    calmar = abs(cagr / max_dd) if max_dd < 0 else 0.0

    # ── VaR / CVaR ────────────────────────────────────────────
    var_95 = float(daily_returns.quantile(0.05)) if len(daily_returns) > 20 else 0.0
    tail = daily_returns[daily_returns <= var_95]
    cvar_95 = float(tail.mean()) if len(tail) > 0 else var_95

    # ── Higher moments ────────────────────────────────────────
    skewness = float(daily_returns.skew()) if len(daily_returns) > 5 else 0.0
    kurtosis = float(daily_returns.kurtosis()) if len(daily_returns) > 5 else 0.0

    # ── Annual returns ────────────────────────────────────────
    yearly = equity_curve.resample("YE").last()
    yearly_ret = yearly.pct_change().dropna()
    best_year = float(yearly_ret.max()) if len(yearly_ret) > 0 else 0.0
    worst_year = float(yearly_ret.min()) if len(yearly_ret) > 0 else 0.0
    pct_positive_years = (
        float((yearly_ret > 0).mean()) if len(yearly_ret) > 0 else 0.0
    )

    # ── Trades ────────────────────────────────────────────────
    trade_pnls = _compute_trade_pnls(trades)
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    n_trades = len(trade_pnls)
    win_rate = len(wins) / max(n_trades, 1)

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0

    total_commission = sum(t.commission + t.slippage for t in trades)

    # ── Assemble base metrics ─────────────────────────────────
    metrics = {
        # Returns
        "total_return": total_return,
        "cagr": cagr,
        "best_year": best_year,
        "worst_year": worst_year,
        "pct_positive_years": pct_positive_years,
        # Risk
        "annual_volatility": ann_vol,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "max_drawdown": max_dd,
        "max_dd_duration": max_dd_duration,
        "current_drawdown": current_dd,
        "var_95": var_95,
        "cvar_95": cvar_95,
        "skewness": skewness,
        "kurtosis": kurtosis,
        # Trading
        "total_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "total_commission": total_commission,
        # Capital
        "initial_capital": initial_capital,
        "final_capital": final,
        "peak_capital": peak,
        # Periods
        "n_days": n_days,
        "n_years": n_years,
        "start_date": equity_curve.index[0],
        "end_date": equity_curve.index[-1],
    }

    # ── Benchmark comparison ──────────────────────────────────
    if (
        benchmark_equity is not None
        and not benchmark_equity.empty
        and len(benchmark_equity) > 30
    ):
        bench_metrics = _compute_benchmark_metrics(
            equity_curve, daily_returns,
            benchmark_equity, initial_capital,
        )
        metrics.update(bench_metrics)

    return metrics


# ═══════════════════════════════════════════════════════════════
#  BENCHMARK COMPARISON
# ═══════════════════════════════════════════════════════════════

def _compute_benchmark_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    bench_equity: pd.Series,
    initial_capital: float,
) -> dict:
    """Compute metrics relative to a benchmark."""
    # Align
    common = equity.index.intersection(bench_equity.index)
    if len(common) < 30:
        return {}

    strat_ret = daily_returns.reindex(common).fillna(0)
    bench_ret = bench_equity.reindex(common).pct_change().fillna(0)

    n_days = (common[-1] - common[0]).days
    n_years = max(n_days / 365.25, 0.01)

    bench_final = bench_equity.reindex(common).iloc[-1]
    bench_cagr = compute_cagr(initial_capital, bench_final, n_years)

    bench_vol = float(bench_ret.std() * np.sqrt(252))
    bench_sharpe = (
        (bench_ret.mean() / bench_ret.std() * np.sqrt(252))
        if bench_ret.std() > 0 else 0.0
    )

    bench_max_dd = (
        (bench_equity.reindex(common) / bench_equity.reindex(common).cummax() - 1).min()
    )

    # Excess return
    strat_cagr = cagr_from_equity(equity.reindex(common))
    excess_cagr = strat_cagr - bench_cagr

    # Tracking error and information ratio
    active_ret = strat_ret - bench_ret
    tracking_error = float(active_ret.std() * np.sqrt(252))
    information_ratio = (
        float(active_ret.mean() / active_ret.std() * np.sqrt(252))
        if active_ret.std() > 0 else 0.0
    )

    # Up/down capture
    up_days = bench_ret > 0
    dn_days = bench_ret < 0

    up_capture = (
        float(strat_ret[up_days].mean() / bench_ret[up_days].mean())
        if up_days.any() and bench_ret[up_days].mean() != 0 else 1.0
    )
    down_capture = (
        float(strat_ret[dn_days].mean() / bench_ret[dn_days].mean())
        if dn_days.any() and bench_ret[dn_days].mean() != 0 else 1.0
    )

    return {
        "bench_cagr": bench_cagr,
        "bench_sharpe": float(bench_sharpe),
        "bench_max_dd": bench_max_dd,
        "bench_volatility": bench_vol,
        "excess_cagr": excess_cagr,
        "information_ratio": information_ratio,
        "tracking_error": tracking_error,
        "up_capture": up_capture,
        "down_capture": down_capture,
    }


# ═══════════════════════════════════════════════════════════════
#  TRADE PnL
# ═══════════════════════════════════════════════════════════════

def _compute_trade_pnls(trades: list[Trade]) -> list[float]:
    """Match BUY→SELL pairs per ticker (FIFO) and compute returns."""
    open_trades: dict[str, list[Trade]] = {}
    pnls: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                entry_cost = entry.price * (1 + 0.0015)
                exit_net = trade.price * (1 - 0.0015)
                pnl = (exit_net / entry_cost) - 1 if entry_cost > 0 else 0
                pnls.append(pnl)

    return pnls


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def metrics_report(run: "BacktestRun") -> str:
    """Format a BacktestRun as a comprehensive text report."""
    m = run.metrics
    if not m:
        return f"No metrics for '{run.strategy.name}'"

    ln: list[str] = []
    div = "=" * 70
    sub = "-" * 70

    ln.append(div)
    ln.append(f"  BACKTEST REPORT: {run.strategy.name}")
    ln.append(f"  {run.strategy.description}")
    ln.append(div)

    ln.append(f"  Period:          {m.get('start_date', '?')} → "
              f"{m.get('end_date', '?')}  ({m.get('n_years', 0):.1f} years)")
    ln.append(f"  Initial capital: ${m.get('initial_capital', 0):,.0f}")
    ln.append(f"  Final capital:   ${m.get('final_capital', 0):,.0f}")
    ln.append(f"  Peak capital:    ${m.get('peak_capital', 0):,.0f}")

    ln.append("")
    ln.append(sub)
    ln.append("  RETURNS")
    ln.append(sub)
    ln.append(f"  Total return:        {m.get('total_return', 0):>+8.2%}")
    ln.append(f"  CAGR:                {m.get('cagr', 0):>+8.2%}")
    ln.append(f"  Best year:           {m.get('best_year', 0):>+8.2%}")
    ln.append(f"  Worst year:          {m.get('worst_year', 0):>+8.2%}")
    ln.append(f"  % positive years:    {m.get('pct_positive_years', 0):>8.0%}")

    ln.append("")
    ln.append(sub)
    ln.append("  RISK")
    ln.append(sub)
    ln.append(f"  Ann. volatility:     {m.get('annual_volatility', 0):>8.2%}")
    ln.append(f"  Sharpe ratio:        {m.get('sharpe_ratio', 0):>8.3f}")
    ln.append(f"  Sortino ratio:       {m.get('sortino_ratio', 0):>8.3f}")
    ln.append(f"  Calmar ratio:        {m.get('calmar_ratio', 0):>8.3f}")
    ln.append(f"  Max drawdown:        {m.get('max_drawdown', 0):>8.2%}")
    ln.append(f"  Max DD duration:     {m.get('max_dd_duration', 0):>5d} days")
    ln.append(f"  VaR (95%):           {m.get('var_95', 0):>8.4f}")
    ln.append(f"  CVaR (95%):          {m.get('cvar_95', 0):>8.4f}")

    ln.append("")
    ln.append(sub)
    ln.append("  TRADING")
    ln.append(sub)
    ln.append(f"  Total trades:        {m.get('total_trades', 0):>5d}")
    ln.append(f"  Win rate:            {m.get('win_rate', 0):>8.1%}")
    ln.append(f"  Profit factor:       {m.get('profit_factor', 0):>8.2f}")
    ln.append(f"  Avg win:             {m.get('avg_win', 0):>+8.2%}")
    ln.append(f"  Avg loss:            {m.get('avg_loss', 0):>+8.2%}")
    ln.append(f"  Total costs:         ${m.get('total_commission', 0):>10,.2f}")

    if "bench_cagr" in m:
        ln.append("")
        ln.append(sub)
        ln.append("  vs BENCHMARK (SPY)")
        ln.append(sub)
        ln.append(f"  Benchmark CAGR:      {m.get('bench_cagr', 0):>+8.2%}")
        ln.append(f"  Excess CAGR:         {m.get('excess_cagr', 0):>+8.2%}")
        ln.append(f"  Information ratio:   {m.get('information_ratio', 0):>8.3f}")
        ln.append(f"  Tracking error:      {m.get('tracking_error', 0):>8.2%}")
        ln.append(f"  Up capture:          {m.get('up_capture', 0):>8.2f}")
        ln.append(f"  Down capture:        {m.get('down_capture', 0):>8.2f}")
        ln.append(f"  Bench max DD:        {m.get('bench_max_dd', 0):>8.2%}")

    # Annual returns
    if not run.annual_returns.empty:
        ln.append("")
        ln.append(sub)
        ln.append("  ANNUAL RETURNS")
        ln.append(sub)
        for year, ret in run.annual_returns.items():
            bar = "█" * max(0, int(ret * 100))
            ln.append(f"  {year}:  {ret:>+7.2%}  {bar}")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)

###############################

"""
backtest/engine.py
------------------
Core backtesting engine.

Ties together data loading, the full CASH pipeline, and the
portfolio simulation into a single ``run_backtest_period()``
call.

For strategy comparison, the engine accepts parameter overrides
via a ``StrategyConfig`` object.  Overrides are applied to the
global config dicts using a context manager, then restored
after the run — safe for sequential comparison of many variants.

Key improvements
----------------
- Config overrides now wrap both pipeline AND portfolio simulation
- Minimum holding period prevents excessive churn (default 20 cal-days)
- Breadth crisis response blocks equity BUYs in weak/critical regimes
- Cash proxy (SHY) earns returns on idle capital instead of 0 %
- Robust column detection for any signal DataFrame schema
"""

from __future__ import annotations

import copy
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SCORING_WEIGHTS,
    SCORING_PARAMS,
    SIGNAL_PARAMS,
    BREADTH_PORTFOLIO,
)
from pipeline.orchestrator import Orchestrator, PipelineResult
from portfolio.backtest import (
    BacktestConfig,
    BacktestResult,
    run_backtest as run_portfolio_backtest,
    compute_performance_metrics,
)
from portfolio.sizing import SizingConfig
from portfolio.rebalance import RebalanceConfig
from output.signals import SignalConfig

from backtest.data_loader import slice_period, data_summary
from backtest.metrics import compute_full_metrics

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

DEFENSIVE_TICKERS = frozenset({
    "AGG", "SHY", "TLT", "IEF", "GLD", "BIL",
})

# Column-name candidates (checked in priority order)
_SIGNAL_COLS = ("signal", "action", "trade_signal")
_TICKER_COLS = ("ticker", "symbol", "asset")
_DATE_COLS   = ("date", "trade_date", "timestamp")
_SCORE_COLS  = ("composite_score", "score", "total_score", "rank_score")
_REGIME_COLS = (
    "breadth_regime", "regime", "market_regime", "breadth_label",
)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL VALUE HELPERS
# ═══════════════════════════════════════════════════════════════

def _is_buy(val) -> bool:
    """Return True if *val* represents a BUY / entry signal."""
    if isinstance(val, str):
        return val.upper() in ("BUY", "STRONG_BUY", "ENTRY")
    try:
        return float(val) == 1.0
    except (TypeError, ValueError):
        return False


def _is_sell(val) -> bool:
    """Return True if *val* represents a SELL / exit signal."""
    if isinstance(val, str):
        return val.upper() in ("SELL", "STRONG_SELL", "EXIT")
    try:
        return float(val) == -1.0
    except (TypeError, ValueError):
        return False


def _buy_value(sample) -> Any:
    """Return the BUY constant matching the dtype of *sample*."""
    return "BUY" if isinstance(sample, str) else 1


def _hold_value(sample) -> Any:
    """Return the HOLD / neutral constant matching *sample*."""
    return "HOLD" if isinstance(sample, str) else 0


# ═══════════════════════════════════════════════════════════════
#  STRATEGY CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class StrategyConfig:
    """
    A named set of parameter overrides for backtesting.

    Each dict field is either ``None`` (use global default) or a
    mapping of key → value that will be merged into the
    corresponding config dict for the duration of the run.

    Example
    -------
    >>> aggressive = StrategyConfig(
    ...     name="aggressive_momentum",
    ...     description="Heavy momentum weighting",
    ...     scoring_weights={"pillar_momentum": 0.40},
    ...     signal_params={"entry_score_min": 0.50},
    ...     min_hold_days=30,
    ... )
    """
    name: str = "baseline"
    description: str = "Default CASH parameters"

    # ── Config-dict overrides (applied to globals) ────────────
    scoring_weights:   dict | None = None
    scoring_params:    dict | None = None
    signal_params:     dict | None = None
    portfolio_params:  dict | None = None
    breadth_portfolio: dict | None = None

    # ── Component-config overrides ────────────────────────────
    signal_config_overrides:   dict | None = None
    sizing_config_overrides:   dict | None = None
    backtest_config_overrides: dict | None = None

    # ── Universe filter ───────────────────────────────────────
    universe_filter: list[str] | None = None

    # ── Trading rules ─────────────────────────────────────────
    min_hold_days: int = 20
    """Minimum calendar days between entry and exit.  0 = disabled."""

    cash_proxy: str | None = "SHY"
    """Ticker whose returns are applied to idle cash.  None = disabled."""

    # ── Breadth crisis response ───────────────────────────────
    breadth_defensive: bool = True
    """Block equity BUY signals during weak / critical breadth."""

    max_equity_weak: float = 0.40
    """Maximum equity exposure allowed during *weak* breadth."""

    max_equity_critical: float = 0.15
    """Maximum equity exposure allowed during *critical* breadth."""


# ═══════════════════════════════════════════════════════════════
#  BACKTEST RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestRun:
    """
    Complete output of a single backtest run.

    Wraps the pipeline result, the portfolio simulation result,
    and comprehensive performance metrics.
    """
    strategy: StrategyConfig
    start_date: pd.Timestamp
    end_date: pd.Timestamp

    pipeline_result: PipelineResult | None = None
    backtest_result: BacktestResult | None = None

    metrics: dict = field(default_factory=dict)
    annual_returns: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )
    monthly_returns: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark_equity: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )

    elapsed_seconds: float = 0.0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.metrics.get("cagr") is not None

    @property
    def cagr(self) -> float:
        return self.metrics.get("cagr", 0.0)

    @property
    def sharpe(self) -> float:
        return self.metrics.get("sharpe_ratio", 0.0)

    @property
    def max_drawdown(self) -> float:
        return self.metrics.get("max_drawdown", 0.0)

    def summary_line(self) -> str:
        if not self.ok:
            return f"{self.strategy.name:<24s}  ERROR: {self.error}"
        return (
            f"{self.strategy.name:<24s}  "
            f"CAGR={self.cagr:>+7.2%}  "
            f"Sharpe={self.sharpe:>5.2f}  "
            f"MaxDD={self.max_drawdown:>7.2%}  "
            f"Trades={self.metrics.get('total_trades', 0):>5d}  "
            f"({self.elapsed_seconds:.0f}s)"
        )


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_backtest_period(
    data: dict[str, pd.DataFrame],
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    strategy: StrategyConfig | None = None,
    capital: float = 100_000.0,
    benchmark: str | None = None,
) -> BacktestRun:
    """
    Run a complete backtest over a date range.

    Steps
    -----
    1. Slice data to ``[start, end]``
    2. Apply strategy parameter overrides  (context manager)
    3. Run the full CASH pipeline  (Orchestrator)
    4. **Pre-process signals**  (min-hold, breadth override)
    5. Run the portfolio simulation  (portfolio/backtest.py)
    6. **Adjust equity for cash-proxy returns**
    7. Compute performance metrics and benchmarks
    8. Return everything in a ``BacktestRun``

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        ``{ticker: OHLCV}`` from ``ensure_history()``.
    start, end : str / Timestamp / None
        Period bounds.  *None* → earliest / latest available.
    strategy : StrategyConfig or None
        Override set.  *None* → defaults.
    capital : float
        Initial portfolio value.
    benchmark : str or None
        Benchmark ticker.  Default = SPY.
    """
    if strategy is None:
        strategy = StrategyConfig()

    benchmark = benchmark or BENCHMARK_TICKER
    t0 = time.perf_counter()

    # ── 1. Slice data ─────────────────────────────────────────
    sliced = slice_period(data, start=start, end=end)
    if not sliced:
        return BacktestRun(
            strategy=strategy,
            start_date=pd.Timestamp(start) if start else pd.NaT,
            end_date=pd.Timestamp(end) if end else pd.NaT,
            error="No data after slicing to requested period",
        )

    # ── 2. Apply universe filter ──────────────────────────────
    if strategy.universe_filter:
        # Always keep benchmark + cash proxy in the universe
        must_keep = {benchmark}
        if strategy.cash_proxy and strategy.cash_proxy in data:
            must_keep.add(strategy.cash_proxy)

        sliced = {
            k: v for k, v in sliced.items()
            if k in strategy.universe_filter or k in must_keep
        }

    summary = data_summary(sliced)
    actual_start = summary["earliest_start"]
    actual_end = summary["latest_end"]

    logger.info(
        f"Backtest '{strategy.name}': "
        f"{summary['n_tickers']} tickers, "
        f"{actual_start.date()} \u2192 {actual_end.date()}, "
        f"${capital:,.0f}"
    )

    # ── 3. Extract benchmark equity ───────────────────────────
    bench_df = sliced.get(benchmark)
    if bench_df is None or bench_df.empty:
        logger.warning(
            f"Benchmark {benchmark} not in data — "
            f"benchmark comparison will be unavailable"
        )
        bench_equity = pd.Series(dtype=float)
    else:
        bench_equity = (
            bench_df["close"] / bench_df["close"].iloc[0] * capital
        )
        bench_equity.name = "benchmark"

    # ══════════════════════════════════════════════════════════
    #  Everything inside _config_overrides so that scoring
    #  weights, signal params, portfolio params, breadth
    #  settings, etc. are consistently applied to BOTH the
    #  pipeline AND the portfolio simulation.
    # ══════════════════════════════════════════════════════════
    with _config_overrides(strategy):

        # ── 4a. Run CASH pipeline (Phases 0 – 4) ─────────────
        try:
            orch = Orchestrator(
                universe=list(sliced.keys()),
                benchmark=benchmark,
                capital=capital,
                enable_breadth=True,
                enable_sectors=True,
                enable_signals=True,
                enable_backtest=False,   # we run our own below
            )

            orch.load_data(preloaded=sliced, bench_df=bench_df)
            orch.compute_universe_context()
            orch.run_tickers()
            orch.cross_sectional_analysis()
            pipeline_result = orch.generate_reports()

        except Exception as e:
            logger.error(f"Pipeline failed for '{strategy.name}': {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Pipeline error: {e}",
            )

        # ── 4b. Validate signals ─────────────────────────────
        signals_df = pipeline_result.signals
        if signals_df is None or signals_df.empty:
            logger.warning(
                f"No signals generated for '{strategy.name}'"
            )
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error="No signals generated — check pipeline logs",
            )

        # ── 4c. Pre-process signals ──────────────────────────
        signals_df = _preprocess_signals(
            signals_df, strategy, pipeline_result, sliced,
        )

        # ── 4d. Run portfolio simulation ──────────────────────
        try:
            bt_config = _build_backtest_config(strategy, capital)
            bt_result = run_portfolio_backtest(
                signals_df=signals_df,
                config=bt_config,
            )
        except Exception as e:
            logger.error(f"Backtest simulation failed: {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Simulation error: {e}",
            )

    # ── 5. Adjust equity for cash-proxy returns ───────────────
    equity_curve = bt_result.equity_curve.copy()

    if strategy.cash_proxy and strategy.cash_proxy in sliced:
        equity_curve = _apply_cash_proxy_to_equity(
            equity_curve=equity_curve,
            bt_result=bt_result,
            proxy_prices=sliced[strategy.cash_proxy],
            initial_capital=capital,
        )

    # Recompute daily returns from (possibly adjusted) equity
    daily_returns = equity_curve.pct_change().dropna()

    # ── 6. Compute comprehensive metrics ──────────────────────
    metrics = compute_full_metrics(
        equity_curve=equity_curve,
        daily_returns=daily_returns,
        trades=bt_result.trades,
        initial_capital=capital,
        benchmark_equity=bench_equity,
    )

    annual = _compute_annual_returns(equity_curve)
    monthly = _compute_monthly_returns(daily_returns)
    elapsed = time.perf_counter() - t0

    run = BacktestRun(
        strategy=strategy,
        start_date=actual_start,
        end_date=actual_end,
        pipeline_result=pipeline_result,
        backtest_result=bt_result,
        metrics=metrics,
        annual_returns=annual,
        monthly_returns=monthly,
        benchmark_equity=bench_equity,
        elapsed_seconds=elapsed,
    )

    logger.info(run.summary_line())
    return run


# ═══════════════════════════════════════════════════════════════
#  CONFIG OVERRIDE CONTEXT MANAGER
# ═══════════════════════════════════════════════════════════════

@contextmanager
def _config_overrides(strategy: StrategyConfig):
    """
    Temporarily patch global config dicts with strategy overrides.

    Saves originals, applies overrides, yields control, then
    restores originals in a ``finally`` block.  Safe for
    sequential use across many strategies.
    """
    import common.config as cfg

    config_targets = [
        ("SCORING_WEIGHTS",  cfg.SCORING_WEIGHTS,  strategy.scoring_weights),
        ("SCORING_PARAMS",   cfg.SCORING_PARAMS,   strategy.scoring_params),
        ("SIGNAL_PARAMS",    cfg.SIGNAL_PARAMS,    strategy.signal_params),
        ("PORTFOLIO_PARAMS", cfg.PORTFOLIO_PARAMS,  strategy.portfolio_params),
        ("BREADTH_PORTFOLIO", cfg.BREADTH_PORTFOLIO, strategy.breadth_portfolio),
    ]

    # Save originals  ──  shallow copy is sufficient because we
    # only call .update() (not nested mutation) on the dicts.
    originals: list[tuple[str, dict, dict]] = []
    for name, target_dict, overrides in config_targets:
        originals.append((name, target_dict, dict(target_dict)))
        if overrides:
            logger.debug(
                f"Config override [{strategy.name}] {name}: "
                f"{overrides}"
            )
            target_dict.update(overrides)

    n_overridden = sum(1 for _, _, ov in config_targets if ov)
    if n_overridden:
        logger.info(
            f"Applied {n_overridden} config overrides for "
            f"'{strategy.name}'"
        )

    try:
        yield
    finally:
        for _name, target_dict, original_values in originals:
            target_dict.clear()
            target_dict.update(original_values)


# ═══════════════════════════════════════════════════════════════
#  COLUMN DETECTION
# ═══════════════════════════════════════════════════════════════

def _detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    """
    Identify key column names in a signals DataFrame.

    Returns a dict with keys ``signal``, ``ticker``, ``date``,
    ``score``, ``regime`` mapped to the actual column name found
    (or *None* if absent).
    """
    def _find(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    return {
        "signal": _find(_SIGNAL_COLS),
        "ticker": _find(_TICKER_COLS),
        "date":   _find(_DATE_COLS),
        "score":  _find(_SCORE_COLS),
        "regime": _find(_REGIME_COLS),
    }


# ═══════════════════════════════════════════════════════════════
#  SIGNAL PRE-PROCESSING  (master function)
# ═══════════════════════════════════════════════════════════════

def _preprocess_signals(
    signals_df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    price_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Apply all signal-level modifications before the portfolio sim.

    1. Breadth crisis response  — block equity BUYs during
       weak / critical regimes and boost cash-proxy score.
    2. Minimum holding period   — suppress premature SELLs.
    """
    cols = _detect_columns(signals_df)
    sig_col    = cols["signal"]
    ticker_col = cols["ticker"]
    date_col   = cols["date"]

    if not sig_col or not ticker_col:
        logger.warning(
            "Cannot detect signal/ticker columns in signals_df "
            f"(columns: {list(signals_df.columns)[:15]}…) — "
            "skipping all signal preprocessing"
        )
        return signals_df

    df = signals_df.copy()

    # ── 1. Breadth crisis override ────────────────────────────
    if strategy.breadth_defensive:
        df = _apply_breadth_override(
            df, strategy, pipeline_result,
            sig_col, ticker_col, date_col, cols["score"],
        )

    # ── 2. Minimum holding period ─────────────────────────────
    if strategy.min_hold_days > 0 and date_col:
        df = _enforce_min_hold(
            df, strategy.min_hold_days,
            sig_col, ticker_col, date_col,
        )

    return df


# ═══════════════════════════════════════════════════════════════
#  BREADTH CRISIS OVERRIDE
# ═══════════════════════════════════════════════════════════════

def _extract_regime_column(
    df: pd.DataFrame,
    pipeline_result: PipelineResult,
    date_col: str | None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Ensure *df* has a breadth-regime column.

    Checks the DataFrame first; if absent, tries to pull regime
    data from ``pipeline_result`` and merge it in by date.

    Returns ``(possibly-modified df, regime_column_name or None)``.
    """
    # Already present?
    for col in _REGIME_COLS:
        if col in df.columns:
            return df, col

    if pipeline_result is None or date_col is None:
        return df, None

    # Search pipeline_result for breadth data
    breadth = None
    for attr in (
        "breadth", "breadth_data", "breadth_df",
        "market_breadth", "universe_context",
    ):
        val = getattr(pipeline_result, attr, None)
        if val is not None:
            breadth = val
            break

    if breadth is None:
        return df, None

    # Normalise to a single-column DataFrame with a regime label
    regime_series: pd.Series | None = None

    if isinstance(breadth, pd.Series):
        regime_series = breadth

    elif isinstance(breadth, pd.DataFrame):
        for col in _REGIME_COLS:
            if col in breadth.columns:
                regime_series = breadth[col]
                break
        # Fallback: last column if it looks categorical
        if regime_series is None and breadth.shape[1] > 0:
            last = breadth.iloc[:, -1]
            if last.dtype == object or str(last.dtype) == "category":
                regime_series = last

    elif isinstance(breadth, dict) and "regime" in breadth:
        val = breadth["regime"]
        if isinstance(val, (pd.Series, pd.DataFrame)):
            regime_series = (
                val if isinstance(val, pd.Series) else val.iloc[:, 0]
            )

    if regime_series is None:
        return df, None

    # Merge into df by date
    regime_df = regime_series.to_frame(name="_breadth_regime")
    if date_col in df.columns:
        df = df.merge(
            regime_df,
            left_on=date_col,
            right_index=True,
            how="left",
        )
        return df, "_breadth_regime"

    return df, None


def _apply_breadth_override(
    df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    sig_col: str,
    ticker_col: str,
    date_col: str | None,
    score_col: str | None,
) -> pd.DataFrame:
    """
    Block equity BUY signals during weak / critical breadth
    regimes and (optionally) boost the cash-proxy score so it
    gets selected by the portfolio builder.
    """
    df, regime_col = _extract_regime_column(
        df, pipeline_result, date_col,
    )

    if regime_col is None:
        logger.debug(
            "No breadth-regime column found — "
            "skipping crisis override"
        )
        return df

    # Identify which rows are in a weak/critical regime
    regime_lower = df[regime_col].astype(str).str.lower()
    weak_mask     = regime_lower == "weak"
    critical_mask = regime_lower == "critical"
    crisis_mask   = weak_mask | critical_mask

    if not crisis_mask.any():
        return df

    # Determine signal type (string vs numeric)
    sample_sig = df[sig_col].dropna().iloc[0] if len(df) else "BUY"
    hold = _hold_value(sample_sig)
    buy  = _buy_value(sample_sig)

    # ── Block equity BUYs ─────────────────────────────────────
    is_equity = ~df[ticker_col].isin(DEFENSIVE_TICKERS)
    is_buy    = df[sig_col].apply(_is_buy)
    block     = crisis_mask & is_equity & is_buy
    n_blocked = int(block.sum())

    if n_blocked:
        df.loc[block, sig_col] = hold
        logger.info(
            f"Breadth override: blocked {n_blocked:,} equity BUY "
            f"signals in weak/critical regime"
        )

    # ── Boost cash proxy so it fills freed slots ──────────────
    proxy = strategy.cash_proxy
    if proxy and score_col and proxy in df[ticker_col].values:
        proxy_crisis = crisis_mask & (df[ticker_col] == proxy)
        if proxy_crisis.any():
            # Give cash proxy the maximum score during crises
            max_score = df[score_col].max()
            df.loc[proxy_crisis, score_col] = max_score
            df.loc[proxy_crisis, sig_col]   = buy
            logger.info(
                f"Breadth override: boosted {proxy} score on "
                f"{int(proxy_crisis.sum()):,} crisis days"
            )

    return df


# ═══════════════════════════════════════════════════════════════
#  MINIMUM HOLDING PERIOD
# ═══════════════════════════════════════════════════════════════

def _enforce_min_hold(
    df: pd.DataFrame,
    min_hold_days: int,
    sig_col: str,
    ticker_col: str,
    date_col: str,
) -> pd.DataFrame:
    """
    Suppress SELL signals that arrive fewer than *min_hold_days*
    calendar days after the most recent BUY for the same ticker.

    Operates ticker-by-ticker using fast index iteration rather
    than ``iterrows()``.
    """
    if min_hold_days <= 0:
        return df

    # Ensure we have a sortable date column
    if date_col not in df.columns:
        logger.debug(
            f"Date column '{date_col}' not in DataFrame — "
            "skipping min-hold enforcement"
        )
        return df

    result = df.copy()

    # Determine the replacement value for suppressed SELLs.
    # We replace with BUY (= "keep holding") rather than HOLD,
    # because the portfolio sim interprets BUY as "remain in
    # position" for an existing holding.
    sample_sig = (
        result[sig_col].dropna().iloc[0] if len(result) else "BUY"
    )
    keep_holding = _buy_value(sample_sig)

    total_suppressed = 0

    for ticker, group_indices in result.groupby(ticker_col).groups.items():
        sub = result.loc[group_indices, [date_col, sig_col]].sort_values(
            date_col
        )
        dates   = sub[date_col].values        # numpy datetime64
        signals = sub[sig_col].values          # numpy object / int
        indices = sub.index.values             # positional index

        in_position = False
        entry_ts: np.datetime64 | None = None
        fix_list: list = []

        for i in range(len(indices)):
            sig = signals[i]
            dt  = dates[i]

            if _is_buy(sig) and not in_position:
                in_position = True
                entry_ts = dt
            elif _is_buy(sig) and in_position:
                # Consecutive BUY — stay in position, don't reset
                # entry date (hold clock runs from original entry)
                pass
            elif _is_sell(sig) and in_position:
                try:
                    days_held = (
                        pd.Timestamp(dt) - pd.Timestamp(entry_ts)
                    ).days
                except Exception:
                    days_held = min_hold_days  # fail-open

                if days_held < min_hold_days:
                    fix_list.append(indices[i])
                else:
                    in_position = False
                    entry_ts = None
            elif _is_sell(sig) and not in_position:
                pass  # not holding — irrelevant

        if fix_list:
            result.loc[fix_list, sig_col] = keep_holding
            total_suppressed += len(fix_list)

    if total_suppressed:
        logger.info(
            f"Min-hold filter: suppressed {total_suppressed:,} "
            f"premature exits (min {min_hold_days} cal-days)"
        )

    return result


# ═══════════════════════════════════════════════════════════════
#  CASH PROXY — EQUITY ADJUSTMENT
# ═══════════════════════════════════════════════════════════════

def _apply_cash_proxy_to_equity(
    equity_curve: pd.Series,
    bt_result: BacktestResult,
    proxy_prices: pd.DataFrame,
    initial_capital: float,
) -> pd.Series:
    """
    Retroactively credit idle cash with the proxy's return.

    Tries to obtain a per-day cash balance from the backtest
    result.  If unavailable, falls back to a conservative
    estimate (20 % of equity assumed idle on average).

    Parameters
    ----------
    equity_curve : pd.Series
        Unadjusted equity from the portfolio sim.
    bt_result : BacktestResult
        Portfolio simulation output.
    proxy_prices : pd.DataFrame
        OHLCV for the cash proxy ticker.
    initial_capital : float
        Starting capital.

    Returns
    -------
    pd.Series
        Adjusted equity curve.
    """
    # ── Proxy daily returns ───────────────────────────────────
    if "close" in proxy_prices.columns:
        proxy_close = proxy_prices["close"]
    else:
        proxy_close = proxy_prices.iloc[:, 0]

    proxy_ret = proxy_close.pct_change().fillna(0.0)

    # ── Try to get actual cash balance ────────────────────────
    cash_series: pd.Series | None = None

    for attr in (
        "cash", "cash_series", "cash_balance",
        "cash_values", "available_cash",
    ):
        val = getattr(bt_result, attr, None)
        if isinstance(val, pd.Series) and len(val) > 0:
            cash_series = val
            break

    # Try reconstructing: equity minus position values
    if cash_series is None:
        for attr in (
            "positions_value", "invested_value",
            "position_values", "gross_exposure",
        ):
            val = getattr(bt_result, attr, None)
            if isinstance(val, pd.Series) and len(val) > 0:
                cash_series = (equity_curve - val).clip(lower=0.0)
                break

    if cash_series is None:
        # Last resort: assume 20 % of equity is idle (conservative)
        logger.debug(
            "No cash-balance data in BacktestResult — "
            "using 20 %% fallback for cash-proxy adjustment"
        )
        cash_series = equity_curve * 0.20

    # ── Align indices ─────────────────────────────────────────
    common_idx = (
        equity_curve.index
        .intersection(proxy_ret.index)
        .intersection(cash_series.index)
    )
    if len(common_idx) < 2:
        logger.debug("Not enough overlap for cash-proxy adjustment")
        return equity_curve

    cash   = cash_series.reindex(common_idx).ffill().fillna(0.0)
    p_ret  = proxy_ret.reindex(common_idx).fillna(0.0)

    # ── Walk forward: compound cash PnL day-by-day ────────────
    adjusted = equity_curve.reindex(common_idx).copy()
    cum_cash_pnl = 0.0

    values = adjusted.values.copy()  # numpy for speed
    cash_vals = cash.values
    ret_vals  = p_ret.values

    for i in range(1, len(values)):
        # Cash at start-of-day earns the proxy's return
        cash_pnl = cash_vals[i - 1] * ret_vals[i]
        cum_cash_pnl += cash_pnl
        values[i] += cum_cash_pnl

    adjusted = pd.Series(values, index=common_idx, name=equity_curve.name)

    added_pct = (
        (adjusted.iloc[-1] / equity_curve.reindex(common_idx).iloc[-1])
        - 1.0
    ) * 100
    logger.info(
        f"Cash proxy adjustment: +{added_pct:.2f} %% total return "
        f"from idle cash in {strategy_cash_proxy_name(proxy_prices)}"
    )

    return adjusted


def strategy_cash_proxy_name(proxy_prices: pd.DataFrame) -> str:
    """Best-effort human-readable name for the proxy ticker."""
    if hasattr(proxy_prices, "name") and proxy_prices.name:
        return str(proxy_prices.name)
    if hasattr(proxy_prices, "attrs") and "ticker" in proxy_prices.attrs:
        return proxy_prices.attrs["ticker"]
    return "SHY"


# ═══════════════════════════════════════════════════════════════
#  CONFIG BUILDERS
# ═══════════════════════════════════════════════════════════════

def _build_backtest_config(
    strategy: StrategyConfig,
    capital: float,
) -> BacktestConfig:
    """Build ``BacktestConfig`` from strategy overrides."""
    sizing_kw    = strategy.sizing_config_overrides or {}
    bt_overrides = strategy.backtest_config_overrides or {}

    sizing = SizingConfig(**{
        k: v for k, v in sizing_kw.items()
        if k in SizingConfig.__dataclass_fields__
    }) if sizing_kw else SizingConfig()

    rebalance_kw = {
        k: v for k, v in bt_overrides.items()
        if k in RebalanceConfig.__dataclass_fields__
    }
    rebalance = RebalanceConfig(**rebalance_kw) if rebalance_kw else RebalanceConfig()

    # Build the BacktestConfig, passing through only the
    # fields that BacktestConfig actually accepts.
    bc_fields = set(BacktestConfig.__dataclass_fields__)
    bc_kwargs: dict[str, Any] = {
        "initial_capital": capital,
        "sizing": sizing,
        "rebalance": rebalance,
    }

    # Standard optional fields
    for key in ("execution_delay", "rebalance_holds"):
        if key in bt_overrides and key in bc_fields:
            bc_kwargs[key] = bt_overrides[key]

    # Pass through min_hold_days and cash_proxy if BacktestConfig
    # supports them (future-proof).
    if "min_hold_days" in bc_fields:
        bc_kwargs["min_hold_days"] = strategy.min_hold_days
    if "cash_proxy" in bc_fields:
        bc_kwargs["cash_proxy"] = strategy.cash_proxy

    return BacktestConfig(**bc_kwargs)


# ═══════════════════════════════════════════════════════════════
#  RETURN COMPUTATIONS
# ═══════════════════════════════════════════════════════════════

def _compute_annual_returns(equity: pd.Series) -> pd.Series:
    """Year-by-year returns from an equity curve."""
    if equity.empty:
        return pd.Series(dtype=float)

    yearly = equity.resample("YE").last()
    returns = yearly.pct_change().dropna()
    returns.index = returns.index.year
    returns.name = "annual_return"
    return returns


def _compute_monthly_returns(
    daily_returns: pd.Series,
) -> pd.DataFrame:
    """
    Monthly returns as a year × month pivot table.

    Returns a DataFrame with years as rows, months (1–12) as
    columns, and monthly percentage returns as values.
    """
    if daily_returns.empty:
        return pd.DataFrame()

    monthly = (1 + daily_returns).resample("ME").prod() - 1
    monthly_df = pd.DataFrame({
        "year":   monthly.index.year,
        "month":  monthly.index.month,
        "return": monthly.values,
    })

    pivot = monthly_df.pivot_table(
        values="return",
        index="year",
        columns="month",
        aggfunc="first",
    )
    pivot.columns = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][: len(pivot.columns)]

    return pivot

##########################################

"""
backtest/data_loader.py
-----------------------
Download, cache, and serve 20 years of OHLCV data for backtesting.

Primary source is yfinance (``period="max"``).  Data is cached as a
single parquet file at ``data/backtest/backtest_universe.parquet`` so
subsequent runs load in < 2 seconds.

The default backtest universe is a subset of the full CASH universe
consisting of tickers with 15–25 years of history.  The user can
override with any ticker list.

Integration
-----------
Returns data in the same ``{ticker: DataFrame}`` format that
``src/db/loader.py`` produces, so the pipeline accepts it seamlessly
via ``Orchestrator.load_data(preloaded=data)``.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import DATA_DIR, BENCHMARK_TICKER

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  PATHS
# ═══════════════════════════════════════════════════════════════

BACKTEST_DIR = DATA_DIR / "backtest"
CACHE_PATH = BACKTEST_DIR / "backtest_universe.parquet"

# ═══════════════════════════════════════════════════════════════
#  DEFAULT BACKTEST UNIVERSE
#  Tickers with 15–25 years of Yahoo Finance history.
#  Intentionally smaller than the full CASH universe so that
#  20-year backtests are meaningful (no survivorship bias from
#  tickers that didn't exist yet).
# ═══════════════════════════════════════════════════════════════

BACKTEST_CORE_UNIVERSE = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "MDY",
    # Sectors
    "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLY", "XLP", "XLU", "XLB",
    # International
    "EFA", "EEM", "EWJ", "EWZ",
    # Fixed income
    "TLT", "IEF", "SHY", "LQD", "HYG", "AGG", "TIP",
    # Commodities / alternatives
    "GLD", "SLV", "USO", "DBC", "VNQ",
    # Thematic (10+ years)
    "SOXX", "XBI", "IBB", "IGV",
    "HACK", "TAN", "ICLN", "URA",
    "IBIT",
    # Communication (newer but important)
    "XLC", "XLRE",
]

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def ensure_history(
    tickers: list[str] | None = None,
    force_refresh: bool = False,
    max_age_days: int = 7,
) -> dict[str, pd.DataFrame]:
    """
    Ensure 20-year OHLCV data is available.  Downloads from
    yfinance if the cache is missing or stale.

    Parameters
    ----------
    tickers : list[str] or None
        Symbols to download.  Defaults to ``BACKTEST_CORE_UNIVERSE``.
    force_refresh : bool
        If True, re-download even if cache exists.
    max_age_days : int
        Re-download if the cache is older than this many days.

    Returns
    -------
    dict[str, pd.DataFrame]
        ``{ticker: OHLCV DataFrame}`` with DatetimeIndex, lowercase
        columns, sorted ascending.  Ready to pass to
        ``Orchestrator.load_data(preloaded=...)``.
    """
    tickers = tickers or list(BACKTEST_CORE_UNIVERSE)

    # Ensure benchmark is included
    if BENCHMARK_TICKER not in tickers:
        tickers = [BENCHMARK_TICKER] + tickers

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)

    needs_download = (
        force_refresh
        or not CACHE_PATH.exists()
        or _cache_age_days() > max_age_days
    )

    if needs_download:
        logger.info(
            f"Downloading {len(tickers)} tickers from yfinance "
            f"(period=max) ..."
        )
        _download_and_cache(tickers)
    else:
        logger.info(
            f"Using cached data: {CACHE_PATH.name} "
            f"(age: {_cache_age_days():.0f} days)"
        )

    return load_cached_history(tickers)


def load_cached_history(
    tickers: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load previously cached backtest data from parquet.

    Parameters
    ----------
    tickers : list[str] or None
        Filter to these tickers.  None = return all cached.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    if not CACHE_PATH.exists():
        logger.warning(
            f"Cache not found: {CACHE_PATH}.  "
            f"Call ensure_history() first."
        )
        return {}

    raw = pd.read_parquet(CACHE_PATH)

    # Find symbol column
    sym_col = _find_symbol_col(raw)
    if sym_col is None:
        logger.error(
            f"No symbol column found in {CACHE_PATH}.  "
            f"Columns: {list(raw.columns)}"
        )
        return {}

    # Filter tickers
    if tickers is not None:
        upper = {t.upper() for t in tickers}
        raw = raw[raw[sym_col].str.upper().isin(upper)]

    # Split into per-ticker DataFrames
    result: dict[str, pd.DataFrame] = {}
    for ticker, group in raw.groupby(sym_col):
        df = _normalise(group.drop(columns=[sym_col]))
        if not df.empty and len(df) >= 60:
            result[str(ticker)] = df

    logger.info(
        f"Loaded {len(result)} tickers from cache "
        f"({sum(len(d) for d in result.values()):,} total bars)"
    )
    return result


def slice_period(
    data: dict[str, pd.DataFrame],
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Slice every DataFrame in the universe to a date range.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        ``{ticker: OHLCV}`` from ``ensure_history()`` or
        ``load_cached_history()``.
    start : str or Timestamp or None
        Inclusive start date.  None = earliest available.
    end : str or Timestamp or None
        Inclusive end date.  None = latest available.

    Returns
    -------
    dict[str, pd.DataFrame]
        Sliced data.  Tickers with < 60 bars after slicing
        are dropped.
    """
    result: dict[str, pd.DataFrame] = {}

    for ticker, df in data.items():
        sliced = df.loc[start:end] if start or end else df.copy()
        if len(sliced) >= 60:
            result[ticker] = sliced

    n_dropped = len(data) - len(result)
    if n_dropped > 0:
        logger.info(
            f"Period slice: {len(result)} tickers retained, "
            f"{n_dropped} dropped (< 60 bars)"
        )

    return result


def data_summary(data: dict[str, pd.DataFrame]) -> dict:
    """Quick summary of loaded backtest data."""
    if not data:
        return {"n_tickers": 0}

    all_starts = []
    all_ends = []
    total_bars = 0

    for ticker, df in data.items():
        all_starts.append(df.index[0])
        all_ends.append(df.index[-1])
        total_bars += len(df)

    return {
        "n_tickers": len(data),
        "total_bars": total_bars,
        "earliest_start": min(all_starts),
        "latest_end": max(all_ends),
        "median_bars": int(np.median([len(d) for d in data.values()])),
        "tickers": sorted(data.keys()),
    }


# ═══════════════════════════════════════════════════════════════
#  DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def _download_and_cache(tickers: list[str]) -> None:
    """Download max-period data from yfinance and save as parquet."""
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError(
            "yfinance is required for backtest data download.  "
            "pip install yfinance"
        )

    t0 = time.time()

    # Batch download — yfinance handles multi-ticker efficiently
    logger.info(f"yfinance batch download: {len(tickers)} tickers")

    raw = yf.download(
        tickers=tickers,
        period="max",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=True,
    )

    if raw.empty:
        logger.error("yfinance returned empty DataFrame")
        return

    # Reshape from MultiIndex columns to long format with symbol column
    records: list[pd.DataFrame] = []

    if len(tickers) == 1:
        sym = tickers[0]
        tmp = raw.copy().reset_index()
        tmp["symbol"] = sym
        records.append(tmp)
    else:
        for sym in tickers:
            try:
                tmp = raw[sym].copy()
                tmp = tmp.dropna(how="all")
                if tmp.empty:
                    logger.warning(f"  {sym}: no data")
                    continue
                tmp = tmp.reset_index()
                tmp["symbol"] = sym
                records.append(tmp)
            except KeyError:
                logger.warning(f"  {sym}: not in download result")
                continue

    if not records:
        logger.error("No data collected from yfinance")
        return

    combined = pd.concat(records, ignore_index=True)

    # Normalise column names to lowercase
    combined.columns = [str(c).lower().strip() for c in combined.columns]
    rename_map = {"adj close": "adj_close"}
    combined.rename(
        columns={k: v for k, v in rename_map.items() if k in combined.columns},
        inplace=True,
    )

    # Save
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(CACHE_PATH, index=False)

    elapsed = time.time() - t0
    size_mb = CACHE_PATH.stat().st_size / (1024 * 1024)
    n_syms = combined["symbol"].nunique()

    logger.info(
        f"Saved → {CACHE_PATH} "
        f"({size_mb:.1f} MB, {len(combined):,} rows, "
        f"{n_syms} symbols, {elapsed:.0f}s)"
    )


# ═══════════════════════════════════════════════════════════════
#  NORMALISATION
# ═══════════════════════════════════════════════════════════════

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise to the standard format expected by compute/:
    lowercase columns, DatetimeIndex, no NaN closes.
    """
    df = df.copy()

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    df.columns = [str(c).lower().strip() for c in df.columns]
    df.rename(columns={"adj close": "adj_close"}, inplace=True)

    # Set date index
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            return pd.DataFrame()

    df.index.name = "date"

    # Keep only OHLCV
    keep = [c for c in _REQUIRED_COLS if c in df.columns]
    if len(keep) < 5:
        return pd.DataFrame()
    df = df[keep]

    # Clean
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df["close"].notna() & (df["close"] > 0)]
    for col in _REQUIRED_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype(np.int64)

    return df


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _cache_age_days() -> float:
    """How old is the cache file in days."""
    if not CACHE_PATH.exists():
        return float("inf")
    mtime = CACHE_PATH.stat().st_mtime
    age = time.time() - mtime
    return age / 86400.0


def _find_symbol_col(df: pd.DataFrame) -> str | None:
    """Find the symbol/ticker column."""
    for candidate in ["symbol", "Symbol", "ticker", "Ticker", "SYMBOL"]:
        if candidate in df.columns:
            return candidate
    return None


######################################################


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


##################################

"""
backtest/comparison.py
----------------------
Run multiple strategy variants over the same period and produce
a side-by-side comparison ranked by CAGR (or any metric).

Usage
-----
    from backtest.comparison import compare_strategies
    from backtest.data_loader import ensure_history

    data = ensure_history()
    results = compare_strategies(data, start="2010-01-01")
    print(results["report"])
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtest.engine import BacktestRun, StrategyConfig, run_backtest_period
from backtest.strategies import ALL_STRATEGIES, BASELINE
from backtest.metrics import metrics_report

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  COMPARE ALL STRATEGIES
# ═══════════════════════════════════════════════════════════════

def compare_strategies(
    data: dict[str, pd.DataFrame],
    *,
    strategies: list[StrategyConfig] | None = None,
    start: str | None = None,
    end: str | None = None,
    capital: float = 100_000.0,
    rank_by: str = "cagr",
) -> dict[str, Any]:
    """
    Run every strategy variant over the same period and return
    a comparison.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        From ``ensure_history()``.
    strategies : list[StrategyConfig] or None
        Strategies to test.  None = all from ``ALL_STRATEGIES``.
    start, end : str or None
        Date range.
    capital : float
        Initial capital for each run.
    rank_by : str
        Metric to rank by.  Default ``"cagr"``.

    Returns
    -------
    dict with keys:
        runs          list[BacktestRun]  — individual results
        table         pd.DataFrame       — comparison table
        report        str                — formatted text report
        best          BacktestRun        — best by rank_by metric
        worst         BacktestRun        — worst by rank_by metric
    """
    if strategies is None:
        strategies = list(ALL_STRATEGIES.values())

    logger.info(
        f"Comparing {len(strategies)} strategies "
        f"({start or 'earliest'} → {end or 'latest'})"
    )

    runs: list[BacktestRun] = []

    for i, strat in enumerate(strategies, 1):
        logger.info(
            f"[{i}/{len(strategies)}] Running: {strat.name}"
        )
        run = run_backtest_period(
            data,
            start=start,
            end=end,
            strategy=strat,
            capital=capital,
        )
        runs.append(run)

    # ── Build comparison table ────────────────────────────────
    table = _build_comparison_table(runs, rank_by=rank_by)

    # ── Text report ───────────────────────────────────────────
    report = _comparison_report(runs, table, rank_by)

    # ── Best / worst ──────────────────────────────────────────
    valid_runs = [r for r in runs if r.ok]
    best = max(valid_runs, key=lambda r: r.metrics.get(rank_by, -999)) if valid_runs else None
    worst = min(valid_runs, key=lambda r: r.metrics.get(rank_by, 999)) if valid_runs else None

    return {
        "runs": runs,
        "table": table,
        "report": report,
        "best": best,
        "worst": worst,
    }


# ═══════════════════════════════════════════════════════════════
#  COMPARISON TABLE
# ═══════════════════════════════════════════════════════════════

def _build_comparison_table(
    runs: list[BacktestRun],
    rank_by: str = "cagr",
) -> pd.DataFrame:
    """Build a DataFrame comparing all strategy runs."""
    rows = []
    for run in runs:
        m = run.metrics
        rows.append({
            "strategy":       run.strategy.name,
            "cagr":           m.get("cagr", None),
            "total_return":   m.get("total_return", None),
            "sharpe":         m.get("sharpe_ratio", None),
            "sortino":        m.get("sortino_ratio", None),
            "calmar":         m.get("calmar_ratio", None),
            "max_drawdown":   m.get("max_drawdown", None),
            "annual_vol":     m.get("annual_volatility", None),
            "win_rate":       m.get("win_rate", None),
            "total_trades":   m.get("total_trades", None),
            "profit_factor":  m.get("profit_factor", None),
            "excess_cagr":    m.get("excess_cagr", None),
            "info_ratio":     m.get("information_ratio", None),
            "final_capital":  m.get("final_capital", None),
            "best_year":      m.get("best_year", None),
            "worst_year":     m.get("worst_year", None),
            "elapsed_s":      run.elapsed_seconds,
            "error":          run.error,
        })

    df = pd.DataFrame(rows)

    # Sort by rank_by metric (descending for returns/ratios)
    if rank_by in df.columns and df[rank_by].notna().any():
        ascending = rank_by in ("max_drawdown", "annual_vol")
        df = df.sort_values(rank_by, ascending=ascending, na_position="last")

    df = df.reset_index(drop=True)
    df.index = df.index + 1  # 1-based ranking
    df.index.name = "rank"

    return df


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def _comparison_report(
    runs: list[BacktestRun],
    table: pd.DataFrame,
    rank_by: str,
) -> str:
    """Format the comparison as a text report."""
    ln: list[str] = []
    div = "=" * 90
    sub = "-" * 90

    # Header
    valid = [r for r in runs if r.ok]
    if not valid:
        return "No successful backtest runs to compare."

    first = valid[0]
    ln.append(div)
    ln.append("  STRATEGY COMPARISON")
    ln.append(div)
    ln.append(
        f"  Period:     {first.start_date.date()} → "
        f"{first.end_date.date()}"
    )
    ln.append(
        f"  Capital:    ${first.metrics.get('initial_capital', 0):,.0f}"
    )
    ln.append(
        f"  Strategies: {len(runs)} tested, "
        f"{len(valid)} successful"
    )
    ln.append(f"  Ranked by:  {rank_by}")

    # Comparison table
    ln.append("")
    ln.append(sub)
    ln.append(
        f"  {'#':>2}  {'Strategy':<24s} {'CAGR':>8} {'Sharpe':>7} "
        f"{'MaxDD':>8} {'Win%':>6} {'Trades':>7} "
        f"{'Final$':>12} {'ExcessCAGR':>10}"
    )
    ln.append(sub)

    for idx, row in table.iterrows():
        if row.get("error"):
            ln.append(
                f"  {idx:>2}  {row['strategy']:<24s}  "
                f"ERROR: {row['error']}"
            )
            continue

        cagr_s = f"{row['cagr']:>+7.2%}" if pd.notna(row.get("cagr")) else "    N/A"
        sharpe_s = f"{row['sharpe']:>6.2f}" if pd.notna(row.get("sharpe")) else "   N/A"
        dd_s = f"{row['max_drawdown']:>7.2%}" if pd.notna(row.get("max_drawdown")) else "    N/A"
        wr_s = f"{row['win_rate']:>5.1%}" if pd.notna(row.get("win_rate")) else "  N/A"
        trades_s = f"{int(row['total_trades']):>6d}" if pd.notna(row.get("total_trades")) else "   N/A"
        final_s = f"${row['final_capital']:>10,.0f}" if pd.notna(row.get("final_capital")) else "       N/A"
        excess_s = f"{row['excess_cagr']:>+9.2%}" if pd.notna(row.get("excess_cagr")) else "      N/A"

        ln.append(
            f"  {idx:>2}  {row['strategy']:<24s} {cagr_s} {sharpe_s} "
            f"{dd_s} {wr_s} {trades_s} {final_s} {excess_s}"
        )

    # Best/worst summary
    ln.append("")
    ln.append(sub)
    ln.append("  HIGHLIGHTS")
    ln.append(sub)

    if not table.empty and table["cagr"].notna().any():
        best_idx = table["cagr"].idxmax()
        worst_idx = table["cagr"].idxmin()
        best_row = table.loc[best_idx]
        worst_row = table.loc[worst_idx]

        ln.append(
            f"  Best CAGR:    {best_row['strategy']:<20s} "
            f"{best_row['cagr']:>+7.2%}"
        )
        ln.append(
            f"  Worst CAGR:   {worst_row['strategy']:<20s} "
            f"{worst_row['cagr']:>+7.2%}"
        )

    if not table.empty and table["sharpe"].notna().any():
        best_sh = table.loc[table["sharpe"].idxmax()]
        ln.append(
            f"  Best Sharpe:  {best_sh['strategy']:<20s} "
            f"{best_sh['sharpe']:>6.2f}"
        )

    if not table.empty and table["max_drawdown"].notna().any():
        best_dd = table.loc[table["max_drawdown"].idxmax()]
        ln.append(
            f"  Smallest DD:  {best_dd['strategy']:<20s} "
            f"{best_dd['max_drawdown']:>7.2%}"
        )

    ln.append("")
    ln.append(div)
    return "\n".join(ln)


###################

"""
backtest/runner.py
------------------
CLI entry point for backtesting.

Usage:
    python -m backtest.runner                                 # 20Y default
    python -m backtest.runner --start 2010 --end 2020         # custom period
    python -m backtest.runner --strategy momentum_heavy       # single variant
    python -m backtest.runner --compare                       # all strategies
    python -m backtest.runner --compare --rank-by sharpe      # rank by Sharpe
    python -m backtest.runner --list                          # list strategies
    python -m backtest.runner --refresh                       # re-download data
    python -m backtest.runner --capital 500000                # custom capital
    python -m backtest.runner --output backtest_results/      # save reports
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from backtest.data_loader import (
    ensure_history,
    data_summary,
    BACKTEST_CORE_UNIVERSE,
)
from backtest.engine import run_backtest_period, StrategyConfig
from backtest.strategies import (
    ALL_STRATEGIES,
    get_strategy,
    list_strategies,
)
from backtest.comparison import compare_strategies
from backtest.metrics import metrics_report

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="backtest",
        description="CASH — Historical Backtesting Harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backtest.runner                          # 20Y backtest, baseline
  python -m backtest.runner --start 2015 --end 2024  # custom period
  python -m backtest.runner --compare                # compare all strategies
  python -m backtest.runner --strategy conservative  # single variant
  python -m backtest.runner --list                   # show available strategies
        """,
    )

    p.add_argument(
        "--start", type=str, default=None,
        help="Backtest start date (YYYY or YYYY-MM-DD).  Default: earliest available",
    )
    p.add_argument(
        "--end", type=str, default=None,
        help="Backtest end date.  Default: latest available",
    )
    p.add_argument(
        "--strategy", "-s", type=str, default="baseline",
        help="Strategy name to run (default: baseline)",
    )
    p.add_argument(
        "--compare", action="store_true",
        help="Run all strategies and compare side-by-side",
    )
    p.add_argument(
        "--rank-by", type=str, default="cagr",
        choices=["cagr", "sharpe", "sortino", "calmar", "max_drawdown"],
        help="Metric to rank strategies by (default: cagr)",
    )
    p.add_argument(
        "--capital", type=float, default=100_000,
        help="Initial capital (default: 100,000)",
    )
    p.add_argument(
        "--output", "-o", type=str, default=None,
        help="Directory to save backtest reports",
    )
    p.add_argument(
        "--refresh", action="store_true",
        help="Force re-download of historical data",
    )
    p.add_argument(
        "--list", action="store_true", dest="list_strats",
        help="List available strategies and exit",
    )
    p.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override the backtest universe with specific tickers",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Debug logging",
    )

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── Logging ───────────────────────────────────────────────
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    # ── List strategies ───────────────────────────────────────
    if args.list_strats:
        print("\nAvailable strategies:")
        print("-" * 60)
        for name, strat in ALL_STRATEGIES.items():
            print(f"  {name:<24s} {strat.description}")
        print()
        return

    # ── Load data ─────────────────────────────────────────────
    t0 = time.time()
    tickers = (
        [t.upper() for t in args.tickers]
        if args.tickers
        else None
    )

    print("\n" + "=" * 70)
    print("  CASH — BACKTESTING HARNESS")
    print("=" * 70)

    data = ensure_history(
        tickers=tickers,
        force_refresh=args.refresh,
    )

    if not data:
        print("ERROR: No data loaded.  Check your internet connection.")
        sys.exit(1)

    summary = data_summary(data)
    print(f"\n  Data loaded: {summary['n_tickers']} tickers")
    print(f"  Range:       {summary['earliest_start'].date()} → "
          f"{summary['latest_end'].date()}")
    print(f"  Total bars:  {summary['total_bars']:,}")

    # ── Normalise date args ───────────────────────────────────
    start = _normalise_date(args.start)
    end = _normalise_date(args.end)

    # ── Compare mode ──────────────────────────────────────────
    if args.compare:
        print(f"\n  Running comparison of {len(ALL_STRATEGIES)} strategies...")
        print()

        result = compare_strategies(
            data,
            start=start,
            end=end,
            capital=args.capital,
            rank_by=args.rank_by,
        )

        print(result["report"])

        # Detailed report for best strategy
        if result["best"]:
            print("\n" + metrics_report(result["best"]))

        # Save if output dir specified
        if args.output:
            _save_comparison(result, args.output)

        elapsed = time.time() - t0
        print(f"\n  Total time: {elapsed:.0f}s")
        return

    # ── Single strategy mode ──────────────────────────────────
    try:
        strategy = get_strategy(args.strategy)
    except KeyError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"\n  Strategy:    {strategy.name}")
    print(f"  Description: {strategy.description}")
    print(f"  Period:      {start or 'earliest'} → {end or 'latest'}")
    print(f"  Capital:     ${args.capital:,.0f}")
    print()

    run = run_backtest_period(
        data,
        start=start,
        end=end,
        strategy=strategy,
        capital=args.capital,
    )

    if run.ok:
        print(metrics_report(run))
    else:
        print(f"\n  ERROR: {run.error}")

    # Save if output dir specified
    if args.output and run.ok:
        _save_single(run, args.output)

    elapsed = time.time() - t0
    print(f"\n  Total time: {elapsed:.0f}s")


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _normalise_date(date_str: str | None) -> str | None:
    """Convert 'YYYY' to 'YYYY-01-01' for convenience."""
    if date_str is None:
        return None
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return date_str


def _save_comparison(result: dict, output_dir: str) -> None:
    """Save comparison report and CSV to output directory."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    # ── Text report ───────────────────────────────────────
    report_path = os.path.join(output_dir, "comparison_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(result["report"])

    # ── CSV summary ───────────────────────────────────────
    if "summary_df" in result:
        csv_path = os.path.join(output_dir, "comparison_summary.csv")
        result["summary_df"].to_csv(csv_path, index=False, encoding="utf-8")

    # ── Per-strategy equity curves ────────────────────────
    if "equity_curves" in result:
        eq_path = os.path.join(output_dir, "equity_curves.csv")
        result["equity_curves"].to_csv(eq_path, encoding="utf-8")

    print(f"\n  Results saved to: {output_dir}/")


def _save_single(run: "BacktestRun", output_dir: str) -> None:
    """Save a single backtest run."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_path = out / f"backtest_{run.strategy.name}_{ts}.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(metrics_report(run))
    print(f"  Report saved → {report_path}")

    # Save equity curve as CSV
    if run.backtest_result and not run.backtest_result.equity_curve.empty:
        eq_path = out / f"equity_{run.strategy.name}_{ts}.csv"
        eq_df = pd.DataFrame({
            "equity": run.backtest_result.equity_curve,
        })
        if not run.benchmark_equity.empty:
            eq_df["benchmark"] = run.benchmark_equity
        eq_df.to_csv(eq_path)
        print(f"  Equity saved → {eq_path}")


# Need pandas import for _save_single
import pandas as pd

if __name__ == "__main__":
    main()

#####################################################