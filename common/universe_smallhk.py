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
    
    "9988.HK",   # Alibaba
  

    
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
         
            # HK-listed shares
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