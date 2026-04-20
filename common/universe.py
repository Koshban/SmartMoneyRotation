"""
common/universe.py
-----------
Full investable universe for Smart Money Rotation.

Two tiers:
  1. ETF Universe   — used by the core rotation engine (scoring, signals, orders)
  2. Single Names   — organized by theme, for future stock-picking layer

Both tiers get ingested into daily_prices so we always have data ready.

Hong Kong tickers use "XXXX.HK" format.
ingest.py must parse the ".HK" suffix → exchange SEHK, currency HKD for IBKR.
"""

# ═════════════════════════════════════════════════════════════════
#  TIER 1 :  ETF UNIVERSE  (core rotation engine)
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


# Replace the existing parse_hk_symbol with:
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
    """Return all HK-listed tickers (ETFs + single names)."""
    hk = [s for s in ETF_UNIVERSE if is_hk_ticker(s)]
    for theme in SINGLE_NAMES.values():
        hk.extend(s for s in theme["tickers"] if is_hk_ticker(s))
    return sorted(set(hk))


def get_india_only() -> list[str]:
    """Return all India-listed tickers (.NS / .BO) from single names."""
    india = []
    for theme in SINGLE_NAMES.values():
        india.extend(s for s in theme["tickers"] if is_india_ticker(s))
    return sorted(set(india))


def get_full_universe() -> list[str]:
    """
    Everything — ETFs + single names + theme proxies.
    Used by ingest.py for full backfill.
    """
    all_syms: set[str] = set(ETF_UNIVERSE)
    all_syms.update(get_all_single_names())
    all_syms.update(get_theme_etf_proxies())
    return sorted(all_syms)


# ═════════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═════════════════════════════════════════════════════════════════

def print_universe():
    """Pretty-print both tiers for verification."""

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

    # ── Tier 2: Single Names ───────────────────────────────────
    singles = get_all_single_names()
    print(f"\n{'='*65}")
    print(f"  TIER 2 : SINGLE NAMES  ({len(singles)} unique across "
          f"{len(SINGLE_NAMES)} themes)")
    print(f"{'='*65}")
    for key, theme in SINGLE_NAMES.items():
        print(f"  {theme['name']:30s} ({len(theme['tickers']):2d})"
              f"  proxy: {theme['etf_proxy']:5s}"
              f"  | {', '.join(theme['tickers'])}")

    # ── HK Tickers ─────────────────────────────────────────────
    hk_all = get_hk_only()
    print(f"\n{'='*65}")
    print(f"  HK-LISTED TICKERS  ({len(hk_all)} symbols — need SEHK/HKD)")
    print(f"{'='*65}")
    print(f"  {', '.join(hk_all)}")

    # ── India Tickers ──────────────────────────────────────────
    india_all = get_india_only()
    print(f"\n{'='*65}")
    print(f"  INDIA-LISTED TICKERS  ({len(india_all)} symbols — need NSE/BSE)")
    print(f"{'='*65}")
    for i in range(0, len(india_all), 6):
        print(f"  {', '.join(india_all[i:i+6])}")

    # ── Combined ───────────────────────────────────────────────
    full = get_full_universe()
    us_etfs = get_us_only_etfs()
    print(f"\n{'='*65}")
    print(f"  FULL UNIVERSE    : {len(full)} unique symbols")
    print(f"  US ETFs only     : {len(us_etfs)} symbols")
    print(f"  HK tickers       : {len(hk_all)} symbols")
    print(f"  India tickers    : {len(india_all)} symbols")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    print_universe()