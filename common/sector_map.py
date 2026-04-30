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