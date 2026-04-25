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

FIXED_INCOME = ["TLT", "IEF", "HYG", "LQD", "TIP", "AGG"]

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
    "BORORENEW.NS", "BRITANNIA.NS", "CAPLIPOINT.NS", "CENTURYTEX.NS", "CGPOWER.NS",
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
    "MARUTI.NS", "MEGH.NS", "METROBRAND.NS", "MINDACORP.NS", "MINDAIND.NS",
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
            "MSFT", "GOOGL", "META", "SNPS", "CDNS",
        ],
    },
    "ai_platform": {
        "name": "AI Platform / Software",
        "etf_proxy": "AIQ",
        "tickers": [
            "MSFT", "GOOGL", "META", "NOW", "SNOW", "DDOG", "PATH", "TWLO", "PLTR", "APP",
        ],
    },
    "chips": {
        "name": "Semiconductors",
        "etf_proxy": "SOXX",
        "tickers": [
            "NVDA", "AMD", "AVGO", "MRVL", "QCOM", "INTC", "MU", "LRCX",
            "KLAC", "AMAT", "TSM", "ASML", "ARM", "SMCI", "MBLY", "SIMO", "SNDK",
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
        "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AVGO", "BRK.B", "LLY"],
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
            "ROKU", "TTD", "HOOD", "SOUN", "UPST", "SOFI", "TOST", "GLBE", "GENI", "VRT",
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
        "tickers": ["SOFI", "UPST", "HOOD", "TOST", "PGY", "GLBE", "MELI"],
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
            "BORORENEW.NS", "CAPLIPOINT.NS", "CENTURYTEX.NS", "COALINDIA.NS", "COCHINSHIP.NS",
            "CONTROLPR.NS", "CRAFTSMAN.NS", "CYIENTDLM.NS", "DABUR.NS", "DATAPATTNS.NS",
            "DBREALTY.NS", "DECCANCE.NS", "DIXON.NS", "EICHERMOT.NS", "FIEMIND.NS",
            "FSL.NS", "GABRIEL.NS", "GALAXYSURF.NS", "GESHIP.NS", "GRSE.NS",
            "HDFCBANK.NS", "HEIDELBERG.NS", "HGINFRA.NS", "HIKAL.NS", "ICICIBANK.NS",
            "ICICIPRULI.NS", "IDEAFORGE.NS", "INDUSINDBK.NS", "INSECTICID.NS", "INTELLECT.NS",
            "ITC.NS", "JUBLFOOD.NS", "JYOTHYLAB.NS", "KALYANIFRG.NS", "KAYNES.NS",
            "KEI.NS", "KOTAKBANK.NS", "LAOPALA.NS", "LTF.NS", "LT.NS",
            "LTIM.NS", "MANAPPURAM.NS", "MARICO.NS", "MEGH.NS", "METROBRAND.NS",
            "MINDACORP.NS", "MINDAIND.NS", "MTARTECH.NS", "MUTHOOTFIN.NS", "NAVINFLUOR.NS",
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
        return list(ETF_UNIVERSE)
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