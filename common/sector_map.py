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