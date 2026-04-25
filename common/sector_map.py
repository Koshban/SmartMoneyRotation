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
    per GICS (software / IT services). COIN -> Financials (exchange).
  - Nuclear reactor builders (SMR, NNE) -> Industrials (pre-revenue
    equipment makers). Nuclear utilities (CEG, VST, TLN, OKLO) -> Utilities.
    Uranium miners (CCJ, UEC, LEU) -> Energy.
  - Solar hardware (FSLR, ENPH, SEDG) -> Technology per GICS
    (semiconductor equipment / electronic components).
  - UBER -> Industrials (GICS reclassified to Ground Transportation 2023).
  - Each ticker gets exactly one sector, even if it appears in
    multiple themes in universe.py.
  - Consumer Staples has no single names currently. Add PG, KO,
    PEP, COST, WMT etc. if you want stock picks when XLP leads.

This file keeps strict GICS sector mapping for rotation, while theme
classification is handled separately so AI infra / neo-cloud names
can be grouped without contaminating sector rankings.
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
    ],
}


# ═══════════════════════════════════════════════════════════════
#  THEMATIC CLASSIFICATION — separate from GICS sectors
# ═══════════════════════════════════════════════════════════════

THEME_MAP: dict[str, str] = {
    # AI infrastructure / neo-cloud
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

    # AI platform / software
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

    # High momentum beta
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

    # Quantum
    "IONQ": "Quantum",
    "QBTS": "Quantum",
    "RGTI": "Quantum",
    "QUBT": "Quantum",
    "ARQQ": "Quantum",

    # Bitcoin / digital assets
    "MSTR": "Bitcoin / Digital Assets",
    "COIN": "Bitcoin / Digital Assets",
    "MARA": "Bitcoin / Digital Assets",
    "RIOT": "Bitcoin / Digital Assets",
    "CLSK": "Bitcoin / Digital Assets",
    "HIVE": "Bitcoin / Digital Assets",
    "CRCL": "Bitcoin / Digital Assets",

    # China / HK tech
    "BABA": "HK / China Tech",
    "JD": "HK / China Tech",
    "PDD": "HK / China Tech",
    "BIDU": "HK / China Tech",
    "NIO": "HK / China Tech",
    "XPEV": "HK / China Tech",
    "LI": "HK / China Tech",
    "TCOM": "HK / China Tech",
}


# ═══════════════════════════════════════════════════════════════
#  NON-SECTOR ASSETS  (excluded from sector rotation)
# ═══════════════════════════════════════════════════════════════

NON_SECTOR_ASSETS: dict[str, str] = {
    # Broad Market
    "SPY": "Broad Market",
    "QQQ": "Broad Market",
    "IWM": "Broad Market",
    "DIA": "Broad Market",
    "MDY": "Broad Market",
    # Factor
    "MTUM": "Factor",
    # International
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
#  COMPILED FLAT MAPS
# ═══════════════════════════════════════════════════════════════

TICKER_SECTOR_MAP: dict[str, str] = {}
for _sector, _etf in SECTOR_ETFS.items():
    TICKER_SECTOR_MAP[_etf] = _sector

for _sector, _tickers in _SECTOR_TICKERS.items():
    for _t in _tickers:
        TICKER_SECTOR_MAP[_t] = _sector


# ═══════════════════════════════════════════════════════════════
#  LOOKUP HELPERS
# ═══════════════════════════════════════════════════════════════

def get_sector(ticker: str) -> str | None:
    """Return GICS sector for a ticker, or None if it doesn't map to any of the 11 sectors."""
    return TICKER_SECTOR_MAP.get(ticker)


def get_theme(ticker: str) -> str | None:
    """Return theme classification for a ticker, if any."""
    return THEME_MAP.get(ticker)


def get_asset_class(ticker: str) -> str | None:
    """Return asset class for non-sector tickers."""
    return NON_SECTOR_ASSETS.get(ticker)


def get_sector_or_class(ticker: str) -> str:
    """Return sector if available, else asset class, else theme, else 'Unknown'."""
    return (
        TICKER_SECTOR_MAP.get(ticker)
        or NON_SECTOR_ASSETS.get(ticker)
        or THEME_MAP.get(ticker)
        or "Unknown"
    )


def get_tickers_for_sector(sector: str) -> list[str]:
    """All tickers (single names + thematic ETFs) in a GICS sector, excluding the sector ETF itself."""
    return [t for t, s in TICKER_SECTOR_MAP.items() if s == sector and t not in SECTOR_ETFS.values()]


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
    return [t for t, s in TICKER_SECTOR_MAP.items() if s == sector and t.endswith(".NS")]


def get_hk_tickers_for_sector(sector: str) -> list[str]:
    """Return only .HK tickers for a given sector."""
    return [t for t, s in TICKER_SECTOR_MAP.items() if s == sector and t.endswith(".HK")]


def get_theme_tickers(theme: str) -> list[str]:
    """Return all tickers mapped to a given theme string."""
    return sorted([t for t, th in THEME_MAP.items() if th == theme])


def validate_universe_coverage():
    """Cross-check against universe.py to find any unmapped tickers."""
    from common.universe import get_full_universe

    all_known = set(TICKER_SECTOR_MAP.keys()) | set(NON_SECTOR_ASSETS.keys()) | set(THEME_MAP.keys())
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
        us = sorted(t for t in tickers if not t.endswith('.HK') and not t.endswith('.NS'))
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