# run_once_fetch_sectors.py
"""
One-time script: fetches GICS sector from Yahoo Finance for unmapped
Indian tickers and prints a Python dict you can paste into sector_map.py.
"""
import yfinance as yf
from common.sector_map import get_sector_or_class
from common.universe import get_india_only  # adjust import

SECTOR_NORMALIZE = {
    "Financial Services": "Financials",
    "Consumer Cyclical":  "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Basic Materials":    "Materials",
    "Communication Services": "Communication Services",
    "Healthcare":         "Healthcare",
    "Technology":         "Technology",
    "Industrials":        "Industrials",
    "Energy":             "Energy",
    "Utilities":          "Utilities",
    "Real Estate":        "Real Estate",
}

unknowns = [
    t for t in get_india_only()
    if get_sector_or_class(t) in (None, "Unknown", "Other", "")
]

print(f"Fetching sectors for {len(unknowns)} tickers...\n")
mapping = {}
failed = []

for i, ticker in enumerate(sorted(unknowns)):
    try:
        info = yf.Ticker(ticker).info
        raw_sector = info.get("sector", "")
        sector = SECTOR_NORMALIZE.get(raw_sector, raw_sector)
        if sector and sector not in ("", "Unknown"):
            # Store without .NS suffix if your sector_map uses bare symbols
            bare = ticker.replace(".NS", "").replace(".BO", "")
            mapping[bare] = sector
            print(f"  [{i+1}/{len(unknowns)}] {ticker:20s} → {sector}")
        else:
            failed.append(ticker)
            print(f"  [{i+1}/{len(unknowns)}] {ticker:20s} → ??? (sector={raw_sector!r})")
    except Exception as e:
        failed.append(ticker)
        print(f"  [{i+1}/{len(unknowns)}] {ticker:20s} → ERROR: {e}")

print("\n# ── Paste this into sector_map.py ──")
print("INDIA_SECTOR_MAP = {")
for k in sorted(mapping):
    print(f'    "{k}": "{mapping[k]}",')
print("}")

if failed:
    print(f"\n# {len(failed)} tickers still unmapped:")
    for t in failed:
        print(f"#   {t}")