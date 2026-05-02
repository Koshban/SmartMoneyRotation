"""End-to-end test: indicators → stock RS → scoring → sector RS overlay."""

import yfinance as yf
import pandas as pd

from cash.compute.indicators import compute_all_indicators
from cash.compute.relative_strength import compute_all_rs
from cash.compute.scoring import compute_composite_score
from cash.compute.sector_rs import (
    fetch_sector_data,
    compute_all_sector_rs,
    sector_snapshot,
    lookup_sector,
    merge_sector_context,
)


def clean(raw):
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def main():
    divider = "=" * 60

    # ── STEP 1 ────────────────────────────────────────────────
    print(divider)
    print("STEP 1: Fetching 11 sector ETFs + SPY benchmark...")
    print(divider)
    sector_data, bench = fetch_sector_data(period="2y")
    print(f"Loaded {len(sector_data)} sectors, benchmark {len(bench)} rows")

    # ── STEP 2 ────────────────────────────────────────────────
    print()
    print(divider)
    print("STEP 2: Computing sector relative strength...")
    print(divider)
    sector_rs = compute_all_sector_rs(sector_data, bench)
    print(f"Shape: {sector_rs.shape}")

    # ── STEP 3 ────────────────────────────────────────────────
    print()
    print(divider)
    print("STEP 3: Latest sector snapshot (strongest → weakest)")
    print(divider)
    snap = sector_snapshot(sector_rs)
    display_cols = [
        "etf", "sect_rs_zscore", "sect_rs_regime",
        "sect_rs_rank", "sect_rs_pctrank", "sector_tailwind",
    ]
    print(snap[display_cols].to_string(float_format="%.3f"))

    # ── STEP 4 ────────────────────────────────────────────────
    print()
    print(divider)
    print("STEP 4: Sector lookup for AAPL")
    print(divider)
    aapl_sector = lookup_sector("AAPL")
    print(f"AAPL → {aapl_sector}")

    if aapl_sector is None:
        print("Sector lookup failed. Using 'Technology' as fallback.")
        aapl_sector = "Technology"

    # ── STEP 5 ────────────────────────────────────────────────
    print()
    print(divider)
    print("STEP 5: Full AAPL pipeline with sector overlay")
    print(divider)
    stock = clean(yf.download("AAPL", period="2y", progress=False))
    stock = compute_all_indicators(stock)
    stock = compute_all_rs(stock, bench)
    stock = compute_composite_score(stock)
    stock = merge_sector_context(stock, sector_rs, aapl_sector)

    print(f"Final shape: {stock.shape}")
    print()

    cols = [
        "score_composite", "sector_tailwind", "score_adjusted",
        "sect_rs_regime", "sect_rs_rank", "sector_name",
    ]
    print("Last 5 days:")
    print(stock[cols].tail(5).to_string(float_format="%.4f"))

    print()
    comp = stock["score_composite"].dropna()
    adj = stock["score_adjusted"].dropna()
    tailwind = stock["sector_tailwind"].dropna()

    print(f"Composite  — mean: {comp.mean():.4f}  latest: {comp.iloc[-1]:.4f}")
    print(f"Adjusted   — mean: {adj.mean():.4f}  latest: {adj.iloc[-1]:.4f}")
    print(f"Tailwind   — latest: {tailwind.iloc[-1]:.4f}")


if __name__ == "__main__":
    main()