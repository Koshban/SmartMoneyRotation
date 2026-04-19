"""End-to-end test: indicators → RS → scoring → sector → signals."""

import yfinance as yf
import pandas as pd

from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score
from compute.sector_rs import (
    fetch_sector_data,
    compute_all_sector_rs,
    lookup_sector,
    merge_sector_context,
)
from strategy.signals import generate_signals, signal_summary


def clean(raw):
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def main():
    div = "=" * 60

    # ── Sector context ────────────────────────────────────────
    print(div)
    print("Fetching sector data...")
    print(div)
    sector_data, bench = fetch_sector_data(period="2y")
    sector_rs = compute_all_sector_rs(sector_data, bench)
    print(f"Sector RS shape: {sector_rs.shape}")

    # ── AAPL full pipeline ────────────────────────────────────
    print()
    print(div)
    print("Running full AAPL pipeline...")
    print(div)
    stock = clean(yf.download("AAPL", period="2y", progress=False))
    stock = compute_all_indicators(stock)
    stock = compute_all_rs(stock, bench)
    stock = compute_composite_score(stock)

    aapl_sector = lookup_sector("AAPL")
    if aapl_sector is None:
        aapl_sector = "Technology"
    stock = merge_sector_context(stock, sector_rs, aapl_sector)

    stock = generate_signals(stock)
    print(f"Final shape: {stock.shape}")

    # ── Signal columns for last 10 days ───────────────────────
    print()
    print(div)
    print("Last 10 days — signal detail")
    print(div)
    sig_cols = [
        "score_adjusted", "sig_regime_ok", "sig_sector_ok",
        "sig_momentum_ok", "sig_raw", "sig_confirmed",
        "sig_position_pct", "sig_reason",
    ]
    print(stock[sig_cols].tail(10).to_string(float_format="%.3f"))

    # ── Entry / exit triggers ─────────────────────────────────
    print()
    print(div)
    print("All entry triggers")
    print(div)
    entries = stock[stock["sig_entry_trigger"]]
    if len(entries) > 0:
        entry_cols = [
            "score_adjusted", "rs_regime", "sect_rs_regime",
            "sig_position_pct", "sig_reason",
        ]
        print(entries[entry_cols].to_string(float_format="%.3f"))
    else:
        print("No entry triggers in history.")

    print()
    print(div)
    print("All exit triggers")
    print(div)
    exits = stock[stock["sig_exit_trigger"]]
    if len(exits) > 0:
        exit_cols = [
            "score_adjusted", "rs_regime", "sig_reason",
        ]
        print(exits[exit_cols].to_string(float_format="%.3f"))
    else:
        print("No exit triggers in history.")

    # ── Summary stats ─────────────────────────────────────────
    print()
    print(div)
    print("Signal summary")
    print(div)
    summary = signal_summary(stock)
    for k, v in summary.items():
        print(f"  {k:.<30} {v}")

    # ── Signal distribution ───────────────────────────────────
    print()
    print(div)
    print("Reason distribution (last 60 days)")
    print(div)
    recent = stock["sig_reason"].tail(60)
    # Group by first word (LONG / FLAT / BLOCKED / WAIT / COOLDOWN)
    categories = recent.str.split(":").str[0]
    print(categories.value_counts().to_string())


if __name__ == "__main__":
    main()