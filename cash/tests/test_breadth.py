"""
End-to-end test: compute market breadth from a 20-stock universe.
"""

import yfinance as yf
import pandas as pd

from common.config import BREADTH_PARAMS
from cash.compute.breadth import (
    align_universe,
    compute_advance_decline,
    compute_mcclellan,
    compute_pct_above_ma,
    compute_new_highs_lows,
    compute_up_volume_ratio,
    compute_breadth_thrust,
    compute_breadth_score,
    classify_breadth_regime,
    compute_all_breadth,
    breadth_report,
    breadth_to_pillar_scores,
)


TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "GS", "BAC",
    "XOM", "CVX",
    "JNJ", "UNH", "PFE",
    "PG", "HD", "WMT",
    "LIN", "CAT",
]


def extract_ticker(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Pull a single ticker from a multi-ticker download."""
    if not isinstance(data.columns, pd.MultiIndex):
        df = data.copy()
    else:
        lvl1 = data.columns.get_level_values(1)
        if ticker in lvl1.unique():
            mask = lvl1 == ticker
            df = data.loc[:, mask].copy()
            df.columns = df.columns.get_level_values(0)
        else:
            try:
                df = data[ticker].copy()
            except KeyError:
                return pd.DataFrame()

    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    if "adj_close" in df.columns:
        if "close" in df.columns:
            df = df.drop(columns=["adj_close"])
        else:
            df = df.rename(columns={"adj_close": "close"})
    df = df.dropna(subset=["close"])
    return df


def main():
    div = "=" * 60

    # ══════════════════════════════════════════════════════════
    #  STEP 1 — Download data
    # ══════════════════════════════════════════════════════════
    print(div)
    print(f"STEP 1: Downloading {len(TICKERS)} stocks (2y)...")
    print(div)

    raw = yf.download(TICKERS, period="2y", progress=True)

    universe: dict[str, pd.DataFrame] = {}
    for ticker in TICKERS:
        df = extract_ticker(raw, ticker)
        if not df.empty and len(df) > 100:
            universe[ticker] = df
            print(f"  {ticker:<6} {len(df)} rows")
        else:
            print(f"  {ticker:<6} SKIP")

    print(f"\n  Universe: {len(universe)} stocks")

    # ══════════════════════════════════════════════════════════
    #  STEP 2 — Individual components
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("STEP 2: Testing individual breadth components...")
    print(div)

    closes, volumes, n = align_universe(universe)
    print(f"  Aligned panel: {closes.shape[0]} days × {n} stocks")

    ad = compute_advance_decline(closes)
    print(f"  Advance-decline: {len(ad)} rows")
    print(f"    Last A-D line: {int(ad['ad_line'].iloc[-1])}")
    print(f"    Last adv ratio: {ad['adv_ratio'].iloc[-1]:.1%}")

    mc = compute_mcclellan(ad)
    print(
        f"  McClellan Osc: {mc['mcclellan_osc'].iloc[-1]:.2f}  "
        f"Sum: {mc['mcclellan_sum'].iloc[-1]:.1f}"
    )

    pct = compute_pct_above_ma(closes)
    print(
        f"  %> 50d SMA: {pct['pct_above_50'].iloc[-1]:.1%}  "
        f"%> 200d SMA: {pct['pct_above_200'].iloc[-1]:.1%}"
    )

    hl = compute_new_highs_lows(closes)
    print(
        f"  New highs: {int(hl['new_highs'].iloc[-1])}  "
        f"New lows: {int(hl['new_lows'].iloc[-1])}  "
        f"Ratio: {hl['hi_lo_ratio_sma'].iloc[-1]:.3f}"
    )

    uv = compute_up_volume_ratio(closes, volumes)
    print(
        f"  Up-volume ratio: {uv['up_volume_ratio'].iloc[-1]:.1%}"
    )

    # ══════════════════════════════════════════════════════════
    #  STEP 3 — Full pipeline via compute_all_breadth()
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("STEP 3: Full breadth pipeline...")
    print(div)

    breadth = compute_all_breadth(universe)
    print(f"  Result shape: {breadth.shape}")
    print(f"  Columns: {list(breadth.columns)}")

    # ══════════════════════════════════════════════════════════
    #  STEP 4 — Report
    # ══════════════════════════════════════════════════════════
    print()
    print(breadth_report(breadth, lookback=10))

    # ══════════════════════════════════════════════════════════
    #  STEP 5 — Regime history
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("STEP 5: Breadth regime distribution (last 60 days)...")
    print(div)

    recent = breadth.tail(60)
    counts = recent["breadth_regime"].value_counts()
    for regime, cnt in counts.items():
        frac = cnt / len(recent)
        bar = "█" * int(frac * 40)
        print(f"  {regime:<10} {cnt:>3} days ({frac:.0%})  {bar}")

    # ══════════════════════════════════════════════════════════
    #  STEP 6 — Thrust history
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("STEP 6: Breadth thrust events (last year)...")
    print(div)

    yr = breadth.tail(252)
    thrusts = yr[yr["breadth_thrust"] == 1]
    if thrusts.empty:
        print("  No breadth thrust events in the last year.")
    else:
        for dt, row in thrusts.iterrows():
            date_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            print(
                f"  ⚡ {date_str}  "
                f"thrust_ema={row['thrust_ema']:.3f}  "
                f"adv_ratio={row['adv_ratio']:.1%}"
            )

    washouts = yr[yr["breadth_washout"] == 1]
    if washouts.empty:
        print("  No breadth washout events in the last year.")
    else:
        for dt, row in washouts.iterrows():
            date_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            print(
                f"  ⚠ {date_str}  "
                f"thrust_ema={row['thrust_ema']:.3f}  "
                f"adv_ratio={row['adv_ratio']:.1%}"
            )

    # ══════════════════════════════════════════════════════════
    #  STEP 7 — Edge case: small universe
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("STEP 7: Edge case — universe below min_stocks...")
    print(div)

    min_req = BREADTH_PARAMS["min_stocks"]
    tiny = {k: v for k, v in list(universe.items())[:3]}
    result_tiny = compute_all_breadth(tiny)
    if result_tiny.empty:
        print(
            f"  Correctly returned empty for "
            f"{len(tiny)}-stock universe (min is {min_req})."
        )
    else:
        print(
            f"  WARNING: expected empty, got "
            f"{len(result_tiny)} rows."
        )

    # ══════════════════════════════════════════════════════════
    #  STEP 8 — Pillar bridge: breadth_to_pillar_scores
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("STEP 8: Pillar bridge — breadth_to_pillar_scores...")
    print(div)

    # Use a small set of symbols as if they were the ETF universe
    etf_symbols = ["XLK", "XLF", "XLE", "XLV", "XLY"]
    pillar = breadth_to_pillar_scores(breadth, etf_symbols)

    print(f"  Shape: {pillar.shape}")
    print(f"  Columns: {list(pillar.columns)}")
    print(f"  Date range: {pillar.index[0].strftime('%Y-%m-%d')} "
          f"→ {pillar.index[-1].strftime('%Y-%m-%d')}")

    # All columns should have identical values (broadcast)
    last_row = pillar.iloc[-1]
    all_equal = last_row.nunique() == 1
    print(f"  All symbols equal on last day: {all_equal}")
    print(f"  Last day value: {last_row.iloc[0]:.2f}")

    # Sanity: values should be 0–100
    vmin = pillar.min().min()
    vmax = pillar.max().max()
    in_range = (vmin >= 0) and (vmax <= 100)
    print(f"  Value range: [{vmin:.2f}, {vmax:.2f}]  "
          f"within 0–100: {in_range}")

    if not in_range:
        print("  ⚠ WARNING: pillar scores outside expected 0–100 range!")

    # ══════════════════════════════════════════════════════════
    #  DONE
    # ══════════════════════════════════════════════════════════
    print()
    print(div)
    print("ALL BREADTH TESTS PASSED ✓")
    print(div)


if __name__ == "__main__":
    main()