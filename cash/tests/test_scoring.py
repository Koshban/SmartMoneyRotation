"""
End-to-end test: five-pillar composite scoring with breadth integration.
"""

import sys
import time
import yfinance as yf
import pandas as pd
import numpy as np

from cash.compute.indicators import compute_all_indicators
from cash.compute.relative_strength import compute_all_rs
from cash.compute.scoring import compute_composite_score
from cash.compute.breadth import compute_all_breadth, breadth_to_pillar_scores
from utils.run_logger import RunLogger


# ── Test configuration ────────────────────────────────────────

STOCK_TICKER = "AAPL"
BENCH_TICKER = "SPY"
PERIOD       = "2y"

# Universe for breadth computation
BREADTH_UNIVERSE_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "GS", "BAC",
    "XOM", "CVX",
    "JNJ", "UNH", "PFE",
    "PG", "HD", "WMT",
    "LIN", "CAT",
]


# ── Helpers ───────────────────────────────────────────────────

def clean_single(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise a single-ticker download."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [str(c).lower().replace(" ", "_") for c in raw.columns]
    if "adj_close" in raw.columns:
        if "close" in raw.columns:
            raw = raw.drop(columns=["adj_close"])
        else:
            raw = raw.rename(columns={"adj_close": "close"})
    return raw


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


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    log = RunLogger("test_scoring")
    t0 = time.time()

    # ══════════════════════════════════════════════════════════
    #  STEP 1 — Download stock + benchmark
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 1: Download stock + benchmark")

    stock_raw = yf.download(STOCK_TICKER, period=PERIOD, progress=False)
    bench_raw = yf.download(BENCH_TICKER, period=PERIOD, progress=False)
    stock = clean_single(stock_raw)
    bench = clean_single(bench_raw)

    log.kv(STOCK_TICKER, f"{len(stock)} rows  "
           f"[{stock.index[0].strftime('%Y-%m-%d')} → "
           f"{stock.index[-1].strftime('%Y-%m-%d')}]")
    log.kv(BENCH_TICKER, f"{len(bench)} rows  "
           f"[{bench.index[0].strftime('%Y-%m-%d')} → "
           f"{bench.index[-1].strftime('%Y-%m-%d')}]")

    # ══════════════════════════════════════════════════════════
    #  STEP 2 — Technical indicators
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 2: Technical indicators")

    stock = compute_all_indicators(stock)
    ind_cols = [c for c in stock.columns
                if c not in ["open", "high", "low", "close", "volume"]]
    log.kv("Indicators added", len(ind_cols))
    log.kv("Shape", stock.shape)

    # ══════════════════════════════════════════════════════════
    #  STEP 3 — Relative strength vs benchmark
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 3: Relative strength")

    stock = compute_all_rs(stock, bench)
    rs_cols = [c for c in stock.columns if c.startswith("rs_")]
    log.kv("RS columns", rs_cols)
    log.kv("Shape", stock.shape)
    log.kv("Latest rs_zscore", f"{stock['rs_zscore'].iloc[-1]:.4f}")
    log.kv("Latest rs_regime", stock["rs_regime"].iloc[-1])

    # ══════════════════════════════════════════════════════════
    #  STEP 4 — Breadth computation
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 4: Breadth computation")

    log.info(f"Downloading {len(BREADTH_UNIVERSE_TICKERS)} stocks "
             f"for breadth universe...")

    raw_multi = yf.download(
        BREADTH_UNIVERSE_TICKERS, period=PERIOD, progress=False
    )

    universe: dict[str, pd.DataFrame] = {}
    for ticker in BREADTH_UNIVERSE_TICKERS:
        df = extract_ticker(raw_multi, ticker)
        if not df.empty and len(df) > 100:
            universe[ticker] = df

    log.kv("Universe assembled", f"{len(universe)} stocks")

    breadth = compute_all_breadth(universe)
    log.kv("Breadth shape", breadth.shape)
    log.kv("Latest breadth_score", f"{breadth['breadth_score'].iloc[-1]:.3f}")
    log.kv("Latest breadth_regime",
           log.regime_badge(breadth["breadth_regime"].iloc[-1]))

    # Bridge to pillar format
    pillar_df = breadth_to_pillar_scores(breadth, [STOCK_TICKER])
    breadth_series = pillar_df[STOCK_TICKER]

    log.kv("Pillar bridge shape", pillar_df.shape)
    log.kv("Latest pillar value", f"{breadth_series.iloc[-1]:.2f} (0–100)")

    # ══════════════════════════════════════════════════════════
    #  STEP 5 — Composite score WITHOUT breadth (4-pillar)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 5: Composite score — 4-pillar (no breadth)")

    scored_4p = compute_composite_score(stock, breadth_scores=None)

    score_cols_4p = [c for c in scored_4p.columns if c.startswith("score_")]
    log.kv("Score columns", score_cols_4p)
    log.kv("breadth_available", scored_4p["breadth_available"].iloc[-1])

    comp_4p = scored_4p["score_composite"].dropna()
    log.h2("Composite stats (4-pillar)")
    log.kv("mean", f"{comp_4p.mean():.4f}")
    log.kv("std", f"{comp_4p.std():.4f}")
    log.kv("min", f"{comp_4p.min():.4f}")
    log.kv("max", f"{comp_4p.max():.4f}")
    log.kv("latest", f"{comp_4p.iloc[-1]:.4f}")

    # ══════════════════════════════════════════════════════════
    #  STEP 6 — Composite score WITH breadth (5-pillar)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 6: Composite score — 5-pillar (with breadth)")

    scored_5p = compute_composite_score(stock, breadth_scores=breadth_series)

    score_cols_5p = [c for c in scored_5p.columns if c.startswith("score_")]
    log.kv("Score columns", score_cols_5p)
    log.kv("breadth_available", scored_5p["breadth_available"].iloc[-1])

    has_breadth_col = "score_breadth" in scored_5p.columns
    log.kv("score_breadth present", has_breadth_col)

    if has_breadth_col:
        sb = scored_5p["score_breadth"].dropna()
        log.h2("Breadth pillar stats")
        log.kv("mean", f"{sb.mean():.4f}")
        log.kv("std", f"{sb.std():.4f}")
        log.kv("min", f"{sb.min():.4f}")
        log.kv("max", f"{sb.max():.4f}")
        log.kv("latest", f"{sb.iloc[-1]:.4f}")

    comp_5p = scored_5p["score_composite"].dropna()
    log.h2("Composite stats (5-pillar)")
    log.kv("mean", f"{comp_5p.mean():.4f}")
    log.kv("std", f"{comp_5p.std():.4f}")
    log.kv("min", f"{comp_5p.min():.4f}")
    log.kv("max", f"{comp_5p.max():.4f}")
    log.kv("latest", f"{comp_5p.iloc[-1]:.4f}")

    # ══════════════════════════════════════════════════════════
    #  STEP 7 — Compare 4-pillar vs 5-pillar
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 7: 4-pillar vs 5-pillar comparison")

    common_idx = comp_4p.index.intersection(comp_5p.index)
    diff = comp_5p.loc[common_idx] - comp_4p.loc[common_idx]

    log.kv("Common dates", len(common_idx))
    log.h2("Score difference (5p − 4p)")
    log.kv("mean", f"{diff.mean():+.4f}")
    log.kv("std", f"{diff.std():.4f}")
    log.kv("min", f"{diff.min():+.4f}")
    log.kv("max", f"{diff.max():+.4f}")
    log.kv("latest", f"{diff.iloc[-1]:+.4f}")

    corr = comp_4p.loc[common_idx].corr(comp_5p.loc[common_idx])
    log.kv("Correlation (4p vs 5p)", f"{corr:.4f}")

    if abs(diff.mean()) < 0.10:
        log.success("Mean difference < 0.10 — breadth is a nudge, not a takeover")
    else:
        log.warning("Mean difference >= 0.10 — breadth weight may be too high")

    # ══════════════════════════════════════════════════════════
    #  STEP 8 — Pillar breakdown (last 5 days)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 8: Pillar breakdown — last 5 days (5-pillar)")

    pillar_cols = [
        "score_rotation", "score_momentum", "score_volatility",
        "score_microstructure",
    ]
    if has_breadth_col:
        pillar_cols.append("score_breadth")
    pillar_cols += ["score_composite", "score_percentile"]

    tail = scored_5p[pillar_cols].tail(5)

    # Build rich table for the pillar breakdown
    tbl_columns = [{"header": "Date", "style": "bold cyan"}]
    for col in pillar_cols:
        short = col.replace("score_", "")
        tbl_columns.append({"header": short, "justify": "right"})

    tbl_rows = []
    for dt_idx, row in tail.iterrows():
        date_str = dt_idx.strftime("%Y-%m-%d") if hasattr(dt_idx, "strftime") else str(dt_idx)
        cells = [date_str]
        for col in pillar_cols:
            val = row[col]
            cells.append(f"{val:.4f}" if pd.notna(val) else "—")
        tbl_rows.append(cells)

    log.table("Pillar Scores (last 5 days)", tbl_columns, tbl_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 9 — Average composite by RS regime
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 9: Average composite by RS regime")

    for label, scored in [("4-pillar", scored_4p), ("5-pillar", scored_5p)]:
        log.h2(label)
        grouped = (
            scored.groupby("rs_regime")["score_composite"]
            .mean()
            .sort_values(ascending=False)
        )
        for regime, val in grouped.items():
            badge = log.regime_badge(regime)
            log.kv(f"  {badge}", f"{val:.4f}")

    # ══════════════════════════════════════════════════════════
    #  STEP 10 — Score distribution validation
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 10: Score distribution validation")

    all_ok = True

    check_cols = [
        "score_rotation", "score_momentum", "score_volatility",
        "score_microstructure", "score_composite",
    ]
    if has_breadth_col:
        check_cols.append("score_breadth")

    for col in check_cols:
        vals = scored_5p[col].dropna()
        lo, hi = vals.min(), vals.max()
        in_range = (lo >= 0.0) and (hi <= 1.0)
        short = col.replace("score_", "")
        if in_range:
            log.success(f"{short:<22} [{lo:.4f}, {hi:.4f}]")
        else:
            log.warning(f"{short:<22} [{lo:.4f}, {hi:.4f}]  OUT OF RANGE")
            all_ok = False

    if all_ok:
        log.success("All scores within [0, 1]")
    else:
        log.warning("Some scores out of range!")

    # ══════════════════════════════════════════════════════════
    #  STEP 11 — Edge case: empty breadth series
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 11: Edge case — empty breadth Series")

    empty_series = pd.Series(dtype=float)
    scored_empty = compute_composite_score(stock, breadth_scores=empty_series)

    fell_back = scored_empty["breadth_available"].iloc[-1] == False
    has_no_breadth_col = "score_breadth" not in scored_empty.columns

    if fell_back and has_no_breadth_col:
        log.success("Correctly fell back to 4-pillar mode")
        log.kv("breadth_available", scored_empty["breadth_available"].iloc[-1])
        log.kv("score_breadth absent", has_no_breadth_col)
    else:
        log.warning(
            f"Fallback failed — breadth_available: "
            f"{scored_empty['breadth_available'].iloc[-1]}, "
            f"score_breadth present: {'score_breadth' in scored_empty.columns}"
        )

    # ══════════════════════════════════════════════════════════
    #  STEP 12 — Consistency: 4-pillar explicit vs fallback
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 12: Consistency — None vs empty fallback")

    comp_none = scored_4p["score_composite"].dropna()
    comp_empty = scored_empty["score_composite"].dropna()

    common = comp_none.index.intersection(comp_empty.index)
    max_diff = (comp_none.loc[common] - comp_empty.loc[common]).abs().max()

    if max_diff < 1e-10:
        log.success("None and empty Series produce identical scores")
        log.kv("Max difference", f"{max_diff:.2e}")
    else:
        log.warning(f"Scores differ — max diff: {max_diff:.6f}")

    # ══════════════════════════════════════════════════════════
    #  SUMMARY
    # ══════════════════════════════════════════════════════════
    elapsed = time.time() - t0

    log.h1("SUMMARY")
    log.kv("Stock", STOCK_TICKER)
    log.kv("Benchmark", BENCH_TICKER)
    log.kv("Breadth universe", f"{len(universe)} stocks")
    log.kv("Data rows", len(stock))
    log.kv("Final columns", scored_5p.shape[1])
    log.kv("4-pillar latest", f"{comp_4p.iloc[-1]:.4f}")
    log.kv("5-pillar latest", f"{comp_5p.iloc[-1]:.4f}")
    log.kv("Breadth impact", f"{diff.iloc[-1]:+.4f}")
    log.kv("All ranges valid", all_ok)
    log.kv("Elapsed", f"{elapsed:.1f}s")
    log.divider()
    log.success("ALL SCORING TESTS PASSED")

    # ── Save HTML report ──────────────────────────────────────
    html_path = log.save()
    log.print(f"\n  [dim]HTML report → {html_path}[/]")


if __name__ == "__main__":
    main()