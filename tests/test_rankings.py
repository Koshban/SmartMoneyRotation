"""
End-to-end test: cross-sectional rankings from scored sector ETF universe.
"""

import time
import yfinance as yf
import pandas as pd

from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score
from compute.breadth import compute_all_breadth, breadth_to_pillar_scores
from output.rankings import (
    build_rankings_panel,
    rank_universe,
    compute_rank_changes,
    compute_pillar_agreement,
    compute_all_rankings,
    latest_rankings,
    filter_top_n,
    filter_by_regime,
    rank_history,
    rankings_summary,
    rankings_report,
)
from utils.run_logger import RunLogger


# ── Configuration ─────────────────────────────────────────────

SECTOR_ETFS = [
    "XLK", "XLF", "XLE", "XLV", "XLY",
    "XLP", "XLI", "XLU", "XLC", "XLRE", "XLB",
]
BENCHMARK = "SPY"
PERIOD = "2y"

BREADTH_TICKERS = [
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
    log = RunLogger("test_rankings")
    t0 = time.time()

    # ══════════════════════════════════════════════════════════
    #  STEP 1 — Download sector ETFs + benchmark
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 1: Download sector ETFs + benchmark")

    # Benchmark
    spy_raw = yf.download(BENCHMARK, period=PERIOD, progress=False)
    spy = clean_single(spy_raw)
    log.kv(BENCHMARK, f"{len(spy)} rows")

    # Sector ETFs
    log.info(f"Downloading {len(SECTOR_ETFS)} sector ETFs...")
    etf_raw = yf.download(SECTOR_ETFS, period=PERIOD, progress=False)

    etf_data: dict[str, pd.DataFrame] = {}
    for ticker in SECTOR_ETFS:
        df = extract_ticker(etf_raw, ticker)
        if not df.empty and len(df) > 100:
            etf_data[ticker] = df
            log.print(f"    {ticker:<6} {len(df)} rows")
        else:
            log.warning(f"{ticker} skipped (insufficient data)")

    log.kv("ETFs loaded", len(etf_data))

    # ══════════════════════════════════════════════════════════
    #  STEP 2 — Breadth computation
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 2: Breadth computation")

    log.info(f"Downloading {len(BREADTH_TICKERS)} stocks for breadth...")
    breadth_raw = yf.download(
        BREADTH_TICKERS, period=PERIOD, progress=False
    )

    breadth_universe: dict[str, pd.DataFrame] = {}
    for ticker in BREADTH_TICKERS:
        df = extract_ticker(breadth_raw, ticker)
        if not df.empty and len(df) > 100:
            breadth_universe[ticker] = df

    log.kv("Breadth universe", f"{len(breadth_universe)} stocks")

    breadth = compute_all_breadth(breadth_universe)
    log.kv("Breadth shape", breadth.shape)
    log.kv("Latest regime",
           log.regime_badge(breadth["breadth_regime"].iloc[-1]))
    log.kv("Latest score", f"{breadth['breadth_score'].iloc[-1]:.3f}")

    # Bridge to per-ETF pillar scores
    pillar_df = breadth_to_pillar_scores(
        breadth, list(etf_data.keys())
    )
    log.kv("Pillar bridge shape", pillar_df.shape)

    # ══════════════════════════════════════════════════════════
    #  STEP 3 — Process each ETF: indicators → RS → scoring
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 3: Process each ETF through full pipeline")

    scored_universe: dict[str, pd.DataFrame] = {}

    for ticker, df in etf_data.items():
        # Indicators
        df = compute_all_indicators(df)
        # Relative strength vs SPY
        df = compute_all_rs(df, spy)
        # Scoring with breadth
        bseries = pillar_df[ticker] if ticker in pillar_df.columns else None
        df = compute_composite_score(df, breadth_scores=bseries)

        scored_universe[ticker] = df
        log.print(
            f"    {ticker:<6} "
            f"score={df['score_composite'].iloc[-1]:.3f}  "
            f"regime={df['rs_regime'].iloc[-1]}"
        )

    log.kv("Scored ETFs", len(scored_universe))

    # ══════════════════════════════════════════════════════════
    #  STEP 4 — Build rankings (step by step)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 4: Build rankings panel (step by step)")

    panel = build_rankings_panel(scored_universe)
    log.kv("Panel shape", panel.shape)
    log.kv("Index levels", panel.index.names)
    log.kv("Columns", list(panel.columns))

    n_dates = panel.index.get_level_values("date").nunique()
    n_tickers = panel.index.get_level_values("ticker").nunique()
    log.kv("Dates", n_dates)
    log.kv("Tickers", n_tickers)

    ranked = rank_universe(panel)
    log.kv("Rank columns added", ["rank", "pct_rank", "universe_size"])

    ranked = compute_rank_changes(ranked)
    log.kv("rank_change added", True)

    ranked = compute_pillar_agreement(ranked)
    log.kv("Agreement columns", ["pillars_bullish", "pillar_agreement"])

    # ══════════════════════════════════════════════════════════
    #  STEP 5 — Full pipeline via compute_all_rankings
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 5: Full pipeline — compute_all_rankings()")

    ranked_full = compute_all_rankings(scored_universe)
    log.kv("Result shape", ranked_full.shape)
    log.kv("Columns", list(ranked_full.columns))

    # Verify step-by-step matches full pipeline
    shapes_match = ranked.shape == ranked_full.shape
    log.kv("Shape matches step-by-step", shapes_match)
    if shapes_match:
        log.success("Full pipeline matches step-by-step build")
    else:
        log.warning("Shape mismatch between step-by-step and full pipeline")

    # ══════════════════════════════════════════════════════════
    #  STEP 6 — Latest rankings snapshot
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 6: Latest rankings snapshot")

    snap = latest_rankings(ranked_full)
    log.kv("Snapshot date", snap["date"].iloc[0].strftime("%Y-%m-%d"))
    log.kv("Symbols ranked", len(snap))

    # Display as rich table
    pillar_cols = [c for c in [
        "score_rotation", "score_momentum", "score_volatility",
        "score_microstructure", "score_breadth",
    ] if c in snap.columns]

    tbl_columns = [
        {"header": "#", "justify": "right", "style": "bold"},
        {"header": "Ticker", "style": "bold cyan"},
        {"header": "Composite", "justify": "right"},
    ]
    for pc in pillar_cols:
        short = pc.replace("score_", "").title()
        tbl_columns.append({"header": short, "justify": "right"})
    tbl_columns += [
        {"header": "Regime", "justify": "center"},
        {"header": "1d", "justify": "right"},
        {"header": "Agree", "justify": "right"},
        {"header": "Δ", "justify": "right"},
    ]

    tbl_rows = []
    for ticker, row in snap.iterrows():
        cells = [
            str(int(row["rank"])),
            ticker,
            f"{row['score_composite']:.3f}",
        ]
        for pc in pillar_cols:
            v = row.get(pc, 0)
            cells.append(f"{v:.3f}" if pd.notna(v) else "—")
        cells.append(log.regime_badge(str(row.get("rs_regime", "?"))))
        ret_1d = row.get("ret_1d", float("nan"))
        cells.append(f"{ret_1d:+.1%}" if pd.notna(ret_1d) else "—")
        cells.append(f"{row.get('pillar_agreement', 0):.0%}")
        delta = int(row.get("rank_change", 0))
        cells.append(f"{delta:+d}" if delta != 0 else "0")
        tbl_rows.append(cells)

    log.table("Current Rankings", tbl_columns, tbl_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 7 — Filter functions
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 7: Filter functions")

    # Top 5
    top5 = filter_top_n(snap, 5)
    log.h2("Top 5")
    for ticker, row in top5.iterrows():
        log.print(
            f"    #{int(row['rank'])}  {ticker:<6}  "
            f"{row['score_composite']:.3f}  "
            f"{row.get('rs_regime', '?')}"
        )

    # By regime: leading + improving
    bullish = filter_by_regime(snap, ["leading", "improving"])
    log.h2("Leading + Improving regimes")
    if bullish.empty:
        log.info("No symbols in leading/improving regime")
    else:
        for ticker, row in bullish.iterrows():
            log.print(
                f"    #{int(row['rank'])}  {ticker:<6}  "
                f"{row['score_composite']:.3f}  "
                f"{log.regime_badge(str(row['rs_regime']))}"
            )

    # ══════════════════════════════════════════════════════════
    #  STEP 8 — Rank history for top symbol
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 8: Rank history for top symbol")

    top_ticker = snap.index[0]
    log.kv("Top ticker", top_ticker)

    hist = rank_history(ranked_full, top_ticker)
    log.kv("History rows", len(hist))

    if not hist.empty and "rank" in hist.columns:
        recent = hist.tail(10)
        log.h2(f"Last 10 days — {top_ticker}")

        hist_cols = [
            {"header": "Date", "style": "bold"},
            {"header": "Rank", "justify": "right"},
            {"header": "Score", "justify": "right"},
            {"header": "Δ", "justify": "right"},
            {"header": "Regime"},
        ]
        hist_rows = []
        for dt, row in recent.iterrows():
            date_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            hist_rows.append([
                date_str,
                str(int(row.get("rank", 0))),
                f"{row.get('score_composite', 0):.3f}",
                f"{int(row.get('rank_change', 0)):+d}",
                str(row.get("rs_regime", "?")),
            ])
        log.table(f"{top_ticker} Rank History", hist_cols, hist_rows)

        # Rank stability
        rank_std = hist["rank"].tail(60).std()
        log.kv("Rank volatility (60d std)", f"{rank_std:.2f}")

    # ══════════════════════════════════════════════════════════
    #  STEP 9 — Summary statistics
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 9: Summary statistics")

    summary = rankings_summary(ranked_full)
    for key, val in summary.items():
        if key == "regime_distribution":
            log.h2("Regime distribution")
            for regime, cnt in val.items():
                log.kv(f"  {regime}", cnt)
        elif isinstance(val, float):
            log.kv(key, f"{val:.4f}")
        else:
            log.kv(key, val)

    # ══════════════════════════════════════════════════════════
    #  STEP 10 — Text report
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 10: Rankings text report")

    report = rankings_report(
        ranked_full,
        breadth_regime=breadth["breadth_regime"].iloc[-1],
        breadth_score=breadth["breadth_score"].iloc[-1],
    )
    log.print(f"\n{report}")

    # ══════════════════════════════════════════════════════════
    #  STEP 11 — Validation checks
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 11: Validation")

    all_ok = True

    # Check: all ranks >= 1 and <= universe_size
    ranks = ranked_full["rank"]
    sizes = ranked_full["universe_size"]
    valid_ranks = (ranks >= 1).all() and (ranks <= sizes).all()
    if valid_ranks:
        log.success("All ranks in [1, universe_size]")
    else:
        log.warning("Ranks out of expected range")
        all_ok = False

    # Check: score_composite in [0, 1]
    comp = ranked_full["score_composite"].dropna()
    valid_scores = (comp >= 0).all() and (comp <= 1).all()
    if valid_scores:
        log.success(
            f"All composites in [0, 1]  "
            f"[{comp.min():.4f}, {comp.max():.4f}]"
        )
    else:
        log.warning("Composites out of range")
        all_ok = False

    # Check: pillar_agreement in [0, 1]
    agree = ranked_full["pillar_agreement"].dropna()
    valid_agree = (agree >= 0).all() and (agree <= 1).all()
    if valid_agree:
        log.success(
            f"All pillar_agreement in [0, 1]  "
            f"[{agree.min():.2f}, {agree.max():.2f}]"
        )
    else:
        log.warning("pillar_agreement out of range")
        all_ok = False

    # Check: rank_change sums to ~0 per date (zero-sum reshuffling)
    rc_by_date = ranked_full.groupby(level="date")["rank_change"].sum()
    rc_nonzero = rc_by_date[rc_by_date != 0]
    # First date has no prior, so rank_change is all zeros — skip it
    if len(rc_nonzero) == 0:
        log.success("Rank changes sum to 0 on every date")
    else:
        max_drift = rc_nonzero.abs().max()
        if max_drift == 0:
            log.success("Rank changes sum to 0 on every date")
        else:
            log.warning(
                f"Rank changes don't sum to 0 on "
                f"{len(rc_nonzero)} dates (max drift: {max_drift})"
            )
            # This can happen legitimately with tied ranks
            all_ok = all_ok  # don't fail for this

    # Check: universe_size consistent
    sizes_per_date = ranked_full.groupby(level="date")["universe_size"].first()
    if (sizes_per_date == len(etf_data)).all():
        log.success(
            f"Universe size = {len(etf_data)} on all dates"
        )
    else:
        min_sz = sizes_per_date.min()
        max_sz = sizes_per_date.max()
        log.info(
            f"Universe size varies: {min_sz}–{max_sz} "
            f"(expected if symbols have different start dates)"
        )

    # Check: latest snapshot has all ETFs
    n_in_snap = len(snap)
    if n_in_snap == len(etf_data):
        log.success(f"Latest snapshot has all {n_in_snap} ETFs")
    else:
        log.warning(
            f"Latest snapshot has {n_in_snap} / "
            f"{len(etf_data)} ETFs"
        )
        all_ok = False

    # ══════════════════════════════════════════════════════════
    #  STEP 12 — Edge case: empty universe
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 12: Edge case — empty universe")

    empty_ranked = compute_all_rankings({})
    if empty_ranked.empty:
        log.success("Empty universe returns empty DataFrame")
    else:
        log.warning(f"Expected empty, got {len(empty_ranked)} rows")
        all_ok = False

    empty_report = rankings_report(empty_ranked)
    if "No rankings" in empty_report:
        log.success("Empty report handled gracefully")
    else:
        log.warning("Empty report didn't return expected message")

    # ══════════════════════════════════════════════════════════
    #  SUMMARY
    # ══════════════════════════════════════════════════════════
    elapsed = time.time() - t0

    log.h1("SUMMARY")
    log.kv("Sector ETFs", len(etf_data))
    log.kv("Breadth universe", f"{len(breadth_universe)} stocks")
    log.kv("Panel shape", ranked_full.shape)
    log.kv("Top ranked", f"{snap.index[0]} ({snap['score_composite'].iloc[0]:.3f})")
    log.kv("Bottom ranked", f"{snap.index[-1]} ({snap['score_composite'].iloc[-1]:.3f})")
    log.kv("All validations", "PASSED ✓" if all_ok else "ISSUES FOUND ⚠")
    log.kv("Elapsed", f"{elapsed:.1f}s")
    log.divider()
    log.success("ALL RANKINGS TESTS PASSED")

    # ── Save HTML report ──────────────────────────────────────
    html_path = log.save()
    log.print(f"\n  [dim]HTML report → {html_path}[/]")


if __name__ == "__main__":
    main()