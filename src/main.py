# main.py
"""
Daily workflow orchestrator.
Run after market close:  python main.py
"""

import os
import datetime as dt

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for server / cron
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

from config import (
    ALL_TICKERS,
    BENCHMARKS,
    OUTPUT_DIR,
    TICKER_THEME,
    UNIVERSE,
)
from data import load_all_data
from indicators import compute_all
from breadth import theme_breadth, intra_theme_correlation
from scoring import build_snapshot, compute_category_scores, compute_score_delta


# ════════════════════════════════════════════════════════════════════
#  STEP A : Load data
# ════════════════════════════════════════════════════════════════════
def step_load_data():
    print("=" * 60)
    print("STEP 1 / 6 : Loading market data")
    print("=" * 60)
    data = load_all_data()
    return data


# ════════════════════════════════════════════════════════════════════
#  STEP B : Compute indicators for every ticker
# ════════════════════════════════════════════════════════════════════
def step_compute_indicators(data):
    print("=" * 60)
    print("STEP 2 / 6 : Computing indicators")
    print("=" * 60)

    # Prepare benchmark close Series
    benchmark_closes = {}
    for bm in BENCHMARKS:
        if bm in data:
            benchmark_closes[bm.lower()] = data[bm]["Close"]

    indicators = {}
    for ticker in data:
        if ticker in BENCHMARKS:
            continue
        try:
            ind = compute_all(data[ticker], benchmark_closes)
            indicators[ticker] = ind
            print(f"  [OK] {ticker:6s}  rows={len(ind)}")
        except Exception as e:
            print(f"  [ERR] {ticker}: {e}")

    return indicators


# ════════════════════════════════════════════════════════════════════
#  STEP C : Compute breadth & correlation
# ════════════════════════════════════════════════════════════════════
def step_compute_breadth(data):
    print("=" * 60)
    print("STEP 3 / 6 : Computing theme breadth & correlation")
    print("=" * 60)
    tb = theme_breadth(data)
    tc = intra_theme_correlation(data)
    for theme in tb:
        latest = tb[theme].iloc[-1]
        corr_val = tc[theme].iloc[-1] if theme in tc else float("nan")
        print(
            f"  {theme:22s}  above_20d={latest['pct_above_20d']:.0%}"
            f"  above_50d={latest['pct_above_50d']:.0%}"
            f"  intra_corr={corr_val:.2f}"
        )
    return tb, tc


# ════════════════════════════════════════════════════════════════════
#  STEP D : Score and rank
# ════════════════════════════════════════════════════════════════════
def step_score_and_rank(indicators, tb, tc):
    print("=" * 60)
    print("STEP 4 / 6 : Scoring & ranking")
    print("=" * 60)

    # Find latest common date
    all_dates = set()
    for ind_df in indicators.values():
        all_dates.update(ind_df.index)
    latest_date = max(all_dates)
    print(f"  Scoring date: {latest_date.date()}")

    # Today's snapshot and scores
    snapshot = build_snapshot(indicators, latest_date)
    scored = compute_category_scores(snapshot, tb, tc, latest_date)

    # Attempt to compute score delta vs 5 trading days ago
    delta_date = latest_date - pd.tseries.offsets.BDay(5)
    snap_past = build_snapshot(indicators, delta_date)
    if not snap_past.empty:
        scored_past = compute_category_scores(snap_past, tb, tc, delta_date)
        scored["score_delta"] = compute_score_delta(scored, scored_past)
    else:
        scored["score_delta"] = 0.0

    return scored, latest_date


# ════════════════════════════════════════════════════════════════════
#  STEP E : Export CSV
# ════════════════════════════════════════════════════════════════════
def step_export_csv(scored: pd.DataFrame, run_date):
    print("=" * 60)
    print("STEP 5 / 6 : Exporting CSV")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = run_date.strftime("%Y-%m-%d") if hasattr(run_date, "strftime") else str(run_date)[:10]

    # ── Select columns for the output CSV ───────────────────────────
    export_cols = [
        "theme", "close", "ret_5d", "ret_10d", "ret_15d",
        "rsi", "macd_hist", "adx",
        "dist_ema_30", "dist_sma_50", "dist_avwap",
        "rvol", "obv_slope", "ad_slope",
        "atr_pct", "realized_vol", "rv_rank", "amihud",
        "flow_proxy",
    ]
    # Add RS columns dynamically
    rs_cols = [c for c in scored.columns if c.startswith("rs_")]
    export_cols.extend(rs_cols)

    # Add score columns
    score_cols = [
        "score_trend", "score_momentum", "score_rs",
        "score_volume", "score_breadth", "score_vol_quality",
        "score_flow", "rotation_score", "score_delta", "rank",
        "theme_breadth", "theme_corr",
    ]
    export_cols.extend(score_cols)

    # Only keep columns that exist
    export_cols = [c for c in export_cols if c in scored.columns]

    out = scored[export_cols].copy()
    out.insert(0, "date", date_str)

    path = os.path.join(OUTPUT_DIR, f"rotation_{date_str}.csv")
    out.to_csv(path, float_format="%.4f")
    print(f"  Saved: {path}  ({len(out)} rows)")

    # Also append to cumulative file
    cum_path = os.path.join(OUTPUT_DIR, "rotation_cumulative.csv")
    header = not os.path.exists(cum_path)
    out.to_csv(cum_path, mode="a", header=header, float_format="%.4f")
    print(f"  Appended to: {cum_path}")

    return path


# ════════════════════════════════════════════════════════════════════
#  STEP F : Charts
# ════════════════════════════════════════════════════════════════════
def step_generate_charts(scored: pd.DataFrame, indicators: dict, run_date):
    print("=" * 60)
    print("STEP 6 / 6 : Generating charts")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = run_date.strftime("%Y-%m-%d") if hasattr(run_date, "strftime") else str(run_date)[:10]

    # ── Chart 1: Top-20 Rotation Score bar chart ────────────────────
    top = scored.head(20).copy()
    fig, ax = plt.subplots(figsize=(12, 7))
    colors = []
    for delta in top["score_delta"]:
        if delta > 0.05:
            colors.append("#22c55e")   # green – improving
        elif delta < -0.05:
            colors.append("#ef4444")   # red – deteriorating
        else:
            colors.append("#3b82f6")   # blue – stable

    bars = ax.barh(
        top.index[::-1],
        top["rotation_score"].values[::-1],
        color=colors[::-1],
        edgecolor="white",
        linewidth=0.5,
    )
    ax.set_xlabel("Rotation Score")
    ax.set_title(f"Smart-Money Rotation Rankings — {date_str}")
    ax.axvline(0, color="gray", linewidth=0.5)
    plt.tight_layout()
    path1 = os.path.join(OUTPUT_DIR, f"ranking_{date_str}.png")
    fig.savefig(path1, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path1}")

    # ── Chart 2: Theme-level heatmap ────────────────────────────────
    theme_scores = (
        scored.groupby("theme")["rotation_score"]
        .mean()
        .sort_values(ascending=False)
    )
    fig, ax = plt.subplots(figsize=(10, 4))
    cmap = plt.cm.RdYlGn
    norm_vals = (theme_scores - theme_scores.min()) / (
        theme_scores.max() - theme_scores.min() + 1e-12
    )
    bars = ax.barh(
        theme_scores.index[::-1],
        theme_scores.values[::-1],
        color=[cmap(v) for v in norm_vals.values[::-1]],
        edgecolor="white",
    )
    ax.set_xlabel("Avg Rotation Score")
    ax.set_title(f"Theme Rotation Heatmap — {date_str}")
    ax.axvline(0, color="gray", linewidth=0.5)
    plt.tight_layout()
    path2 = os.path.join(OUTPUT_DIR, f"theme_heatmap_{date_str}.png")
    fig.savefig(path2, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path2}")

    # ── Chart 3: RS trend for top-5 tickers over last 30 days ──────
    top5 = scored.head(5).index.tolist()
    fig, ax = plt.subplots(figsize=(12, 5))
    for ticker in top5:
        if ticker in indicators and "rs_spy" in indicators[ticker].columns:
            rs = indicators[ticker]["rs_spy"].iloc[-30:]
            ax.plot(rs.index, rs.values, label=ticker, linewidth=1.5)
    ax.set_ylabel("RS vs SPY (10D)")
    ax.set_title(f"Relative-Strength Trend — Top 5 — {date_str}")
    ax.legend(loc="upper left")
    ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    plt.tight_layout()
    path3 = os.path.join(OUTPUT_DIR, f"rs_trend_{date_str}.png")
    fig.savefig(path3, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path3}")


# ════════════════════════════════════════════════════════════════════
#  CONSOLE SUMMARY
# ════════════════════════════════════════════════════════════════════
def print_summary(scored: pd.DataFrame):
    print("\n" + "=" * 60)
    print("ROTATION SUMMARY")
    print("=" * 60)

    print("\n── TOP 10 TICKERS ─────────────────────────────────────────")
    top10 = scored.head(10)[
        ["theme", "rotation_score", "score_delta", "ret_5d", "rs_spy",
         "rvol", "adx", "flow_proxy", "theme_breadth"]
    ].copy()
    # Format for readability
    for col in top10.columns:
        if col not in ("theme", "flow_proxy"):
            top10[col] = top10[col].astype(float).map(lambda x: f"{x:+.3f}" if not pd.isna(x) else "  N/A")
    print(top10.to_string())

    print("\n── THEMES RANKED BY AVG SCORE ──────────────────────────────")
    theme_agg = (
        scored.groupby("theme")
        .agg(
            avg_score=("rotation_score", "mean"),
            best_ticker=("rotation_score", "idxmax"),
            avg_breadth=("theme_breadth", "mean"),
            count=("rotation_score", "size"),
        )
        .sort_values("avg_score", ascending=False)
    )
    print(theme_agg.to_string(float_format="%.3f"))

    print("\n── BIGGEST SCORE IMPROVERS (delta > 0.1) ───────────────────")
    if "score_delta" in scored.columns:
        improvers = scored[scored["score_delta"].astype(float) > 0.1][
            ["theme", "rotation_score", "score_delta"]
        ].head(10)
        if len(improvers) > 0:
            print(improvers.to_string(float_format="%.3f"))
        else:
            print("  None with delta > 0.1")

    print("\n── ACTIONABILITY FILTER ────────────────────────────────────")
    print("  Criteria: score > 0.3, ADX > 20, RVOL > 1.0, flow >= 2")
    actionable = scored[
        (scored["rotation_score"].astype(float) > 0.3)
        & (scored["adx"].astype(float) > 20)
        & (scored["rvol"].astype(float) > 1.0)
        & (scored["flow_proxy"].astype(float) >= 2)
    ][["theme", "rotation_score", "score_delta", "adx", "rvol", "flow_proxy"]].head(15)
    if len(actionable) > 0:
        print(actionable.to_string(float_format="%.3f"))
    else:
        print("  No tickers pass all filters today.")


# ════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ════════════════════════════════════════════════════════════════════
def main():
    data = step_load_data()
    indicators = step_compute_indicators(data)
    tb, tc = step_compute_breadth(data)
    scored, run_date = step_score_and_rank(indicators, tb, tc)
    step_export_csv(scored, run_date)
    step_generate_charts(scored, indicators, run_date)
    print_summary(scored)
    print("\n✅ Pipeline complete.\n")


if __name__ == "__main__":
    main()