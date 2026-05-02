"""
output/rankings.py
------------------
Daily cross-sectional rankings across the scored ETF / stock universe.

Takes the output of the scoring pipeline — one scored DataFrame per
symbol — and produces ranked tables showing which names have the
strongest composite scores on any given trading day.

This is the bridge between scoring and portfolio: the strategy layer
consumes these rankings to decide what to buy, hold, and sell.

Key Columns Added
─────────────────
  rank              1 = best (highest composite score)
  pct_rank          0–1 percentile within universe
  universe_size     how many symbols are ranked that day
  rank_change       +N = improved N places vs prior day
  pillars_bullish   count of pillar scores > 0.50
  pillar_agreement  fraction of pillars > 0.50  (0–1)
  ret_1d / 5d / 20d recent returns for context

Pipeline
────────
  {ticker: scored_df}
       ↓
  build_rankings_panel()     — stack into MultiIndex panel
       ↓
  rank_universe()            — cross-sectional rank per date
       ↓
  compute_rank_changes()     — day-over-day rank movement
       ↓
  compute_pillar_agreement() — signal agreement across pillars
       ↓
  compute_all_rankings()     — master orchestrator → ranked panel
       ↓
  latest_rankings()          — snapshot for a single date
  filter_top_n()             — top N symbols
  filter_by_regime()         — filter by RS regime
  rank_history()             — single ticker over time
  rankings_summary()         — summary statistics dict
  rankings_report()          — formatted text report
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  COLUMN LISTS
# ═══════════════════════════════════════════════════════════════

_SCORE_COLS = [
    "score_composite",
    "score_adjusted",
    "score_rotation",
    "score_momentum",
    "score_volatility",
    "score_microstructure",
    "score_breadth",
    "score_percentile",
]

_META_COLS = [
    "rs_regime",
    "rs_zscore",
    "rs_momentum",
    "sect_rs_regime",
    "close",
    "breadth_available",
]

_PILLAR_COLS = [
    "score_rotation",
    "score_momentum",
    "score_volatility",
    "score_microstructure",
    "score_breadth",
]


# ═══════════════════════════════════════════════════════════════
#  PANEL BUILDER
# ═══════════════════════════════════════════════════════════════

def build_rankings_panel(
    scored_universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Stack per-symbol scored DataFrames into a single panel.

    Parameters
    ----------
    scored_universe : dict
        {ticker: DataFrame} where each DataFrame has been through
        compute_all_indicators → compute_all_rs →
        compute_composite_score, and optionally
        strategy.signals.generate_signals().

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) with score, metadata, signal
        gate, and return columns.  Symbols missing
        ``score_composite`` are silently skipped.

    Notes
    -----
    Any column starting with ``sig_`` is automatically carried
    forward so that per-ticker gates from ``strategy/signals.py``
    are available to ``output/signals.py`` for entry qualification.
    """
    frames: list[pd.DataFrame] = []

    for ticker, df in scored_universe.items():
        if df is None or df.empty:
            continue
        if "score_composite" not in df.columns:
            continue

        # ── Core score + metadata columns ─────────────────
        available = [c for c in _SCORE_COLS + _META_COLS
                     if c in df.columns]

        # ── Per-ticker signal gate columns (strategy/) ────
        sig_cols = [c for c in df.columns if c.startswith("sig_")]
        available += sig_cols

        subset = df[available].copy()
        subset["ticker"] = ticker

        # Recent returns for context
        if "close" in df.columns:
            c = df["close"]
            subset["ret_1d"]  = c.pct_change(1)
            subset["ret_5d"]  = c.pct_change(5)
            subset["ret_20d"] = c.pct_change(20)

        frames.append(subset)

    if not frames:
        return pd.DataFrame()

    panel = pd.concat(frames)
    panel = panel.set_index("ticker", append=True)
    panel.index.names = ["date", "ticker"]

    return panel.sort_index()


# ═══════════════════════════════════════════════════════════════
#  CROSS-SECTIONAL RANKING
# ═══════════════════════════════════════════════════════════════

def rank_universe(
    panel: pd.DataFrame,
    rank_col: str = "score_composite",
) -> pd.DataFrame:
    """
    Cross-sectional rank on each trading day.

    Higher score = better = rank 1.

    Parameters
    ----------
    panel : pd.DataFrame
        Output of ``build_rankings_panel()``.
    rank_col : str
        Column to rank by (default ``score_composite``).

    Returns
    -------
    pd.DataFrame
        Same frame with ``rank``, ``pct_rank``, ``universe_size``
        added.
    """
    if panel.empty or rank_col not in panel.columns:
        return panel

    result = panel.copy()
    grouped = result.groupby(level="date")[rank_col]

    result["rank"] = grouped.rank(
        ascending=False, method="min",
    ).astype(int)
    result["pct_rank"] = grouped.rank(ascending=False, pct=True)
    result["universe_size"] = grouped.transform("count").astype(int)

    return result


# ═══════════════════════════════════════════════════════════════
#  RANK CHANGES
# ═══════════════════════════════════════════════════════════════

def compute_rank_changes(
    ranked: pd.DataFrame,
) -> pd.DataFrame:
    """
    Day-over-day rank movement for each ticker.

    rank_change > 0  → symbol moved UP in ranking (improved)
    rank_change < 0  → symbol dropped
    rank_change = 0  → unchanged
    """
    if ranked.empty or "rank" not in ranked.columns:
        return ranked

    result = ranked.copy()

    rank_wide = result["rank"].unstack(level="ticker")
    # previous_rank − current_rank → positive = improved
    change_wide = -rank_wide.diff()

    change_long = change_wide.stack()
    change_long.name = "rank_change"
    change_long.index.names = ["date", "ticker"]

    result = result.join(change_long)
    result["rank_change"] = result["rank_change"].fillna(0).astype(int)

    return result


# ═══════════════════════════════════════════════════════════════
#  PILLAR AGREEMENT
# ═══════════════════════════════════════════════════════════════

def compute_pillar_agreement(
    ranked: pd.DataFrame,
    threshold: float = 0.50,
) -> pd.DataFrame:
    """
    Count how many pillar scores exceed a threshold.

    High agreement (4/5 or 5/5 pillars bullish) signals broad
    confirmation — the composite isn't being carried by a single
    strong pillar masking weakness elsewhere.

    Parameters
    ----------
    ranked : pd.DataFrame
        Ranked panel with pillar score columns.
    threshold : float
        Score above which a pillar counts as bullish (default 0.50).

    Returns
    -------
    pd.DataFrame
        Same frame with ``pillars_bullish`` (int count) and
        ``pillar_agreement`` (0–1 fraction) appended.
    """
    if ranked.empty:
        return ranked

    result = ranked.copy()
    available = [c for c in _PILLAR_COLS if c in result.columns]

    if not available:
        result["pillars_bullish"]  = 0
        result["pillar_agreement"] = 0.0
        return result

    above = (result[available] > threshold).sum(axis=1)
    result["pillars_bullish"]  = above.astype(int)
    result["pillar_agreement"] = above / len(available)

    return result


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def latest_rankings(
    ranked: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Extract one day's rankings as a flat ticker-indexed DataFrame.

    Parameters
    ----------
    ranked : pd.DataFrame
        Fully ranked panel (MultiIndex: date, ticker).
    date : pd.Timestamp or None
        Date to extract.  If None, uses the most recent date.

    Returns
    -------
    pd.DataFrame
        Ticker-indexed, sorted by rank (1 = best first).
    """
    if ranked.empty:
        return pd.DataFrame()

    dates = ranked.index.get_level_values("date").unique()

    if date is not None:
        if date not in dates:
            prior = dates[dates <= date]
            if prior.empty:
                return pd.DataFrame()
            date = prior[-1]
    else:
        date = dates[-1]

    snapshot = ranked.xs(date, level="date").copy()
    snapshot["date"] = date

    return snapshot.sort_values("rank")


def filter_top_n(
    snapshot: pd.DataFrame,
    n: int = 5,
) -> pd.DataFrame:
    """Filter a snapshot to the top N ranked symbols."""
    if snapshot.empty or "rank" not in snapshot.columns:
        return snapshot
    return snapshot[snapshot["rank"] <= n].copy()


def filter_by_regime(
    snapshot: pd.DataFrame,
    regimes: list[str],
) -> pd.DataFrame:
    """Filter a snapshot to symbols in the given RS regimes."""
    if snapshot.empty or "rs_regime" not in snapshot.columns:
        return snapshot
    return snapshot[snapshot["rs_regime"].isin(regimes)].copy()


# ═══════════════════════════════════════════════════════════════
#  RANK HISTORY  (single ticker over time)
# ═══════════════════════════════════════════════════════════════

def rank_history(
    ranked: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """
    Extract one ticker's ranking history.

    Returns a date-indexed DataFrame showing rank, score, and
    change columns over time.
    """
    if ranked.empty:
        return pd.DataFrame()

    tickers = ranked.index.get_level_values("ticker").unique()
    if ticker not in tickers:
        return pd.DataFrame()

    return ranked.xs(ticker, level="ticker").copy()


# ═══════════════════════════════════════════════════════════════
#  SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════

def rankings_summary(ranked: pd.DataFrame) -> dict:
    """
    Compute summary statistics for the latest day's rankings.

    Returns a dict with keys: date, universe_size, mean_composite,
    median_composite, std_composite, spread, top_ticker, top_score,
    bottom_ticker, bottom_score, regime_distribution.
    """
    snap = latest_rankings(ranked)
    if snap.empty:
        return {}

    comp = snap.get("score_composite")

    summary: dict = {
        "date":             snap["date"].iloc[0] if "date" in snap.columns else None,
        "universe_size":    len(snap),
        "mean_composite":   comp.mean()   if comp is not None else None,
        "median_composite": comp.median() if comp is not None else None,
        "std_composite":    comp.std()    if comp is not None else None,
        "spread":           (comp.max() - comp.min()) if comp is not None else None,
        "top_ticker":       snap.index[0],
        "top_score":        comp.iloc[0]  if comp is not None else None,
        "bottom_ticker":    snap.index[-1],
        "bottom_score":     comp.iloc[-1] if comp is not None else None,
    }

    if "rs_regime" in snap.columns:
        summary["regime_distribution"] = (
            snap["rs_regime"].value_counts().to_dict()
        )

    if "pillar_agreement" in snap.columns:
        summary["mean_agreement"] = snap["pillar_agreement"].mean()

    return summary


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_rankings(
    scored_universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Full rankings pipeline.

    Parameters
    ----------
    scored_universe : dict
        {ticker: scored_df} — output of the scoring pipeline
        for each symbol in the universe.  May optionally include
        ``sig_*`` columns from ``strategy/signals.py``; these
        are carried forward into the ranked panel.

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) panel with all scores,
        ranks, rank changes, pillar agreement metrics, and
        any per-ticker signal gate columns.
    """
    panel = build_rankings_panel(scored_universe)
    if panel.empty:
        return pd.DataFrame()

    ranked = rank_universe(panel)
    ranked = compute_rank_changes(ranked)
    ranked = compute_pillar_agreement(ranked)

    return ranked


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def rankings_report(
    ranked: pd.DataFrame,
    top_n: int | None = None,
    breadth_regime: str = "unknown",
    breadth_score: float = 0.0,
) -> str:
    """
    Formatted text report of the latest rankings.

    Parameters
    ----------
    ranked : pd.DataFrame
        Output of ``compute_all_rankings()``.
    top_n : int or None
        If set, only show the top N symbols.  None = show all.
    breadth_regime : str
        Current breadth regime label (for the header).
    breadth_score : float
        Current breadth score (0–1) for the header.

    Returns
    -------
    str
        Human-readable rankings report.
    """
    if ranked.empty:
        return "No rankings data available."

    snap = latest_rankings(ranked)
    if snap.empty:
        return "No rankings data available."

    summary = rankings_summary(ranked)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )

    ln: list[str] = []
    div = "=" * 72
    sub = "-" * 72

    # ── Header ────────────────────────────────────────────────
    ln.append(div)
    ln.append(f"UNIVERSE RANKINGS — {date_str}")
    ln.append(div)
    ln.append(
        f"  Universe:      {summary.get('universe_size', 0)} symbols"
    )
    ln.append(
        f"  Breadth:       {breadth_regime} ({breadth_score:.3f})"
    )
    ln.append(
        f"  Mean score:    {summary.get('mean_composite', 0):.3f}"
    )
    ln.append(
        f"  Median score:  {summary.get('median_composite', 0):.3f}"
    )
    ln.append(
        f"  Spread:        {summary.get('spread', 0):.3f}  "
        f"(top {summary.get('top_score', 0):.3f} → "
        f"bottom {summary.get('bottom_score', 0):.3f})"
    )
    if summary.get("mean_agreement") is not None:
        ln.append(
            f"  Mean agree:    {summary['mean_agreement']:.0%}"
        )

    # ── Rankings table ────────────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("RANKINGS")
    ln.append(sub)

    display = filter_top_n(snap, top_n) if top_n else snap

    pillar_present = [c for c in _PILLAR_COLS if c in display.columns]
    short_names = {
        "score_rotation":       "Rot",
        "score_momentum":       "Mom",
        "score_volatility":     "Vol",
        "score_microstructure": "Micro",
        "score_breadth":        "Brdth",
    }

    # Header line
    hdr = f"  {'#':>3}  {'Ticker':<7} {'Comp':>6}"
    for pc in pillar_present:
        hdr += f" {short_names.get(pc, pc[-5:]):>6}"
    hdr += f"  {'Regime':<12} {'1d':>7} {'5d':>7} {'Agree':>5} {'Δ':>3}"
    ln.append(hdr)

    sep = f"  {'───':>3}  {'───────':<7} {'──────':>6}"
    for _ in pillar_present:
        sep += f" {'──────':>6}"
    sep += (
        f"  {'────────────':<12}"
        f" {'───────':>7} {'───────':>7} {'─────':>5} {'───':>3}"
    )
    ln.append(sep)

    for ticker, row in display.iterrows():
        rank_val = int(row.get("rank", 0))
        comp_val = row.get("score_composite", 0)
        regime   = str(row.get("rs_regime", "?"))
        ret_1d   = row.get("ret_1d", np.nan)
        ret_5d   = row.get("ret_5d", np.nan)
        agree    = row.get("pillar_agreement", 0)
        delta    = int(row.get("rank_change", 0))

        line = f"  {rank_val:>3}  {ticker:<7} {comp_val:>6.3f}"
        for pc in pillar_present:
            v = row.get(pc, 0)
            line += f" {v:>6.3f}" if pd.notna(v) else f" {'—':>6}"

        ret_1d_str = f"{ret_1d:>+7.1%}" if pd.notna(ret_1d) else f"{'—':>7}"
        ret_5d_str = f"{ret_5d:>+7.1%}" if pd.notna(ret_5d) else f"{'—':>7}"
        delta_str  = f"{delta:+d}" if delta != 0 else "0"

        line += (
            f"  {regime:<12}"
            f" {ret_1d_str}"
            f" {ret_5d_str}"
            f" {agree:>5.0%}"
            f" {delta_str:>3}"
        )
        ln.append(line)

    # ── Top movers ────────────────────────────────────────────
    if "rank_change" in snap.columns:
        ln.append("")
        ln.append(sub)
        ln.append("TOP MOVERS")
        ln.append(sub)

        risers = snap[snap["rank_change"] > 0].sort_values(
            "rank_change", ascending=False
        )
        fallers = snap[snap["rank_change"] < 0].sort_values(
            "rank_change", ascending=True
        )

        if not risers.empty:
            parts = [
                f"{t} ({int(r):+d})"
                for t, r in risers["rank_change"].head(5).items()
            ]
            ln.append(f"  Risers:   {', '.join(parts)}")
        else:
            ln.append(f"  Risers:   (none)")

        if not fallers.empty:
            parts = [
                f"{t} ({int(r):+d})"
                for t, r in fallers["rank_change"].head(5).items()
            ]
            ln.append(f"  Fallers:  {', '.join(parts)}")
        else:
            ln.append(f"  Fallers:  (none)")

    # ── Regime distribution ───────────────────────────────────
    if "rs_regime" in snap.columns:
        ln.append("")
        ln.append(sub)
        ln.append("REGIME DISTRIBUTION")
        ln.append(sub)

        n_total = len(snap)
        regime_counts = snap["rs_regime"].value_counts()
        for regime in ["leading", "improving", "weakening", "lagging"]:
            cnt = regime_counts.get(regime, 0)
            frac = cnt / n_total if n_total > 0 else 0
            bar = "█" * int(frac * 30)
            ln.append(
                f"  {regime:<12} {cnt:>2} / {n_total}"
                f"  ({frac:>4.0%})  {bar}"
            )

    # ── Pillar agreement ──────────────────────────────────────
    if "pillars_bullish" in snap.columns and pillar_present:
        ln.append("")
        ln.append(sub)
        ln.append("PILLAR AGREEMENT")
        ln.append(sub)

        n_pillars = len(pillar_present)
        for i in range(n_pillars, -1, -1):
            cnt = (snap["pillars_bullish"] == i).sum()
            if cnt > 0:
                matched = list(snap[snap["pillars_bullish"] == i].index)
                ticker_str = ", ".join(matched[:6])
                if len(matched) > 6:
                    ticker_str += f" (+{len(matched) - 6} more)"
                ln.append(
                    f"  {i}/{n_pillars} bullish:"
                    f"  {cnt} symbol{'s' if cnt != 1 else ''}"
                    f"  ({ticker_str})"
                )

    return "\n".join(ln)