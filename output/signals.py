"""
output/signals.py
-----------------
Portfolio-level trade signal generation.

Layers on top of ``strategy/signals.py`` (per-ticker quality
gates) and ``output/rankings.py`` (cross-sectional rankings)
to produce final portfolio signals: BUY, HOLD, SELL, NEUTRAL.

Architecture
────────────
  strategy/signals.py answers "Is this ticker trade-worthy?"
    · Six per-ticker gates: regime, sector, breadth, momentum,
      cooldown, score threshold
    · Produces: sig_confirmed (0/1), sig_exit (0/1),
      sig_position_pct, gate diagnostics

  output/rankings.py answers "Where does this ticker rank?"
    · Cross-sectional ranking by composite score
    · Produces: rank, pillar_agreement, universe_size

  output/signals.py answers "Which tickers do we hold?"     ← this file
    · Uses sig_confirmed for entry qualification
    · Adds cross-sectional rank filter with hysteresis
    · Enforces position limits
    · Portfolio-level breadth circuit breaker
    · Conviction scoring for downstream sizing

  When strategy/signals.py has NOT been run (sig_confirmed
  absent from the panel), entry falls back to:
    score_composite ≥ entry_score_min

Signal Types
────────────
  BUY      Enter new position
  HOLD     Maintain existing position
  SELL     Exit position
  NEUTRAL  No position

Entry (AND — all required)
──────────────────────────
  sig_confirmed == 1   (or score ≥ entry_score_min as fallback)
  rank ≤ entry_rank_max
  slots available      (< max_positions)
  breadth not bearish

Exit (OR — any one fires)
─────────────────────────
  rank > exit_rank_max
  score_composite < exit_score_min
  breadth bearish + exit_all mode

Hysteresis
──────────
  Rank band: enter at rank ≤ 5, exit only at rank > 8.
  A symbol entering at rank 3 stays through rank 7 without
  churning.

  Per-ticker hysteresis (cooldown, momentum streak) is
  handled by strategy/signals.py through sig_confirmed.
  The two layers stack: a ticker must survive both the
  per-ticker quality bar AND the cross-sectional rank bar.

Signal Strength
───────────────
  A 0–1 conviction score blending:
    composite score    30%   — raw quality
    rank percentile    20%   — position in universe
    pillar agreement   20%   — breadth of confirmation
    regime quality     15%   — RS regime desirability
    breadth quality    15%   — market health

Pipeline
────────
  ranked_panel (with optional sig_* columns from strategy/)
       ↓
  check_entry_eligible()     — boolean per row
       ↓
  check_exit_triggered()     — boolean per row
       ↓
  generate_signals()         — stateful BUY/HOLD/SELL/NEUTRAL
       ↓
  compute_signal_strength()  — 0–1 conviction score
       ↓
  compute_all_signals()      — master orchestrator
       ↓
  latest_signals()           — single-day snapshot
  signal_changes()           — entries / exits / transitions
  signal_history()           — single ticker over time
  active_positions()         — currently held symbols
  compute_turnover()         — entry/exit frequency
  signals_summary()          — summary statistics dict
  signals_report()           — formatted text report
"""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

BUY     = "BUY"
HOLD    = "HOLD"
SELL    = "SELL"
NEUTRAL = "NEUTRAL"

_REGIME_QUALITY: dict[str, float] = {
    "leading":    1.00,
    "improving":  0.75,
    "weakening":  0.25,
    "lagging":    0.00,
}

_BREADTH_QUALITY: dict[str, float] = {
    "strong":   1.00,
    "healthy":  0.80,
    "neutral":  0.50,
    "caution":  0.30,
    "weak":     0.10,
    "critical": 0.00,
}


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class SignalConfig:
    """
    Portfolio-level signal thresholds.

    Per-ticker quality thresholds (regime, sector, momentum,
    cooldown, score) live in ``SIGNAL_PARAMS`` and are enforced
    by ``strategy/signals.py``.  This config controls only the
    cross-sectional and portfolio layers.
    the portfolio-level allocator. It takes the full cross-sectional panel (after every ticker has been scored and gated) and 
    answers "which of the trade-worthy tickers do we actually hold?" through rank filtering with hysteresis, position limits, and 
    a portfolio-level breadth circuit breaker.
    """

    # ── Rank thresholds (hysteresis band) ─────────────────
    entry_rank_max: int = 5
    exit_rank_max:  int = 8

    # ── Score thresholds ──────────────────────────────────
    #    exit_score_min always applies as an OR exit trigger.
    #    entry_score_min is used only when sig_confirmed is
    #    absent (strategy/signals.py was not run).
    entry_score_min: float = 0.40
    exit_score_min:  float = 0.30

    # ── Breadth circuit breaker ───────────────────────────
    breadth_bearish:        tuple[str, ...] = ("weak", "critical")
    breadth_bearish_action: str = "reduce"    # "reduce" or "exit_all"

    # ── Position limits ───────────────────────────────────
    max_positions: int = 5

    # ── Signal strength weights ───────────────────────────
    w_score:     float = 0.30
    w_rank:      float = 0.20
    w_agreement: float = 0.20
    w_regime:    float = 0.15
    w_breadth:   float = 0.15


# ═══════════════════════════════════════════════════════════════
#  ENTRY / EXIT ELIGIBILITY
# ═══════════════════════════════════════════════════════════════

def check_entry_eligible(
    ranked: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.Series:
    """
    Boolean mask: True where entry criteria are met.

    When ``sig_confirmed`` is present (strategy/signals.py
    was run on each ticker before ranking):

        sig_confirmed == 1  AND  rank ≤ entry_rank_max

    Fallback (``sig_confirmed`` absent):

        score_composite ≥ entry_score_min  AND  rank ≤ entry_rank_max
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.Series(dtype=bool)

    rank_ok = ranked["rank"] <= config.entry_rank_max

    if "sig_confirmed" in ranked.columns:
        ticker_ok = ranked["sig_confirmed"] == 1
    else:
        ticker_ok = ranked["score_composite"] >= config.entry_score_min

    return rank_ok & ticker_ok


def check_exit_triggered(
    ranked: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.Series:
    """
    Boolean mask: True where any exit threshold is breached.

    Triggers (OR):
      rank              > exit_rank_max
      score_composite   < exit_score_min
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.Series(dtype=bool)

    breach = ranked["rank"] > config.exit_rank_max
    breach = breach | (
        ranked["score_composite"] < config.exit_score_min
    )

    return breach


# ═══════════════════════════════════════════════════════════════
#  STATEFUL SIGNAL GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_signals(
    ranked: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Generate BUY / HOLD / SELL / NEUTRAL signals with hysteresis.

    Processes the ranked panel date-by-date, maintaining a set
    of held positions.

    For held positions each day:
      · Force exit if breadth bearish + exit_all mode
      · Exit if any exit trigger fires (rank or score)
      · Otherwise HOLD

    For non-held tickers each day:
      · Skip if breadth bearish (both modes block new entries)
      · BUY if entry eligible and slots available
      · Otherwise NEUTRAL

    New entries are prioritised by composite score (highest
    first) when more candidates than available slots.

    Parameters
    ----------
    ranked : pd.DataFrame
        MultiIndex (date, ticker) panel from
        ``compute_all_rankings()``, optionally containing
        ``sig_confirmed`` / ``sig_exit`` columns from
        ``strategy/signals.py``.
    breadth : pd.DataFrame or None
        Breadth data with ``breadth_regime`` column, indexed
        by date.
    config : SignalConfig or None
        Portfolio-level thresholds.

    Returns
    -------
    pd.DataFrame
        Input panel with ``signal``, ``entry_eligible``,
        ``exit_triggered``, and ``in_position`` columns added.
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.DataFrame()

    # Pre-compute masks over the entire panel
    entry_mask = check_entry_eligible(ranked, config)
    exit_mask  = check_exit_triggered(ranked, config)

    dates = ranked.index.get_level_values("date").unique().sort_values()

    held: set[str]                = set()
    signals: dict[tuple, str]     = {}

    for date in dates:

        # ── Day slices ────────────────────────────────────
        try:
            day_data  = ranked.xs(date, level="date")
            day_entry = entry_mask.xs(date, level="date")
            day_exit  = exit_mask.xs(date, level="date")
        except KeyError:
            continue

        day_tickers = set(day_data.index.tolist())

        # ── Breadth circuit breaker ───────────────────────
        breadth_is_bearish = False
        if (
            breadth is not None
            and "breadth_regime" in breadth.columns
            and date in breadth.index
        ):
            br = breadth.loc[date, "breadth_regime"]
            if isinstance(br, pd.Series):
                br = br.iloc[0]
            breadth_is_bearish = br in config.breadth_bearish

        day_signals: dict[str, str] = {}
        sells: set[str] = set()

        # ── 1. Process held positions ─────────────────────
        for ticker in list(held):
            if ticker not in day_tickers:
                # Ticker dropped from panel (no data today)
                held.discard(ticker)
                continue

            force_exit = (
                breadth_is_bearish
                and config.breadth_bearish_action == "exit_all"
            )

            if force_exit or day_exit.loc[ticker]:
                day_signals[ticker] = SELL
                sells.add(ticker)
            else:
                day_signals[ticker] = HOLD

        held -= sells

        # ── 2. New entries ────────────────────────────────
        #    Blocked when breadth is bearish (both modes).
        if not breadth_is_bearish:
            slots = config.max_positions - len(held)
            if slots > 0:
                candidates: list[tuple[str, float]] = []
                for ticker in day_tickers:
                    if ticker in day_signals:
                        continue          # already HOLD or SELL
                    if day_entry.loc[ticker]:
                        score = day_data.loc[
                            ticker, "score_composite"
                        ]
                        candidates.append((ticker, score))

                # Best composite score gets priority
                candidates.sort(key=lambda x: x[1], reverse=True)

                for ticker, _ in candidates[: max(0, slots)]:
                    day_signals[ticker] = BUY
                    held.add(ticker)

        # ── 3. Remainder → NEUTRAL ───────────────────────
        for ticker in day_tickers:
            if ticker not in day_signals:
                day_signals[ticker] = NEUTRAL

        # ── Store ─────────────────────────────────────────
        for ticker, sig in day_signals.items():
            signals[(date, ticker)] = sig

    # ── Assemble result ───────────────────────────────────
    sig_series = pd.Series(signals, name="signal")
    sig_series.index = pd.MultiIndex.from_tuples(
        sig_series.index, names=["date", "ticker"],
    )

    result = ranked.copy()
    result["signal"]         = sig_series
    result["signal"]         = result["signal"].fillna(NEUTRAL)
    result["entry_eligible"] = entry_mask
    result["exit_triggered"] = exit_mask
    result["in_position"]    = result["signal"].isin([BUY, HOLD])

    return result


# ═══════════════════════════════════════════════════════════════
#  SIGNAL STRENGTH  (CONVICTION)
# ═══════════════════════════════════════════════════════════════

def compute_signal_strength(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Compute 0–1 conviction score for each row.

    Components and default weights:
      score_composite    30%  — raw composite quality
      rank_factor        20%  — position in universe
      pillar_agreement   20%  — breadth of confirmation
      regime_quality     15%  — RS regime desirability
      breadth_quality    15%  — market breadth health

    Strength is zeroed for SELL and NEUTRAL signals.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``generate_signals()``.
    breadth : pd.DataFrame or None
        Breadth data for the breadth quality component.
    config : SignalConfig or None
        Weights.

    Returns
    -------
    pd.DataFrame
        Same frame with ``signal_strength`` column added.
    """
    if config is None:
        config = SignalConfig()
    if signals_df.empty:
        return signals_df

    result = signals_df.copy()
    active = result["signal"].isin([BUY, HOLD])

    # ── 1. Score factor (already 0–1) ─────────────────────
    score_f = result["score_composite"].clip(0, 1)

    # ── 2. Rank factor ────────────────────────────────────
    usize  = result["universe_size"].clip(lower=2)
    rank_f = (1.0 - (result["rank"] - 1) / (usize - 1)).clip(0, 1)

    # ── 3. Agreement factor ───────────────────────────────
    if "pillar_agreement" in result.columns:
        agree_f = result["pillar_agreement"].fillna(0.5)
    else:
        agree_f = pd.Series(0.5, index=result.index)

    # ── 4. Regime factor ──────────────────────────────────
    if "rs_regime" in result.columns:
        regime_f = (
            result["rs_regime"].map(_REGIME_QUALITY).fillna(0.5)
        )
    else:
        regime_f = pd.Series(0.5, index=result.index)

    # ── 5. Breadth factor ─────────────────────────────────
    breadth_f = pd.Series(0.5, index=result.index)
    if breadth is not None and "breadth_regime" in breadth.columns:
        b_quality = breadth["breadth_regime"].map(_BREADTH_QUALITY)
        dates     = result.index.get_level_values("date")
        breadth_f = b_quality.reindex(dates).fillna(0.5)
        breadth_f.index = result.index

    # ── Weighted blend ────────────────────────────────────
    strength = (
        config.w_score     * score_f
        + config.w_rank    * rank_f
        + config.w_agreement * agree_f
        + config.w_regime  * regime_f
        + config.w_breadth * breadth_f
    ).clip(0, 1)

    # Zero out non-active signals
    result["signal_strength"] = strength.where(active, 0.0)

    return result


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT / HISTORY EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def latest_signals(
    signals_df: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Extract one day's signals as a flat ticker-indexed DataFrame.

    Sorted: BUY first, then HOLD, then NEUTRAL, then SELL,
    each group sorted by rank.
    """
    if signals_df.empty:
        return pd.DataFrame()

    dates = signals_df.index.get_level_values("date").unique()

    if date is not None:
        if date not in dates:
            prior = dates[dates <= date]
            if prior.empty:
                return pd.DataFrame()
            date = prior[-1]
    else:
        date = dates[-1]

    snap = signals_df.xs(date, level="date").copy()
    snap["date"] = date

    _priority = {BUY: 0, HOLD: 1, NEUTRAL: 2, SELL: 3}
    snap["_sort"] = snap["signal"].map(_priority).fillna(4)
    snap = snap.sort_values(["_sort", "rank"]).drop(columns="_sort")

    return snap


def signal_changes(
    signals_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract rows where the signal changed from the prior day.

    Adds ``prev_signal`` and ``transition`` columns
    (e.g. ``NEUTRAL → BUY``, ``HOLD → SELL``).
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return pd.DataFrame()

    sig_wide  = signals_df["signal"].unstack(level="ticker")
    prev_wide = sig_wide.shift(1)

    changed_wide = sig_wide != prev_wide
    # First date always "changed" — exclude
    changed_wide.iloc[0] = False

    changed_long = changed_wide.stack()
    changed_long.name = "changed"
    changed_long.index.names = ["date", "ticker"]

    mask = changed_long[changed_long]
    if mask.empty:
        return pd.DataFrame()

    changes = signals_df.loc[mask.index].copy()

    prev_long = prev_wide.stack()
    prev_long.name = "prev_signal"
    prev_long.index.names = ["date", "ticker"]

    changes = changes.join(prev_long)
    changes["transition"] = (
        changes["prev_signal"].astype(str) + " → "
        + changes["signal"].astype(str)
    )

    return changes


def signal_history(
    signals_df: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Extract one ticker's signal history over time."""
    if signals_df.empty:
        return pd.DataFrame()

    tickers = signals_df.index.get_level_values("ticker").unique()
    if ticker not in tickers:
        return pd.DataFrame()

    return signals_df.xs(ticker, level="ticker").copy()


def active_positions(
    signals_df: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> list[str]:
    """
    Return tickers with BUY or HOLD on a given date.

    Sorted by rank (best first).
    """
    snap = latest_signals(signals_df, date)
    if snap.empty:
        return []

    active = snap[snap["signal"].isin([BUY, HOLD])]
    return active.index.tolist()


# ═══════════════════════════════════════════════════════════════
#  TURNOVER ANALYSIS
# ═══════════════════════════════════════════════════════════════

def compute_turnover(
    signals_df: pd.DataFrame,
    lookback: int = 20,
) -> pd.DataFrame:
    """
    Compute daily position turnover.

    turnover = (entries + exits) / universe_size per day,
    smoothed over a rolling window.

    Returns a date-indexed DataFrame with buys, sells, active
    positions count, daily turnover, and rolling turnover.
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return pd.DataFrame()

    dates = (
        signals_df.index.get_level_values("date")
        .unique().sort_values()
    )

    buys = signals_df[signals_df["signal"] == BUY].groupby(
        level="date",
    ).size()
    sells = signals_df[signals_df["signal"] == SELL].groupby(
        level="date",
    ).size()
    active = signals_df[
        signals_df["signal"].isin([BUY, HOLD])
    ].groupby(level="date").size()
    universe = signals_df.groupby(level="date").size()

    buys     = buys.reindex(dates, fill_value=0)
    sells    = sells.reindex(dates, fill_value=0)
    active   = active.reindex(dates, fill_value=0)
    universe = universe.reindex(dates, fill_value=1)

    turnover = pd.DataFrame({
        "buys":            buys,
        "sells":           sells,
        "active":          active,
        "daily_turnover":  (buys + sells) / universe.clip(lower=1),
    }, index=dates)

    turnover["rolling_turnover"] = (
        turnover["daily_turnover"]
        .rolling(lookback, min_periods=1)
        .mean()
    )

    return turnover


# ═══════════════════════════════════════════════════════════════
#  SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════

def signals_summary(signals_df: pd.DataFrame) -> dict:
    """
    Summary statistics for the latest day's signals.

    Returns dict with: date, n_buy, n_hold, n_sell, n_neutral,
    n_active, positions, mean_strength, total_strength,
    strongest, weakest, regime_mix.
    """
    snap = latest_signals(signals_df)
    if snap.empty:
        return {}

    active_snap = snap[snap["signal"].isin([BUY, HOLD])]

    summary: dict = {
        "date":      (
            snap["date"].iloc[0] if "date" in snap.columns
            else None
        ),
        "n_buy":     int((snap["signal"] == BUY).sum()),
        "n_hold":    int((snap["signal"] == HOLD).sum()),
        "n_sell":    int((snap["signal"] == SELL).sum()),
        "n_neutral": int((snap["signal"] == NEUTRAL).sum()),
        "n_active":  len(active_snap),
        "positions": active_snap.index.tolist(),
    }

    if "signal_strength" in snap.columns and not active_snap.empty:
        summary["mean_strength"]  = float(
            active_snap["signal_strength"].mean()
        )
        summary["total_strength"] = float(
            active_snap["signal_strength"].sum()
        )
        summary["strongest"] = active_snap[
            "signal_strength"
        ].idxmax()
        summary["weakest"] = active_snap[
            "signal_strength"
        ].idxmin()

    if "rs_regime" in active_snap.columns and not active_snap.empty:
        summary["regime_mix"] = (
            active_snap["rs_regime"].value_counts().to_dict()
        )

    return summary


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_signals(
    ranked: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Full portfolio signal pipeline.

    Parameters
    ----------
    ranked : pd.DataFrame
        Output of ``compute_all_rankings()``, optionally with
        ``sig_confirmed`` columns from ``strategy/signals.py``.
    breadth : pd.DataFrame or None
        Breadth data for circuit breaker and strength scoring.
    config : SignalConfig or None
        Portfolio-level thresholds.

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) panel with signal, strength,
        eligibility, and position columns appended.
    """
    if config is None:
        config = SignalConfig()

    signals = generate_signals(ranked, breadth, config)
    if signals.empty:
        return pd.DataFrame()

    signals = compute_signal_strength(signals, breadth, config)

    return signals


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def signals_report(
    signals_df: pd.DataFrame,
    breadth_regime: str = "unknown",
    breadth_score: float = 0.0,
    config: SignalConfig | None = None,
) -> str:
    """
    Formatted text report of the latest signals.

    Includes: header summary, active positions table, new
    entries, exits with reasons, watchlist, per-ticker gate
    diagnostics (when available), and config reference.
    """
    if config is None:
        config = SignalConfig()

    if signals_df.empty:
        return "No signal data available."

    snap = latest_signals(signals_df)
    if snap.empty:
        return "No signal data available."

    summary = signals_summary(signals_df)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )

    # Track whether per-ticker gates are present
    has_gates = "sig_confirmed" in snap.columns

    ln: list[str] = []
    div = "=" * 72
    sub = "-" * 72

    # ── Header ────────────────────────────────────────────
    ln.append(div)
    ln.append(f"TRADE SIGNALS — {date_str}")
    ln.append(div)
    ln.append(
        f"  Breadth:       {breadth_regime} ({breadth_score:.3f})"
    )
    ln.append(
        f"  Positions:     {summary.get('n_active', 0)} / "
        f"{config.max_positions} max"
    )
    ln.append(
        f"  BUY: {summary.get('n_buy', 0)}  "
        f"HOLD: {summary.get('n_hold', 0)}  "
        f"SELL: {summary.get('n_sell', 0)}  "
        f"NEUTRAL: {summary.get('n_neutral', 0)}"
    )
    if summary.get("mean_strength") is not None:
        ln.append(
            f"  Mean strength: {summary['mean_strength']:.3f}"
        )
    ln.append(
        f"  Ticker gates:  "
        f"{'active (strategy/signals.py)' if has_gates else 'fallback (score only)'}"
    )

    # ── Active positions ──────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("ACTIVE POSITIONS")
    ln.append(sub)

    active = snap[snap["signal"].isin([BUY, HOLD])]
    if active.empty:
        ln.append("  (no active positions)")
    else:
        hdr = (
            f"  {'Signal':<8} {'Ticker':<7} {'#':>3} "
            f"{'Comp':>6} {'Str':>5} {'Regime':<12} "
            f"{'1d':>7} {'5d':>7}"
        )
        if has_gates:
            hdr += f" {'Gates':>5}"
        ln.append(hdr)
        sep_line = (
            f"  {'────────':<8} {'───────':<7} {'───':>3} "
            f"{'──────':>6} {'─────':>5} {'────────────':<12} "
            f"{'───────':>7} {'───────':>7}"
        )
        if has_gates:
            sep_line += f" {'─────':>5}"
        ln.append(sep_line)

        for ticker, row in active.iterrows():
            sig      = row["signal"]
            rank_val = int(row.get("rank", 0))
            comp     = row.get("score_composite", 0)
            strength = row.get("signal_strength", 0)
            regime   = str(row.get("rs_regime", "?"))
            ret_1d   = row.get("ret_1d", np.nan)
            ret_5d   = row.get("ret_5d", np.nan)

            r1 = (
                f"{ret_1d:>+7.1%}" if pd.notna(ret_1d)
                else f"{'—':>7}"
            )
            r5 = (
                f"{ret_5d:>+7.1%}" if pd.notna(ret_5d)
                else f"{'—':>7}"
            )

            line = (
                f"  {sig:<8} {ticker:<7} {rank_val:>3} "
                f"{comp:>6.3f} {strength:>5.3f} {regime:<12} "
                f"{r1} {r5}"
            )
            if has_gates:
                gates_passed = _count_gates(row)
                line += f" {gates_passed:>5}"
            ln.append(line)

    # ── New entries ───────────────────────────────────────
    new_buys = snap[snap["signal"] == BUY]
    if not new_buys.empty:
        ln.append("")
        ln.append(sub)
        ln.append("NEW ENTRIES")
        ln.append(sub)
        for ticker, row in new_buys.iterrows():
            reason = row.get("sig_reason", "")
            ln.append(
                f"  → BUY  {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"strength={row.get('signal_strength', 0):.3f}  "
                f"{row.get('rs_regime', '?')}"
                f"{f'  ({reason})' if reason else ''}"
            )

    # ── Exits ─────────────────────────────────────────────
    exits = snap[snap["signal"] == SELL]
    if not exits.empty:
        ln.append("")
        ln.append(sub)
        ln.append("EXITS")
        ln.append(sub)
        for ticker, row in exits.iterrows():
            reasons: list[str] = []
            if row.get("rank", 0) > config.exit_rank_max:
                reasons.append(
                    f"rank {int(row['rank'])} > "
                    f"{config.exit_rank_max}"
                )
            if (
                row.get("score_composite", 1)
                < config.exit_score_min
            ):
                reasons.append(
                    f"score {row['score_composite']:.3f} < "
                    f"{config.exit_score_min:.3f}"
                )
            reason_str = (
                ", ".join(reasons) if reasons else "forced exit"
            )

            ln.append(
                f"  ✕ SELL {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"({reason_str})"
            )

    # ── Gate diagnostics (when strategy/signals.py active) ─
    if has_gates:
        ln.append("")
        ln.append(sub)
        ln.append("PER-TICKER GATE DIAGNOSTICS")
        ln.append(sub)
        gate_cols = [
            ("sig_regime_ok",   "Regime"),
            ("sig_sector_ok",   "Sector"),
            ("sig_breadth_ok",  "Breadth"),
            ("sig_momentum_ok", "Momentum"),
            ("sig_in_cooldown", "Cooldown"),
        ]
        available_gates = [
            (col, label) for col, label in gate_cols
            if col in snap.columns
        ]

        for ticker, row in snap.sort_values("rank").iterrows():
            flags: list[str] = []
            for col, label in available_gates:
                val = row.get(col, None)
                if val is None:
                    continue
                if col == "sig_in_cooldown":
                    flags.append(
                        f"{'✕' if val else '✓'} {label}"
                    )
                else:
                    flags.append(
                        f"{'✓' if val else '✕'} {label}"
                    )

            conf = (
                "CONF" if row.get("sig_confirmed") == 1
                else "—"
            )
            ln.append(
                f"  #{int(row.get('rank', 0)):<3} "
                f"{ticker:<7} [{conf:<4}] "
                f"{'  '.join(flags)}"
            )

    # ── Watchlist ─────────────────────────────────────────
    if "entry_eligible" in snap.columns:
        neutral_elig = snap[
            (snap["signal"] == NEUTRAL) & (snap["entry_eligible"])
        ]
    else:
        neutral_elig = pd.DataFrame()

    if not neutral_elig.empty:
        ln.append("")
        ln.append(sub)
        ln.append("WATCHLIST (eligible but no slot)")
        ln.append(sub)
        for ticker, row in neutral_elig.iterrows():
            ln.append(
                f"  ○ {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"{row.get('rs_regime', '?')}"
            )

    # ── Config reference ──────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("SIGNAL CONFIG (portfolio level)")
    ln.append(sub)
    ln.append(
        f"  Entry:  rank ≤ {config.entry_rank_max}"
        + (
            f", sig_confirmed == 1"
            if has_gates
            else f", score ≥ {config.entry_score_min:.2f}"
        )
    )
    ln.append(
        f"  Exit:   rank > {config.exit_rank_max}, "
        f"score < {config.exit_score_min:.2f}"
    )
    ln.append(
        f"  Max positions: {config.max_positions}  "
        f"Breadth bearish: {config.breadth_bearish_action}"
    )

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _count_gates(row: pd.Series) -> str:
    """Count passed / total per-ticker gates for display."""
    gate_cols = [
        "sig_regime_ok", "sig_sector_ok", "sig_breadth_ok",
        "sig_momentum_ok",
    ]
    cooldown_col = "sig_in_cooldown"

    total = 0
    passed = 0

    for col in gate_cols:
        if col in row.index:
            total += 1
            if row[col]:
                passed += 1

    if cooldown_col in row.index:
        total += 1
        if not row[cooldown_col]:
            passed += 1

    return f"{passed}/{total}" if total > 0 else "—"