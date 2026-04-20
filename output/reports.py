"""
output/reports.py
-----------------
Comprehensive strategy reports that combine rankings, signals,
breadth, gate diagnostics, and (optional) backtest performance
into a single unified report.

Layers
──────
  daily_report()          One-day strategy snapshot
  transition_report()     Recent signal changes
  breadth_section()       Market breadth analysis section
  strategy_overview()     Static strategy rules reference
  performance_report()    Backtest results (when available)
  generate_full_report()  Master — combines all sections

Each function returns a plain-text string.  The master function
concatenates whichever sections are available, so it works
both during live signal generation (no backtest) and after a
historical simulation.

NOTE: compute/breadth.py already exports ``breadth_report()``
      which provides a historical breadth dump with lookback.
      This module's ``breadth_section()`` is a shorter summary
      intended as one section of the unified strategy report.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from output.signals import (
    SignalConfig,
    BUY, HOLD, SELL, NEUTRAL,
    latest_signals,
    signal_changes,
    active_positions,
    signals_summary,
    compute_turnover,
    _count_gates,
)
from output.rankings import (
    latest_rankings,
    rankings_summary,
)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

_DIV = "=" * 76
_SUB = "-" * 76
_THIN = "·" * 76

_REGIME_ICON = {
    "leading":   "🟢",
    "improving": "🔵",
    "weakening": "🟡",
    "lagging":   "🔴",
}

_SIG_ICON = {
    BUY:     "🟢 BUY ",
    HOLD:    "🔵 HOLD",
    SELL:    "🔴 SELL",
    NEUTRAL: "⚪ —   ",
}


# ═══════════════════════════════════════════════════════════════
#  DAILY REPORT
# ═══════════════════════════════════════════════════════════════

def daily_report(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> str:
    """
    Comprehensive single-day strategy report.

    Includes: market context, active positions with gate
    diagnostics, eligible watchlist, exit candidates, recent
    transitions, and turnover metrics.
    """
    if config is None:
        config = SignalConfig()
    if signals_df.empty:
        return "No data available for daily report."

    snap = latest_signals(signals_df)
    if snap.empty:
        return "No data available for daily report."

    summary = signals_summary(signals_df)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )
    has_gates = "sig_confirmed" in snap.columns

    ln: list[str] = []

    # ── Header ────────────────────────────────────────────
    ln.append(_DIV)
    ln.append(f"  DAILY STRATEGY REPORT — {date_str}")
    ln.append(_DIV)

    # Market context
    br_regime = "unknown"
    br_score = 0.0
    if breadth is not None and not breadth.empty:
        br_regime = breadth["breadth_regime"].iloc[-1]
        if "breadth_score" in breadth.columns:
            br_score = breadth["breadth_score"].iloc[-1]

    ln.append("")
    ln.append("  MARKET CONTEXT")
    ln.append(f"  Breadth regime:  {br_regime} ({br_score:.3f})")
    ln.append(
        f"  Positions:       {summary.get('n_active', 0)}"
        f" / {config.max_positions} max"
    )
    ln.append(
        f"  Entry mode:      "
        f"{'sig_confirmed (6 gates)' if has_gates else 'score threshold (fallback)'}"
    )
    ln.append(
        f"  Signal mix:      "
        f"BUY {summary.get('n_buy', 0)}  "
        f"HOLD {summary.get('n_hold', 0)}  "
        f"SELL {summary.get('n_sell', 0)}  "
        f"NEUTRAL {summary.get('n_neutral', 0)}"
    )
    if summary.get("mean_strength") is not None:
        ln.append(
            f"  Mean conviction: {summary['mean_strength']:.3f}"
        )

    # ── Active positions ──────────────────────────────────
    ln.append("")
    ln.append(_SUB)
    ln.append("  ACTIVE POSITIONS")
    ln.append(_SUB)

    active = snap[snap["signal"].isin([BUY, HOLD])]
    if active.empty:
        ln.append("  (no active positions)")
    else:
        for ticker, row in active.iterrows():
            sig = _SIG_ICON.get(row["signal"], row["signal"])
            rank = int(row.get("rank", 0))
            comp = row.get("score_composite", 0)
            strength = row.get("signal_strength", 0)
            regime = str(row.get("rs_regime", "?"))
            r_icon = _REGIME_ICON.get(regime, "")
            ret_1d = row.get("ret_1d", np.nan)
            ret_5d = row.get("ret_5d", np.nan)

            r1 = f"{ret_1d:+.1%}" if pd.notna(ret_1d) else "—"
            r5 = f"{ret_5d:+.1%}" if pd.notna(ret_5d) else "—"

            ln.append(
                f"  {sig}  {ticker:<6}  "
                f"#{rank}  score={comp:.3f}  "
                f"str={strength:.3f}  "
                f"{r_icon} {regime:<11}  "
                f"1d={r1}  5d={r5}"
            )

            # Gate detail for active positions
            if has_gates:
                gates = _format_gate_line(row)
                ln.append(f"           {gates}")

    # ── Gate diagnostics for full universe ────────────────
    if has_gates:
        ln.append("")
        ln.append(_SUB)
        ln.append("  UNIVERSE GATE DIAGNOSTICS")
        ln.append(_SUB)

        for ticker in snap.sort_values("rank").index:
            row = snap.loc[ticker]
            rank = int(row.get("rank", 0))
            conf = "CONF" if row.get("sig_confirmed") == 1 else "—"
            reason = str(row.get("sig_reason", ""))
            gates = _format_gate_line(row)
            ln.append(
                f"  #{rank:<3} {ticker:<6} [{conf:<4}] "
                f"{gates}  {reason}"
            )

    # ── Watchlist ─────────────────────────────────────────
    if "entry_eligible" in snap.columns:
        watchlist = snap[
            (snap["signal"] == NEUTRAL)
            & (snap["entry_eligible"])
        ]
        if not watchlist.empty:
            ln.append("")
            ln.append(_SUB)
            ln.append("  WATCHLIST (eligible, no slot)")
            ln.append(_SUB)
            for ticker, row in watchlist.iterrows():
                ln.append(
                    f"  ○ {ticker:<6}  "
                    f"#{int(row.get('rank', 0))}  "
                    f"score={row.get('score_composite', 0):.3f}  "
                    f"{row.get('rs_regime', '?')}"
                )

    # ── Exit candidates ───────────────────────────────────
    if "exit_triggered" in snap.columns:
        exits = snap[
            (snap["signal"].isin([BUY, HOLD]))
            & (snap["exit_triggered"])
        ]
        if not exits.empty:
            ln.append("")
            ln.append(_SUB)
            ln.append("  ⚠ EXIT CANDIDATES (threshold breached)")
            ln.append(_SUB)
            for ticker, row in exits.iterrows():
                ln.append(
                    f"  ✕ {ticker:<6}  "
                    f"#{int(row.get('rank', 0))}  "
                    f"score={row.get('score_composite', 0):.3f}"
                )

    # ── Recent transitions ────────────────────────────────
    changes = signal_changes(signals_df)
    if not changes.empty:
        recent = changes.tail(10)
        ln.append("")
        ln.append(_SUB)
        ln.append("  RECENT TRANSITIONS (last 10)")
        ln.append(_SUB)
        for (dt, tkr), row in recent.iterrows():
            dt_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            ln.append(
                f"  {dt_str}  {tkr:<6}  "
                f"{row.get('transition', '?')}"
            )

    # ── Turnover ──────────────────────────────────────────
    turnover = compute_turnover(signals_df, lookback=20)
    if not turnover.empty:
        ln.append("")
        ln.append(_SUB)
        ln.append("  TURNOVER")
        ln.append(_SUB)
        total_buys = int(turnover["buys"].sum())
        total_sells = int(turnover["sells"].sum())
        avg_active = turnover["active"].mean()
        roll_turn = turnover["rolling_turnover"].iloc[-1]
        ln.append(f"  Total entries:    {total_buys}")
        ln.append(f"  Total exits:      {total_sells}")
        ln.append(f"  Avg active:       {avg_active:.1f}")
        ln.append(f"  Rolling turnover: {roll_turn:.3f} (20d)")

    ln.append("")
    ln.append(_DIV)
    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  BREADTH SECTION
# ═══════════════════════════════════════════════════════════════

def breadth_section(
    breadth: pd.DataFrame | None,
) -> str:
    """
    Market breadth analysis section for the unified report.

    This is a *report section formatter* — a short summary of
    the current breadth state suitable for embedding in the
    strategy report.  For a detailed historical breadth dump
    with configurable lookback, use ``compute.breadth.breadth_report()``.
    """
    if breadth is None or breadth.empty:
        return "No breadth data available."

    ln: list[str] = []
    ln.append(_SUB)
    ln.append("  BREADTH ANALYSIS")
    ln.append(_SUB)

    regime = breadth["breadth_regime"].iloc[-1]
    score = (
        breadth["breadth_score"].iloc[-1]
        if "breadth_score" in breadth.columns else 0
    )
    smooth = (
        breadth["breadth_score_smooth"].iloc[-1]
        if "breadth_score_smooth" in breadth.columns else score
    )

    ln.append(f"  Current regime:  {regime}")
    ln.append(f"  Raw score:       {score:.3f}")
    ln.append(f"  Smoothed score:  {smooth:.3f}")

    # Regime history (last 5 unique)
    if "breadth_regime" in breadth.columns:
        regimes = breadth["breadth_regime"].dropna()
        if len(regimes) > 0:
            shifted = regimes != regimes.shift(1)
            transitions = regimes[shifted].tail(5)
            if len(transitions) > 0:
                parts = []
                for dt, r in transitions.items():
                    d = (
                        dt.strftime("%m-%d")
                        if hasattr(dt, "strftime") else str(dt)
                    )
                    parts.append(f"{d}: {r}")
                ln.append(f"  Recent shifts:   {' → '.join(parts)}")

    # Score distribution
    if "breadth_score" in breadth.columns:
        bs = breadth["breadth_score"].dropna()
        if len(bs) > 20:
            ln.append(f"  Score range:     "
                      f"[{bs.min():.3f}, {bs.max():.3f}]")
            ln.append(f"  20d mean:        "
                      f"{bs.tail(20).mean():.3f}")

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  STRATEGY OVERVIEW
# ═══════════════════════════════════════════════════════════════

def strategy_overview(
    config: SignalConfig | None = None,
) -> str:
    """
    Static description of the strategy rules and parameters.
    Useful as a reference appendix in any report.
    """
    if config is None:
        config = SignalConfig()

    has_gates = True  # describe full system

    ln: list[str] = []
    ln.append(_DIV)
    ln.append("  STRATEGY OVERVIEW")
    ln.append(_DIV)

    ln.append("")
    ln.append("  Per-Ticker Quality Gates (strategy/signals.py)")
    ln.append(_THIN)
    ln.append("  1. Score threshold  — score_adjusted ≥ entry_min")
    ln.append("  2. RS regime        — stock in leading/improving")
    ln.append("  3. Sector regime    — sector tide favourable")
    ln.append("  4. Breadth regime   — market not weak")
    ln.append("  5. Momentum streak  — N consecutive days > 0.5")
    ln.append("  6. Cooldown         — not recently exited")
    ln.append("  All six must pass → sig_confirmed = 1")

    ln.append("")
    ln.append("  Portfolio-Level Signals (output/signals.py)")
    ln.append(_THIN)
    ln.append(
        f"  Entry:   sig_confirmed AND rank ≤ "
        f"{config.entry_rank_max}"
    )
    ln.append(
        f"  Exit:    rank > {config.exit_rank_max} OR "
        f"score < {config.exit_score_min}"
    )
    ln.append(
        f"  Max positions:   {config.max_positions}"
    )
    ln.append(
        f"  Rank hysteresis: enter ≤ {config.entry_rank_max}, "
        f"exit > {config.exit_rank_max}"
    )
    ln.append(
        f"  Breadth breaker: "
        f"{config.breadth_bearish_action} when "
        f"regime ∈ {config.breadth_bearish}"
    )

    ln.append("")
    ln.append("  Signal Strength Weights")
    ln.append(_THIN)
    ln.append(f"  Composite score:  {config.w_score:.0%}")
    ln.append(f"  Rank percentile:  {config.w_rank:.0%}")
    ln.append(f"  Pillar agreement: {config.w_agreement:.0%}")
    ln.append(f"  Regime quality:   {config.w_regime:.0%}")
    ln.append(f"  Breadth quality:  {config.w_breadth:.0%}")

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE REPORT
# ═══════════════════════════════════════════════════════════════

def performance_report(
    backtest_result,
) -> str:
    """
    Format backtest results into a text report section.

    Parameters
    ----------
    backtest_result
        A ``BacktestResult`` from ``portfolio.backtest``.
        If None, returns placeholder text.
    """
    if backtest_result is None:
        return "No backtest results available."

    m = backtest_result.metrics
    if not m:
        return "No performance metrics computed."

    ln: list[str] = []
    ln.append(_DIV)
    ln.append("  BACKTEST PERFORMANCE")
    ln.append(_DIV)

    ln.append("")
    ln.append("  Returns")
    ln.append(_THIN)
    ln.append(
        f"  Total return:     "
        f"{m.get('total_return', 0):+.2%}"
    )
    ln.append(
        f"  CAGR:             "
        f"{m.get('cagr', 0):+.2%}"
    )
    ln.append(
        f"  Volatility (ann): "
        f"{m.get('annual_volatility', 0):.2%}"
    )

    ln.append("")
    ln.append("  Risk-Adjusted")
    ln.append(_THIN)
    ln.append(
        f"  Sharpe ratio:     "
        f"{m.get('sharpe_ratio', 0):.3f}"
    )
    ln.append(
        f"  Sortino ratio:    "
        f"{m.get('sortino_ratio', 0):.3f}"
    )
    ln.append(
        f"  Calmar ratio:     "
        f"{m.get('calmar_ratio', 0):.3f}"
    )

    ln.append("")
    ln.append("  Drawdown")
    ln.append(_THIN)
    ln.append(
        f"  Max drawdown:     "
        f"{m.get('max_drawdown', 0):.2%}"
    )
    ln.append(
        f"  Max DD duration:  "
        f"{m.get('max_dd_duration', 0)} days"
    )
    ln.append(
        f"  Current DD:       "
        f"{m.get('current_drawdown', 0):.2%}"
    )

    ln.append("")
    ln.append("  Trading")
    ln.append(_THIN)
    ln.append(
        f"  Total trades:     "
        f"{m.get('total_trades', 0)}"
    )
    ln.append(
        f"  Win rate:         "
        f"{m.get('win_rate', 0):.1%}"
    )
    ln.append(
        f"  Profit factor:    "
        f"{m.get('profit_factor', 0):.2f}"
    )
    ln.append(
        f"  Avg win / loss:   "
        f"{m.get('avg_win', 0):+.2%} / "
        f"{m.get('avg_loss', 0):+.2%}"
    )
    ln.append(
        f"  Total commission: "
        f"${m.get('total_commission', 0):,.2f}"
    )

    ln.append("")
    ln.append("  Capital")
    ln.append(_THIN)
    ln.append(
        f"  Initial:          "
        f"${m.get('initial_capital', 0):,.2f}"
    )
    ln.append(
        f"  Final:            "
        f"${m.get('final_capital', 0):,.2f}"
    )
    ln.append(
        f"  Peak:             "
        f"${m.get('peak_capital', 0):,.2f}"
    )

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_full_report(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
    backtest_result=None,
    include_strategy: bool = True,
) -> str:
    """
    Combine all report sections into one comprehensive document.

    Includes whichever sections have data: daily signals are
    always included, breadth if provided, backtest performance
    if a result object is passed, and the strategy overview
    if requested.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``compute_all_signals()``.
    breadth : pd.DataFrame or None
        Breadth data.
    config : SignalConfig or None
        Portfolio-level config.
    backtest_result : BacktestResult or None
        Output of ``run_backtest()``.
    include_strategy : bool
        Whether to append the strategy rules reference.

    Returns
    -------
    str
        Full text report.
    """
    if config is None:
        config = SignalConfig()

    sections: list[str] = []

    # Daily snapshot
    sections.append(daily_report(signals_df, breadth, config))

    # Breadth detail
    if breadth is not None and not breadth.empty:
        sections.append(breadth_section(breadth))

    # Backtest performance
    if backtest_result is not None:
        sections.append(performance_report(backtest_result))

    # Strategy reference
    if include_strategy:
        sections.append(strategy_overview(config))

    return "\n\n".join(sections)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _format_gate_line(row: pd.Series) -> str:
    """Format per-ticker gate status as a compact string."""
    gate_cols = [
        ("sig_regime_ok",   "Reg"),
        ("sig_sector_ok",   "Sec"),
        ("sig_breadth_ok",  "Brd"),
        ("sig_momentum_ok", "Mom"),
        ("sig_in_cooldown", "CD"),
    ]

    parts: list[str] = []
    for col, label in gate_cols:
        if col not in row.index:
            continue
        val = row[col]
        if col == "sig_in_cooldown":
            icon = "✕" if val else "✓"
        else:
            icon = "✓" if val else "✕"
        parts.append(f"{icon}{label}")

    return "  ".join(parts) if parts else "—"