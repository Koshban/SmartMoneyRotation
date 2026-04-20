"""
backtest/comparison.py
----------------------
Run multiple strategy variants over the same period and produce
a side-by-side comparison ranked by CAGR (or any metric).

Usage
-----
    from backtest.comparison import compare_strategies
    from backtest.data_loader import ensure_history

    data = ensure_history()
    results = compare_strategies(data, start="2010-01-01")
    print(results["report"])
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtest.engine import BacktestRun, StrategyConfig, run_backtest_period
from backtest.strategies import ALL_STRATEGIES, BASELINE
from backtest.metrics import metrics_report

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  COMPARE ALL STRATEGIES
# ═══════════════════════════════════════════════════════════════

def compare_strategies(
    data: dict[str, pd.DataFrame],
    *,
    strategies: list[StrategyConfig] | None = None,
    start: str | None = None,
    end: str | None = None,
    capital: float = 100_000.0,
    rank_by: str = "cagr",
) -> dict[str, Any]:
    """
    Run every strategy variant over the same period and return
    a comparison.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        From ``ensure_history()``.
    strategies : list[StrategyConfig] or None
        Strategies to test.  None = all from ``ALL_STRATEGIES``.
    start, end : str or None
        Date range.
    capital : float
        Initial capital for each run.
    rank_by : str
        Metric to rank by.  Default ``"cagr"``.

    Returns
    -------
    dict with keys:
        runs          list[BacktestRun]  — individual results
        table         pd.DataFrame       — comparison table
        report        str                — formatted text report
        best          BacktestRun        — best by rank_by metric
        worst         BacktestRun        — worst by rank_by metric
    """
    if strategies is None:
        strategies = list(ALL_STRATEGIES.values())

    logger.info(
        f"Comparing {len(strategies)} strategies "
        f"({start or 'earliest'} → {end or 'latest'})"
    )

    runs: list[BacktestRun] = []

    for i, strat in enumerate(strategies, 1):
        logger.info(
            f"[{i}/{len(strategies)}] Running: {strat.name}"
        )
        run = run_backtest_period(
            data,
            start=start,
            end=end,
            strategy=strat,
            capital=capital,
        )
        runs.append(run)

    # ── Build comparison table ────────────────────────────────
    table = _build_comparison_table(runs, rank_by=rank_by)

    # ── Text report ───────────────────────────────────────────
    report = _comparison_report(runs, table, rank_by)

    # ── Best / worst ──────────────────────────────────────────
    valid_runs = [r for r in runs if r.ok]
    best = max(valid_runs, key=lambda r: r.metrics.get(rank_by, -999)) if valid_runs else None
    worst = min(valid_runs, key=lambda r: r.metrics.get(rank_by, 999)) if valid_runs else None

    return {
        "runs": runs,
        "table": table,
        "report": report,
        "best": best,
        "worst": worst,
    }


# ═══════════════════════════════════════════════════════════════
#  COMPARISON TABLE
# ═══════════════════════════════════════════════════════════════

def _build_comparison_table(
    runs: list[BacktestRun],
    rank_by: str = "cagr",
) -> pd.DataFrame:
    """Build a DataFrame comparing all strategy runs."""
    rows = []
    for run in runs:
        m = run.metrics
        rows.append({
            "strategy":       run.strategy.name,
            "cagr":           m.get("cagr", None),
            "total_return":   m.get("total_return", None),
            "sharpe":         m.get("sharpe_ratio", None),
            "sortino":        m.get("sortino_ratio", None),
            "calmar":         m.get("calmar_ratio", None),
            "max_drawdown":   m.get("max_drawdown", None),
            "annual_vol":     m.get("annual_volatility", None),
            "win_rate":       m.get("win_rate", None),
            "total_trades":   m.get("total_trades", None),
            "profit_factor":  m.get("profit_factor", None),
            "excess_cagr":    m.get("excess_cagr", None),
            "info_ratio":     m.get("information_ratio", None),
            "final_capital":  m.get("final_capital", None),
            "best_year":      m.get("best_year", None),
            "worst_year":     m.get("worst_year", None),
            "elapsed_s":      run.elapsed_seconds,
            "error":          run.error,
        })

    df = pd.DataFrame(rows)

    # Sort by rank_by metric (descending for returns/ratios)
    if rank_by in df.columns and df[rank_by].notna().any():
        ascending = rank_by in ("max_drawdown", "annual_vol")
        df = df.sort_values(rank_by, ascending=ascending, na_position="last")

    df = df.reset_index(drop=True)
    df.index = df.index + 1  # 1-based ranking
    df.index.name = "rank"

    return df


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def _comparison_report(
    runs: list[BacktestRun],
    table: pd.DataFrame,
    rank_by: str,
) -> str:
    """Format the comparison as a text report."""
    ln: list[str] = []
    div = "=" * 90
    sub = "-" * 90

    # Header
    valid = [r for r in runs if r.ok]
    if not valid:
        return "No successful backtest runs to compare."

    first = valid[0]
    ln.append(div)
    ln.append("  STRATEGY COMPARISON")
    ln.append(div)
    ln.append(
        f"  Period:     {first.start_date.date()} → "
        f"{first.end_date.date()}"
    )
    ln.append(
        f"  Capital:    ${first.metrics.get('initial_capital', 0):,.0f}"
    )
    ln.append(
        f"  Strategies: {len(runs)} tested, "
        f"{len(valid)} successful"
    )
    ln.append(f"  Ranked by:  {rank_by}")

    # Comparison table
    ln.append("")
    ln.append(sub)
    ln.append(
        f"  {'#':>2}  {'Strategy':<24s} {'CAGR':>8} {'Sharpe':>7} "
        f"{'MaxDD':>8} {'Win%':>6} {'Trades':>7} "
        f"{'Final$':>12} {'ExcessCAGR':>10}"
    )
    ln.append(sub)

    for idx, row in table.iterrows():
        if row.get("error"):
            ln.append(
                f"  {idx:>2}  {row['strategy']:<24s}  "
                f"ERROR: {row['error']}"
            )
            continue

        cagr_s = f"{row['cagr']:>+7.2%}" if pd.notna(row.get("cagr")) else "    N/A"
        sharpe_s = f"{row['sharpe']:>6.2f}" if pd.notna(row.get("sharpe")) else "   N/A"
        dd_s = f"{row['max_drawdown']:>7.2%}" if pd.notna(row.get("max_drawdown")) else "    N/A"
        wr_s = f"{row['win_rate']:>5.1%}" if pd.notna(row.get("win_rate")) else "  N/A"
        trades_s = f"{int(row['total_trades']):>6d}" if pd.notna(row.get("total_trades")) else "   N/A"
        final_s = f"${row['final_capital']:>10,.0f}" if pd.notna(row.get("final_capital")) else "       N/A"
        excess_s = f"{row['excess_cagr']:>+9.2%}" if pd.notna(row.get("excess_cagr")) else "      N/A"

        ln.append(
            f"  {idx:>2}  {row['strategy']:<24s} {cagr_s} {sharpe_s} "
            f"{dd_s} {wr_s} {trades_s} {final_s} {excess_s}"
        )

    # Best/worst summary
    ln.append("")
    ln.append(sub)
    ln.append("  HIGHLIGHTS")
    ln.append(sub)

    if not table.empty and table["cagr"].notna().any():
        best_idx = table["cagr"].idxmax()
        worst_idx = table["cagr"].idxmin()
        best_row = table.loc[best_idx]
        worst_row = table.loc[worst_idx]

        ln.append(
            f"  Best CAGR:    {best_row['strategy']:<20s} "
            f"{best_row['cagr']:>+7.2%}"
        )
        ln.append(
            f"  Worst CAGR:   {worst_row['strategy']:<20s} "
            f"{worst_row['cagr']:>+7.2%}"
        )

    if not table.empty and table["sharpe"].notna().any():
        best_sh = table.loc[table["sharpe"].idxmax()]
        ln.append(
            f"  Best Sharpe:  {best_sh['strategy']:<20s} "
            f"{best_sh['sharpe']:>6.2f}"
        )

    if not table.empty and table["max_drawdown"].notna().any():
        best_dd = table.loc[table["max_drawdown"].idxmax()]
        ln.append(
            f"  Smallest DD:  {best_dd['strategy']:<20s} "
            f"{best_dd['max_drawdown']:>7.2%}"
        )

    ln.append("")
    ln.append(div)
    return "\n".join(ln)