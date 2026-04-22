"""
backtest/comparison.py
----------------------
Run multiple strategy variants over the same period and produce
a side-by-side comparison.

Enhanced with equity curve export and per-period analysis.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtest.engine import BacktestRun, StrategyConfig, run_backtest_period
from backtest.strategies import ALL_STRATEGIES, US_STRATEGIES
from backtest.metrics import metrics_report

logger = logging.getLogger(__name__)


def compare_strategies(
    data: dict[str, pd.DataFrame],
    *,
    strategies: list[StrategyConfig] | None = None,
    market: str = "US",
    start: str | None = None,
    end: str | None = None,
    capital: float = 100_000.0,
    rank_by: str = "cagr",
    current_holdings: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run every strategy variant over the same period and return
    a comparison.

    Enhanced: filters strategies by market, exports equity curves.
    """
    if strategies is None:
        market_strats = {
            k: v for k, v in ALL_STRATEGIES.items()
            if v.market == market.upper()
        }
        strategies = list(market_strats.values())

    logger.info(
        f"Comparing {len(strategies)} strategies [{market}] "
        f"({start or 'earliest'} → {end or 'latest'})"
    )

    runs: list[BacktestRun] = []

    for i, strat in enumerate(strategies, 1):
        logger.info(f"[{i}/{len(strategies)}] Running: {strat.name}")
        run = run_backtest_period(
            data, start=start, end=end,
            strategy=strat, capital=capital,
            current_holdings=current_holdings,
        )
        runs.append(run)

    table = _build_comparison_table(runs, rank_by=rank_by)
    report = _comparison_report(runs, table, rank_by)
    equity_curves = _build_equity_curves(runs)

    valid_runs = [r for r in runs if r.ok]
    best = max(valid_runs, key=lambda r: r.metrics.get(rank_by, -999)) if valid_runs else None
    worst = min(valid_runs, key=lambda r: r.metrics.get(rank_by, 999)) if valid_runs else None

    return {
        "runs": runs,
        "table": table,
        "report": report,
        "equity_curves": equity_curves,
        "best": best,
        "worst": worst,
    }


def _build_equity_curves(runs: list[BacktestRun]) -> pd.DataFrame:
    """Build a DataFrame of equity curves for all successful runs."""
    curves: dict[str, pd.Series] = {}
    for run in runs:
        if run.ok and run.backtest_result:
            eq = run.backtest_result.equity_curve
            if not eq.empty:
                curves[run.strategy.name] = eq
    if not curves:
        return pd.DataFrame()
    return pd.DataFrame(curves)


def _build_comparison_table(
    runs: list[BacktestRun],
    rank_by: str = "cagr",
) -> pd.DataFrame:
    rows = []
    for run in runs:
        m = run.metrics
        rows.append({
            "strategy":       run.strategy.name,
            "market":         run.strategy.market,
            "cagr":           m.get("cagr"),
            "total_return":   m.get("total_return"),
            "sharpe":         m.get("sharpe_ratio"),
            "sortino":        m.get("sortino_ratio"),
            "calmar":         m.get("calmar_ratio"),
            "max_drawdown":   m.get("max_drawdown"),
            "annual_vol":     m.get("annual_volatility"),
            "win_rate":       m.get("win_rate"),
            "total_trades":   m.get("total_trades"),
            "profit_factor":  m.get("profit_factor"),
            "expectancy":     m.get("expectancy"),
            "excess_cagr":    m.get("excess_cagr"),
            "alpha":          m.get("alpha"),
            "beta":           m.get("beta"),
            "info_ratio":     m.get("information_ratio"),
            "final_capital":  m.get("final_capital"),
            "best_year":      m.get("best_year"),
            "worst_year":     m.get("worst_year"),
            "elapsed_s":      run.elapsed_seconds,
            "error":          run.error,
        })

    df = pd.DataFrame(rows)
    if rank_by in df.columns and df[rank_by].notna().any():
        ascending = rank_by in ("max_drawdown", "annual_vol")
        df = df.sort_values(rank_by, ascending=ascending, na_position="last")
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "rank"
    return df


def _comparison_report(
    runs: list[BacktestRun],
    table: pd.DataFrame,
    rank_by: str,
) -> str:
    ln: list[str] = []
    div = "=" * 94
    sub = "-" * 94

    valid = [r for r in runs if r.ok]
    if not valid:
        return "No successful backtest runs to compare."

    first = valid[0]
    ln.append(div)
    ln.append(f"  STRATEGY COMPARISON [{first.strategy.market}]")
    ln.append(div)
    ln.append(f"  Period:     {first.start_date.date()} → {first.end_date.date()}")
    ln.append(f"  Capital:    ${first.metrics.get('initial_capital', 0):,.0f}")
    ln.append(f"  Strategies: {len(runs)} tested, {len(valid)} successful")
    ln.append(f"  Ranked by:  {rank_by}")

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
            ln.append(f"  {idx:>2}  {row['strategy']:<24s}  ERROR: {row['error']}")
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

    ln.append("")
    ln.append(sub)
    ln.append("  HIGHLIGHTS")
    ln.append(sub)

    if not table.empty and table["cagr"].notna().any():
        best_row = table.loc[table["cagr"].idxmax()]
        worst_row = table.loc[table["cagr"].idxmin()]
        ln.append(f"  Best CAGR:    {best_row['strategy']:<20s} {best_row['cagr']:>+7.2%}")
        ln.append(f"  Worst CAGR:   {worst_row['strategy']:<20s} {worst_row['cagr']:>+7.2%}")

    if not table.empty and table["sharpe"].notna().any():
        best_sh = table.loc[table["sharpe"].idxmax()]
        ln.append(f"  Best Sharpe:  {best_sh['strategy']:<20s} {best_sh['sharpe']:>6.2f}")

    if not table.empty and table["max_drawdown"].notna().any():
        best_dd = table.loc[table["max_drawdown"].idxmax()]
        ln.append(f"  Smallest DD:  {best_dd['strategy']:<20s} {best_dd['max_drawdown']:>7.2%}")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)