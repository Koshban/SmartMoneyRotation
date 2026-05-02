"""
backtest/metrics.py
-------------------
Comprehensive performance analytics for backtesting.

Standalone functions — no state.  Each takes an equity curve,
daily returns, or trade list and returns metrics.

Enhancements over the original:
  - Monthly returns heatmap data (year × month pivot)
  - Regime-conditional metrics (performance during strong/weak breadth)
  - Rolling Sharpe and rolling drawdown
  - Win/loss streak analysis
  - Expectancy (avg win × win_rate - avg loss × loss_rate)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from cash.portfolio.rebalance import Trade


# ═══════════════════════════════════════════════════════════════
#  CAGR  (Compound Annual Growth Rate)
# ═══════════════════════════════════════════════════════════════

def compute_cagr(
    initial_value: float,
    final_value: float,
    n_years: float,
) -> float:
    if initial_value <= 0 or n_years <= 0:
        return 0.0
    if final_value <= 0:
        return -1.0
    return (final_value / initial_value) ** (1.0 / n_years) - 1.0


def cagr_from_equity(equity: pd.Series) -> float:
    if equity.empty or len(equity) < 2:
        return 0.0
    initial = equity.iloc[0]
    final = equity.iloc[-1]
    n_days = (equity.index[-1] - equity.index[0]).days
    n_years = max(n_days / 365.25, 0.01)
    return compute_cagr(initial, final, n_years)


def cagr_from_returns(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return 0.0
    equity = (1 + daily_returns).cumprod()
    return cagr_from_equity(equity)


# ═══════════════════════════════════════════════════════════════
#  ROLLING CAGR
# ═══════════════════════════════════════════════════════════════

def rolling_cagr(
    equity: pd.Series,
    window_years: int = 3,
) -> pd.Series:
    if equity.empty:
        return pd.Series(dtype=float)
    window_days = int(window_years * 252)
    if len(equity) < window_days:
        return pd.Series(dtype=float, index=equity.index)
    result = pd.Series(np.nan, index=equity.index)
    for i in range(window_days, len(equity)):
        initial = equity.iloc[i - window_days]
        final = equity.iloc[i]
        if initial > 0:
            result.iloc[i] = (final / initial) ** (1.0 / window_years) - 1
    result.name = f"rolling_{window_years}y_cagr"
    return result


# ═══════════════════════════════════════════════════════════════
#  ROLLING SHARPE
# ═══════════════════════════════════════════════════════════════

def rolling_sharpe(
    daily_returns: pd.Series,
    window: int = 252,
) -> pd.Series:
    if daily_returns.empty or len(daily_returns) < window:
        return pd.Series(dtype=float, index=daily_returns.index)
    roll_mean = daily_returns.rolling(window).mean()
    roll_std = daily_returns.rolling(window).std()
    result = (roll_mean / roll_std.replace(0, np.nan)) * np.sqrt(252)
    result.name = f"rolling_{window}d_sharpe"
    return result


# ═══════════════════════════════════════════════════════════════
#  MONTHLY RETURNS HEATMAP
# ═══════════════════════════════════════════════════════════════

def compute_monthly_returns_heatmap(
    daily_returns: pd.Series,
) -> pd.DataFrame:
    """
    Monthly returns as a year × month pivot table.

    Returns a DataFrame with years as rows, months (Jan–Dec) as
    columns, and monthly percentage returns as values.
    """
    if daily_returns.empty:
        return pd.DataFrame()
    monthly = (1 + daily_returns).resample("ME").prod() - 1
    monthly_df = pd.DataFrame({
        "year": monthly.index.year,
        "month": monthly.index.month,
        "return": monthly.values,
    })
    pivot = monthly_df.pivot_table(
        values="return", index="year", columns="month", aggfunc="first",
    )
    month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    pivot.columns = [month_names[c - 1] for c in pivot.columns]
    return pivot


# ═══════════════════════════════════════════════════════════════
#  REGIME-CONDITIONAL METRICS
# ═══════════════════════════════════════════════════════════════

def compute_regime_metrics(
    daily_returns: pd.Series,
    regime_series: pd.Series,
) -> dict[str, dict]:
    """
    Compute performance metrics conditioned on breadth regime.

    Parameters
    ----------
    daily_returns : pd.Series
        Daily return series (DatetimeIndex).
    regime_series : pd.Series
        Breadth regime labels (DatetimeIndex), e.g. 'strong', 'neutral', 'weak'.

    Returns
    -------
    dict[str, dict]
        {regime_label: {cagr, sharpe, vol, n_days, pct_time}}
    """
    if daily_returns.empty or regime_series.empty:
        return {}

    common = daily_returns.index.intersection(regime_series.index)
    if len(common) < 30:
        return {}

    rets = daily_returns.reindex(common)
    regimes = regime_series.reindex(common).ffill()

    result: dict[str, dict] = {}
    total_days = len(common)

    for regime in regimes.unique():
        if pd.isna(regime):
            continue
        mask = regimes == regime
        r = rets[mask]
        n = len(r)
        if n < 5:
            continue

        ann_ret = r.mean() * 252
        ann_vol = r.std() * np.sqrt(252)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

        result[str(regime)] = {
            "annualised_return": float(ann_ret),
            "annualised_vol": float(ann_vol),
            "sharpe": float(sharpe),
            "n_days": n,
            "pct_time": n / total_days,
            "mean_daily": float(r.mean()),
            "worst_day": float(r.min()),
            "best_day": float(r.max()),
        }

    return result


# ═══════════════════════════════════════════════════════════════
#  WIN/LOSS STREAK ANALYSIS
# ═══════════════════════════════════════════════════════════════

def compute_streak_stats(trade_pnls: list[float]) -> dict:
    """Analyse consecutive win/loss streaks."""
    if not trade_pnls:
        return {"max_win_streak": 0, "max_loss_streak": 0,
                "current_streak": 0, "current_streak_type": "none"}

    max_win = max_loss = cur = 0
    cur_type = "none"

    for pnl in trade_pnls:
        if pnl > 0:
            if cur_type == "win":
                cur += 1
            else:
                cur = 1
                cur_type = "win"
            max_win = max(max_win, cur)
        elif pnl < 0:
            if cur_type == "loss":
                cur += 1
            else:
                cur = 1
                cur_type = "loss"
            max_loss = max(max_loss, cur)
        else:
            cur = 0
            cur_type = "flat"

    return {
        "max_win_streak": max_win,
        "max_loss_streak": max_loss,
        "current_streak": cur,
        "current_streak_type": cur_type,
    }


# ═══════════════════════════════════════════════════════════════
#  COMPREHENSIVE METRICS
# ═══════════════════════════════════════════════════════════════

def compute_full_metrics(
    equity_curve: pd.Series,
    daily_returns: pd.Series,
    trades: list[Trade],
    initial_capital: float,
    benchmark_equity: pd.Series | None = None,
    breadth_regime: pd.Series | None = None,
) -> dict:
    """
    Compute every performance metric needed for strategy evaluation.

    Enhanced with: monthly returns, regime-conditional metrics,
    streak analysis, expectancy, and Ulcer index.
    """
    if equity_curve.empty:
        return {}

    final = equity_curve.iloc[-1]
    peak = equity_curve.max()
    n_days = len(equity_curve)
    calendar_days = (equity_curve.index[-1] - equity_curve.index[0]).days
    n_years = max(calendar_days / 365.25, 0.01)

    # ── Returns ───────────────────────────────────────────────
    total_return = (final / initial_capital) - 1
    cagr = compute_cagr(initial_capital, final, n_years)

    ann_vol = float(daily_returns.std() * np.sqrt(252)) if len(daily_returns) > 1 else 0.0
    mean_daily = daily_returns.mean() if len(daily_returns) > 0 else 0.0

    # ── Risk-adjusted ─────────────────────────────────────────
    sharpe = (
        (mean_daily / daily_returns.std() * np.sqrt(252))
        if daily_returns.std() > 0 else 0.0
    )

    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 0 else 0.001
    sortino = (
        mean_daily / down_std * np.sqrt(252)
        if down_std > 0 else 0.0
    )

    # ── Drawdown ──────────────────────────────────────────────
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    max_dd = drawdown.min()
    current_dd = drawdown.iloc[-1]

    is_dd = drawdown < 0
    dd_groups = (~is_dd).cumsum()
    dd_lengths = is_dd.groupby(dd_groups).sum()
    max_dd_duration = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0

    calmar = abs(cagr / max_dd) if max_dd < 0 else 0.0

    # ── Ulcer Index ───────────────────────────────────────────
    dd_squared = drawdown ** 2
    ulcer_index = float(np.sqrt(dd_squared.mean())) if len(dd_squared) > 0 else 0.0
    ulcer_perf = cagr / ulcer_index if ulcer_index > 0 else 0.0

    # ── VaR / CVaR ────────────────────────────────────────────
    var_95 = float(daily_returns.quantile(0.05)) if len(daily_returns) > 20 else 0.0
    tail = daily_returns[daily_returns <= var_95]
    cvar_95 = float(tail.mean()) if len(tail) > 0 else var_95

    # ── Higher moments ────────────────────────────────────────
    skewness = float(daily_returns.skew()) if len(daily_returns) > 5 else 0.0
    kurtosis = float(daily_returns.kurtosis()) if len(daily_returns) > 5 else 0.0

    # ── Annual returns ────────────────────────────────────────
    yearly = equity_curve.resample("YE").last()
    yearly_ret = yearly.pct_change().dropna()
    best_year = float(yearly_ret.max()) if len(yearly_ret) > 0 else 0.0
    worst_year = float(yearly_ret.min()) if len(yearly_ret) > 0 else 0.0
    pct_positive_years = (
        float((yearly_ret > 0).mean()) if len(yearly_ret) > 0 else 0.0
    )

    # ── Trades ────────────────────────────────────────────────
    trade_pnls = _compute_trade_pnls(trades)
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    n_trades = len(trade_pnls)
    win_rate = len(wins) / max(n_trades, 1)

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0

    # Expectancy
    loss_rate = 1.0 - win_rate
    expectancy = (avg_win * win_rate) + (avg_loss * loss_rate)

    total_commission = sum(t.commission + t.slippage for t in trades)

    # Streak analysis
    streak_stats = compute_streak_stats(trade_pnls)

    # Holding period (from trade timestamps)
    holding_days = _compute_avg_holding_days(trades)

    # ── Assemble base metrics ─────────────────────────────────
    metrics = {
        # Returns
        "total_return": total_return,
        "cagr": cagr,
        "best_year": best_year,
        "worst_year": worst_year,
        "pct_positive_years": pct_positive_years,
        # Risk
        "annual_volatility": ann_vol,
        "sharpe_ratio": float(sharpe),
        "sortino_ratio": float(sortino),
        "calmar_ratio": float(calmar),
        "max_drawdown": max_dd,
        "max_dd_duration": max_dd_duration,
        "current_drawdown": current_dd,
        "var_95": var_95,
        "cvar_95": cvar_95,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "ulcer_index": ulcer_index,
        "ulcer_performance": ulcer_perf,
        # Trading
        "total_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "total_commission": total_commission,
        "avg_holding_days": holding_days,
        "max_win_streak": streak_stats["max_win_streak"],
        "max_loss_streak": streak_stats["max_loss_streak"],
        # Capital
        "initial_capital": initial_capital,
        "final_capital": final,
        "peak_capital": peak,
        # Periods
        "n_days": n_days,
        "n_years": n_years,
        "start_date": equity_curve.index[0],
        "end_date": equity_curve.index[-1],
    }

    # ── Benchmark comparison ──────────────────────────────────
    if (
        benchmark_equity is not None
        and not benchmark_equity.empty
        and len(benchmark_equity) > 30
    ):
        bench_metrics = _compute_benchmark_metrics(
            equity_curve, daily_returns,
            benchmark_equity, initial_capital,
        )
        metrics.update(bench_metrics)

    # ── Regime-conditional metrics ────────────────────────────
    if breadth_regime is not None and not breadth_regime.empty:
        regime_m = compute_regime_metrics(daily_returns, breadth_regime)
        if regime_m:
            metrics["regime_metrics"] = regime_m

    return metrics


# ═══════════════════════════════════════════════════════════════
#  BENCHMARK COMPARISON
# ═══════════════════════════════════════════════════════════════

def _compute_benchmark_metrics(
    equity: pd.Series,
    daily_returns: pd.Series,
    bench_equity: pd.Series,
    initial_capital: float,
) -> dict:
    common = equity.index.intersection(bench_equity.index)
    if len(common) < 30:
        return {}

    strat_ret = daily_returns.reindex(common).fillna(0)
    bench_ret = bench_equity.reindex(common).pct_change().fillna(0)

    n_days = (common[-1] - common[0]).days
    n_years = max(n_days / 365.25, 0.01)

    bench_final = bench_equity.reindex(common).iloc[-1]
    bench_initial = bench_equity.reindex(common).iloc[0]
    bench_cagr = compute_cagr(bench_initial, bench_final, n_years)

    bench_vol = float(bench_ret.std() * np.sqrt(252))
    bench_sharpe = (
        (bench_ret.mean() / bench_ret.std() * np.sqrt(252))
        if bench_ret.std() > 0 else 0.0
    )

    bench_max_dd = (
        (bench_equity.reindex(common) / bench_equity.reindex(common).cummax() - 1).min()
    )

    strat_cagr = cagr_from_equity(equity.reindex(common))
    excess_cagr = strat_cagr - bench_cagr

    active_ret = strat_ret - bench_ret
    tracking_error = float(active_ret.std() * np.sqrt(252))
    information_ratio = (
        float(active_ret.mean() / active_ret.std() * np.sqrt(252))
        if active_ret.std() > 0 else 0.0
    )

    up_days = bench_ret > 0
    dn_days = bench_ret < 0

    up_capture = (
        float(strat_ret[up_days].mean() / bench_ret[up_days].mean())
        if up_days.any() and bench_ret[up_days].mean() != 0 else 1.0
    )
    down_capture = (
        float(strat_ret[dn_days].mean() / bench_ret[dn_days].mean())
        if dn_days.any() and bench_ret[dn_days].mean() != 0 else 1.0
    )

    # Beta and alpha
    if bench_ret.var() > 0:
        beta = float(strat_ret.cov(bench_ret) / bench_ret.var())
        alpha = float((strat_ret.mean() - beta * bench_ret.mean()) * 252)
    else:
        beta = 1.0
        alpha = 0.0

    return {
        "bench_cagr": bench_cagr,
        "bench_sharpe": float(bench_sharpe),
        "bench_max_dd": bench_max_dd,
        "bench_volatility": bench_vol,
        "excess_cagr": excess_cagr,
        "information_ratio": information_ratio,
        "tracking_error": tracking_error,
        "up_capture": up_capture,
        "down_capture": down_capture,
        "beta": beta,
        "alpha": alpha,
    }


# ═══════════════════════════════════════════════════════════════
#  TRADE PnL
# ═══════════════════════════════════════════════════════════════

def _compute_trade_pnls(trades: list[Trade]) -> list[float]:
    open_trades: dict[str, list[Trade]] = {}
    pnls: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                entry_cost = entry.price * (1 + 0.0015)
                exit_net = trade.price * (1 - 0.0015)
                pnl = (exit_net / entry_cost) - 1 if entry_cost > 0 else 0
                pnls.append(pnl)

    return pnls


def _compute_avg_holding_days(trades: list[Trade]) -> float:
    """Compute average holding period from BUY→SELL pairs."""
    open_trades: dict[str, list[Trade]] = {}
    holding_days: list[float] = []

    for trade in trades:
        if trade.action == "BUY":
            open_trades.setdefault(trade.ticker, []).append(trade)
        elif trade.action == "SELL":
            opens = open_trades.get(trade.ticker, [])
            if opens:
                entry = opens.pop(0)
                try:
                    days = (trade.date - entry.date).days
                    holding_days.append(max(days, 1))
                except Exception:
                    pass

    return float(np.mean(holding_days)) if holding_days else 0.0


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def metrics_report(run: "BacktestRun") -> str:
    m = run.metrics
    if not m:
        return f"No metrics for '{runrefactor.strategy.name}'"

    ln: list[str] = []
    div = "=" * 70
    sub = "-" * 70

    ln.append(div)
    ln.append(f"  BACKTEST REPORT: {runrefactor.strategy.name}")
    ln.append(f"  {runrefactor.strategy.description}")
    if runrefactor.strategy.market != "US":
        ln.append(f"  Market: {runrefactor.strategy.market}")
    ln.append(div)

    ln.append(f"  Period:          {m.get('start_date', '?')} → "
              f"{m.get('end_date', '?')}  ({m.get('n_years', 0):.1f} years)")
    ln.append(f"  Initial capital: ${m.get('initial_capital', 0):,.0f}")
    ln.append(f"  Final capital:   ${m.get('final_capital', 0):,.0f}")
    ln.append(f"  Peak capital:    ${m.get('peak_capital', 0):,.0f}")

    ln.append("")
    ln.append(sub)
    ln.append("  RETURNS")
    ln.append(sub)
    ln.append(f"  Total return:        {m.get('total_return', 0):>+8.2%}")
    ln.append(f"  CAGR:                {m.get('cagr', 0):>+8.2%}")
    ln.append(f"  Best year:           {m.get('best_year', 0):>+8.2%}")
    ln.append(f"  Worst year:          {m.get('worst_year', 0):>+8.2%}")
    ln.append(f"  % positive years:    {m.get('pct_positive_years', 0):>8.0%}")

    ln.append("")
    ln.append(sub)
    ln.append("  RISK")
    ln.append(sub)
    ln.append(f"  Ann. volatility:     {m.get('annual_volatility', 0):>8.2%}")
    ln.append(f"  Sharpe ratio:        {m.get('sharpe_ratio', 0):>8.3f}")
    ln.append(f"  Sortino ratio:       {m.get('sortino_ratio', 0):>8.3f}")
    ln.append(f"  Calmar ratio:        {m.get('calmar_ratio', 0):>8.3f}")
    ln.append(f"  Max drawdown:        {m.get('max_drawdown', 0):>8.2%}")
    ln.append(f"  Max DD duration:     {m.get('max_dd_duration', 0):>5d} days")
    ln.append(f"  VaR (95%):           {m.get('var_95', 0):>8.4f}")
    ln.append(f"  CVaR (95%):          {m.get('cvar_95', 0):>8.4f}")
    ln.append(f"  Ulcer Index:         {m.get('ulcer_index', 0):>8.4f}")

    ln.append("")
    ln.append(sub)
    ln.append("  TRADING")
    ln.append(sub)
    ln.append(f"  Total trades:        {m.get('total_trades', 0):>5d}")
    ln.append(f"  Win rate:            {m.get('win_rate', 0):>8.1%}")
    ln.append(f"  Profit factor:       {m.get('profit_factor', 0):>8.2f}")
    ln.append(f"  Avg win:             {m.get('avg_win', 0):>+8.2%}")
    ln.append(f"  Avg loss:            {m.get('avg_loss', 0):>+8.2%}")
    ln.append(f"  Expectancy:          {m.get('expectancy', 0):>+8.4f}")
    ln.append(f"  Avg holding days:    {m.get('avg_holding_days', 0):>8.1f}")
    ln.append(f"  Max win streak:      {m.get('max_win_streak', 0):>5d}")
    ln.append(f"  Max loss streak:     {m.get('max_loss_streak', 0):>5d}")
    ln.append(f"  Total costs:         ${m.get('total_commission', 0):>10,.2f}")

    if "bench_cagr" in m:
        ln.append("")
        ln.append(sub)
        ln.append("  vs BENCHMARK")
        ln.append(sub)
        ln.append(f"  Benchmark CAGR:      {m.get('bench_cagr', 0):>+8.2%}")
        ln.append(f"  Excess CAGR:         {m.get('excess_cagr', 0):>+8.2%}")
        ln.append(f"  Alpha (ann):         {m.get('alpha', 0):>+8.2%}")
        ln.append(f"  Beta:                {m.get('beta', 0):>8.3f}")
        ln.append(f"  Information ratio:   {m.get('information_ratio', 0):>8.3f}")
        ln.append(f"  Tracking error:      {m.get('tracking_error', 0):>8.2%}")
        ln.append(f"  Up capture:          {m.get('up_capture', 0):>8.2f}")
        ln.append(f"  Down capture:        {m.get('down_capture', 0):>8.2f}")
        ln.append(f"  Bench max DD:        {m.get('bench_max_dd', 0):>8.2%}")

    # Regime-conditional metrics
    regime_m = m.get("regime_metrics", {})
    if regime_m:
        ln.append("")
        ln.append(sub)
        ln.append("  REGIME-CONDITIONAL PERFORMANCE")
        ln.append(sub)
        for regime, rm in sorted(regime_m.items()):
            ln.append(
                f"  {regime:<10s}  "
                f"ret={rm['annualised_return']:>+7.2%}  "
                f"vol={rm['annualised_vol']:>6.2%}  "
                f"sharpe={rm['sharpe']:>6.2f}  "
                f"({rm['pct_time']:.0%} of time)"
            )

    # Annual returns
    if not run.annual_returns.empty:
        ln.append("")
        ln.append(sub)
        ln.append("  ANNUAL RETURNS")
        ln.append(sub)
        for year, ret in run.annual_returns.items():
            bar = "█" * max(0, int(ret * 100))
            ln.append(f"  {year}:  {ret:>+7.2%}  {bar}")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)