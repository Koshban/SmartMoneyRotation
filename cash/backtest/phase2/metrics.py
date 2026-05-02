"""
backtest/phase2/metrics.py
Performance metrics from backtest results, including benchmark comparison.

FIX (2026-04-28): annualization now uses calendar dates, not trading-day
count.  The old formula ``252 / n_days`` broke when data had gaps —
e.g. 310 trading days over a 28-month span was treated as 1.23 years
instead of 2.32 years, inflating CAGR from ~11% to ~21%.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Any, Dict


def compute_metrics(results: Dict[str, Any]) -> Dict[str, Any]:
    equity = results["equity_curve"].copy()
    trades = results.get("trade_log", pd.DataFrame())
    initial = results["initial_capital"]
    final = results["final_value"]

    # ── returns ───────────────────────────────────────────────────
    total_ret = (final - initial) / initial
    equity["daily_ret"] = equity["value"].pct_change()
    daily = equity["daily_ret"].dropna()

    n_days = len(equity)

    # ── calendar-based annualization (robust to data gaps) ────────
    actual_start = actual_end = None
    if "date" in equity.columns and n_days >= 2:
        actual_start = pd.Timestamp(equity["date"].iloc[0])
        actual_end = pd.Timestamp(equity["date"].iloc[-1])
        calendar_days = (actual_end - actual_start).days
        years = calendar_days / 365.25
    else:
        years = n_days / 252.0

    years = max(years, 1.0 / 365.25)       # floor at 1 calendar day
    ann_factor = 1.0 / years                # exponent for CAGR

    ann_ret = (1 + total_ret) ** ann_factor - 1

    # expected trading days (for data-coverage check)
    expected_trading_days = int(round(years * 252))

    # ── volatility ────────────────────────────────────────────────
    ann_vol = daily.std() * np.sqrt(252) if len(daily) > 1 else 0.0
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

    down = daily[daily < 0]
    down_vol = down.std() * np.sqrt(252) if len(down) > 1 else 0.001
    sortino = ann_ret / down_vol

    # ── drawdown ──────────────────────────────────────────────────
    equity["peak"] = equity["value"].cummax()
    equity["dd"] = (equity["value"] - equity["peak"]) / equity["peak"]
    max_dd = equity["dd"].min()

    in_dd = equity["dd"] < 0
    if in_dd.any():
        groups = (~in_dd).cumsum()
        max_dd_dur = int(in_dd.groupby(groups).sum().max())
    else:
        max_dd_dur = 0

    calmar = ann_ret / abs(max_dd) if max_dd != 0 else 0.0

    # ── portfolio utilisation ─────────────────────────────────────
    dl = results.get("daily_log", pd.DataFrame())
    avg_pos = dl["n_positions"].mean() if ("n_positions" in dl.columns and not dl.empty) else 0
    max_pos = int(dl["n_positions"].max()) if ("n_positions" in dl.columns and not dl.empty) else 0
    total_buys = int(dl["n_buys"].sum()) if ("n_buys" in dl.columns and not dl.empty) else 0
    total_sells = int(dl["n_sells"].sum()) if ("n_sells" in dl.columns and not dl.empty) else 0

    # ── trade stats ───────────────────────────────────────────────
    tm = _trade_metrics(trades)

    # ── benchmark comparison (uses same ann_factor) ───────────────
    bm = _benchmark_metrics(equity, ann_ret, ann_factor, years)

    return {
        # ── core ──
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "final_value": final,
        "initial_capital": initial,
        # ── period ──
        "years": years,
        "actual_start": str(actual_start.date()) if actual_start is not None else "",
        "actual_end": str(actual_end.date()) if actual_end is not None else "",
        "trading_days": n_days,
        "expected_trading_days": expected_trading_days,
        # ── risk ──
        "annualized_vol": ann_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
        "max_dd_duration_days": max_dd_dur,
        "calmar_ratio": calmar,
        # ── utilisation ──
        "avg_positions": avg_pos,
        "max_positions_held": max_pos,
        "total_buy_signals": total_buys,
        "total_sell_signals": total_sells,
        # ── trades + benchmark (spread in) ──
        **tm,
        **bm,
    }


# ------------------------------------------------------------------
def _benchmark_metrics(
    equity: pd.DataFrame,
    strategy_ann_ret: float,
    ann_factor: float,
    years: float,
) -> Dict[str, Any]:
    """
    Benchmark return, risk, and relative metrics.

    Uses the same *ann_factor* (1 / calendar_years) as the portfolio
    so that CAGR values are directly comparable.
    """
    defaults = {
        "benchmark_total_return": 0.0,
        "benchmark_ann_return": 0.0,
        "benchmark_ann_vol": 0.0,
        "benchmark_sharpe": 0.0,
        "benchmark_sortino": 0.0,
        "benchmark_max_dd": 0.0,
        "benchmark_calmar": 0.0,
        "alpha": 0.0,
        "beta": 0.0,
        "tracking_error": 0.0,
        "information_ratio": 0.0,
    }

    if "benchmark" not in equity.columns:
        return defaults

    bench = equity["benchmark"]
    if bench.isna().all():
        return defaults

    # Forward-fill any gaps, then drop remaining NaNs
    bench = bench.ffill()
    valid = equity[["value", "benchmark"]].dropna()
    if len(valid) < 10:
        return defaults

    bench_initial = valid["benchmark"].iloc[0]
    bench_final = valid["benchmark"].iloc[-1]
    bench_total_ret = (
        (bench_final - bench_initial) / bench_initial
        if bench_initial > 0
        else 0.0
    )
    bench_ann_ret = (1 + bench_total_ret) ** ann_factor - 1

    # Daily returns for both
    combined = pd.DataFrame({
        "port_ret": valid["value"].pct_change(),
        "bench_ret": valid["benchmark"].pct_change(),
    }).dropna()

    if len(combined) < 5:
        return {
            **defaults,
            "benchmark_total_return": bench_total_ret,
            "benchmark_ann_return": bench_ann_ret,
        }

    # ── benchmark vol ─────────────────────────────────────────────
    bench_ann_vol = combined["bench_ret"].std() * np.sqrt(252)
    bench_sharpe = bench_ann_ret / bench_ann_vol if bench_ann_vol > 0 else 0.0

    # ── benchmark sortino ─────────────────────────────────────────
    bench_down = combined["bench_ret"][combined["bench_ret"] < 0]
    bench_down_vol = (
        bench_down.std() * np.sqrt(252) if len(bench_down) > 1 else 0.001
    )
    bench_sortino = bench_ann_ret / bench_down_vol

    # ── benchmark drawdown ────────────────────────────────────────
    bench_peak = valid["benchmark"].cummax()
    bench_dd = (valid["benchmark"] - bench_peak) / bench_peak
    bench_max_dd = bench_dd.min()

    # ── benchmark calmar ──────────────────────────────────────────
    bench_calmar = (
        bench_ann_ret / abs(bench_max_dd) if bench_max_dd != 0 else 0.0
    )

    # ── excess returns / tracking error ───────────────────────────
    excess = combined["port_ret"] - combined["bench_ret"]
    tracking_error = (
        excess.std() * np.sqrt(252) if len(excess) > 1 else 0.0
    )

    # ── beta ──────────────────────────────────────────────────────
    bench_var = combined["bench_ret"].var()
    if bench_var > 0:
        beta = (
            combined[["port_ret", "bench_ret"]].cov().iloc[0, 1] / bench_var
        )
    else:
        beta = 0.0

    # ── Jensen's alpha ────────────────────────────────────────────
    alpha = strategy_ann_ret - beta * bench_ann_ret

    # ── information ratio ─────────────────────────────────────────
    info_ratio = (
        (strategy_ann_ret - bench_ann_ret) / tracking_error
        if tracking_error > 0
        else 0.0
    )

    return {
        "benchmark_total_return": bench_total_ret,
        "benchmark_ann_return": bench_ann_ret,
        "benchmark_ann_vol": bench_ann_vol,
        "benchmark_sharpe": bench_sharpe,
        "benchmark_sortino": bench_sortino,
        "benchmark_max_dd": bench_max_dd,
        "benchmark_calmar": bench_calmar,
        "alpha": alpha,
        "beta": beta,
        "tracking_error": tracking_error,
        "information_ratio": info_ratio,
    }


# ------------------------------------------------------------------
def _trade_metrics(trades: pd.DataFrame) -> Dict[str, Any]:
    empty = {
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "win_rate": 0.0,
        "avg_win_pct": 0.0,
        "avg_loss_pct": 0.0,
        "profit_factor": 0.0,
        "avg_pnl_pct": 0.0,
        "median_pnl_pct": 0.0,
        "avg_holding_days": 0.0,
        "best_trade_pct": 0.0,
        "worst_trade_pct": 0.0,
        "expectancy_dollar": 0.0,
    }
    if trades.empty:
        return empty

    w = trades[trades["pnl"] > 0]
    l = trades[trades["pnl"] <= 0]
    n = len(trades)

    gross_win = w["pnl"].sum() if not w.empty else 0.0
    gross_loss = abs(l["pnl"].sum()) if not l.empty else 0.001

    return {
        "total_trades": n,
        "winning_trades": len(w),
        "losing_trades": len(l),
        "win_rate": len(w) / n,
        "avg_win_pct": w["pnl_pct"].mean() if not w.empty else 0.0,
        "avg_loss_pct": l["pnl_pct"].mean() if not l.empty else 0.0,
        "profit_factor": gross_win / gross_loss,
        "avg_pnl_pct": trades["pnl_pct"].mean(),
        "median_pnl_pct": trades["pnl_pct"].median(),
        "avg_holding_days": trades["holding_days"].mean(),
        "best_trade_pct": trades["pnl_pct"].max(),
        "worst_trade_pct": trades["pnl_pct"].min(),
        "expectancy_dollar": trades["pnl"].mean(),
    }