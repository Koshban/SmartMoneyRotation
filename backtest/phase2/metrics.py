"""
backtest/phase2/metrics.py
Performance metrics from backtest results.
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
    ann_factor = 252 / max(n_days, 1)
    ann_ret = (1 + total_ret) ** ann_factor - 1

    # ── volatility ────────────────────────────────────────────────
    ann_vol = daily.std() * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

    down = daily[daily < 0]
    down_vol = down.std() * np.sqrt(252) if len(down) > 0 else 0.001
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
    avg_pos = dl["n_positions"].mean() if not dl.empty else 0
    max_pos = int(dl["n_positions"].max()) if not dl.empty else 0
    total_buys = int(dl["n_buys"].sum()) if not dl.empty else 0
    total_sells = int(dl["n_sells"].sum()) if not dl.empty else 0

    # ── trade stats ───────────────────────────────────────────────
    tm = _trade_metrics(trades)

    return {
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "final_value": final,
        "annualized_vol": ann_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
        "max_dd_duration_days": max_dd_dur,
        "calmar_ratio": calmar,
        "avg_positions": avg_pos,
        "max_positions_held": max_pos,
        "trading_days": n_days,
        "total_buy_signals": total_buys,
        "total_sell_signals": total_sells,
        **tm,
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