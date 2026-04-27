"""
backtest/phase2/run_backtest.py
CLI entry point — single-config backtest.

    python -m backtest.phase2.run_backtest --market HK --start 2025-10-01 --end 2026-04-20
    python -m backtest.phase2.run_backtest --market IN --start 2025-06-01 --end 2026-04-20
    python -m backtest.phase2.run_backtest --market US --start 2025-06-01 --end 2026-04-20
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.engine import BacktestEngine
from backtest.phase2.metrics import compute_metrics
from common.universe import get_universe_for_market
from refactor.common.config_refactor import (
    VOLREGIMEPARAMS,
    SCORINGWEIGHTS_V2,
    SCORINGPARAMS_V2,
    SIGNALPARAMS_V2,
    CONVERGENCEPARAMS_V2,
    ACTIONPARAMS_V2,
    BREADTHPARAMS,
    ROTATIONPARAMS,
)

console = Console()
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════════
LOGS_DIR = Path("logs") / "backtest"


def _setup_logging(market: str, level: int = logging.INFO) -> Path:
    """
    Configure the root logger to write to both:
      • stderr   (concise, INFO+)
      • file     (verbose, DEBUG+)

    Returns the path to the log file.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"backtest_{market}_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # ── file handler (verbose — captures DEBUG from all modules) ──
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    # ── console handler (concise — INFO only) ─────────────────────
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(ch)

    # ── silence noisy modules on the console ──────────────────────
    for noisy in (
        "refactor.strategy.rotation_v2",
        "refactor.pipeline_v2",
        "refactor.strategy",
        "refactor.scoring",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return log_file


def _log_and_print(msg: str, rich_msg: str | None = None, level: int = logging.INFO):
    """Log a plain-text message AND print a (possibly styled) version to Rich console."""
    log.log(level, msg)
    console.print(rich_msg if rich_msg is not None else msg)


# ══════════════════════════════════════════════════════════════════
#  UNIVERSE + BENCHMARK MAPPING
# ══════════════════════════════════════════════════════════════════

BENCHMARKS = {
    "HK": "2800.HK",
    "IN": "NIFTYBEES.NS",
    "US": "SPY",
}


def _get_tickers(market: str) -> list[str]:
    return get_universe_for_market(market)


# ══════════════════════════════════════════════════════════════════
#  PARQUET DATA LOADING
# ══════════════════════════════════════════════════════════════════

def load_data(
    market: str,
    data_dir: str = "data",
    lookback_bars: int = 300,
) -> BacktestDataSource:
    parquet_path = Path(data_dir) / f"{market}_cash.parquet"
    tickers = _get_tickers(market)
    benchmark = BENCHMARKS.get(market)

    plain = (
        f"Market: {market}   Universe: {len(tickers)} tickers   "
        f"Benchmark: {benchmark or 'none'}   File: {parquet_path}"
    )
    rich = (
        f"[bold]Market:[/] {market}   "
        f"[bold]Universe:[/] {len(tickers)} tickers   "
        f"[bold]Benchmark:[/] {benchmark or 'none'}   "
        f"[bold]File:[/] {parquet_path}"
    )
    _log_and_print(plain, rich)

    ds = BacktestDataSource.from_parquet(
        parquet_path=parquet_path,
        tickers=tickers,
        benchmark_ticker=benchmark,
        lookback_bars=lookback_bars,
    )

    lo, hi = ds.get_date_range()
    loaded = ds.get_tickers()

    plain = (
        f"Loaded {len(loaded)}/{len(tickers)} tickers  "
        f"({lo.strftime('%Y-%m-%d')} -> {hi.strftime('%Y-%m-%d')})"
    )
    rich = (
        f"[green]Loaded {len(loaded)}/{len(tickers)} tickers  "
        f"({lo.strftime('%Y-%m-%d')} → {hi.strftime('%Y-%m-%d')})[/]"
    )
    _log_and_print(plain, rich)

    if len(loaded) < len(tickers):
        missing = set(tickers) - set(loaded)
        preview = sorted(missing)[:15]
        suffix = "..." if len(missing) > 15 else ""
        plain = f"Missing {len(missing)} tickers: {preview}{suffix}"
        rich = f"[yellow]Missing {len(missing)} tickers: {preview}{suffix}[/]"
        _log_and_print(plain, rich, level=logging.WARNING)

    return ds


# ══════════════════════════════════════════════════════════════════
#  SINGLE CONFIG — imported from refactor.common.config_refactor
# ══════════════════════════════════════════════════════════════════

CONFIG = {
    "vol_regime_params":  VOLREGIMEPARAMS,
    "scoring_weights":    SCORINGWEIGHTS_V2,
    "scoring_params":     SCORINGPARAMS_V2,
    "signal_params":      SIGNALPARAMS_V2,
    "convergence_params": CONVERGENCEPARAMS_V2,
    "action_params":      ACTIONPARAMS_V2,
    "breadth_params":     BREADTHPARAMS,
    "rotation_params":    ROTATIONPARAMS,
}


# ══════════════════════════════════════════════════════════════════
#  METRICS DISPLAY
# ══════════════════════════════════════════════════════════════════

def _print_metrics(metrics: dict) -> None:
    """Pretty-print backtest metrics as a Rich table."""
    table = Table(
        title="Backtest Results",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="bold", min_width=22)
    table.add_column("Value", justify="right")

    rows = [
        # ── returns ──────────────────────────────────────────
        ("Total Return",          "total_return",          lambda v: f"{v:+.2%}"),
        ("Ann. Return",           "annualized_return",     lambda v: f"{v:+.2%}"),
        ("Final Value",           "final_value",           lambda v: f"${v:,.0f}"),
        ("Ann. Volatility",       "annualized_vol",        lambda v: f"{v:.2%}"),
        ("Sharpe Ratio",          "sharpe_ratio",          lambda v: f"{v:.3f}"),
        ("Sortino Ratio",         "sortino_ratio",         lambda v: f"{v:.3f}"),
        ("Max Drawdown",          "max_drawdown",          lambda v: f"{v:.2%}"),
        ("Max DD Duration",       "max_dd_duration_days",  lambda v: f"{v:.0f} days"),
        ("Calmar Ratio",          "calmar_ratio",          lambda v: f"{v:.3f}"),
        # ── benchmark ────────────────────────────────────────
        ("Benchmark Return",      "benchmark_total_return", lambda v: f"{v:+.2%}"),
        ("Benchmark Ann. Return", "benchmark_ann_return",   lambda v: f"{v:+.2%}"),
        ("Benchmark Max DD",      "benchmark_max_dd",       lambda v: f"{v:.2%}"),
        ("Alpha (Jensen)",        "alpha",                  lambda v: f"{v:+.2%}"),
        ("Beta",                  "beta",                   lambda v: f"{v:.3f}"),
        ("Tracking Error",        "tracking_error",         lambda v: f"{v:.2%}"),
        ("Information Ratio",     "information_ratio",      lambda v: f"{v:.3f}"),
        # ── trades ───────────────────────────────────────────
        ("Total Trades",          "total_trades",          lambda v: f"{v:.0f}"),
        ("Winning Trades",        "winning_trades",        lambda v: f"{v:.0f}"),
        ("Losing Trades",         "losing_trades",         lambda v: f"{v:.0f}"),
        ("Win Rate",              "win_rate",              lambda v: f"{v:.1%}"),
        ("Avg Win",               "avg_win_pct",           lambda v: f"{v:+.2%}"),
        ("Avg Loss",              "avg_loss_pct",          lambda v: f"{v:+.2%}"),
        ("Profit Factor",         "profit_factor",         lambda v: f"{v:.2f}"),
        ("Avg PnL",               "avg_pnl_pct",           lambda v: f"{v:+.2%}"),
        ("Median PnL",            "median_pnl_pct",        lambda v: f"{v:+.2%}"),
        ("Avg Holding Days",      "avg_holding_days",      lambda v: f"{v:.1f}"),
        ("Best Trade",            "best_trade_pct",        lambda v: f"{v:+.2%}"),
        ("Worst Trade",           "worst_trade_pct",       lambda v: f"{v:+.2%}"),
        ("Expectancy ($)",        "expectancy_dollar",     lambda v: f"${v:,.0f}"),
        # ── utilisation ──────────────────────────────────────
        ("Avg Positions",         "avg_positions",         lambda v: f"{v:.1f}"),
        ("Max Positions Held",    "max_positions_held",    lambda v: f"{v:.0f}"),
        ("Trading Days",          "trading_days",          lambda v: f"{v:.0f}"),
        ("Total Buy Signals",     "total_buy_signals",     lambda v: f"{v:.0f}"),
        ("Total Sell Signals",    "total_sell_signals",    lambda v: f"{v:.0f}"),
    ]

    for label, key, formatter in rows:
        if key in metrics:
            table.add_row(label, formatter(metrics[key]))

    console.print(table)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Backtest strategy against a market universe"
    )
    parser.add_argument(
        "--market", required=True,
        help="Market code as defined in common/universe.py  (e.g. HK, IN, US)",
    )
    parser.add_argument("--start", required=True, help="Start date  YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date    YYYY-MM-DD")
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--max-positions", type=int, default=12)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--lookback", type=int, default=300,
                        help="Lookback bars for indicator warmup")
    args = parser.parse_args()

    # ── set up dual logging (file + console) ──────────────────
    log_file = _setup_logging(args.market)

    # ── log the full command / args ───────────────────────────
    log.info("=" * 70)
    log.info("Backtest started  market=%s  start=%s  end=%s", args.market, args.start, args.end)
    log.info("capital=%.0f  max_positions=%d  lookback=%d", args.capital, args.max_positions, args.lookback)
    log.info("data_dir=%s", args.data_dir)
    log.info("Log file: %s", log_file)
    log.info("=" * 70)

    # ── load data ─────────────────────────────────────────────
    console.rule(f"[bold cyan]Backtest: {args.market} market")
    log.info("--- Loading data for %s market ---", args.market)
    ds = load_data(args.market, args.data_dir, args.lookback)

    # ── run backtest ──────────────────────────────────────────
    console.rule("[bold cyan]Running backtest")
    log.info("--- Running backtest with config from config_refactor ---")

    engine = BacktestEngine(
        data_source=ds,
        market=args.market,
        config=CONFIG,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        max_positions=args.max_positions,
        config_name="Strategy",
    )
    results = engine.run()
    metrics = compute_metrics(results)
    results["metrics"] = metrics

    # ── display ───────────────────────────────────────────────
    console.print()
    _print_metrics(metrics)

    # ── log metrics to file ───────────────────────────────────
    log.info("--- Metrics ---")
    for key, val in metrics.items():
        log.info("  %-30s  %s", key, val)

    # ── summary ───────────────────────────────────────────────
    bench_ret = metrics.get("benchmark_total_return", 0)
    summary_lines = [
        f"Market          : {args.market}",
        f"Period          : {args.start} -> {args.end}",
        f"Start capital   : ${args.capital:,.0f}",
        f"Benchmark       : {BENCHMARKS.get(args.market, '?')}  ({bench_ret:+.1%})",
        f"Strategy        : ${results['final_value']:,.0f}  (alpha {metrics.get('alpha', 0):+.1%})",
    ]
    console.print()
    for line in summary_lines:
        _log_and_print(f"  {line}", f"  {line}")

    # ── save ──────────────────────────────────────────────────
    out = Path("backtest_results") / args.market
    out.mkdir(parents=True, exist_ok=True)

    # Metrics CSV
    metrics_rows = [{"Metric": k, "Value": v} for k, v in metrics.items()]
    pd.DataFrame(metrics_rows).to_csv(out / "metrics.csv", index=False)

    # Equity curve (includes benchmark column)
    results["equity_curve"].to_csv(out / "equity_curve.csv", index=False)

    # Trade log
    if not results["trade_log"].empty:
        results["trade_log"].to_csv(out / "trades.csv", index=False)

    # Daily log
    results["daily_log"].to_csv(out / "daily_log.csv", index=False)

    _log_and_print(
        f"Results saved to {out}/",
        f"\n[green]Results saved to {out}/[/]",
    )
    _log_and_print(
        f"Log file: {log_file}",
        f"[green]Log file: {log_file}[/]",
    )
    log.info("Backtest finished successfully.")


if __name__ == "__main__":
    main()
################################
"""
backtest/phase2/tracker.py
Virtual portfolio tracker with minimum hold period.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List
import logging

log = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float


class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        min_hold_days: int = 5,
        min_profit_early_exit_pct: float = 0.05,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.min_hold_days = min_hold_days
        self.min_profit_early_exit_pct = min_profit_early_exit_pct

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

    def _can_sell(self, pos: Position, date, current_price: float) -> bool:
        """Check if minimum hold period is satisfied or profit target met."""
        held_days = (date - pos.entry_date).days
        if held_days >= self.min_hold_days:
            return True
        # Early exit allowed if profit target met
        unrealised_pct = (current_price / pos.entry_price) - 1.0
        if unrealised_pct >= self.min_profit_early_exit_pct:
            log.debug(
                "  early exit allowed %s: +%.1f%% after %dd",
                pos.ticker, unrealised_pct * 100, held_days,
            )
            return True
        return False

    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
    ) -> None:
        # ── sells first (respect min hold) ────────────────────
        blocked_sells = []
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker not in prices:
                    continue
                pos = self.positions[ticker]
                if self._can_sell(pos, date, prices[ticker]):
                    self._sell(date, ticker, prices[ticker])
                else:
                    held = (date - pos.entry_date).days
                    blocked_sells.append((ticker, held))

        if blocked_sells:
            log.debug(
                "  min-hold blocked %d sells: %s",
                len(blocked_sells),
                [(t, f"{d}d") for t, d in blocked_sells[:5]],
            )

        # ── then buys ────────────────────────────────────────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]
        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            self._buy(date, ticker, prices[ticker])

    def _buy(self, date, ticker: str, raw_price: float) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)
        target_value = min(
            self.initial_capital / self.max_positions,
            self.cash * 0.95,
        )
        if target_value < 1_000:
            return

        shares = int(target_value / exec_price)
        if shares <= 0:
            return

        cost = shares * exec_price
        commission = cost * self.commission_rate
        total = cost + commission
        if total > self.cash:
            return

        self.cash -= total
        self.positions[ticker] = Position(
            ticker=ticker,
            entry_date=date,
            entry_price=exec_price,
            shares=shares,
            cost_basis=total,
        )
        log.info(
            "  ▲ BUY  %-10s  %d shares @ %.2f  cost $%s",
            ticker, shares, exec_price, f"{total:,.0f}",
        )

    def _sell(self, date, ticker: str, raw_price: float) -> None:
        pos = self.positions.get(ticker)
        if pos is None:
            return

        exec_price = raw_price * (1 - self.slippage_rate)
        proceeds = pos.shares * exec_price
        commission = proceeds * self.commission_rate
        net = proceeds - commission

        pnl = net - pos.cost_basis
        pnl_pct = pnl / pos.cost_basis
        held = (date - pos.entry_date).days

        self.cash += net
        del self.positions[ticker]

        self.closed_trades.append({
            "ticker": ticker,
            "entry_date": pos.entry_date,
            "exit_date": date,
            "entry_price": pos.entry_price,
            "exit_price": exec_price,
            "shares": pos.shares,
            "cost_basis": pos.cost_basis,
            "net_proceeds": net,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "holding_days": held,
        })
        log.info(
            "  ▼ SELL %-10s  %d shares @ %.2f  PnL $%s (%+.1f%%)  held %dd",
            ticker, pos.shares, exec_price, f"{pnl:,.0f}", pnl_pct * 100, held,
        )

    def mark_to_market(self, date, close_prices: Dict[str, float]) -> float:
        pos_val = sum(
            close_prices.get(t, p.entry_price) * p.shares
            for t, p in self.positions.items()
        )
        return self.cash + pos_val
###################
"""
backtest/phase2/metrics.py
Performance metrics from backtest results, including benchmark comparison.
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

    # ── benchmark comparison ──────────────────────────────────────
    bm = _benchmark_metrics(equity, ann_ret, ann_factor)

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
        **bm,
    }


# ------------------------------------------------------------------
def _benchmark_metrics(
    equity: pd.DataFrame,
    strategy_ann_ret: float,
    ann_factor: float,
) -> Dict[str, Any]:
    """
    Compute benchmark return and relative metrics (alpha, beta,
    tracking error, information ratio) from the equity DataFrame.

    Expects columns: 'value', 'benchmark'.
    """
    defaults = {
        "benchmark_total_return": 0.0,
        "benchmark_ann_return": 0.0,
        "benchmark_ann_vol": 0.0,
        "benchmark_sharpe": 0.0,
        "benchmark_max_dd": 0.0,
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
    bench_total_ret = (bench_final - bench_initial) / bench_initial if bench_initial > 0 else 0.0
    bench_ann_ret = (1 + bench_total_ret) ** ann_factor - 1

    # Daily returns for both
    combined = pd.DataFrame({
        "port_ret": valid["value"].pct_change(),
        "bench_ret": valid["benchmark"].pct_change(),
    }).dropna()

    if len(combined) < 5:
        return {**defaults, "benchmark_total_return": bench_total_ret, "benchmark_ann_return": bench_ann_ret}

    bench_ann_vol = combined["bench_ret"].std() * np.sqrt(252)
    bench_sharpe = bench_ann_ret / bench_ann_vol if bench_ann_vol > 0 else 0.0

    # Benchmark drawdown
    bench_peak = valid["benchmark"].cummax()
    bench_dd = (valid["benchmark"] - bench_peak) / bench_peak
    bench_max_dd = bench_dd.min()

    # Excess returns
    excess = combined["port_ret"] - combined["bench_ret"]
    tracking_error = excess.std() * np.sqrt(252) if len(excess) > 1 else 0.0

    # Beta = Cov(Rp, Rb) / Var(Rb)
    bench_var = combined["bench_ret"].var()
    if bench_var > 0:
        beta = combined[["port_ret", "bench_ret"]].cov().iloc[0, 1] / bench_var
    else:
        beta = 0.0

    # Jensen's alpha
    alpha = strategy_ann_ret - beta * bench_ann_ret

    # Information ratio
    info_ratio = (strategy_ann_ret - bench_ann_ret) / tracking_error if tracking_error > 0 else 0.0

    return {
        "benchmark_total_return": bench_total_ret,
        "benchmark_ann_return": bench_ann_ret,
        "benchmark_ann_vol": bench_ann_vol,
        "benchmark_sharpe": bench_sharpe,
        "benchmark_max_dd": bench_max_dd,
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

###################################
"""
backtest/phase2/engine.py
Core backtesting engine with pre-computed indicators for speed.
"""
from __future__ import annotations

import logging
import time
import pandas as pd
from typing import Any, Dict, List

from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.tracker import PortfolioTracker

log = logging.getLogger(__name__)


class BacktestEngine:

    def __init__(
        self,
        data_source: BacktestDataSource,
        market: str,
        config: Dict[str, Any],
        start_date: str,
        end_date: str,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        config_name: str = "default",
    ):
        self.data_source = data_source
        self.market = market
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.config_name = config_name

        self.daily_log: List[Dict] = []
        self.equity_curve: List[tuple] = []
        self.action_history: Dict = {}

        # Pre-computed caches (populated in _precompute_all)
        self._precomputed_frames: Dict[str, pd.DataFrame] = {}
        self._precomputed_regime_df: pd.DataFrame | None = None

    # ==================================================================
    #  PRE-COMPUTATION — run all expensive rolling ops ONCE
    # ==================================================================
    def _precompute_all(self):
        """
        Pre-compute per-ticker indicators, RS z-scores, and benchmark
        regime on the FULL history.  Each backtest day then just slices
        into these frames — no recomputation.
        """
        from refactor.pipeline_v2 import (
            _canonicalize_indicator_columns,
            _fill_missing_indicators,
            annotate_scoreability,
        )
        from refactor.strategy.adapters_v2 import ensure_columns
        from refactor.strategy.regime_v2 import classify_volatility_regime
        from refactor.strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
        from compute.indicators import compute_all_indicators

        t0 = time.time()

        # 1 — per-ticker indicators
        raw_frames = {}
        for ticker, df in self.data_source.ticker_data.items():
            if df is not None and not df.empty:
                enriched = compute_all_indicators(df.copy())
                enriched = _canonicalize_indicator_columns(enriched)
                enriched = _fill_missing_indicators(enriched)
                enriched = ensure_columns(enriched)
                raw_frames[ticker] = enriched

        log.info(
            "[%s] Pre-computed indicators for %d tickers (%.1fs)",
            self.config_name, len(raw_frames), time.time() - t0,
        )

        # 2 — cross-sectional RS z-scores + regimes
        t1 = time.time()
        bench_df = self.data_source.benchmark_data
        if bench_df is not None and not bench_df.empty:
            raw_frames = compute_rs_zscores(raw_frames, bench_df)
            raw_frames = enrich_rs_regimes(raw_frames)
            self._precomputed_regime_df = classify_volatility_regime(
                bench_df,
                params=self.config.get("vol_regime_params"),
            )
        log.info(
            "[%s] Pre-computed RS + regimes (%.1fs)",
            self.config_name, time.time() - t1,
        )

        # 3 — annotate scoreability
        for ticker in raw_frames:
            raw_frames[ticker] = annotate_scoreability(raw_frames[ticker])

        self._precomputed_frames = raw_frames
        log.info(
            "[%s] Pre-computation complete: %d tickers, total %.1fs",
            self.config_name, len(raw_frames), time.time() - t0,
        )

    # ==================================================================
    #  MAIN LOOP
    # ==================================================================
    def run(self) -> Dict[str, Any]:
        tickers = self.data_source.get_tickers()
        trading_days = self.data_source.get_trading_days(
            self.start_date, self.end_date
        )
        if not trading_days:
            raise ValueError(
                f"No trading days between {self.start_date} and {self.end_date}"
            )

        # ── pre-compute everything once ───────────────────────
        self._precompute_all()

        # ── read min-hold params from signal config ───────────
        sig_params = self.config.get("signal_params", {}) or {}
        min_hold = sig_params.get("min_hold_days", 5)
        min_profit = sig_params.get("min_profit_early_exit_pct", 0.05)

        tracker = PortfolioTracker(
            initial_capital=self.initial_capital,
            max_positions=self.max_positions,
            commission_rate=self.commission_rate,
            slippage_rate=self.slippage_rate,
            min_hold_days=min_hold,
            min_profit_early_exit_pct=min_profit,
        )

        log.info(
            "[%s] backtest %s → %s  (%d days, %d tickers)  "
            "min_hold=%dd  min_profit=%.0f%%",
            self.config_name, self.start_date, self.end_date,
            len(trading_days), len(tickers), min_hold, min_profit * 100,
        )

        prev_actions: Dict[str, str] = {}
        bench_start_close: float | None = None

        for i, day in enumerate(trading_days):

            # 1 — run pipeline (fast path) ─────────────────────
            try:
                output = self._run_pipeline_fast(day, tickers)
                actions = self._extract_actions(output)
            except Exception as exc:
                log.warning(
                    "[%s] pipeline error %s: %s",
                    self.config_name, day.strftime("%Y-%m-%d"), exc,
                )
                actions = {}

            # 2 — log signals ──────────────────────────────────
            buy_sigs = [t for t, a in actions.items() if a in ("BUY", "STRONG_BUY")]
            sell_sigs = [t for t, a in actions.items() if a == "SELL"]
            if buy_sigs or sell_sigs:
                log.info(
                    "[%s] %s  BUY %s | SELL %d names",
                    self.config_name,
                    day.strftime("%Y-%m-%d"),
                    buy_sigs if buy_sigs else "—",
                    len(sell_sigs),
                )

            # 3 — execute PREVIOUS day's signals at TODAY's open
            prices_open = self._prices_fast(day, tickers, field="open")
            if i > 0 and prev_actions:
                tracker.process_signals(day, prev_actions, prices_open)

            # 4 — mark-to-market at close ──────────────────────
            prices_close = self._prices_fast(day, tickers, field="close")
            port_value = tracker.mark_to_market(day, prices_close)

            # 5 — benchmark value ──────────────────────────────
            bench_value = self._benchmark_value(day, bench_start_close)
            if bench_value is not None and bench_start_close is None:
                bench_df = self.data_source.benchmark_data
                if bench_df is not None and not bench_df.empty and day in bench_df.index:
                    bench_start_close = float(bench_df.loc[day, "close"])
                    bench_value = self.initial_capital

            # 6 — record ──────────────────────────────────────
            self.equity_curve.append((day, port_value, bench_value))
            self.daily_log.append({
                "date": day,
                "portfolio_value": port_value,
                "benchmark_value": bench_value,
                "cash": tracker.cash,
                "n_positions": len(tracker.positions),
                "positions": list(tracker.positions.keys()),
                "n_buys": sum(1 for a in actions.values()
                              if a in ("BUY", "STRONG_BUY")),
                "n_sells": sum(1 for a in actions.values()
                               if a == "SELL"),
            })
            prev_actions = actions
            self.action_history[day] = actions

            if (i + 1) % 50 == 0 or i == len(trading_days) - 1:
                log.info(
                    "[%s] %d/%d  %s  portfolio=$%s  pos=%d  cash=$%s",
                    self.config_name, i + 1, len(trading_days),
                    day.strftime("%Y-%m-%d"),
                    f"{port_value:,.0f}",
                    len(tracker.positions),
                    f"{tracker.cash:,.0f}",
                )

        return {
            "config_name": self.config_name,
            "config": self.config,
            "equity_curve": pd.DataFrame(
                self.equity_curve, columns=["date", "value", "benchmark"]
            ),
            "daily_log": pd.DataFrame(self.daily_log),
            "trade_log": (
                pd.DataFrame(tracker.closed_trades)
                if tracker.closed_trades
                else pd.DataFrame()
            ),
            "final_value": (
                self.equity_curve[-1][1]
                if self.equity_curve
                else self.initial_capital
            ),
            "initial_capital": self.initial_capital,
            "open_positions": dict(tracker.positions),
        }

    # ==================================================================
    #  FAST PIPELINE — uses pre-computed data
    # ==================================================================
    def _run_pipeline_fast(self, day, tickers) -> Dict:
        from refactor.pipeline_v2 import run_pipeline_v2

        cutoff = pd.Timestamp(day)
        lookback = self.data_source.lookback_bars

        # Slice pre-computed frames to current day
        tradable_frames = {}
        for t in tickers:
            if t not in self._precomputed_frames:
                continue
            df = self._precomputed_frames[t]
            sliced = df.loc[df.index <= cutoff]
            if sliced.empty:
                continue
            if len(sliced) > lookback:
                sliced = sliced.iloc[-lookback:]
            tradable_frames[t] = sliced

        # Slice benchmark
        bench_df = self.data_source.benchmark_data
        if bench_df is not None:
            bench_df = bench_df.loc[bench_df.index <= cutoff]
            if len(bench_df) > lookback:
                bench_df = bench_df.iloc[-lookback:]

        if bench_df is None or bench_df.empty:
            raise ValueError(f"Benchmark empty on {day}")

        pipeline_config = {
            "vol_regime_params": self.config.get("vol_regime_params"),
            "scoring_weights": self.config.get("scoring_weights"),
            "scoring_params": self.config.get("scoring_params"),
            "signal_params": self.config.get("signal_params"),
            "convergence_params": self.config.get("convergence_params"),
            "action_params": self.config.get("action_params"),
            "breadth_params": self.config.get("breadth_params"),
            "rotation_params": self.config.get("rotation_params"),
        }

        return run_pipeline_v2(
            tradable_frames=tradable_frames,
            bench_df=bench_df,
            market=self.market,
            config=pipeline_config,
            precomputed=True,
        )

    # ==================================================================
    #  Fast price lookup from pre-computed frames
    # ==================================================================
    def _prices_fast(
        self, day, tickers, field: str = "open"
    ) -> Dict[str, float]:
        cutoff = pd.Timestamp(day)
        prices = {}
        for t in tickers:
            df = self._precomputed_frames.get(t)
            if df is not None and not df.empty and cutoff in df.index:
                val = df.loc[cutoff, field]
                if pd.notna(val):
                    prices[t] = float(val)
        return prices

    # ==================================================================
    #  Benchmark helper
    # ==================================================================
    def _benchmark_value(self, day, bench_start_close) -> float | None:
        bench_df = self.data_source.benchmark_data
        if bench_df is None or bench_df.empty:
            return None
        if day not in bench_df.index:
            return None
        if bench_start_close is None:
            return self.initial_capital
        return self.initial_capital * (float(bench_df.loc[day, "close"]) / bench_start_close)

    # ==================================================================
    #  Extract actions from pipeline output
    # ==================================================================
    def _extract_actions(self, output: Dict) -> Dict[str, str]:
        actions: Dict[str, str] = {}
        for key in ("action_table", "snapshot", "actions"):
            if key not in output:
                continue
            df = output[key]
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            act_col = next(
                (c for c in ("action_v2", "action", "signal") if c in df.columns),
                None,
            )
            if act_col is None:
                continue
            if "ticker" in df.columns:
                for _, row in df.iterrows():
                    actions[row["ticker"]] = str(row[act_col]).upper()
            else:
                for ticker, row in df.iterrows():
                    actions[str(ticker)] = str(row[act_col]).upper()
            break
        return actions

#################################
"""backtest/phase2/diagnostics.py

Drop-in signal diagnostics. Wire into your engine's day loop
and signal generator to capture *why* BUY/SELL/HOLD decisions
are made.

Usage:
    from backtest.phase2.diagnostics import SignalDiagnostics
    diag = SignalDiagnostics("Loose", enabled=True)
    # ... wire calls at each decision point (see integration notes below)
    diag.print_summary()
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ── reason tags (use these as constants so summaries group cleanly) ─────
R_SCORE_BELOW_EXIT    = "score<exit"
R_SCORE_ABOVE_ENTRY   = "score>=entry"
R_RANK_BELOW_FLOOR    = "rank<exit_floor"
R_RANK_ABOVE_MIN      = "rank>=min_rank"
R_RANK_BELOW_MIN      = "rank<min_rank"
R_RS_BLOCKED          = "rs_regime_blocked"
R_RS_FAIL_PENALTY     = "rs_fail_penalty_applied"
R_SECTOR_BLOCKED      = "sector_regime_blocked"
R_BREADTH_HARD_BLOCK  = "breadth_hard_block"
R_VOL_HARD_BLOCK      = "vol_hard_block"
R_COOLDOWN            = "cooldown_active"
R_MAX_POSITIONS       = "max_positions_reached"
R_CONTINUATION        = "continuation_pass"
R_CONTINUATION_FAIL   = "continuation_fail"
R_PULLBACK            = "pullback_pass"
R_PULLBACK_FAIL       = "pullback_fail"
R_CHAOTIC_EXIT_BUMP   = "chaotic_exit_bump"
R_NOT_HELD            = "not_held"
R_HELD_OK             = "held_score_ok"


class SignalDiagnostics:
    """Captures per-day, per-ticker decision data and emits summaries."""

    def __init__(self, name: str = "", enabled: bool = True,
                 verbose_top_n: int = 5, verbose_sells: int = 3):
        self.name = name
        self.enabled = enabled
        self.verbose_top_n = verbose_top_n
        self.verbose_sells = verbose_sells

        # accumulation across entire backtest
        self.daily_records: list[dict] = []
        self._rec: dict | None = None

    # ── per-day lifecycle ───────────────────────────────────────────────

    def begin_day(
        self,
        date,
        vol_regime: str,
        breadth_regime: str,
        base_entry: float,
        base_exit: float,
        regime_entry_adj: float,
        breadth_entry_adj: float,
        adjusted_entry: float,
        adjusted_exit: float,
        size_multiplier: float = 1.0,
        n_held: int = 0,
        max_positions: int = 0,
    ):
        """Call once at the start of each trading day, after computing
        regime adjustments but before iterating over tickers."""
        if not self.enabled:
            return
        self._rec = {
            "date": date,
            "vol_regime": vol_regime,
            "breadth_regime": breadth_regime,
            "base_entry": base_entry,
            "base_exit": base_exit,
            "regime_entry_adj": regime_entry_adj,
            "breadth_entry_adj": breadth_entry_adj,
            "adjusted_entry": adjusted_entry,
            "adjusted_exit": adjusted_exit,
            "size_multiplier": size_multiplier,
            "n_held": n_held,
            "max_positions": max_positions,
            # populated by log_score / log_decision
            "composites": {},          # ticker -> float
            "sub_scores": {},          # ticker -> dict
            "ranks": {},               # ticker -> float
            "rs_regimes": {},          # ticker -> str
            "sector_regimes": {},      # ticker -> str
            "decisions": {},           # ticker -> (action, [reasons])
            "sell_triggers": defaultdict(list),
        }

    def log_score(
        self,
        ticker: str,
        composite: float,
        rank_pct: float | None = None,
        sub_scores: dict | None = None,
        rs_regime: str | None = None,
        sector_regime: str | None = None,
    ):
        """Call for every ticker after the scoring pipeline runs."""
        if not self.enabled or self._rec is None:
            return
        self._rec["composites"][ticker] = composite
        if rank_pct is not None:
            self._rec["ranks"][ticker] = rank_pct
        if sub_scores:
            self._rec["sub_scores"][ticker] = sub_scores
        if rs_regime:
            self._rec["rs_regimes"][ticker] = rs_regime
        if sector_regime:
            self._rec["sector_regimes"][ticker] = sector_regime

    def log_decision(self, ticker: str, action: str, reasons: list[str]):
        """Call when a final BUY / SELL / HOLD / BLOCKED decision is made.

        action: one of 'BUY', 'SELL', 'HOLD', 'BLOCKED'
        reasons: list of R_* tags (or free-form strings)
        """
        if not self.enabled or self._rec is None:
            return
        self._rec["decisions"][ticker] = (action, reasons)
        if action == "SELL":
            for r in reasons:
                self._rec["sell_triggers"][r].append(ticker)

    def end_day(self):
        """Call at end of day. Logs summary lines and archives the record."""
        if not self.enabled or self._rec is None:
            return
        rec = self._rec
        scores = np.array(list(rec["composites"].values())) if rec["composites"] else np.array([])
        ranks = np.array(list(rec["ranks"].values())) if rec["ranks"] else np.array([])

        # ── 1. score distribution vs thresholds ────────────────────────
        if len(scores) > 0:
            p = np.percentile(scores, [5, 10, 25, 50, 75, 90, 95])
            above_entry = int(np.sum(scores >= rec["adjusted_entry"]))
            in_band = int(np.sum(
                (scores >= rec["adjusted_exit"]) & (scores < rec["adjusted_entry"])
            ))
            below_exit = int(np.sum(scores < rec["adjusted_exit"]))

            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SCORES  "
                f"n={len(scores)}  "
                f"p5={p[0]:.3f} p10={p[1]:.3f} p25={p[2]:.3f} "
                f"p50={p[3]:.3f} p75={p[4]:.3f} p90={p[5]:.3f} p95={p[6]:.3f}  "
                f"above_entry={above_entry}  in_band={in_band}  below_exit={below_exit}"
            )

        # ── 2. threshold computation trace ─────────────────────────────
        logger.info(
            f"[{self.name}] {rec['date']}  DIAG-THRESH  "
            f"vol={rec['vol_regime']}  breadth={rec['breadth_regime']}  "
            f"base_entry={rec['base_entry']:.3f}  "
            f"+regime_adj={rec['regime_entry_adj']:+.3f}  "
            f"+breadth_adj={rec['breadth_entry_adj']:+.3f}  "
            f"= adjusted_entry={rec['adjusted_entry']:.3f}  "
            f"adjusted_exit={rec['adjusted_exit']:.3f}  "
            f"size_mult={rec['size_multiplier']:.2f}  "
            f"held={rec['n_held']}/{rec['max_positions']}"
        )

        # ── 3. rank distribution (if populated) ────────────────────────
        if len(ranks) > 0:
            rp = np.percentile(ranks, [10, 50, 90])
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"n={len(ranks)}  p10={rp[0]:.3f}  p50={rp[1]:.3f}  p90={rp[2]:.3f}"
            )
        else:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"** NO RANK DATA — rank keys may not be wired **"
            )

        # ── 4. top N scores with full breakdown ────────────────────────
        if rec["composites"]:
            top = sorted(rec["composites"].items(), key=lambda x: x[1], reverse=True)
            for ticker, comp in top[: self.verbose_top_n]:
                sub = rec["sub_scores"].get(ticker, {})
                rank = rec["ranks"].get(ticker)
                rs = rec["rs_regimes"].get(ticker, "?")
                sec = rec["sector_regimes"].get(ticker, "?")
                dec_action, dec_reasons = rec["decisions"].get(ticker, ("?", []))

                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                rank_str = f"rank={rank:.3f}" if rank is not None else "rank=N/A"

                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-TOP     "
                    f"{ticker:20s}  comp={comp:.4f}  {rank_str}  "
                    f"rs={rs}  sec={sec}  {sub_str}  "
                    f"→ {dec_action}  {dec_reasons}"
                )

        # ── 5. sell trigger breakdown ──────────────────────────────────
        if rec["sell_triggers"]:
            counts = {k: len(v) for k, v in rec["sell_triggers"].items()}
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SELLS   {counts}"
            )
            # show a few examples of score-below-exit sells
            score_sells = rec["sell_triggers"].get(R_SCORE_BELOW_EXIT, [])
            for ticker in score_sells[: self.verbose_sells]:
                comp = rec["composites"].get(ticker, 0)
                sub = rec["sub_scores"].get(ticker, {})
                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-SELL-EX "
                    f"{ticker:20s}  comp={comp:.4f}  {sub_str}"
                )

        # ── 6. blocked buys (score above entry but blocked by filter) ──
        blocked = [
            (t, d) for t, d in rec["decisions"].items() if d[0] == "BLOCKED"
        ]
        if blocked:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                f"{len(blocked)} tickers passed score threshold but were blocked"
            )
            for ticker, (_, reasons) in blocked[: self.verbose_top_n]:
                comp = rec["composites"].get(ticker, 0)
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                    f"{ticker:20s}  comp={comp:.4f}  {reasons}"
                )

        self.daily_records.append(rec)
        self._rec = None

    # ── end-of-backtest summary ─────────────────────────────────────────

    def print_summary(self):
        """Call once after the backtest loop finishes."""
        if not self.daily_records:
            logger.info(f"[{self.name}] DIAG-SUMMARY  no data recorded")
            return

        n_days = len(self.daily_records)
        all_scores = []
        all_entries = []
        all_exits = []
        action_counts = defaultdict(int)
        trigger_counts = defaultdict(int)
        regime_day_counts = defaultdict(int)
        breadth_day_counts = defaultdict(int)
        days_with_rank = 0

        for rec in self.daily_records:
            all_scores.extend(rec["composites"].values())
            all_entries.append(rec["adjusted_entry"])
            all_exits.append(rec["adjusted_exit"])
            regime_day_counts[rec["vol_regime"]] += 1
            breadth_day_counts[rec["breadth_regime"]] += 1
            if rec["ranks"]:
                days_with_rank += 1
            for _, (action, reasons) in rec["decisions"].items():
                action_counts[action] += 1
                if action == "SELL":
                    for r in reasons:
                        trigger_counts[r] += 1

        scores_arr = np.array(all_scores) if all_scores else np.array([0])
        entries_arr = np.array(all_entries)
        exits_arr = np.array(all_exits)

        logger.info(f"[{self.name}] {'=' * 60}")
        logger.info(f"[{self.name}] DIAGNOSTIC SUMMARY  ({n_days} trading days)")
        logger.info(f"[{self.name}] {'=' * 60}")

        logger.info(
            f"[{self.name}]   Score distribution (all ticker-days):  "
            f"mean={scores_arr.mean():.4f}  std={scores_arr.std():.4f}  "
            f"min={scores_arr.min():.4f}  max={scores_arr.max():.4f}"
        )
        p = np.percentile(scores_arr, [5, 25, 50, 75, 90, 95, 99])
        logger.info(
            f"[{self.name}]   Score percentiles:  "
            f"p5={p[0]:.4f}  p25={p[1]:.4f}  p50={p[2]:.4f}  "
            f"p75={p[3]:.4f}  p90={p[4]:.4f}  p95={p[5]:.4f}  p99={p[6]:.4f}"
        )

        pct_above_entry = 100.0 * np.mean(scores_arr >= entries_arr.mean())
        pct_below_exit = 100.0 * np.mean(scores_arr < exits_arr.mean())
        logger.info(
            f"[{self.name}]   Avg entry threshold: {entries_arr.mean():.4f}  "
            f"Avg exit threshold: {exits_arr.mean():.4f}"
        )
        logger.info(
            f"[{self.name}]   %% ticker-days above avg entry: {pct_above_entry:.1f}%%  "
            f"below avg exit: {pct_below_exit:.1f}%%"
        )

        logger.info(f"[{self.name}]   Vol regime days:     {dict(regime_day_counts)}")
        logger.info(f"[{self.name}]   Breadth regime days:  {dict(breadth_day_counts)}")
        logger.info(f"[{self.name}]   Days with rank data:  {days_with_rank}/{n_days}")

        logger.info(f"[{self.name}]   Decision totals:      {dict(action_counts)}")
        if trigger_counts:
            logger.info(f"[{self.name}]   Sell trigger totals:  {dict(trigger_counts)}")

        # gap analysis: how far is p90 score from entry threshold?
        daily_gaps = []
        for rec in self.daily_records:
            s = list(rec["composites"].values())
            if s:
                daily_gaps.append(np.percentile(s, 90) - rec["adjusted_entry"])
        if daily_gaps:
            gaps = np.array(daily_gaps)
            logger.info(
                f"[{self.name}]   Daily (p90_score - entry_thresh):  "
                f"mean={gaps.mean():+.4f}  min={gaps.min():+.4f}  max={gaps.max():+.4f}"
            )
            if gaps.mean() < 0:
                logger.warning(
                    f"[{self.name}]   ⚠ p90 score is BELOW entry threshold on average. "
                    f"Scoring pipeline may be systematically too low, or thresholds too high."
                )

        logger.info(f"[{self.name}] {'=' * 60}")

##############
"""backtest/phase2/diagnostics.py

Drop-in signal diagnostics. Wire into your engine's day loop
and signal generator to capture *why* BUY/SELL/HOLD decisions
are made.

Usage:
    from backtest.phase2.diagnostics import SignalDiagnostics
    diag = SignalDiagnostics("Loose", enabled=True)
    # ... wire calls at each decision point (see integration notes below)
    diag.print_summary()
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ── reason tags (use these as constants so summaries group cleanly) ─────
R_SCORE_BELOW_EXIT    = "score<exit"
R_SCORE_ABOVE_ENTRY   = "score>=entry"
R_RANK_BELOW_FLOOR    = "rank<exit_floor"
R_RANK_ABOVE_MIN      = "rank>=min_rank"
R_RANK_BELOW_MIN      = "rank<min_rank"
R_RS_BLOCKED          = "rs_regime_blocked"
R_RS_FAIL_PENALTY     = "rs_fail_penalty_applied"
R_SECTOR_BLOCKED      = "sector_regime_blocked"
R_BREADTH_HARD_BLOCK  = "breadth_hard_block"
R_VOL_HARD_BLOCK      = "vol_hard_block"
R_COOLDOWN            = "cooldown_active"
R_MAX_POSITIONS       = "max_positions_reached"
R_CONTINUATION        = "continuation_pass"
R_CONTINUATION_FAIL   = "continuation_fail"
R_PULLBACK            = "pullback_pass"
R_PULLBACK_FAIL       = "pullback_fail"
R_CHAOTIC_EXIT_BUMP   = "chaotic_exit_bump"
R_NOT_HELD            = "not_held"
R_HELD_OK             = "held_score_ok"


class SignalDiagnostics:
    """Captures per-day, per-ticker decision data and emits summaries."""

    def __init__(self, name: str = "", enabled: bool = True,
                 verbose_top_n: int = 5, verbose_sells: int = 3):
        self.name = name
        self.enabled = enabled
        self.verbose_top_n = verbose_top_n
        self.verbose_sells = verbose_sells

        # accumulation across entire backtest
        self.daily_records: list[dict] = []
        self._rec: dict | None = None

    # ── per-day lifecycle ───────────────────────────────────────────────

    def begin_day(
        self,
        date,
        vol_regime: str,
        breadth_regime: str,
        base_entry: float,
        base_exit: float,
        regime_entry_adj: float,
        breadth_entry_adj: float,
        adjusted_entry: float,
        adjusted_exit: float,
        size_multiplier: float = 1.0,
        n_held: int = 0,
        max_positions: int = 0,
    ):
        """Call once at the start of each trading day, after computing
        regime adjustments but before iterating over tickers."""
        if not self.enabled:
            return
        self._rec = {
            "date": date,
            "vol_regime": vol_regime,
            "breadth_regime": breadth_regime,
            "base_entry": base_entry,
            "base_exit": base_exit,
            "regime_entry_adj": regime_entry_adj,
            "breadth_entry_adj": breadth_entry_adj,
            "adjusted_entry": adjusted_entry,
            "adjusted_exit": adjusted_exit,
            "size_multiplier": size_multiplier,
            "n_held": n_held,
            "max_positions": max_positions,
            # populated by log_score / log_decision
            "composites": {},          # ticker -> float
            "sub_scores": {},          # ticker -> dict
            "ranks": {},               # ticker -> float
            "rs_regimes": {},          # ticker -> str
            "sector_regimes": {},      # ticker -> str
            "decisions": {},           # ticker -> (action, [reasons])
            "sell_triggers": defaultdict(list),
        }

    def log_score(
        self,
        ticker: str,
        composite: float,
        rank_pct: float | None = None,
        sub_scores: dict | None = None,
        rs_regime: str | None = None,
        sector_regime: str | None = None,
    ):
        """Call for every ticker after the scoring pipeline runs."""
        if not self.enabled or self._rec is None:
            return
        self._rec["composites"][ticker] = composite
        if rank_pct is not None:
            self._rec["ranks"][ticker] = rank_pct
        if sub_scores:
            self._rec["sub_scores"][ticker] = sub_scores
        if rs_regime:
            self._rec["rs_regimes"][ticker] = rs_regime
        if sector_regime:
            self._rec["sector_regimes"][ticker] = sector_regime

    def log_decision(self, ticker: str, action: str, reasons: list[str]):
        """Call when a final BUY / SELL / HOLD / BLOCKED decision is made.

        action: one of 'BUY', 'SELL', 'HOLD', 'BLOCKED'
        reasons: list of R_* tags (or free-form strings)
        """
        if not self.enabled or self._rec is None:
            return
        self._rec["decisions"][ticker] = (action, reasons)
        if action == "SELL":
            for r in reasons:
                self._rec["sell_triggers"][r].append(ticker)

    def end_day(self):
        """Call at end of day. Logs summary lines and archives the record."""
        if not self.enabled or self._rec is None:
            return
        rec = self._rec
        scores = np.array(list(rec["composites"].values())) if rec["composites"] else np.array([])
        ranks = np.array(list(rec["ranks"].values())) if rec["ranks"] else np.array([])

        # ── 1. score distribution vs thresholds ────────────────────────
        if len(scores) > 0:
            p = np.percentile(scores, [5, 10, 25, 50, 75, 90, 95])
            above_entry = int(np.sum(scores >= rec["adjusted_entry"]))
            in_band = int(np.sum(
                (scores >= rec["adjusted_exit"]) & (scores < rec["adjusted_entry"])
            ))
            below_exit = int(np.sum(scores < rec["adjusted_exit"]))

            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SCORES  "
                f"n={len(scores)}  "
                f"p5={p[0]:.3f} p10={p[1]:.3f} p25={p[2]:.3f} "
                f"p50={p[3]:.3f} p75={p[4]:.3f} p90={p[5]:.3f} p95={p[6]:.3f}  "
                f"above_entry={above_entry}  in_band={in_band}  below_exit={below_exit}"
            )

        # ── 2. threshold computation trace ─────────────────────────────
        logger.info(
            f"[{self.name}] {rec['date']}  DIAG-THRESH  "
            f"vol={rec['vol_regime']}  breadth={rec['breadth_regime']}  "
            f"base_entry={rec['base_entry']:.3f}  "
            f"+regime_adj={rec['regime_entry_adj']:+.3f}  "
            f"+breadth_adj={rec['breadth_entry_adj']:+.3f}  "
            f"= adjusted_entry={rec['adjusted_entry']:.3f}  "
            f"adjusted_exit={rec['adjusted_exit']:.3f}  "
            f"size_mult={rec['size_multiplier']:.2f}  "
            f"held={rec['n_held']}/{rec['max_positions']}"
        )

        # ── 3. rank distribution (if populated) ────────────────────────
        if len(ranks) > 0:
            rp = np.percentile(ranks, [10, 50, 90])
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"n={len(ranks)}  p10={rp[0]:.3f}  p50={rp[1]:.3f}  p90={rp[2]:.3f}"
            )
        else:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"** NO RANK DATA — rank keys may not be wired **"
            )

        # ── 4. top N scores with full breakdown ────────────────────────
        if rec["composites"]:
            top = sorted(rec["composites"].items(), key=lambda x: x[1], reverse=True)
            for ticker, comp in top[: self.verbose_top_n]:
                sub = rec["sub_scores"].get(ticker, {})
                rank = rec["ranks"].get(ticker)
                rs = rec["rs_regimes"].get(ticker, "?")
                sec = rec["sector_regimes"].get(ticker, "?")
                dec_action, dec_reasons = rec["decisions"].get(ticker, ("?", []))

                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                rank_str = f"rank={rank:.3f}" if rank is not None else "rank=N/A"

                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-TOP     "
                    f"{ticker:20s}  comp={comp:.4f}  {rank_str}  "
                    f"rs={rs}  sec={sec}  {sub_str}  "
                    f"→ {dec_action}  {dec_reasons}"
                )

        # ── 5. sell trigger breakdown ──────────────────────────────────
        if rec["sell_triggers"]:
            counts = {k: len(v) for k, v in rec["sell_triggers"].items()}
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SELLS   {counts}"
            )
            # show a few examples of score-below-exit sells
            score_sells = rec["sell_triggers"].get(R_SCORE_BELOW_EXIT, [])
            for ticker in score_sells[: self.verbose_sells]:
                comp = rec["composites"].get(ticker, 0)
                sub = rec["sub_scores"].get(ticker, {})
                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-SELL-EX "
                    f"{ticker:20s}  comp={comp:.4f}  {sub_str}"
                )

        # ── 6. blocked buys (score above entry but blocked by filter) ──
        blocked = [
            (t, d) for t, d in rec["decisions"].items() if d[0] == "BLOCKED"
        ]
        if blocked:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                f"{len(blocked)} tickers passed score threshold but were blocked"
            )
            for ticker, (_, reasons) in blocked[: self.verbose_top_n]:
                comp = rec["composites"].get(ticker, 0)
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                    f"{ticker:20s}  comp={comp:.4f}  {reasons}"
                )

        self.daily_records.append(rec)
        self._rec = None

    # ── end-of-backtest summary ─────────────────────────────────────────

    def print_summary(self):
        """Call once after the backtest loop finishes."""
        if not self.daily_records:
            logger.info(f"[{self.name}] DIAG-SUMMARY  no data recorded")
            return

        n_days = len(self.daily_records)
        all_scores = []
        all_entries = []
        all_exits = []
        action_counts = defaultdict(int)
        trigger_counts = defaultdict(int)
        regime_day_counts = defaultdict(int)
        breadth_day_counts = defaultdict(int)
        days_with_rank = 0

        for rec in self.daily_records:
            all_scores.extend(rec["composites"].values())
            all_entries.append(rec["adjusted_entry"])
            all_exits.append(rec["adjusted_exit"])
            regime_day_counts[rec["vol_regime"]] += 1
            breadth_day_counts[rec["breadth_regime"]] += 1
            if rec["ranks"]:
                days_with_rank += 1
            for _, (action, reasons) in rec["decisions"].items():
                action_counts[action] += 1
                if action == "SELL":
                    for r in reasons:
                        trigger_counts[r] += 1

        scores_arr = np.array(all_scores) if all_scores else np.array([0])
        entries_arr = np.array(all_entries)
        exits_arr = np.array(all_exits)

        logger.info(f"[{self.name}] {'=' * 60}")
        logger.info(f"[{self.name}] DIAGNOSTIC SUMMARY  ({n_days} trading days)")
        logger.info(f"[{self.name}] {'=' * 60}")

        logger.info(
            f"[{self.name}]   Score distribution (all ticker-days):  "
            f"mean={scores_arr.mean():.4f}  std={scores_arr.std():.4f}  "
            f"min={scores_arr.min():.4f}  max={scores_arr.max():.4f}"
        )
        p = np.percentile(scores_arr, [5, 25, 50, 75, 90, 95, 99])
        logger.info(
            f"[{self.name}]   Score percentiles:  "
            f"p5={p[0]:.4f}  p25={p[1]:.4f}  p50={p[2]:.4f}  "
            f"p75={p[3]:.4f}  p90={p[4]:.4f}  p95={p[5]:.4f}  p99={p[6]:.4f}"
        )

        pct_above_entry = 100.0 * np.mean(scores_arr >= entries_arr.mean())
        pct_below_exit = 100.0 * np.mean(scores_arr < exits_arr.mean())
        logger.info(
            f"[{self.name}]   Avg entry threshold: {entries_arr.mean():.4f}  "
            f"Avg exit threshold: {exits_arr.mean():.4f}"
        )
        logger.info(
            f"[{self.name}]   %% ticker-days above avg entry: {pct_above_entry:.1f}%%  "
            f"below avg exit: {pct_below_exit:.1f}%%"
        )

        logger.info(f"[{self.name}]   Vol regime days:     {dict(regime_day_counts)}")
        logger.info(f"[{self.name}]   Breadth regime days:  {dict(breadth_day_counts)}")
        logger.info(f"[{self.name}]   Days with rank data:  {days_with_rank}/{n_days}")

        logger.info(f"[{self.name}]   Decision totals:      {dict(action_counts)}")
        if trigger_counts:
            logger.info(f"[{self.name}]   Sell trigger totals:  {dict(trigger_counts)}")

        # gap analysis: how far is p90 score from entry threshold?
        daily_gaps = []
        for rec in self.daily_records:
            s = list(rec["composites"].values())
            if s:
                daily_gaps.append(np.percentile(s, 90) - rec["adjusted_entry"])
        if daily_gaps:
            gaps = np.array(daily_gaps)
            logger.info(
                f"[{self.name}]   Daily (p90_score - entry_thresh):  "
                f"mean={gaps.mean():+.4f}  min={gaps.min():+.4f}  max={gaps.max():+.4f}"
            )
            if gaps.mean() < 0:
                logger.warning(
                    f"[{self.name}]   ⚠ p90 score is BELOW entry threshold on average. "
                    f"Scoring pipeline may be systematically too low, or thresholds too high."
                )

        logger.info(f"[{self.name}] {'=' * 60}")

#########################
"""
backtest/phase2/metrics.py
Performance metrics from backtest results, including benchmark comparison.
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

    # ── benchmark comparison ──────────────────────────────────────
    bm = _benchmark_metrics(equity, ann_ret, ann_factor)

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
        **bm,
    }


# ------------------------------------------------------------------
def _benchmark_metrics(
    equity: pd.DataFrame,
    strategy_ann_ret: float,
    ann_factor: float,
) -> Dict[str, Any]:
    """
    Compute benchmark return and relative metrics (alpha, beta,
    tracking error, information ratio) from the equity DataFrame.

    Expects columns: 'value', 'benchmark'.
    """
    defaults = {
        "benchmark_total_return": 0.0,
        "benchmark_ann_return": 0.0,
        "benchmark_ann_vol": 0.0,
        "benchmark_sharpe": 0.0,
        "benchmark_max_dd": 0.0,
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
    bench_total_ret = (bench_final - bench_initial) / bench_initial if bench_initial > 0 else 0.0
    bench_ann_ret = (1 + bench_total_ret) ** ann_factor - 1

    # Daily returns for both
    combined = pd.DataFrame({
        "port_ret": valid["value"].pct_change(),
        "bench_ret": valid["benchmark"].pct_change(),
    }).dropna()

    if len(combined) < 5:
        return {**defaults, "benchmark_total_return": bench_total_ret, "benchmark_ann_return": bench_ann_ret}

    bench_ann_vol = combined["bench_ret"].std() * np.sqrt(252)
    bench_sharpe = bench_ann_ret / bench_ann_vol if bench_ann_vol > 0 else 0.0

    # Benchmark drawdown
    bench_peak = valid["benchmark"].cummax()
    bench_dd = (valid["benchmark"] - bench_peak) / bench_peak
    bench_max_dd = bench_dd.min()

    # Excess returns
    excess = combined["port_ret"] - combined["bench_ret"]
    tracking_error = excess.std() * np.sqrt(252) if len(excess) > 1 else 0.0

    # Beta = Cov(Rp, Rb) / Var(Rb)
    bench_var = combined["bench_ret"].var()
    if bench_var > 0:
        beta = combined[["port_ret", "bench_ret"]].cov().iloc[0, 1] / bench_var
    else:
        beta = 0.0

    # Jensen's alpha
    alpha = strategy_ann_ret - beta * bench_ann_ret

    # Information ratio
    info_ratio = (strategy_ann_ret - bench_ann_ret) / tracking_error if tracking_error > 0 else 0.0

    return {
        "benchmark_total_return": bench_total_ret,
        "benchmark_ann_return": bench_ann_ret,
        "benchmark_ann_vol": bench_ann_vol,
        "benchmark_sharpe": bench_sharpe,
        "benchmark_max_dd": bench_max_dd,
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

##############
"""
backtest/phase2/data_source.py
Date-aware data source wrapper for backtesting.

Loads from a single parquet file per market (data/{market}_cash.parquet),
filters to a given ticker universe, and presents a sliding window up to
the current backtest date.

Expected parquet schema (long format):
    date | ticker | open | high | low | close | volume

The 'date' column should be parseable as datetime.  Column names are
normalised to lowercase on load.  If the parquet uses 'symbol' instead
of 'ticker', that works too.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

log = logging.getLogger(__name__)


class BacktestDataSource:

    def __init__(
        self,
        ticker_data: Dict[str, pd.DataFrame],
        benchmark_data: Optional[pd.DataFrame] = None,
        lookback_bars: int = 300,
    ):
        self.ticker_data = ticker_data
        self.benchmark_data = benchmark_data
        self.lookback_bars = lookback_bars
        self._cutoff: Optional[pd.Timestamp] = None

    # ==================================================================
    #  Factory — build from parquet + universe list
    # ==================================================================
    @classmethod
    def from_parquet(
        cls,
        parquet_path: str | Path,
        tickers: List[str],
        benchmark_ticker: Optional[str] = None,
        lookback_bars: int = 300,
    ) -> "BacktestDataSource":
        """
        Load ``data/{market}_cash.parquet`` and split into per-ticker
        DataFrames keyed by ticker with a DatetimeIndex.

        Args:
            parquet_path:     Path to the parquet file.
            tickers:          Universe tickers to keep.
            benchmark_ticker: e.g. "2800.HK".  Looked up in the same
                              parquet; ignored if not found.
            lookback_bars:    Max bars per fetch() call.
        """
        path = Path(parquet_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        raw = pd.read_parquet(path)
        raw.columns = raw.columns.str.strip().str.lower()

        # ── normalise ticker column ──────────────────────────────
        ticker_col = _find_column(raw, ("ticker", "symbol", "code", "stock"))
        if ticker_col is None:
            raise KeyError(
                f"Cannot find a ticker/symbol column in {path}.  "
                f"Columns present: {list(raw.columns)}"
            )

        # ── normalise date column / index ────────────────────────
        date_col = _find_column(raw, ("date", "datetime", "timestamp", "time"))
        if date_col is not None:
            raw[date_col] = pd.to_datetime(raw[date_col])
        elif isinstance(raw.index, pd.DatetimeIndex):
            raw = raw.reset_index()
            date_col = raw.columns[0]  # the old index name
        else:
            raise KeyError(
                f"Cannot find a date column in {path}.  "
                f"Columns present: {list(raw.columns)}"
            )

        # ── normalise OHLCV column names ─────────────────────────
        rename_map = {}
        for target, candidates in [
            ("open",   ("open", "o")),
            ("high",   ("high", "h")),
            ("low",    ("low", "l")),
            ("close",  ("close", "c", "adj close", "adj_close", "adjclose")),
            ("volume", ("volume", "vol", "v")),
        ]:
            found = _find_column(raw, candidates)
            if found and found != target:
                rename_map[found] = target
        if rename_map:
            raw = raw.rename(columns=rename_map)

        needed = {"open", "high", "low", "close", "volume"}
        missing = needed - set(raw.columns)
        if missing:
            raise KeyError(f"Missing OHLCV columns after rename: {missing}")

        # ── filter to universe tickers ───────────────────────────
        all_tickers_in_file = set(raw[ticker_col].unique())
        want = set(tickers)
        if benchmark_ticker:
            want.add(benchmark_ticker)

        found_tickers = want & all_tickers_in_file
        not_found = want - all_tickers_in_file
        if not_found:
            log.warning(
                "%d tickers not in parquet and will be skipped: %s",
                len(not_found),
                sorted(not_found)[:20],
            )

        raw = raw[raw[ticker_col].isin(found_tickers)].copy()
        raw = raw.sort_values([ticker_col, date_col])

        # ── split into {ticker: DataFrame} ───────────────────────
        ticker_data: Dict[str, pd.DataFrame] = {}
        benchmark_data: Optional[pd.DataFrame] = None

        for tkr, group in raw.groupby(ticker_col):
            df = (
                group.set_index(date_col)[["open", "high", "low", "close", "volume"]]
                .sort_index()
                .copy()
            )
            df.index.name = "date"
            # drop exact duplicate indices if any
            df = df[~df.index.duplicated(keep="last")]
            ticker_data[tkr] = df

            if tkr == benchmark_ticker:
                benchmark_data = df.copy()

        log.info(
            "Loaded %d tickers from %s  (date range %s → %s)",
            len(ticker_data),
            path.name,
            raw[date_col].min().strftime("%Y-%m-%d"),
            raw[date_col].max().strftime("%Y-%m-%d"),
        )

        return cls(
            ticker_data=ticker_data,
            benchmark_data=benchmark_data,
            lookback_bars=lookback_bars,
        )

    # ==================================================================
    #  Cutoff management
    # ==================================================================
    def set_cutoff(self, dt) -> None:
        self._cutoff = pd.Timestamp(dt)

    # ==================================================================
    #  Data access
    # ==================================================================
    def fetch(self, ticker: str) -> pd.DataFrame:
        if ticker not in self.ticker_data:
            return pd.DataFrame()
        df = self.ticker_data[ticker]
        if self._cutoff is not None:
            df = df.loc[df.index <= self._cutoff]
        if len(df) > self.lookback_bars:
            df = df.iloc[-self.lookback_bars:]
        return df.copy()

    def fetch_benchmark(self) -> pd.DataFrame:
        if self.benchmark_data is None:
            return pd.DataFrame()
        df = self.benchmark_data
        if self._cutoff is not None:
            df = df.loc[df.index <= self._cutoff]
        if len(df) > self.lookback_bars:
            df = df.iloc[-self.lookback_bars:]
        return df.copy()

    def get_tickers(self) -> List[str]:
        return list(self.ticker_data.keys())

    def get_trading_days(self, start: str, end: str) -> List[pd.Timestamp]:
        all_dates: set = set()
        for df in self.ticker_data.values():
            all_dates.update(df.index.tolist())
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        return sorted(d for d in all_dates if s <= d <= e)

    def get_date_range(self) -> tuple:
        """Min and max dates across all loaded tickers."""
        lo, hi = pd.Timestamp.max, pd.Timestamp.min
        for df in self.ticker_data.values():
            if not df.empty:
                lo = min(lo, df.index.min())
                hi = max(hi, df.index.max())
        return lo, hi


# ------------------------------------------------------------------
#  helpers
# ------------------------------------------------------------------

def _find_column(df: pd.DataFrame, candidates: tuple) -> Optional[str]:
    """Return the first column name from *candidates* present in df."""
    cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in cols_lower:
            return cols_lower[c]
    return None