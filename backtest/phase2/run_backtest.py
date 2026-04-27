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