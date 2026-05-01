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
from rich.panel import Panel
from rich.text import Text

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
    "buy_ranking_params": {
    # How much to tilt toward momentum/beta (0=pure composite, 1=pure momentum)
    "momentum_tilt": 0.50,

    # Within the momentum signal, how much weight to give volatility (beta proxy)
    "vol_preference": 0.30,

    # FILTER: skip any ticker with annualized vol below this (kills bonds/utilities)
    # SPY is ~15-20%, high-beta growth is 35-60%. Set to 0.25 to filter out low-vol.
    "min_trailing_vol": 0.25,

    # FILTER: skip any ticker with RS z-score below this (underperformers)
    # 0.0 = must be at least matching benchmark. -0.5 = slight underperformance ok.
    "min_rszscore": -0.5,

    # Lookback window for trailing vol computation (trading days)
    "vol_window": 60,

    # Whether to compute/use realized vol at all
    "use_realized_vol": True,
},
}


# ══════════════════════════════════════════════════════════════════
#  EDGE-STYLING HELPER
# ══════════════════════════════════════════════════════════════════

def _edge(port_val: float, bench_val: float, higher_is_better: bool = True) -> str:
    """Return a Rich-styled string for the edge column."""
    diff = port_val - bench_val
    # For metrics where lower is better (vol, drawdown), flip sign for colour
    is_good = diff > 0 if higher_is_better else diff < 0
    colour = "green" if is_good else ("red" if (diff != 0) else "dim")
    return f"[{colour}]{diff:+.2%}[/{colour}]"


def _edge_ratio(port_val: float, bench_val: float) -> str:
    """Edge for ratio metrics (not percentages)."""
    diff = port_val - bench_val
    colour = "green" if diff > 0 else ("red" if diff < 0 else "dim")
    return f"[{colour}]{diff:+.3f}[/{colour}]"


# ══════════════════════════════════════════════════════════════════
#  METRICS DISPLAY
# ══════════════════════════════════════════════════════════════════

def _print_metrics(
    metrics: dict,
    market: str = "",
    benchmark_name: str = "",
) -> None:
    """Pretty-print backtest metrics with side-by-side comparison."""
    m = metrics  # shorthand

    actual_start = m.get("actual_start", "?")
    actual_end = m.get("actual_end", "?")
    years = m.get("years", 0)
    n_days = m.get("trading_days", 0)
    expected = m.get("expected_trading_days", 0)
    initial = m.get("initial_capital", 0)
    final = m.get("final_value", 0)

    # ── Header Panel ──────────────────────────────────────────────
    header_lines = [
        f"[bold]Period :[/]  {actual_start}  →  {actual_end}   "
        f"([cyan]{n_days}[/] trading days  /  [cyan]{years:.2f}[/] years)",
        f"[bold]Capital:[/]  \({initial:,.0f}  →  [bold green]\){final:,.0f}[/]",
        f"[bold]Bench  :[/]  {benchmark_name or '—'}",
    ]
    console.print()
    console.print(Panel(
        "\n".join(header_lines),
        title=f"[bold cyan]Backtest Results — {market}[/]",
        border_style="cyan",
        padding=(1, 2),
    ))

    # ── Data coverage warning ─────────────────────────────────────
    if expected > 0 and n_days < expected * 0.85:
        console.print(
            f"  [yellow]⚠  Data coverage:[/] {n_days} trading days found vs "
            f"~{expected} expected for {years:.1f} years.  "
            f"Check parquet data completeness.\n"
        )

    # ══════════════════════════════════════════════════════════════
    #  TABLE 1 — Performance Comparison
    # ══════════════════════════════════════════════════════════════
    t1 = Table(
        title="Performance Comparison",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=64,
    )
    t1.add_column("Metric", style="bold", min_width=20)
    t1.add_column("Portfolio", justify="right", min_width=12)
    t1.add_column(f"Benchmark ({benchmark_name})", justify="right", min_width=12)
    t1.add_column("Edge", justify="right", min_width=10)

    # helper: percentage row
    def _pct_row(label, port_key, bench_key, higher_is_better=True):
        pv = m.get(port_key, 0)
        bv = m.get(bench_key, 0)
        t1.add_row(
            label,
            f"{pv:+.2%}",
            f"{bv:+.2%}" if bv != 0 else "—",
            _edge(pv, bv, higher_is_better) if bv != 0 else "—",
        )

    def _ratio_row(label, port_key, bench_key):
        pv = m.get(port_key, 0)
        bv = m.get(bench_key, 0)
        t1.add_row(
            label,
            f"{pv:.3f}",
            f"{bv:.3f}" if bv != 0 else "—",
            _edge_ratio(pv, bv) if bv != 0 else "—",
        )

    _pct_row("Total Return",    "total_return",       "benchmark_total_return")
    _pct_row("CAGR",            "annualized_return",   "benchmark_ann_return")
    _pct_row("Volatility",      "annualized_vol",      "benchmark_ann_vol",      higher_is_better=False)
    _ratio_row("Sharpe Ratio",  "sharpe_ratio",        "benchmark_sharpe")
    _ratio_row("Sortino Ratio", "sortino_ratio",       "benchmark_sortino")
    _pct_row("Max Drawdown",    "max_drawdown",        "benchmark_max_dd",       higher_is_better=False)
    _ratio_row("Calmar Ratio",  "calmar_ratio",        "benchmark_calmar")

    # Max DD duration (portfolio only)
    dd_dur = m.get("max_dd_duration_days", 0)
    t1.add_row("Max DD Duration", f"{dd_dur:.0f} days", "—", "—")

    console.print(t1)
    console.print()

    # ══════════════════════════════════════════════════════════════
    #  TABLE 2 — Risk-Adjusted Alpha
    # ══════════════════════════════════════════════════════════════
    t2 = Table(
        title="Risk-Adjusted Alpha",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=40,
    )
    t2.add_column("Metric", style="bold", min_width=20)
    t2.add_column("Value", justify="right", min_width=12)

    alpha_val = m.get("alpha", 0)
    alpha_colour = "green" if alpha_val > 0 else "red"
    t2.add_row("Alpha (Jensen)", f"[{alpha_colour}]{alpha_val:+.2%}[/{alpha_colour}]")
    t2.add_row("Beta", f"{m.get('beta', 0):.3f}")
    t2.add_row("Tracking Error", f"{m.get('tracking_error', 0):.2%}")

    ir = m.get("information_ratio", 0)
    ir_colour = "green" if ir > 0 else "red"
    t2.add_row("Information Ratio", f"[{ir_colour}]{ir:.3f}[/{ir_colour}]")

    console.print(t2)
    console.print()

    # ══════════════════════════════════════════════════════════════
    #  TABLE 3 — Trade Statistics
    # ══════════════════════════════════════════════════════════════
    t3 = Table(
        title="Trade Statistics",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=40,
    )
    t3.add_column("Metric", style="bold", min_width=20)
    t3.add_column("Value", justify="right", min_width=14)

    total_t = m.get("total_trades", 0)
    win_t = m.get("winning_trades", 0)
    lose_t = m.get("losing_trades", 0)

    t3.add_row("Total Trades", f"{total_t:.0f}")
    t3.add_row("Win / Loss", f"[green]{win_t}[/] / [red]{lose_t}[/]")
    t3.add_row("Win Rate", f"{m.get('win_rate', 0):.1%}")
    t3.add_row("Avg Win", f"[green]{m.get('avg_win_pct', 0):+.2%}[/]")
    t3.add_row("Avg Loss", f"[red]{m.get('avg_loss_pct', 0):+.2%}[/]")
    t3.add_row("Profit Factor", f"{m.get('profit_factor', 0):.2f}")
    t3.add_row("Avg PnL", f"{m.get('avg_pnl_pct', 0):+.2%}")
    t3.add_row("Median PnL", f"{m.get('median_pnl_pct', 0):+.2%}")
    t3.add_row("Avg Holding Days", f"{m.get('avg_holding_days', 0):.1f}")
    t3.add_row(
        "Best / Worst Trade",
        f"[green]{m.get('best_trade_pct', 0):+.1%}[/]  /  "
        f"[red]{m.get('worst_trade_pct', 0):+.1%}[/]",
    )
    t3.add_row("Expectancy ($)", f"${m.get('expectancy_dollar', 0):,.0f}")

    console.print(t3)
    console.print()

    # ══════════════════════════════════════════════════════════════
    #  TABLE 4 — Portfolio Utilisation
    # ══════════════════════════════════════════════════════════════
    t4 = Table(
        title="Portfolio Utilisation",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=40,
    )
    t4.add_column("Metric", style="bold", min_width=20)
    t4.add_column("Value", justify="right", min_width=12)

    t4.add_row("Avg Positions", f"{m.get('avg_positions', 0):.1f}")
    t4.add_row("Max Positions Held", f"{m.get('max_positions_held', 0):.0f}")
    t4.add_row("Total Buy Signals", f"{m.get('total_buy_signals', 0):,.0f}")
    t4.add_row("Total Sell Signals", f"{m.get('total_sell_signals', 0):,.0f}")

    console.print(t4)
    console.print()


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
    benchmark_name = BENCHMARKS.get(args.market, "?")
    _print_metrics(metrics, market=args.market, benchmark_name=benchmark_name)

    # ── log metrics to file ───────────────────────────────────
    log.info("--- Metrics ---")
    for key, val in metrics.items():
        log.info("  %-30s  %s", key, val)

    # ── compact summary for quick scan ────────────────────────
    bench_ret = metrics.get("benchmark_total_return", 0)
    ann_ret = metrics.get("annualized_return", 0)
    bench_cagr = metrics.get("benchmark_ann_return", 0)
    alpha = metrics.get("alpha", 0)
    years = metrics.get("years", 0)

    summary_lines = [
        f"Market          : {args.market}",
        f"Period          : {args.start} -> {args.end}  ({years:.2f} years)",
        f"Start capital   : ${args.capital:,.0f}",
        f"Benchmark       : {benchmark_name}  "
        f"(total {bench_ret:+.1%} / CAGR {bench_cagr:+.1%})",
        f"Strategy        : ${results['final_value']:,.0f}  "
        f"(total {metrics['total_return']:+.1%} / CAGR {ann_ret:+.1%} / alpha {alpha:+.1%})",
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