"""
backtest/phase2/run_backtest.py
CLI entry point — single-config or comparison backtest.

Usage:
    # Single run
    python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20

    # With cache bypass
    python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --fresh

    # Comparison mode (A vs B)
    python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --compare

    # Debug mode (diagnostics enabled, verbose logging)
    python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --debug

    # Override max positions / capital
    python -m backtest.phase2.run_backtest --market IN --start 2023-01-01 --end 2026-04-20 \
        --capital 500000 --max-positions 15 --trailing-stop 0.15
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from cash.backtest.phase2.data_source import BacktestDataSource
from cash.backtest.phase2.engine import BacktestEngine
from cash.backtest.phase2.metrics import compute_metrics
from common.universe import get_universe_for_market
from common.config_refactor import (
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
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════

BENCHMARKS = {
    "HK": "2800.HK",
    "IN": "NIFTYBEES.NS",
    "US": "SPY",
}

LOGS_DIR = Path("logs") / "backtest"
RESULTS_DIR = Path("backtest_results")


# ══════════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════════

def _setup_logging(market: str, level: int = logging.INFO, debug: bool = False) -> Path:
    """Configure dual logging (file + console)."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"backtest_{market}_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # File handler — always DEBUG level
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    # Console handler — INFO or DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if debug else level)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(ch)

    # Suppress noisy modules unless debug
    if not debug:
        for noisy in (
            "refactor.strategy.rotation_v2",
            "refactor.pipeline_v2",
            "refactor.strategy",
            "refactor.scoring",
            "compute.indicators",
        ):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    return log_file


def _log_and_print(msg: str, rich_msg: str | None = None, level: int = logging.INFO):
    log.log(level, msg)
    console.print(rich_msg if rich_msg is not None else msg)


# ══════════════════════════════════════════════════════════════════
#  DATA LOADING
# ══════════════════════════════════════════════════════════════════

def _get_tickers(market: str) -> list[str]:
    return get_universe_for_market(market)


def load_data(
    market: str,
    data_dir: str = "data",
    lookback_bars: int = 300,
) -> BacktestDataSource:
    """Load parquet and build BacktestDataSource."""
    parquet_path = Path(data_dir) / f"{market}_cash.parquet"
    tickers = _get_tickers(market)
    benchmark = BENCHMARKS.get(market)

    _log_and_print(
        f"Market: {market}  Universe: {len(tickers)} tickers  "
        f"Benchmark: {benchmark or 'none'}  File: {parquet_path}",
        f"[bold]Market:[/] {market}   "
        f"[bold]Universe:[/] {len(tickers)} tickers   "
        f"[bold]Benchmark:[/] {benchmark or 'none'}   "
        f"[bold]File:[/] {parquet_path}",
    )

    if not parquet_path.exists():
        console.print(f"[bold red]ERROR:[/] Parquet file not found: {parquet_path}")
        sys.exit(1)

    ds = BacktestDataSource.from_parquet(
        parquet_path=parquet_path,
        tickers=tickers,
        benchmark_ticker=benchmark,
        lookback_bars=lookback_bars,
    )

    lo, hi = ds.get_date_range()
    loaded = ds.get_tickers()

    _log_and_print(
        f"Loaded {len(loaded)}/{len(tickers)} tickers  "
        f"({lo.strftime('%Y-%m-%d')} -> {hi.strftime('%Y-%m-%d')})",
        f"[green]Loaded {len(loaded)}/{len(tickers)} tickers  "
        f"({lo.strftime('%Y-%m-%d')} → {hi.strftime('%Y-%m-%d')})[/]",
    )

    if len(loaded) < len(tickers):
        missing = set(tickers) - set(loaded)
        preview = sorted(missing)[:15]
        suffix = "..." if len(missing) > 15 else ""
        _log_and_print(
            f"Missing {len(missing)} tickers: {preview}{suffix}",
            f"[yellow]Missing {len(missing)} tickers: {preview}{suffix}[/]",
            level=logging.WARNING,
        )

    return ds


# ══════════════════════════════════════════════════════════════════
#  CONFIG BUILDER
# ══════════════════════════════════════════════════════════════════

def build_config(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Build the strategy config dict from defaults + CLI overrides.

    Architecture notes:
    ───────────────────
    CONFIG flows through:
      BacktestEngine.run()
        └─ _run_pipeline_fast(day, tickers)
             └─ run_pipeline_v2(config={
                    vol_regime_params  → classify_volatility_regime()
                    scoring_weights    → compute_composite_v2()  ✅ reads weights
                    scoring_params     → compute_composite_v2()  ✅ reads params
                    signal_params      → apply_signals_v2()      ✅ reads thresholds
                    convergence_params → apply_convergence_v2()  ✅ reads params
                    action_params      → _generate_actions()     ⚠ partially used
                    breadth_params     → breadth regime calc
                    rotation_params    → rotation logic
                })

    Known issues (documented, not fixed here):
      - _generate_actions() has hardcoded thresholds (0.90, 0.76, etc.)
        that partially ignore action_params. The engine's signal capping
        layer compensates by re-ranking anyway.
      - convergence_params: rotationrec is never stamped upstream, so
        convergence is effectively score passthrough + minor adjustments.
      - leadership boost is hardcoded at +10% in pipeline_v2.py.
    """
    # ── Signal params with CLI overrides ─────────────────────────
    signal_params = dict(SIGNALPARAMS_V2)
    if args.trailing_stop is not None:
        signal_params["trailing_stop_pct"] = args.trailing_stop
    if args.max_hold is not None:
        signal_params["max_hold_days"] = args.max_hold
    if args.min_hold is not None:
        signal_params["min_hold_days"] = args.min_hold

    # ── Buy ranking params ───────────────────────────────────────
    buy_ranking = {
        "momentum_tilt": 0.50,
        "vol_preference": 0.30,
        "min_trailing_vol": 0.25,
        "min_rszscore": -0.50,
        "vol_window": 60,
        "use_realized_vol": True,
    }

    # ── Signal cap params ────────────────────────────────────────
    signal_cap = {
        "strong_buy_limit": 15,
        "max_buy_signals": 25,
    }
    if args.no_cap:
        signal_cap["strong_buy_limit"] = 999
        signal_cap["max_buy_signals"] = 999

    # ── Assemble ─────────────────────────────────────────────────
    config = {
        "vol_regime_params": VOLREGIMEPARAMS,
        "scoring_weights": SCORINGWEIGHTS_V2,
        "scoring_params": SCORINGPARAMS_V2,
        "signal_params": signal_params,
        "convergence_params": CONVERGENCEPARAMS_V2,
        "action_params": ACTIONPARAMS_V2,
        "breadth_params": BREADTHPARAMS,
        "rotation_params": ROTATIONPARAMS,
        "buy_ranking_params": buy_ranking,
        "signal_cap_params": signal_cap,
        # Cache control
        "invalidate_cache": args.fresh,
    }

    return config


def build_config_b(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Build an alternate config for --compare mode.
    Override whatever you're testing here.
    """
    config = build_config(args)

    # ── Example: tighter stops, fewer positions, lower momentum tilt ──
    config["signal_params"] = dict(config["signal_params"])
    config["signal_params"]["trailing_stop_pct"] = 0.15
    config["signal_params"]["max_hold_days"] = 90

    config["buy_ranking_params"] = dict(config["buy_ranking_params"])
    config["buy_ranking_params"]["momentum_tilt"] = 0.30
    config["buy_ranking_params"]["min_trailing_vol"] = 0.20

    config["signal_cap_params"] = {
        "strong_buy_limit": 10,
        "max_buy_signals": 20,
    }

    return config


# ══════════════════════════════════════════════════════════════════
#  EDGE-STYLING HELPERS
# ══════════════════════════════════════════════════════════════════

def _edge(port_val: float, bench_val: float, higher_is_better: bool = True) -> str:
    diff = port_val - bench_val
    is_good = diff > 0 if higher_is_better else diff < 0
    colour = "green" if is_good else ("red" if (diff != 0) else "dim")
    return f"[{colour}]{diff:+.2%}[/{colour}]"


def _edge_ratio(port_val: float, bench_val: float) -> str:
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
    """Pretty-print backtest metrics with side-by-side benchmark comparison."""
    m = metrics

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
        f"[bold]Capital:[/]  ${initial:,.0f}  →  [bold green]${final:,.0f}[/]",
        f"[bold]Bench  :[/]  {benchmark_name or '—'}",
    ]
    console.print()
    console.print(Panel(
        "\n".join(header_lines),
        title=f"[bold cyan]Backtest Results — {market}[/]",
        border_style="cyan",
        padding=(1, 2),
    ))

    if expected > 0 and n_days < expected * 0.85:
        console.print(
            f"  [yellow]⚠  Data coverage:[/] {n_days} trading days found vs "
            f"~{expected} expected for {years:.1f} years.  "
            f"Check parquet data completeness.\n"
        )

    # ══════════════════════════════════════════════════════════════
    #  TABLE 1 — Performance Comparison vs Benchmark
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

    _pct_row("Total Return",    "total_return",        "benchmark_total_return")
    _pct_row("CAGR",            "annualized_return",   "benchmark_ann_return")
    _pct_row("Volatility",      "annualized_vol",      "benchmark_ann_vol", higher_is_better=False)
    _ratio_row("Sharpe Ratio",  "sharpe_ratio",        "benchmark_sharpe")
    _ratio_row("Sortino Ratio", "sortino_ratio",       "benchmark_sortino")
    _pct_row("Max Drawdown",    "max_drawdown",        "benchmark_max_dd", higher_is_better=False)
    _ratio_row("Calmar Ratio",  "calmar_ratio",        "benchmark_calmar")

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
    #  TABLE 4 — Portfolio Utilisation & Signal Quality
    # ══════════════════════════════════════════════════════════════
    t4 = Table(
        title="Portfolio Utilisation & Signal Quality",
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
    t4.add_row("Avg Signals/Day (capped)", f"{m.get('avg_daily_signals', 0):.1f}")
    t4.add_row("Avg Cash %", f"{m.get('avg_cash_pct', 0):.1f}%")

    console.print(t4)
    console.print()


# ══════════════════════════════════════════════════════════════════
#  COMPARISON DISPLAY
# ══════════════════════════════════════════════════════════════════

def _print_comparison(
    metrics_a: dict,
    metrics_b: dict,
    name_a: str,
    name_b: str,
) -> None:
    """Side-by-side comparison table for two configs."""
    ROWS = [
        ("Total Return",       "total_return",         ".1%",  False),
        ("CAGR",               "annualized_return",    ".1%",  False),
        ("Sharpe",             "sharpe_ratio",         ".3f",  False),
        ("Sortino",            "sortino_ratio",        ".3f",  False),
        ("Max Drawdown",       "max_drawdown",         ".1%",  True),
        ("Calmar",             "calmar_ratio",         ".2f",  False),
        ("Volatility",         "annualized_vol",       ".1%",  True),
        None,  # separator
        ("Win Rate",           "win_rate",             ".1%",  False),
        ("Profit Factor",      "profit_factor",        ".2f",  False),
        ("Avg PnL",            "avg_pnl_pct",          ".2%",  False),
        ("Total Trades",       "total_trades",         ".0f",  None),
        ("Avg Holding Days",   "avg_holding_days",     ".1f",  None),
        None,
        ("Alpha",              "alpha",                ".2%",  False),
        ("Beta",               "beta",                 ".2f",  None),
        ("Info Ratio",         "information_ratio",    ".3f",  False),
        None,
        ("Avg Positions",      "avg_positions",        ".1f",  None),
        ("Avg Cash %",         "avg_cash_pct",         ".1f",  True),
        ("Avg Signals/Day",    "avg_daily_signals",    ".1f",  None),
    ]

    table = Table(
        title="⚔️  Config Comparison",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=70,
    )
    table.add_column("Metric", style="bold", min_width=18)
    table.add_column(name_a, justify="right", min_width=12)
    table.add_column(name_b, justify="right", min_width=12)
    table.add_column("Winner", justify="right", min_width=10)

    for row in ROWS:
        if row is None:
            table.add_row("─" * 18, "─" * 10, "─" * 10, "─" * 8)
            continue

        label, key, fmt, lower_is_better = row
        va = metrics_a.get(key, 0)
        vb = metrics_b.get(key, 0)

        try:
            sa = f"{va:{fmt}}"
            sb = f"{vb:{fmt}}"
        except (ValueError, TypeError):
            sa, sb = str(va), str(vb)

        if lower_is_better is None:
            winner = ""
        elif lower_is_better:
            winner = f"[cyan]◄ {name_a}[/]" if va < vb else (f"[green]{name_b} ►[/]" if vb < va else "Tie")
        else:
            winner = f"[cyan]◄ {name_a}[/]" if va > vb else (f"[green]{name_b} ►[/]" if vb > va else "Tie")

        table.add_row(label, sa, sb, winner)

    console.print()
    console.print(table)
    console.print()


# ══════════════════════════════════════════════════════════════════
#  CONFIG SUMMARY (what's actually being used)
# ══════════════════════════════════════════════════════════════════

def _print_config_summary(config: Dict[str, Any], name: str = "Strategy") -> None:
    """Print key config params so you know what you're running."""
    sig = config.get("signal_params", {})
    cap = config.get("signal_cap_params", {})
    rank = config.get("buy_ranking_params", {})

    lines = [
        f"[bold]{name}[/] config summary:",
        f"  trailing_stop={sig.get('trailing_stop_pct', '?')}  "
        f"max_hold={sig.get('max_hold_days', '?')}d  "
        f"min_hold={sig.get('min_hold_days', '?')}d",
        f"  signal_cap: strong_buy_limit={cap.get('strong_buy_limit', '?')}  "
        f"max_buy_signals={cap.get('max_buy_signals', '?')}",
        f"  buy_ranking: momentum_tilt={rank.get('momentum_tilt', '?')}  "
        f"vol_pref={rank.get('vol_preference', '?')}  "
        f"min_vol={rank.get('min_trailing_vol', '?')}  "
        f"min_rs={rank.get('min_rszscore', '?')}",
        f"  cache: {'[yellow]FRESH (recomputing)[/]' if config.get('invalidate_cache') else '[green]enabled[/]'}",
    ]
    for line in lines:
        console.print(line)
    console.print()


# ══════════════════════════════════════════════════════════════════
#  SAVE RESULTS
# ══════════════════════════════════════════════════════════════════

def _save_results(
    results: Dict[str, Any],
    metrics: Dict[str, Any],
    market: str,
    suffix: str = "",
) -> Path:
    """Save equity curve, trades, daily log, and metrics CSV."""
    out = RESULTS_DIR / market
    if suffix:
        out = out / suffix
    out.mkdir(parents=True, exist_ok=True)

    # Metrics
    metrics_rows = [{"Metric": k, "Value": v} for k, v in metrics.items()]
    pd.DataFrame(metrics_rows).to_csv(out / "metrics.csv", index=False)

    # Equity curve
    results["equity_curve"].to_csv(out / "equity_curve.csv", index=False)

    # Trades
    if not results["trade_log"].empty:
        results["trade_log"].to_csv(out / "trades.csv", index=False)

    # Daily log
    results["daily_log"].to_csv(out / "daily_log.csv", index=False)

    return out


# ══════════════════════════════════════════════════════════════════
#  SINGLE BACKTEST
# ══════════════════════════════════════════════════════════════════

def run_single(args: argparse.Namespace) -> None:
    """Run a single-config backtest."""
    config = build_config(args)
    benchmark_name = BENCHMARKS.get(args.market, "?")

    _print_config_summary(config, "Strategy")

    # Load data
    console.rule(f"[bold cyan]Loading data: {args.market}")
    ds = load_data(args.market, args.data_dir, args.lookback)

    # Run
    console.rule("[bold cyan]Running backtest")
    t0 = time.time()

    engine = BacktestEngine(
        data_source=ds,
        market=args.market,
        config=config,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        max_positions=args.max_positions,
        config_name="Strategy",
    )
    results = engine.run()
    elapsed = time.time() - t0

    # Metrics
    metrics = compute_metrics(results)
    metrics["avg_daily_signals"] = results.get("avg_daily_signals", 0)
    metrics["avg_cash_pct"] = results.get("avg_cash_pct", 0)
    results["metrics"] = metrics

    # Display
    console.rule("[bold cyan]Results")
    _print_metrics(metrics, market=args.market, benchmark_name=benchmark_name)

    # Summary
    _print_summary(results, metrics, args, benchmark_name, elapsed)

    # Save
    out = _save_results(results, metrics, args.market)
    _log_and_print(
        f"Results saved to {out}/",
        f"[green]Results saved to {out}/[/]",
    )


# ══════════════════════════════════════════════════════════════════
#  COMPARISON BACKTEST
# ══════════════════════════════════════════════════════════════════

def run_compare(args: argparse.Namespace) -> None:
    """Run two configs side-by-side."""
    config_a = build_config(args)
    config_b = build_config_b(args)
    benchmark_name = BENCHMARKS.get(args.market, "?")

    name_a = "Baseline"
    name_b = "Variant"

    console.rule(f"[bold cyan]Comparison: {name_a} vs {name_b}")
    _print_config_summary(config_a, name_a)
    _print_config_summary(config_b, name_b)

    # Load data (shared)
    console.rule(f"[bold cyan]Loading data: {args.market}")
    ds = load_data(args.market, args.data_dir, args.lookback)

    # Run A
    console.rule(f"[bold cyan]Running {name_a}")
    t0 = time.time()
    engine_a = BacktestEngine(
        data_source=ds,
        market=args.market,
        config=config_a,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        max_positions=args.max_positions,
        config_name=name_a,
    )
    results_a = engine_a.run()
    elapsed_a = time.time() - t0

    # Run B
    console.rule(f"[bold cyan]Running {name_b}")
    t1 = time.time()
    engine_b = BacktestEngine(
        data_source=ds,
        market=args.market,
        config=config_b,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        max_positions=args.max_positions,
        config_name=name_b,
    )
    results_b = engine_b.run()
    elapsed_b = time.time() - t1

    # Metrics
    metrics_a = compute_metrics(results_a)
    metrics_a["avg_daily_signals"] = results_a.get("avg_daily_signals", 0)
    metrics_a["avg_cash_pct"] = results_a.get("avg_cash_pct", 0)

    metrics_b = compute_metrics(results_b)
    metrics_b["avg_daily_signals"] = results_b.get("avg_daily_signals", 0)
    metrics_b["avg_cash_pct"] = results_b.get("avg_cash_pct", 0)

    # Display
    console.rule("[bold cyan]Comparison Results")
    _print_comparison(metrics_a, metrics_b, name_a, name_b)

    # Timing
    console.print(
        f"  [dim]Elapsed: {name_a}={elapsed_a:.1f}s  "
        f"{name_b}={elapsed_b:.1f}s[/]"
    )

    # Save both
    out_a = _save_results(results_a, metrics_a, args.market, suffix=name_a.lower())
    out_b = _save_results(results_b, metrics_b, args.market, suffix=name_b.lower())
    console.print(f"[green]Saved: {out_a}/ and {out_b}/[/]")


# ══════════════════════════════════════════════════════════════════
#  SUMMARY HELPER
# ══════════════════════════════════════════════════════════════════

def _print_summary(
    results: Dict,
    metrics: Dict,
    args: argparse.Namespace,
    benchmark_name: str,
    elapsed: float,
) -> None:
    """Print compact end-of-run summary."""
    bench_ret = metrics.get("benchmark_total_return", 0)
    ann_ret = metrics.get("annualized_return", 0)
    bench_cagr = metrics.get("benchmark_ann_return", 0)
    alpha = metrics.get("alpha", 0)
    years = metrics.get("years", 0)
    sharpe = metrics.get("sharpe_ratio", 0)

    alpha_colour = "green" if alpha > 0 else "red"
    sharpe_colour = "green" if sharpe > 1.0 else ("yellow" if sharpe > 0.5 else "red")

    summary_lines = [
        f"[bold]Market[/]          : {args.market}",
        f"[bold]Period[/]          : {args.start} → {args.end}  ({years:.2f} years)",
        f"[bold]Capital[/]         : ${args.capital:,.0f} → ${results['final_value']:,.0f}",
        f"[bold]Benchmark[/]       : {benchmark_name}  "
        f"(total {bench_ret:+.1%} / CAGR {bench_cagr:+.1%})",
        f"[bold]Strategy[/]        : "
        f"total {metrics['total_return']:+.1%} / CAGR {ann_ret:+.1%} / "
        f"[{alpha_colour}]alpha {alpha:+.1%}[/{alpha_colour}] / "
        f"[{sharpe_colour}]sharpe {sharpe:.2f}[/{sharpe_colour}]",
        f"[bold]Signals[/]         : avg {metrics.get('avg_daily_signals', 0):.1f}/day  "
        f"(cap={args.max_positions})",
        f"[bold]Elapsed[/]         : {elapsed:.1f}s",
    ]

    console.print()
    console.print(Panel(
        "\n".join(summary_lines),
        title="[bold]Summary[/]",
        border_style="dim",
        padding=(0, 2),
    ))


# ══════════════════════════════════════════════════════════════════
#  CLI ARGUMENT PARSER
# ══════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest strategy against a market universe",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --market HK --start 2022-01-01 --end 2026-04-20
  %(prog)s --market HK --start 2022-01-01 --end 2026-04-20 --fresh
  %(prog)s --market HK --start 2022-01-01 --end 2026-04-20 --compare
  %(prog)s --market IN --start 2023-01-01 --end 2026-04-20 --trailing-stop 0.15 --max-hold 90
        """,
    )

    # ── Required ─────────────────────────────────────────────────
    parser.add_argument("--market", required=True, help="Market code (HK, IN, US)")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")

    # ── Portfolio ────────────────────────────────────────────────
    parser.add_argument("--capital", type=float, default=1_000_000,
                        help="Initial capital (default: 1,000,000)")
    parser.add_argument("--max-positions", type=int, default=25,
                        help="Max concurrent positions (default: 25)")

    # ── Strategy overrides ───────────────────────────────────────
    parser.add_argument("--trailing-stop", type=float, default=None,
                        help="Trailing stop %% as decimal (e.g. 0.18)")
    parser.add_argument("--max-hold", type=int, default=None,
                        help="Max holding period in days")
    parser.add_argument("--min-hold", type=int, default=None,
                        help="Min holding period in days")

    # ── Data & cache ─────────────────────────────────────────────
    parser.add_argument("--data-dir", default="data",
                        help="Directory containing parquet files")
    parser.add_argument("--lookback", type=int, default=300,
                        help="Lookback bars for indicator warmup (default: 300)")
    parser.add_argument("--fresh", action="store_true",
                        help="Bypass indicator cache, recompute from parquet")

    # ── Mode ─────────────────────────────────────────────────────
    parser.add_argument("--compare", action="store_true",
                        help="Run A/B comparison (edit build_config_b for variant)")
    parser.add_argument("--no-cap", action="store_true",
                        help="Disable signal capping (for debugging)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable verbose debug logging to console")

    return parser.parse_args()


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    args = parse_args()

    log_file = _setup_logging(args.market, debug=args.debug)

    log.info("=" * 70)
    log.info(
        "Backtest started  market=%s  start=%s  end=%s  mode=%s",
        args.market, args.start, args.end,
        "compare" if args.compare else "single",
    )
    log.info(
        "capital=%.0f  max_positions=%d  lookback=%d  fresh=%s  no_cap=%s",
        args.capital, args.max_positions, args.lookback, args.fresh, args.no_cap,
    )
    log.info("Log file: %s", log_file)
    log.info("=" * 70)

    try:
        if args.compare:
            run_compare(args)
        else:
            run_single(args)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/]")
        sys.exit(1)
    except Exception as exc:
        log.exception("Backtest failed")
        console.print(f"\n[bold red]ERROR:[/] {exc}")
        console.print(f"[dim]See log: {log_file}[/]")
        sys.exit(1)

    _log_and_print(
        f"Log file: {log_file}",
        f"[dim]Log file: {log_file}[/]",
    )
    log.info("Backtest finished successfully.")


if __name__ == "__main__":
    main()