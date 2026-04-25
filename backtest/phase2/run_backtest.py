"""
backtest/phase2/run_backtest.py
CLI entry point.

    python -m backtest.run_backtest --market HK --start 2025-10-01 --end 2026-04-20
    python -m backtest.run_backtest --market IN --start 2025-06-01 --end 2026-04-20
    python -m backtest.run_backtest --market US --start 2025-06-01 --end 2026-04-20
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.console import Console

from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.compare import build_config_dict, run_comparison, print_comparison

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
    # These still write DEBUG to the log file via the file handler.
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

from common.universe import get_universe_for_market

BENCHMARKS = {
    "HK": "2800.HK",      # Tracker Fund of Hong Kong
    "IN": "NIFTYBEES.NS",  # Nifty ETF (adjust to your universe)
    "US": "SPY",           # S&P 500 ETF
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
#  TWO CONFIGS TO COMPARE
# ══════════════════════════════════════════════════════════════════

_VOL_COMMON = {
    "atrp_window": 14, "realized_vol_window": 20,
    "dispersion_window": 20, "gap_window": 20,
    "calm_atrp_max": 0.035, "volatile_atrp_max": 0.060,
    "calm_rvol_max": 0.28, "volatile_rvol_max": 0.42,
    "volatile_gap_rate": 0.18, "chaotic_gap_rate": 0.28,
    "calm_dispersion_max": 0.022, "volatile_dispersion_max": 0.040,
    "score_weights": {"atrp": 0.35, "realized_vol": 0.35,
                      "gap_rate": 0.15, "dispersion": 0.15},
}

CONFIG_LOOSE = build_config_dict(
    vol_regime_params=_VOL_COMMON,
    scoring_weights={"trend": 0.38, "participation": 0.22, "risk": 0.25, "regime": 0.15},
    scoring_params={
        "trend": {"w_stock_rs": 0.45, "w_sector_rs": 0.25, "w_rs_accel": 0.15, "w_trend_confirm": 0.15},
        "participation": {"w_rvol": 0.35, "w_obv": 0.30, "w_adline": 0.20, "w_dollar_volume": 0.15},
        "risk": {"w_vol_penalty": 0.35, "w_liquidity_penalty": 0.25, "w_gap_penalty": 0.20, "w_extension_penalty": 0.20},
        "regime": {"w_breadth": 0.60, "w_vol_regime": 0.40},
        "penalties": {
            "rsi_soft_low": 38.0, "rsi_soft_high": 78.0, "adx_soft_min": 16.0,
            "atrp_high": 0.07, "extension_warn": 0.12, "extension_bad": 0.22,
            "illiquidity_bad": 0.015,
        },
    },
    signal_params={
        "base_entry_threshold": 0.58, "base_exit_threshold": 0.42,
        "allowed_rs_regimes": ("leading", "improving"),
        "blocked_sector_regimes": ("lagging",),
        "hard_block_breadth_regimes": ("critical",),
        "hard_block_vol_regimes": ("chaotic",),
        "continuation_min_trend": 0.62, "pullback_min_trend": 0.68,
        "pullback_max_short_extension": 0.04, "pullback_rsi_max": 58.0,
        "cooldown_days": 4,
        "regime_entry_adjustment": {"calm": 0.00, "volatile": 0.03, "chaotic": 0.10},
        "breadth_entry_adjustment": {"strong": -0.02, "neutral": 0.00, "weak": 0.03, "critical": 0.08, "unknown": 0.00},
        "size_multipliers": {"calm": 1.00, "volatile": 0.70, "chaotic": 0.35},
    },
    convergence_params={
        "tiers": {"aligned_long": 4, "rotation_long_only": 3, "score_long_only": 2, "mixed": 1, "avoid": 0},
        "adjustments": {"calm": 0.04, "volatile": 0.02, "chaotic": 0.00},
    },
)

CONFIG_TIGHT = build_config_dict(
    vol_regime_params=_VOL_COMMON,
    scoring_weights={"trend": 0.36, "participation": 0.18, "risk": 0.26, "regime": 0.20},
    scoring_params={
        "trend": {"w_stock_rs": 0.42, "w_sector_rs": 0.28, "w_rs_accel": 0.15, "w_trend_confirm": 0.15},
        "participation": {"w_rvol": 0.35, "w_obv": 0.25, "w_adline": 0.20, "w_dollar_volume": 0.20},
        "risk": {"w_vol_penalty": 0.32, "w_liquidity_penalty": 0.23, "w_gap_penalty": 0.20, "w_extension_penalty": 0.25},
        "regime": {"w_breadth": 0.65, "w_vol_regime": 0.35},
        "penalties": {
            "rsi_soft_low": 40.0, "rsi_soft_high": 76.0, "adx_soft_min": 18.0,
            "atrp_high": 0.065, "extension_warn": 0.10, "extension_bad": 0.18,
            "illiquidity_bad": 0.012,
        },
    },
    signal_params={
        "base_entry_threshold": 0.60, "base_exit_threshold": 0.44,
        "allowed_rs_regimes": ("leading", "improving"),
        "blocked_sector_regimes": ("lagging",),
        "hard_block_breadth_regimes": ("critical",),
        "hard_block_vol_regimes": ("chaotic",),
        "continuation_min_trend": 0.64, "pullback_min_trend": 0.70,
        "pullback_max_short_extension": 0.06, "pullback_rsi_max": 62.0,
        "cooldown_days": 4,
        "regime_entry_adjustment": {"calm": 0.00, "volatile": 0.04, "chaotic": 0.12},
        "breadth_entry_adjustment": {"strong": -0.01, "neutral": 0.02, "weak": 0.08, "critical": 0.14, "unknown": 0.03},
        "size_multipliers": {"calm": 1.00, "volatile": 0.65, "chaotic": 0.30},
    },
    convergence_params={
        "tiers": {"aligned_long": 4, "rotation_long_only": 3, "score_long_only": 2, "mixed": 1, "avoid": 0},
        "adjustments": {"calm": 0.04, "volatile": 0.01, "chaotic": 0.00},
    },
)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Backtest config comparison against a market universe"
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
    parser.add_argument(
        "--name-a", default="Loose",
        help="Display name for Config A",
    )
    parser.add_argument(
        "--name-b", default="Tight",
        help="Display name for Config B",
    )
    args = parser.parse_args()

    # ── set up dual logging (file + console) ──────────────────
    log_file = _setup_logging(args.market)

    # ── log the full command / args ───────────────────────────
    log.info("=" * 70)
    log.info("Backtest started  market=%s  start=%s  end=%s", args.market, args.start, args.end)
    log.info("capital=%.0f  max_positions=%d  lookback=%d", args.capital, args.max_positions, args.lookback)
    log.info("name_a=%s  name_b=%s  data_dir=%s", args.name_a, args.name_b, args.data_dir)
    log.info("Log file: %s", log_file)
    log.info("=" * 70)

    # ── load data ─────────────────────────────────────────────
    console.rule(f"[bold cyan]Backtest: {args.market} market")
    log.info("--- Loading data for %s market ---", args.market)
    ds = load_data(args.market, args.data_dir, args.lookback)

    # ── run both configs ──────────────────────────────────────
    console.rule("[bold cyan]Running comparison")
    log.info("--- Running comparison: %s vs %s ---", args.name_a, args.name_b)
    ra, rb, comp = run_comparison(
        data_source=ds,
        market=args.market,
        config_a=CONFIG_LOOSE,
        config_b=CONFIG_TIGHT,
        start_date=args.start,
        end_date=args.end,
        name_a=args.name_a,
        name_b=args.name_b,
        initial_capital=args.capital,
        max_positions=args.max_positions,
    )

    # ── display ───────────────────────────────────────────────
    console.print()
    print_comparison(comp, args.name_a, args.name_b, console)

    # Log comparison table to file as well
    log.info("--- Comparison Table ---")
    for _, row in comp.iterrows():
        log.info("  %-30s  %s=%s  %s=%s",
                 row.get("Metric", ""),
                 args.name_a, row.get(args.name_a, ""),
                 args.name_b, row.get(args.name_b, ""))

    # ── summary ───────────────────────────────────────────────
    bench_ret_a = ra.get("metrics", {}).get("benchmark_total_return", 0)
    summary_lines = [
        f"Market          : {args.market}",
        f"Period          : {args.start} -> {args.end}",
        f"Start capital   : ${args.capital:,.0f}",
        f"Benchmark       : {BENCHMARKS.get(args.market, '?')}  ({bench_ret_a:+.1%})",
        f"{args.name_a:15s} : ${ra['final_value']:,.0f}  (alpha {ra.get('metrics', {}).get('alpha', 0):+.1%})",
        f"{args.name_b:15s} : ${rb['final_value']:,.0f}  (alpha {rb.get('metrics', {}).get('alpha', 0):+.1%})",
    ]
    for line in summary_lines:
        _log_and_print(f"  {line}", f"  {line}")

    # ── save ──────────────────────────────────────────────────
    out = Path("backtest_results") / args.market
    out.mkdir(parents=True, exist_ok=True)

    comp.to_csv(out / "comparison.csv", index=False)

    # Merge equity curves — include benchmark once
    eq_a = ra["equity_curve"][["date", "value"]].rename(columns={"value": f"value_{args.name_a}"})
    eq_b = rb["equity_curve"][["date", "value"]].rename(columns={"value": f"value_{args.name_b}"})
    eq_bench = ra["equity_curve"][["date", "benchmark"]].rename(columns={"benchmark": "benchmark"})

    eq = eq_a.merge(eq_b, on="date").merge(eq_bench, on="date")
    eq.to_csv(out / "equity_curves.csv", index=False)

    if not ra["trade_log"].empty:
        ra["trade_log"].to_csv(out / f"trades_{args.name_a}.csv", index=False)
    if not rb["trade_log"].empty:
        rb["trade_log"].to_csv(out / f"trades_{args.name_b}.csv", index=False)

    # ── daily log for debugging ───────────────────────────────
    ra["daily_log"].to_csv(out / f"daily_{args.name_a}.csv", index=False)
    rb["daily_log"].to_csv(out / f"daily_{args.name_b}.csv", index=False)

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