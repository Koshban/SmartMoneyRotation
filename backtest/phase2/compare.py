"""
backtest/phase2/compare.py
Run two configs side-by-side and print a comparison table,
including benchmark performance.
"""
from __future__ import annotations

import pandas as pd
from typing import Any, Dict, Tuple

from rich.console import Console
from rich.table import Table

from backtest.phase2.engine import BacktestEngine
from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.metrics import compute_metrics


# ------------------------------------------------------------------
# ------------------------------------------------------------------
def build_config_dict(
    vol_regime_params: dict,
    scoring_weights: dict,
    scoring_params: dict,
    signal_params: dict,
    convergence_params: dict,
    action_params: dict | None = None,                     # ← ADD
) -> Dict[str, Any]:
    """Bundle config blocks into one dict for the engine."""
    cfg = {
        "VOLREGIMEPARAMS": vol_regime_params,
        "SCORINGWEIGHTS_V2": scoring_weights,
        "SCORINGPARAMS_V2": scoring_params,
        "SIGNALPARAMS_V2": signal_params,
        "CONVERGENCEPARAMS_V2": convergence_params,
    }
    if action_params is not None:
        cfg["ACTIONPARAMS_V2"] = action_params             # ← ADD
    return cfg


# ------------------------------------------------------------------
def run_comparison(
    data_source: BacktestDataSource,
    market: str,
    config_a: Dict[str, Any],
    config_b: Dict[str, Any],
    start_date: str,
    end_date: str,
    name_a: str = "Config A",
    name_b: str = "Config B",
    initial_capital: float = 1_000_000.0,
    max_positions: int = 12,
    commission_rate: float = 0.0010,
    slippage_rate: float = 0.0010,
) -> Tuple[Dict, Dict, pd.DataFrame]:
    """
    Returns (results_a, results_b, comparison_dataframe).
    """
    common = dict(
        data_source=data_source,
        market=market,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        max_positions=max_positions,
        commission_rate=commission_rate,
        slippage_rate=slippage_rate,
    )

    ra = BacktestEngine(config=config_a, config_name=name_a, **common).run()
    rb = BacktestEngine(config=config_b, config_name=name_b, **common).run()

    ma = compute_metrics(ra)
    mb = compute_metrics(rb)
    ra["metrics"], rb["metrics"] = ma, mb

    comp = _comparison_df(ma, mb, name_a, name_b)
    return ra, rb, comp


# ------------------------------------------------------------------
_ROWS = [
    # (label,                  key,                    fmt,   lower_is_better)
    ("Total Return",           "total_return",         ".1%",  False),
    ("Annualized Return",      "annualized_return",    ".1%",  False),
    ("Final Value",            "final_value",          ",.0f", False),
    ("SEP", None, None, None),
    ("Annualized Vol",         "annualized_vol",       ".1%",  True),
    ("Sharpe Ratio",           "sharpe_ratio",         ".2f",  False),
    ("Sortino Ratio",          "sortino_ratio",        ".2f",  False),
    ("Max Drawdown",           "max_drawdown",         ".1%",  True),
    ("Max DD Duration (days)", "max_dd_duration_days", "d",    True),
    ("Calmar Ratio",           "calmar_ratio",         ".2f",  False),
    ("SEP", None, None, None),
    ("Total Trades",           "total_trades",         "d",    None),
    ("Win Rate",               "win_rate",             ".1%",  False),
    ("Avg Win",                "avg_win_pct",          ".1%",  False),
    ("Avg Loss",               "avg_loss_pct",         ".1%",  True),
    ("Profit Factor",          "profit_factor",        ".2f",  False),
    ("Avg PnL %",              "avg_pnl_pct",          ".2%",  False),
    ("Expectancy ($)",         "expectancy_dollar",    ",.0f", False),
    ("Avg Holding Days",       "avg_holding_days",     ".1f",  None),
    ("SEP", None, None, None),
    ("Best Trade",             "best_trade_pct",       ".1%",  False),
    ("Worst Trade",            "worst_trade_pct",      ".1%",  True),
    ("Avg Positions",          "avg_positions",        ".1f",  None),
    ("Buy Signals",            "total_buy_signals",    "d",    None),
    ("Sell Signals",           "total_sell_signals",   "d",    None),
    ("SEP", None, None, None),
    # ── Benchmark & relative metrics ──────────────────────────────
    ("Benchmark Return",       "benchmark_total_return", ".1%", None),
    ("Benchmark Ann. Return",  "benchmark_ann_return", ".1%",  None),
    ("Benchmark Sharpe",       "benchmark_sharpe",     ".2f",  None),
    ("Benchmark Max DD",       "benchmark_max_dd",     ".1%",  None),
    ("SEP", None, None, None),
    ("Alpha (Jensen)",         "alpha",                ".2%",  False),
    ("Beta",                   "beta",                 ".2f",  None),
    ("Tracking Error",         "tracking_error",       ".1%",  True),
    ("Information Ratio",      "information_ratio",    ".2f",  False),
]


def _comparison_df(
    ma: Dict, mb: Dict, name_a: str, name_b: str
) -> pd.DataFrame:
    rows = []
    for label, key, fmt, lower_better in _ROWS:
        if key is None:
            rows.append({"Metric": "─" * 28, name_a: "", name_b: "", "Better": ""})
            continue

        va, vb = ma.get(key, 0), mb.get(key, 0)

        # who wins?
        if lower_better is None:
            better = ""
        elif lower_better:
            better = name_a if va > vb else name_b if vb > va else "Tie"
        else:
            better = name_a if va > vb else name_b if vb > va else "Tie"

        try:
            sa = f"{va:{fmt}}"
            sb = f"{vb:{fmt}}"
        except (ValueError, TypeError):
            sa, sb = str(va), str(vb)

        rows.append({"Metric": label, name_a: sa, name_b: sb, "Better": better})

    return pd.DataFrame(rows)


# ------------------------------------------------------------------
def print_comparison(
    comp: pd.DataFrame,
    name_a: str = "Config A",
    name_b: str = "Config B",
    console: Console | None = None,
) -> None:
    """Pretty-print with Rich."""
    if console is None:
        console = Console()

    table = Table(title="⚔️  Backtest Comparison", show_lines=False, padding=(0, 1))
    for col in comp.columns:
        table.add_column(col, justify="left" if col == "Metric" else "right")

    for _, row in comp.iterrows():
        cells = []
        for col in comp.columns:
            v = str(row[col])
            if col == "Better":
                if v == name_a:
                    v = f"[cyan]◄ {v}[/]"
                elif v == name_b:
                    v = f"[green]{v} ►[/]"
            cells.append(v)
        table.add_row(*cells)

    console.print(table)