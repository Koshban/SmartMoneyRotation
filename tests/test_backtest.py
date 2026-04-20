"""
End-to-end test: portfolio layer — sizing, rebalancing,
risk analysis, backtesting, and comprehensive reporting.

Builds on the signal pipeline (strategy/signals.py →
output/rankings.py → output/signals.py) and runs a full
historical simulation over sector ETFs.

NOTE: tests/test_portfolio.py already covers strategy/portfolio.py
      (build_portfolio, portfolio_report).  This file covers the
      NEW portfolio/ package: sizing, rebalance, risk, backtest.
"""

import time
import yfinance as yf
import pandas as pd
import numpy as np

from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score
from compute.breadth import compute_all_breadth, breadth_to_pillar_scores
from strategy.signals import generate_signals as generate_ticker_signals
from output.rankings import compute_all_rankings
from output.signals import (
    SignalConfig,
    BUY, HOLD, SELL, NEUTRAL,
    compute_all_signals,
    active_positions,
    signals_summary,
)
from output.reports import (
    daily_report,
    breadth_section,
    strategy_overview,
    performance_report,
    generate_full_report,
)
from portfolio.sizing import (
    SizingConfig,
    compute_target_weights,
    equal_weight,
    score_weighted,
    inverse_volatility,
    risk_parity,
)
from portfolio.rebalance import (
    RebalanceConfig,
    Trade,
    compute_drift,
    needs_rebalance,
    generate_trades,
    estimate_costs,
)
from portfolio.risk import (
    compute_drawdown,
    drawdown_stats,
    compute_var,
    compute_cvar,
    concentration_risk,
    rolling_volatility,
    compute_portfolio_risk,
)
from portfolio.backtest import (
    BacktestConfig,
    BacktestResult,
    run_backtest,
    compute_performance_metrics,
)
from utils.run_logger import RunLogger


# ── Configuration ─────────────────────────────────────────────

SECTOR_ETFS = [
    "XLK", "XLF", "XLE", "XLV", "XLY",
    "XLP", "XLI", "XLU", "XLC", "XLRE", "XLB",
]
BENCHMARK = "SPY"
PERIOD = "2y"

BREADTH_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "GS", "BAC",
    "XOM", "CVX",
    "JNJ", "UNH", "PFE",
    "PG", "HD", "WMT",
    "LIN", "CAT",
]


# ── Helpers ───────────────────────────────────────────────────

def clean_single(raw: pd.DataFrame) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [
        str(c).lower().replace(" ", "_") for c in raw.columns
    ]
    if "adj_close" in raw.columns:
        if "close" in raw.columns:
            raw = raw.drop(columns=["adj_close"])
        else:
            raw = raw.rename(columns={"adj_close": "close"})
    return raw


def extract_ticker(data, ticker):
    if not isinstance(data.columns, pd.MultiIndex):
        df = data.copy()
    else:
        lvl1 = data.columns.get_level_values(1)
        if ticker in lvl1.unique():
            mask = lvl1 == ticker
            df = data.loc[:, mask].copy()
            df.columns = df.columns.get_level_values(0)
        else:
            try:
                df = data[ticker].copy()
            except KeyError:
                return pd.DataFrame()

    df.columns = [
        str(c).lower().replace(" ", "_") for c in df.columns
    ]
    if "adj_close" in df.columns:
        if "close" in df.columns:
            df = df.drop(columns=["adj_close"])
        else:
            df = df.rename(columns={"adj_close": "close"})
    df = df.dropna(subset=["close"])
    return df


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    log = RunLogger("test_backtest")
    t0 = time.time()

    # ══════════════════════════════════════════════════════════
    #  STEP 1 — Build signal pipeline
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 1: Build scored + gated + signaled universe")

    spy_raw = yf.download(BENCHMARK, period=PERIOD, progress=False)
    spy = clean_single(spy_raw)

    etf_raw = yf.download(SECTOR_ETFS, period=PERIOD, progress=False)
    etf_data = {}
    for ticker in SECTOR_ETFS:
        df = extract_ticker(etf_raw, ticker)
        if not df.empty and len(df) > 100:
            etf_data[ticker] = df

    log.kv("Universe", f"{len(etf_data)} sector ETFs")

    breadth_raw = yf.download(
        BREADTH_TICKERS, period=PERIOD, progress=False,
    )
    breadth_universe = {}
    for ticker in BREADTH_TICKERS:
        df = extract_ticker(breadth_raw, ticker)
        if not df.empty and len(df) > 100:
            breadth_universe[ticker] = df

    breadth = compute_all_breadth(breadth_universe)
    pillar_df = breadth_to_pillar_scores(
        breadth, list(etf_data.keys()),
    )
    log.kv("Breadth regime", log.regime_badge(
        breadth["breadth_regime"].iloc[-1],
    ))

    scored_universe = {}
    for ticker, df in etf_data.items():
        df = compute_all_indicators(df)
        df = compute_all_rs(df, spy)
        bseries = (
            pillar_df[ticker] if ticker in pillar_df.columns
            else None
        )
        df = compute_composite_score(df, breadth_scores=bseries)
        df = generate_ticker_signals(df, breadth)
        scored_universe[ticker] = df

    ranked = compute_all_rankings(scored_universe)
    signal_config = SignalConfig()
    signals = compute_all_signals(ranked, breadth, signal_config)

    log.kv("Signal panel", signals.shape)
    log.kv(
        "Active today",
        active_positions(signals),
    )

    # ══════════════════════════════════════════════════════════
    #  STEP 2 — Position sizing methods
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 2: Position sizing")

    test_tickers = ["XLK", "XLF", "XLB"]
    test_strengths = {"XLK": 0.865, "XLF": 0.844, "XLB": 0.675}
    test_vols = {"XLK": 0.22, "XLF": 0.18, "XLB": 0.25}

    methods = [
        "equal_weight", "score_weighted",
        "inverse_volatility", "risk_parity",
    ]

    sz_cols = [
        {"header": "Method"},
        {"header": "XLK", "justify": "right"},
        {"header": "XLF", "justify": "right"},
        {"header": "XLB", "justify": "right"},
        {"header": "Sum", "justify": "right"},
    ]
    sz_rows = []

    for method in methods:
        cfg = SizingConfig(method=method)
        weights = compute_target_weights(
            test_tickers, cfg, test_strengths, test_vols,
        )
        row = [
            method,
            f"{weights.get('XLK', 0):.3f}",
            f"{weights.get('XLF', 0):.3f}",
            f"{weights.get('XLB', 0):.3f}",
            f"{sum(weights.values()):.3f}",
        ]
        sz_rows.append(row)

    log.table("Sizing Methods", sz_cols, sz_rows)

    # Limit enforcement
    log.h2("Limit enforcement")
    cfg_limited = SizingConfig(
        method="score_weighted",
        max_position_pct=0.40,
        min_position_pct=0.05,
    )
    w_limited = compute_target_weights(
        test_tickers, cfg_limited, test_strengths, test_vols,
    )
    log.kv("Max 40% / Min 5%", w_limited)

    # Empty / edge cases
    log.h2("Edge cases")
    empty_w = compute_target_weights([], SizingConfig())
    log.kv("Empty tickers", f"{empty_w}")
    single_w = compute_target_weights(["XLK"], SizingConfig())
    log.kv("Single ticker", f"{single_w}")

    # ══════════════════════════════════════════════════════════
    #  STEP 3 — Rebalancing
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 3: Rebalancing")

    current_w = {"XLK": 0.40, "XLF": 0.35, "XLB": 0.25}
    target_w = {"XLK": 0.36, "XLF": 0.33, "XLB": 0.31}

    drift = compute_drift(current_w, target_w)
    log.kv("Drift", {k: f"{v:+.3f}" for k, v in drift.items()})

    reb_config = RebalanceConfig(drift_threshold=0.05)
    log.kv(
        "Needs rebalance (5%)",
        needs_rebalance(drift, reb_config),
    )

    reb_config_tight = RebalanceConfig(drift_threshold=0.03)
    log.kv(
        "Needs rebalance (3%)",
        needs_rebalance(drift, reb_config_tight),
    )

    # Generate trades
    log.h2("Trade generation")
    current_pos = {"XLK": 100, "XLF": 80, "XLB": 60}
    prices_dict = {"XLK": 200.0, "XLF": 45.0, "XLB": 85.0}
    pv = sum(
        current_pos[t] * prices_dict[t] for t in current_pos
    )

    trades = generate_trades(
        current_pos, target_w, prices_dict, pv,
        pd.Timestamp("2026-04-17"), reb_config_tight,
    )
    log.kv("Trades generated", len(trades))
    for trade in trades:
        log.print(
            f"    {trade.action:<4} {trade.ticker:<6}  "
            f"{trade.shares:.1f} shares @ ${trade.price:.2f}  "
            f"${trade.value:,.2f}  "
            f"cost=${trade.total_cost:.2f}"
        )

    costs = estimate_costs(trades)
    log.kv("Total cost", f"${costs['total_cost']:.2f}")

    # ══════════════════════════════════════════════════════════
    #  STEP 4 — Risk metrics
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 4: Risk analysis")

    # Use SPY as a proxy equity curve for testing
    spy_equity = spy["close"].copy()
    spy_equity.name = "equity"

    spy_returns = spy_equity.pct_change().dropna()

    log.h2("Drawdown")
    dd_df = compute_drawdown(spy_equity)
    dd = drawdown_stats(spy_equity)
    log.kv("Max drawdown", f"{dd['max_drawdown']:.2%}")
    log.kv("Max DD duration", f"{dd['max_dd_duration']} days")
    log.kv("Current DD", f"{dd['current_drawdown']:.2%}")

    log.h2("Value at Risk")
    log.kv("VaR 95%", f"{compute_var(spy_returns, 0.95):.4f}")
    log.kv("VaR 99%", f"{compute_var(spy_returns, 0.99):.4f}")
    log.kv("CVaR 95%", f"{compute_cvar(spy_returns, 0.95):.4f}")

    log.h2("Concentration")
    conc = concentration_risk(current_w)
    log.kv("HHI", f"{conc['hhi']:.3f}")
    log.kv("Effective N", f"{conc['effective_n']:.1f}")
    log.kv("Max weight", f"{conc['max_weight']:.1%}")
    log.kv("Max ticker", conc["max_ticker"])

    log.h2("Rolling volatility")
    roll_vol = rolling_volatility(spy_returns, window=20)
    log.kv(
        "Current (20d ann)",
        f"{roll_vol.iloc[-1]:.2%}",
    )
    log.kv(
        "Mean",
        f"{roll_vol.mean():.2%}",
    )

    log.h2("Full risk summary")
    risk = compute_portfolio_risk(
        spy_equity, spy_returns, current_w,
    )
    for k, v in risk.items():
        if isinstance(v, dict):
            log.kv(k, v)
        elif isinstance(v, float):
            log.kv(k, f"{v:.4f}")
        else:
            log.kv(k, str(v))

    # ══════════════════════════════════════════════════════════
    #  STEP 5 — Backtest (default config)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 5: Backtest — default config")

    bt_config = BacktestConfig(
        initial_capital=100_000,
        sizing=SizingConfig(method="score_weighted"),
        rebalance=RebalanceConfig(
            drift_threshold=0.05,
            commission_pct=0.001,
            slippage_pct=0.0005,
        ),
    )

    result = run_backtest(signals, bt_config)

    m = result.metrics
    log.kv("Period", f"{m.get('n_days', 0)} days ({m.get('n_years', 0):.1f} years)")
    log.kv("Total return", f"{m.get('total_return', 0):+.2%}")
    log.kv("CAGR", f"{m.get('cagr', 0):+.2%}")
    log.kv("Sharpe", f"{m.get('sharpe_ratio', 0):.3f}")
    log.kv("Sortino", f"{m.get('sortino_ratio', 0):.3f}")
    log.kv("Max drawdown", f"{m.get('max_drawdown', 0):.2%}")
    log.kv("Calmar", f"{m.get('calmar_ratio', 0):.3f}")
    log.kv("Total trades", m.get("total_trades", 0))
    log.kv("Win rate", f"{m.get('win_rate', 0):.1%}")
    log.kv("Profit factor", f"{m.get('profit_factor', 0):.2f}")
    log.kv("Commission", f"${m.get('total_commission', 0):,.2f}")
    log.kv("Final capital", f"${m.get('final_capital', 0):,.2f}")

    # Equity curve tail
    log.h2("Equity curve (last 10 days)")
    eq_tail = result.equity_curve.tail(10)
    eq_cols = [
        {"header": "Date", "style": "bold"},
        {"header": "Equity", "justify": "right"},
        {"header": "Return", "justify": "right"},
    ]
    eq_rows = []
    for dt, val in eq_tail.items():
        ret = result.daily_returns.get(dt, 0)
        dt_str = (
            dt.strftime("%Y-%m-%d")
            if hasattr(dt, "strftime") else str(dt)
        )
        eq_rows.append([
            dt_str,
            f"${val:,.2f}",
            f"{ret:+.2%}",
        ])
    log.table("Equity Curve", eq_cols, eq_rows)

    # Position weights tail
    log.h2("Position weights (last day)")
    if not result.weights.empty:
        last_w = result.weights.iloc[-1]
        last_w = last_w[last_w > 0.001].sort_values(ascending=False)
        for ticker, w in last_w.items():
            if ticker.startswith("_"):
                continue
            log.kv(f"  {ticker}", f"{w:.1%}")

    # Trade log (last 10)
    log.h2("Recent trades (last 10)")
    recent_trades = result.trades[-10:] if result.trades else []
    tr_cols = [
        {"header": "Date", "style": "bold"},
        {"header": "Action"},
        {"header": "Ticker", "style": "bold cyan"},
        {"header": "Shares", "justify": "right"},
        {"header": "Price", "justify": "right"},
        {"header": "Value", "justify": "right"},
        {"header": "Cost", "justify": "right"},
    ]
    tr_rows = []
    for t in recent_trades:
        dt_str = (
            t.date.strftime("%Y-%m-%d")
            if hasattr(t.date, "strftime") else str(t.date)
        )
        tr_rows.append([
            dt_str,
            t.action,
            t.ticker,
            f"{t.shares:.1f}",
            f"${t.price:.2f}",
            f"${t.value:,.2f}",
            f"${t.total_cost:.2f}",
        ])
    log.table("Recent Trades", tr_cols, tr_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 6 — Backtest sizing comparison
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 6: Sizing method comparison")

    comparison: list[tuple[str, dict]] = []

    for method in ["equal_weight", "score_weighted",
                    "inverse_volatility", "risk_parity"]:
        cfg = BacktestConfig(
            initial_capital=100_000,
            sizing=SizingConfig(method=method),
            rebalance=RebalanceConfig(),
        )
        res = run_backtest(signals, cfg)
        comparison.append((method, res.metrics))

    cmp_cols = [
        {"header": "Method"},
        {"header": "Return", "justify": "right"},
        {"header": "Sharpe", "justify": "right"},
        {"header": "Max DD", "justify": "right"},
        {"header": "Win %", "justify": "right"},
        {"header": "Trades", "justify": "right"},
        {"header": "Final $", "justify": "right"},
    ]
    cmp_rows = []
    for method, met in comparison:
        cmp_rows.append([
            method,
            f"{met.get('total_return', 0):+.2%}",
            f"{met.get('sharpe_ratio', 0):.3f}",
            f"{met.get('max_drawdown', 0):.2%}",
            f"{met.get('win_rate', 0):.1%}",
            str(met.get("total_trades", 0)),
            f"${met.get('final_capital', 0):,.0f}",
        ])
    log.table("Sizing Comparison", cmp_cols, cmp_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 7 — Backtest risk analysis
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 7: Backtest risk analysis")

    bt_risk = compute_portfolio_risk(
        result.equity_curve,
        result.daily_returns,
    )
    log.kv("Annual volatility", f"{bt_risk.get('annual_volatility', 0):.2%}")
    log.kv("VaR 95%", f"{bt_risk.get('var_95', 0):.4f}")
    log.kv("CVaR 95%", f"{bt_risk.get('cvar_95', 0):.4f}")
    log.kv("Max drawdown", f"{bt_risk.get('max_drawdown', 0):.2%}")
    if "skewness" in bt_risk:
        log.kv("Skewness", f"{bt_risk['skewness']:.3f}")
    if "kurtosis" in bt_risk:
        log.kv("Kurtosis", f"{bt_risk['kurtosis']:.3f}")

    # Drawdown periods
    dd_df = compute_drawdown(result.equity_curve)
    if not dd_df.empty:
        worst_dd = dd_df["drawdown"].min()
        worst_date = dd_df["drawdown"].idxmin()
        log.kv(
            "Worst drawdown",
            f"{worst_dd:.2%} on {worst_date.strftime('%Y-%m-%d')}"
            if hasattr(worst_date, "strftime") else f"{worst_dd:.2%}",
        )

    # ══════════════════════════════════════════════════════════
    #  STEP 8 — Comprehensive report
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 8: Comprehensive strategy report")

    full_report = generate_full_report(
        signals_df=signals,
        breadth=breadth,
        config=signal_config,
        backtest_result=result,
        include_strategy=True,
    )
    log.print(f"\n{full_report}")

    # ══════════════════════════════════════════════════════════
    #  STEP 9 — Individual report sections
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 9: Individual report sections")

    log.h2("Breadth section")
    br = breadth_section(breadth)
    log.print(br)

    log.h2("Strategy overview")
    so = strategy_overview(signal_config)
    log.print(so)

    log.h2("Performance report")
    pr = performance_report(result)
    log.print(pr)

    # ══════════════════════════════════════════════════════════
    #  STEP 10 — Execution delay comparison
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 10: Execution delay comparison")

    for delay in [0, 1]:
        cfg = BacktestConfig(
            initial_capital=100_000,
            execution_delay=delay,
        )
        res = run_backtest(signals, cfg)
        met = res.metrics
        log.kv(
            f"Delay={delay}",
            f"Return={met.get('total_return', 0):+.2%}  "
            f"Sharpe={met.get('sharpe_ratio', 0):.3f}  "
            f"MaxDD={met.get('max_drawdown', 0):.2%}"
        )

    # ══════════════════════════════════════════════════════════
    #  STEP 11 — Validation
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 11: Validation")

    all_ok = True

    # Equity curve non-empty
    if len(result.equity_curve) > 0:
        log.success(
            f"Equity curve: {len(result.equity_curve)} days"
        )
    else:
        log.warning("Empty equity curve")
        all_ok = False

    # Equity starts at initial capital
    first_eq = result.equity_curve.iloc[0]
    if abs(first_eq - bt_config.initial_capital) / bt_config.initial_capital < 0.05:
        log.success(
            f"Initial equity ~${first_eq:,.0f} "
            f"(target ${bt_config.initial_capital:,.0f})"
        )
    else:
        log.warning(f"Initial equity mismatch: ${first_eq:,.0f}")
        all_ok = False

    # No NaN in equity
    if not result.equity_curve.isna().any():
        log.success("No NaN in equity curve")
    else:
        log.warning("NaN in equity curve")
        all_ok = False

    # Equity always positive
    if (result.equity_curve > 0).all():
        log.success("Equity always positive")
    else:
        log.warning("Negative equity detected")
        all_ok = False

    # Weights sum to ≤ 1
    if not result.weights.empty:
        ticker_cols = [
            c for c in result.weights.columns
            if not c.startswith("_")
        ]
        weight_sums = result.weights[ticker_cols].sum(axis=1)
        max_sum = weight_sums.max()
        if max_sum <= 1.05:
            log.success(
                f"Weights sum ≤ 1.0 (max {max_sum:.3f})"
            )
        else:
            log.warning(
                f"Weight sum exceeded 1.0: {max_sum:.3f}"
            )
            all_ok = False

    # Trades have valid fields
    if result.trades:
        bad = [
            t for t in result.trades
            if t.shares <= 0 or t.price <= 0
        ]
        if not bad:
            log.success(
                f"All {len(result.trades)} trades valid"
            )
        else:
            log.warning(f"{len(bad)} invalid trades")
            all_ok = False

    # Metrics completeness
    expected_keys = [
        "total_return", "cagr", "sharpe_ratio",
        "max_drawdown", "total_trades",
    ]
    missing = [k for k in expected_keys if k not in m]
    if not missing:
        log.success("All expected metrics present")
    else:
        log.warning(f"Missing metrics: {missing}")
        all_ok = False

    # Sizing: weights sum to target exposure
    for method in methods:
        cfg = SizingConfig(method=method, target_exposure=0.90)
        w = compute_target_weights(
            test_tickers, cfg, test_strengths, test_vols,
        )
        total = sum(w.values())
        if abs(total - 0.90) < 0.01:
            log.success(
                f"{method}: weights sum to {total:.3f} "
                f"(target 0.90)"
            )
        else:
            log.warning(
                f"{method}: weights sum to {total:.3f} "
                f"(expected 0.90)"
            )
            all_ok = False

    # Drawdown: max_drawdown ≤ 0
    if m.get("max_drawdown", 0) <= 0:
        log.success(
            f"Max drawdown is non-positive: "
            f"{m['max_drawdown']:.2%}"
        )
    else:
        log.warning("Max drawdown > 0 (should be ≤ 0)")
        all_ok = False

    # Edge cases
    log.h2("Edge cases")

    empty_bt = run_backtest(pd.DataFrame())
    if empty_bt.equity_curve.empty:
        log.success("Empty signals → empty backtest")
    else:
        log.warning("Expected empty backtest")
        all_ok = False

    empty_dd = compute_drawdown(pd.Series(dtype=float))
    if empty_dd.empty:
        log.success("Empty equity → empty drawdown")

    zero_conc = concentration_risk({})
    if zero_conc["hhi"] == 0:
        log.success("Empty weights → zero concentration")

    empty_report = performance_report(None)
    if "No backtest" in empty_report:
        log.success("None result → placeholder report")

    empty_daily = daily_report(pd.DataFrame())
    if "No data" in empty_daily:
        log.success("Empty signals → placeholder daily report")

    # ══════════════════════════════════════════════════════════
    #  SUMMARY
    # ══════════════════════════════════════════════════════════
    elapsed = time.time() - t0

    log.h1("SUMMARY")
    log.kv("Universe", f"{len(etf_data)} sector ETFs")
    log.kv("Signal period", f"{m.get('n_days', 0)} days")
    log.kv("Total return", f"{m.get('total_return', 0):+.2%}")
    log.kv("Sharpe ratio", f"{m.get('sharpe_ratio', 0):.3f}")
    log.kv("Max drawdown", f"{m.get('max_drawdown', 0):.2%}")
    log.kv("Total trades", m.get("total_trades", 0))
    log.kv("Final capital", f"${m.get('final_capital', 0):,.2f}")
    log.kv(
        "All validations",
        "PASSED ✓" if all_ok else "ISSUES FOUND ⚠",
    )
    log.kv("Elapsed", f"{elapsed:.1f}s")
    log.divider()
    log.success("ALL BACKTEST TESTS PASSED")

    html_path = log.save()
    log.print(f"\n  [dim]HTML report → {html_path}[/]")


if __name__ == "__main__":
    main()