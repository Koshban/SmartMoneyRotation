"""
backtest/engine.py
------------------
Core backtesting engine.

Ties together data loading, the full CASH pipeline, and the
portfolio simulation into a single ``run_backtest_period()``
call.

For strategy comparison, the engine accepts parameter overrides
via a ``StrategyConfig`` object.  Overrides are applied to the
global config dicts using a context manager, then restored
after the run — safe for sequential comparison of many variants.

Key improvements
----------------
- Config overrides now wrap both pipeline AND portfolio simulation
- Minimum holding period prevents excessive churn (default 20 cal-days)
- Breadth crisis response blocks equity BUYs in weak/critical regimes
- Cash proxy (SHY) earns returns on idle capital instead of 0 %
- Robust column detection for any signal DataFrame schema
"""

from __future__ import annotations

import copy
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SCORING_WEIGHTS,
    SCORING_PARAMS,
    SIGNAL_PARAMS,
    BREADTH_PORTFOLIO,
)
from pipeline.orchestrator import Orchestrator, PipelineResult
from portfolio.backtest import (
    BacktestConfig,
    BacktestResult,
    run_backtest as run_portfolio_backtest,
    compute_performance_metrics,
)
from portfolio.sizing import SizingConfig
from portfolio.rebalance import RebalanceConfig
from output.signals import SignalConfig

from backtest.data_loader import slice_period, data_summary
from backtest.metrics import compute_full_metrics

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

DEFENSIVE_TICKERS = frozenset({
    "AGG", "SHY", "TLT", "IEF", "GLD", "BIL",
})

# Column-name candidates (checked in priority order)
_SIGNAL_COLS = ("signal", "action", "trade_signal")
_TICKER_COLS = ("ticker", "symbol", "asset")
_DATE_COLS   = ("date", "trade_date", "timestamp")
_SCORE_COLS  = ("composite_score", "score", "total_score", "rank_score")
_REGIME_COLS = (
    "breadth_regime", "regime", "market_regime", "breadth_label",
)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL VALUE HELPERS
# ═══════════════════════════════════════════════════════════════

def _is_buy(val) -> bool:
    """Return True if *val* represents a BUY / entry signal."""
    if isinstance(val, str):
        return val.upper() in ("BUY", "STRONG_BUY", "ENTRY")
    try:
        return float(val) == 1.0
    except (TypeError, ValueError):
        return False


def _is_sell(val) -> bool:
    """Return True if *val* represents a SELL / exit signal."""
    if isinstance(val, str):
        return val.upper() in ("SELL", "STRONG_SELL", "EXIT")
    try:
        return float(val) == -1.0
    except (TypeError, ValueError):
        return False


def _buy_value(sample) -> Any:
    """Return the BUY constant matching the dtype of *sample*."""
    return "BUY" if isinstance(sample, str) else 1


def _hold_value(sample) -> Any:
    """Return the HOLD / neutral constant matching *sample*."""
    return "HOLD" if isinstance(sample, str) else 0


# ═══════════════════════════════════════════════════════════════
#  STRATEGY CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class StrategyConfig:
    """
    A named set of parameter overrides for backtesting.

    Each dict field is either ``None`` (use global default) or a
    mapping of key → value that will be merged into the
    corresponding config dict for the duration of the run.

    Example
    -------
    >>> aggressive = StrategyConfig(
    ...     name="aggressive_momentum",
    ...     description="Heavy momentum weighting",
    ...     scoring_weights={"pillar_momentum": 0.40},
    ...     signal_params={"entry_score_min": 0.50},
    ...     min_hold_days=30,
    ... )
    """
    name: str = "baseline"
    description: str = "Default CASH parameters"

    # ── Config-dict overrides (applied to globals) ────────────
    scoring_weights:   dict | None = None
    scoring_params:    dict | None = None
    signal_params:     dict | None = None
    portfolio_params:  dict | None = None
    breadth_portfolio: dict | None = None

    # ── Component-config overrides ────────────────────────────
    signal_config_overrides:   dict | None = None
    sizing_config_overrides:   dict | None = None
    backtest_config_overrides: dict | None = None

    # ── Universe filter ───────────────────────────────────────
    universe_filter: list[str] | None = None

    # ── Trading rules ─────────────────────────────────────────
    min_hold_days: int = 20
    """Minimum calendar days between entry and exit.  0 = disabled."""

    cash_proxy: str | None = "SHY"
    """Ticker whose returns are applied to idle cash.  None = disabled."""

    # ── Breadth crisis response ───────────────────────────────
    breadth_defensive: bool = True
    """Block equity BUY signals during weak / critical breadth."""

    max_equity_weak: float = 0.40
    """Maximum equity exposure allowed during *weak* breadth."""

    max_equity_critical: float = 0.15
    """Maximum equity exposure allowed during *critical* breadth."""


# ═══════════════════════════════════════════════════════════════
#  BACKTEST RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class BacktestRun:
    """
    Complete output of a single backtest run.

    Wraps the pipeline result, the portfolio simulation result,
    and comprehensive performance metrics.
    """
    strategy: StrategyConfig
    start_date: pd.Timestamp
    end_date: pd.Timestamp

    pipeline_result: PipelineResult | None = None
    backtest_result: BacktestResult | None = None

    metrics: dict = field(default_factory=dict)
    annual_returns: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )
    monthly_returns: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark_equity: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float),
    )

    elapsed_seconds: float = 0.0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.metrics.get("cagr") is not None

    @property
    def cagr(self) -> float:
        return self.metrics.get("cagr", 0.0)

    @property
    def sharpe(self) -> float:
        return self.metrics.get("sharpe_ratio", 0.0)

    @property
    def max_drawdown(self) -> float:
        return self.metrics.get("max_drawdown", 0.0)

    def summary_line(self) -> str:
        if not self.ok:
            return f"{self.strategy.name:<24s}  ERROR: {self.error}"
        return (
            f"{self.strategy.name:<24s}  "
            f"CAGR={self.cagr:>+7.2%}  "
            f"Sharpe={self.sharpe:>5.2f}  "
            f"MaxDD={self.max_drawdown:>7.2%}  "
            f"Trades={self.metrics.get('total_trades', 0):>5d}  "
            f"({self.elapsed_seconds:.0f}s)"
        )


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_backtest_period(
    data: dict[str, pd.DataFrame],
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    strategy: StrategyConfig | None = None,
    capital: float = 100_000.0,
    benchmark: str | None = None,
) -> BacktestRun:
    """
    Run a complete backtest over a date range.

    Steps
    -----
    1. Slice data to ``[start, end]``
    2. Apply strategy parameter overrides  (context manager)
    3. Run the full CASH pipeline  (Orchestrator)
    4. **Pre-process signals**  (min-hold, breadth override)
    5. Run the portfolio simulation  (portfolio/backtest.py)
    6. **Adjust equity for cash-proxy returns**
    7. Compute performance metrics and benchmarks
    8. Return everything in a ``BacktestRun``

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        ``{ticker: OHLCV}`` from ``ensure_history()``.
    start, end : str / Timestamp / None
        Period bounds.  *None* → earliest / latest available.
    strategy : StrategyConfig or None
        Override set.  *None* → defaults.
    capital : float
        Initial portfolio value.
    benchmark : str or None
        Benchmark ticker.  Default = SPY.
    """
    if strategy is None:
        strategy = StrategyConfig()

    benchmark = benchmark or BENCHMARK_TICKER
    t0 = time.perf_counter()

    # ── 1. Slice data ─────────────────────────────────────────
    sliced = slice_period(data, start=start, end=end)
    if not sliced:
        return BacktestRun(
            strategy=strategy,
            start_date=pd.Timestamp(start) if start else pd.NaT,
            end_date=pd.Timestamp(end) if end else pd.NaT,
            error="No data after slicing to requested period",
        )

    # ── 2. Apply universe filter ──────────────────────────────
    if strategy.universe_filter:
        # Always keep benchmark + cash proxy in the universe
        must_keep = {benchmark}
        if strategy.cash_proxy and strategy.cash_proxy in data:
            must_keep.add(strategy.cash_proxy)

        sliced = {
            k: v for k, v in sliced.items()
            if k in strategy.universe_filter or k in must_keep
        }

    summary = data_summary(sliced)
    actual_start = summary["earliest_start"]
    actual_end = summary["latest_end"]

    logger.info(
        f"Backtest '{strategy.name}': "
        f"{summary['n_tickers']} tickers, "
        f"{actual_start.date()} \u2192 {actual_end.date()}, "
        f"${capital:,.0f}"
    )

    # ── 3. Extract benchmark equity ───────────────────────────
    bench_df = sliced.get(benchmark)
    if bench_df is None or bench_df.empty:
        logger.warning(
            f"Benchmark {benchmark} not in data — "
            f"benchmark comparison will be unavailable"
        )
        bench_equity = pd.Series(dtype=float)
    else:
        bench_equity = (
            bench_df["close"] / bench_df["close"].iloc[0] * capital
        )
        bench_equity.name = "benchmark"

    # ══════════════════════════════════════════════════════════
    #  Everything inside _config_overrides so that scoring
    #  weights, signal params, portfolio params, breadth
    #  settings, etc. are consistently applied to BOTH the
    #  pipeline AND the portfolio simulation.
    # ══════════════════════════════════════════════════════════
    with _config_overrides(strategy):

        # ── 4a. Run CASH pipeline (Phases 0 – 4) ─────────────
        try:
            orch = Orchestrator(
                universe=list(sliced.keys()),
                benchmark=benchmark,
                capital=capital,
                enable_breadth=True,
                enable_sectors=True,
                enable_signals=True,
                enable_backtest=False,   # we run our own below
            )

            orch.load_data(preloaded=sliced, bench_df=bench_df)
            orch.compute_universe_context()
            orch.run_tickers()
            orch.cross_sectional_analysis()
            pipeline_result = orch.generate_reports()

        except Exception as e:
            logger.error(f"Pipeline failed for '{strategy.name}': {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Pipeline error: {e}",
            )

        # ── 4b. Validate signals ─────────────────────────────
        signals_df = pipeline_result.signals
        if signals_df is None or signals_df.empty:
            logger.warning(
                f"No signals generated for '{strategy.name}'"
            )
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error="No signals generated — check pipeline logs",
            )

        # ── 4c. Pre-process signals ──────────────────────────
        signals_df = _preprocess_signals(
            signals_df, strategy, pipeline_result, sliced,
        )

        # ── 4d. Run portfolio simulation ──────────────────────
        try:
            bt_config = _build_backtest_config(strategy, capital)
            bt_result = run_portfolio_backtest(
                signals_df=signals_df,
                config=bt_config,
            )
        except Exception as e:
            logger.error(f"Backtest simulation failed: {e}")
            return BacktestRun(
                strategy=strategy,
                start_date=actual_start,
                end_date=actual_end,
                pipeline_result=pipeline_result,
                elapsed_seconds=time.perf_counter() - t0,
                error=f"Simulation error: {e}",
            )

    # ── 5. Adjust equity for cash-proxy returns ───────────────
    equity_curve = bt_result.equity_curve.copy()

    if strategy.cash_proxy and strategy.cash_proxy in sliced:
        equity_curve = _apply_cash_proxy_to_equity(
            equity_curve=equity_curve,
            bt_result=bt_result,
            proxy_prices=sliced[strategy.cash_proxy],
            initial_capital=capital,
        )

    # Recompute daily returns from (possibly adjusted) equity
    daily_returns = equity_curve.pct_change().dropna()

    # ── 6. Compute comprehensive metrics ──────────────────────
    metrics = compute_full_metrics(
        equity_curve=equity_curve,
        daily_returns=daily_returns,
        trades=bt_result.trades,
        initial_capital=capital,
        benchmark_equity=bench_equity,
    )

    annual = _compute_annual_returns(equity_curve)
    monthly = _compute_monthly_returns(daily_returns)
    elapsed = time.perf_counter() - t0

    run = BacktestRun(
        strategy=strategy,
        start_date=actual_start,
        end_date=actual_end,
        pipeline_result=pipeline_result,
        backtest_result=bt_result,
        metrics=metrics,
        annual_returns=annual,
        monthly_returns=monthly,
        benchmark_equity=bench_equity,
        elapsed_seconds=elapsed,
    )

    logger.info(run.summary_line())
    return run


# ═══════════════════════════════════════════════════════════════
#  CONFIG OVERRIDE CONTEXT MANAGER
# ═══════════════════════════════════════════════════════════════

@contextmanager
def _config_overrides(strategy: StrategyConfig):
    """
    Temporarily patch global config dicts with strategy overrides.

    Saves originals, applies overrides, yields control, then
    restores originals in a ``finally`` block.  Safe for
    sequential use across many strategies.
    """
    import common.config as cfg

    config_targets = [
        ("SCORING_WEIGHTS",  cfg.SCORING_WEIGHTS,  strategy.scoring_weights),
        ("SCORING_PARAMS",   cfg.SCORING_PARAMS,   strategy.scoring_params),
        ("SIGNAL_PARAMS",    cfg.SIGNAL_PARAMS,    strategy.signal_params),
        ("PORTFOLIO_PARAMS", cfg.PORTFOLIO_PARAMS,  strategy.portfolio_params),
        ("BREADTH_PORTFOLIO", cfg.BREADTH_PORTFOLIO, strategy.breadth_portfolio),
    ]

    # Save originals  ──  shallow copy is sufficient because we
    # only call .update() (not nested mutation) on the dicts.
    originals: list[tuple[str, dict, dict]] = []
    for name, target_dict, overrides in config_targets:
        originals.append((name, target_dict, dict(target_dict)))
        if overrides:
            logger.debug(
                f"Config override [{strategy.name}] {name}: "
                f"{overrides}"
            )
            target_dict.update(overrides)

    n_overridden = sum(1 for _, _, ov in config_targets if ov)
    if n_overridden:
        logger.info(
            f"Applied {n_overridden} config overrides for "
            f"'{strategy.name}'"
        )

    try:
        yield
    finally:
        for _name, target_dict, original_values in originals:
            target_dict.clear()
            target_dict.update(original_values)


# ═══════════════════════════════════════════════════════════════
#  COLUMN DETECTION
# ═══════════════════════════════════════════════════════════════

def _detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    """
    Identify key column names in a signals DataFrame.

    Returns a dict with keys ``signal``, ``ticker``, ``date``,
    ``score``, ``regime`` mapped to the actual column name found
    (or *None* if absent).
    """
    def _find(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    return {
        "signal": _find(_SIGNAL_COLS),
        "ticker": _find(_TICKER_COLS),
        "date":   _find(_DATE_COLS),
        "score":  _find(_SCORE_COLS),
        "regime": _find(_REGIME_COLS),
    }


# ═══════════════════════════════════════════════════════════════
#  SIGNAL PRE-PROCESSING  (master function)
# ═══════════════════════════════════════════════════════════════

def _preprocess_signals(
    signals_df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    price_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Apply all signal-level modifications before the portfolio sim.

    1. Breadth crisis response  — block equity BUYs during
       weak / critical regimes and boost cash-proxy score.
    2. Minimum holding period   — suppress premature SELLs.
    """
    cols = _detect_columns(signals_df)
    sig_col    = cols["signal"]
    ticker_col = cols["ticker"]
    date_col   = cols["date"]

    if not sig_col or not ticker_col:
        logger.warning(
            "Cannot detect signal/ticker columns in signals_df "
            f"(columns: {list(signals_df.columns)[:15]}…) — "
            "skipping all signal preprocessing"
        )
        return signals_df

    df = signals_df.copy()

    # ── 1. Breadth crisis override ────────────────────────────
    if strategy.breadth_defensive:
        df = _apply_breadth_override(
            df, strategy, pipeline_result,
            sig_col, ticker_col, date_col, cols["score"],
        )

    # ── 2. Minimum holding period ─────────────────────────────
    if strategy.min_hold_days > 0 and date_col:
        df = _enforce_min_hold(
            df, strategy.min_hold_days,
            sig_col, ticker_col, date_col,
        )

    return df


# ═══════════════════════════════════════════════════════════════
#  BREADTH CRISIS OVERRIDE
# ═══════════════════════════════════════════════════════════════

def _extract_regime_column(
    df: pd.DataFrame,
    pipeline_result: PipelineResult,
    date_col: str | None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Ensure *df* has a breadth-regime column.

    Checks the DataFrame first; if absent, tries to pull regime
    data from ``pipeline_result`` and merge it in by date.

    Returns ``(possibly-modified df, regime_column_name or None)``.
    """
    # Already present?
    for col in _REGIME_COLS:
        if col in df.columns:
            return df, col

    if pipeline_result is None or date_col is None:
        return df, None

    # Search pipeline_result for breadth data
    breadth = None
    for attr in (
        "breadth", "breadth_data", "breadth_df",
        "market_breadth", "universe_context",
    ):
        val = getattr(pipeline_result, attr, None)
        if val is not None:
            breadth = val
            break

    if breadth is None:
        return df, None

    # Normalise to a single-column DataFrame with a regime label
    regime_series: pd.Series | None = None

    if isinstance(breadth, pd.Series):
        regime_series = breadth

    elif isinstance(breadth, pd.DataFrame):
        for col in _REGIME_COLS:
            if col in breadth.columns:
                regime_series = breadth[col]
                break
        # Fallback: last column if it looks categorical
        if regime_series is None and breadth.shape[1] > 0:
            last = breadth.iloc[:, -1]
            if last.dtype == object or str(last.dtype) == "category":
                regime_series = last

    elif isinstance(breadth, dict) and "regime" in breadth:
        val = breadth["regime"]
        if isinstance(val, (pd.Series, pd.DataFrame)):
            regime_series = (
                val if isinstance(val, pd.Series) else val.iloc[:, 0]
            )

    if regime_series is None:
        return df, None

    # Merge into df by date
    regime_df = regime_series.to_frame(name="_breadth_regime")
    if date_col in df.columns:
        df = df.merge(
            regime_df,
            left_on=date_col,
            right_index=True,
            how="left",
        )
        return df, "_breadth_regime"

    return df, None


def _apply_breadth_override(
    df: pd.DataFrame,
    strategy: StrategyConfig,
    pipeline_result: PipelineResult,
    sig_col: str,
    ticker_col: str,
    date_col: str | None,
    score_col: str | None,
) -> pd.DataFrame:
    """
    Block equity BUY signals during weak / critical breadth
    regimes and (optionally) boost the cash-proxy score so it
    gets selected by the portfolio builder.
    """
    df, regime_col = _extract_regime_column(
        df, pipeline_result, date_col,
    )

    if regime_col is None:
        logger.debug(
            "No breadth-regime column found — "
            "skipping crisis override"
        )
        return df

    # Identify which rows are in a weak/critical regime
    regime_lower = df[regime_col].astype(str).str.lower()
    weak_mask     = regime_lower == "weak"
    critical_mask = regime_lower == "critical"
    crisis_mask   = weak_mask | critical_mask

    if not crisis_mask.any():
        return df

    # Determine signal type (string vs numeric)
    sample_sig = df[sig_col].dropna().iloc[0] if len(df) else "BUY"
    hold = _hold_value(sample_sig)
    buy  = _buy_value(sample_sig)

    # ── Block equity BUYs ─────────────────────────────────────
    is_equity = ~df[ticker_col].isin(DEFENSIVE_TICKERS)
    is_buy    = df[sig_col].apply(_is_buy)
    block     = crisis_mask & is_equity & is_buy
    n_blocked = int(block.sum())

    if n_blocked:
        df.loc[block, sig_col] = hold
        logger.info(
            f"Breadth override: blocked {n_blocked:,} equity BUY "
            f"signals in weak/critical regime"
        )

    # ── Boost cash proxy so it fills freed slots ──────────────
    proxy = strategy.cash_proxy
    if proxy and score_col and proxy in df[ticker_col].values:
        proxy_crisis = crisis_mask & (df[ticker_col] == proxy)
        if proxy_crisis.any():
            # Give cash proxy the maximum score during crises
            max_score = df[score_col].max()
            df.loc[proxy_crisis, score_col] = max_score
            df.loc[proxy_crisis, sig_col]   = buy
            logger.info(
                f"Breadth override: boosted {proxy} score on "
                f"{int(proxy_crisis.sum()):,} crisis days"
            )

    return df


# ═══════════════════════════════════════════════════════════════
#  MINIMUM HOLDING PERIOD
# ═══════════════════════════════════════════════════════════════

def _enforce_min_hold(
    df: pd.DataFrame,
    min_hold_days: int,
    sig_col: str,
    ticker_col: str,
    date_col: str,
) -> pd.DataFrame:
    """
    Suppress SELL signals that arrive fewer than *min_hold_days*
    calendar days after the most recent BUY for the same ticker.

    Operates ticker-by-ticker using fast index iteration rather
    than ``iterrows()``.
    """
    if min_hold_days <= 0:
        return df

    # Ensure we have a sortable date column
    if date_col not in df.columns:
        logger.debug(
            f"Date column '{date_col}' not in DataFrame — "
            "skipping min-hold enforcement"
        )
        return df

    result = df.copy()

    # Determine the replacement value for suppressed SELLs.
    # We replace with BUY (= "keep holding") rather than HOLD,
    # because the portfolio sim interprets BUY as "remain in
    # position" for an existing holding.
    sample_sig = (
        result[sig_col].dropna().iloc[0] if len(result) else "BUY"
    )
    keep_holding = _buy_value(sample_sig)

    total_suppressed = 0

    for ticker, group_indices in result.groupby(ticker_col).groups.items():
        sub = result.loc[group_indices, [date_col, sig_col]].sort_values(
            date_col
        )
        dates   = sub[date_col].values        # numpy datetime64
        signals = sub[sig_col].values          # numpy object / int
        indices = sub.index.values             # positional index

        in_position = False
        entry_ts: np.datetime64 | None = None
        fix_list: list = []

        for i in range(len(indices)):
            sig = signals[i]
            dt  = dates[i]

            if _is_buy(sig) and not in_position:
                in_position = True
                entry_ts = dt
            elif _is_buy(sig) and in_position:
                # Consecutive BUY — stay in position, don't reset
                # entry date (hold clock runs from original entry)
                pass
            elif _is_sell(sig) and in_position:
                try:
                    days_held = (
                        pd.Timestamp(dt) - pd.Timestamp(entry_ts)
                    ).days
                except Exception:
                    days_held = min_hold_days  # fail-open

                if days_held < min_hold_days:
                    fix_list.append(indices[i])
                else:
                    in_position = False
                    entry_ts = None
            elif _is_sell(sig) and not in_position:
                pass  # not holding — irrelevant

        if fix_list:
            result.loc[fix_list, sig_col] = keep_holding
            total_suppressed += len(fix_list)

    if total_suppressed:
        logger.info(
            f"Min-hold filter: suppressed {total_suppressed:,} "
            f"premature exits (min {min_hold_days} cal-days)"
        )

    return result


# ═══════════════════════════════════════════════════════════════
#  CASH PROXY — EQUITY ADJUSTMENT
# ═══════════════════════════════════════════════════════════════

def _apply_cash_proxy_to_equity(
    equity_curve: pd.Series,
    bt_result: BacktestResult,
    proxy_prices: pd.DataFrame,
    initial_capital: float,
) -> pd.Series:
    """
    Retroactively credit idle cash with the proxy's return.

    Tries to obtain a per-day cash balance from the backtest
    result.  If unavailable, falls back to a conservative
    estimate (20 % of equity assumed idle on average).

    Parameters
    ----------
    equity_curve : pd.Series
        Unadjusted equity from the portfolio sim.
    bt_result : BacktestResult
        Portfolio simulation output.
    proxy_prices : pd.DataFrame
        OHLCV for the cash proxy ticker.
    initial_capital : float
        Starting capital.

    Returns
    -------
    pd.Series
        Adjusted equity curve.
    """
    # ── Proxy daily returns ───────────────────────────────────
    if "close" in proxy_prices.columns:
        proxy_close = proxy_prices["close"]
    else:
        proxy_close = proxy_prices.iloc[:, 0]

    proxy_ret = proxy_close.pct_change().fillna(0.0)

    # ── Try to get actual cash balance ────────────────────────
    cash_series: pd.Series | None = None

    for attr in (
        "cash", "cash_series", "cash_balance",
        "cash_values", "available_cash",
    ):
        val = getattr(bt_result, attr, None)
        if isinstance(val, pd.Series) and len(val) > 0:
            cash_series = val
            break

    # Try reconstructing: equity minus position values
    if cash_series is None:
        for attr in (
            "positions_value", "invested_value",
            "position_values", "gross_exposure",
        ):
            val = getattr(bt_result, attr, None)
            if isinstance(val, pd.Series) and len(val) > 0:
                cash_series = (equity_curve - val).clip(lower=0.0)
                break

    if cash_series is None:
        # Last resort: assume 20 % of equity is idle (conservative)
        logger.debug(
            "No cash-balance data in BacktestResult — "
            "using 20 %% fallback for cash-proxy adjustment"
        )
        cash_series = equity_curve * 0.20

    # ── Align indices ─────────────────────────────────────────
    common_idx = (
        equity_curve.index
        .intersection(proxy_ret.index)
        .intersection(cash_series.index)
    )
    if len(common_idx) < 2:
        logger.debug("Not enough overlap for cash-proxy adjustment")
        return equity_curve

    cash   = cash_series.reindex(common_idx).ffill().fillna(0.0)
    p_ret  = proxy_ret.reindex(common_idx).fillna(0.0)

    # ── Walk forward: compound cash PnL day-by-day ────────────
    adjusted = equity_curve.reindex(common_idx).copy()
    cum_cash_pnl = 0.0

    values = adjusted.values.copy()  # numpy for speed
    cash_vals = cash.values
    ret_vals  = p_ret.values

    for i in range(1, len(values)):
        # Cash at start-of-day earns the proxy's return
        cash_pnl = cash_vals[i - 1] * ret_vals[i]
        cum_cash_pnl += cash_pnl
        values[i] += cum_cash_pnl

    adjusted = pd.Series(values, index=common_idx, name=equity_curve.name)

    added_pct = (
        (adjusted.iloc[-1] / equity_curve.reindex(common_idx).iloc[-1])
        - 1.0
    ) * 100
    logger.info(
        f"Cash proxy adjustment: +{added_pct:.2f} %% total return "
        f"from idle cash in {strategy_cash_proxy_name(proxy_prices)}"
    )

    return adjusted


def strategy_cash_proxy_name(proxy_prices: pd.DataFrame) -> str:
    """Best-effort human-readable name for the proxy ticker."""
    if hasattr(proxy_prices, "name") and proxy_prices.name:
        return str(proxy_prices.name)
    if hasattr(proxy_prices, "attrs") and "ticker" in proxy_prices.attrs:
        return proxy_prices.attrs["ticker"]
    return "SHY"


# ═══════════════════════════════════════════════════════════════
#  CONFIG BUILDERS
# ═══════════════════════════════════════════════════════════════

def _build_backtest_config(
    strategy: StrategyConfig,
    capital: float,
) -> BacktestConfig:
    """Build ``BacktestConfig`` from strategy overrides."""
    sizing_kw    = strategy.sizing_config_overrides or {}
    bt_overrides = strategy.backtest_config_overrides or {}

    sizing = SizingConfig(**{
        k: v for k, v in sizing_kw.items()
        if k in SizingConfig.__dataclass_fields__
    }) if sizing_kw else SizingConfig()

    rebalance_kw = {
        k: v for k, v in bt_overrides.items()
        if k in RebalanceConfig.__dataclass_fields__
    }
    rebalance = RebalanceConfig(**rebalance_kw) if rebalance_kw else RebalanceConfig()

    # Build the BacktestConfig, passing through only the
    # fields that BacktestConfig actually accepts.
    bc_fields = set(BacktestConfig.__dataclass_fields__)
    bc_kwargs: dict[str, Any] = {
        "initial_capital": capital,
        "sizing": sizing,
        "rebalance": rebalance,
    }

    # Standard optional fields
    for key in ("execution_delay", "rebalance_holds"):
        if key in bt_overrides and key in bc_fields:
            bc_kwargs[key] = bt_overrides[key]

    # Pass through min_hold_days and cash_proxy if BacktestConfig
    # supports them (future-proof).
    if "min_hold_days" in bc_fields:
        bc_kwargs["min_hold_days"] = strategy.min_hold_days
    if "cash_proxy" in bc_fields:
        bc_kwargs["cash_proxy"] = strategy.cash_proxy

    return BacktestConfig(**bc_kwargs)


# ═══════════════════════════════════════════════════════════════
#  RETURN COMPUTATIONS
# ═══════════════════════════════════════════════════════════════

def _compute_annual_returns(equity: pd.Series) -> pd.Series:
    """Year-by-year returns from an equity curve."""
    if equity.empty:
        return pd.Series(dtype=float)

    yearly = equity.resample("YE").last()
    returns = yearly.pct_change().dropna()
    returns.index = returns.index.year
    returns.name = "annual_return"
    return returns


def _compute_monthly_returns(
    daily_returns: pd.Series,
) -> pd.DataFrame:
    """
    Monthly returns as a year × month pivot table.

    Returns a DataFrame with years as rows, months (1–12) as
    columns, and monthly percentage returns as values.
    """
    if daily_returns.empty:
        return pd.DataFrame()

    monthly = (1 + daily_returns).resample("ME").prod() - 1
    monthly_df = pd.DataFrame({
        "year":   monthly.index.year,
        "month":  monthly.index.month,
        "return": monthly.values,
    })

    pivot = monthly_df.pivot_table(
        values="return",
        index="year",
        columns="month",
        aggfunc="first",
    )
    pivot.columns = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][: len(pivot.columns)]

    return pivot