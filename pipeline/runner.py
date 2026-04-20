"""
pipeline/runner.py
------------------
Single-ticker pipeline.

Accepts raw OHLCV for one ticker + benchmark, runs every CASH
compute module in sequence, returns the enriched DataFrame and
a summary snapshot of the latest row.

This is the atomic unit of work — orchestrator.py calls this
once per ticker, then layers on cross-sectional logic
(rankings, portfolio signals, backtesting).

Pipeline Order
──────────────
  raw OHLCV → date slice (optional as_of cut)
       ↓
  1. compute_all_indicators()     ~30 technical indicator columns
       ↓
  2. compute_all_rs()             RS ratio/slope/zscore/regime
       ↓
  3. compute_composite_score()    5-pillar composite (breadth opt.)
       ↓
  4. merge_sector_context()       sector tailwind (optional)
       ↓
  5. generate_signals()           6-gate entry/exit filter
       ↓
  TickerResult { df, snapshot, error }

Each stage validates its inputs and fails fast with a clear
error message.  Optional stages (sector, breadth) degrade
gracefully — the ticker is still scored and ranked without
sector adjustments or breadth gating.

Dependencies
────────────
  compute/indicators.py          — technical indicators
  compute/relative_strength.py   — RS vs benchmark
  compute/scoring.py             — 5-pillar composite
  compute/sectors.py             — sector merge (optional)
  strategy/signals.py            — 6-gate filter (optional)
  common/config.py               — all parameters
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from common.config import (
    INDICATOR_PARAMS,
    TICKER_SECTOR_MAP,
)
from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score
from compute.sector_rs import merge_sector_context
from strategy.signals import generate_signals

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Minimum bars needed for meaningful output.
#
# Breakdown of the slowest lookback chains:
#   indicators.py:   sma_long (50)
#   relative_strength.py:
#     rs_sma_span (50) → rs_slope_window (20) → rs_zscore_window (60)
#     Total: ~130 bars for RS z-score to produce values
#   scoring.py:      rank_window (60) for rolling percentile ranks
#   breadth.py:      high_low_window (252) but that's universe-level
#
# With 200 bars the latest ~60 rows have fully warmed-up
# values across all pillars.
_MIN_BARS = 200

# ATR multiplier for initial stop-loss calculation.
# stop_price = close − ATR_STOP_MULT × ATR
ATR_STOP_MULT = 2.0


# ═══════════════════════════════════════════════════════════════
#  RESULT OBJECT
# ═══════════════════════════════════════════════════════════════

@dataclass
class TickerResult:
    """
    Output of the single-ticker pipeline.

    Attributes
    ----------
    ticker : str
        Symbol, e.g. "AAPL".
    df : pd.DataFrame
        Full enriched DataFrame with indicator, RS, scoring,
        sector, and signal columns appended.  Empty if error.
    snapshot : dict
        Latest-row summary values for quick access by the
        orchestrator and report generators.
    error : str or None
        Error message if the pipeline failed for this ticker.
        None on success.
    stages_completed : list[str]
        Which pipeline stages ran successfully.  Useful for
        diagnosing partial failures (e.g. scoring succeeded
        but sector merge failed).
    """

    ticker: str
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    snapshot: dict = field(default_factory=dict)
    error: Optional[str] = None
    stages_completed: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True if the core pipeline (indicators + RS + scoring) succeeded."""
        return self.error is None and not self.df.empty

    @property
    def has_signals(self) -> bool:
        """True if signal generation ran successfully."""
        return "signals" in self.stages_completed

    @property
    def has_sector(self) -> bool:
        """True if sector context was merged."""
        return "sector" in self.stages_completed


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_ticker(
    ticker: str,
    ohlcv: pd.DataFrame,
    bench_df: pd.DataFrame,
    *,
    breadth: pd.DataFrame | None = None,
    breadth_scores: pd.Series | None = None,
    sector_rs: pd.DataFrame | None = None,
    sector_name: str | None = None,
    as_of: pd.Timestamp | None = None,
) -> TickerResult:
    """
    Full CASH pipeline for a single ticker.

    Parameters
    ----------
    ticker : str
        Symbol, e.g. "AAPL".
    ohlcv : pd.DataFrame
        Columns: open, high, low, close, volume.
        DatetimeIndex sorted ascending.
    bench_df : pd.DataFrame
        Benchmark OHLCV (SPY), same format.  Must have at
        least a ``close`` column and DatetimeIndex.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()`` — universe-level
        breadth with ``breadth_regime`` column.  Used by the
        signal gate (Gate 3) and position-size scaling.
    breadth_scores : pd.Series, optional
        This ticker's column from ``breadth_to_pillar_scores()``
        — daily breadth score on 0–100 scale for Pillar 5 of
        the composite score.
    sector_rs : pd.DataFrame, optional
        Output of ``compute_all_sector_rs()`` — MultiIndex
        (date, sector).  Used for sector tailwind/headwind
        adjustments to the composite score.
    sector_name : str, optional
        Sector label matching a key in ``SECTOR_ETFS`` (e.g.
        ``"Technology"``).  If None, auto-looked up from
        ``TICKER_SECTOR_MAP`` in config.
    as_of : pd.Timestamp, optional
        Cut-off date — everything after is excluded.  Used by
        the backtester; None means use all available data.

    Returns
    -------
    TickerResult
        .df        — enriched DataFrame (empty on error)
        .snapshot  — latest-row summary dict
        .error     — error message or None
        .stages_completed — list of completed stages

    Notes
    -----
    Stages 1–3 (indicators, RS, scoring) are **required** —
    failure in any of these produces an error result.

    Stages 4–5 (sector merge, signal generation) are
    **optional** — failure degrades gracefully.  The ticker
    will still appear in rankings and backtests, just without
    sector adjustments or per-ticker signal gates.

    The ``compute_all_rs()`` call aligns the stock and
    benchmark on common trading dates, so the returned
    DataFrame may have fewer rows than the input.
    """
    stages: list[str] = []

    # ── 0. Date slice ─────────────────────────────────────────
    df = ohlcv.copy()
    bench = bench_df.copy()

    if as_of is not None:
        df = df.loc[:as_of]
        bench = bench.loc[:as_of]

    if len(df) < _MIN_BARS:
        return TickerResult(
            ticker=ticker,
            error=(
                f"Insufficient data: need ≥ {_MIN_BARS} bars, "
                f"got {len(df)}"
            ),
        )

    # ── 1. Technical indicators (REQUIRED) ────────────────────
    #
    #    Adds ~30 columns: returns, RSI, MACD, ADX, moving
    #    averages, ATR, realized vol, OBV, A/D line, volume
    #    metrics, Amihud illiquidity, VWAP distance.
    #
    try:
        df = compute_all_indicators(df)
        stages.append("indicators")
    except (ValueError, KeyError) as e:
        return TickerResult(
            ticker=ticker,
            error=f"Stage 1 (indicators) failed: {e}",
            stages_completed=stages,
        )

    # ── 2. Relative strength vs benchmark (REQUIRED) ──────────
    #
    #    Adds: rs_raw, rs_ema, rs_sma, rs_slope, rs_zscore,
    #    rs_momentum, rs_rel_volume, rs_vol_confirmed, rs_regime.
    #
    #    Also aligns stock and benchmark on common dates — the
    #    returned DataFrame may be shorter than the input.
    #
    try:
        df = compute_all_rs(df, bench)
        stages.append("relative_strength")
    except ValueError as e:
        return TickerResult(
            ticker=ticker,
            error=f"Stage 2 (RS) failed: {e}",
            stages_completed=stages,
        )

    # ── 3. Composite scoring (REQUIRED) ───────────────────────
    #
    #    Adds: score_rotation, score_momentum, score_volatility,
    #    score_microstructure, score_composite, score_percentile.
    #    Optionally score_breadth if breadth_scores provided.
    #
    try:
        df = compute_composite_score(df, breadth_scores)
        stages.append("scoring")
    except (ValueError, KeyError) as e:
        return TickerResult(
            ticker=ticker,
            error=f"Stage 3 (scoring) failed: {e}",
            stages_completed=stages,
        )

    # ── 4. Sector context (OPTIONAL) ──────────────────────────
    #
    #    Adds: sect_rs_zscore, sect_rs_regime, sect_rs_rank,
    #    sect_rs_pctrank, sector_tailwind, sector_name.
    #    If score_composite exists, also creates score_adjusted.
    #
    #    Degrades gracefully — ticker is still scored without
    #    sector adjustments.
    #
    resolved_sector = sector_name or TICKER_SECTOR_MAP.get(ticker)

    if sector_rs is not None and resolved_sector is not None:
        try:
            df = merge_sector_context(df, sector_rs, resolved_sector)
            stages.append("sector")
            logger.debug(
                f"{ticker}: sector context merged "
                f"({resolved_sector})"
            )
        except (ValueError, KeyError) as e:
            # Sector data missing or sector name not in panel —
            # continue without sector adjustments
            logger.debug(
                f"{ticker}: sector merge skipped — {e}"
            )
            # Still tag the sector name for downstream grouping
            df["sector_name"] = resolved_sector
    else:
        # No sector RS data provided or ticker not in map —
        # tag what we know
        if resolved_sector:
            df["sector_name"] = resolved_sector

    # ── 5. Entry / exit signals (OPTIONAL) ────────────────────
    #
    #    Adds: sig_regime_ok, sig_sector_ok, sig_breadth_ok,
    #    sig_momentum_ok, sig_in_cooldown, sig_confirmed,
    #    sig_exit, sig_position_pct, sig_reason.
    #
    #    Requires RS and sector columns to be present.
    #    Degrades gracefully — ticker appears in rankings
    #    without per-ticker quality gates.
    #
    try:
        df = generate_signals(df, breadth)
        stages.append("signals")
    except Exception as e:
        logger.warning(
            f"{ticker}: signal generation failed — {e}.  "
            f"Ticker will still be scored and ranked."
        )

    # ── 6. Build snapshot ─────────────────────────────────────
    snapshot = _build_snapshot(ticker, df)

    logger.debug(
        f"{ticker}: pipeline complete — "
        f"{len(df)} bars, "
        f"stages={stages}, "
        f"composite={snapshot.get('composite', 0):.3f}"
    )

    return TickerResult(
        ticker=ticker,
        df=df,
        snapshot=snapshot,
        stages_completed=stages,
    )


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT BUILDER
# ═══════════════════════════════════════════════════════════════

def _build_snapshot(ticker: str, df: pd.DataFrame) -> dict:
    """
    Extract latest-row values into a flat dict.

    Used by the orchestrator for quick-access summaries and by
    the report generators for display.  Downstream modules may
    enrich this with allocation fields (shares, dollar_alloc,
    weight_pct, category, bucket, themes).

    The snapshot includes nested dicts ``sub_scores``,
    ``indicators``, and ``rs`` for compatibility with the
    recommendation report format.
    """
    if df.empty:
        return {"ticker": ticker, "error": "empty DataFrame"}

    last = df.iloc[-1]
    close = _f(last, "close", 0.0)
    date = df.index[-1]

    # ── ATR-based stop ────────────────────────────────────────
    atr_period = INDICATOR_PARAMS["atr_period"]
    atr_col = f"atr_{atr_period}"
    atr_pct_col = f"{atr_col}_pct"
    atr_val = _f(last, atr_col, 0.0)

    stop_price = None
    risk_per_share = None
    if close > 0 and atr_val > 0:
        stop_price = round(close - ATR_STOP_MULT * atr_val, 4)
        risk_per_share = round(ATR_STOP_MULT * atr_val, 4)

    # ── Best available composite score ────────────────────────
    # score_adjusted includes sector tailwind; falls back to
    # score_composite if sector merge didn't run.
    composite = _f(last, "score_adjusted", None)
    if composite is None:
        composite = _f(last, "score_composite", 0.0)

    # ── Simplified per-ticker signal ──────────────────────────
    # This is NOT the final portfolio signal (that comes from
    # output/signals.py cross-sectional logic).  This is the
    # per-ticker quality assessment from strategy/signals.py.
    sig_confirmed = _i(last, "sig_confirmed", 0)
    sig_exit = _i(last, "sig_exit", 0)

    if sig_exit:
        action = "SELL"
    elif sig_confirmed:
        action = "BUY"
    else:
        action = "HOLD"

    # ── Confidence proxy ──────────────────────────────────────
    # Map composite [0.5, 1.0] → confidence [0.0, 1.0].
    # Below 0.5 → 0 confidence.
    confidence = max(0.0, min(1.0, (composite - 0.5) * 2.0))

    # ── Indicator column names from config ────────────────────
    rsi_col = f"rsi_{INDICATOR_PARAMS['rsi_period']}"
    adx_col = f"adx_{INDICATOR_PARAMS['adx_period']}"
    vol_col = f"realized_vol_{INDICATOR_PARAMS['realized_vol_window']}d"

    return {
        # ── Identity ──────────────────────────────────────
        "ticker":       ticker,
        "date":         date,
        "close":        round(close, 4),

        # ── Composite score (best available) ──────────────
        "composite":    round(composite, 4),
        "confidence":   round(confidence, 4),
        "signal":       action,

        # ── Pillar scores ─────────────────────────────────
        "score_composite":      round(_f(last, "score_composite", 0), 4),
        "score_adjusted":       round(_f(last, "score_adjusted", 0), 4) if "score_adjusted" in df.columns else None,
        "score_rotation":       round(_f(last, "score_rotation", 0), 4),
        "score_momentum":       round(_f(last, "score_momentum", 0), 4),
        "score_volatility":     round(_f(last, "score_volatility", 0), 4),
        "score_microstructure": round(_f(last, "score_microstructure", 0), 4),
        "score_breadth":        round(_f(last, "score_breadth", 0), 4) if "score_breadth" in df.columns else None,
        "score_percentile":     round(_f(last, "score_percentile", 0), 4),

        # ── Sub-scores (recommendations.py compatibility) ─
        "sub_scores": {
            "trend":        round(_f(last, "score_rotation", 0), 4),
            "momentum":     round(_f(last, "score_momentum", 0), 4),
            "volatility":   round(_f(last, "score_volatility", 0), 4),
            "rel_strength": round(_f(last, "score_rotation", 0), 4),
        },

        # ── Relative strength ─────────────────────────────
        "rs": {
            "rs_ratio":      round(_f(last, "rs_raw", 1.0), 6),
            "rs_percentile": round(_f(last, "score_percentile", 0), 4),
            "rs_regime":     _s(last, "rs_regime", "unknown"),
            "rs_zscore":     round(_f(last, "rs_zscore", 0), 4),
            "rs_momentum":   round(_f(last, "rs_momentum", 0), 6),
        },
        "rs_regime":        _s(last, "rs_regime", "unknown"),
        "rs_zscore":        round(_f(last, "rs_zscore", 0), 4),

        # ── Key indicators ────────────────────────────────
        "indicators": {
            "rsi":             round(_f(last, rsi_col, 50), 2),
            "adx":             round(_f(last, adx_col, 0), 2),
            "macd_line":       round(_f(last, "macd_line", 0), 4),
            "macd_signal":     round(_f(last, "macd_signal", 0), 4),
            "macd_hist":       round(_f(last, "macd_hist", 0), 4),
            "atr":             round(atr_val, 4),
            "atr_pct":         round(_f(last, atr_pct_col, 0), 4),
            "realized_vol":    round(_f(last, vol_col, 0), 4),
            "relative_volume": round(_f(last, "relative_volume", 1), 4),
            "obv_slope":       round(_f(last, "obv_slope_10d", 0), 4),
        },

        # ── Sector context ────────────────────────────────
        "sector_name":     _s(last, "sector_name", None),
        "sect_rs_regime":  _s(last, "sect_rs_regime", None),
        "sect_rs_rank":    _f(last, "sect_rs_rank", None),
        "sector_tailwind": round(_f(last, "sector_tailwind", 0), 4) if "sector_tailwind" in df.columns else 0.0,

        # ── Per-ticker signal gates ───────────────────────
        "sig_confirmed":    sig_confirmed,
        "sig_exit":         sig_exit,
        "sig_reason":       _s(last, "sig_reason", "no_signal"),
        "sig_position_pct": round(_f(last, "sig_position_pct", 0), 4) if "sig_position_pct" in df.columns else 0.0,

        # ── Risk ──────────────────────────────────────────
        "stop_price":      stop_price,
        "risk_per_share":  risk_per_share,

        # ── Metadata (enriched by orchestrator) ───────────
        "bars_used":           len(df),
        "breadth_available":   bool(_f(last, "breadth_available", False)) if "breadth_available" in df.columns else False,
        "category":            None,     # set by orchestrator
        "bucket":              None,     # set by orchestrator
        "themes":              [],       # set by orchestrator
        "shares":              None,     # set by orchestrator
        "dollar_alloc":        None,     # set by orchestrator
        "weight_pct":          None,     # set by orchestrator
    }


# ═══════════════════════════════════════════════════════════════
#  VALUE EXTRACTION HELPERS
# ═══════════════════════════════════════════════════════════════

def _f(row: pd.Series, col: str, default: float | None = 0.0) -> float | None:
    """
    Extract a float value from a pandas Series row.

    Handles NaN, None, and missing columns gracefully.
    Returns ``default`` when the value is missing or not numeric.
    """
    val = row.get(col)
    if val is None:
        return default
    try:
        fval = float(val)
        if np.isnan(fval):
            return default
        return fval
    except (TypeError, ValueError):
        return default


def _s(row: pd.Series, col: str, default: str | None = "unknown") -> str | None:
    """
    Extract a string value from a pandas Series row.

    Handles NaN, None, and missing columns gracefully.
    Returns ``default`` when the value is missing.
    """
    val = row.get(col)
    if val is None:
        return default
    if isinstance(val, float) and np.isnan(val):
        return default
    return str(val)


def _i(row: pd.Series, col: str, default: int = 0) -> int:
    """Extract an int value from a pandas Series row."""
    val = row.get(col)
    if val is None:
        return default
    try:
        fval = float(val)
        if np.isnan(fval):
            return default
        return int(fval)
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════
#  BATCH RUNNER
# ═══════════════════════════════════════════════════════════════

def run_batch(
    universe: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    *,
    breadth: pd.DataFrame | None = None,
    breadth_scores_panel: pd.DataFrame | None = None,
    sector_rs: pd.DataFrame | None = None,
    as_of: pd.Timestamp | None = None,
    skip_benchmark: bool = True,
    benchmark_ticker: str = "SPY",
    progress: bool = True,
) -> dict[str, TickerResult]:
    """
    Run the single-ticker pipeline for every ticker in the
    universe.

    This is a convenience wrapper around ``run_ticker()`` that
    handles breadth-score extraction and progress logging.  The
    orchestrator may call this directly or implement its own
    loop with additional enrichment.

    Parameters
    ----------
    universe : dict
        {ticker: OHLCV DataFrame} for all symbols.
    bench_df : pd.DataFrame
        Benchmark OHLCV (e.g. SPY).
    breadth : pd.DataFrame, optional
        Universe-level breadth from ``compute_all_breadth()``.
    breadth_scores_panel : pd.DataFrame, optional
        Output of ``breadth_to_pillar_scores()`` — DataFrame
        with columns = tickers, values = 0–100 daily scores.
        Each ticker gets its own column as ``breadth_scores``
        argument to ``run_ticker()``.
    sector_rs : pd.DataFrame, optional
        Output of ``compute_all_sector_rs()`` — MultiIndex
        (date, sector) panel.
    as_of : pd.Timestamp, optional
        Cut-off date for backtesting.
    skip_benchmark : bool
        If True, skip the benchmark ticker (e.g. SPY) to avoid
        computing RS of SPY vs itself.  Default True.
    benchmark_ticker : str
        Benchmark symbol to skip.  Default ``"SPY"``.
    progress : bool
        Log progress every 10 tickers.  Default True.

    Returns
    -------
    dict[str, TickerResult]
        {ticker: TickerResult} for every processed ticker.
    """
    results: dict[str, TickerResult] = {}
    total = len(universe)
    ok = 0
    errors = 0
    skipped = 0

    for i, (ticker, ohlcv) in enumerate(universe.items(), 1):

        # Skip benchmark (RS of SPY vs SPY is meaningless)
        if skip_benchmark and ticker == benchmark_ticker:
            skipped += 1
            continue

        if progress and (i % 10 == 0 or i == 1 or i == total):
            logger.info(
                f"  [{i}/{total}] {ticker}..."
            )

        # Extract per-ticker breadth score Series if available
        b_scores = None
        if (
            breadth_scores_panel is not None
            and ticker in breadth_scores_panel.columns
        ):
            b_scores = breadth_scores_panel[ticker]

        result = run_ticker(
            ticker=ticker,
            ohlcv=ohlcv,
            bench_df=bench_df,
            breadth=breadth,
            breadth_scores=b_scores,
            sector_rs=sector_rs,
            as_of=as_of,
        )

        results[ticker] = result

        if result.ok:
            ok += 1
        else:
            errors += 1
            logger.debug(f"  {ticker}: {result.error}")

    logger.info(
        f"Batch complete: {ok} succeeded, "
        f"{errors} errors, {skipped} skipped "
        f"(of {total} total)"
    )

    return results


# ═══════════════════════════════════════════════════════════════
#  RESULT EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def results_to_scored_universe(
    results: dict[str, TickerResult],
) -> dict[str, pd.DataFrame]:
    """
    Extract ``{ticker: enriched_df}`` from results, keeping
    only successful tickers.

    The returned dict is ready to pass directly to:
      - ``output.rankings.compute_all_rankings()``
      - ``strategy.portfolio.build_portfolio()``
    """
    return {
        ticker: r.df
        for ticker, r in results.items()
        if r.ok
    }


def results_to_snapshots(
    results: dict[str, TickerResult],
) -> list[dict]:
    """
    Extract snapshot dicts from results, keeping only
    successful tickers.  Sorted by composite score descending.

    Useful for feeding into the recommendation report.
    """
    snapshots = [
        r.snapshot
        for r in results.values()
        if r.ok
    ]
    snapshots.sort(
        key=lambda s: s.get("composite", 0),
        reverse=True,
    )
    return snapshots


def results_errors(
    results: dict[str, TickerResult],
) -> list[str]:
    """
    Collect error messages from failed tickers.

    Returns list of ``"TICKER: error message"`` strings.
    """
    return [
        f"{ticker}: {r.error}"
        for ticker, r in results.items()
        if r.error is not None
    ]