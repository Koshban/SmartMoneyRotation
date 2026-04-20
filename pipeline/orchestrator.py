"""
pipeline/orchestrator.py
------------------------
Top-level coordinator for the CASH system.

Ties together every phase of analysis into a single call or
a phase-by-phase interactive workflow:

  Phase 0 — Data Loading
      Load OHLCV for all tickers + benchmark from the
      configured data source.

  Phase 1 — Universe-Level Computations
      Breadth indicators, sector relative strength, and
      breadth-to-pillar-score mapping.  These feed into
      the per-ticker pipeline as contextual inputs.

  Phase 2 — Per-Ticker Pipeline
      Run ``runner.run_batch()`` which chains indicators →
      RS → scoring → sector merge → signals for each ticker.

  Phase 3 — Cross-Sectional Analysis
      Rankings across the scored universe, portfolio
      construction with position sizing, and portfolio-level
      signal reconciliation.

  Phase 4 — Reporting
      Generate recommendation report, weekly summary, and
      optional backtest results.

The orchestrator can be run end-to-end via
``run_full_pipeline()`` or phase-by-phase for interactive /
notebook use via the ``Orchestrator`` class.

Typical Usage
─────────────
  # One-shot (CLI / cron)
  result = run_full_pipeline()

  # Interactive (notebook)
  orch = Orchestrator()
  orch.load_data()
  orch.compute_universe_context()
  orch.run_tickers()
  orch.cross_sectional_analysis()
  report = orch.generate_reports()

Dependencies
────────────
  pipeline/runner.py              — single-ticker pipeline
  compute/breadth.py              — universe breadth
  compute/sectors.py              — sector RS panel
  output/rankings.py              — cross-sectional rankings
  output/signals.py               — portfolio-level signals
  strategy/portfolio.py           — position sizing & allocation
  strategy/backtest.py            — historical backtest
  reports/recommendations.py      — ticker recommendations
  reports/weekly_report.py        — weekly summary
  data/loader.py                  — OHLCV data loading
  common/config.py                — all parameters
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SECTOR_ETFS,
    TICKER_SECTOR_MAP,
    UNIVERSE,
)
from compute.breadth import (
    breadth_to_pillar_scores,
    compute_all_breadth,
)
from compute.sector_rs import compute_all_sector_rs
from data.loader import load_ohlcv
from pipeline.runner import (
    TickerResult,
    results_errors,
    results_to_scored_universe,
    results_to_snapshots,
    run_batch,
    run_ticker,
)
from output.rankings import compute_all_rankings
from output.signals import reconcile_signals
from reports.recommendations import build_recommendation_report
from reports.weekly_report import build_weekly_report
from strategy.backtest import run_backtest
from strategy.portfolio import build_portfolio

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  PIPELINE RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class PipelineResult:
    """
    Complete output of a full pipeline run.

    Every downstream consumer (CLI, web dashboard, notebook,
    report generator) reads from this single object.

    Attributes
    ----------
    ticker_results : dict[str, TickerResult]
        Per-ticker enriched DataFrames and snapshots.
    scored_universe : dict[str, pd.DataFrame]
        ``{ticker: enriched_df}`` for successful tickers only.
        Ready for rankings and portfolio construction.
    snapshots : list[dict]
        Latest-row summaries sorted by composite score
        descending.  Each dict is one ticker's snapshot from
        ``TickerResult.snapshot``, enriched with allocation
        fields after portfolio construction.
    rankings : pd.DataFrame
        Cross-sectional rankings DataFrame with composite,
        pillar, and percentile rank columns for every ticker.
    portfolio : dict
        Output of ``build_portfolio()`` — allocation table,
        position sizes, cash reserve, metadata.
    signals : pd.DataFrame
        Reconciled portfolio-level signals combining per-ticker
        quality gates with cross-sectional ranking filters.
    breadth : pd.DataFrame or None
        Universe-level breadth indicators.
    breadth_scores : pd.DataFrame or None
        Per-ticker breadth pillar scores (columns = tickers).
    sector_rs : pd.DataFrame or None
        Sector RS panel (MultiIndex: date × sector).
    recommendation_report : dict or None
        Structured recommendation report for display.
    weekly_report : dict or None
        Structured weekly summary report.
    backtest : dict or None
        Backtest results if requested.
    errors : list[str]
        Error messages from failed tickers.
    timings : dict[str, float]
        Wall-clock seconds for each phase.
    run_date : pd.Timestamp
        Date of this pipeline run.
    as_of : pd.Timestamp or None
        Cut-off date if backtesting; None for live.
    """

    ticker_results: dict[str, TickerResult] = field(default_factory=dict)
    scored_universe: dict[str, pd.DataFrame] = field(default_factory=dict)
    snapshots: list[dict] = field(default_factory=list)
    rankings: pd.DataFrame = field(default_factory=pd.DataFrame)
    portfolio: dict = field(default_factory=dict)
    signals: pd.DataFrame = field(default_factory=pd.DataFrame)

    breadth: Optional[pd.DataFrame] = None
    breadth_scores: Optional[pd.DataFrame] = None
    sector_rs: Optional[pd.DataFrame] = None

    recommendation_report: Optional[dict] = None
    weekly_report: Optional[dict] = None
    backtest: Optional[dict] = None

    errors: list[str] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    run_date: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    as_of: Optional[pd.Timestamp] = None

    @property
    def n_tickers(self) -> int:
        """Number of successfully scored tickers."""
        return len(self.scored_universe)

    @property
    def n_errors(self) -> int:
        """Number of tickers that failed."""
        return len(self.errors)

    @property
    def total_time(self) -> float:
        """Total wall-clock time in seconds."""
        return sum(self.timings.values())

    def top_n(self, n: int = 10) -> list[dict]:
        """Return top-N snapshots by composite score."""
        return self.snapshots[:n]

    def summary(self) -> str:
        """One-line summary string for logging / display."""
        top = self.snapshots[0]["ticker"] if self.snapshots else "N/A"
        return (
            f"CASH Pipeline — {self.run_date.strftime('%Y-%m-%d')} — "
            f"{self.n_tickers} tickers scored, "
            f"{self.n_errors} errors, "
            f"top={top}, "
            f"{self.total_time:.1f}s"
        )


# ═══════════════════════════════════════════════════════════════
#  ORCHESTRATOR CLASS  (phase-by-phase control)
# ═══════════════════════════════════════════════════════════════

class Orchestrator:
    """
    Stateful pipeline coordinator.

    Use this when you want fine-grained control over each
    phase (e.g. in a notebook, or when injecting custom data).
    For one-shot usage, prefer ``run_full_pipeline()``.

    The orchestrator accumulates state across phases and
    produces a ``PipelineResult`` at the end.

    Example
    -------
    ::

        orch = Orchestrator(capital=100_000)
        orch.load_data()
        orch.compute_universe_context()
        orch.run_tickers()
        orch.cross_sectional_analysis()
        result = orch.generate_reports()

    Each phase can be re-run independently (e.g. to test
    different portfolio parameters without re-computing
    indicators).
    """

    def __init__(
        self,
        *,
        universe: list[str] | None = None,
        benchmark: str | None = None,
        capital: float | None = None,
        as_of: pd.Timestamp | None = None,
        enable_breadth: bool = True,
        enable_sectors: bool = True,
        enable_signals: bool = True,
        enable_backtest: bool = False,
        backtest_start: pd.Timestamp | None = None,
    ):
        """
        Parameters
        ----------
        universe : list[str], optional
            Ticker symbols.  Defaults to ``UNIVERSE`` from config.
        benchmark : str, optional
            Benchmark ticker.  Defaults to ``BENCHMARK_TICKER``.
        capital : float, optional
            Portfolio capital for position sizing.  Defaults to
            ``PORTFOLIO_PARAMS["total_capital"]``.
        as_of : pd.Timestamp, optional
            Cut-off date for point-in-time analysis.  None means
            use all available data (live mode).
        enable_breadth : bool
            Compute universe breadth (Pillar 5, Gate 3).
        enable_sectors : bool
            Compute sector RS and merge tailwind adjustments.
        enable_signals : bool
            Run per-ticker and portfolio-level signal generation.
        enable_backtest : bool
            Run historical backtest after portfolio construction.
        backtest_start : pd.Timestamp, optional
            Start date for backtest.  Defaults to 1 year before
            the earliest data date.
        """
        # ── Configuration ─────────────────────────────────────
        self.tickers: list[str] = universe or list(UNIVERSE)
        self.benchmark: str = benchmark or BENCHMARK_TICKER
        self.capital: float = capital or PORTFOLIO_PARAMS["total_capital"]
        self.as_of: pd.Timestamp | None = as_of

        self.enable_breadth: bool = enable_breadth
        self.enable_sectors: bool = enable_sectors
        self.enable_signals: bool = enable_signals
        self.enable_backtest: bool = enable_backtest
        self.backtest_start: pd.Timestamp | None = backtest_start

        # ── Mutable state (populated phase by phase) ──────────
        self._ohlcv: dict[str, pd.DataFrame] = {}
        self._bench_df: pd.DataFrame = pd.DataFrame()
        self._breadth: pd.DataFrame | None = None
        self._breadth_scores: pd.DataFrame | None = None
        self._sector_rs: pd.DataFrame | None = None
        self._ticker_results: dict[str, TickerResult] = {}
        self._scored_universe: dict[str, pd.DataFrame] = {}
        self._snapshots: list[dict] = []
        self._rankings: pd.DataFrame = pd.DataFrame()
        self._portfolio: dict = {}
        self._signals: pd.DataFrame = pd.DataFrame()
        self._recommendation_report: dict | None = None
        self._weekly_report: dict | None = None
        self._backtest: dict | None = None

        self._timings: dict[str, float] = {}
        self._phases_completed: list[str] = []

    # ───────────────────────────────────────────────────────
    #  Phase 0 — Data Loading
    # ───────────────────────────────────────────────────────

    def load_data(
        self,
        preloaded: dict[str, pd.DataFrame] | None = None,
        bench_df: pd.DataFrame | None = None,
    ) -> None:
        """
        Load OHLCV data for all tickers and the benchmark.

        Parameters
        ----------
        preloaded : dict, optional
            ``{ticker: OHLCV DataFrame}`` to skip data loading.
            Useful when data is already in memory (tests,
            notebooks, backtester warm-start).
        bench_df : pd.DataFrame, optional
            Pre-loaded benchmark OHLCV.  If None and not in
            ``preloaded``, loaded via ``load_ohlcv()``.

        After this call, ``self._ohlcv`` and ``self._bench_df``
        are populated.
        """
        t0 = time.perf_counter()

        if preloaded is not None:
            self._ohlcv = preloaded
            logger.info(
                f"Phase 0: Using {len(preloaded)} pre-loaded "
                f"ticker DataFrames"
            )
        else:
            # All tickers + benchmark in one call if the loader
            # supports batch, otherwise iterate.
            all_symbols = list(set(self.tickers + [self.benchmark]))
            self._ohlcv = _load_universe(all_symbols)
            logger.info(
                f"Phase 0: Loaded {len(self._ohlcv)} tickers "
                f"from data source"
            )

        # ── Extract or load benchmark ─────────────────────────
        if bench_df is not None:
            self._bench_df = bench_df
        elif self.benchmark in self._ohlcv:
            self._bench_df = self._ohlcv[self.benchmark]
        else:
            self._bench_df = load_ohlcv(self.benchmark)

        if self._bench_df.empty:
            raise ValueError(
                f"Benchmark {self.benchmark} has no data. "
                f"Cannot proceed."
            )

        elapsed = time.perf_counter() - t0
        self._timings["load_data"] = elapsed
        self._phases_completed.append("load_data")
        logger.info(
            f"Phase 0 complete: {len(self._ohlcv)} tickers, "
            f"benchmark={self.benchmark} "
            f"({len(self._bench_df)} bars), "
            f"{elapsed:.1f}s"
        )

    # ───────────────────────────────────────────────────────
    #  Phase 1 — Universe-Level Context
    # ───────────────────────────────────────────────────────

    def compute_universe_context(self) -> None:
        """
        Compute universe-level breadth and sector RS.

        These are contextual inputs to the per-ticker pipeline:
        breadth feeds Pillar 5 (scoring) and Gate 3 (signals),
        sector RS feeds sector tailwind adjustments (scoring)
        and Gate 2 (signals).

        Both are optional — if disabled or if computation fails,
        the per-ticker pipeline degrades gracefully.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        # ── Breadth ───────────────────────────────────────────
        #
        # Universe breadth measures the internal health of the
        # market: advance/decline ratios, % above moving
        # averages, new highs vs lows, McClellan oscillator.
        #
        # From this we derive:
        #   _breadth       — DataFrame with daily breadth cols
        #   _breadth_scores — per-ticker daily scores (0–100)
        #                     for Pillar 5 of composite scoring
        #
        if self.enable_breadth:
            try:
                close_panel = _extract_close_panel(self._ohlcv)
                self._breadth = compute_all_breadth(close_panel)
                self._breadth_scores = breadth_to_pillar_scores(
                    self._breadth
                )
                logger.info(
                    f"Phase 1: Breadth computed — "
                    f"{len(self._breadth)} bars, "
                    f"regime={self._breadth['breadth_regime'].iloc[-1]}"
                )
            except Exception as e:
                logger.warning(
                    f"Phase 1: Breadth computation failed — {e}.  "
                    f"Proceeding without breadth context."
                )
                self._breadth = None
                self._breadth_scores = None
        else:
            logger.info("Phase 1: Breadth disabled — skipping")

        # ── Sector RS ─────────────────────────────────────────
        #
        # Sector RS measures the relative strength of each
        # sector ETF vs the benchmark.  Produces a MultiIndex
        # (date × sector) panel with rs_zscore, rs_regime,
        # rs_rank, rs_pctrank.
        #
        # The per-ticker pipeline uses this to add sector
        # tailwind/headwind adjustments to the composite score
        # and to gate entries in weak sectors.
        #
        if self.enable_sectors:
            try:
                sector_ohlcv = _extract_sector_ohlcv(self._ohlcv)
                if sector_ohlcv:
                    self._sector_rs = compute_all_sector_rs(
                        sector_ohlcv, self._bench_df
                    )
                    logger.info(
                        f"Phase 1: Sector RS computed — "
                        f"{len(sector_ohlcv)} sectors"
                    )
                else:
                    logger.info(
                        "Phase 1: No sector ETFs found in "
                        "universe — skipping sector RS"
                    )
            except Exception as e:
                logger.warning(
                    f"Phase 1: Sector RS computation failed — "
                    f"{e}.  Proceeding without sector context."
                )
                self._sector_rs = None
        else:
            logger.info("Phase 1: Sectors disabled — skipping")

        elapsed = time.perf_counter() - t0
        self._timings["universe_context"] = elapsed
        self._phases_completed.append("universe_context")
        logger.info(f"Phase 1 complete: {elapsed:.1f}s")

    # ───────────────────────────────────────────────────────
    #  Phase 2 — Per-Ticker Pipeline
    # ───────────────────────────────────────────────────────

    def run_tickers(self) -> None:
        """
        Run the single-ticker pipeline for every ticker.

        Calls ``runner.run_batch()`` which chains:
          indicators → RS → scoring → sector → signals
        for each ticker.  Results are stored in
        ``self._ticker_results``.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        self._ticker_results = run_batch(
            universe=self._ohlcv,
            bench_df=self._bench_df,
            breadth=self._breadth,
            breadth_scores_panel=self._breadth_scores,
            sector_rs=self._sector_rs,
            as_of=self.as_of,
            skip_benchmark=True,
            benchmark_ticker=self.benchmark,
        )

        # Extract convenience views
        self._scored_universe = results_to_scored_universe(
            self._ticker_results
        )
        self._snapshots = results_to_snapshots(self._ticker_results)

        elapsed = time.perf_counter() - t0
        self._timings["run_tickers"] = elapsed
        self._phases_completed.append("run_tickers")

        n_ok = len(self._scored_universe)
        n_err = len(results_errors(self._ticker_results))
        logger.info(
            f"Phase 2 complete: {n_ok} scored, "
            f"{n_err} errors, {elapsed:.1f}s"
        )

    # ───────────────────────────────────────────────────────
    #  Phase 3 — Cross-Sectional Analysis
    # ───────────────────────────────────────────────────────

    def cross_sectional_analysis(self) -> None:
        """
        Rank the scored universe, build the portfolio, and
        reconcile signals.

        This phase operates across all tickers simultaneously
        (cross-sectional), unlike Phase 2 which is per-ticker.

        Sub-phases
        ──────────
        3a. Rankings
            Rank every ticker by composite score, pillar
            scores, and RS metrics.  Produces a DataFrame with
            rank and percentile-rank columns.

        3b. Portfolio Construction
            Select top-N tickers, size positions using ATR-
            based risk parity, enforce concentration limits,
            and compute dollar allocations.

        3c. Signal Reconciliation
            Combine per-ticker quality gates (from Phase 2)
            with cross-sectional ranking filters to produce
            final BUY / HOLD / SELL signals for each ticker
            in the portfolio.

        After this phase, ``self._snapshots`` are enriched
        with allocation fields (shares, dollar_alloc,
        weight_pct, category, bucket).
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        if not self._scored_universe:
            logger.warning(
                "Phase 3: No scored tickers — skipping "
                "cross-sectional analysis"
            )
            self._phases_completed.append("cross_sectional")
            return

        # ── 3a. Rankings ──────────────────────────────────────
        #
        # Cross-sectional percentile ranks for composite and
        # each pillar score.  Rankings are date-aligned — each
        # row is one ticker's latest values with rank columns.
        #
        try:
            self._rankings = compute_all_rankings(
                self._scored_universe
            )
            logger.info(
                f"Phase 3a: Rankings computed for "
                f"{len(self._rankings)} tickers"
            )
        except Exception as e:
            logger.warning(f"Phase 3a: Rankings failed — {e}")
            self._rankings = pd.DataFrame()

        # ── 3b. Portfolio Construction ────────────────────────
        #
        # Select top-ranked tickers, allocate capital using
        # ATR-based risk parity with concentration limits.
        #
        # The portfolio dict contains:
        #   allocations   — list of {ticker, shares, dollar_alloc,
        #                   weight_pct, stop_price, risk_per_share}
        #   cash_reserve  — unallocated capital
        #   metadata      — total_capital, n_positions, etc.
        #
        try:
            self._portfolio = build_portfolio(
                snapshots=self._snapshots,
                rankings=self._rankings,
                capital=self.capital,
                breadth=self._breadth,
            )
            logger.info(
                f"Phase 3b: Portfolio built — "
                f"{self._portfolio.get('metadata', {}).get('n_positions', 0)} "
                f"positions"
            )

            # Enrich snapshots with allocation fields
            _enrich_snapshots_with_allocations(
                self._snapshots, self._portfolio
            )
        except Exception as e:
            logger.warning(f"Phase 3b: Portfolio build failed — {e}")
            self._portfolio = {}

        # ── 3c. Signal Reconciliation ─────────────────────────
        #
        # Combine per-ticker gates with portfolio-level logic:
        #   - Only top-ranked tickers get BUY signals
        #   - Tickers dropping out of top-N get SELL
        #   - Cooldown enforcement across the portfolio
        #   - Position-size adjustments for breadth regime
        #
        if self.enable_signals:
            try:
                self._signals = reconcile_signals(
                    snapshots=self._snapshots,
                    rankings=self._rankings,
                    portfolio=self._portfolio,
                    breadth=self._breadth,
                )
                logger.info(
                    f"Phase 3c: Signals reconciled — "
                    f"{len(self._signals)} tickers"
                )

                # Update snapshot signals from reconciled output
                _enrich_snapshots_with_signals(
                    self._snapshots, self._signals
                )
            except Exception as e:
                logger.warning(
                    f"Phase 3c: Signal reconciliation failed — {e}"
                )
                self._signals = pd.DataFrame()
        else:
            logger.info("Phase 3c: Signals disabled — skipping")

        elapsed = time.perf_counter() - t0
        self._timings["cross_sectional"] = elapsed
        self._phases_completed.append("cross_sectional")
        logger.info(f"Phase 3 complete: {elapsed:.1f}s")

    # ───────────────────────────────────────────────────────
    #  Phase 4 — Reports & Optional Backtest
    # ───────────────────────────────────────────────────────

    def generate_reports(self) -> PipelineResult:
        """
        Generate reports and assemble the final PipelineResult.

        This is the terminal phase.  It produces structured
        report dicts (recommendation + weekly) and optionally
        runs a historical backtest.

        Returns
        -------
        PipelineResult
            Complete pipeline output.  All downstream consumers
            (CLI, dashboard, export) read from this object.
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        # ── Recommendation Report ─────────────────────────────
        #
        # Structured output for the top-N recommended tickers
        # with scores, indicators, risk levels, and rationale.
        #
        try:
            self._recommendation_report = build_recommendation_report(
                snapshots=self._snapshots,
                rankings=self._rankings,
                portfolio=self._portfolio,
                breadth=self._breadth,
            )
            logger.info("Phase 4: Recommendation report built")
        except Exception as e:
            logger.warning(
                f"Phase 4: Recommendation report failed — {e}"
            )

        # ── Weekly Report ─────────────────────────────────────
        #
        # High-level summary: market regime, sector rotation,
        # breadth health, portfolio changes, risk metrics.
        #
        try:
            self._weekly_report = build_weekly_report(
                snapshots=self._snapshots,
                rankings=self._rankings,
                portfolio=self._portfolio,
                breadth=self._breadth,
                sector_rs=self._sector_rs,
            )
            logger.info("Phase 4: Weekly report built")
        except Exception as e:
            logger.warning(
                f"Phase 4: Weekly report failed — {e}"
            )

        # ── Backtest (optional) ───────────────────────────────
        #
        # Historical simulation using the same scoring and
        # signal logic.  Walks forward day-by-day, re-ranking
        # and re-allocating at each rebalance date.
        #
        if self.enable_backtest:
            try:
                self._backtest = run_backtest(
                    scored_universe=self._scored_universe,
                    bench_df=self._bench_df,
                    capital=self.capital,
                    start=self.backtest_start,
                )
                logger.info(
                    f"Phase 4: Backtest complete — "
                    f"CAGR={self._backtest.get('cagr', 0):.1%}"
                )
            except Exception as e:
                logger.warning(f"Phase 4: Backtest failed — {e}")

        elapsed = time.perf_counter() - t0
        self._timings["reports"] = elapsed
        self._phases_completed.append("reports")

        # ── Assemble PipelineResult ───────────────────────────
        errors = results_errors(self._ticker_results)

        result = PipelineResult(
            ticker_results=self._ticker_results,
            scored_universe=self._scored_universe,
            snapshots=self._snapshots,
            rankings=self._rankings,
            portfolio=self._portfolio,
            signals=self._signals,
            breadth=self._breadth,
            breadth_scores=self._breadth_scores,
            sector_rs=self._sector_rs,
            recommendation_report=self._recommendation_report,
            weekly_report=self._weekly_report,
            backtest=self._backtest,
            errors=errors,
            timings=self._timings,
            run_date=pd.Timestamp.now(),
            as_of=self.as_of,
        )

        logger.info(result.summary())
        return result

    # ───────────────────────────────────────────────────────
    #  Convenience: Run All Phases
    # ───────────────────────────────────────────────────────

    def run_all(
        self,
        preloaded: dict[str, pd.DataFrame] | None = None,
        bench_df: pd.DataFrame | None = None,
    ) -> PipelineResult:
        """
        Execute all phases in sequence and return the result.

        Equivalent to calling ``load_data()``,
        ``compute_universe_context()``, ``run_tickers()``,
        ``cross_sectional_analysis()``, and
        ``generate_reports()`` in order.
        """
        self.load_data(preloaded=preloaded, bench_df=bench_df)
        self.compute_universe_context()
        self.run_tickers()
        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Re-run Portfolio with Different Parameters
    # ───────────────────────────────────────────────────────

    def rebuild_portfolio(
        self,
        capital: float | None = None,
    ) -> PipelineResult:
        """
        Re-run Phase 3 + 4 without re-computing indicators.

        Useful for testing different capital amounts or
        portfolio parameters without the cost of Phase 1–2.
        """
        self._require_phase("run_tickers")

        if capital is not None:
            self.capital = capital

        # Reset downstream state
        self._snapshots = results_to_snapshots(self._ticker_results)
        self._rankings = pd.DataFrame()
        self._portfolio = {}
        self._signals = pd.DataFrame()
        self._recommendation_report = None
        self._weekly_report = None

        # Remove downstream phases from completed list
        self._phases_completed = [
            p for p in self._phases_completed
            if p in ("load_data", "universe_context", "run_tickers")
        ]

        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Single-Ticker Re-run
    # ───────────────────────────────────────────────────────

    def rerun_ticker(self, ticker: str) -> TickerResult:
        """
        Re-run the pipeline for a single ticker using the
        current universe context (breadth, sector RS).

        Useful for debugging or refreshing a single ticker
        without re-running the full batch.

        The result is also updated in ``self._ticker_results``.
        """
        self._require_phase("load_data")

        if ticker not in self._ohlcv:
            return TickerResult(
                ticker=ticker,
                error=f"No OHLCV data for {ticker}",
            )

        b_scores = None
        if (
            self._breadth_scores is not None
            and ticker in self._breadth_scores.columns
        ):
            b_scores = self._breadth_scores[ticker]

        result = run_ticker(
            ticker=ticker,
            ohlcv=self._ohlcv[ticker],
            bench_df=self._bench_df,
            breadth=self._breadth,
            breadth_scores=b_scores,
            sector_rs=self._sector_rs,
            as_of=self.as_of,
        )

        self._ticker_results[ticker] = result
        logger.info(
            f"Re-ran {ticker}: "
            f"{'OK' if result.ok else result.error}"
        )
        return result

    # ───────────────────────────────────────────────────────
    #  Internal Helpers
    # ───────────────────────────────────────────────────────

    def _require_phase(self, phase: str) -> None:
        """Raise if a prerequisite phase has not been completed."""
        if phase not in self._phases_completed:
            raise RuntimeError(
                f"Phase '{phase}' has not been run yet.  "
                f"Completed phases: {self._phases_completed}"
            )


# ═══════════════════════════════════════════════════════════════
#  ONE-SHOT ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_full_pipeline(
    *,
    universe: list[str] | None = None,
    benchmark: str | None = None,
    capital: float | None = None,
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, pd.DataFrame] | None = None,
    bench_df: pd.DataFrame | None = None,
    enable_breadth: bool = True,
    enable_sectors: bool = True,
    enable_signals: bool = True,
    enable_backtest: bool = False,
    backtest_start: pd.Timestamp | None = None,
) -> PipelineResult:
    """
    Run the full CASH pipeline end-to-end.

    This is the main entry point for CLI usage and scheduled
    jobs.  For interactive control, use the ``Orchestrator``
    class directly.

    Parameters
    ----------
    universe : list[str], optional
        Ticker symbols.  Defaults to ``UNIVERSE`` from config.
    benchmark : str, optional
        Benchmark ticker.  Defaults to ``BENCHMARK_TICKER``.
    capital : float, optional
        Portfolio capital.  Defaults to config value.
    as_of : pd.Timestamp, optional
        Point-in-time cut-off for backtesting.
    preloaded : dict, optional
        ``{ticker: OHLCV DataFrame}`` to skip data loading.
    bench_df : pd.DataFrame, optional
        Pre-loaded benchmark OHLCV.
    enable_breadth : bool
        Compute universe breadth.  Default True.
    enable_sectors : bool
        Compute sector RS.  Default True.
    enable_signals : bool
        Generate signals.  Default True.
    enable_backtest : bool
        Run historical backtest.  Default False.
    backtest_start : pd.Timestamp, optional
        Backtest start date.

    Returns
    -------
    PipelineResult
        Complete pipeline output.
    """
    orch = Orchestrator(
        universe=universe,
        benchmark=benchmark,
        capital=capital,
        as_of=as_of,
        enable_breadth=enable_breadth,
        enable_sectors=enable_sectors,
        enable_signals=enable_signals,
        enable_backtest=enable_backtest,
        backtest_start=backtest_start,
    )

    return orch.run_all(preloaded=preloaded, bench_df=bench_df)


# ═══════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═══════════════════════════════════════════════════════════════

def _load_universe(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """
    Load OHLCV for every symbol via ``data.loader.load_ohlcv``.

    Skips symbols that fail to load and logs warnings.
    Returns ``{ticker: DataFrame}`` for successfully loaded
    symbols.
    """
    loaded: dict[str, pd.DataFrame] = {}

    for sym in symbols:
        try:
            df = load_ohlcv(sym)
            if df is not None and not df.empty:
                loaded[sym] = df
            else:
                logger.warning(f"No data returned for {sym}")
        except Exception as e:
            logger.warning(f"Failed to load {sym}: {e}")

    return loaded


def _extract_close_panel(
    ohlcv: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Build a ``close`` price panel (columns = tickers) from
    the universe OHLCV dict.

    Used as input to ``compute_all_breadth()``.  Aligns all
    tickers on a common DatetimeIndex using forward-fill
    (max 5 days) to handle missing trading days.
    """
    close_dict = {}
    for ticker, df in ohlcv.items():
        if "close" in df.columns and not df.empty:
            close_dict[ticker] = df["close"]

    panel = pd.DataFrame(close_dict)
    panel.sort_index(inplace=True)
    panel.ffill(limit=5, inplace=True)
    return panel


def _extract_sector_ohlcv(
    ohlcv: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Extract OHLCV for sector ETFs that are present in the
    loaded universe.

    Returns ``{sector_name: OHLCV DataFrame}`` for each sector
    whose ETF ticker exists in the data.

    The ``SECTOR_ETFS`` config maps sector names to ETF tickers
    (e.g. ``{"Technology": "XLK", ...}``).
    """
    sector_data: dict[str, pd.DataFrame] = {}

    for sector_name, etf_ticker in SECTOR_ETFS.items():
        if etf_ticker in ohlcv:
            sector_data[sector_name] = ohlcv[etf_ticker]

    return sector_data


def _enrich_snapshots_with_allocations(
    snapshots: list[dict],
    portfolio: dict,
) -> None:
    """
    Merge portfolio allocation fields into ticker snapshots.

    Modifies snapshots in-place, adding: shares, dollar_alloc,
    weight_pct, category, bucket.

    Tickers not in the portfolio get zero allocations and
    category ``"not_selected"``.
    """
    allocations = portfolio.get("allocations", [])

    # Build lookup by ticker
    alloc_map: dict[str, dict] = {}
    for alloc in allocations:
        t = alloc.get("ticker")
        if t:
            alloc_map[t] = alloc

    for snap in snapshots:
        ticker = snap["ticker"]
        alloc = alloc_map.get(ticker)

        if alloc:
            snap["shares"] = alloc.get("shares", 0)
            snap["dollar_alloc"] = alloc.get("dollar_alloc", 0)
            snap["weight_pct"] = alloc.get("weight_pct", 0)
            snap["category"] = alloc.get("category", "selected")
            snap["bucket"] = alloc.get("bucket")
        else:
            snap["shares"] = 0
            snap["dollar_alloc"] = 0
            snap["weight_pct"] = 0
            snap["category"] = "not_selected"
            snap["bucket"] = None


def _enrich_snapshots_with_signals(
    snapshots: list[dict],
    signals: pd.DataFrame,
) -> None:
    """
    Update snapshot ``signal`` field from reconciled portfolio
    signals.

    The reconciled signal overrides the per-ticker signal
    because it incorporates cross-sectional ranking and
    portfolio-level constraints (max positions, cooldowns,
    breadth gating).

    Modifies snapshots in-place.
    """
    if signals.empty:
        return

    # Build lookup: ticker → reconciled signal
    if "ticker" in signals.columns and "signal" in signals.columns:
        sig_map = dict(
            zip(signals["ticker"], signals["signal"])
        )
    elif signals.index.name == "ticker" and "signal" in signals.columns:
        sig_map = signals["signal"].to_dict()
    else:
        return

    for snap in snapshots:
        ticker = snap["ticker"]
        if ticker in sig_map:
            snap["signal"] = sig_map[ticker]