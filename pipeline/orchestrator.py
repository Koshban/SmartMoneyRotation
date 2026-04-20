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
      Generate recommendation report and optional backtest.

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
  result = orch.generate_reports()

Dependencies
────────────
  pipeline/runner.py              — single-ticker pipeline
  compute/breadth.py              — universe breadth
  compute/sector_rs.py            — sector RS panel
  output/rankings.py              — cross-sectional rankings
  output/signals.py               — portfolio-level signals
  strategy/portfolio.py           — position sizing & allocation
  portfolio/backtest.py           — historical backtest
  reports/recommendations.py      — ticker recommendations
  src/db/loader.py                — OHLCV data loading
  common/config.py                — all parameters
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional
import numpy as np
import pandas as pd
# ── Convergence ───────────────────────────────────────────────
from strategy.convergence import (
    run_convergence,
    build_price_matrix,
    enrich_snapshots,
    convergence_report,
    MarketSignalResult,
)
from strategy.rotation import (
    run_rotation,
    RotationConfig,
    RotationResult,
)
# ── Config ────────────────────────────────────────────────────
from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SECTOR_ETFS,
    TICKER_SECTOR_MAP,
    UNIVERSE,
    MARKET_CONFIG, 
    ACTIVE_MARKETS
)

# ── Compute ───────────────────────────────────────────────────
from compute.breadth import (
    breadth_to_pillar_scores,
    compute_all_breadth,
)
from compute.sector_rs import compute_all_sector_rs

# ── Data loading ──────────────────────────────────────────────
from src.db.loader import load_ohlcv, load_universe_ohlcv

# ── Pipeline ──────────────────────────────────────────────────
from pipeline.runner import (
    TickerResult,
    results_errors,
    results_to_scored_universe,
    results_to_snapshots,
    run_batch,
    run_ticker,
)

# ── Output ────────────────────────────────────────────────────
from output.rankings import compute_all_rankings
from output.signals import compute_all_signals

# ── Strategy ──────────────────────────────────────────────────
from strategy.portfolio import build_portfolio

# ── Portfolio ─────────────────────────────────────────────────
from portfolio.backtest import run_backtest, BacktestConfig

# ── Reports ───────────────────────────────────────────────────
from reports.recommendations import build_report


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
    bench_df: Optional[pd.DataFrame] = None       # ← NEW

    # ── NEW: convergence + rotation ───────────────────────────
    rotation_result: Optional[Any] = None            # RotationResult
    convergence: Optional[Any] = None                # MarketSignalResult
    market: str = "US"

    recommendation_report: Optional[dict] = None
    backtest: Any = None                          # BacktestResult or None

    errors: list[str] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    run_date: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    as_of: Optional[pd.Timestamp] = None

    @property
    def n_tickers(self) -> int:
        return len(self.scored_universe)

    @property
    def n_errors(self) -> int:
        return len(self.errors)

    @property
    def total_time(self) -> float:
        return sum(self.timings.values())

    def top_n(self, n: int = 10) -> list[dict]:
        return self.snapshots[:n]

    def summary(self) -> str:
        top = self.snapshots[0]["ticker"] if self.snapshots else "N/A"
        return (
            f"CASH Pipeline — {self.run_date.strftime('%Y-%m-%d')} — "
            f"{self.n_tickers} tickers scored, "
            f"{self.n_errors} errors, "
            f"top={top}, "
            f"{self.total_time:.1f}s"
        )


# ═══════════════════════════════════════════════════════════════
#  ORCHESTRATOR CLASS
# ═══════════════════════════════════════════════════════════════

class Orchestrator:
    """
    Stateful pipeline coordinator.

    Use for fine-grained phase-by-phase control (notebooks,
    debugging).  For one-shot usage, prefer ``run_full_pipeline()``.
    """

    class Orchestrator:
        def __init__(
            self,
            *,
            market: str = "US",                       # ← NEW
            universe: list[str] | None = None,
            benchmark: str | None = None,
            capital: float | None = None,
            as_of: pd.Timestamp | None = None,
            enable_breadth: bool = True,
            enable_sectors: bool = True,
            enable_signals: bool = True,
            enable_backtest: bool = False,
        ):
            # ── Market-aware defaults ─────────────────────────────
            self.market: str = market                  # ← NEW
            mcfg = MARKET_CONFIG.get(market, {})

            self.tickers: list[str] = universe or list(
                mcfg.get("universe", UNIVERSE)
            )
            self.benchmark: str = benchmark or mcfg.get(
                "benchmark", BENCHMARK_TICKER
            )
            self.capital: float = capital or PORTFOLIO_PARAMS["total_capital"]
            self.as_of: pd.Timestamp | None = as_of

            # Respect market config for feature flags
            self.enable_breadth: bool = (
                enable_breadth
                and mcfg.get("scoring_weights", {}).get(
                    "pillar_breadth", 0.10
                ) > 0
            )
            self.enable_sectors: bool = (
                enable_sectors
                and mcfg.get("sector_rs_enabled", True)
            )
            self.enable_signals: bool = enable_signals
            self.enable_backtest: bool = enable_backtest

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
            self._backtest: Any = None

            # ── NEW: rotation + convergence state ─────────────────
            self._rotation_result: Any = None           # RotationResult
            self._convergence_result: Any = None        # MarketSignalResult

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
        bench_df : pd.DataFrame, optional
            Pre-loaded benchmark OHLCV.
        """
        t0 = time.perf_counter()

        if preloaded is not None:
            self._ohlcv = preloaded
            logger.info(
                f"Phase 0: Using {len(preloaded)} pre-loaded "
                f"ticker DataFrames"
            )
        else:
            all_symbols = list(set(self.tickers + [self.benchmark]))
            self._ohlcv = load_universe_ohlcv(all_symbols)
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

        Breadth feeds Pillar 5 (scoring) and Gate 3 (signals).
        Sector RS feeds sector tailwind adjustments and Gate 2.
        Both are optional — the per-ticker pipeline degrades
        gracefully without them.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        # ── Breadth ───────────────────────────────────────────
        #
        # compute_all_breadth() expects {ticker: OHLCV DataFrame}
        # with at least 'close' and optionally 'volume' columns.
        # It internally aligns them into a panel.
        #
        # breadth_to_pillar_scores() takes the breadth DataFrame
        # plus a list of symbols and broadcasts the breadth score
        # to every ticker (same market-level score per day).
        #
        if self.enable_breadth:
            try:
                self._breadth = compute_all_breadth(self._ohlcv)

                if not self._breadth.empty:
                    symbols = list(self._ohlcv.keys())
                    self._breadth_scores = breadth_to_pillar_scores(
                        self._breadth, symbols
                    )
                    logger.info(
                        f"Phase 1: Breadth computed — "
                        f"{len(self._breadth)} bars, "
                        f"regime="
                        f"{self._breadth['breadth_regime'].iloc[-1]}"
                    )
                else:
                    logger.warning(
                        "Phase 1: Breadth returned empty — "
                        "universe may be too small"
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
        # compute_all_sector_rs() expects:
        #   sector_data: {sector_name: OHLCV DataFrame}
        #   benchmark_df: OHLCV DataFrame
        # Returns MultiIndex (date, sector) panel.
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
        for each ticker.
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
    #  Phase 2.5 — Rotation Engine  (US only)
    # ───────────────────────────────────────────────────────

    def run_rotation_engine(
        self,
        current_holdings: list[str] | None = None,
        config: RotationConfig | None = None,
    ) -> None:
        """
        Run the top-down sector rotation engine.

        Only meaningful for US — skipped silently for HK/IN.
        Requires Phase 2 (run_tickers) to have completed so
        that OHLCV data is available.
        """
        self._require_phase("run_tickers")

        mcfg = MARKET_CONFIG.get(self.market, {})
        engines = mcfg.get("engines", ["scoring"])

        if "rotation" not in engines:
            logger.info(
                f"Phase 2.5: Rotation not configured for "
                f"{self.market} — skipping"
            )
            return

        t0 = time.perf_counter()

        # Build wide price matrix from loaded OHLCV
        prices = build_price_matrix(self._ohlcv)

        if prices.empty or self.benchmark not in prices.columns:
            logger.warning(
                "Phase 2.5: Cannot build price matrix for "
                "rotation — skipping"
            )
            return

        try:
            r_cfg = config or RotationConfig(
                benchmark=self.benchmark,
            )
            self._rotation_result = run_rotation(
                prices=prices,
                current_holdings=current_holdings or [],
                config=r_cfg,
            )

            rr = self._rotation_result
            logger.info(
                f"Phase 2.5: Rotation complete — "
                f"{len(rr.buys)} BUY, "
                f"{len(rr.sells)} SELL, "
                f"{len(rr.reduces)} REDUCE, "
                f"{len(rr.holds)} HOLD  |  "
                f"leading={rr.leading_sectors}"
            )
        except Exception as e:
            logger.warning(
                f"Phase 2.5: Rotation failed — {e}.  "
                f"Proceeding with scoring only."
            )
            self._rotation_result = None

        elapsed = time.perf_counter() - t0
        self._timings["rotation"] = elapsed
        self._phases_completed.append("rotation")
    

    # ───────────────────────────────────────────────────────
    #  Phase 2.75 — Convergence Merge
    # ───────────────────────────────────────────────────────

    def apply_convergence(self) -> None:
        """
        Merge scoring + rotation signals via the convergence layer.

        For US:  dual-list merge (scoring + rotation)
        For HK/IN: scoring passthrough

        Updates ``self._snapshots`` with convergence labels and
        adjusted scores so downstream phases (rankings, portfolio,
        reports) benefit from the convergence intelligence.
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        self._convergence_result = run_convergence(
            market=self.market,
            scoring_snapshots=self._snapshots,
            rotation_result=self._rotation_result,
        )

        # Enrich snapshots in-place with convergence data
        enrich_snapshots(self._snapshots, self._convergence_result)

        n_strong = len(self._convergence_result.strong_buys)
        n_conflict = len(self._convergence_result.conflicts)
        logger.info(
            f"Phase 2.75: Convergence applied — "
            f"{self._convergence_result.n_tickers} tickers, "
            f"{n_strong} STRONG_BUY, "
            f"{n_conflict} CONFLICT"
        )

        elapsed = time.perf_counter() - t0
        self._timings["convergence"] = elapsed
        self._phases_completed.append("convergence")


    # ───────────────────────────────────────────────────────
    #  Phase 3 — Cross-Sectional Analysis
    # ───────────────────────────────────────────────────────

    def cross_sectional_analysis(self) -> None:
        """
        Rank the scored universe, build the portfolio, and
        generate portfolio-level signals.

        Sub-phases
        ──────────
        3a. Rankings — cross-sectional rank per date
        3b. Portfolio — position selection + weight allocation
        3c. Signals — BUY/HOLD/SELL with hysteresis
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
        # compute_all_rankings() takes {ticker: scored_df} and
        # returns MultiIndex (date, ticker) panel with rank,
        # pct_rank, pillar_agreement, rank_change columns.
        #
        try:
            self._rankings = compute_all_rankings(
                self._scored_universe
            )
            n_rows = len(self._rankings)
            logger.info(
                f"Phase 3a: Rankings computed — "
                f"{n_rows} rows"
            )
        except Exception as e:
            logger.warning(f"Phase 3a: Rankings failed — {e}")
            self._rankings = pd.DataFrame()

        # ── 3b. Portfolio Construction ────────────────────────
        #
        # build_portfolio() takes {ticker: scored_df} and
        # internally does snapshot extraction, candidate
        # filtering, ranking, selection, and weight
        # normalization.  Returns a dict with target_weights,
        # holdings DataFrame, sector_exposure, metadata, etc.
        #
        try:
            self._portfolio = build_portfolio(
                universe=self._scored_universe,
                breadth=self._breadth,
            )

            n_pos = self._portfolio.get(
                "metadata", {}
            ).get("num_holdings", 0)
            logger.info(
                f"Phase 3b: Portfolio built — "
                f"{n_pos} positions"
            )

            # Enrich orchestrator snapshots with allocation info
            _enrich_snapshots_with_allocations(
                self._snapshots,
                self._portfolio,
                self.capital,
            )
        except Exception as e:
            logger.warning(
                f"Phase 3b: Portfolio build failed — {e}"
            )
            self._portfolio = {}

        # ── 3c. Portfolio-Level Signal Generation ─────────────
        #
        # compute_all_signals() takes the ranked panel from 3a
        # and applies cross-sectional rank filters with
        # hysteresis, position limits, and a breadth circuit
        # breaker to produce BUY/HOLD/SELL/NEUTRAL signals.
        #
        if self.enable_signals and not self._rankings.empty:
            try:
                self._signals = compute_all_signals(
                    ranked=self._rankings,
                    breadth=self._breadth,
                )
                logger.info(
                    f"Phase 3c: Signals generated — "
                    f"{len(self._signals)} rows"
                )

                # Update snapshots with reconciled signals
                _enrich_snapshots_with_signals(
                    self._snapshots, self._signals
                )
            except Exception as e:
                logger.warning(
                    f"Phase 3c: Signal generation failed — {e}"
                )
                self._signals = pd.DataFrame()
        else:
            if not self.enable_signals:
                logger.info(
                    "Phase 3c: Signals disabled — skipping"
                )
            else:
                logger.warning(
                    "Phase 3c: No rankings — cannot generate "
                    "signals"
                )

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

        Returns
        -------
        PipelineResult
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        # ── Recommendation Report ─────────────────────────────
        #
        # build_report() expects a specific dict format with
        # keys: summary, regime, risk_flags, portfolio_actions,
        # ranked_buys, sells, holds, bucket_weights.
        #
        # _build_report_input() bridges from the orchestrator's
        # internal state to that format.
        #
        try:
            report_input = self._build_report_input()
            self._recommendation_report = build_report(
                report_input
            )
            logger.info("Phase 4: Recommendation report built")
        except Exception as e:
            logger.warning(
                f"Phase 4: Recommendation report failed — {e}"
            )
            self._recommendation_report = None

        # ── Backtest (optional) ───────────────────────────────
        #
        # run_backtest() takes the signals DataFrame from
        # Phase 3c (MultiIndex: date × ticker with signal,
        # signal_strength, close columns).
        #
        if self.enable_backtest:
            if not self._signals.empty:
                try:
                    bt_config = BacktestConfig(
                        initial_capital=self.capital,
                    )
                    self._backtest = run_backtest(
                        signals_df=self._signals,
                        config=bt_config,
                    )
                    metrics = (
                        self._backtest.metrics
                        if self._backtest else {}
                    )
                    logger.info(
                        f"Phase 4: Backtest complete — "
                        f"CAGR="
                        f"{metrics.get('cagr', 0):.1%}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Phase 4: Backtest failed — {e}"
                    )
            else:
                logger.warning(
                    "Phase 4: Cannot run backtest — "
                    "no signals generated"
                )

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
            bench_df=self._bench_df,
            rotation_result=self._rotation_result,       # ← NEW
            convergence=self._convergence_result,        # ← NEW
            market=self.market,                          # ← NEW
            recommendation_report=self._recommendation_report,
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
        current_holdings: list[str] | None = None,
    ) -> PipelineResult:
        """Execute all phases in sequence."""
        self.load_data(preloaded=preloaded, bench_df=bench_df)
        self.compute_universe_context()
        self.run_tickers()

        # ── NEW: rotation + convergence ───────────────────
        self.run_rotation_engine(
            current_holdings=current_holdings,
        )
        self.apply_convergence()

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
        """
        self._require_phase("run_tickers")

        if capital is not None:
            self.capital = capital

        self._snapshots = results_to_snapshots(
            self._ticker_results
        )
        self._rankings = pd.DataFrame()
        self._portfolio = {}
        self._signals = pd.DataFrame()
        self._recommendation_report = None

        self._phases_completed = [
            p for p in self._phases_completed
            if p in (
                "load_data", "universe_context", "run_tickers"
            )
        ]

        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Single-Ticker Re-run
    # ───────────────────────────────────────────────────────

    def rerun_ticker(self, ticker: str) -> TickerResult:
        """
        Re-run the pipeline for a single ticker.
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
    #  Report Input Bridge
    # ───────────────────────────────────────────────────────

    def _build_report_input(self) -> dict:
        """
        Construct the dict format that
        ``reports.recommendations.build_report()`` expects.

        Bridges from the orchestrator's internal state
        (PipelineResult-style) to the legacy dict format with
        keys: summary, regime, risk_flags, portfolio_actions,
        ranked_buys, sells, holds, bucket_weights.

        The snapshot dicts produced by ``runner._build_snapshot``
        already contain the nested structure (sub_scores,
        indicators, rs) that ``build_report`` reads, so this
        is primarily a partitioning + metadata assembly step.
        """
        # ── Regime detection ──────────────────────────────
        regime_label, regime_desc = _detect_regime(
            self._bench_df, self._breadth
        )

        spy_close = 0.0
        spy_sma200 = None
        if not self._bench_df.empty:
            spy_close = float(
                self._bench_df["close"].iloc[-1]
            )
            if len(self._bench_df) >= 200:
                sma = self._bench_df["close"].rolling(200).mean()
                spy_sma200 = float(sma.iloc[-1])

        breadth_label = "unknown"
        if (
            self._breadth is not None
            and not self._breadth.empty
            and "breadth_regime" in self._breadth.columns
        ):
            breadth_label = str(
                self._breadth["breadth_regime"].iloc[-1]
            )

        # ── Split snapshots by signal ─────────────────────
        buys = [
            s for s in self._snapshots
            if s.get("signal") == "BUY"
        ]
        sells = [
            s for s in self._snapshots
            if s.get("signal") == "SELL"
        ]
        holds = [
            s for s in self._snapshots
            if s.get("signal") not in ("BUY", "SELL")
        ]

        # Ensure allocation fields default to 0 (not None)
        for s in buys + sells + holds:
            s.setdefault("shares", 0)
            s.setdefault("dollar_alloc", 0)
            s.setdefault("weight_pct", 0)
            s.setdefault("stop_price", None)
            s.setdefault("risk_per_share", None)
            s.setdefault("themes", [])
            s.setdefault("category", "")
            s.setdefault("bucket", "")
            if s["shares"] is None:
                s["shares"] = 0
            if s["dollar_alloc"] is None:
                s["dollar_alloc"] = 0
            if s["weight_pct"] is None:
                s["weight_pct"] = 0

        # ── Summary values ────────────────────────────────
        total_buy = sum(
            s.get("dollar_alloc", 0) or 0 for s in buys
        )
        cash_rem = self.capital - total_buy
        cash_pct = (
            (cash_rem / self.capital * 100)
            if self.capital > 0 else 100
        )

        date = (
            self._snapshots[0]["date"]
            if self._snapshots
            else pd.Timestamp.now()
        )

        # ── Risk flags ────────────────────────────────────
        risk_flags: list[str] = []
        if breadth_label == "weak":
            risk_flags.append(
                "BREADTH_WEAK: Market breadth is weak — "
                "reduced exposure recommended"
            )
        if regime_label in ("bear_mild", "bear_severe"):
            risk_flags.append(
                f"REGIME: {regime_label} — defensive "
                f"positioning recommended"
            )
        if regime_label == "bear_severe":
            risk_flags.append(
                "CIRCUIT_BREAKER: Severe bear — "
                "consider halting new buys"
            )

        # ── Bucket weights from sector exposure ───────────
        bucket_weights: dict[str, float] = {}
        if self._portfolio:
            se = self._portfolio.get("sector_exposure", {})
            meta = self._portfolio.get("metadata", {})
            for sector, weight in se.items():
                bucket_weights[sector] = weight
            bucket_weights["cash"] = meta.get("cash_pct", 0.05)
        else:
            bucket_weights = {
                "core_equity": 0.70,
                "thematic": 0.20,
                "cash": 0.10,
            }

        return {
            "summary": {
                "date":             date,
                "portfolio_value":  self.capital,
                "regime":           regime_label,
                "regime_desc":      regime_desc,
                "spy_close":        spy_close,
                "bucket_breakdown": {},
                "cash_pct":         cash_pct,
                "tickers_analysed": len(self._snapshots),
                "buy_count":        len(buys),
                "sell_count":       len(sells),
                "hold_count":       len(holds),
                "error_count":      len(
                    results_errors(self._ticker_results)
                ),
                "total_buy_dollar": total_buy,
                "cash_remaining":   cash_rem,
            },
            "regime": {
                "label":       regime_label,
                "description": regime_desc,
                "spy_close":   spy_close,
                "spy_sma200":  spy_sma200,
                "breadth":     breadth_label,
            },
            "risk_flags":        risk_flags,
            "portfolio_actions": [],
            "ranked_buys":       buys,
            "sells":             sells,
            "holds":             holds,
            "bucket_weights":    bucket_weights,
        }

    # ───────────────────────────────────────────────────────
    #  Internal Helpers
    # ───────────────────────────────────────────────────────

    def _require_phase(self, phase: str) -> None:
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
    market: str = "US",                                # ← NEW
    universe: list[str] | None = None,
    benchmark: str | None = None,
    capital: float | None = None,
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, pd.DataFrame] | None = None,
    bench_df: pd.DataFrame | None = None,
    current_holdings: list[str] | None = None,         # ← NEW
    enable_breadth: bool = True,
    enable_sectors: bool = True,
    enable_signals: bool = True,
    enable_backtest: bool = False,
) -> PipelineResult:
    """
    Run the full CASH pipeline end-to-end for one market.

    This is the main entry point for CLI usage and scheduled
    jobs.  For multi-market, use ``run_multi_market_pipeline()``.
    For interactive control, use ``Orchestrator`` directly.
    """
    orch = Orchestrator(
        market=market,                                 # ← NEW
        universe=universe,
        benchmark=benchmark,
        capital=capital,
        as_of=as_of,
        enable_breadth=enable_breadth,
        enable_sectors=enable_sectors,
        enable_signals=enable_signals,
        enable_backtest=enable_backtest,
    )

    return orch.run_all(
        preloaded=preloaded,
        bench_df=bench_df,
        current_holdings=current_holdings,             # ← NEW
    )


# ═══════════════════════════════════════════════════════════════
#  MULTI-MARKET PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_multi_market_pipeline(
    *,
    active_markets: list[str] | None = None,
    capital: float | None = None,
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, dict[str, pd.DataFrame]] | None = None,
    current_holdings: dict[str, list[str]] | None = None,
    enable_backtest: bool = False,
) -> dict[str, PipelineResult]:
    """
    Run the full CASH pipeline for every active market.

    Creates a separate Orchestrator per market, each with the
    correct benchmark, universe, and feature flags.

    Parameters
    ----------
    active_markets : list[str], optional
        Markets to run.  Defaults to ``ACTIVE_MARKETS`` from config
        (typically ``["US", "HK", "IN"]``).
    capital : float, optional
        Portfolio value per market.
    as_of : pd.Timestamp, optional
        Cut-off date for backtesting.
    preloaded : dict, optional
        ``{market: {ticker: OHLCV DataFrame}}``.  If provided,
        skips data loading for that market.
    current_holdings : dict, optional
        ``{market: [ticker, ...]}``.  Holdings are passed to
        the rotation engine (US) for sell evaluation.
    enable_backtest : bool
        Run historical backtest for each market.

    Returns
    -------
    dict[str, PipelineResult]
        ``{market_code: PipelineResult}`` for each market that
        ran successfully.

    Example
    -------
    ::

        results = run_multi_market_pipeline()
        us = results["US"]
        hk = results["HK"]

        # US has convergence data
        for s in us.convergence.strong_buys:
            print(f"{s.ticker}: STRONG BUY, adj={s.adjusted_score:.3f}")

        # HK has scoring-only signals
        for s in hk.convergence.buys:
            print(f"{s.ticker}: BUY, score={s.composite_score:.3f}")
    """
    markets = active_markets or ACTIVE_MARKETS
    results: dict[str, PipelineResult] = {}

    for market in markets:
        mcfg = MARKET_CONFIG.get(market)
        if mcfg is None:
            logger.warning(
                f"Market '{market}' not in MARKET_CONFIG — skipping"
            )
            continue

        logger.info(f"\n{'=' * 60}")
        logger.info(f"  MARKET: {market}")
        logger.info(f"  Benchmark: {mcfg['benchmark']}")
        logger.info(f"  Universe: {len(mcfg['universe'])} tickers")
        logger.info(f"  Engines: {mcfg['engines']}")
        logger.info(f"{'=' * 60}")

        # Pre-loaded data for this market
        pre = (
            preloaded.get(market) if preloaded else None
        )
        holdings = (
            current_holdings.get(market, [])
            if current_holdings else None
        )

        try:
            orch = Orchestrator(
                market=market,
                capital=capital,
                as_of=as_of,
                enable_backtest=enable_backtest,
            )

            result = orch.run_all(
                preloaded=pre,
                current_holdings=holdings,
            )
            results[market] = result

            logger.info(
                f"[{market}] Pipeline complete: "
                f"{result.n_tickers} tickers, "
                f"{result.n_errors} errors, "
                f"{result.total_time:.1f}s"
            )

            # Log convergence summary
            if result.convergence:
                logger.info(
                    f"[{market}] {result.convergence.summary()}"
                )

        except Exception as e:
            logger.error(
                f"[{market}] Pipeline failed: {e}",
                exc_info=True,
            )

    logger.info(f"\nMulti-market complete: {list(results.keys())}")
    return results

# ═══════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═══════════════════════════════════════════════════════════════

def _detect_regime(
    bench_df: pd.DataFrame,
    breadth: pd.DataFrame | None,
) -> tuple[str, str]:
    """
    Simple market regime detection from benchmark price action
    and breadth state.

    Returns (label, description) where label is one of:
    bull_confirmed, bull_cautious, bear_mild, bear_severe.
    """
    if bench_df is None or bench_df.empty:
        return "bull_cautious", "Insufficient data for regime"

    close = float(bench_df["close"].iloc[-1])

    # Check SPY vs 200-day SMA
    above_sma200 = True
    if len(bench_df) >= 200:
        sma200 = float(
            bench_df["close"].rolling(200).mean().iloc[-1]
        )
        above_sma200 = close > sma200

    # Check breadth regime
    b_regime = "unknown"
    if (
        breadth is not None
        and not breadth.empty
        and "breadth_regime" in breadth.columns
    ):
        b_regime = str(breadth["breadth_regime"].iloc[-1])

    if above_sma200 and b_regime == "strong":
        return (
            "bull_confirmed",
            "SPY above 200d SMA, breadth strong",
        )
    elif above_sma200:
        return (
            "bull_cautious",
            f"SPY above 200d SMA, breadth {b_regime}",
        )
    elif b_regime == "weak":
        return (
            "bear_severe",
            "SPY below 200d SMA, breadth weak",
        )
    else:
        return (
            "bear_mild",
            f"SPY below 200d SMA, breadth {b_regime}",
        )


def _extract_sector_ohlcv(
    ohlcv: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Extract OHLCV for sector ETFs present in the loaded data.

    Returns ``{sector_name: OHLCV DataFrame}`` for sectors
    whose ETF ticker exists in ``ohlcv``.
    """
    sector_data: dict[str, pd.DataFrame] = {}
    for sector_name, etf_ticker in SECTOR_ETFS.items():
        if etf_ticker in ohlcv:
            sector_data[sector_name] = ohlcv[etf_ticker]
    return sector_data


def _enrich_snapshots_with_allocations(
    snapshots: list[dict],
    portfolio: dict,
    capital: float,
) -> None:
    """
    Merge portfolio allocation fields into ticker snapshots.

    ``build_portfolio()`` returns ``target_weights`` as
    ``{ticker: weight_fraction}``.  This function converts
    those weights to dollar allocations and share counts,
    then writes them into the snapshot dicts in-place.

    Tickers not in the portfolio get zero allocations.
    """
    target_weights = portfolio.get("target_weights", {})

    for snap in snapshots:
        ticker = snap["ticker"]
        weight = target_weights.get(ticker, 0.0)

        if weight > 0:
            dollar_alloc = weight * capital
            close = snap.get("close", 0) or 0
            shares = int(dollar_alloc / close) if close > 0 else 0

            snap["weight_pct"] = round(weight * 100, 2)
            snap["dollar_alloc"] = round(dollar_alloc, 2)
            snap["shares"] = shares
            snap["category"] = "selected"
        else:
            snap["weight_pct"] = 0.0
            snap["dollar_alloc"] = 0.0
            snap["shares"] = 0
            snap["category"] = "not_selected"


def _enrich_snapshots_with_signals(
    snapshots: list[dict],
    signals_df: pd.DataFrame,
) -> None:
    """
    Update snapshot ``signal`` field from the portfolio-level
    signals DataFrame.

    ``compute_all_signals()`` returns a MultiIndex (date, ticker)
    panel.  We extract the latest date's signals and overwrite
    the per-ticker signal (from ``strategy/signals.py``) with
    the portfolio-level signal (which incorporates rank
    hysteresis, position limits, and breadth gating).
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return

    dates = (
        signals_df.index.get_level_values("date")
        .unique()
        .sort_values()
    )
    if len(dates) == 0:
        return

    latest_date = dates[-1]

    try:
        latest = signals_df.xs(latest_date, level="date")
        sig_map = latest["signal"].to_dict()
    except (KeyError, TypeError):
        return

    for snap in snapshots:
        ticker = snap["ticker"]
        if ticker in sig_map:
            snap["signal"] = sig_map[ticker]