"""
backtest/phase2/engine.py
Core backtesting engine with pre-computed indicators for speed.

Fixes applied:
  - EXIT DOUBLE-GATE REMOVED: sigexit_v2=1 on held tickers → SELL regardless
    of action_v2 column. The signal layer's exit flag is authoritative.
  - force_exits() runs unconditionally every day (not gated by prev_actions)
  - process_signals() receives prev_scores
  - try_upgrades() called exactly once per day
  - Position sizing uses current NAV (not frozen initial_capital)
  - Exit metadata captured on closed trades for post-hoc analysis
  - Daily debug spam moved to DEBUG level, only on first occurrence
"""
from __future__ import annotations

import logging
import time
import pandas as pd
from typing import Any, Dict, List, Set

from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.tracker import PortfolioTracker

log = logging.getLogger(__name__)


class BacktestEngine:

    def __init__(
        self,
        data_source: BacktestDataSource,
        market: str,
        config: Dict[str, Any],
        start_date: str,
        end_date: str,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        config_name: str = "default",
    ):
        self.data_source = data_source
        self.market = market
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.config_name = config_name

        self.daily_log: List[Dict] = []
        self.equity_curve: List[tuple] = []
        self.action_history: Dict = {}

        # Pre-computed caches (populated in _precompute_all)
        self._precomputed_frames: Dict[str, pd.DataFrame] = {}
        self._precomputed_regime_df: pd.DataFrame | None = None

        # Diagnostic tracking
        self._columns_logged: bool = False
        self._exit_source_counts: Dict[str, int] = {
            "signal_exit": 0,
            "force_exit_trailing": 0,
            "force_exit_max_hold": 0,
            "force_exit_other": 0,
        }

    def _extract_scores(self, output: Dict) -> Dict[str, float]:
        """Pull composite scores from the pipeline output table."""
        scores: Dict[str, float] = {}
        for key in ("action_table", "snapshot", "actions"):
            if key not in output:
                continue
            df = output[key]
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            score_col = next(
                (c for c in (
                    "composite_v2", "scorecomposite_v2", "composite", "score",
                    "total_score", "weighted_score",
                ) if c in df.columns),
                None,
            )
            if score_col is None:
                continue
            if "ticker" in df.columns:
                for _, row in df.iterrows():
                    if pd.notna(row[score_col]):
                        scores[row["ticker"]] = float(row[score_col])
            else:
                for ticker, row in df.iterrows():
                    if pd.notna(row[score_col]):
                        scores[str(ticker)] = float(row[score_col])
            break
        return scores

    # ==================================================================
    #  EXIT METADATA EXTRACTION — for trade-level audit trail
    # ==================================================================
    def _extract_exit_metadata(self, output: Dict) -> Dict[str, Dict[str, Any]]:
        """
        Extract per-ticker exit diagnostic fields from the pipeline output.
        Returns {ticker: {exit_reason, exit_momentum, ..., indicator values}}.
        """
        metadata: Dict[str, Dict[str, Any]] = {}
        for key in ("action_table", "snapshot"):
            df = output.get(key)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            if "exit_reason" not in df.columns:
                break

            ticker_in_col = "ticker" in df.columns
            for idx, row in df.iterrows():
                ticker = str(row["ticker"]) if ticker_in_col else str(idx)
                metadata[ticker] = {
                    "exit_reason": str(row.get("exit_reason", "") or ""),
                    "exit_momentum": bool(row.get("exit_momentum", False)),
                    "exit_trend": bool(row.get("exit_trend", False)),
                    "exit_rs": bool(row.get("exit_rs", False)),
                    "exit_no_trend": bool(row.get("exit_no_trend", False)),
                    "exit_score_floor": bool(row.get("exit_score_floor", False)),
                    "rsi_at_exit": float(row.get("rsi14", 0) or 0),
                    "adx_at_exit": float(row.get("adx14", 0) or 0),
                    "macdhist_at_exit": float(row.get("macdhist", 0) or 0),
                    "closevsema30_at_exit": float(row.get("closevsema30pct", 0) or 0),
                    "rszscore_at_exit": float(row.get("rszscore", 0) or 0),
                    "composite_at_exit": float(row.get("scorecomposite_v2", 0) or 0),
                }
            break
        return metadata

    # ==================================================================
    #  EXTRACT sigexit_v2 DIRECTLY for held tickers
    # ==================================================================
    def _extract_exit_flags(
        self, output: Dict, held_tickers: Set[str]
    ) -> Set[str]:
        """
        Scan pipeline output for held tickers with sigexit_v2 >= 1.
        This is INDEPENDENT of the action_v2 column — the signal layer's
        exit flag is authoritative for held positions.

        Returns set of tickers that should be sold.
        """
        if not held_tickers:
            return set()

        exit_tickers: Set[str] = set()
        for key in ("action_table", "snapshot"):
            df = output.get(key)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            exit_col = next(
                (c for c in ("sigexit_v2", "sig_exit_v2", "exit_signal")
                 if c in df.columns),
                None,
            )
            if exit_col is None:
                break

            ticker_in_col = "ticker" in df.columns
            for idx, row in df.iterrows():
                ticker = str(row["ticker"]) if ticker_in_col else str(idx)
                if ticker not in held_tickers:
                    continue
                try:
                    if float(row[exit_col]) >= 1.0:
                        exit_tickers.add(ticker)
                except (TypeError, ValueError):
                    pass
            break

        if exit_tickers:
            log.info(
                "[%s] EXIT FLAGS (direct): %d of %d held tickers flagged: %s",
                self.config_name,
                len(exit_tickers),
                len(held_tickers),
                sorted(exit_tickers),
            )

        return exit_tickers

    # ==================================================================
    #  EXIT CHURN DIAGNOSTIC — log distribution when churn is extreme
    # ==================================================================
    def _log_exit_churn_diagnostic(self, output: Dict, day) -> None:
        """
        When >60% of the universe is flagged for exit, log the underlying
        indicator distributions to diagnose threshold calibration.
        """
        for key in ("action_table", "snapshot"):
            df = output.get(key)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            if "sigexit_v2" not in df.columns:
                break

            exit_pct = 100.0 * df["sigexit_v2"].mean()
            if exit_pct <= 60:
                break

            if "closevsema30pct" in df.columns:
                col = df["closevsema30pct"].dropna()
                if not col.empty:
                    exit_trend_count = (
                        int(df["exit_trend"].sum())
                        if "exit_trend" in df.columns else -1
                    )
                    log.warning(
                        "[%s] %s EXIT DEEP DIVE — closevsema30pct: "
                        "min=%.4f p10=%.4f p25=%.4f med=%.4f p75=%.4f max=%.4f | "
                        "exit_trend=%d/%d tickers",
                        self.config_name,
                        day.strftime("%Y-%m-%d"),
                        float(col.min()),
                        float(col.quantile(0.10)),
                        float(col.quantile(0.25)),
                        float(col.median()),
                        float(col.quantile(0.75)),
                        float(col.max()),
                        exit_trend_count,
                        len(df),
                    )

            if "rsi14" in df.columns:
                rsi = df["rsi14"].dropna()
                if not rsi.empty:
                    exit_mom_count = (
                        int(df["exit_momentum"].sum())
                        if "exit_momentum" in df.columns else -1
                    )
                    log.warning(
                        "[%s] %s EXIT DEEP DIVE — rsi14: "
                        "min=%.1f p10=%.1f p25=%.1f med=%.1f p75=%.1f max=%.1f | "
                        "exit_momentum=%d/%d tickers",
                        self.config_name,
                        day.strftime("%Y-%m-%d"),
                        float(rsi.min()),
                        float(rsi.quantile(0.10)),
                        float(rsi.quantile(0.25)),
                        float(rsi.median()),
                        float(rsi.quantile(0.75)),
                        float(rsi.max()),
                        exit_mom_count,
                        len(df),
                    )

            if "rszscore" in df.columns:
                rs = df["rszscore"].dropna()
                if not rs.empty:
                    exit_rs_count = (
                        int(df["exit_rs"].sum())
                        if "exit_rs" in df.columns else -1
                    )
                    log.warning(
                        "[%s] %s EXIT DEEP DIVE — rszscore: "
                        "min=%.3f p10=%.3f p25=%.3f med=%.3f p75=%.3f max=%.3f | "
                        "exit_rs=%d/%d tickers",
                        self.config_name,
                        day.strftime("%Y-%m-%d"),
                        float(rs.min()),
                        float(rs.quantile(0.10)),
                        float(rs.quantile(0.25)),
                        float(rs.median()),
                        float(rs.quantile(0.75)),
                        float(rs.max()),
                        exit_rs_count,
                        len(df),
                    )
            break

    # ==================================================================
    #  PRE-COMPUTATION — run all expensive rolling ops ONCE
    # ==================================================================
    def _precompute_all(self):
        """
        Pre-compute per-ticker indicators, RS z-scores, and benchmark
        regime on the FULL history.  Each backtest day then just slices
        into these frames — no recomputation.
        """
        from refactor.pipeline_v2 import (
            _canonicalize_indicator_columns,
            _fill_missing_indicators,
            annotate_scoreability,
        )
        from refactor.strategy.adapters_v2 import ensure_columns
        from refactor.strategy.regime_v2 import classify_volatility_regime
        from refactor.strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
        from compute.indicators import compute_all_indicators

        t0 = time.time()

        # 1 — per-ticker indicators
        raw_frames = {}
        for ticker, df in self.data_source.ticker_data.items():
            if df is not None and not df.empty:
                enriched = compute_all_indicators(df.copy())
                enriched = _canonicalize_indicator_columns(enriched)
                enriched = _fill_missing_indicators(enriched)
                enriched = ensure_columns(enriched)
                raw_frames[ticker] = enriched

        log.info(
            "[%s] Pre-computed indicators for %d tickers (%.1fs)",
            self.config_name, len(raw_frames), time.time() - t0,
        )

        # 2 — cross-sectional RS z-scores + regimes
        t1 = time.time()
        bench_df = self.data_source.benchmark_data
        if bench_df is not None and not bench_df.empty:
            raw_frames = compute_rs_zscores(raw_frames, bench_df)
            raw_frames = enrich_rs_regimes(raw_frames)
            self._precomputed_regime_df = classify_volatility_regime(
                bench_df,
                params=self.config.get("vol_regime_params"),
            )
        log.info(
            "[%s] Pre-computed RS + regimes (%.1fs)",
            self.config_name, time.time() - t1,
        )

        # 3 — annotate scoreability
        for ticker in raw_frames:
            raw_frames[ticker] = annotate_scoreability(raw_frames[ticker])

        self._precomputed_frames = raw_frames
        log.info(
            "[%s] Pre-computation complete: %d tickers, total %.1fs",
            self.config_name, len(raw_frames), time.time() - t0,
        )

    # ==================================================================
    #  CURRENT NAV CALCULATION — for dynamic position sizing
    # ==================================================================
    # def _compute_current_nav(self, tracker: PortfolioTracker) -> float:
    #     """
    #     Estimate NAV for position sizing.
        
    #     Uses entry_price as a conservative proxy since the original tracker
    #     doesn't store current market price on Position objects.
    #     The real MTM happens in mark_to_market() at end of day.
    #     """
    #     nav = tracker.cash
    #     for ticker, pos in tracker.positions.items():
    #         nav += pos.shares * pos.entry_price
    #     return nav

    def _compute_current_nav(self, tracker: PortfolioTracker) -> float:
        return tracker.nav

    # ==================================================================
    #  MAIN LOOP
    # ==================================================================
    def run(self) -> Dict[str, Any]:
        tickers = self.data_source.get_tickers()
        trading_days = self.data_source.get_trading_days(
            self.start_date, self.end_date
        )
        if not trading_days:
            raise ValueError(
                f"No trading days between {self.start_date} and {self.end_date}"
            )

        # ── pre-compute everything once ───────────────────────
        self._precompute_all()

        # ── read min-hold params from signal config ───────────
        sig_params = self.config.get("signal_params", {}) or {}
        min_hold = sig_params.get("min_hold_days", 5)
        min_profit = sig_params.get("min_profit_early_exit_pct", 0.05)

        tracker = PortfolioTracker(
            initial_capital=self.initial_capital,
            max_positions=self.max_positions,
            commission_rate=self.commission_rate,
            slippage_rate=self.slippage_rate,
            min_hold_days=min_hold,
            min_profit_early_exit_pct=min_profit,
            trailing_stop_pct=sig_params.get("trailing_stop_pct", 0.18),
            max_hold_days=sig_params.get("max_hold_days", 120),
            upgrade_min_score_gap=sig_params.get("upgrade_min_score_gap", 999),
        )

        log.info(
            "[%s] backtest %s → %s  (%d days, %d tickers)  "
            "min_hold=%dd  min_profit=%.0f%%  trailing_stop=%.0f%%  "
            "max_hold=%dd",
            self.config_name, self.start_date, self.end_date,
            len(trading_days), len(tickers), min_hold, min_profit * 100,
            sig_params.get("trailing_stop_pct", 0.18) * 100,
            sig_params.get("max_hold_days", 120),
        )

        prev_actions: Dict[str, str] = {}
        prev_scores: Dict[str, float] = {}
        prev_exit_metadata: Dict[str, Dict[str, Any]] = {}
        prev_exit_tickers: Set[str] = set()
        bench_start_close: float | None = None

        # Cash utilization tracking
        cash_utilization_pcts: List[float] = []

        for i, day in enumerate(trading_days):

            # ── 1. run pipeline (fast path) ───────────────────
            try:
                output = self._run_pipeline_fast(day, tickers)

                # Debug: log columns only once (not every day)
                if not self._columns_logged:
                    for key in ("action_table", "snapshot"):
                        df = output.get(key)
                        if isinstance(df, pd.DataFrame):
                            log.info(
                                "[%s] Pipeline output '%s' columns: %s",
                                self.config_name, key, list(df.columns),
                            )
                            if "sigexit_v2" in df.columns:
                                log.info(
                                    "[%s] sigexit_v2 present — exit signal "
                                    "path is active",
                                    self.config_name,
                                )
                            else:
                                log.warning(
                                    "[%s] sigexit_v2 NOT in output — exits "
                                    "will only fire via force_exits!",
                                    self.config_name,
                                )
                            self._columns_logged = True
                            break

                # Log exit churn diagnostic when extreme
                self._log_exit_churn_diagnostic(output, day)

                held = set(tracker.positions.keys())
                actions = self._extract_actions(output, held_tickers=held)
                scores = self._extract_scores(output)

                # ── CRITICAL FIX: Extract exit flags DIRECTLY ──────
                # This is independent of the action_v2 column.
                # sigexit_v2=1 on a HELD ticker → SELL, period.
                exit_tickers = self._extract_exit_flags(output, held)

                # Merge direct exit flags into actions
                # (these override any HOLD from the action layer)
                for ticker in exit_tickers:
                    if ticker not in actions or actions[ticker] != "SELL":
                        if ticker in actions and actions[ticker] in ("BUY", "STRONG_BUY"):
                            # Edge case: pipeline says BUY but we hold it
                            # and exit flag is set. Exit takes priority.
                            log.warning(
                                "[%s] %s CONFLICT: %s has BUY action but "
                                "sigexit_v2=1 (held). Forcing SELL.",
                                self.config_name,
                                day.strftime("%Y-%m-%d"),
                                ticker,
                            )
                        actions[ticker] = "SELL"

                # Extract exit metadata for trade-level audit trail
                exit_metadata = self._extract_exit_metadata(output)

            except Exception as exc:
                log.warning(
                    "[%s] pipeline error %s: %s",
                    self.config_name, day.strftime("%Y-%m-%d"), exc,
                )
                actions, scores = {}, {}
                exit_metadata = {}
                exit_tickers = set()

            # ── 2. log signals ────────────────────────────────
            buy_sigs = [t for t, a in actions.items() if a in ("BUY", "STRONG_BUY")]
            sell_sigs = [t for t, a in actions.items() if a == "SELL"]
            if buy_sigs or sell_sigs:
                log.info(
                    "[%s] %s  BUY %s | SELL %s",
                    self.config_name,
                    day.strftime("%Y-%m-%d"),
                    buy_sigs if buy_sigs else "—",
                    sell_sigs if sell_sigs else "—",
                )

            # ── 3. execute PREVIOUS day's signals at TODAY's open ─
            prices_open = self._prices_fast(day, tickers, field="open")

            if i > 0:
                # snapshot closed trade count before execution
                n_closed_before = len(tracker.closed_trades)

                # 3a — force-exit stale / stopped-out positions
                #       runs UNCONDITIONALLY — not gated by prev_actions
                tracker.force_exits(day, prices_open)

                # Count force exits
                n_force_closed = len(tracker.closed_trades) - n_closed_before

                # 3b — signal-driven sells + score-ranked buys
                #       CRITICAL: prev_exit_tickers ensures sigexit_v2
                #       tickers get SELL action even if action layer said HOLD
                if prev_actions:
                    # ─── DYNAMIC POSITION SIZING ──────────────────
                    # Pass current NAV so tracker can size new positions
                    # based on current equity, not frozen initial_capital.
                    current_nav = self._compute_current_nav(tracker)

                    tracker.process_signals(
                        day, prev_actions, prices_open,
                        scores=prev_scores,
                        current_nav=current_nav,  # ← NEW: enables dynamic sizing
                    )

                # 3c — upgrade weak held positions (single call)
                if prev_scores:
                    swaps = tracker.try_upgrades(
                        day,
                        candidate_scores=prev_scores,
                        prices=prices_open,
                        max_upgrades=2,
                    )
                    if swaps:
                        log.info(
                            "[%s] %s  upgrades=%d: %s",
                            self.config_name,
                            day.strftime("%Y-%m-%d"),
                            len(swaps),
                            swaps,
                        )

                # Attach exit metadata to newly closed trades
                n_closed_after = len(tracker.closed_trades)
                if n_closed_after > n_closed_before:
                    for j, trade in enumerate(
                        tracker.closed_trades[n_closed_before:n_closed_after]
                    ):
                        ticker = trade.get("ticker", "")

                        # Determine exit source
                        if j < n_force_closed:
                            # These were closed by force_exits
                            source = trade.get("exit_type", "force_exit_other")
                            trade.setdefault("exit_source", source)
                            self._exit_source_counts[
                                source if source in self._exit_source_counts
                                else "force_exit_other"
                            ] += 1
                        else:
                            # These were closed by process_signals (sigexit_v2)
                            trade.setdefault("exit_source", "signal_exit")
                            self._exit_source_counts["signal_exit"] += 1

                        # Attach indicator metadata from the exit signal
                        if ticker in prev_exit_metadata:
                            trade.update(prev_exit_metadata[ticker])
                        elif "exit_reason" not in trade:
                            trade["exit_reason"] = trade.get(
                                "exit_source", "unknown"
                            )

            # ── 4. mark-to-market at close ────────────────────
            prices_close = self._prices_fast(day, tickers, field="close")
            port_value = tracker.mark_to_market(day, prices_close)

            # refresh held-position scores for tomorrow's upgrades
            tracker.update_scores(scores)

            # ── 5. cash utilization tracking ──────────────────
            if port_value > 0:
                cash_pct = 100.0 * tracker.cash / port_value
                cash_utilization_pcts.append(cash_pct)
                # Warn if cash consistently too high
                if i > 20 and i % 20 == 0:
                    recent_cash = cash_utilization_pcts[-20:]
                    avg_cash = sum(recent_cash) / len(recent_cash)
                    if avg_cash > 40:
                        log.warning(
                            "[%s] %s CASH DRAG: avg cash=%.1f%% over last "
                            "20 days. NAV=$%s but only %d/%d slots filled. "
                            "Consider: (1) lower entry threshold, "
                            "(2) lower min_rank_pct, (3) dynamic sizing.",
                            self.config_name,
                            day.strftime("%Y-%m-%d"),
                            avg_cash,
                            f"{port_value:,.0f}",
                            len(tracker.positions),
                            self.max_positions,
                        )

            # ── 6. benchmark value ────────────────────────────
            bench_value = self._benchmark_value(day, bench_start_close)
            if bench_value is not None and bench_start_close is None:
                bench_df = self.data_source.benchmark_data
                if bench_df is not None and not bench_df.empty and day in bench_df.index:
                    bench_start_close = float(bench_df.loc[day, "close"])
                    bench_value = self.initial_capital

            # ── 7. record ─────────────────────────────────────
            self.equity_curve.append((day, port_value, bench_value))
            self.daily_log.append({
                "date": day,
                "portfolio_value": port_value,
                "benchmark_value": bench_value,
                "cash": tracker.cash,
                "cash_pct": 100.0 * tracker.cash / max(port_value, 1),
                "n_positions": len(tracker.positions),
                "positions": list(tracker.positions.keys()),
                "n_buys": sum(1 for a in actions.values()
                              if a in ("BUY", "STRONG_BUY")),
                "n_sells": sum(1 for a in actions.values()
                               if a == "SELL"),
                "n_exit_flags": len(exit_tickers),
            })

            prev_actions = actions
            prev_scores = scores
            prev_exit_metadata = exit_metadata
            prev_exit_tickers = exit_tickers
            self.action_history[day] = actions

            if (i + 1) % 50 == 0 or i == len(trading_days) - 1:
                log.info(
                    "[%s] %d/%d  %s  portfolio=$%s  pos=%d/%d  "
                    "cash=$%s (%.1f%%)",
                    self.config_name, i + 1, len(trading_days),
                    day.strftime("%Y-%m-%d"),
                    f"{port_value:,.0f}",
                    len(tracker.positions),
                    self.max_positions,
                    f"{tracker.cash:,.0f}",
                    100.0 * tracker.cash / max(port_value, 1),
                )

        # ── End-of-backtest summary ───────────────────────────────────
        log.info(
            "[%s] EXIT SOURCE SUMMARY: %s",
            self.config_name, self._exit_source_counts,
        )
        if cash_utilization_pcts:
            avg_cash_all = sum(cash_utilization_pcts) / len(cash_utilization_pcts)
            log.info(
                "[%s] CASH UTILIZATION: avg=%.1f%% min=%.1f%% max=%.1f%%",
                self.config_name,
                avg_cash_all,
                min(cash_utilization_pcts),
                max(cash_utilization_pcts),
            )

        return {
            "config_name": self.config_name,
            "config": self.config,
            "equity_curve": pd.DataFrame(
                self.equity_curve, columns=["date", "value", "benchmark"]
            ),
            "daily_log": pd.DataFrame(self.daily_log),
            "trade_log": (
                pd.DataFrame(tracker.closed_trades)
                if tracker.closed_trades
                else pd.DataFrame()
            ),
            "final_value": (
                self.equity_curve[-1][1]
                if self.equity_curve
                else self.initial_capital
            ),
            "initial_capital": self.initial_capital,
            "open_positions": dict(tracker.positions),
            "exit_source_counts": dict(self._exit_source_counts),
            "avg_cash_pct": (
                sum(cash_utilization_pcts) / len(cash_utilization_pcts)
                if cash_utilization_pcts else 0
            ),
        }

    # ==================================================================
    #  FAST PIPELINE — uses pre-computed data
    # ==================================================================
    def _run_pipeline_fast(self, day, tickers) -> Dict:
        from refactor.pipeline_v2 import run_pipeline_v2

        cutoff = pd.Timestamp(day)
        lookback = self.data_source.lookback_bars

        # Slice pre-computed frames to current day
        tradable_frames = {}
        for t in tickers:
            if t not in self._precomputed_frames:
                continue
            df = self._precomputed_frames[t]
            sliced = df.loc[df.index <= cutoff]
            if sliced.empty:
                continue
            if len(sliced) > lookback:
                sliced = sliced.iloc[-lookback:]
            tradable_frames[t] = sliced

        # Slice benchmark
        bench_df = self.data_source.benchmark_data
        if bench_df is not None:
            bench_df = bench_df.loc[bench_df.index <= cutoff]
            if len(bench_df) > lookback:
                bench_df = bench_df.iloc[-lookback:]

        if bench_df is None or bench_df.empty:
            raise ValueError(f"Benchmark empty on {day}")

        pipeline_config = {
            "vol_regime_params": self.config.get("vol_regime_params"),
            "scoring_weights": self.config.get("scoring_weights"),
            "scoring_params": self.config.get("scoring_params"),
            "signal_params": self.config.get("signal_params"),
            "convergence_params": self.config.get("convergence_params"),
            "action_params": self.config.get("action_params"),
            "breadth_params": self.config.get("breadth_params"),
            "rotation_params": self.config.get("rotation_params"),
        }

        return run_pipeline_v2(
            tradable_frames=tradable_frames,
            bench_df=bench_df,
            market=self.market,
            config=pipeline_config,
            precomputed=True,
        )

    # ==================================================================
    #  Fast price lookup from pre-computed frames
    # ==================================================================
    def _prices_fast(
        self, day, tickers, field: str = "open"
    ) -> Dict[str, float]:
        cutoff = pd.Timestamp(day)
        prices = {}
        for t in tickers:
            df = self._precomputed_frames.get(t)
            if df is not None and not df.empty and cutoff in df.index:
                val = df.loc[cutoff, field]
                if pd.notna(val):
                    prices[t] = float(val)
        return prices

    # ==================================================================
    #  Benchmark helper
    # ==================================================================
    def _benchmark_value(self, day, bench_start_close) -> float | None:
        bench_df = self.data_source.benchmark_data
        if bench_df is None or bench_df.empty:
            return None
        if day not in bench_df.index:
            return None
        if bench_start_close is None:
            return self.initial_capital
        return self.initial_capital * (
            float(bench_df.loc[day, "close"]) / bench_start_close
        )

    # ==================================================================
    #  Extract actions from pipeline output
    # ==================================================================
    def _extract_actions(
        self, output: Dict, held_tickers: Set[str] = None
    ) -> Dict[str, str]:
        """
        Extract ticker → action mapping from pipeline output.

        BUY/STRONG_BUY  → BUY  (cross-sectional entry — unheld tickers only)
        SELL/EXIT       → SELL (held tickers only)

        NOTE: This method extracts actions from the action_v2 column.
        The DIRECT exit flag check (_extract_exit_flags) runs separately
        and merges its results into the final action dict. This means
        sigexit_v2=1 triggers a SELL even if action_v2 says "HOLD".
        """
        if held_tickers is None:
            held_tickers = set()

        actions: Dict[str, str] = {}
        for key in ("action_table", "snapshot", "actions"):
            if key not in output:
                continue
            df = output[key]
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            act_col = next(
                (c for c in ("action_v2", "action", "signal")
                 if c in df.columns),
                None,
            )
            if act_col is None:
                continue

            ticker_in_col = "ticker" in df.columns

            for idx, row in df.iterrows():
                ticker = str(row["ticker"]) if ticker_in_col else str(idx)
                raw_action = str(row[act_col]).upper()

                if raw_action in ("BUY", "STRONG_BUY"):
                    # Only emit BUY for tickers NOT already held
                    if ticker not in held_tickers:
                        actions[ticker] = raw_action

                elif raw_action in ("SELL", "EXIT", "STRONG_SELL"):
                    # Only emit SELL for tickers we actually hold
                    if ticker in held_tickers:
                        actions[ticker] = "SELL"
                    # NOTE: No longer double-gating with sigexit_v2 here.
                    # The action layer says SELL → we trust it.
                    # sigexit_v2 is checked independently in _extract_exit_flags.

            break

        return actions