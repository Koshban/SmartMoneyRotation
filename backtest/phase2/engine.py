"""
backtest/phase2/engine.py
Core backtesting engine with pre-computed indicators for speed.

KEY CHANGE: Signal capping via _cap_buy_signals() ensures that:
  - At most MAX_BUY_SIGNALS_PER_DAY buy signals are acted on
  - Top STRONG_BUY_LIMIT get STRONG_BUY, rest become BUY
  - This mirrors real-life trading: you can't act on 81 signals/day

Other fixes:
  - EXIT DOUBLE-GATE REMOVED: sigexit_v2=1 on held tickers → SELL
  - force_exits() runs unconditionally every day
  - Position sizing: equal weight (operator decides real sizing)
  - MOMENTUM/BETA TILT in buy ranking
"""
from __future__ import annotations

import logging
import time
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Set

from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.tracker import PortfolioTracker

log = logging.getLogger(__name__)

# ── Signal caps (must match portfolio_v2.py) ──────────────────────────────────
STRONG_BUY_LIMIT = 15
MAX_BUY_SIGNALS_PER_DAY = 25


class BacktestEngine:

    def __init__(
        self,
        data_source: BacktestDataSource,
        market: str,
        config: Dict[str, Any],
        start_date: str,
        end_date: str,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 25,
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
        self._sizing_logged: bool = False
        self._momentum_logged: bool = False
        self._signal_cap_logged: bool = False
        self._exit_source_counts: Dict[str, int] = {
            "signal_exit": 0,
            "force_exit_trailing": 0,
            "force_exit_max_hold": 0,
            "force_exit_other": 0,
        }

        # Buy ranking config
        self._buy_ranking_params = config.get("buy_ranking_params", {}) or {}

        # Signal cap config (can be overridden via config)
        signal_cap_cfg = config.get("signal_cap_params", {}) or {}
        self._strong_buy_limit = signal_cap_cfg.get(
            "strong_buy_limit", STRONG_BUY_LIMIT
        )
        self._max_buy_signals = signal_cap_cfg.get(
            "max_buy_signals", MAX_BUY_SIGNALS_PER_DAY
        )

    # ==================================================================
    #  SIGNAL CAPPING — the key change for real-life signal quality
    # ==================================================================
    def _cap_buy_signals(
        self,
        actions: Dict[str, str],
        scores: Dict[str, float],
    ) -> Dict[str, str]:
        """
        Cap total buy signals per day and enforce STRONG_BUY limit.

        This is the CORE mechanism that reduces 81 signals/day → ~20.
        In real trading, you cannot meaningfully act on 81 names.
        The top 15 by score get STRONG_BUY, next 5-10 get BUY, rest dropped.

        Sell signals pass through unchanged.
        """
        # Separate sells from buys
        sells = {t: a for t, a in actions.items() if a == "SELL"}
        buys = {t: a for t, a in actions.items() if a in ("BUY", "STRONG_BUY")}

        if not buys:
            return sells

        # Rank buy candidates by composite score (higher = better)
        ranked_tickers = sorted(
            buys.keys(),
            key=lambda t: scores.get(t, 0.0),
            reverse=True,
        )

        # Cap total signals
        ranked_tickers = ranked_tickers[:self._max_buy_signals]

        # Assign tiers: top N = STRONG_BUY, rest = BUY
        capped = {}
        for i, ticker in enumerate(ranked_tickers):
            if i < self._strong_buy_limit:
                capped[ticker] = "STRONG_BUY"
            else:
                capped[ticker] = "BUY"

        # Log the first time we cap
        if not self._signal_cap_logged and len(buys) > self._max_buy_signals:
            log.info(
                "[%s] SIGNAL CAP: %d raw buy signals → %d "
                "(%d STRONG_BUY + %d BUY). Dropped %d low-ranked names.",
                self.config_name,
                len(buys),
                len(capped),
                min(self._strong_buy_limit, len(capped)),
                max(0, len(capped) - self._strong_buy_limit),
                len(buys) - len(capped),
            )
            self._signal_cap_logged = True

        # Merge sells back in
        capped.update(sells)
        return capped

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
    #  MOMENTUM / BETA METRICS EXTRACTION
    # ==================================================================
    def _extract_momentum_metrics(self, output: Dict) -> Dict[str, Dict[str, float]]:
        """
        Extract per-ticker momentum and volatility metrics from the pipeline.
        """
        metrics: Dict[str, Dict[str, float]] = {}

        MOMENTUM_COLS = (
            "rszscore", "rs_zscore", "rsz_score",
            "rsi14", "rsi_14",
            "adx14", "adx_14",
            "closevsema30pct", "close_vs_ema30_pct",
            "realized_vol", "hist_vol", "atr_pct",
            "macdhist", "macd_hist",
        )

        for key in ("action_table", "snapshot"):
            df = output.get(key)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            available = [c for c in MOMENTUM_COLS if c in df.columns]
            if not available:
                break

            if not self._momentum_logged:
                log.info(
                    "[%s] MOMENTUM METRICS: found columns %s in '%s'",
                    self.config_name, available, key,
                )
                self._momentum_logged = True

            ticker_in_col = "ticker" in df.columns
            for idx, row in df.iterrows():
                ticker = str(row["ticker"]) if ticker_in_col else str(idx)
                m = {}
                for col in available:
                    try:
                        val = float(row[col])
                        if pd.notna(val):
                            m[col] = val
                    except (TypeError, ValueError):
                        pass
                if m:
                    metrics[ticker] = m
            break

        return metrics

    # ==================================================================
    #  COMPUTE REALIZED VOLATILITY
    # ==================================================================
    def _compute_trailing_vol(self, day, tickers: List[str], window: int = 60) -> Dict[str, float]:
        """Compute annualized trailing volatility for each ticker."""
        cutoff = pd.Timestamp(day)
        vols: Dict[str, float] = {}

        for t in tickers:
            df = self._precomputed_frames.get(t)
            if df is None or df.empty:
                continue
            sliced = df.loc[df.index <= cutoff]
            if len(sliced) < window:
                continue
            recent = sliced["close"].iloc[-window:]
            rets = recent.pct_change().dropna()
            if len(rets) > 10:
                vols[t] = float(rets.std() * np.sqrt(252))

        return vols

    # ==================================================================
    #  POSITION SIZING EXTRACTION
    # ==================================================================
    def _extract_position_sizes(self, output: Dict) -> Dict[str, float]:
        """
        Extract per-ticker position sizing from the pipeline output.
        Returns {ticker: weight} where weight is a fraction of NAV.
        """
        sizing: Dict[str, float] = {}

        SIZING_CANDIDATES = (
            "position_pct", "position_size_pct",
            "weight", "target_weight",
            "confidence", "signal_strength",
            "size_multiplier", "size_mult",
        )

        for key in ("action_table", "snapshot", "actions"):
            if key not in output:
                continue
            df = output[key]
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            size_col = next(
                (c for c in SIZING_CANDIDATES if c in df.columns),
                None,
            )
            if size_col is None:
                continue

            if not self._sizing_logged:
                log.info(
                    "[%s] SIZING: using column '%s' from pipeline output '%s'",
                    self.config_name, size_col, key,
                )
                sample = df[size_col].dropna().head(5)
                log.info(
                    "[%s] SIZING sample values: %s",
                    self.config_name, sample.tolist(),
                )
                self._sizing_logged = True

            ticker_in_col = "ticker" in df.columns
            for idx, row in df.iterrows():
                ticker = str(row["ticker"]) if ticker_in_col else str(idx)
                try:
                    val = float(row[size_col])
                    if pd.notna(val) and val > 0:
                        sizing[ticker] = val
                except (TypeError, ValueError):
                    pass
            break

        return sizing

    # ==================================================================
    #  EXIT METADATA EXTRACTION
    # ==================================================================
    def _extract_exit_metadata(self, output: Dict) -> Dict[str, Dict[str, Any]]:
        """Extract per-ticker exit diagnostic fields from the pipeline output."""
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
    #  EXIT CHURN DIAGNOSTIC
    # ==================================================================
    def _log_exit_churn_diagnostic(self, output: Dict, day) -> None:
        """When >60% of the universe is flagged for exit, log diagnostics."""
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
    #  PRE-COMPUTATION
    # ==================================================================
    def _precompute_all(self):
        """Pre-compute per-ticker indicators, RS z-scores, and benchmark regime."""
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
    #  CURRENT NAV CALCULATION
    # ==================================================================
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

        # ── log config ────────────────────────────────────────
        brp = self._buy_ranking_params
        log.info(
            "[%s] BUY RANKING CONFIG: momentum_tilt=%.2f  "
            "min_vol=%.3f  vol_preference=%.2f  "
            "min_rszscore=%.2f  use_realized_vol=%s",
            self.config_name,
            brp.get("momentum_tilt", 0.4),
            brp.get("min_trailing_vol", 0.0),
            brp.get("vol_preference", 0.3),
            brp.get("min_rszscore", -99),
            brp.get("use_realized_vol", True),
        )
        log.info(
            "[%s] SIGNAL CAP CONFIG: strong_buy_limit=%d  max_buy_signals=%d",
            self.config_name, self._strong_buy_limit, self._max_buy_signals,
        )

        log.info(
            "[%s] backtest %s → %s  (%d days, %d tickers)  "
            "max_positions=%d  min_hold=%dd  trailing_stop=%.0f%%  "
            "max_hold=%dd",
            self.config_name, self.start_date, self.end_date,
            len(trading_days), len(tickers), self.max_positions,
            min_hold,
            sig_params.get("trailing_stop_pct", 0.18) * 100,
            sig_params.get("max_hold_days", 120),
        )

        prev_actions: Dict[str, str] = {}
        prev_scores: Dict[str, float] = {}
        prev_sizes: Dict[str, float] = {}
        prev_momentum: Dict[str, Dict[str, float]] = {}
        prev_trailing_vol: Dict[str, float] = {}
        prev_exit_metadata: Dict[str, Dict[str, Any]] = {}
        prev_exit_tickers: Set[str] = set()
        bench_start_close: float | None = None

        # Diagnostics
        cash_utilization_pcts: List[float] = []
        daily_signal_counts: List[int] = []

        for i, day in enumerate(trading_days):

            # ── 1. run pipeline (fast path) ───────────────────
            try:
                output = self._run_pipeline_fast(day, tickers)

                # Debug: log columns only once
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

                # ══════════════════════════════════════════════════
                #  SIGNAL CAPPING — the critical change
                # ══════════════════════════════════════════════════
                raw_buy_count = sum(
                    1 for a in actions.values() if a in ("BUY", "STRONG_BUY")
                )
                actions = self._cap_buy_signals(actions, scores)
                capped_buy_count = sum(
                    1 for a in actions.values() if a in ("BUY", "STRONG_BUY")
                )
                daily_signal_counts.append(capped_buy_count)

                sizes = self._extract_position_sizes(output)
                momentum = self._extract_momentum_metrics(output)

                # Compute trailing vol (beta proxy) for buy candidates
                buy_candidates = [
                    t for t, a in actions.items()
                    if a in ("BUY", "STRONG_BUY")
                ]
                if buy_candidates and brp.get("use_realized_vol", True):
                    trailing_vol = self._compute_trailing_vol(
                        day, buy_candidates,
                        window=brp.get("vol_window", 60),
                    )
                else:
                    trailing_vol = {}

                # ── Extract exit flags DIRECTLY ────────────────
                exit_tickers = self._extract_exit_flags(output, held)

                # Merge direct exit flags into actions
                for ticker in exit_tickers:
                    if ticker not in actions or actions[ticker] != "SELL":
                        if ticker in actions and actions[ticker] in ("BUY", "STRONG_BUY"):
                            log.warning(
                                "[%s] %s CONFLICT: %s has BUY action but "
                                "sigexit_v2=1 (held). Forcing SELL.",
                                self.config_name,
                                day.strftime("%Y-%m-%d"),
                                ticker,
                            )
                        actions[ticker] = "SELL"

                # Extract exit metadata
                exit_metadata = self._extract_exit_metadata(output)

            except Exception as exc:
                log.warning(
                    "[%s] pipeline error %s: %s",
                    self.config_name, day.strftime("%Y-%m-%d"), exc,
                )
                actions, scores, sizes = {}, {}, {}
                momentum, trailing_vol = {}, {}
                exit_metadata = {}
                exit_tickers = set()
                daily_signal_counts.append(0)

            # ── 2. log signals ────────────────────────────────
            buy_sigs = [t for t, a in actions.items() if a in ("BUY", "STRONG_BUY")]
            sell_sigs = [t for t, a in actions.items() if a == "SELL"]
            strong_buys = [t for t, a in actions.items() if a == "STRONG_BUY"]
            if buy_sigs or sell_sigs:
                log.info(
                    "[%s] %s  STRONG_BUY(%d) %s | BUY(%d) | SELL %s",
                    self.config_name,
                    day.strftime("%Y-%m-%d"),
                    len(strong_buys),
                    strong_buys[:5] if strong_buys else "—",
                    len(buy_sigs) - len(strong_buys),
                    sell_sigs if sell_sigs else "—",
                )

            # ── 3. execute PREVIOUS day's signals at TODAY's open ─
            prices_open = self._prices_fast(day, tickers, field="open")

            if i > 0:
                n_closed_before = len(tracker.closed_trades)

                # 3a — force-exit stale / stopped-out positions
                tracker.force_exits(day, prices_open)
                n_force_closed = len(tracker.closed_trades) - n_closed_before

                # 3b — signal-driven sells + score-ranked buys
                if prev_actions:
                    current_nav = self._compute_current_nav(tracker)

                    tracker.process_signals(
                        day, prev_actions, prices_open,
                        scores=prev_scores,
                        current_nav=current_nav,
                        position_sizes=prev_sizes,
                        momentum_metrics=prev_momentum,
                        trailing_vols=prev_trailing_vol,
                        buy_ranking_params=self._buy_ranking_params,
                    )

                # 3c — upgrade weak held positions
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
                        if j < n_force_closed:
                            source = trade.get("exit_type", "force_exit_other")
                            trade.setdefault("exit_source", source)
                            self._exit_source_counts[
                                source if source in self._exit_source_counts
                                else "force_exit_other"
                            ] += 1
                        else:
                            trade.setdefault("exit_source", "signal_exit")
                            self._exit_source_counts["signal_exit"] += 1

                        if ticker in prev_exit_metadata:
                            trade.update(prev_exit_metadata[ticker])
                        elif "exit_reason" not in trade:
                            trade["exit_reason"] = trade.get(
                                "exit_source", "unknown"
                            )

            # ── 4. mark-to-market at close ────────────────────
            prices_close = self._prices_fast(day, tickers, field="close")
            port_value = tracker.mark_to_market(day, prices_close)

            # refresh held-position scores
            tracker.update_scores(scores)

            # ── 5. cash utilization tracking ──────────────────
            if port_value > 0:
                cash_pct = 100.0 * tracker.cash / port_value
                cash_utilization_pcts.append(cash_pct)
                if i > 20 and i % 20 == 0:
                    recent_cash = cash_utilization_pcts[-20:]
                    avg_cash = sum(recent_cash) / len(recent_cash)
                    if avg_cash > 40:
                        log.warning(
                            "[%s] %s CASH DRAG: avg cash=%.1f%% over last "
                            "20 days. NAV=$%s but only %d/%d slots filled.",
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
                "n_buys_raw": raw_buy_count if 'raw_buy_count' in dir() else 0,
                "n_buys_capped": capped_buy_count if 'capped_buy_count' in dir() else 0,
                "n_strong_buys": len(strong_buys) if 'strong_buys' in dir() else 0,
                "n_sells": sum(1 for a in actions.values()
                               if a == "SELL"),
                "n_exit_flags": len(exit_tickers),
            })

            prev_actions = actions
            prev_scores = scores
            prev_sizes = sizes
            prev_momentum = momentum
            prev_trailing_vol = trailing_vol
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
        if daily_signal_counts:
            avg_signals = sum(daily_signal_counts) / len(daily_signal_counts)
            max_signals = max(daily_signal_counts)
            log.info(
                "[%s] SIGNAL STATS: avg=%.1f/day  max=%d/day  "
                "cap=%d  (raw pipeline avg was higher)",
                self.config_name, avg_signals, max_signals,
                self._max_buy_signals,
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
            "avg_daily_signals": (
                sum(daily_signal_counts) / len(daily_signal_counts)
                if daily_signal_counts else 0
            ),
        }

    # ==================================================================
    #  FAST PIPELINE
    # ==================================================================
    def _run_pipeline_fast(self, day, tickers) -> Dict:
        from refactor.pipeline_v2 import run_pipeline_v2

        cutoff = pd.Timestamp(day)
        lookback = self.data_source.lookback_bars

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
    #  Fast price lookup
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
                    if ticker not in held_tickers:
                        actions[ticker] = raw_action

                elif raw_action in ("SELL", "EXIT", "STRONG_SELL"):
                    if ticker in held_tickers:
                        actions[ticker] = "SELL"

            break

        return actions