"""
backtest/phase2/engine.py
Core backtesting engine with pre-computed indicators for speed.
"""
from __future__ import annotations

import logging
import time
import pandas as pd
from typing import Any, Dict, List

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
        )

        log.info(
            "[%s] backtest %s → %s  (%d days, %d tickers)  "
            "min_hold=%dd  min_profit=%.0f%%",
            self.config_name, self.start_date, self.end_date,
            len(trading_days), len(tickers), min_hold, min_profit * 100,
        )

        prev_actions: Dict[str, str] = {}
        bench_start_close: float | None = None

        for i, day in enumerate(trading_days):

            # 1 — run pipeline (fast path) ─────────────────────
            try:
                output = self._run_pipeline_fast(day, tickers)
                actions = self._extract_actions(output)
            except Exception as exc:
                log.warning(
                    "[%s] pipeline error %s: %s",
                    self.config_name, day.strftime("%Y-%m-%d"), exc,
                )
                actions = {}

            # 2 — log signals ──────────────────────────────────
            buy_sigs = [t for t, a in actions.items() if a in ("BUY", "STRONG_BUY")]
            sell_sigs = [t for t, a in actions.items() if a == "SELL"]
            if buy_sigs or sell_sigs:
                log.info(
                    "[%s] %s  BUY %s | SELL %d names",
                    self.config_name,
                    day.strftime("%Y-%m-%d"),
                    buy_sigs if buy_sigs else "—",
                    len(sell_sigs),
                )

            # 3 — execute PREVIOUS day's signals at TODAY's open
            prices_open = self._prices_fast(day, tickers, field="open")
            if i > 0 and prev_actions:
                tracker.process_signals(day, prev_actions, prices_open)

            # 4 — mark-to-market at close ──────────────────────
            prices_close = self._prices_fast(day, tickers, field="close")
            port_value = tracker.mark_to_market(day, prices_close)

            # 5 — benchmark value ──────────────────────────────
            bench_value = self._benchmark_value(day, bench_start_close)
            if bench_value is not None and bench_start_close is None:
                bench_df = self.data_source.benchmark_data
                if bench_df is not None and not bench_df.empty and day in bench_df.index:
                    bench_start_close = float(bench_df.loc[day, "close"])
                    bench_value = self.initial_capital

            # 6 — record ──────────────────────────────────────
            self.equity_curve.append((day, port_value, bench_value))
            self.daily_log.append({
                "date": day,
                "portfolio_value": port_value,
                "benchmark_value": bench_value,
                "cash": tracker.cash,
                "n_positions": len(tracker.positions),
                "positions": list(tracker.positions.keys()),
                "n_buys": sum(1 for a in actions.values()
                              if a in ("BUY", "STRONG_BUY")),
                "n_sells": sum(1 for a in actions.values()
                               if a == "SELL"),
            })
            prev_actions = actions
            self.action_history[day] = actions

            if (i + 1) % 50 == 0 or i == len(trading_days) - 1:
                log.info(
                    "[%s] %d/%d  %s  portfolio=$%s  pos=%d  cash=$%s",
                    self.config_name, i + 1, len(trading_days),
                    day.strftime("%Y-%m-%d"),
                    f"{port_value:,.0f}",
                    len(tracker.positions),
                    f"{tracker.cash:,.0f}",
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
        return self.initial_capital * (float(bench_df.loc[day, "close"]) / bench_start_close)

    # ==================================================================
    #  Extract actions from pipeline output
    # ==================================================================
    def _extract_actions(self, output: Dict) -> Dict[str, str]:
        """
        Extract ticker → action mapping from pipeline output.

        Implements 3-state exit gating via sigexit_v2:

          BUY/STRONG_BUY                → BUY   (entry criteria met)
          SELL + sigexit_v2 == 1        → SELL   (genuine exit signal)
          SELL + sigexit_v2 != 1        → omitted from dict (HOLD)
          HOLD / other                  → omitted from dict (HOLD)

        Tickers absent from the returned dict are treated as HOLD by
        the portfolio tracker — existing positions are kept, no new
        entries are made.

        Without this gating, the pipeline labels ~77% of the universe
        as SELL every day (anything not meeting top-15% BUY criteria).
        Positions entered at rank 0.85+ drop to rank 0.70 the next day,
        get queued for exit, and are dumped at min-hold expiry with
        -3% to -8% losses — even though sigexit_v2 == 0.
        """
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

            # ── detect exit-signal column for 3-state gating ──
            exit_col = next(
                (c for c in ("sigexit_v2", "sig_exit_v2", "exit_signal")
                 if c in df.columns),
                None,
            )
            use_3state = exit_col is not None

            if not use_3state:
                log.warning(
                    "[%s] sigexit_v2 not found in pipeline output '%s' "
                    "— SELL gating disabled, expect high exit churn",
                    self.config_name, key,
                )

            ticker_in_col = "ticker" in df.columns
            n_gated = 0

            for idx, row in df.iterrows():
                ticker = (
                    str(row["ticker"]) if ticker_in_col else str(idx)
                )
                raw_action = str(row[act_col]).upper()

                if raw_action in ("BUY", "STRONG_BUY"):
                    actions[ticker] = raw_action

                elif raw_action in ("SELL", "EXIT", "STRONG_SELL"):
                    if use_3state:
                        try:
                            is_genuine_exit = (
                                float(row[exit_col]) >= 1.0
                            )
                        except (TypeError, ValueError):
                            # NaN or unparseable → safe default: allow
                            is_genuine_exit = True

                        if is_genuine_exit:
                            actions[ticker] = "SELL"
                        else:
                            n_gated += 1
                            # omit → tracker treats as HOLD
                    else:
                        # no exit column → preserve old behavior
                        actions[ticker] = "SELL"

                # else: HOLD / unknown → omit from actions dict

            if use_3state:
                n_sells = sum(
                    1 for a in actions.values() if a == "SELL"
                )
                log.debug(
                    "[%s] exit gating: %d genuine SELLs, "
                    "%d SELL→HOLD (gated)",
                    self.config_name, n_sells, n_gated,
                )

            break

        return actions