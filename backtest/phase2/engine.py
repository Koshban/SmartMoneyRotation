"""
backtest/phase2/engine.py
Core backtesting engine.

Replays history day-by-day, runs the pipeline on each day's visible
data, and feeds actions to the portfolio tracker.
"""
from __future__ import annotations

import logging
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

        # result accumulators
        self.daily_log: List[Dict] = []
        self.equity_curve: List[tuple] = []
        self.action_history: Dict = {}

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

        tracker = PortfolioTracker(
            initial_capital=self.initial_capital,
            max_positions=self.max_positions,
            commission_rate=self.commission_rate,
            slippage_rate=self.slippage_rate,
        )

        log.info(
            "[%s] backtest %s → %s  (%d days, %d tickers)",
            self.config_name,
            self.start_date,
            self.end_date,
            len(trading_days),
            len(tickers),
        )

        prev_actions: Dict[str, str] = {}
        bench_start_close: float | None = None

        for i, day in enumerate(trading_days):

            # 1 — set visible data window ──────────────────────────
            self.data_source.set_cutoff(day)

            # 2 — run pipeline ─────────────────────────────────────
            try:
                output = self._run_pipeline(day, tickers)
                actions = self._extract_actions(output)
            except Exception as exc:
                log.warning(
                    "[%s] pipeline error %s: %s",
                    self.config_name, day.strftime("%Y-%m-%d"), exc,
                )
                actions = {}

            # 3 — log signals that will drive tomorrow's execution ─
            buy_sigs = [t for t, a in actions.items() if a in ("BUY", "STRONG_BUY")]
            sell_sigs = [t for t, a in actions.items() if a == "SELL"]
            if buy_sigs or sell_sigs:
                log.info(
                    "[%s] %s  signals → BUY %s | SELL %s",
                    self.config_name,
                    day.strftime("%Y-%m-%d"),
                    buy_sigs if buy_sigs else "—",
                    sell_sigs if sell_sigs else "—",
                )

            # 4 — execute PREVIOUS day's signals at TODAY's open ───
            prices_open = self._prices(day, tickers, field="open")
            if i > 0 and prev_actions:
                tracker.process_signals(day, prev_actions, prices_open)

            # 5 — mark-to-market at close ──────────────────────────
            prices_close = self._prices(day, tickers, field="close")
            port_value = tracker.mark_to_market(day, prices_close)

            # 6 — benchmark value (buy-and-hold from day 1) ───────
            bench_value = self._benchmark_value(day, bench_start_close)
            if bench_value is not None and bench_start_close is None:
                # first day — initialise benchmark baseline
                bench_df = self.data_source.fetch_benchmark()
                if not bench_df.empty and day in bench_df.index:
                    bench_start_close = float(bench_df.loc[day, "close"])
                    bench_value = self.initial_capital

            # 7 — record ──────────────────────────────────────────
            self.equity_curve.append((day, port_value, bench_value))
            self.daily_log.append(
                {
                    "date": day,
                    "portfolio_value": port_value,
                    "benchmark_value": bench_value,
                    "cash": tracker.cash,
                    "n_positions": len(tracker.positions),
                    "positions": list(tracker.positions.keys()),
                    "n_buys": sum(
                        1 for a in actions.values()
                        if a in ("BUY", "STRONG_BUY")
                    ),
                    "n_sells": sum(
                        1 for a in actions.values() if a == "SELL"
                    ),
                }
            )
            prev_actions = actions
            self.action_history[day] = actions

            # Progress every 50 days or on the last day
            if (i + 1) % 50 == 0 or i == len(trading_days) - 1:
                log.info(
                    "[%s] %d/%d  %s  portfolio=$%s  pos=%d",
                    self.config_name,
                    i + 1,
                    len(trading_days),
                    day.strftime("%Y-%m-%d"),
                    f"{port_value:,.0f}",
                    len(tracker.positions),
                )

        # ── assemble results ──────────────────────────────────────
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
    #  Benchmark helper
    # ==================================================================
    def _benchmark_value(
        self, day, bench_start_close: float | None
    ) -> float | None:
        """
        Return the benchmark portfolio value for *day* assuming
        a buy-and-hold from the first trading day.
        """
        if bench_start_close is None:
            # Will be initialised on first successful fetch
            bench_df = self.data_source.fetch_benchmark()
            if not bench_df.empty and day in bench_df.index:
                return self.initial_capital  # first day
            return None

        bench_df = self.data_source.fetch_benchmark()
        if bench_df.empty or day not in bench_df.index:
            return None
        bench_close_today = float(bench_df.loc[day, "close"])
        return self.initial_capital * (bench_close_today / bench_start_close)

    # ==================================================================
    #  >>> INTEGRATION POINT 1 — run the pipeline for one day <<<
    # ==================================================================
    def _run_pipeline(self, day, tickers) -> Dict:
        """
        Translate engine state into the arguments run_pipeline_v2
        actually expects.
        """
        from refactor.pipeline_v2 import run_pipeline_v2

        # 1 — build tradable_frames: {ticker: visible_df}
        tradable_frames = {}
        for t in tickers:
            df = self.data_source.fetch(t)
            if not df.empty:
                tradable_frames[t] = df

        # 2 — benchmark DataFrame via dedicated accessor
        bench_df = self.data_source.fetch_benchmark()
        if bench_df is None or bench_df.empty:
            bench_ticker = self.config.get("BENCH_TICKER", "SPY")
            bench_df = self.data_source.fetch(bench_ticker)
        if bench_df is None or bench_df.empty:
            raise ValueError(
                f"Benchmark returned empty frame on {day}. "
                f"Ensure benchmark_ticker is set in "
                f"BacktestDataSource.from_parquet()."
            )

        # 3 — breadth (optional — pipeline computes its own)
        breadth_df = None

        # 4 — leadership frames (optional)
        leadership_frames = None
        leadership_tickers = self.config.get("LEADERSHIP_TICKERS", None)
        if leadership_tickers:
            leadership_frames = {}
            for t in leadership_tickers:
                df = self.data_source.fetch(t)
                if not df.empty:
                    leadership_frames[t] = df

        # 5 — pack config the way run_pipeline_v2 unpacks it
        pipeline_config = {
            "scoring_weights": self.config.get("SCORINGWEIGHTS_V2"),
            "scoring_params": self.config.get("SCORINGPARAMS_V2"),
            "signal_params": self.config.get("SIGNALPARAMS_V2"),
            "convergence_params": self.config.get("CONVERGENCEPARAMS_V2"),
            "action_params": self.config.get("ACTIONPARAMS_V2"),
        }

        # 6 — portfolio params (optional)
        portfolio_params = self.config.get("PORTFOLIO_PARAMS", None)

        return run_pipeline_v2(
            tradable_frames=tradable_frames,
            bench_df=bench_df,
            breadth_df=breadth_df,
            market=self.market,
            leadership_frames=leadership_frames,
            portfolio_params=portfolio_params,
            config=pipeline_config,
        )

    # ==================================================================
    #  >>> INTEGRATION POINT 2 — extract {ticker: action} from output <<<
    # ==================================================================
    def _extract_actions(self, output: Dict) -> Dict[str, str]:
        """
        Pull {ticker: action_string} from whatever the pipeline returns.
        Tries keys: action_table, snapshot, actions — in that order.
        """
        actions: Dict[str, str] = {}

        for key in ("action_table", "snapshot", "actions"):
            if key not in output:
                continue
            df = output[key]
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            act_col = next(
                (
                    c
                    for c in ("action_v2", "action", "signal")
                    if c in df.columns
                ),
                None,
            )
            if act_col is None:
                continue

            if "ticker" in df.columns:
                for _, row in df.iterrows():
                    actions[row["ticker"]] = str(row[act_col]).upper()
            else:
                for ticker, row in df.iterrows():
                    actions[str(ticker)] = str(row[act_col]).upper()
            break

        return actions

    # ------------------------------------------------------------------
    #  helpers
    # ------------------------------------------------------------------
    def _prices(
        self, day, tickers, field: str = "open"
    ) -> Dict[str, float]:
        prices = {}
        for t in tickers:
            df = self.data_source.fetch(t)
            if not df.empty and day in df.index:
                prices[t] = float(df.loc[day, field])
        return prices