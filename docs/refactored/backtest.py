"""
backtest/phase2/compare.py
Run two configs side-by-side and print a comparison table.
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
def build_config_dict(
    vol_regime_params: dict,
    scoring_weights: dict,
    scoring_params: dict,
    signal_params: dict,
    convergence_params: dict,
    action_params: dict | None = None,
) -> Dict[str, Any]:
    """Bundle the config blocks into one dict for the engine.

    Keys must match what run_pipeline_v2 reads via config.get(...).
    """
    cfg = {
        "vol_regime_params": vol_regime_params,
        "scoring_weights": scoring_weights,
        "scoring_params": scoring_params,
        "signal_params": signal_params,
        "convergence_params": convergence_params,
    }
    if action_params is not None:
        cfg["action_params"] = action_params
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
            # for drawdown / vol: less negative is better
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

############################################
"""
backtest/phase2/data_source.py
Date-aware data source wrapper for backtesting.

Loads from a single parquet file per market (data/{market}_cash.parquet),
filters to a given ticker universe, and presents a sliding window up to
the current backtest date.

Expected parquet schema (long format):
    date | ticker | open | high | low | close | volume

The 'date' column should be parseable as datetime.  Column names are
normalised to lowercase on load.  If the parquet uses 'symbol' instead
of 'ticker', that works too.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

log = logging.getLogger(__name__)


class BacktestDataSource:

    def __init__(
        self,
        ticker_data: Dict[str, pd.DataFrame],
        benchmark_data: Optional[pd.DataFrame] = None,
        lookback_bars: int = 300,
    ):
        self.ticker_data = ticker_data
        self.benchmark_data = benchmark_data
        self.lookback_bars = lookback_bars
        self._cutoff: Optional[pd.Timestamp] = None

    # ==================================================================
    #  Factory — build from parquet + universe list
    # ==================================================================
    @classmethod
    def from_parquet(
        cls,
        parquet_path: str | Path,
        tickers: List[str],
        benchmark_ticker: Optional[str] = None,
        lookback_bars: int = 300,
    ) -> "BacktestDataSource":
        """
        Load ``data/{market}_cash.parquet`` and split into per-ticker
        DataFrames keyed by ticker with a DatetimeIndex.

        Args:
            parquet_path:     Path to the parquet file.
            tickers:          Universe tickers to keep.
            benchmark_ticker: e.g. "2800.HK".  Looked up in the same
                              parquet; ignored if not found.
            lookback_bars:    Max bars per fetch() call.
        """
        path = Path(parquet_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        raw = pd.read_parquet(path)
        raw.columns = raw.columns.str.strip().str.lower()

        # ── normalise ticker column ──────────────────────────────
        ticker_col = _find_column(raw, ("ticker", "symbol", "code", "stock"))
        if ticker_col is None:
            raise KeyError(
                f"Cannot find a ticker/symbol column in {path}.  "
                f"Columns present: {list(raw.columns)}"
            )

        # ── normalise date column / index ────────────────────────
        date_col = _find_column(raw, ("date", "datetime", "timestamp", "time"))
        if date_col is not None:
            raw[date_col] = pd.to_datetime(raw[date_col])
        elif isinstance(raw.index, pd.DatetimeIndex):
            raw = raw.reset_index()
            date_col = raw.columns[0]  # the old index name
        else:
            raise KeyError(
                f"Cannot find a date column in {path}.  "
                f"Columns present: {list(raw.columns)}"
            )

        # ── normalise OHLCV column names ─────────────────────────
        rename_map = {}
        for target, candidates in [
            ("open",   ("open", "o")),
            ("high",   ("high", "h")),
            ("low",    ("low", "l")),
            ("close",  ("close", "c", "adj close", "adj_close", "adjclose")),
            ("volume", ("volume", "vol", "v")),
        ]:
            found = _find_column(raw, candidates)
            if found and found != target:
                rename_map[found] = target
        if rename_map:
            raw = raw.rename(columns=rename_map)

        needed = {"open", "high", "low", "close", "volume"}
        missing = needed - set(raw.columns)
        if missing:
            raise KeyError(f"Missing OHLCV columns after rename: {missing}")

        # ── filter to universe tickers ───────────────────────────
        all_tickers_in_file = set(raw[ticker_col].unique())
        want = set(tickers)
        if benchmark_ticker:
            want.add(benchmark_ticker)

        found_tickers = want & all_tickers_in_file
        not_found = want - all_tickers_in_file
        if not_found:
            log.warning(
                "%d tickers not in parquet and will be skipped: %s",
                len(not_found),
                sorted(not_found)[:20],
            )

        raw = raw[raw[ticker_col].isin(found_tickers)].copy()
        raw = raw.sort_values([ticker_col, date_col])

        # ── split into {ticker: DataFrame} ───────────────────────
        ticker_data: Dict[str, pd.DataFrame] = {}
        benchmark_data: Optional[pd.DataFrame] = None

        for tkr, group in raw.groupby(ticker_col):
            df = (
                group.set_index(date_col)[["open", "high", "low", "close", "volume"]]
                .sort_index()
                .copy()
            )
            df.index.name = "date"
            # drop exact duplicate indices if any
            df = df[~df.index.duplicated(keep="last")]
            ticker_data[tkr] = df

            if tkr == benchmark_ticker:
                benchmark_data = df.copy()

        log.info(
            "Loaded %d tickers from %s  (date range %s → %s)",
            len(ticker_data),
            path.name,
            raw[date_col].min().strftime("%Y-%m-%d"),
            raw[date_col].max().strftime("%Y-%m-%d"),
        )

        return cls(
            ticker_data=ticker_data,
            benchmark_data=benchmark_data,
            lookback_bars=lookback_bars,
        )

    # ==================================================================
    #  Cutoff management
    # ==================================================================
    def set_cutoff(self, dt) -> None:
        self._cutoff = pd.Timestamp(dt)

    # ==================================================================
    #  Data access
    # ==================================================================
    def fetch(self, ticker: str) -> pd.DataFrame:
        if ticker not in self.ticker_data:
            return pd.DataFrame()
        df = self.ticker_data[ticker]
        if self._cutoff is not None:
            df = df.loc[df.index <= self._cutoff]
        if len(df) > self.lookback_bars:
            df = df.iloc[-self.lookback_bars:]
        return df.copy()

    def fetch_benchmark(self) -> pd.DataFrame:
        if self.benchmark_data is None:
            return pd.DataFrame()
        df = self.benchmark_data
        if self._cutoff is not None:
            df = df.loc[df.index <= self._cutoff]
        if len(df) > self.lookback_bars:
            df = df.iloc[-self.lookback_bars:]
        return df.copy()

    def get_tickers(self) -> List[str]:
        return list(self.ticker_data.keys())

    def get_trading_days(self, start: str, end: str) -> List[pd.Timestamp]:
        all_dates: set = set()
        for df in self.ticker_data.values():
            all_dates.update(df.index.tolist())
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        return sorted(d for d in all_dates if s <= d <= e)

    def get_date_range(self) -> tuple:
        """Min and max dates across all loaded tickers."""
        lo, hi = pd.Timestamp.max, pd.Timestamp.min
        for df in self.ticker_data.values():
            if not df.empty:
                lo = min(lo, df.index.min())
                hi = max(hi, df.index.max())
        return lo, hi


# ------------------------------------------------------------------
#  helpers
# ------------------------------------------------------------------

def _find_column(df: pd.DataFrame, candidates: tuple) -> Optional[str]:
    """Return the first column name from *candidates* present in df."""
    cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in cols_lower:
            return cols_lower[c]
    return None

#############################################
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
            "[%s] backtest %s -> %s  (%d days, %d tickers)",
            self.config_name,
            self.start_date,
            self.end_date,
            len(trading_days),
            len(tickers),
        )

        prev_actions: Dict[str, str] = {}

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
                    self.config_name, day, exc,
                )
                actions = {}

            # 3 — execute PREVIOUS day's signals at TODAY's open ───
            prices_open = self._prices(day, tickers, field="open")
            if i > 0 and prev_actions:
                tracker.process_signals(day, prev_actions, prices_open)

            # 4 — mark-to-market at close ──────────────────────────
            prices_close = self._prices(day, tickers, field="close")
            port_value = tracker.mark_to_market(day, prices_close)

            # 5 — record ──────────────────────────────────────────
            self.equity_curve.append((day, port_value))
            self.daily_log.append(
                {
                    "date": day,
                    "portfolio_value": port_value,
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

            if (i + 1) % 20 == 0 or i == len(trading_days) - 1:
                log.info(
                    "[%s] %d/%d  %s  $%s  pos=%d",
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
                self.equity_curve, columns=["date", "value"]
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
            # fallback: try fetching benchmark by ticker name from
            # ticker_data in case it was loaded as a regular ticker
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

            # find action column
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

            # find ticker column or use index
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
    
###############################################################################

"""
backtest/phase2/metrics.py
Performance metrics from backtest results.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Any, Dict


def compute_metrics(results: Dict[str, Any]) -> Dict[str, Any]:
    equity = results["equity_curve"].copy()
    trades = results.get("trade_log", pd.DataFrame())
    initial = results["initial_capital"]
    final = results["final_value"]

    # ── returns ───────────────────────────────────────────────────
    total_ret = (final - initial) / initial
    equity["daily_ret"] = equity["value"].pct_change()
    daily = equity["daily_ret"].dropna()

    n_days = len(equity)
    ann_factor = 252 / max(n_days, 1)
    ann_ret = (1 + total_ret) ** ann_factor - 1

    # ── volatility ────────────────────────────────────────────────
    ann_vol = daily.std() * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

    down = daily[daily < 0]
    down_vol = down.std() * np.sqrt(252) if len(down) > 0 else 0.001
    sortino = ann_ret / down_vol

    # ── drawdown ──────────────────────────────────────────────────
    equity["peak"] = equity["value"].cummax()
    equity["dd"] = (equity["value"] - equity["peak"]) / equity["peak"]
    max_dd = equity["dd"].min()

    in_dd = equity["dd"] < 0
    if in_dd.any():
        groups = (~in_dd).cumsum()
        max_dd_dur = int(in_dd.groupby(groups).sum().max())
    else:
        max_dd_dur = 0

    calmar = ann_ret / abs(max_dd) if max_dd != 0 else 0.0

    # ── portfolio utilisation ─────────────────────────────────────
    dl = results.get("daily_log", pd.DataFrame())
    avg_pos = dl["n_positions"].mean() if not dl.empty else 0
    max_pos = int(dl["n_positions"].max()) if not dl.empty else 0
    total_buys = int(dl["n_buys"].sum()) if not dl.empty else 0
    total_sells = int(dl["n_sells"].sum()) if not dl.empty else 0

    # ── trade stats ───────────────────────────────────────────────
    tm = _trade_metrics(trades)

    return {
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "final_value": final,
        "annualized_vol": ann_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
        "max_dd_duration_days": max_dd_dur,
        "calmar_ratio": calmar,
        "avg_positions": avg_pos,
        "max_positions_held": max_pos,
        "trading_days": n_days,
        "total_buy_signals": total_buys,
        "total_sell_signals": total_sells,
        **tm,
    }


# ------------------------------------------------------------------
def _trade_metrics(trades: pd.DataFrame) -> Dict[str, Any]:
    empty = {
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "win_rate": 0.0,
        "avg_win_pct": 0.0,
        "avg_loss_pct": 0.0,
        "profit_factor": 0.0,
        "avg_pnl_pct": 0.0,
        "median_pnl_pct": 0.0,
        "avg_holding_days": 0.0,
        "best_trade_pct": 0.0,
        "worst_trade_pct": 0.0,
        "expectancy_dollar": 0.0,
    }
    if trades.empty:
        return empty

    w = trades[trades["pnl"] > 0]
    l = trades[trades["pnl"] <= 0]
    n = len(trades)

    gross_win = w["pnl"].sum() if not w.empty else 0.0
    gross_loss = abs(l["pnl"].sum()) if not l.empty else 0.001

    return {
        "total_trades": n,
        "winning_trades": len(w),
        "losing_trades": len(l),
        "win_rate": len(w) / n,
        "avg_win_pct": w["pnl_pct"].mean() if not w.empty else 0.0,
        "avg_loss_pct": l["pnl_pct"].mean() if not l.empty else 0.0,
        "profit_factor": gross_win / gross_loss,
        "avg_pnl_pct": trades["pnl_pct"].mean(),
        "median_pnl_pct": trades["pnl_pct"].median(),
        "avg_holding_days": trades["holding_days"].mean(),
        "best_trade_pct": trades["pnl_pct"].max(),
        "worst_trade_pct": trades["pnl_pct"].min(),
        "expectancy_dollar": trades["pnl"].mean(),
    }


###################################################################
"""
backtest/phase2/run_backtest.py
CLI entry point.

    python -m backtest.run_backtest --market HK --start 2025-10-01 --end 2026-04-20
    python -m backtest.run_backtest --market IN --start 2025-06-01 --end 2026-04-20
    python -m backtest.run_backtest --market US --start 2025-06-01 --end 2026-04-20
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.console import Console

from backtest.phase2.data_source import BacktestDataSource
from backtest.phase2.compare import build_config_dict, run_comparison, print_comparison

console = Console()
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════════
LOGS_DIR = Path("logs") / "backtest"


def _setup_logging(market: str, level: int = logging.INFO) -> Path:
    """
    Configure the root logger to write to both:
      • stderr   (concise, INFO+)
      • file     (verbose, DEBUG+)

    Returns the path to the log file.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"backtest_{market}_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # ── file handler (verbose) ────────────────────────────────
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    # ── console handler (concise) ─────────────────────────────
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(ch)

    return log_file


def _log_and_print(msg: str, rich_msg: str | None = None, level: int = logging.INFO):
    """Log a plain-text message AND print a (possibly styled) version to Rich console."""
    log.log(level, msg)
    console.print(rich_msg if rich_msg is not None else msg)


# ══════════════════════════════════════════════════════════════════
#  UNIVERSE + BENCHMARK MAPPING
# ══════════════════════════════════════════════════════════════════

from common.universe import get_universe_for_market

BENCHMARKS = {
    "HK": "2800.HK",      # Tracker Fund of Hong Kong
    "IN": "NIFTYBEES.NS",  # Nifty ETF (adjust to your universe)
    "US": "SPY",           # S&P 500 ETF
}


def _get_tickers(market: str) -> list[str]:
    """
    Pull the ticker list from common/universe.py via get_universe_for_market().
    Supports 'US', 'HK', and 'IN'.
    """
    return get_universe_for_market(market)


# ══════════════════════════════════════════════════════════════════
#  PARQUET DATA LOADING
# ══════════════════════════════════════════════════════════════════

def load_data(
    market: str,
    data_dir: str = "data",
    lookback_bars: int = 300,
) -> BacktestDataSource:
    """
    Load from  data/{market}_cash.parquet  and filter to the
    universe defined in common/universe.py.
    """
    parquet_path = Path(data_dir) / f"{market}_cash.parquet"
    tickers = _get_tickers(market)
    benchmark = BENCHMARKS.get(market)

    plain = (
        f"Market: {market}   Universe: {len(tickers)} tickers   "
        f"Benchmark: {benchmark or 'none'}   File: {parquet_path}"
    )
    rich = (
        f"[bold]Market:[/] {market}   "
        f"[bold]Universe:[/] {len(tickers)} tickers   "
        f"[bold]Benchmark:[/] {benchmark or 'none'}   "
        f"[bold]File:[/] {parquet_path}"
    )
    _log_and_print(plain, rich)

    ds = BacktestDataSource.from_parquet(
        parquet_path=parquet_path,
        tickers=tickers,
        benchmark_ticker=benchmark,
        lookback_bars=lookback_bars,
    )

    lo, hi = ds.get_date_range()
    loaded = ds.get_tickers()

    plain = (
        f"Loaded {len(loaded)}/{len(tickers)} tickers  "
        f"({lo.strftime('%Y-%m-%d')} -> {hi.strftime('%Y-%m-%d')})"
    )
    rich = (
        f"[green]Loaded {len(loaded)}/{len(tickers)} tickers  "
        f"({lo.strftime('%Y-%m-%d')} → {hi.strftime('%Y-%m-%d')})[/]"
    )
    _log_and_print(plain, rich)

    if len(loaded) < len(tickers):
        missing = set(tickers) - set(loaded)
        preview = sorted(missing)[:15]
        suffix = "..." if len(missing) > 15 else ""
        plain = f"Missing {len(missing)} tickers: {preview}{suffix}"
        rich = f"[yellow]Missing {len(missing)} tickers: {preview}{suffix}[/]"
        _log_and_print(plain, rich, level=logging.WARNING)

    return ds


# ══════════════════════════════════════════════════════════════════
#  TWO CONFIGS TO COMPARE
# ══════════════════════════════════════════════════════════════════

_VOL_COMMON = {
    "atrp_window": 14, "realized_vol_window": 20,
    "dispersion_window": 20, "gap_window": 20,
    "calm_atrp_max": 0.035, "volatile_atrp_max": 0.060,
    "calm_rvol_max": 0.28, "volatile_rvol_max": 0.42,
    "volatile_gap_rate": 0.18, "chaotic_gap_rate": 0.28,
    "calm_dispersion_max": 0.022, "volatile_dispersion_max": 0.040,
    "score_weights": {"atrp": 0.35, "realized_vol": 0.35,
                      "gap_rate": 0.15, "dispersion": 0.15},
}

CONFIG_LOOSE = build_config_dict(
    vol_regime_params=_VOL_COMMON,
    scoring_weights={"trend": 0.38, "participation": 0.22, "risk": 0.25, "regime": 0.15},
    scoring_params={
        "trend": {"w_stock_rs": 0.45, "w_sector_rs": 0.25, "w_rs_accel": 0.15, "w_trend_confirm": 0.15},
        "participation": {"w_rvol": 0.35, "w_obv": 0.30, "w_adline": 0.20, "w_dollar_volume": 0.15},
        "risk": {"w_vol_penalty": 0.35, "w_liquidity_penalty": 0.25, "w_gap_penalty": 0.20, "w_extension_penalty": 0.20},
        "regime": {"w_breadth": 0.60, "w_vol_regime": 0.40},
        "penalties": {
            "rsi_soft_low": 38.0, "rsi_soft_high": 78.0, "adx_soft_min": 16.0,
            "atrp_high": 0.07, "extension_warn": 0.12, "extension_bad": 0.22,
            "illiquidity_bad": 0.015,
        },
    },
    signal_params={
        "base_entry_threshold": 0.58, "base_exit_threshold": 0.42,
        "allowed_rs_regimes": ("leading", "improving"),
        "blocked_sector_regimes": ("lagging",),
        "hard_block_breadth_regimes": ("critical",),
        "hard_block_vol_regimes": ("chaotic",),
        "continuation_min_trend": 0.62, "pullback_min_trend": 0.68,
        "pullback_max_short_extension": 0.04, "pullback_rsi_max": 58.0,
        "cooldown_days": 4,
        "regime_entry_adjustment": {"calm": 0.00, "volatile": 0.03, "chaotic": 0.10},
        "breadth_entry_adjustment": {"strong": -0.01, "neutral": 0.02, "weak": 0.07, "critical": 0.12, "unknown": 0.00},
        "size_multipliers": {"calm": 1.00, "volatile": 0.70, "chaotic": 0.35},
    },
    convergence_params={
        "tiers": {"aligned_long": 4, "rotation_long_only": 3, "score_long_only": 2, "mixed": 1, "avoid": 0},
        "adjustments": {"calm": 0.04, "volatile": 0.02, "chaotic": 0.00},
    },
)

CONFIG_TIGHT = build_config_dict(
    vol_regime_params=_VOL_COMMON,
    scoring_weights={"trend": 0.36, "participation": 0.18, "risk": 0.26, "regime": 0.20},
    scoring_params={
        "trend": {"w_stock_rs": 0.42, "w_sector_rs": 0.28, "w_rs_accel": 0.15, "w_trend_confirm": 0.15},
        "participation": {"w_rvol": 0.35, "w_obv": 0.25, "w_adline": 0.20, "w_dollar_volume": 0.20},
        "risk": {"w_vol_penalty": 0.32, "w_liquidity_penalty": 0.23, "w_gap_penalty": 0.20, "w_extension_penalty": 0.25},
        "regime": {"w_breadth": 0.65, "w_vol_regime": 0.35},
        "penalties": {
            "rsi_soft_low": 40.0, "rsi_soft_high": 76.0, "adx_soft_min": 18.0,
            "atrp_high": 0.065, "extension_warn": 0.10, "extension_bad": 0.18,
            "illiquidity_bad": 0.012,
        },
    },
    signal_params={
        "base_entry_threshold": 0.60, "base_exit_threshold": 0.44,
        "allowed_rs_regimes": ("leading", "improving"),
        "blocked_sector_regimes": ("lagging",),
        "hard_block_breadth_regimes": ("critical",),
        "hard_block_vol_regimes": ("chaotic",),
        "continuation_min_trend": 0.64, "pullback_min_trend": 0.70,
        "pullback_max_short_extension": 0.06, "pullback_rsi_max": 62.0,
        "cooldown_days": 4,
        "regime_entry_adjustment": {"calm": 0.00, "volatile": 0.04, "chaotic": 0.12},
        "breadth_entry_adjustment": {"strong": -0.01, "neutral": 0.02, "weak": 0.08, "critical": 0.14, "unknown": 0.03},
        "size_multipliers": {"calm": 1.00, "volatile": 0.65, "chaotic": 0.30},
    },
    convergence_params={
        "tiers": {"aligned_long": 4, "rotation_long_only": 3, "score_long_only": 2, "mixed": 1, "avoid": 0},
        "adjustments": {"calm": 0.04, "volatile": 0.01, "chaotic": 0.00},
    },
)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Backtest config comparison against a market universe"
    )
    parser.add_argument(
        "--market", required=True,
        help="Market code as defined in common/universe.py  (e.g. HK, IN, US)",
    )
    parser.add_argument("--start", required=True, help="Start date  YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date    YYYY-MM-DD")
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--max-positions", type=int, default=12)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--lookback", type=int, default=300,
                        help="Lookback bars for indicator warmup")
    parser.add_argument(
        "--name-a", default="Loose",
        help="Display name for Config A",
    )
    parser.add_argument(
        "--name-b", default="Tight",
        help="Display name for Config B",
    )
    args = parser.parse_args()

    # ── set up dual logging (file + console) ──────────────────
    log_file = _setup_logging(args.market)

    # ── log the full command / args ───────────────────────────
    log.info("=" * 70)
    log.info("Backtest started  market=%s  start=%s  end=%s", args.market, args.start, args.end)
    log.info("capital=%.0f  max_positions=%d  lookback=%d", args.capital, args.max_positions, args.lookback)
    log.info("name_a=%s  name_b=%s  data_dir=%s", args.name_a, args.name_b, args.data_dir)
    log.info("Log file: %s", log_file)
    log.info("=" * 70)

    # ── load data ─────────────────────────────────────────────
    console.rule(f"[bold cyan]Backtest: {args.market} market")
    log.info("--- Loading data for %s market ---", args.market)
    ds = load_data(args.market, args.data_dir, args.lookback)

    # ── run both configs ──────────────────────────────────────
    console.rule("[bold cyan]Running comparison")
    log.info("--- Running comparison: %s vs %s ---", args.name_a, args.name_b)
    ra, rb, comp = run_comparison(
        data_source=ds,
        market=args.market,
        config_a=CONFIG_LOOSE,
        config_b=CONFIG_TIGHT,
        start_date=args.start,
        end_date=args.end,
        name_a=args.name_a,
        name_b=args.name_b,
        initial_capital=args.capital,
        max_positions=args.max_positions,
    )

    # ── display ───────────────────────────────────────────────
    console.print()
    print_comparison(comp, args.name_a, args.name_b, console)

    # Log comparison table to file as well
    log.info("--- Comparison Table ---")
    for _, row in comp.iterrows():
        log.info("  %-30s  %s=%s  %s=%s",
                 row.get("metric", ""),
                 args.name_a, row.get(args.name_a, ""),
                 args.name_b, row.get(args.name_b, ""))

    summary_lines = [
        f"Market          : {args.market}",
        f"Period          : {args.start} -> {args.end}",
        f"Start capital   : ${args.capital:,.0f}",
        f"{args.name_a:15s} : ${ra['final_value']:,.0f}",
        f"{args.name_b:15s} : ${rb['final_value']:,.0f}",
    ]
    for line in summary_lines:
        _log_and_print(f"  {line}", f"  {line}")

    # ── save ──────────────────────────────────────────────────
    out = Path("backtest_results") / args.market
    out.mkdir(parents=True, exist_ok=True)

    comp.to_csv(out / "comparison.csv", index=False)

    eq = ra["equity_curve"].merge(
        rb["equity_curve"], on="date", suffixes=(f"_{args.name_a}", f"_{args.name_b}")
    )
    eq.to_csv(out / "equity_curves.csv", index=False)

    if not ra["trade_log"].empty:
        ra["trade_log"].to_csv(out / f"trades_{args.name_a}.csv", index=False)
    if not rb["trade_log"].empty:
        rb["trade_log"].to_csv(out / f"trades_{args.name_b}.csv", index=False)

    # ── daily log for debugging ───────────────────────────────
    ra["daily_log"].to_csv(out / f"daily_{args.name_a}.csv", index=False)
    rb["daily_log"].to_csv(out / f"daily_{args.name_b}.csv", index=False)

    _log_and_print(
        f"Results saved to {out}/",
        f"\n[green]Results saved to {out}/[/]",
    )
    _log_and_print(
        f"Log file: {log_file}",
        f"[green]Log file: {log_file}[/]",
    )
    log.info("Backtest finished successfully.")


if __name__ == "__main__":
    main()


#################################################
"""
backtest/phase2/tracker.py
Virtual portfolio tracker.

Manages cash, positions, trade execution (with slippage + commission),
and records every completed round-trip for analysis.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
import logging

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float          # total cost including commission


# ------------------------------------------------------------------
class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,   # 10 bps per trade
        slippage_rate: float = 0.0010,     # 10 bps per trade
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

    # ------------------------------------------------------------------
    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
    ) -> None:
        """
        Execute one day's actions.  Sells run first (free up cash),
        then buys fill up to max_positions.

        Args:
            date:    current date
            actions: {ticker: "BUY" | "SELL" | "HOLD" | ...}
            prices:  {ticker: open_price} for execution
        """
        # ── sells first ──────────────────────────────────────────
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker in prices:
                    self._sell(date, ticker, prices[ticker])

        # ── then buys ────────────────────────────────────────────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]
        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            self._buy(date, ticker, prices[ticker])

    # ------------------------------------------------------------------
    def _buy(self, date, ticker: str, raw_price: float) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)

        target_value = min(
            self.initial_capital / self.max_positions,
            self.cash * 0.95,                          # keep 5 % buffer
        )
        if target_value < 1_000:
            return

        shares = int(target_value / exec_price)
        if shares <= 0:
            return

        cost = shares * exec_price
        commission = cost * self.commission_rate
        total = cost + commission
        if total > self.cash:
            return

        self.cash -= total
        self.positions[ticker] = Position(
            ticker=ticker,
            entry_date=date,
            entry_price=exec_price,
            shares=shares,
            cost_basis=total,
        )
        log.debug(
            "BUY  %s  %d @ %.3f  cost $%,.0f  comm $%.0f",
            ticker, shares, exec_price, cost, commission,
        )

    # ------------------------------------------------------------------
    def _sell(self, date, ticker: str, raw_price: float) -> None:
        pos = self.positions.get(ticker)
        if pos is None:
            return

        exec_price = raw_price * (1 - self.slippage_rate)
        proceeds = pos.shares * exec_price
        commission = proceeds * self.commission_rate
        net = proceeds - commission

        pnl = net - pos.cost_basis
        pnl_pct = pnl / pos.cost_basis
        held = (date - pos.entry_date).days

        self.cash += net
        del self.positions[ticker]

        self.closed_trades.append(
            {
                "ticker": ticker,
                "entry_date": pos.entry_date,
                "exit_date": date,
                "entry_price": pos.entry_price,
                "exit_price": exec_price,
                "shares": pos.shares,
                "cost_basis": pos.cost_basis,
                "net_proceeds": net,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "holding_days": held,
            }
        )
        log.debug(
            "SELL %s  %d @ %.3f  PnL $%,.0f (%+.1f%%)  held %dd",
            ticker, pos.shares, exec_price, pnl, pnl_pct * 100, held,
        )

    # ------------------------------------------------------------------
    def mark_to_market(
        self, date, close_prices: Dict[str, float]
    ) -> float:
        """Total portfolio value at today's close."""
        pos_val = sum(
            close_prices.get(t, p.entry_price) * p.shares
            for t, p in self.positions.items()
        )
        return self.cash + pos_val

############################################################
"""
backtest/phase2/data_source.py
Date-aware data source wrapper for backtesting.

Loads from a single parquet file per market (data/{market}_cash.parquet),
filters to a given ticker universe, and presents a sliding window up to
the current backtest date.

Expected parquet schema (long format):
    date | ticker | open | high | low | close | volume

The 'date' column should be parseable as datetime.  Column names are
normalised to lowercase on load.  If the parquet uses 'symbol' instead
of 'ticker', that works too.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

log = logging.getLogger(__name__)


class BacktestDataSource:

    def __init__(
        self,
        ticker_data: Dict[str, pd.DataFrame],
        benchmark_data: Optional[pd.DataFrame] = None,
        lookback_bars: int = 300,
    ):
        self.ticker_data = ticker_data
        self.benchmark_data = benchmark_data
        self.lookback_bars = lookback_bars
        self._cutoff: Optional[pd.Timestamp] = None

    # ==================================================================
    #  Factory — build from parquet + universe list
    # ==================================================================
    @classmethod
    def from_parquet(
        cls,
        parquet_path: str | Path,
        tickers: List[str],
        benchmark_ticker: Optional[str] = None,
        lookback_bars: int = 300,
    ) -> "BacktestDataSource":
        """
        Load ``data/{market}_cash.parquet`` and split into per-ticker
        DataFrames keyed by ticker with a DatetimeIndex.

        Args:
            parquet_path:     Path to the parquet file.
            tickers:          Universe tickers to keep.
            benchmark_ticker: e.g. "2800.HK".  Looked up in the same
                              parquet; ignored if not found.
            lookback_bars:    Max bars per fetch() call.
        """
        path = Path(parquet_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        raw = pd.read_parquet(path)
        raw.columns = raw.columns.str.strip().str.lower()

        # ── normalise ticker column ──────────────────────────────
        ticker_col = _find_column(raw, ("ticker", "symbol", "code", "stock"))
        if ticker_col is None:
            raise KeyError(
                f"Cannot find a ticker/symbol column in {path}.  "
                f"Columns present: {list(raw.columns)}"
            )

        # ── normalise date column / index ────────────────────────
        date_col = _find_column(raw, ("date", "datetime", "timestamp", "time"))
        if date_col is not None:
            raw[date_col] = pd.to_datetime(raw[date_col])
        elif isinstance(raw.index, pd.DatetimeIndex):
            raw = raw.reset_index()
            date_col = raw.columns[0]  # the old index name
        else:
            raise KeyError(
                f"Cannot find a date column in {path}.  "
                f"Columns present: {list(raw.columns)}"
            )

        # ── normalise OHLCV column names ─────────────────────────
        rename_map = {}
        for target, candidates in [
            ("open",   ("open", "o")),
            ("high",   ("high", "h")),
            ("low",    ("low", "l")),
            ("close",  ("close", "c", "adj close", "adj_close", "adjclose")),
            ("volume", ("volume", "vol", "v")),
        ]:
            found = _find_column(raw, candidates)
            if found and found != target:
                rename_map[found] = target
        if rename_map:
            raw = raw.rename(columns=rename_map)

        needed = {"open", "high", "low", "close", "volume"}
        missing = needed - set(raw.columns)
        if missing:
            raise KeyError(f"Missing OHLCV columns after rename: {missing}")

        # ── filter to universe tickers ───────────────────────────
        all_tickers_in_file = set(raw[ticker_col].unique())
        want = set(tickers)
        if benchmark_ticker:
            want.add(benchmark_ticker)

        found_tickers = want & all_tickers_in_file
        not_found = want - all_tickers_in_file
        if not_found:
            log.warning(
                "%d tickers not in parquet and will be skipped: %s",
                len(not_found),
                sorted(not_found)[:20],
            )

        raw = raw[raw[ticker_col].isin(found_tickers)].copy()
        raw = raw.sort_values([ticker_col, date_col])

        # ── split into {ticker: DataFrame} ───────────────────────
        ticker_data: Dict[str, pd.DataFrame] = {}
        benchmark_data: Optional[pd.DataFrame] = None

        for tkr, group in raw.groupby(ticker_col):
            df = (
                group.set_index(date_col)[["open", "high", "low", "close", "volume"]]
                .sort_index()
                .copy()
            )
            df.index.name = "date"
            # drop exact duplicate indices if any
            df = df[~df.index.duplicated(keep="last")]
            ticker_data[tkr] = df

            if tkr == benchmark_ticker:
                benchmark_data = df.copy()

        log.info(
            "Loaded %d tickers from %s  (date range %s → %s)",
            len(ticker_data),
            path.name,
            raw[date_col].min().strftime("%Y-%m-%d"),
            raw[date_col].max().strftime("%Y-%m-%d"),
        )

        return cls(
            ticker_data=ticker_data,
            benchmark_data=benchmark_data,
            lookback_bars=lookback_bars,
        )

    # ==================================================================
    #  Cutoff management
    # ==================================================================
    def set_cutoff(self, dt) -> None:
        self._cutoff = pd.Timestamp(dt)

    # ==================================================================
    #  Data access
    # ==================================================================
    def fetch(self, ticker: str) -> pd.DataFrame:
        if ticker not in self.ticker_data:
            return pd.DataFrame()
        df = self.ticker_data[ticker]
        if self._cutoff is not None:
            df = df.loc[df.index <= self._cutoff]
        if len(df) > self.lookback_bars:
            df = df.iloc[-self.lookback_bars:]
        return df.copy()

    def fetch_benchmark(self) -> pd.DataFrame:
        if self.benchmark_data is None:
            return pd.DataFrame()
        df = self.benchmark_data
        if self._cutoff is not None:
            df = df.loc[df.index <= self._cutoff]
        if len(df) > self.lookback_bars:
            df = df.iloc[-self.lookback_bars:]
        return df.copy()

    def get_tickers(self) -> List[str]:
        return list(self.ticker_data.keys())

    def get_trading_days(self, start: str, end: str) -> List[pd.Timestamp]:
        all_dates: set = set()
        for df in self.ticker_data.values():
            all_dates.update(df.index.tolist())
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        return sorted(d for d in all_dates if s <= d <= e)

    def get_date_range(self) -> tuple:
        """Min and max dates across all loaded tickers."""
        lo, hi = pd.Timestamp.max, pd.Timestamp.min
        for df in self.ticker_data.values():
            if not df.empty:
                lo = min(lo, df.index.min())
                hi = max(hi, df.index.max())
        return lo, hi


# ------------------------------------------------------------------
#  helpers
# ------------------------------------------------------------------

def _find_column(df: pd.DataFrame, candidates: tuple) -> Optional[str]:
    """Return the first column name from *candidates* present in df."""
    cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in cols_lower:
            return cols_lower[c]
    return None

####################
"""backtest/phase2/diagnostics.py

Drop-in signal diagnostics. Wire into your engine's day loop
and signal generator to capture *why* BUY/SELL/HOLD decisions
are made.

Usage:
    from backtest.phase2.diagnostics import SignalDiagnostics
    diag = SignalDiagnostics("Loose", enabled=True)
    # ... wire calls at each decision point (see integration notes below)
    diag.print_summary()
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ── reason tags (use these as constants so summaries group cleanly) ─────
R_SCORE_BELOW_EXIT    = "score<exit"
R_SCORE_ABOVE_ENTRY   = "score>=entry"
R_RANK_BELOW_FLOOR    = "rank<exit_floor"
R_RANK_ABOVE_MIN      = "rank>=min_rank"
R_RANK_BELOW_MIN      = "rank<min_rank"
R_RS_BLOCKED          = "rs_regime_blocked"
R_RS_FAIL_PENALTY     = "rs_fail_penalty_applied"
R_SECTOR_BLOCKED      = "sector_regime_blocked"
R_BREADTH_HARD_BLOCK  = "breadth_hard_block"
R_VOL_HARD_BLOCK      = "vol_hard_block"
R_COOLDOWN            = "cooldown_active"
R_MAX_POSITIONS       = "max_positions_reached"
R_CONTINUATION        = "continuation_pass"
R_CONTINUATION_FAIL   = "continuation_fail"
R_PULLBACK            = "pullback_pass"
R_PULLBACK_FAIL       = "pullback_fail"
R_CHAOTIC_EXIT_BUMP   = "chaotic_exit_bump"
R_NOT_HELD            = "not_held"
R_HELD_OK             = "held_score_ok"


class SignalDiagnostics:
    """Captures per-day, per-ticker decision data and emits summaries."""

    def __init__(self, name: str = "", enabled: bool = True,
                 verbose_top_n: int = 5, verbose_sells: int = 3):
        self.name = name
        self.enabled = enabled
        self.verbose_top_n = verbose_top_n
        self.verbose_sells = verbose_sells

        # accumulation across entire backtest
        self.daily_records: list[dict] = []
        self._rec: dict | None = None

    # ── per-day lifecycle ───────────────────────────────────────────────

    def begin_day(
        self,
        date,
        vol_regime: str,
        breadth_regime: str,
        base_entry: float,
        base_exit: float,
        regime_entry_adj: float,
        breadth_entry_adj: float,
        adjusted_entry: float,
        adjusted_exit: float,
        size_multiplier: float = 1.0,
        n_held: int = 0,
        max_positions: int = 0,
    ):
        """Call once at the start of each trading day, after computing
        regime adjustments but before iterating over tickers."""
        if not self.enabled:
            return
        self._rec = {
            "date": date,
            "vol_regime": vol_regime,
            "breadth_regime": breadth_regime,
            "base_entry": base_entry,
            "base_exit": base_exit,
            "regime_entry_adj": regime_entry_adj,
            "breadth_entry_adj": breadth_entry_adj,
            "adjusted_entry": adjusted_entry,
            "adjusted_exit": adjusted_exit,
            "size_multiplier": size_multiplier,
            "n_held": n_held,
            "max_positions": max_positions,
            # populated by log_score / log_decision
            "composites": {},          # ticker -> float
            "sub_scores": {},          # ticker -> dict
            "ranks": {},               # ticker -> float
            "rs_regimes": {},          # ticker -> str
            "sector_regimes": {},      # ticker -> str
            "decisions": {},           # ticker -> (action, [reasons])
            "sell_triggers": defaultdict(list),
        }

    def log_score(
        self,
        ticker: str,
        composite: float,
        rank_pct: float | None = None,
        sub_scores: dict | None = None,
        rs_regime: str | None = None,
        sector_regime: str | None = None,
    ):
        """Call for every ticker after the scoring pipeline runs."""
        if not self.enabled or self._rec is None:
            return
        self._rec["composites"][ticker] = composite
        if rank_pct is not None:
            self._rec["ranks"][ticker] = rank_pct
        if sub_scores:
            self._rec["sub_scores"][ticker] = sub_scores
        if rs_regime:
            self._rec["rs_regimes"][ticker] = rs_regime
        if sector_regime:
            self._rec["sector_regimes"][ticker] = sector_regime

    def log_decision(self, ticker: str, action: str, reasons: list[str]):
        """Call when a final BUY / SELL / HOLD / BLOCKED decision is made.

        action: one of 'BUY', 'SELL', 'HOLD', 'BLOCKED'
        reasons: list of R_* tags (or free-form strings)
        """
        if not self.enabled or self._rec is None:
            return
        self._rec["decisions"][ticker] = (action, reasons)
        if action == "SELL":
            for r in reasons:
                self._rec["sell_triggers"][r].append(ticker)

    def end_day(self):
        """Call at end of day. Logs summary lines and archives the record."""
        if not self.enabled or self._rec is None:
            return
        rec = self._rec
        scores = np.array(list(rec["composites"].values())) if rec["composites"] else np.array([])
        ranks = np.array(list(rec["ranks"].values())) if rec["ranks"] else np.array([])

        # ── 1. score distribution vs thresholds ────────────────────────
        if len(scores) > 0:
            p = np.percentile(scores, [5, 10, 25, 50, 75, 90, 95])
            above_entry = int(np.sum(scores >= rec["adjusted_entry"]))
            in_band = int(np.sum(
                (scores >= rec["adjusted_exit"]) & (scores < rec["adjusted_entry"])
            ))
            below_exit = int(np.sum(scores < rec["adjusted_exit"]))

            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SCORES  "
                f"n={len(scores)}  "
                f"p5={p[0]:.3f} p10={p[1]:.3f} p25={p[2]:.3f} "
                f"p50={p[3]:.3f} p75={p[4]:.3f} p90={p[5]:.3f} p95={p[6]:.3f}  "
                f"above_entry={above_entry}  in_band={in_band}  below_exit={below_exit}"
            )

        # ── 2. threshold computation trace ─────────────────────────────
        logger.info(
            f"[{self.name}] {rec['date']}  DIAG-THRESH  "
            f"vol={rec['vol_regime']}  breadth={rec['breadth_regime']}  "
            f"base_entry={rec['base_entry']:.3f}  "
            f"+regime_adj={rec['regime_entry_adj']:+.3f}  "
            f"+breadth_adj={rec['breadth_entry_adj']:+.3f}  "
            f"= adjusted_entry={rec['adjusted_entry']:.3f}  "
            f"adjusted_exit={rec['adjusted_exit']:.3f}  "
            f"size_mult={rec['size_multiplier']:.2f}  "
            f"held={rec['n_held']}/{rec['max_positions']}"
        )

        # ── 3. rank distribution (if populated) ────────────────────────
        if len(ranks) > 0:
            rp = np.percentile(ranks, [10, 50, 90])
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"n={len(ranks)}  p10={rp[0]:.3f}  p50={rp[1]:.3f}  p90={rp[2]:.3f}"
            )
        else:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"** NO RANK DATA — rank keys may not be wired **"
            )

        # ── 4. top N scores with full breakdown ────────────────────────
        if rec["composites"]:
            top = sorted(rec["composites"].items(), key=lambda x: x[1], reverse=True)
            for ticker, comp in top[: self.verbose_top_n]:
                sub = rec["sub_scores"].get(ticker, {})
                rank = rec["ranks"].get(ticker)
                rs = rec["rs_regimes"].get(ticker, "?")
                sec = rec["sector_regimes"].get(ticker, "?")
                dec_action, dec_reasons = rec["decisions"].get(ticker, ("?", []))

                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                rank_str = f"rank={rank:.3f}" if rank is not None else "rank=N/A"

                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-TOP     "
                    f"{ticker:20s}  comp={comp:.4f}  {rank_str}  "
                    f"rs={rs}  sec={sec}  {sub_str}  "
                    f"→ {dec_action}  {dec_reasons}"
                )

        # ── 5. sell trigger breakdown ──────────────────────────────────
        if rec["sell_triggers"]:
            counts = {k: len(v) for k, v in rec["sell_triggers"].items()}
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SELLS   {counts}"
            )
            # show a few examples of score-below-exit sells
            score_sells = rec["sell_triggers"].get(R_SCORE_BELOW_EXIT, [])
            for ticker in score_sells[: self.verbose_sells]:
                comp = rec["composites"].get(ticker, 0)
                sub = rec["sub_scores"].get(ticker, {})
                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-SELL-EX "
                    f"{ticker:20s}  comp={comp:.4f}  {sub_str}"
                )

        # ── 6. blocked buys (score above entry but blocked by filter) ──
        blocked = [
            (t, d) for t, d in rec["decisions"].items() if d[0] == "BLOCKED"
        ]
        if blocked:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                f"{len(blocked)} tickers passed score threshold but were blocked"
            )
            for ticker, (_, reasons) in blocked[: self.verbose_top_n]:
                comp = rec["composites"].get(ticker, 0)
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                    f"{ticker:20s}  comp={comp:.4f}  {reasons}"
                )

        self.daily_records.append(rec)
        self._rec = None

    # ── end-of-backtest summary ─────────────────────────────────────────

    def print_summary(self):
        """Call once after the backtest loop finishes."""
        if not self.daily_records:
            logger.info(f"[{self.name}] DIAG-SUMMARY  no data recorded")
            return

        n_days = len(self.daily_records)
        all_scores = []
        all_entries = []
        all_exits = []
        action_counts = defaultdict(int)
        trigger_counts = defaultdict(int)
        regime_day_counts = defaultdict(int)
        breadth_day_counts = defaultdict(int)
        days_with_rank = 0

        for rec in self.daily_records:
            all_scores.extend(rec["composites"].values())
            all_entries.append(rec["adjusted_entry"])
            all_exits.append(rec["adjusted_exit"])
            regime_day_counts[rec["vol_regime"]] += 1
            breadth_day_counts[rec["breadth_regime"]] += 1
            if rec["ranks"]:
                days_with_rank += 1
            for _, (action, reasons) in rec["decisions"].items():
                action_counts[action] += 1
                if action == "SELL":
                    for r in reasons:
                        trigger_counts[r] += 1

        scores_arr = np.array(all_scores) if all_scores else np.array([0])
        entries_arr = np.array(all_entries)
        exits_arr = np.array(all_exits)

        logger.info(f"[{self.name}] {'=' * 60}")
        logger.info(f"[{self.name}] DIAGNOSTIC SUMMARY  ({n_days} trading days)")
        logger.info(f"[{self.name}] {'=' * 60}")

        logger.info(
            f"[{self.name}]   Score distribution (all ticker-days):  "
            f"mean={scores_arr.mean():.4f}  std={scores_arr.std():.4f}  "
            f"min={scores_arr.min():.4f}  max={scores_arr.max():.4f}"
        )
        p = np.percentile(scores_arr, [5, 25, 50, 75, 90, 95, 99])
        logger.info(
            f"[{self.name}]   Score percentiles:  "
            f"p5={p[0]:.4f}  p25={p[1]:.4f}  p50={p[2]:.4f}  "
            f"p75={p[3]:.4f}  p90={p[4]:.4f}  p95={p[5]:.4f}  p99={p[6]:.4f}"
        )

        pct_above_entry = 100.0 * np.mean(scores_arr >= entries_arr.mean())
        pct_below_exit = 100.0 * np.mean(scores_arr < exits_arr.mean())
        logger.info(
            f"[{self.name}]   Avg entry threshold: {entries_arr.mean():.4f}  "
            f"Avg exit threshold: {exits_arr.mean():.4f}"
        )
        logger.info(
            f"[{self.name}]   %% ticker-days above avg entry: {pct_above_entry:.1f}%%  "
            f"below avg exit: {pct_below_exit:.1f}%%"
        )

        logger.info(f"[{self.name}]   Vol regime days:     {dict(regime_day_counts)}")
        logger.info(f"[{self.name}]   Breadth regime days:  {dict(breadth_day_counts)}")
        logger.info(f"[{self.name}]   Days with rank data:  {days_with_rank}/{n_days}")

        logger.info(f"[{self.name}]   Decision totals:      {dict(action_counts)}")
        if trigger_counts:
            logger.info(f"[{self.name}]   Sell trigger totals:  {dict(trigger_counts)}")

        # gap analysis: how far is p90 score from entry threshold?
        daily_gaps = []
        for rec in self.daily_records:
            s = list(rec["composites"].values())
            if s:
                daily_gaps.append(np.percentile(s, 90) - rec["adjusted_entry"])
        if daily_gaps:
            gaps = np.array(daily_gaps)
            logger.info(
                f"[{self.name}]   Daily (p90_score - entry_thresh):  "
                f"mean={gaps.mean():+.4f}  min={gaps.min():+.4f}  max={gaps.max():+.4f}"
            )
            if gaps.mean() < 0:
                logger.warning(
                    f"[{self.name}]   ⚠ p90 score is BELOW entry threshold on average. "
                    f"Scoring pipeline may be systematically too low, or thresholds too high."
                )

        logger.info(f"[{self.name}] {'=' * 60}")

##########

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

###############
"""
backtest/phase2/tracker.py
Virtual portfolio tracker.

Manages cash, positions, trade execution (with slippage + commission),
and records every completed round-trip for analysis.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
import logging

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float          # total cost including commission


# ------------------------------------------------------------------
class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,   # 10 bps per trade
        slippage_rate: float = 0.0010,     # 10 bps per trade
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

    # ------------------------------------------------------------------
    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
    ) -> None:
        """
        Execute one day's actions.  Sells run first (free up cash),
        then buys fill up to max_positions.
        """
        # ── sells first ──────────────────────────────────────────
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker in prices:
                    self._sell(date, ticker, prices[ticker])

        # ── then buys ────────────────────────────────────────────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]
        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            self._buy(date, ticker, prices[ticker])

    # ------------------------------------------------------------------
    
    def _buy(self, date, ticker: str, raw_price: float) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)

        target_value = min(
            self.initial_capital / self.max_positions,
            self.cash * 0.95,
        )
        if target_value < 1_000:
            return

        shares = int(target_value / exec_price)
        if shares <= 0:
            return

        cost = shares * exec_price
        commission = cost * self.commission_rate
        total = cost + commission
        if total > self.cash:
            return

        self.cash -= total
        self.positions[ticker] = Position(
            ticker=ticker,
            entry_date=date,
            entry_price=exec_price,
            shares=shares,
            cost_basis=total,
        )
        log.info(
            f"  ▲ BUY  {ticker:<10s}  {shares} shares @ {exec_price:.2f}"
            f"  cost ${total:,.0f}"
        )

    # ------------------------------------------------------------------
    def _sell(self, date, ticker: str, raw_price: float) -> None:
        pos = self.positions.get(ticker)
        if pos is None:
            return

        exec_price = raw_price * (1 - self.slippage_rate)
        proceeds = pos.shares * exec_price
        commission = proceeds * self.commission_rate
        net = proceeds - commission

        pnl = net - pos.cost_basis
        pnl_pct = pnl / pos.cost_basis
        held = (date - pos.entry_date).days

        self.cash += net
        del self.positions[ticker]

        self.closed_trades.append(
            {
                "ticker": ticker,
                "entry_date": pos.entry_date,
                "exit_date": date,
                "entry_price": pos.entry_price,
                "exit_price": exec_price,
                "shares": pos.shares,
                "cost_basis": pos.cost_basis,
                "net_proceeds": net,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "holding_days": held,
            }
        )
        log.info(
            f"  ▼ SELL {ticker:<10s}  {pos.shares} shares @ {exec_price:.2f}"
            f"  PnL ${pnl:,.0f} ({pnl_pct:+.1%})  held {held}d"
        )

    # ------------------------------------------------------------------
    def mark_to_market(
        self, date, close_prices: Dict[str, float]
    ) -> float:
        """Total portfolio value at today's close."""
        pos_val = sum(
            close_prices.get(t, p.entry_price) * p.shares
            for t, p in self.positions.items()
        )
        return self.cash + pos_val

#########################
