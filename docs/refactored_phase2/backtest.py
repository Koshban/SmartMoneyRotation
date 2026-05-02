# backtest/phase2/breadth_regime.py

import pandas as pd
import numpy as np


def compute_breadth_regime(
    prices: pd.DataFrame,
    params: dict,
) -> pd.DataFrame:
    """
    Compute daily breadth regime for the full universe.
    
    Returns DataFrame with columns: [date, pct_above_sma50, regime]
    Regime ∈ {'strong', 'neutral', 'weak'}
    """
    sma_period = params.get("ma_short", 50)
    strong_thr = params.get("strong_threshold", 0.65)
    weak_thr = params.get("weak_threshold", 0.35)
    smooth = params.get("smoothing_window", 5)
    
    # For each ticker, is price above its SMA50?
    sma50 = prices.rolling(sma_period).mean()
    above_sma = (prices > sma50).astype(float)
    
    # Percent of universe above SMA50
    pct_above = above_sma.mean(axis=1)
    
    # Smooth to avoid whipsaws
    pct_smooth = pct_above.rolling(smooth, min_periods=1).mean()
    
    # Classify regime
    regime = pd.Series("neutral", index=pct_smooth.index)
    regime[pct_smooth >= strong_thr] = "strong"
    regime[pct_smooth <= weak_thr] = "weak"
    
    result = pd.DataFrame({
        "pct_above_sma50": pct_smooth,
        "regime": regime,
    })
    
    return result


def get_exposure_multiplier(regime: str, params: dict) -> float:
    """Map breadth regime to exposure multiplier."""
    mapping = {
        "strong": params.get("strong_exposure", 1.0),
        "neutral": params.get("neutral_exposure", 0.75),
        "weak": params.get("weak_exposure", 0.40),
    }
    return mapping.get(regime, 0.75)
    
#############################################
"""
backtest/phase2/compare.py
Run two configs side-by-side and print a comparison table,
including benchmark performance.
"""
from __future__ import annotations

import pandas as pd
from typing import Any, Dict, Tuple

from rich.console import Console
from rich.table import Table

from cash.backtest.phase2.engine import BacktestEngine
from cash.backtest.phase2.data_source import BacktestDataSource
from cash.backtest.phase2.metrics import compute_metrics


# ------------------------------------------------------------------
def build_config_dict(
    vol_regime_params: dict,
    scoring_weights: dict,
    scoring_params: dict,
    signal_params: dict,
    convergence_params: dict,
    action_params: dict | None = None,
    breadth_params: dict | None = None,
    rotation_params: dict | None = None,
) -> Dict[str, Any]:
    """Bundle config blocks into one dict for the engine."""
    cfg = {
        "vol_regime_params": vol_regime_params,
        "scoring_weights": scoring_weights,
        "scoring_params": scoring_params,
        "signal_params": signal_params,
        "convergence_params": convergence_params,
    }
    if action_params is not None:
        cfg["action_params"] = action_params
    if breadth_params is not None:
        cfg["breadth_params"] = breadth_params
    if rotation_params is not None:
        cfg["rotation_params"] = rotation_params
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
    ("SEP", None, None, None),
    # ── Benchmark & relative metrics ──────────────────────────────
    ("Benchmark Return",       "benchmark_total_return", ".1%", None),
    ("Benchmark Ann. Return",  "benchmark_ann_return", ".1%",  None),
    ("Benchmark Sharpe",       "benchmark_sharpe",     ".2f",  None),
    ("Benchmark Max DD",       "benchmark_max_dd",     ".1%",  None),
    ("SEP", None, None, None),
    ("Alpha (Jensen)",         "alpha",                ".2%",  False),
    ("Beta",                   "beta",                 ".2f",  None),
    ("Tracking Error",         "tracking_error",       ".1%",  True),
    ("Information Ratio",      "information_ratio",    ".2f",  False),
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
            better = name_a if va < vb else name_b if vb < va else "Tie"
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
    
#####################################
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
    
#####################################
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
        
######################
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

from cash.backtest.phase2.data_source import BacktestDataSource
from cash.backtest.phase2.tracker import PortfolioTracker

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
        from cash.phase2.pipeline_v2 import (
            _canonicalize_indicator_columns,
            _fill_missing_indicators,
            annotate_scoreability,
        )
        from cash.phase2.strategy.adapters_v2 import ensure_columns
        from cash.phase2.strategy.regime_v2 import classify_volatility_regime
        from cash.phase2.strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
        from cash.compute.indicators import compute_all_indicators

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
        from cash.phase2.pipeline_v2 import run_pipeline_v2

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
        
################################
"""
backtest/phase2/metrics.py
Performance metrics from backtest results, including benchmark comparison.

FIX (2026-04-28): annualization now uses calendar dates, not trading-day
count.  The old formula ``252 / n_days`` broke when data had gaps —
e.g. 310 trading days over a 28-month span was treated as 1.23 years
instead of 2.32 years, inflating CAGR from ~11% to ~21%.
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

    # ── calendar-based annualization (robust to data gaps) ────────
    actual_start = actual_end = None
    if "date" in equity.columns and n_days >= 2:
        actual_start = pd.Timestamp(equity["date"].iloc[0])
        actual_end = pd.Timestamp(equity["date"].iloc[-1])
        calendar_days = (actual_end - actual_start).days
        years = calendar_days / 365.25
    else:
        years = n_days / 252.0

    years = max(years, 1.0 / 365.25)       # floor at 1 calendar day
    ann_factor = 1.0 / years                # exponent for CAGR

    ann_ret = (1 + total_ret) ** ann_factor - 1

    # expected trading days (for data-coverage check)
    expected_trading_days = int(round(years * 252))

    # ── volatility ────────────────────────────────────────────────
    ann_vol = daily.std() * np.sqrt(252) if len(daily) > 1 else 0.0
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

    down = daily[daily < 0]
    down_vol = down.std() * np.sqrt(252) if len(down) > 1 else 0.001
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
    avg_pos = dl["n_positions"].mean() if ("n_positions" in dl.columns and not dl.empty) else 0
    max_pos = int(dl["n_positions"].max()) if ("n_positions" in dl.columns and not dl.empty) else 0
    total_buys = int(dl["n_buys"].sum()) if ("n_buys" in dl.columns and not dl.empty) else 0
    total_sells = int(dl["n_sells"].sum()) if ("n_sells" in dl.columns and not dl.empty) else 0

    # ── trade stats ───────────────────────────────────────────────
    tm = _trade_metrics(trades)

    # ── benchmark comparison (uses same ann_factor) ───────────────
    bm = _benchmark_metrics(equity, ann_ret, ann_factor, years)

    return {
        # ── core ──
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "final_value": final,
        "initial_capital": initial,
        # ── period ──
        "years": years,
        "actual_start": str(actual_start.date()) if actual_start is not None else "",
        "actual_end": str(actual_end.date()) if actual_end is not None else "",
        "trading_days": n_days,
        "expected_trading_days": expected_trading_days,
        # ── risk ──
        "annualized_vol": ann_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
        "max_dd_duration_days": max_dd_dur,
        "calmar_ratio": calmar,
        # ── utilisation ──
        "avg_positions": avg_pos,
        "max_positions_held": max_pos,
        "total_buy_signals": total_buys,
        "total_sell_signals": total_sells,
        # ── trades + benchmark (spread in) ──
        **tm,
        **bm,
    }


# ------------------------------------------------------------------
def _benchmark_metrics(
    equity: pd.DataFrame,
    strategy_ann_ret: float,
    ann_factor: float,
    years: float,
) -> Dict[str, Any]:
    """
    Benchmark return, risk, and relative metrics.

    Uses the same *ann_factor* (1 / calendar_years) as the portfolio
    so that CAGR values are directly comparable.
    """
    defaults = {
        "benchmark_total_return": 0.0,
        "benchmark_ann_return": 0.0,
        "benchmark_ann_vol": 0.0,
        "benchmark_sharpe": 0.0,
        "benchmark_sortino": 0.0,
        "benchmark_max_dd": 0.0,
        "benchmark_calmar": 0.0,
        "alpha": 0.0,
        "beta": 0.0,
        "tracking_error": 0.0,
        "information_ratio": 0.0,
    }

    if "benchmark" not in equity.columns:
        return defaults

    bench = equity["benchmark"]
    if bench.isna().all():
        return defaults

    # Forward-fill any gaps, then drop remaining NaNs
    bench = bench.ffill()
    valid = equity[["value", "benchmark"]].dropna()
    if len(valid) < 10:
        return defaults

    bench_initial = valid["benchmark"].iloc[0]
    bench_final = valid["benchmark"].iloc[-1]
    bench_total_ret = (
        (bench_final - bench_initial) / bench_initial
        if bench_initial > 0
        else 0.0
    )
    bench_ann_ret = (1 + bench_total_ret) ** ann_factor - 1

    # Daily returns for both
    combined = pd.DataFrame({
        "port_ret": valid["value"].pct_change(),
        "bench_ret": valid["benchmark"].pct_change(),
    }).dropna()

    if len(combined) < 5:
        return {
            **defaults,
            "benchmark_total_return": bench_total_ret,
            "benchmark_ann_return": bench_ann_ret,
        }

    # ── benchmark vol ─────────────────────────────────────────────
    bench_ann_vol = combined["bench_ret"].std() * np.sqrt(252)
    bench_sharpe = bench_ann_ret / bench_ann_vol if bench_ann_vol > 0 else 0.0

    # ── benchmark sortino ─────────────────────────────────────────
    bench_down = combined["bench_ret"][combined["bench_ret"] < 0]
    bench_down_vol = (
        bench_down.std() * np.sqrt(252) if len(bench_down) > 1 else 0.001
    )
    bench_sortino = bench_ann_ret / bench_down_vol

    # ── benchmark drawdown ────────────────────────────────────────
    bench_peak = valid["benchmark"].cummax()
    bench_dd = (valid["benchmark"] - bench_peak) / bench_peak
    bench_max_dd = bench_dd.min()

    # ── benchmark calmar ──────────────────────────────────────────
    bench_calmar = (
        bench_ann_ret / abs(bench_max_dd) if bench_max_dd != 0 else 0.0
    )

    # ── excess returns / tracking error ───────────────────────────
    excess = combined["port_ret"] - combined["bench_ret"]
    tracking_error = (
        excess.std() * np.sqrt(252) if len(excess) > 1 else 0.0
    )

    # ── beta ──────────────────────────────────────────────────────
    bench_var = combined["bench_ret"].var()
    if bench_var > 0:
        beta = (
            combined[["port_ret", "bench_ret"]].cov().iloc[0, 1] / bench_var
        )
    else:
        beta = 0.0

    # ── Jensen's alpha ────────────────────────────────────────────
    alpha = strategy_ann_ret - beta * bench_ann_ret

    # ── information ratio ─────────────────────────────────────────
    info_ratio = (
        (strategy_ann_ret - bench_ann_ret) / tracking_error
        if tracking_error > 0
        else 0.0
    )

    return {
        "benchmark_total_return": bench_total_ret,
        "benchmark_ann_return": bench_ann_ret,
        "benchmark_ann_vol": bench_ann_vol,
        "benchmark_sharpe": bench_sharpe,
        "benchmark_sortino": bench_sortino,
        "benchmark_max_dd": bench_max_dd,
        "benchmark_calmar": bench_calmar,
        "alpha": alpha,
        "beta": beta,
        "tracking_error": tracking_error,
        "information_ratio": info_ratio,
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
    
    
##############################
"""
backtest/phase2/run_backtest.py
CLI entry point — single-config backtest.

    python -m backtest.phase2.run_backtest --market US --start 2022-01-01 --end 2026-04-20
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from cash.backtest.phase2.data_source import BacktestDataSource
from cash.backtest.phase2.engine import BacktestEngine
from cash.backtest.phase2.metrics import compute_metrics
from common.universe import get_universe_for_market
from common.config_refactor import (
    VOLREGIMEPARAMS,
    SCORINGWEIGHTS_V2,
    SCORINGPARAMS_V2,
    SIGNALPARAMS_V2,
    CONVERGENCEPARAMS_V2,
    ACTIONPARAMS_V2,
    BREADTHPARAMS,
    ROTATIONPARAMS,
)

console = Console()
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════════
LOGS_DIR = Path("logs") / "backtest"


def _setup_logging(market: str, level: int = logging.INFO) -> Path:
    """Configure dual logging (file + console)."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"backtest_{market}_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(ch)

    for noisy in (
        "refactor.strategy.rotation_v2",
        "refactor.pipeline_v2",
        "refactor.strategy",
        "refactor.scoring",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return log_file


def _log_and_print(msg: str, rich_msg: str | None = None, level: int = logging.INFO):
    log.log(level, msg)
    console.print(rich_msg if rich_msg is not None else msg)


# ══════════════════════════════════════════════════════════════════
#  UNIVERSE + BENCHMARK MAPPING
# ══════════════════════════════════════════════════════════════════

BENCHMARKS = {
    "HK": "2800.HK",
    "IN": "NIFTYBEES.NS",
    "US": "SPY",
}


def _get_tickers(market: str) -> list[str]:
    return get_universe_for_market(market)


# ══════════════════════════════════════════════════════════════════
#  PARQUET DATA LOADING
# ══════════════════════════════════════════════════════════════════

def load_data(
    market: str,
    data_dir: str = "data",
    lookback_bars: int = 300,
) -> BacktestDataSource:
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
#  SINGLE CONFIG
# ══════════════════════════════════════════════════════════════════

CONFIG = {
    "vol_regime_params":  VOLREGIMEPARAMS,
    "scoring_weights":    SCORINGWEIGHTS_V2,
    "scoring_params":     SCORINGPARAMS_V2,
    "signal_params":      SIGNALPARAMS_V2,
    "convergence_params": CONVERGENCEPARAMS_V2,
    "action_params":      ACTIONPARAMS_V2,
    "breadth_params":     BREADTHPARAMS,
    "rotation_params":    ROTATIONPARAMS,
    "buy_ranking_params": {
        # How much to tilt toward momentum/beta (0=pure composite, 1=pure momentum)
        "momentum_tilt": 0.50,
        # Within the momentum signal, how much weight to give vol (beta proxy)
        "vol_preference": 0.30,
        # FILTER: skip tickers with annualized vol below this
        "min_trailing_vol": 0.25,
        # FILTER: skip tickers with RS z-score below this
        "min_rszscore": -0.50,
        # Lookback window for trailing vol (trading days)
        "vol_window": 60,
        # Whether to compute/use realized vol
        "use_realized_vol": True,
    },
    "signal_cap_params": {
        # Top 15 names get STRONG_BUY
        "strong_buy_limit": 15,
        # Total buy signals per day (STRONG_BUY + BUY)
        "max_buy_signals": 25,
    },
}


# ══════════════════════════════════════════════════════════════════
#  EDGE-STYLING HELPER
# ══════════════════════════════════════════════════════════════════

def _edge(port_val: float, bench_val: float, higher_is_better: bool = True) -> str:
    diff = port_val - bench_val
    is_good = diff > 0 if higher_is_better else diff < 0
    colour = "green" if is_good else ("red" if (diff != 0) else "dim")
    return f"[{colour}]{diff:+.2%}[/{colour}]"


def _edge_ratio(port_val: float, bench_val: float) -> str:
    diff = port_val - bench_val
    colour = "green" if diff > 0 else ("red" if diff < 0 else "dim")
    return f"[{colour}]{diff:+.3f}[/{colour}]"


# ══════════════════════════════════════════════════════════════════
#  METRICS DISPLAY
# ══════════════════════════════════════════════════════════════════

def _print_metrics(
    metrics: dict,
    market: str = "",
    benchmark_name: str = "",
) -> None:
    """Pretty-print backtest metrics with side-by-side comparison."""
    m = metrics

    actual_start = m.get("actual_start", "?")
    actual_end = m.get("actual_end", "?")
    years = m.get("years", 0)
    n_days = m.get("trading_days", 0)
    expected = m.get("expected_trading_days", 0)
    initial = m.get("initial_capital", 0)
    final = m.get("final_value", 0)

    # ── Header Panel ──────────────────────────────────────────────
    header_lines = [
        f"[bold]Period :[/]  {actual_start}  →  {actual_end}   "
        f"([cyan]{n_days}[/] trading days  /  [cyan]{years:.2f}[/] years)",
        f"[bold]Capital:[/]  \({initial:,.0f}  →  [bold green]\){final:,.0f}[/]",
        f"[bold]Bench  :[/]  {benchmark_name or '—'}",
    ]
    console.print()
    console.print(Panel(
        "\n".join(header_lines),
        title=f"[bold cyan]Backtest Results — {market}[/]",
        border_style="cyan",
        padding=(1, 2),
    ))

    if expected > 0 and n_days < expected * 0.85:
        console.print(
            f"  [yellow]⚠  Data coverage:[/] {n_days} trading days found vs "
            f"~{expected} expected for {years:.1f} years.  "
            f"Check parquet data completeness.\n"
        )

    # ══════════════════════════════════════════════════════════════
    #  TABLE 1 — Performance Comparison
    # ══════════════════════════════════════════════════════════════
    t1 = Table(
        title="Performance Comparison",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=64,
    )
    t1.add_column("Metric", style="bold", min_width=20)
    t1.add_column("Portfolio", justify="right", min_width=12)
    t1.add_column(f"Benchmark ({benchmark_name})", justify="right", min_width=12)
    t1.add_column("Edge", justify="right", min_width=10)

    def _pct_row(label, port_key, bench_key, higher_is_better=True):
        pv = m.get(port_key, 0)
        bv = m.get(bench_key, 0)
        t1.add_row(
            label,
            f"{pv:+.2%}",
            f"{bv:+.2%}" if bv != 0 else "—",
            _edge(pv, bv, higher_is_better) if bv != 0 else "—",
        )

    def _ratio_row(label, port_key, bench_key):
        pv = m.get(port_key, 0)
        bv = m.get(bench_key, 0)
        t1.add_row(
            label,
            f"{pv:.3f}",
            f"{bv:.3f}" if bv != 0 else "—",
            _edge_ratio(pv, bv) if bv != 0 else "—",
        )

    _pct_row("Total Return",    "total_return",       "benchmark_total_return")
    _pct_row("CAGR",            "annualized_return",   "benchmark_ann_return")
    _pct_row("Volatility",      "annualized_vol",      "benchmark_ann_vol",      higher_is_better=False)
    _ratio_row("Sharpe Ratio",  "sharpe_ratio",        "benchmark_sharpe")
    _ratio_row("Sortino Ratio", "sortino_ratio",       "benchmark_sortino")
    _pct_row("Max Drawdown",    "max_drawdown",        "benchmark_max_dd",       higher_is_better=False)
    _ratio_row("Calmar Ratio",  "calmar_ratio",        "benchmark_calmar")

    dd_dur = m.get("max_dd_duration_days", 0)
    t1.add_row("Max DD Duration", f"{dd_dur:.0f} days", "—", "—")

    console.print(t1)
    console.print()

    # ══════════════════════════════════════════════════════════════
    #  TABLE 2 — Risk-Adjusted Alpha
    # ══════════════════════════════════════════════════════════════
    t2 = Table(
        title="Risk-Adjusted Alpha",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=40,
    )
    t2.add_column("Metric", style="bold", min_width=20)
    t2.add_column("Value", justify="right", min_width=12)

    alpha_val = m.get("alpha", 0)
    alpha_colour = "green" if alpha_val > 0 else "red"
    t2.add_row("Alpha (Jensen)", f"[{alpha_colour}]{alpha_val:+.2%}[/{alpha_colour}]")
    t2.add_row("Beta", f"{m.get('beta', 0):.3f}")
    t2.add_row("Tracking Error", f"{m.get('tracking_error', 0):.2%}")

    ir = m.get("information_ratio", 0)
    ir_colour = "green" if ir > 0 else "red"
    t2.add_row("Information Ratio", f"[{ir_colour}]{ir:.3f}[/{ir_colour}]")

    console.print(t2)
    console.print()

    # ══════════════════════════════════════════════════════════════
    #  TABLE 3 — Trade Statistics
    # ══════════════════════════════════════════════════════════════
    t3 = Table(
        title="Trade Statistics",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=40,
    )
    t3.add_column("Metric", style="bold", min_width=20)
    t3.add_column("Value", justify="right", min_width=14)

    total_t = m.get("total_trades", 0)
    win_t = m.get("winning_trades", 0)
    lose_t = m.get("losing_trades", 0)

    t3.add_row("Total Trades", f"{total_t:.0f}")
    t3.add_row("Win / Loss", f"[green]{win_t}[/] / [red]{lose_t}[/]")
    t3.add_row("Win Rate", f"{m.get('win_rate', 0):.1%}")
    t3.add_row("Avg Win", f"[green]{m.get('avg_win_pct', 0):+.2%}[/]")
    t3.add_row("Avg Loss", f"[red]{m.get('avg_loss_pct', 0):+.2%}[/]")
    t3.add_row("Profit Factor", f"{m.get('profit_factor', 0):.2f}")
    t3.add_row("Avg PnL", f"{m.get('avg_pnl_pct', 0):+.2%}")
    t3.add_row("Median PnL", f"{m.get('median_pnl_pct', 0):+.2%}")
    t3.add_row("Avg Holding Days", f"{m.get('avg_holding_days', 0):.1f}")
    t3.add_row(
        "Best / Worst Trade",
        f"[green]{m.get('best_trade_pct', 0):+.1%}[/]  /  "
        f"[red]{m.get('worst_trade_pct', 0):+.1%}[/]",
    )
    t3.add_row("Expectancy ($)", f"${m.get('expectancy_dollar', 0):,.0f}")

    console.print(t3)
    console.print()

    # ══════════════════════════════════════════════════════════════
    #  TABLE 4 — Portfolio Utilisation & Signal Quality
    # ══════════════════════════════════════════════════════════════
    t4 = Table(
        title="Portfolio Utilisation & Signal Quality",
        show_header=True,
        header_style="bold cyan",
        title_style="bold",
        min_width=40,
    )
    t4.add_column("Metric", style="bold", min_width=20)
    t4.add_column("Value", justify="right", min_width=12)

    t4.add_row("Avg Positions", f"{m.get('avg_positions', 0):.1f}")
    t4.add_row("Max Positions Held", f"{m.get('max_positions_held', 0):.0f}")
    t4.add_row("Total Buy Signals", f"{m.get('total_buy_signals', 0):,.0f}")
    t4.add_row("Total Sell Signals", f"{m.get('total_sell_signals', 0):,.0f}")
    t4.add_row("Avg Signals/Day (capped)", f"{m.get('avg_daily_signals', 0):.1f}")

    console.print(t4)
    console.print()


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Backtest strategy against a market universe"
    )
    parser.add_argument(
        "--market", required=True,
        help="Market code (e.g. HK, IN, US)",
    )
    parser.add_argument("--start", required=True, help="Start date  YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date    YYYY-MM-DD")
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--max-positions", type=int, default=25)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--lookback", type=int, default=300,
                        help="Lookback bars for indicator warmup")
    args = parser.parse_args()

    log_file = _setup_logging(args.market)

    log.info("=" * 70)
    log.info("Backtest started  market=%s  start=%s  end=%s", args.market, args.start, args.end)
    log.info("capital=%.0f  max_positions=%d  lookback=%d", args.capital, args.max_positions, args.lookback)
    log.info("data_dir=%s", args.data_dir)
    log.info("Log file: %s", log_file)
    log.info("=" * 70)

    # ── load data ─────────────────────────────────────────────
    console.rule(f"[bold cyan]Backtest: {args.market} market")
    log.info("--- Loading data for %s market ---", args.market)
    ds = load_data(args.market, args.data_dir, args.lookback)

    # ── run backtest ──────────────────────────────────────────
    console.rule("[bold cyan]Running backtest")
    log.info("--- Running backtest ---")

    engine = BacktestEngine(
        data_source=ds,
        market=args.market,
        config=CONFIG,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        max_positions=args.max_positions,
        config_name="Strategy",
    )
    results = engine.run()
    metrics = compute_metrics(results)

    # Add signal quality metric
    metrics["avg_daily_signals"] = results.get("avg_daily_signals", 0)

    results["metrics"] = metrics

    # ── display ───────────────────────────────────────────────
    benchmark_name = BENCHMARKS.get(args.market, "?")
    _print_metrics(metrics, market=args.market, benchmark_name=benchmark_name)

    # ── log metrics to file ───────────────────────────────────
    log.info("--- Metrics ---")
    for key, val in metrics.items():
        log.info("  %-30s  %s", key, val)

    # ── compact summary ───────────────────────────────────────
    bench_ret = metrics.get("benchmark_total_return", 0)
    ann_ret = metrics.get("annualized_return", 0)
    bench_cagr = metrics.get("benchmark_ann_return", 0)
    alpha = metrics.get("alpha", 0)
    years = metrics.get("years", 0)

    summary_lines = [
        f"Market          : {args.market}",
        f"Period          : {args.start} -> {args.end}  ({years:.2f} years)",
        f"Start capital   : ${args.capital:,.0f}",
        f"Benchmark       : {benchmark_name}  "
        f"(total {bench_ret:+.1%} / CAGR {bench_cagr:+.1%})",
        f"Strategy        : ${results['final_value']:,.0f}  "
        f"(total {metrics['total_return']:+.1%} / CAGR {ann_ret:+.1%} / alpha {alpha:+.1%})",
        f"Avg signals/day : {metrics.get('avg_daily_signals', 0):.1f} (cap=25)",
    ]
    console.print()
    for line in summary_lines:
        _log_and_print(f"  {line}", f"  {line}")

    # ── save ──────────────────────────────────────────────────
    out = Path("backtest_results") / args.market
    out.mkdir(parents=True, exist_ok=True)

    metrics_rows = [{"Metric": k, "Value": v} for k, v in metrics.items()]
    pd.DataFrame(metrics_rows).to_csv(out / "metrics.csv", index=False)
    results["equity_curve"].to_csv(out / "equity_curve.csv", index=False)
    if not results["trade_log"].empty:
        results["trade_log"].to_csv(out / "trades.csv", index=False)
    results["daily_log"].to_csv(out / "daily_log.csv", index=False)

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
    
    
################################
# backtest/phase2/stop_loss.py

import numpy as np
from dataclasses import dataclass, field


@dataclass
class PositionTracker:
    """Tracks stop levels for a single position."""
    ticker: str
    entry_price: float
    entry_date: str
    initial_stop: float          # ATR-based initial stop
    high_water_mark: float = 0.0
    trailing_active: bool = False
    current_stop: float = 0.0

    def __post_init__(self):
        self.high_water_mark = self.entry_price
        self.current_stop = self.initial_stop


def update_stops(
    position: PositionTracker,
    current_price: float,
    current_atr: float,
    params: dict,
) -> tuple[PositionTracker, bool]:
    """
    Update stop levels. Returns (updated_position, should_exit).
    """
    entry = position.entry_price
    max_loss_pct = params.get("max_loss_pct", 0.20)
    trail_activation = params.get("trail_activation_pct", 0.10)
    trail_atr_mult = params.get("trail_atr_multiplier", 2.5)
    trail_pct_fallback = params.get("trail_pct_fallback", 0.15)

    # ── Hard max loss check ──────────────────────────────────
    if params.get("max_loss_enabled", True):
        hard_stop = entry * (1.0 - max_loss_pct)
        if current_price <= hard_stop:
            return position, True

    # ── Update high-water mark ───────────────────────────────
    if current_price > position.high_water_mark:
        position.high_water_mark = current_price

    # ── Activate trailing if gain threshold reached ──────────
    gain_pct = (position.high_water_mark - entry) / entry
    if gain_pct >= trail_activation and params.get("trailing_enabled", True):
        position.trailing_active = True

    # ── Compute trailing stop level ──────────────────────────
    if position.trailing_active:
        if current_atr > 0:
            trail_stop = position.high_water_mark - (trail_atr_mult * current_atr)
        else:
            trail_stop = position.high_water_mark * (1.0 - trail_pct_fallback)

        # Trail only goes UP, never down
        position.current_stop = max(position.current_stop, trail_stop)

    # ── Ratchet profit lock-in ───────────────────────────────
    if params.get("ratchet_enabled", True):
        for threshold, lock_pct in params.get("ratchet_levels", []):
            if gain_pct >= threshold:
                ratchet_stop = entry * (1.0 + lock_pct)
                position.current_stop = max(position.current_stop, ratchet_stop)

    # ── Check if stopped out ─────────────────────────────────
    if current_price <= position.current_stop:
        return position, True

    return position, False
    
    
##############################
"""
backtest/phase2/tracker.py
Virtual portfolio tracker with minimum hold period,
trailing stop, max hold duration, and score-ranked buys.

Features:
  - MOMENTUM/BETA-TILTED BUY RANKING: among BUY candidates, ranks by a
    blended score that favors high-momentum, high-vol names.
  - VARIABLE POSITION SIZING: _buy() accepts per-ticker weight from pipeline.
  - Dynamic sizing uses current NAV (not frozen initial_capital).
  - force_exits() tags exit_type for attribution.
  - Trailing stop fires at open price (gap-down protection).
  - MIN BETA FILTER: optionally skips low-vol names entirely.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import logging
import numpy as np

log = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float
    peak_price: float = 0.0
    latest_score: float = 0.0
    current_price: float = 0.0
    target_weight: float = 0.0
    entry_momentum_rank: float = 0.0  # diagnostic: the blended rank at entry

    def __post_init__(self):
        if self.peak_price == 0.0:
            self.peak_price = self.entry_price
        if self.current_price == 0.0:
            self.current_price = self.entry_price

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price


class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        min_hold_days: int = 5,
        min_profit_early_exit_pct: float = 0.05,
        trailing_stop_pct: float = 0.18,
        max_hold_days: int = 120,
        upgrade_min_score_gap: float = 999,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.min_hold_days = min_hold_days
        self.min_profit_early_exit_pct = min_profit_early_exit_pct

        self.trailing_stop_pct = trailing_stop_pct
        self.max_hold_days = max_hold_days
        self.upgrade_min_score_gap = upgrade_min_score_gap

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

        # Diagnostic counters
        self._total_buys: int = 0
        self._total_sells: int = 0
        self._blocked_sells: int = 0
        self._filtered_low_vol: int = 0
        self._ranking_logged: bool = False

    # ──────────────────────────────────────────────────────────
    #  NAV
    # ──────────────────────────────────────────────────────────
    @property
    def nav(self) -> float:
        return self.cash + sum(
            pos.market_value for pos in self.positions.values()
        )

    # ──────────────────────────────────────────────────────────
    #  Min-hold check
    # ──────────────────────────────────────────────────────────
    def _can_sell(self, pos: Position, date, current_price: float) -> bool:
        held_days = (date - pos.entry_date).days
        if held_days >= self.min_hold_days:
            return True
        unrealised_pct = (current_price / pos.entry_price) - 1.0
        if unrealised_pct >= self.min_profit_early_exit_pct:
            log.debug(
                "  early exit allowed %s: +%.1f%% after %dd",
                pos.ticker, unrealised_pct * 100, held_days,
            )
            return True
        return False

    # ──────────────────────────────────────────────────────────
    #  Force exits: trailing stop + max hold
    # ──────────────────────────────────────────────────────────
    def force_exits(self, date, prices, **kw) -> List[str]:
        force_sold = []
        for ticker in list(self.positions):
            pos = self.positions[ticker]
            price = prices.get(ticker)

            if price is None:
                held = (date - pos.entry_date).days
                if held > 5:
                    log.warning(
                        "  ⚠ NO PRICE for %-10s  held %dd  "
                        "entry=$%.2f  peak=$%.2f  — STOP CANNOT FIRE",
                        ticker, held, pos.entry_price, pos.peak_price,
                    )
                continue

            held_days = (date - pos.entry_date).days
            drawdown_from_peak = (price - pos.peak_price) / pos.peak_price
            drawdown_from_entry = (price - pos.entry_price) / pos.entry_price

            if drawdown_from_entry < -0.25:
                log.warning(
                    "  ⚠ BIG LOSS %-10s  from_entry=%.1f%%  from_peak=%.1f%%  "
                    "price=$%.2f  peak=$%.2f  entry=$%.2f  held=%dd",
                    ticker,
                    drawdown_from_entry * 100,
                    drawdown_from_peak * 100,
                    price, pos.peak_price, pos.entry_price, held_days,
                )

            reason = None
            exit_type = None

            if held_days >= self.max_hold_days:
                reason = f"max_hold ({held_days}d >= {self.max_hold_days}d)"
                exit_type = "force_exit_max_hold"
            elif drawdown_from_peak <= -self.trailing_stop_pct:
                reason = (
                    f"trailing_stop ({drawdown_from_peak:+.1%} from "
                    f"peak ${pos.peak_price:.2f})"
                )
                exit_type = "force_exit_trailing"

            if reason:
                log.info("  ✖ FORCE-EXIT %-10s  %s", ticker, reason)
                self._sell(date, ticker, price, exit_type=exit_type)
                force_sold.append(ticker)

        return force_sold

    # ──────────────────────────────────────────────────────────
    #  Upgrade
    # ──────────────────────────────────────────────────────────
    def try_upgrades(
        self,
        date,
        candidate_scores: Dict[str, float],
        prices: Dict[str, float],
        max_upgrades: int = 2,
    ) -> List[Tuple[str, str]]:
        if not self.positions or not candidate_scores:
            return []

        held_scored = [
            (t, pos.latest_score)
            for t, pos in self.positions.items()
            if self._can_sell(pos, date, prices.get(t, pos.entry_price))
        ]
        held_scored.sort(key=lambda x: x[1])

        candidates = [
            (t, s)
            for t, s in candidate_scores.items()
            if t not in self.positions and t in prices
        ]
        candidates.sort(key=lambda x: x[1], reverse=True)

        swaps = []
        used_held = set()
        used_cand = set()

        for cand_ticker, cand_score in candidates:
            if len(swaps) >= max_upgrades:
                break
            if cand_ticker in used_cand:
                continue

            for held_ticker, held_score in held_scored:
                if held_ticker in used_held:
                    continue
                gap = cand_score - held_score
                if gap >= self.upgrade_min_score_gap:
                    log.info(
                        "  ⇄ UPGRADE  sell %-10s (score %.3f) → "
                        "buy %-10s (score %.3f, gap +%.3f)",
                        held_ticker, held_score,
                        cand_ticker, cand_score, gap,
                    )
                    self._sell(
                        date, held_ticker, prices[held_ticker],
                        exit_type="upgrade_out",
                    )
                    self._buy(
                        date, cand_ticker, prices[cand_ticker],
                        current_nav=self.nav,
                    )
                    used_held.add(held_ticker)
                    used_cand.add(cand_ticker)
                    swaps.append((held_ticker, cand_ticker))
                    break

        return swaps

    # ──────────────────────────────────────────────────────────
    #  BLENDED BUY RANKING — momentum/beta tilt
    #
    #  The ranking formula:
    #    blended_rank = composite_rank * (1 - tilt) + momentum_rank * tilt
    #
    #  Where momentum_rank is derived from:
    #    - rszscore (relative strength z-score vs benchmark)
    #    - trailing realized volatility (higher vol = higher beta proxy)
    #    - rsi14 (current momentum)
    #
    #  This ensures that among all BUY candidates with similar scores,
    #  we pick the ones with higher beta / stronger momentum.
    # ──────────────────────────────────────────────────────────
    def _rank_buy_candidates(
        self,
        buy_tickers: List[str],
        scores: Dict[str, float],
        momentum_metrics: Dict[str, Dict[str, float]],
        trailing_vols: Dict[str, float],
        params: Dict,
    ) -> List[str]:
        """
        Re-rank buy candidates using blended composite + momentum score.

        params keys:
            momentum_tilt:     0.0-1.0, how much to favor momentum (default 0.4)
            vol_preference:    0.0-1.0, weight of vol in momentum score (default 0.3)
            min_trailing_vol:  minimum annualized vol to pass filter (default 0.0)
            min_rszscore:      minimum RS z-score to pass filter (default -99)
            prefer_strong_buy: bonus multiplier for STRONG_BUY (default 1.0, no bonus)

        Returns: buy_tickers sorted best-first (highest blended rank).
        """
        if not buy_tickers:
            return []

        tilt = params.get("momentum_tilt", 0.4)
        vol_weight = params.get("vol_preference", 0.3)
        min_vol = params.get("min_trailing_vol", 0.0)
        min_rs = params.get("min_rszscore", -99.0)

        # ── Step 1: Filter out low-vol / low-RS names ────────────
        filtered = []
        for t in buy_tickers:
            vol = trailing_vols.get(t, 0.0)
            mm = momentum_metrics.get(t, {})
            rs = mm.get("rszscore", mm.get("rs_zscore", 0.0))

            # Filter: skip low-vol names (likely bonds, utilities, stable large caps)
            if min_vol > 0 and vol < min_vol:
                self._filtered_low_vol += 1
                log.debug(
                    "  FILTER %-10s: trailing_vol=%.3f < min_vol=%.3f",
                    t, vol, min_vol,
                )
                continue

            # Filter: skip negative RS z-score (underperforming benchmark)
            if rs < min_rs:
                log.debug(
                    "  FILTER %-10s: rszscore=%.3f < min_rszscore=%.3f",
                    t, rs, min_rs,
                )
                continue

            filtered.append(t)

        if not filtered:
            # If all filtered out, fall back to original list (safety)
            log.debug(
                "  RANKING: all %d candidates filtered out, using unfiltered",
                len(buy_tickers),
            )
            filtered = buy_tickers

        # ── Step 2: Compute composite rank (percentile) ──────────
        n = len(filtered)
        if n == 1:
            return filtered

        # Sort by composite score → assign percentile rank (0=worst, 1=best)
        score_sorted = sorted(filtered, key=lambda t: scores.get(t, 0.0))
        composite_rank = {t: i / (n - 1) for i, t in enumerate(score_sorted)}

        # ── Step 3: Compute momentum rank ────────────────────────
        # Momentum signal = weighted blend of rszscore + trailing_vol + rsi
        momentum_raw = {}
        for t in filtered:
            mm = momentum_metrics.get(t, {})
            rs = mm.get("rszscore", mm.get("rs_zscore", 0.0))
            rsi = mm.get("rsi14", mm.get("rsi_14", 50.0))
            vol = trailing_vols.get(t, 0.30)  # default 30% annualized

            # Normalize RSI to 0-1 scale (30-70 range → 0-1)
            rsi_norm = max(0.0, min(1.0, (rsi - 30) / 40.0))

            # Normalize RS z-score (typically -3 to +3 → 0-1)
            rs_norm = max(0.0, min(1.0, (rs + 2.0) / 4.0))

            # Normalize vol (0.15 to 0.80 typical range → 0-1)
            vol_norm = max(0.0, min(1.0, (vol - 0.15) / 0.65))

            # Blend: RS is primary, vol preference configurable, RSI secondary
            rs_weight = 1.0 - vol_weight - 0.2  # remainder after vol + rsi
            momentum_raw[t] = (
                rs_norm * max(rs_weight, 0.3) +
                vol_norm * vol_weight +
                rsi_norm * 0.2
            )

        # Rank momentum (percentile)
        mom_sorted = sorted(filtered, key=lambda t: momentum_raw.get(t, 0.0))
        momentum_rank = {t: i / (n - 1) for i, t in enumerate(mom_sorted)}

        # ── Step 4: Blend ────────────────────────────────────────
        blended = {}
        for t in filtered:
            blended[t] = (
                composite_rank[t] * (1.0 - tilt) +
                momentum_rank[t] * tilt
            )

        # Sort descending (highest blended rank = best candidate)
        result = sorted(filtered, key=lambda t: blended[t], reverse=True)

        # ── Log ranking details (first time only) ────────────────
        if not self._ranking_logged and len(result) >= 3:
            log.info(
                "  RANKING (tilt=%.2f, vol_pref=%.2f, min_vol=%.3f, min_rs=%.1f):",
                tilt, vol_weight, min_vol, min_rs,
            )
            for t in result[:5]:
                mm = momentum_metrics.get(t, {})
                log.info(
                    "    %-12s  composite_rank=%.2f  mom_rank=%.2f  "
                    "blended=%.3f  score=%.3f  rs=%.2f  vol=%.3f",
                    t,
                    composite_rank.get(t, 0),
                    momentum_rank.get(t, 0),
                    blended.get(t, 0),
                    scores.get(t, 0),
                    mm.get("rszscore", mm.get("rs_zscore", 0)),
                    trailing_vols.get(t, 0),
                )
            if len(buy_tickers) > len(filtered):
                log.info(
                    "    (filtered %d → %d candidates)",
                    len(buy_tickers), len(filtered),
                )
            self._ranking_logged = True

        return result

    # ──────────────────────────────────────────────────────────
    #  Process signals — with momentum-tilted ranking
    # ──────────────────────────────────────────────────────────
    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
        scores: Optional[Dict[str, float]] = None,
        current_nav: Optional[float] = None,
        position_sizes: Optional[Dict[str, float]] = None,
        momentum_metrics: Optional[Dict[str, Dict[str, float]]] = None,
        trailing_vols: Optional[Dict[str, float]] = None,
        buy_ranking_params: Optional[Dict] = None,
    ) -> None:
        scores = scores or {}
        position_sizes = position_sizes or {}
        momentum_metrics = momentum_metrics or {}
        trailing_vols = trailing_vols or {}
        buy_ranking_params = buy_ranking_params or {}

        # ── sells first (respect min hold) ────────────────────
        blocked_sells = []
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker not in prices:
                    continue
                pos = self.positions[ticker]
                if self._can_sell(pos, date, prices[ticker]):
                    self._sell(date, ticker, prices[ticker], exit_type="signal_exit")
                else:
                    held = (date - pos.entry_date).days
                    blocked_sells.append((ticker, held))

        if blocked_sells:
            self._blocked_sells += len(blocked_sells)
            log.debug(
                "  min-hold blocked %d sells: %s",
                len(blocked_sells),
                [(t, f"{d}d") for t, d in blocked_sells[:5]],
            )

        # ── then buys — MOMENTUM-TILTED RANKING ──────────────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]

        # Apply blended ranking (composite + momentum/beta tilt)
        if buy_tickers and (momentum_metrics or trailing_vols):
            buy_tickers = self._rank_buy_candidates(
                buy_tickers,
                scores=scores,
                momentum_metrics=momentum_metrics,
                trailing_vols=trailing_vols,
                params=buy_ranking_params,
            )
        else:
            # Fallback: pure composite score ranking
            buy_tickers.sort(key=lambda t: scores.get(t, 0.0), reverse=True)

        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            ticker_weight = position_sizes.get(ticker)
            self._buy(
                date, ticker, prices[ticker],
                current_nav=current_nav,
                target_weight=ticker_weight,
            )

    # ──────────────────────────────────────────────────────────
    #  Update held positions' scores
    # ──────────────────────────────────────────────────────────
    def update_scores(self, scores: Dict[str, float]) -> None:
        for ticker, pos in self.positions.items():
            if ticker in scores:
                pos.latest_score = scores[ticker]

    # ──────────────────────────────────────────────────────────
    #  Buy — VARIABLE SIZING
    # ──────────────────────────────────────────────────────────
    def _buy(
        self,
        date,
        ticker: str,
        raw_price: float,
        current_nav: Optional[float] = None,
        target_weight: Optional[float] = None,
    ) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)

        sizing_nav = current_nav if current_nav else self.nav

        if target_weight is not None and target_weight > 0:
            weight = min(target_weight, 0.25)
            slot_target = sizing_nav * weight
            sizing_source = f"pipeline_weight={target_weight:.3f}"
        else:
            weight = 1.0 / self.max_positions
            slot_target = sizing_nav * weight
            sizing_source = f"equal_weight=1/{self.max_positions}"

        target_value = min(slot_target, self.cash * 0.95)

        if target_value < 1_000:
            log.debug(
                "  SKIP BUY %-10s: target_value=$%.0f too small "
                "(nav=$%s, cash=$%s, weight=%.3f, source=%s)",
                ticker, target_value,
                f"{sizing_nav:,.0f}", f"{self.cash:,.0f}",
                weight, sizing_source,
            )
            return

        shares = int(target_value / exec_price)
        if shares <= 0:
            return

        cost = shares * exec_price
        commission = cost * self.commission_rate
        total = cost + commission
        if total > self.cash:
            max_cost = self.cash / (1 + self.commission_rate)
            shares = int(max_cost / exec_price)
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
            peak_price=exec_price,
            current_price=exec_price,
            target_weight=weight,
        )
        self._total_buys += 1

        log.info(
            "  ▲ BUY  %-10s  %d shares @ %.2f  cost $%s  "
            "(weight=%.1f%%, slot=$%s, nav=$%s, %s)",
            ticker, shares, exec_price,
            f"{total:,.0f}",
            weight * 100,
            f"{slot_target:,.0f}",
            f"{sizing_nav:,.0f}",
            sizing_source,
        )

    # ──────────────────────────────────────────────────────────
    #  Sell
    # ──────────────────────────────────────────────────────────
    def _sell(
        self,
        date,
        ticker: str,
        raw_price: float,
        exit_type: Optional[str] = None,
    ) -> None:
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
        self._total_sells += 1

        trade_record = {
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
            "peak_price": pos.peak_price,
            "max_favorable": (pos.peak_price / pos.entry_price) - 1.0,
            "latest_score": pos.latest_score,
            "entry_weight": pos.target_weight,
        }
        if exit_type:
            trade_record["exit_type"] = exit_type

        self.closed_trades.append(trade_record)

        log.info(
            "  ▼ SELL %-10s  %d shares @ %.2f  PnL $%s (%+.1f%%)  "
            "held %dd  [%s]",
            ticker, pos.shares, exec_price,
            f"{pnl:,.0f}", pnl_pct * 100, held,
            exit_type or "unknown",
        )

    # ──────────────────────────────────────────────────────────
    #  Mark-to-market
    # ──────────────────────────────────────────────────────────
    def mark_to_market(self, date, close_prices: Dict[str, float]) -> float:
        pos_val = 0.0
        for t, p in self.positions.items():
            price = close_prices.get(t, p.current_price)
            p.current_price = price
            pos_val += price * p.shares
            if price > p.peak_price:
                p.peak_price = price
        return self.cash + pos_val
        
#######################################