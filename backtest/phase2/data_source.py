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