# data.py
"""
Data ingestion with adapter pattern.
Swap YFinanceProvider for any other source by implementing DataProvider.
"""

import datetime as dt
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from config import ALL_TICKERS, LOOKBACK_CALENDAR_DAYS


# ── Abstract Interface ──────────────────────────────────────────────
class DataProvider(ABC):
    """Any data source must implement this single method."""

    @abstractmethod
    def fetch_ohlcv(
        self, tickers: List[str], start: dt.date, end: dt.date
    ) -> Dict[str, pd.DataFrame]:
        """
        Return {ticker: DataFrame} where each DataFrame has columns
        ['Open', 'High', 'Low', 'Close', 'Volume'] indexed by date.
        """
        ...


# ── yfinance Implementation ────────────────────────────────────────
class YFinanceProvider(DataProvider):

    def fetch_ohlcv(
        self, tickers: List[str], start: dt.date, end: dt.date
    ) -> Dict[str, pd.DataFrame]:
        if not tickers:
            return {}

        print(f"[DATA] Downloading {len(tickers)} tickers via yfinance …")
        raw = yf.download(
            tickers,
            start=start.isoformat(),
            end=end.isoformat(),
            auto_adjust=True,
            threads=True,
            progress=False,
        )

        result: Dict[str, pd.DataFrame] = {}

        if len(tickers) == 1:
            t = tickers[0]
            cols = ["Open", "High", "Low", "Close", "Volume"]
            if all(c in raw.columns for c in cols):
                df = raw[cols].dropna()
                if len(df) > 0:
                    result[t] = df
        else:
            # yfinance returns MultiIndex columns: (field, ticker)
            for t in tickers:
                try:
                    df = pd.DataFrame(
                        {
                            "Open":   raw[("Open",   t)],
                            "High":   raw[("High",   t)],
                            "Low":    raw[("Low",    t)],
                            "Close":  raw[("Close",  t)],
                            "Volume": raw[("Volume", t)],
                        }
                    ).dropna()
                    if len(df) > 0:
                        result[t] = df
                except KeyError:
                    print(f"  [WARN] No data returned for {t}")

        print(f"[DATA] Got data for {len(result)}/{len(tickers)} tickers.")
        return result


# ── Convenience Loader ──────────────────────────────────────────────
def load_all_data(
    provider: Optional[DataProvider] = None,
    lookback_days: int = LOOKBACK_CALENDAR_DAYS,
) -> Dict[str, pd.DataFrame]:
    """
    One-call loader: fetches the full universe + benchmarks.
    Returns {ticker: OHLCV DataFrame}.
    """
    if provider is None:
        provider = YFinanceProvider()

    end = dt.date.today()
    start = end - dt.timedelta(days=lookback_days)

    return provider.fetch_ohlcv(ALL_TICKERS, start, end)