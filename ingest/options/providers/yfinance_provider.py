"""
ingest/options/providers/yfinance_provider.py
yfinance implementation of OptionsProvider.

Notes
-----
* yfinance does NOT return greeks → provides_greeks = False.
* Soft rate-limit: small sleep between calls.
* `symbol` is the yfinance form (e.g. 'AAPL', '0700.HK').
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import List, Tuple

import pandas as pd
import yfinance as yf

from ingest.options.providers.base import OptionsProvider

log = logging.getLogger(__name__)


class YFinanceProvider(OptionsProvider):
    name = "yfinance"
    provides_greeks = False

    def __init__(self, sleep_between_calls: float = 0.4):
        self.sleep = sleep_between_calls

    # -------------------------- expiries -------------------------- #
    def list_expiries(self, symbol: str) -> List[str]:
        try:
            tk = yf.Ticker(symbol)
            return list(tk.options or [])
        except Exception as e:
            log.warning("yfinance list_expiries(%s) failed: %s", symbol, e)
            return []

    # --------------------------- chain ---------------------------- #
    def fetch_chain(
        self,
        symbol: str,
        expiries: List[Tuple[date, str]],
        snapshot_date: date,
    ) -> pd.DataFrame:
        if not expiries:
            return pd.DataFrame()

        tk = yf.Ticker(symbol)
        spot = self._get_spot(tk, symbol)

        frames: list[pd.DataFrame] = []
        for exp_date, exp_iso in expiries:
            try:
                chain = tk.option_chain(exp_iso)
            except Exception as e:
                log.warning("option_chain(%s, %s) failed: %s", symbol, exp_iso, e)
                continue

            for side_df, opt_type in ((chain.calls, "C"), (chain.puts, "P")):
                if side_df is None or side_df.empty:
                    continue
                f = self._normalise(side_df, opt_type)
                f["expiry"] = exp_date
                frames.append(f)

            time.sleep(self.sleep)

        if not frames:
            return pd.DataFrame()

        out = pd.concat(frames, ignore_index=True)
        out["date"] = snapshot_date
        out["symbol"] = symbol
        out["underlying_price"] = spot
        out["source"] = self.name
        return out

    # -------------------------- helpers --------------------------- #
    @staticmethod
    def _get_spot(tk: "yf.Ticker", symbol: str) -> float | None:
        try:
            p = getattr(tk, "fast_info", {}).get("last_price")
            if p:
                return float(p)
        except Exception:
            pass
        try:
            h = tk.history(period="1d")
            if not h.empty:
                return float(h["Close"].iloc[-1])
        except Exception as e:
            log.warning("spot lookup failed for %s: %s", symbol, e)
        return None

    @staticmethod
    def _normalise(df: pd.DataFrame, opt_type: str) -> pd.DataFrame:
        """Map yfinance columns → schema column names."""
        rename = {
            "strike":            "strike",
            "bid":               "bid",
            "ask":               "ask",
            "lastPrice":         "last",
            "volume":            "volume",
            "openInterest":      "oi",
            "impliedVolatility": "iv",
        }
        keep = [c for c in rename if c in df.columns]
        out = df[keep].rename(columns=rename).copy()
        out["opt_type"] = opt_type
        return out