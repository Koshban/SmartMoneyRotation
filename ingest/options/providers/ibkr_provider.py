"""
ingest/options/providers/ibkr_provider.py
Placeholder for the IBKR provider.  Real implementation will use
ib_insync, reqSecDefOptParams + reqMktData with modelGreeks.
"""

from __future__ import annotations

from datetime import date
from typing import List, Tuple

import pandas as pd

from ingest.options.providers.base import OptionsProvider


class IBKRProvider(OptionsProvider):
    name = "ibkr"
    provides_greeks = True

    def connect(self) -> None:
        raise NotImplementedError("IBKRProvider not implemented yet")

    def list_expiries(self, symbol: str) -> List[str]:
        raise NotImplementedError

    def fetch_chain(
        self,
        symbol: str,
        expiries: List[Tuple[date, str]],
        snapshot_date: date,
    ) -> pd.DataFrame:
        raise NotImplementedError