"""
ingest/options/providers/base.py
Provider strategy interface for options-chain ingestion.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Tuple

import pandas as pd


class OptionsProvider(ABC):
    """Contract every options data provider must satisfy."""

    #: short, lowercase id used in the `provider` DB column
    name: str = "abstract"

    #: True if the provider returns greeks directly; False means the
    #: orchestrator will compute them from quotes via common.greeks
    provides_greeks: bool = False

    # ------------------------------------------------------------------ #
    # Lifecycle (optional override; default is no-op)
    # ------------------------------------------------------------------ #
    def connect(self) -> None:
        """Establish any persistent connection (IBKR socket, etc.)."""
        return None

    def disconnect(self) -> None:
        """Tear down persistent connection."""
        return None

    # ------------------------------------------------------------------ #
    # Required API
    # ------------------------------------------------------------------ #
    @abstractmethod
    def list_expiries(self, symbol: str) -> List[str]:
        """Return all available expiries for *symbol* as ISO strings."""

    @abstractmethod
    def fetch_chain(
        self,
        symbol: str,
        expiries: List[Tuple[date, str]],
        snapshot_date: date,
    ) -> pd.DataFrame:
        """
        Fetch the option chain for *symbol* across the given expiries.

        Returns a DataFrame with at least these columns:
            snapshot_date, symbol, expiry, strike, right,
            bid, ask, last, volume, open_interest, iv,
            underlying_price
        Greeks (delta/gamma/vega/theta/rho) are optional; if absent,
        the orchestrator will compute them.
        """