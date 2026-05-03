"""
ingest/options/ingest_options.py
One CLI to ingest options chains for any market via any provider.

Examples
--------
    # default provider for the market (yfinance for US/HK)
    python -m ingest.options.ingest_options --market US

    # explicit provider
    python -m ingest.options.ingest_options --market HK --provider yfinance

    # dry-run, two symbols only
    python -m ingest.options.ingest_options --market US --symbols AAPL MSFT --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from typing import List, Tuple

import pandas as pd

from common.db_writer import upsert_options
from common.expiry import select_expiries_from_chain
from common.greeks import compute_greeks_df
from common.universe import get_universe_for_market

from ingest.options.providers.base import OptionsProvider
from ingest.options.providers.yfinance_provider import YFinanceProvider
from ingest.options.providers.ibkr_provider import IBKRProvider

log = logging.getLogger("ingest_options")

# ---------------------------------------------------------------------------
# Provider registry & per-market default
# ---------------------------------------------------------------------------
_PROVIDERS = {
    "yfinance": YFinanceProvider,
    "ibkr": IBKRProvider,
}

_DEFAULT_PROVIDER_BY_MARKET = {
    "US": "yfinance",
    "HK": "yfinance",
    "IN": "yfinance",
}


def make_provider(name: str) -> OptionsProvider:
    cls = _PROVIDERS.get(name.lower())
    if cls is None:
        raise ValueError(
            f"Unknown provider {name!r}. Available: {sorted(_PROVIDERS)}"
        )
    return cls()


# ---------------------------------------------------------------------------
# Per-symbol pipeline
# ---------------------------------------------------------------------------
def _process_symbol(
    provider: OptionsProvider,
    symbol: str,
    market: str,
    snapshot_date: date,
    n_expiries: int,
    rf_rate: float,
) -> pd.DataFrame:
    """Fetch + normalise + (maybe) compute greeks for one underlying."""
    available = provider.list_expiries(symbol)
    if not available:
        log.warning("[%s] no expiries returned by %s", symbol, provider.name)
        return pd.DataFrame()

    selected: List[Tuple[date, str]] = select_expiries_from_chain(
        available, n=n_expiries, ref_date=snapshot_date
    )
    if not selected:
        log.warning("[%s] no future expiries within selection window", symbol)
        return pd.DataFrame()

    log.info(
        "[%s] fetching %d expiries: %s",
        symbol, len(selected), [iso for _, iso in selected],
    )

    df = provider.fetch_chain(symbol, selected, snapshot_date)
    if df is None or df.empty:
        log.warning("[%s] provider returned empty chain", symbol)
        return pd.DataFrame()

    if provider.provides_greeks:
        df["greeks_source"] = "ibkr_model"
    else:
        df = compute_greeks_df(df, r=rf_rate)
        df["greeks_source"] = "computed_bs"

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest options chains into DB.")
    p.add_argument("--market", required=True, choices=["US", "HK", "IN"])
    p.add_argument(
        "--provider",
        default=None,
        help="Provider id (default: per-market default)",
    )
    p.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Override universe with explicit symbol list",
    )
    p.add_argument(
        "--n-expiries",
        type=int,
        default=2,
        help="Number of distinct-month expiries to ingest per symbol",
    )
    p.add_argument(
        "--rf-rate",
        type=float,
        default=0.045,
        help="Risk-free rate for BS greeks (used only if provider lacks greeks)",
    )
    p.add_argument(
        "--snapshot-date",
        default=None,
        help="ISO date for the snapshot (default: today UTC)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    market = args.market.upper()
    provider_name = args.provider or _DEFAULT_PROVIDER_BY_MARKET[market]
    provider = make_provider(provider_name)

    snapshot_date = (
        date.fromisoformat(args.snapshot_date)
        if args.snapshot_date else date.today()
    )

    symbols = args.symbols or get_universe_for_market(market)
    log.info(
        "ingest_options start: market=%s provider=%s symbols=%d snapshot=%s",
        market, provider_name, len(symbols), snapshot_date,
    )

    provider.connect()
    total_rows = 0
    failed: list[str] = []
    try:
        for i, sym in enumerate(symbols, 1):
            try:
                df = _process_symbol(
                    provider=provider,
                    symbol=sym,
                    market=market,
                    snapshot_date=snapshot_date,
                    n_expiries=args.n_expiries,
                    rf_rate=args.rf_rate,
                )
                if df.empty:
                    failed.append(sym)
                    continue
                n = upsert_options(df, market=market, dry_run=args.dry_run)
                total_rows += n
                log.info("[%d/%d] %s: %d rows", i, len(symbols), sym, len(df))
            except Exception as e:
                log.exception("[%s] failed: %s", sym, e)
                failed.append(sym)
    finally:
        provider.disconnect()

    log.info(
        "ingest_options done: %d rows written, %d symbols failed/empty: %s",
        total_rows, len(failed), failed[:20],
    )
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())