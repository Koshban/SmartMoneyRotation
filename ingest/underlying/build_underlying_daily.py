"""Build per-symbol per-day feature rows for {market}_underlying_daily.

Modes:
  - Backfill:       --lookback 2y    (one-time history load)
  - Daily increment: --lookback 10d  (cron / manual daily run, 10d cushion for revisions)

Examples:
  python -m ingest.underlying.build_underlying_daily --market us --lookback 2y
  python -m ingest.underlying.build_underlying_daily --market hk --lookback 10d
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from common.universe import get_universe_for_market
from common.db_writer import upsert_underlying_daily
from ingest.underlying.indicators import compute_all

log = logging.getLogger("build_underlying_daily")

BATCH_SIZE = 50          # yfinance multi-ticker chunk
WARMUP_DAYS = 260        # extra bars so SMA200/RV60 are valid at the start of target window


def _parse_lookback(s: str) -> int:
    """'2y' -> 730, '10d' -> 10, '6mo' -> 180. Returns days."""
    s = s.strip().lower()
    if s.endswith("y"):
        return int(float(s[:-1]) * 365)
    if s.endswith("mo"):
        return int(float(s[:-2]) * 30)
    if s.endswith("d"):
        return int(s[:-1])
    return int(s)


def _yf_symbol(symbol: str, market: str) -> str:
    """Map internal ticker to yfinance symbol. HK names need .HK suffix."""
    if market == "hk" and not symbol.endswith(".HK"):
        return f"{symbol}.HK"
    return symbol


def _from_yf(symbol: str, market: str) -> str:
    """Inverse mapping — strip suffix when storing."""
    if market == "hk" and symbol.endswith(".HK"):
        return symbol[:-3]
    return symbol

def _normalize_symbol(symbol: str, market: str) -> str:
    """Universe → canonical bare symbol (strip market suffix if present)."""
    symbol = symbol.strip().upper()
    if market == "hk" and symbol.endswith(".HK"):
        return symbol[:-3]
    # future: .SS, .SZ, .TO, .NS, etc.
    return symbol


def fetch_batch(symbols: list[str], start: date, end: date, market: str) -> dict[str, pd.DataFrame]:
    """Download OHLCV. Returns {bare_symbol: df}."""
    yf_to_bare = {_yf_symbol(s, market): s for s in symbols}
    yf_symbols = list(yf_to_bare.keys())
    log.info(f"  yfinance download: {len(yf_symbols)} symbols, {start} → {end}")
    raw = yf.download(
        tickers=yf_symbols,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    out = {}
    for yfs, bare in yf_to_bare.items():
        try:
            sub = raw[yfs] if len(yf_symbols) > 1 else raw
            sub = sub.dropna(subset=["Close"]).copy()
            if sub.empty:
                continue
            sub.columns = [c.lower() for c in sub.columns]
            sub.index = pd.to_datetime(sub.index).date
            sub.index.name = "date"
            out[bare] = sub[["open", "high", "low", "close", "volume"]]
        except Exception as e:
            log.warning(f"  parse failed for {yfs}: {e}")
    return out


def build_features(symbol: str, ohlcv: pd.DataFrame, target_start: date) -> pd.DataFrame:
    """Compute all indicators, return rows from target_start onward."""
    if len(ohlcv) < 30:
        return pd.DataFrame()
    feats = compute_all(ohlcv)
    feats = feats.reset_index()
    feats["symbol"] = symbol
    feats = feats[feats["date"] >= target_start]
    return feats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["us", "hk", "in", "US", "HK", "IN"])
    ap.add_argument("--lookback", default="10d", help="e.g. '2y', '6mo', '10d'")
    ap.add_argument("--symbols", help="Comma-separated override; default = full universe")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, help="Process only first N symbols (debug)")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    market = args.market.lower()
    lookback_days = _parse_lookback(args.lookback)
    end_dt = date.today()
    target_start = end_dt - timedelta(days=lookback_days)
    fetch_start = target_start - timedelta(days=WARMUP_DAYS)

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        symbols = get_universe_for_market(market)

    # canonicalize: strip any market suffix the universe source might include
    symbols = [_normalize_symbol(s, market) for s in symbols]
    # de-dupe while preserving order
    symbols = list(dict.fromkeys(symbols))

    if args.limit:
        symbols = symbols[: args.limit]

    log.info(
        f"build_underlying_daily start: market={market.upper()} "
        f"symbols={len(symbols)} target_start={target_start} end={end_dt} "
        f"(fetch_start={fetch_start} with {WARMUP_DAYS}d warmup)"
    )

    total_rows = 0
    failed: list[str] = []

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        log.info(f"[batch {i // BATCH_SIZE + 1}] {len(batch)} symbols")
        try:
            ohlcv_map = fetch_batch(batch, fetch_start, end_dt, market)
        except Exception as e:
            log.error(f"  batch fetch failed: {e}")
            failed.extend(batch)
            continue

        all_feats = []
        for sym in batch:
            ohlcv = ohlcv_map.get(sym)
            if ohlcv is None or ohlcv.empty:
                failed.append(sym)
                continue
            feats = build_features(sym, ohlcv, target_start)
            if feats.empty:
                failed.append(sym)
                continue
            all_feats.append(feats)

        if not all_feats:
            continue

        df = pd.concat(all_feats, ignore_index=True)
        n = upsert_underlying_daily(df, market=market, dry_run=args.dry_run)
        total_rows += n
        log.info(f"  → wrote {n} rows ({len(all_feats)} symbols)")

    log.info(
        f"build_underlying_daily done: {total_rows} rows written, "
        f"{len(failed)} symbols failed/empty"
    )
    if failed:
        log.info(f"failed: {failed[:20]}{'...' if len(failed) > 20 else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())