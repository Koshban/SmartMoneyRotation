"""
Fetch option chains for every symbol in the universe and save to CSV.

For each symbol × next 2 monthly expiries:
  • 5 nearest OTM puts  (strike ≤ current price, descending)
  • 5 nearest OTM calls (strike ≥ current price, ascending)

Usage
-----
    python src/ingest_options.py --market us
    python src/ingest_options.py --market us --rungs 7
"""

import argparse, logging, sys, time
from datetime import date
from pathlib import Path

import pandas as pd
import yfinance as yf

# ── Path setup ─────────────────────────────────────────────────────────
# ingest_options.py lives in  <root>/src/
# universe.py    lives in  <root>/common/
SRC  = Path(__file__).resolve().parent          # .../SmartMoneyRotation/src
ROOT = SRC.parent                               # .../SmartMoneyRotation
sys.path.insert(0, str(ROOT))                   # so "from common.xxx" works

from common.universe import (
    get_us_only_etfs,
    get_all_single_names,
    is_hk_ticker,
    is_india_ticker,
)
from common.expiry import next_monthly_expiries, match_expiry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)

DATA    = ROOT / "data" / "options"
RUNGS   = 5
DELAY   = 1.5


# ── Build the US symbol list from universe.py ─────────────────────────
def _us_symbols() -> list[str]:
    """All US-listed ETFs + single names (no .HK, no .NS)."""
    etfs    = get_us_only_etfs()
    singles = [s for s in get_all_single_names()
               if not is_hk_ticker(s) and not is_india_ticker(s)]
    combined = sorted(set(etfs + singles))
    return combined


# ── helpers ────────────────────────────────────────────────────────────
def current_price(ticker: yf.Ticker) -> float | None:
    """Latest price from yfinance fast_info."""
    try:
        fi = ticker.fast_info
        return float(fi.get("lastPrice") or fi.get("regularMarketPrice"))
    except Exception:
        return None


def select_strikes(
    puts_df:  pd.DataFrame,
    calls_df: pd.DataFrame,
    price:    float,
    n:        int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pick *n* nearest OTM strikes on each side of *price*."""
    otm_p = (
        puts_df[puts_df["strike"] <= price]
        .sort_values("strike", ascending=False)
        .head(n)
    )
    otm_c = (
        calls_df[calls_df["strike"] >= price]
        .sort_values("strike", ascending=True)
        .head(n)
    )
    return otm_p, otm_c


def _row(today, symbol, exp_date, opt_type, r):
    return {
        "date":     today.isoformat(),
        "symbol":   symbol,
        "expiry":   exp_date.isoformat(),
        "strike":   r["strike"],
        "opt_type": opt_type,
        "bid":      r.get("bid"),
        "ask":      r.get("ask"),
        "last":     r.get("lastPrice"),
        "volume":   r.get("volume"),
        "oi":       r.get("openInterest"),
        "iv":       r.get("impliedVolatility"),
    }


# ── per-symbol fetch ──────────────────────────────────────────────────
def fetch_symbol(symbol: str, n_rungs: int = 5) -> pd.DataFrame | None:
    ticker = yf.Ticker(symbol)

    price = current_price(ticker)
    if price is None:
        LOG.warning(f"    {symbol}: no price, skipping")
        return None

    try:
        available = ticker.options          # tuple of 'YYYY-MM-DD'
    except Exception as e:
        LOG.warning(f"    {symbol}: cannot read expiries – {e}")
        return None

    if not available:
        LOG.warning(f"    {symbol}: no options listed")
        return None

    targets = next_monthly_expiries(market="us", n=2)
    matched = match_expiry(targets, available)

    if not matched:
        LOG.warning(
            f"    {symbol}: no monthly expiry matched  "
            f"(targets={targets}, first avail={available[:4]})"
        )
        return None

    LOG.info(
        f"    {symbol:<10s}  price={price:>10.2f}   "
        f"expiries={[str(d) for d, _ in matched]}"
    )

    today = date.today()
    rows  = []

    for exp_date, exp_str in matched:
        try:
            chain = ticker.option_chain(exp_str)
        except Exception as e:
            LOG.warning(f"    {symbol} {exp_str}: chain error – {e}")
            continue

        otm_p, otm_c = select_strikes(chain.puts, chain.calls, price, n_rungs)

        for _, r in otm_p.iterrows():
            rows.append(_row(today, symbol, exp_date, "P", r))
        for _, r in otm_c.iterrows():
            rows.append(_row(today, symbol, exp_date, "C", r))

        time.sleep(DELAY)

    return pd.DataFrame(rows) if rows else None


# ── main loop ─────────────────────────────────────────────────────────
def run(n_rungs: int):
    symbols = _us_symbols()
    if not symbols:
        LOG.error("No US symbols found in universe")
        return

    out_dir = DATA / "us"
    out_dir.mkdir(parents=True, exist_ok=True)

    LOG.info(f"{'=' * 60}")
    LOG.info(f"US OPTIONS — {len(symbols)} symbols, {n_rungs} rungs/side")
    LOG.info(f"{'=' * 60}")

    total  = 0
    errors = 0
    skips  = 0

    for i, sym in enumerate(symbols, 1):
        LOG.info(f"[{i}/{len(symbols)}]  {sym}")

        try:
            df = fetch_symbol(sym, n_rungs)
        except Exception as e:
            LOG.error(f"    {sym}: unexpected error – {e}")
            errors += 1
            continue

        if df is None or df.empty:
            skips += 1
            continue

        fname = sym.replace(".", "_").replace("/", "_") + ".csv"
        path  = out_dir / fname

        # append-safe: keep prior days, replace today if re-running
        if path.exists():
            prev = pd.read_csv(path, dtype={"date": str})
            prev = prev[prev["date"] != date.today().isoformat()]
            df   = pd.concat([prev, df], ignore_index=True)

        df.to_csv(path, index=False)
        day_ct = int((df["date"] == date.today().isoformat()).sum())
        total += day_ct
        LOG.info(f"         → {day_ct} contracts  ({path.name})")

        time.sleep(DELAY)

    LOG.info(f"{'=' * 60}")
    LOG.info(f"DONE US: {total} contracts | {skips} skipped | {errors} errors")
    LOG.info(f"Output → {out_dir}")
    LOG.info(f"{'=' * 60}")


# ── CLI ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Fetch US options chains")
    ap.add_argument("--market", default="us",
                    help="Only 'us' supported for now")
    ap.add_argument("--rungs",  type=int, default=RUNGS,
                    help=f"OTM strikes per side (default {RUNGS})")
    args = ap.parse_args()

    run(args.rungs)


if __name__ == "__main__":
    main()